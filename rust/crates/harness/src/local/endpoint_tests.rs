use super::*;
use crate::local::{
    Event, Observer,
    tests::{text, volume, volume_config},
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, PersistenceCut, StreamError, StreamPath, StreamProviderExt,
};
use std::sync::atomic::{AtomicBool, Ordering};

struct Cut(AtomicBool);
impl PersistenceCut for Cut {
    fn admit(&self) -> bool {
        false
    }
    fn acknowledge(&self) -> bool {
        true
    }
    fn crashed(&self, result: std::result::Result<(), acyclic_stream::CutSimulationError>) {
        assert!(
            result.is_ok(),
            "actual journal crash simulation failed: {result:?}"
        );
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn terminal(observer: &mut Observer) -> Error {
    loop {
        if let Err(error) = observer.next().await {
            return error;
        }
    }
}

fn message(error: Error) -> String {
    let Error::Storage(message) = error else {
        panic!("expected original storage classification, got {error:?}");
    };
    message
}

#[tokio::test]
async fn local_commands_and_observation_report_the_original_provider_failure() -> Result<()> {
    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?,
    );
    let connection = harness.connect().await?;
    let mut observer = Observer::open(&connection, 0, 0)?;
    // Wait for the original adoption, not a guessed delay before cutting storage.
    loop {
        if matches!(observer.next().await?, Event::Status(status) if matches!(status.phase, crate::agent::Phase::Idle))
        {
            break;
        }
    }
    assert!(matches!(
        connection.execute(&AgentCommand::Root {}).await?,
        AgentReply::Root { .. }
    ));
    let session = crate::OperationId::from_bytes(connection.root.id_bytes());
    let AgentReply::ClientFiles { snapshot } = connection
        .execute(&AgentCommand::ClientFiles {
            agent: connection.root.clone(),
            session,
            paths: vec!["layout.json".into()],
            after: None,
        })
        .await?
    else {
        return Err(Error::Invalid(
            "client files query returned another reply".into(),
        ));
    };
    let files = AgentCommand::ClientFiles {
        agent: connection.root.clone(),
        session,
        paths: vec!["layout.json".into()],
        after: Some(snapshot.generation),
    };
    let waiting_files = connection.execute(&files);
    tokio::pin!(waiting_files);
    assert!(futures::poll!(&mut waiting_files).is_pending());
    // Retain the same host query independently of the carrier's close. Its
    // original refusal must preserve the cause even when it wins that race.
    let hosted = harness.agents.get().unwrap();
    let paths = ["layout.json".to_owned()];
    let host_files = hosted.server().bound().client_files(
        &connection.root,
        session,
        &paths,
        Some(&snapshot.generation),
    );
    tokio::pin!(host_files);
    assert!(futures::poll!(&mut host_files).is_pending());
    // The same real session without an in-process owner has no local cause to add.
    let mut remote = connection.clone();
    remote._host = None;
    let mut remote_observer = Observer::open(&remote, 0, 0)?;
    // Leave these subscriptions unpolled until their original host has failed.
    // Late opening must terminate too, rather than reconnecting a dead owner.
    let mut projected = Observer::replay(
        &connection,
        &acyclic_harness_client::EventProjection::new("provider-failure".into()),
    )?;
    let stream = harness
        .agent_stream
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing original provider".into()))?;
    stream.flush().map_err(exchange)?;
    let cut = Arc::new(Cut(AtomicBool::new(false)));
    stream.cut_persistence(cut.clone());
    assert_eq!(
        stream
            .append(AppendRequest {
                path: StreamPath::new("failure/report").map_err(exchange)?,
                records: vec![bytes::Bytes::from_static(b"unaccepted").into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await,
        Err(StreamError::Unavailable)
    );
    assert!(cut.0.load(Ordering::SeqCst));
    let cause = stream
        .failure()
        .ok_or_else(|| Error::Invalid("original failure was lost".into()))?
        .to_string();
    // Use the serving codec on the real provider failure, then the exact
    // command projection used by execute; neither delivery order is guessed.
    let host_result = host_files
        .await
        .map(|snapshot| AgentReply::ClientFiles { snapshot });
    let encoded =
        acyclic_harness_client::encode_agent_reply(&host_result, &files).map_err(exchange)?;
    let refusal = acyclic_harness_client::decode_agent_reply(&encoded, &files)
        .map_err(exchange)?
        .expect_err("failed host query returned client files");
    let refusal = AgentExchangeError::Control(refusal);
    assert_eq!(refusal.code(), "unavailable");
    let refused = message(connection.command_failure(&files, refusal));
    assert!(
        refused.contains(&cause),
        "host refusal lost original provider cause: {refused}"
    );
    let closed = message(connection.command_failure(
        &files,
        AgentExchangeError::Session(StreamError::Unavailable),
    ));
    assert!(
        closed.contains(&cause),
        "session close lost cause: {closed}"
    );
    let files_error = message(
        waiting_files
            .await
            .err()
            .ok_or_else(|| Error::Invalid("closed producer answered client files".into()))?,
    );
    assert!(
        files_error.contains(&cause),
        "client files lost original provider cause: {files_error}"
    );
    let command = message(
        connection
            .execute(&AgentCommand::Root {})
            .await
            .err()
            .ok_or_else(|| Error::Invalid("failed provider answered command".into()))?,
    );
    assert!(
        command.contains(&cause),
        "command lost original provider cause: {command}"
    );
    let observed = message(terminal(&mut observer).await);
    assert!(
        observed.contains(&cause),
        "observation lost original provider cause: {observed}"
    );
    let projected = loop {
        match projected.next_projected().await {
            Err(error) | Ok((Err(error), _)) => break message(error),
            Ok(_) => {}
        }
    };
    assert!(
        projected.contains(&cause),
        "projected observation lost original provider cause: {projected}"
    );
    let command = message(
        remote
            .execute(&AgentCommand::Root {})
            .await
            .err()
            .ok_or_else(|| Error::Invalid("failed remote session answered command".into()))?,
    );
    assert!(
        !command.contains(&cause),
        "remote session invented a local cause"
    );
    assert!(!message(terminal(&mut remote_observer).await).contains(&cause));
    // A rejected command still reports its own admission error after storage fails.
    let invalid = AgentCommand::Read {
        agent: connection.root.clone(),
        target: acyclic_harness_client::ReadTarget::ToolOutput {
            path: "..".into(),
            at: None,
        },
        offset: 0,
    };
    let error = message(
        connection
            .execute(&invalid)
            .await
            .err()
            .ok_or_else(|| Error::Invalid("invalid read was admitted".into()))?,
    );
    assert!(
        !error.contains(&cause),
        "unrelated command refusal was rewritten: {error}"
    );
    assert_eq!(provider.request_count(), 0);
    Ok(())
}
