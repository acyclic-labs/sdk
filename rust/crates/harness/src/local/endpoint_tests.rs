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
    eprintln!("original endpoint connected");
    let custody = harness
        .agents
        .get()
        .expect("original hosted runtime")
        .runtime
        .custody();
    let closed = custody.terminated();
    futures::pin_mut!(closed);
    assert!(custody.check().is_ok());
    // The actual serving producer stays live; registration creates no sender.
    assert!(futures::poll!(closed.as_mut()).is_pending());
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
    eprintln!("original adoption observed");
    // Exercise actual signing, serving and enrollment, retaining that exact
    // physical session through the shared controls after its finite Bind ends.
    let hosted = harness.agents.get().expect("original hosted runtime");
    let server = hosted.server();
    let bound = server.bound();
    let observation = bound
        .client_credential(server.root(), server.resource(), harness.clock.clone())
        .await
        .map_err(exchange)?;
    let enrolled = Client::in_process_with_products(
        harness
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Invalid("missing original provider".into()))?,
        observation.authority,
        Some(Arc::new(bound.changes().map_err(exchange)?)),
        server.clone(),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        Some(hosted.runtime.lifetime()),
    );
    enrolled
        .register_credential(0, observation.credential.into())
        .map_err(exchange)?;
    let (controls, _) = AgentControls::discover(enrolled.clone(), 0)
        .await
        .map_err(exchange)?;
    let (selected_controls, _) = controls
        .select_enrolling(connection.root.clone(), 1)
        .await
        .map_err(exchange)?;
    let mut selected = selected_controls
        .observe_all(acyclic_harness_client::AgentSelection {
            agent: connection.root.clone(),
            conversation_from: 0,
            status_from: 0,
        })
        .map_err(exchange)?;
    loop {
        if matches!(selected.next().await.map_err(exchange)?, acyclic_harness_client::AgentEvent::Status { status, .. } if matches!(status.phase, crate::agent::Phase::Idle))
        {
            break;
        }
    }
    // Enrollment accepts only the same original Bind query, before effects.
    let unsupported = controls
        .prepare(PreparedCommand::new(&AgentCommand::Root {}).map_err(exchange)?)
        .map_err(exchange)?;
    assert_eq!(
        enrolled
            .actor_call_enrolling(
                &unsupported.invocations()[0],
                0,
                2,
                acyclic_stream::wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES,
            )
            .await
            .err(),
        Some(StreamError::Unsupported)
    );
    eprintln!("original selected observation enrolled");
    // The same real session without an in-process owner has no local cause to add.
    let mut remote = connection.clone();
    remote._host = None;
    let mut remote_observer = Observer::open(&remote, 0, 0)?;
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
    eprintln!("original storage failure published");
    let cause = stream
        .failure()
        .ok_or_else(|| Error::Invalid("original failure was lost".into()))?
        .to_string();
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
    eprintln!("original local observation ended");
    assert!(
        observed.contains(&cause),
        "observation lost original provider cause: {observed}"
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
    eprintln!("original remote command ended");
    assert!(!message(terminal(&mut remote_observer).await).contains(&cause));
    eprintln!("original remote observation ended");
    assert_eq!(closed.await, StreamError::Unavailable);
    eprintln!("original failed runtime custody ended");
    assert_eq!(custody.check(), Err(StreamError::Unavailable));
    loop {
        if let Err(error) = selected.next().await {
            assert_eq!(error.code(), StreamError::Unavailable.code());
            break;
        }
    }
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

    // Dropping an otherwise healthy original host must also terminate its
    // retained policy owner. The guard itself cannot keep that producer alive.
    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let healthy =
        Arc::new(LocalHarness::open(volume_config(directory.path(), project, text())?).await?);
    let connection = healthy.connect().await?;
    eprintln!("original healthy endpoint connected");
    let custody = healthy
        .agents
        .get()
        .expect("original hosted runtime")
        .runtime
        .custody();
    let closed = custody.terminated();
    futures::pin_mut!(closed);
    assert!(custody.check().is_ok());
    assert!(futures::poll!(closed.as_mut()).is_pending());
    let owner = Arc::downgrade(&healthy);
    drop(connection);
    drop(healthy);
    assert!(owner.upgrade().is_none(), "custody retained its producer");
    assert_eq!(closed.await, StreamError::Unavailable);
    assert_eq!(custody.check(), Err(StreamError::Unavailable));
    Ok(())
}
