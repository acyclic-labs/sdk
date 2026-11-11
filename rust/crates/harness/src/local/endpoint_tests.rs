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

#[tokio::test]
async fn local_tree_reader_uses_only_its_enrolled_stream_scope() -> Result<()> {
    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, text())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .workspace_mounting(crate::local::WorkspaceMounting::Physical),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let mut observer = Observer::open(&connection, 0, 0)?;
    loop {
        if matches!(observer.next().await?, Event::Status(status) if matches!(status.phase, crate::agent::Phase::Idle))
        {
            break;
        }
    }
    let reader = connection.session.read_provider();
    let head = reader
        .head(connection.root.status().map_err(exchange)?)
        .await;
    let page = reader
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(connection.root.children().map_err(exchange)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(exchange)?),
        })
        .await;
    assert!(
        head.is_ok() && page.is_ok(),
        "head: {head:?}; page: {page:?}"
    );
    assert!(page.map_err(exchange)?.children.is_empty());
    let conversation = super::super::agent_conversation::Conversation::open(connection.clone());
    let mut tree = conversation.tree()?;
    tree.view.set_viewport(1, 0, 16);
    loop {
        tree.view.frame(0);
        if tree
            .view
            .reader_ref()
            .cache()
            .listing(&connection.root.children().map_err(exchange)?, 0)
            .is_some_and(|listing| listing.fresh && listing.children.is_empty())
        {
            break;
        }
        futures::future::poll_fn(|cx| tree.poll_changed(0, cx)).await;
    }
    eprintln!("tree reader: original empty page observed");
    let child = match connection
        .execute(&acyclic_harness_client::AgentCommand::Spawn {
            parent: connection.root.clone(),
            id: crate::agent::IntentId::try_from("reader-child").map_err(exchange)?,
            role: "worker".into(),
            brief: crate::model::ModelContent::Text("exercise the enrolled child listing".into()),
        })
        .await
        .map_err(|error| Error::Conflict(format!("spawn child: {error}")))?
    {
        AgentReply::Spawned { agent, .. } => agent.clone(),
        _ => return Err(Error::Conflict("spawn returned no child".into())),
    };
    eprintln!("tree reader: child spawn committed");
    let populated = reader
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(connection.root.children().map_err(exchange)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(exchange)?),
        })
        .await
        .map_err(exchange)?;
    assert_eq!(populated.children.len(), 1, "{populated:?}");
    assert_eq!(populated.children[0].path.as_str(), child.as_str());
    eprintln!("tree reader: populated scoped page observed");
    loop {
        tree.view.frame(0);
        if tree.view.rows().iter().any(|(_, row)| {
            matches!(row,
            acyclic_harness_client::explorer::Row::Agent { path, .. } if path == &child)
        }) {
            break;
        }
        if let Some((path, issue)) = tree.view.reader_ref().issues().next() {
            return Err(Error::Conflict(format!("tree reader {path}: {issue:?}")));
        }
        futures::future::poll_fn(|cx| tree.poll_changed(0, cx)).await;
    }
    eprintln!("tree reader: first child discovered through the live source");
    let outside = AgentPath::root(&crate::agent::AgentId::from_bytes([0; 16]));
    assert_eq!(
        reader.head(outside.status().map_err(exchange)?).await,
        Err(StreamError::AccessDenied)
    );
    let directory_connection = harness.connect_sessions().await?;
    let session = super::super::sessions::create(&directory_connection, "reader-session").await?;
    let page = session
        .session
        .read_provider()
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(session.root.children().map_err(exchange)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(exchange)?),
        })
        .await
        .map_err(exchange)?;
    assert!(page.children.is_empty());
    assert_eq!(
        reader.head(session.root.status().map_err(exchange)?).await,
        Err(StreamError::AccessDenied)
    );
    // The primitive filters broad discovery to authorized children. A directory
    // owner can discover both roots; this original tree credential sees only its own.
    let page = reader
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(StreamPath::new(crate::agent::AGENTS).map_err(exchange)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: None,
        })
        .await
        .map_err(exchange)?;
    assert_eq!(page.children.len(), 1);
    assert_eq!(page.children[0].path.as_str(), connection.root.as_str());
    assert!(page.next_after.is_none());
    Ok(())
}
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
        after: Some(snapshot.generation.clone()),
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
    let cut = Arc::new(Cut(AtomicBool::new(false)));
    stream
        .cut_persistence(cut.clone())
        .await
        .map_err(exchange)?;
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
