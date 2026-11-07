use super::*;
use crate::local::{
    Event, Observer,
    tests::{text, volume, volume_config},
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, PersistenceCut, StreamError, StreamPath, StreamProviderExt,
};
use std::sync::atomic::{AtomicBool, Ordering};

/// The published credential is exercised over the ordinary server transport,
/// including direct primitive reads rather than only actor command replies.
#[cfg(feature = "serve")]
#[tokio::test]
async fn remote_tree_observation_pages_descendants_and_refuses_unrelated_roots() -> Result<()> {
    use crate::agent::AgentRecord as _;
    use acyclic_harness_client::ReadTarget;
    use acyclic_stream::{ChildrenPageRequest, FollowRequest};
    use futures::StreamExt as _;

    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let provider = crate::local::tests::scripted([
        crate::test_support::ScriptedTurn::Text("child output".into()),
        crate::test_support::ScriptedTurn::Text("grandchild output".into()),
    ]);
    // The script publishes two text turns; it has no summary-model requests.
    let config = volume_config(directory.path(), project, provider)?
        .compaction(crate::context::CompactionPolicy::Disabled);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(exchange)?;
    let (ready, endpoint) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut serving = tokio::spawn(crate::server::serve(
        listener,
        crate::server::ServerConfig::new(config),
        move |endpoint| {
            let credential = endpoint
                .observation
                .as_ref()
                .map(|observation| observation.credential.clone());
            let _ = ready.send((endpoint.endpoint.clone(), credential));
        },
        async move {
            let _ = stopped.await;
        },
    ));
    let checked = async {
        let (endpoint, credential) = endpoint.await.map_err(exchange)?;
        let credential = credential
            .ok_or_else(|| Error::Unauthorized("server published no credential".into()))?;
        let remote = acyclic_stream::grpc::Client::connect_loopback(&endpoint, &credential)
            .await
            .map_err(exchange)?;
        let session = remote.session_client().map_err(exchange)?;
        let connection = Connection::bind(
            session.clone(),
            acyclic_auth::credential::capability_from_text(&credential).map_err(exchange)?,
            None,
        )
        .await?;
        let root = connection.root().clone();
        let mut parent = root.clone();
        for intent in ["child", "grandchild"] {
            let AgentReply::Spawned { agent: child, .. } = connection
                .execute(&AgentCommand::Spawn {
                    parent: parent.clone(),
                    id: crate::agent::IntentId::try_from(intent).map_err(exchange)?,
                    brief: crate::model::ModelContent::Text("answer".into()),
                    role: "worker".into(),
                })
                .await?
            else {
                return Err(Error::Conflict("spawn returned no child".into()));
            };
            let page = session
                .children_page(
                    ChildrenPageRequest {
                        parent: Some(parent.children().map_err(exchange)?),
                        after: None,
                        hierarchy_version: None,
                        limit: 1,
                        tail_stream: None,
                    },
                    CREDENTIAL,
                )
                .await
                .map_err(exchange)?;
            assert_eq!(page.children.len(), 1);
            assert_eq!(page.children[0].path, child.inbox().map_err(exchange)?);
            let selected = connection.select(child.clone()).await?;
            assert_eq!(selected.root(), &child);
            let observation = selected.controls.observation_credential();
            assert_ne!(observation, CREDENTIAL);
            // The new read-only grant cannot become a command identity.
            let observer_commands = AgentControls::new(
                session.clone(),
                observation,
                acyclic_harness_client::AgentBinding::new(&selected.controls.binding_bytes())
                    .map_err(exchange)?,
            );
            let root_request = observer_commands
                .prepare(PreparedCommand::new(&AgentCommand::Root {}).map_err(exchange)?)
                .map_err(exchange)?;
            assert!(matches!(
                observer_commands.execute(&root_request).await,
                Err(AgentExchangeError::Session(StreamError::AccessDenied))
            ));
            // A selected frontend follows the child's private tool-output
            // resource as well as its conversation and status streams.
            let _changes = session
                .subscribe(acyclic_stream::wire::SessionSubscription {
                    credential: selected.controls.observation_credential(),
                    resume: Some(acyclic_stream::wire::SessionCursor::default()),
                    kind: Some(acyclic_stream::wire::session_subscription::Kind::Follow(
                        acyclic_stream::wire::SessionFollow {
                            resource: selected.tools.clone(),
                            delivery: acyclic_stream::wire::SessionDelivery::All as i32,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                })
                .await
                .map_err(exchange)?;
            // Adoption publishes the first status; the turn's own status
            // event announces its committed conversation entry.
            let mut status = acyclic_stream::records_only(
                session
                    .follow(FollowRequest::new(child.status().map_err(exchange)?, 0))
                    .await
                    .map_err(exchange)?,
            );
            loop {
                let record = status
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("child status ended".into()))?
                    .map_err(exchange)?;
                crate::agent::codec_fuzz::check(&record.value);
                let status = crate::agent::StatusRecord::decode(&record.value).map_err(exchange)?;
                if status.conversation > 0 {
                    break;
                }
                if let crate::agent::Phase::Done { outcome } = status.phase {
                    return Err(Error::Conflict(format!(
                        "child ended before publishing its conversation: {outcome:?}"
                    )));
                }
            }
            // The entry and its immutable output are read through the same
            // public surfaces that retain native transcript references.
            let mut conversation = acyclic_stream::records_only(
                session
                    .follow(FollowRequest::new(
                        child.conversation().map_err(exchange)?,
                        0,
                    ))
                    .await
                    .map_err(exchange)?,
            );
            let record = conversation
                .next()
                .await
                .ok_or_else(|| Error::Conflict("child conversation ended".into()))?
                .map_err(exchange)?;
            crate::agent::codec_fuzz::check(&record.value);
            let entry = crate::agent::ConversationEntry::decode(&record.value).map_err(exchange)?;
            assert!(matches!(
                selected
                    .execute(&AgentCommand::Read {
                        agent: child.clone(),
                        target: ReadTarget::File { file: entry.output },
                        offset: 0,
                    })
                    .await?,
                AgentReply::Content { .. }
            ));
            parent = child;
        }
        let foreign = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([255; 16]));
        assert!(matches!(
            session
                .follow(FollowRequest::new(foreign.status().map_err(exchange)?, 0))
                .await,
            Err(StreamError::AccessDenied)
        ));
        assert!(matches!(
            session
                .children_page(
                    ChildrenPageRequest {
                        parent: Some(foreign.children().map_err(exchange)?),
                        after: None,
                        hierarchy_version: None,
                        limit: 1,
                        tail_stream: None,
                    },
                    CREDENTIAL
                )
                .await,
            Err(StreamError::AccessDenied)
        ));
        assert!(connection.select(foreign).await.is_err());
        Ok(())
    };
    let checked = tokio::select! {
        result = &mut serving => {
            result.map_err(exchange)?.map_err(exchange)?;
            return Err(Error::Conflict("server stopped before tree observation completed".into()));
        },
        checked = checked => checked,
    };
    let _ = stop.send(());
    serving.await.map_err(exchange)??;
    checked
}

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
