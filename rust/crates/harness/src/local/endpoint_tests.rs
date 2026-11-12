use super::*;
use crate::local::{
    Event, Observer,
    tests::{text, volume, volume_config},
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, PersistenceCut, StreamError, StreamPath, StreamProviderExt,
};
use std::sync::atomic::{AtomicBool, Ordering};

/// A tree-only credential is exercised over the ordinary server transport,
/// including direct primitive reads rather than only actor command replies.
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
        .compaction(crate::context::CompactionPolicy::Disabled)
        .served(Arc::new(crate::server::KeyFileVerifier));
    let local = Arc::new(LocalHarness::open(config).await?);
    local.agents().await?;
    let owner = local.connect_sessions().await?;
    let foreign = super::super::sessions::create(&owner, "unrelated-root", None).await?;
    let mut foreign_events = Observer::open(&foreign, 0, 0)?;
    loop {
        if matches!(foreign_events.next().await?, Event::Status(status) if matches!(status.phase, crate::agent::Phase::Idle))
        {
            break;
        }
    }
    let foreign = foreign.root().clone();
    let hosted = local
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("missing owner".into()))?;
    let server = Arc::clone(hosted.server());
    let credential = server
        .bound()
        .client_credential(server.root(), server.resource(), local.clock.clone())
        .await
        .map_err(exchange)?;
    let credential = acyclic_auth::credential::capability_text(&credential.credential);
    let changes = server.bound().changes().map_err(exchange)?;
    let composition = crate::server::Hosted::new(
        server,
        local.clock.clone(),
        local.clone(),
        Some(Arc::new(changes)),
    )
    .with_completion(hosted.runtime.completion());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(exchange)?;
    let (ready, endpoint) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut serving = tokio::spawn(crate::server::serve(
        listener,
        crate::server::ServerConfig::new(composition),
        move |endpoint| {
            let _ = ready.send((endpoint.endpoint.clone(), credential));
        },
        async move {
            let _ = stopped.await;
        },
    ));
    let checked = async {
        let (endpoint, credential) = endpoint.await.map_err(exchange)?;
        let remote = acyclic_stream::grpc::Client::connect_loopback(&endpoint, &credential)
            .await
            .map_err(exchange)?;
        let session = remote.session_client().map_err(exchange)?;
        let connection = Connection::bind(
            session.clone(),
            acyclic_auth::credential::capability_from_text(&credential).map_err(exchange)?,
            None,
            false,
        )
        .await?;
        let root = connection.root().clone();
        eprintln!("remote tree: authenticated {root}");
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
                        offset: 0,
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
            eprintln!("remote tree: selected {intent} {child}");
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
            for all in [false, true] {
                eprintln!("remote tree: observing {intent}, all={all}");
                let selection = acyclic_harness_client::AgentSelection::new(child.clone(), 0, 0);
                let mut observer = if all {
                    selected.controls.observe_all(selection)
                } else {
                    selected.controls.observe(selection)
                }
                .map_err(exchange)?;
                let controls = observer.controls(
                    acyclic_harness_client::AgentBinding::new(&selected.controls.binding_bytes())
                        .map_err(exchange)?,
                );
                assert_eq!(controls.observation_credential(), observation);
                let request = controls
                    .prepare(PreparedCommand::new(&AgentCommand::Root {}).map_err(exchange)?)
                    .map_err(exchange)?;
                assert!(matches!(
                    controls.execute(&request).await.map_err(exchange)?,
                    AgentReply::Root { .. }
                ));
                let mut status_seen = false;
                let mut turn_seen = false;
                while !status_seen || !turn_seen {
                    let event = observer.next().await.map_err(exchange)?;
                    eprintln!("remote tree: {intent}, all={all}: {event:?}");
                    match event {
                        acyclic_harness_client::AgentEvent::Status { agent, .. } => {
                            assert_eq!(agent, child);
                            status_seen = true;
                        }
                        acyclic_harness_client::AgentEvent::Conversation { agent, .. } => {
                            assert_eq!(agent, child);
                            turn_seen = true;
                        }
                        _ => {}
                    }
                }
            }
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
            eprintln!("remote tree: verified {intent}");
        }
        assert_eq!(
            session
                .follow(FollowRequest::new(foreign.status().map_err(exchange)?, 0))
                .await
                .err()
                .map(|error| error.category()),
            Some(StreamError::AccessDenied)
        );
        assert_eq!(
            session
                .children_page(
                    ChildrenPageRequest {
                        parent: Some(foreign.children().map_err(exchange)?),
                        offset: 0,
                        after: None,
                        hierarchy_version: None,
                        limit: 1,
                        tail_stream: None,
                    },
                    CREDENTIAL
                )
                .await
                .err()
                .map(|error| error.category()),
            Some(StreamError::AccessDenied)
        );
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
    eprintln!("remote tree: stopping server, result={checked:?}");
    serving.await.map_err(exchange)??;
    checked
}

struct Cut(AtomicBool);

#[tokio::test]
async fn selected_session_delivers_peer_input_back_to_directory_root() -> Result<()> {
    use crate::local::agent_conversation::{Conversation, Sent, Settlement};
    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .workspace_mounting(crate::local::WorkspaceMounting::Physical),
        )
        .await?,
    );
    let connection = harness.connect_sessions().await?;
    let selected = super::super::sessions::create(&connection, "selected-sender", None).await?;
    let sender = selected.root().clone();
    let sending = Conversation::open(selected);
    let receiving = Conversation::open(connection.clone());
    let mut events = Observer::open(&connection, 0, 0)?;
    let prepared = sending
        .prepare_peer_message(
            connection.root().clone(),
            "selected-message",
            "Please report your status.",
        )
        .await?;
    let Sent::Accepted(inbox) = sending.deliver_peer_message(&prepared).await? else {
        return Err(Error::Invalid(
            "selected peer delivery was not accepted".into(),
        ));
    };
    assert!(matches!(
        receiving
            .settle("selected-message", inbox, &mut events)
            .await?,
        Settlement::Reply(crate::agent::Reply::Answered { .. })
    ));
    assert_eq!(
        sending.deliver_peer_message(&prepared).await?,
        Sent::Accepted(inbox)
    );
    assert!(provider.requests().iter().flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts()).any(|part|
            matches!(part, crate::model::ModelContentPart::Notices { notices }
                if notices.iter().any(|notice| notice.kind == "peer.message" && notice.source == sender.as_str()))));
    Ok(())
}

#[tokio::test]
async fn local_tree_reader_uses_only_its_enrolled_stream_scope() -> Result<()> {
    let directory = tempfile::tempdir().map_err(exchange)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
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
    // A real agent authors the same private files the visible-row query reads.
    // Neither native mounting nor a project directory named .volumes is needed.
    let selected = conversation.select(child.clone()).await?;
    selected.wait_current_input().await?;
    use crate::{
        model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent},
        test_support::{ScriptedEvent, ScriptedTurn},
    };
    let calls = [
        ("ui-title", "volume:///private/ui/title", "Authored title"),
        (
            "managed-output",
            "volume:///private/tools/forged.json",
            "forged",
        ),
        (
            "escape-output",
            "volume:///private/ui/../tools/forged.json",
            "forged",
        ),
    ]
    .into_iter()
    .map(|(id, path, content)| {
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            "write".into(),
            serde_json::json!({"path":path,"content":content}),
        )))
    })
    .chain(std::iter::once(ScriptedEvent::Event(
        ModelEvent::Completed {
            metadata: serde_json::json!({}),
        },
    )))
    .collect();
    provider.extend([
        ScriptedTurn::Events(calls),
        ScriptedTurn::Text("ui authored".into()),
    ]);
    let before = selected
        .agent_file_previews(vec!["ui/title".into()], None, 64)
        .await?;
    assert_eq!(before.files["ui/title"], None);
    let mut events = selected.observe(None).await?;
    let publish = async {
        let inbox = selected.send("author-ui", "author the private UI").await?;
        selected.settle("author-ui", inbox, &mut events).await?;
        Result::Ok(())
    };
    let (_changed, ()) = futures::try_join!(
        selected.agent_file_previews(vec!["ui/title".into()], Some(before.generation), 64),
        publish,
    )?;
    // The private producer also carries journal publications. The original
    // input's settlement, not its first volume event, certifies this write.
    let changed = selected
        .agent_file_previews(vec!["ui/title".into()], None, 64)
        .await?;
    let request = provider
        .last_request()
        .ok_or_else(|| Error::Conflict("no tool result request".into()))?;
    for part in request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
    {
        if let ModelContentPart::ToolResult {
            call_id, content, ..
        } = part
            && call_id == "ui-title"
        {
            match content {
                ToolResultContent::Error { message } => {
                    eprintln!("private UI write refused: {message}")
                }
                ToolResultContent::Json { value } => eprintln!(
                    "private UI result volume: {:?}",
                    value
                        .pointer("/file/volume/class")
                        .and_then(serde_json::Value::as_str)
                ),
                ToolResultContent::Parts { .. } => {}
            }
        }
    }
    assert_eq!(changed.files["ui/title"].as_deref(), Some("Authored title"));
    for id in ["managed-output", "escape-output"] {
        assert!(request.messages.iter().flat_map(|message| message.content.parts()).any(|part|
            matches!(part, ModelContentPart::ToolResult {call_id, content: ToolResultContent::Error {..}, ..} if call_id == id)),
            "private managed output accepted {id}");
    }
    eprintln!("tree reader: authored private UI followed; managed outputs refused");
    let outside = AgentPath::root(&crate::agent::AgentId::from_bytes([0; 16]));
    assert_eq!(
        reader.head(outside.status().map_err(exchange)?).await,
        Err(StreamError::AccessDenied)
    );
    let directory_connection = harness.connect_sessions().await?;
    let session =
        super::super::sessions::create(&directory_connection, "reader-session", None).await?;
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
