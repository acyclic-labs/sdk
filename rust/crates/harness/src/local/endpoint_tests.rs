use super::*;
use crate::local::{
    Event, Observer, storage,
    tests::{text, volume, volume_config},
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, PersistenceCut, StreamError, StreamPath, StreamProvider,
    StreamProviderExt,
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

    let directory = tempfile::tempdir().map_err(storage)?;
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
    let directory_credential = server
        .bound()
        .session_credential(
            server.root(),
            server.resource(),
            &server.session_resource(),
            local.clock.clone(),
        )
        .await
        .map_err(|error| {
            Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
        })?;
    let directory_credential =
        acyclic_auth::credential::capability_text(&directory_credential.credential);
    let credential = server
        .bound()
        .client_credential(server.root(), server.resource(), local.clock.clone())
        .await
        .map_err(storage)?;
    let credential = acyclic_auth::credential::capability_text(&credential.credential);
    let changes = server.bound().changes().map_err(storage)?;
    let composition = crate::server::Hosted::new(
        server,
        local.clock.clone(),
        local.clone(),
        Some(Arc::new(changes)),
    )
    .with_completion(hosted.runtime.completion());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(storage)?;
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
        let (endpoint, credential) = endpoint.await.map_err(storage)?;
        let remote = acyclic_stream::grpc::Client::connect_loopback(&endpoint, &credential)
            .await
            .map_err(storage)?;
        let session = remote.session_client().map_err(storage)?;
        let connection = Connection::bind(
            session.clone(),
            acyclic_auth::credential::capability_from_text(&credential).map_err(storage)?,
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
                    id: crate::agent::IntentId::try_from(intent).map_err(storage)?,
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
                        parent: Some(parent.children().map_err(storage)?),
                        after: None,
                        hierarchy_version: None,
                        limit: 1,
                        tail_stream: None,
                    },
                    CREDENTIAL,
                )
                .await
                .map_err(storage)?;
            assert_eq!(page.children.len(), 1);
            assert_eq!(page.children[0].path, child.inbox().map_err(storage)?);
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
                    .map_err(storage)?,
            );
            let root_request = observer_commands
                .prepare(PreparedCommand::new(&AgentCommand::Root {}).map_err(storage)?)
                .map_err(storage)?;
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
                .map_err(storage)?;
                let controls = observer.controls(
                    acyclic_harness_client::AgentBinding::new(&selected.controls.binding_bytes())
                        .map_err(storage)?,
                );
                assert_eq!(controls.observation_credential(), observation);
                let request = controls
                    .prepare(PreparedCommand::new(&AgentCommand::Root {}).map_err(storage)?)
                    .map_err(storage)?;
                assert!(matches!(
                    controls.execute(&request).await.map_err(storage)?,
                    AgentReply::Root { .. }
                ));
                let mut status_seen = false;
                let mut turn_seen = false;
                while !status_seen || !turn_seen {
                    let event = observer.next().await.map_err(storage)?;
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
                .map_err(storage)?;
            // Prompt recall follows the selected agent's accepted input with
            // the same read-only credential, without actor invocation rights.
            let mut inputs = session
                .subscribe(acyclic_stream::wire::SessionSubscription {
                    credential: selected.controls.observation_credential(),
                    resume: Some(acyclic_stream::wire::SessionCursor::default()),
                    kind: Some(acyclic_stream::wire::session_subscription::Kind::Follow(
                        acyclic_stream::wire::SessionFollow {
                            resource: acyclic_stream::wire_codec::session::stream_resource(
                                child.inbox().map_err(storage)?.as_str(),
                            ),
                            delivery: acyclic_stream::wire::SessionDelivery::All as i32,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                })
                .await
                .map_err(storage)?;
            let frame = inputs
                .next()
                .await
                .ok_or_else(|| Error::Conflict("accepted input follow ended".into()))?
                .map_err(storage)?;
            assert!(matches!(&frame.wire().body,
                Some(acyclic_stream::wire::session_frame::Body::Data(data))
                if matches!(&data.item, Some(acyclic_stream::wire::session_data::Item::Record(_)))));

            // Adoption publishes the first status; the turn's own status
            // event announces its committed conversation entry.
            let mut status = acyclic_stream::records_only(
                session
                    .follow(FollowRequest::new(child.status().map_err(storage)?, 0))
                    .await
                    .map_err(storage)?,
            );
            loop {
                let record = status
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("child status ended".into()))?
                    .map_err(storage)?;
                crate::agent::codec_fuzz::check(&record.value);
                let status = crate::agent::StatusRecord::decode(&record.value).map_err(storage)?;
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
                        child.conversation().map_err(storage)?,
                        0,
                    ))
                    .await
                    .map_err(storage)?,
            );
            let record = conversation
                .next()
                .await
                .ok_or_else(|| Error::Conflict("child conversation ended".into()))?
                .map_err(storage)?;
            crate::agent::codec_fuzz::check(&record.value);
            let entry = crate::agent::ConversationEntry::decode(&record.value).map_err(storage)?;
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
                .follow(FollowRequest::new(foreign.status().map_err(storage)?, 0))
                .await
                .err()
                .map(|error| error.category()),
            Some(StreamError::AccessDenied)
        );
        assert_eq!(
            session
                .children_page(
                    ChildrenPageRequest {
                        parent: Some(foreign.children().map_err(storage)?),
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
        assert!(connection.select(foreign.clone()).await.is_err());
        // The same transport constructor must honor an authenticated directory
        // owner even though its local attachment mode is not a directory.
        let options = acyclic_harness_client::ConnectionOptions::from_env(
            Some(&endpoint),
            Some(&directory_credential),
        )
        .map_err(|error| {
            Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
        })?;
        let directory = Connection::connect(&options).await?;
        assert!(!directory.directory);
        assert_eq!(directory.select(foreign.clone()).await?.root(), &foreign);
        Ok(())
    };
    let checked = tokio::select! {
        result = &mut serving => {
            result.map_err(storage)?.map_err(storage)?;
            return Err(Error::Conflict("server stopped before tree observation completed".into()));
        },
        checked = checked => checked,
    };
    let _ = stop.send(());
    eprintln!("remote tree: stopping server, result={checked:?}");
    serving.await.map_err(storage)??;
    checked
}

struct Cut(AtomicBool);

#[tokio::test]
async fn selected_session_delivers_peer_input_back_to_directory_root() -> Result<()> {
    use crate::local::agent_conversation::{Conversation, Sent, Settlement};
    let directory = tempfile::tempdir().map_err(storage)?;
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
    let directory = tempfile::tempdir().map_err(storage)?;
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
        .head(connection.root.status().map_err(storage)?)
        .await;
    let page = reader
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(connection.root.children().map_err(storage)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(storage)?),
        })
        .await;
    assert!(
        head.is_ok() && page.is_ok(),
        "head: {head:?}; page: {page:?}"
    );
    assert!(page.map_err(storage)?.children.is_empty());
    let conversation = super::super::agent_conversation::Conversation::open(connection.clone());
    let mut tree = conversation.tree()?;
    tree.view.set_viewport(1, 0, 16);
    loop {
        tree.view.frame(0);
        if tree
            .view
            .reader_ref()
            .cache()
            .listing(&connection.root.children().map_err(storage)?, 0)
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
            id: crate::agent::IntentId::try_from("reader-child").map_err(storage)?,
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
            parent: Some(connection.root.children().map_err(storage)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(storage)?),
        })
        .await
        .map_err(storage)?;
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
        reader.head(outside.status().map_err(storage)?).await,
        Err(StreamError::AccessDenied)
    );
    let directory_connection = harness.connect_sessions().await?;
    let session =
        super::super::sessions::create(&directory_connection, "reader-session", None).await?;
    let page = session
        .session
        .read_provider()
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(session.root.children().map_err(storage)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: Some(StreamPath::new("conversation").map_err(storage)?),
        })
        .await
        .map_err(storage)?;
    assert!(page.children.is_empty());
    assert_eq!(
        reader.head(session.root.status().map_err(storage)?).await,
        Err(StreamError::AccessDenied)
    );
    // The primitive filters broad discovery to authorized children. A directory
    // owner can discover both roots; this original tree credential sees only its own.
    let page = reader
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(StreamPath::new(crate::agent::AGENTS).map_err(storage)?),
            after: None,
            hierarchy_version: None,
            limit: 16,
            tail_stream: None,
        })
        .await
        .map_err(storage)?;
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

/// Fixture plumbing failures, outside the typed exchange under test.
fn exchange(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

/// The full cause chain of a typed exchange failure, which stays storage.
fn message(error: Error) -> String {
    assert_eq!(error.code(), crate::wire::ErrorCode::Storage, "{error:?}");
    let Error::Exchange { .. } = &error else {
        panic!("expected a typed exchange failure, got {error:?}");
    };
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        message = format!("{message}: {cause}");
        source = cause.source();
    }
    message
}

fn provider_message(error: Error, expected: acyclic_stream::LocalStreamFailure) -> String {
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        if let Some(retained) = cause.downcast_ref::<acyclic_stream::LocalStreamFailure>() {
            assert_eq!(*retained, expected);
            return message(error);
        }
        source = cause.source();
    }
    panic!("exchange lost the typed provider cause: {error:?}");
}

#[tokio::test]
async fn local_commands_and_observation_report_the_original_provider_failure() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?,
    );
    let connection = harness.connect().await?;
    let mut observer = Observer::open(&connection, 0, 0)?;
    let mut tree = connection
        .follow_tree(StreamPath::new("supervision").map_err(storage)?)
        .await?;
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
            root: crate::client_files::FileRoot::ClientSession { session },
            publication: None,
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
        root: crate::client_files::FileRoot::ClientSession { session },
        publication: None,
        paths: vec!["layout.json".into()],
        after: Some(snapshot.cut.clone()),
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
        Some(&snapshot.cut),
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
    stream.cut_persistence(cut.clone()).await.map_err(storage)?;
    assert_eq!(
        stream
            .append(AppendRequest {
                path: StreamPath::new("failure/report").map_err(storage)?,
                records: vec![bytes::Bytes::from_static(b"unaccepted").into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await,
        Err(StreamError::Unavailable)
    );
    assert!(cut.0.load(Ordering::SeqCst));
    let original = stream
        .failure()
        .ok_or_else(|| Error::Invalid("original failure was lost".into()))?;
    let cause = original.to_string();
    // Use the serving codec on the real provider failure, then the exact
    // command projection used by execute; neither delivery order is guessed.
    let host_result = host_files
        .await
        .map(|snapshot| AgentReply::ClientFiles { snapshot });
    let encoded =
        acyclic_harness_client::encode_agent_reply(&host_result, &files).map_err(storage)?;
    let refusal = acyclic_harness_client::decode_agent_reply(&encoded, &files)
        .map_err(storage)?
        .expect_err("failed host query returned client files");
    let refusal = AgentExchangeError::Control(refusal);
    assert_eq!(refusal.code(), "unavailable");
    let refused = provider_message(connection.command_failure(&files, refusal), original);
    assert!(
        refused.contains(&cause),
        "host refusal lost original provider cause: {refused}"
    );
    let closed = provider_message(
        connection.command_failure(
            &files,
            AgentExchangeError::Session(StreamError::Unavailable),
        ),
        original,
    );
    assert!(
        closed.contains(&cause),
        "session close lost cause: {closed}"
    );
    let files_error = provider_message(
        waiting_files
            .await
            .err()
            .ok_or_else(|| Error::Invalid("closed producer answered client files".into()))?,
        original,
    );
    assert!(
        files_error.contains(&cause),
        "client files lost original provider cause: {files_error}"
    );
    let command = provider_message(
        connection
            .execute(&AgentCommand::Root {})
            .await
            .err()
            .ok_or_else(|| Error::Invalid("failed provider answered command".into()))?,
        original,
    );
    assert!(
        command.contains(&cause),
        "command lost original provider cause: {command}"
    );
    let tree_failure = loop {
        match tree.next().await {
            Ok(_) => continue,
            Err(error) => break provider_message(error, original),
        }
    };
    assert!(
        tree_failure.contains(&cause),
        "tree feed lost original provider cause: {tree_failure}"
    );
    let observed = provider_message(terminal(&mut observer).await, original);
    assert!(
        observed.contains(&cause),
        "observation lost original provider cause: {observed}"
    );
    let projected = loop {
        match projected.next_projected().await {
            Err(error) | Ok((Err(error), _)) => break provider_message(error, original),
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
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn agent_file_roots_share_conditional_publication_and_follow_over_remote_sessions()
-> Result<()> {
    use crate::{
        client_files::{ClientFileChange, FileRoot},
        local::agent_conversation::Conversation,
        server::{Hosted, ServerConfig},
        supervision::Lifecycle,
    };
    let directory = tempfile::tempdir().map_err(|error| {
        Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
    })?;
    let project = volume(directory.path()).await?;
    let provider = text();
    // The model only answers; file and spawn operations use the normal client.
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?.without_default_tools(),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let local = Conversation::open(connection.clone());
    let mut observer = local.observe(None).await?;
    loop {
        if matches!(observer.next().await?, Event::Status(status) if matches!(status.phase, crate::agent::Phase::Idle))
        {
            break;
        }
    }
    let hosted = harness
        .agents
        .get()
        .ok_or_else(|| Error::Conflict("test host missing".into()))?;
    let changes = hosted.server().bound().changes().map_err(|error| {
        Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
    })?;
    let composition = Hosted::new(
        hosted.server().clone(),
        harness.clock.clone(),
        harness.clone(),
        Some(Arc::new(changes)),
    )
    .with_completion(hosted.runtime.completion());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| {
            Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
        })?;
    let (ready, announced) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(crate::server::serve(
        listener,
        ServerConfig::new(composition),
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
    let (url, credential) = announced.await.map_err(|error| {
        Error::exchange(crate::ExchangeStep::Step("await remote endpoint"), error)
    })?;
    let credential = credential
        .ok_or_else(|| Error::Conflict("server did not publish a client credential".into()))?;
    let options =
        acyclic_harness_client::ConnectionOptions::from_env(Some(&url), Some(&credential))
            .map_err(|error| {
                Error::exchange(
                    crate::ExchangeStep::Step("configure remote endpoint"),
                    error,
                )
            })?;
    let remote_connection = super::Connection::connect(&options)
        .await
        .map_err(|error| Error::exchange(crate::ExchangeStep::Step("remote admission"), error))?;
    let remote = Conversation::open(remote_connection.clone());
    assert_eq!(local.task(), remote.task());
    let paths = vec!["control/pause".to_owned(), "notes/steer.md".to_owned()];
    let mut sources = Vec::new();
    for root in [FileRoot::Private {}, FileRoot::Home {}] {
        let initial = remote
            .files(root, paths.clone(), None)
            .await
            .map_err(|error| {
                Error::exchange(
                    crate::ExchangeStep::Step("read remote initial files"),
                    error,
                )
            })?;
        assert!(initial.files.values().all(Option::is_none));
        let operation = crate::OperationId::new();
        let changes = vec![
            ClientFileChange {
                path: paths[0].clone(),
                expected: None,
                text: Some("pause".into()),
            },
            ClientFileChange {
                path: paths[1].clone(),
                expected: None,
                text: Some("Investigate the failure.\n".into()),
            },
        ];
        let (published, generation) = futures::try_join!(
            local.files(root, paths.clone(), Some(initial.cut)),
            remote.publish_files(root, operation, changes.clone()),
        )?;
        assert_eq!(published.cut.generation, generation);
        assert_eq!(
            published.volume,
            published.files[&paths[0]]
                .as_ref()
                .unwrap()
                .original()
                .unwrap()
                .volume()
                .clone()
        );
        assert_eq!(
            local.publish_files(root, operation, changes).await?,
            generation,
            "exact replay changed the publication cut"
        );
        let pause = published.files[&paths[0]]
            .as_ref()
            .unwrap()
            .original()
            .unwrap()
            .clone();
        let steer = published.files[&paths[1]]
            .as_ref()
            .unwrap()
            .original()
            .unwrap()
            .clone();
        let appended = "Investigate the failure.\nThen run the focused check.\n";
        remote
            .publish_files(
                root,
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: paths[1].clone(),
                    expected: Some(steer.clone()),
                    text: Some(appended.into()),
                }],
            )
            .await?;
        assert!(
            remote
                .publish_files(
                    root,
                    crate::OperationId::new(),
                    vec![ClientFileChange {
                        path: paths[1].clone(),
                        expected: Some(steer),
                        text: Some("stale replacement".into()),
                    }]
                )
                .await
                .is_err(),
            "stale source overwrote an appended delta"
        );
        let read = local.files(root, paths.clone(), None).await?;
        assert_eq!(read.files[&paths[1]].as_ref().unwrap().text, appended);
        sources.push(pause);
    }
    // A source in another bound volume cannot authorize an otherwise identical path.
    assert!(
        remote
            .publish_files(
                FileRoot::Private {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: paths[0].clone(),
                    expected: sources.get(1).cloned(),
                    text: Some("wrong root".into()),
                }]
            )
            .await
            .is_err()
    );
    remote
        .publish_files(
            FileRoot::Private {},
            crate::OperationId::new(),
            vec![ClientFileChange {
                path: paths[0].clone(),
                expected: sources.first().cloned(),
                text: None,
            }],
        )
        .await?;
    assert!(local.files(FileRoot::Private {}, paths, None).await?.files["control/pause"].is_none());
    assert!(
        provider.request(0).is_none(),
        "file control publication dispatched a model"
    );
    let mut local_feed = connection.supervision().await?;
    let mut remote_feed = remote_connection.supervision().await?;
    // The same ordinary remote publication now drives the host-owned control
    // actor. These waits prove actuation, rather than just file visibility.
    for paused in [true, false] {
        let event = local_feed.next().await?;
        assert_eq!(event.run, None);
        assert!(matches!(
            (&event.event, paused),
            (crate::supervision::Lifecycle::Paused { signal: None }, true)
                | (crate::supervision::Lifecycle::Resumed {}, false)
        ));
        assert_eq!(event, remote_feed.next().await?);
    }
    let input = local.send("healthy", "Finish this healthy turn.").await?;
    let (started_position, local_started) = local_feed.next_record().await?;
    assert!(matches!(
        local_started.event,
        crate::supervision::Lifecycle::Started { input: accepted } if accepted == input
    ));
    assert_eq!(local_started, remote_feed.next().await?);
    local.settle("healthy", input, &mut observer).await?;
    let local_cut = local_feed.root_cut().await?;
    let remote_cut = remote_feed.root_cut().await?;
    assert!(!local_feed.reached(&local_cut)?);
    assert!(!remote_feed.reached(&remote_cut)?);
    let (done_position, local_done) = local_feed.next_record().await?;
    assert_eq!(local_done.run, local_started.run);
    assert!(
        matches!(local_done.event, crate::supervision::Lifecycle::TurnDone { turn } if Some(turn) == local_started.run)
    );
    assert_eq!(local_done, remote_feed.next().await?);
    assert!(local_feed.reached(&local_cut)?);
    assert!(remote_feed.reached(&remote_cut)?);
    let foreign = connection.follow_tree(StreamPath::new("inbox")?).await?;
    assert!(foreign.reached(&local_cut).is_err());
    let stream = harness.agent_stream.as_ref().unwrap();
    let record = |path, from| acyclic_stream::ReadRequest {
        path,
        from,
        limit: 1,
        key: None,
        at_least: Default::default(),
    };
    use futures::TryStreamExt as _;
    // Source and event must share the exact original commit, for admission
    // and settlement. A separate best-effort lifecycle write cannot pass.
    let mut source = stream
        .read(record(
            local_started.source.stream.clone(),
            local_started.source.sequence,
        ))
        .await?;
    let source = source.try_next().await?.unwrap();
    let mut events = stream
        .read(record(
            crate::supervision::streams::path(connection.root())?,
            started_position.sequence,
        ))
        .await?;
    let event = events.try_next().await?.unwrap();
    assert_eq!(source.commit_id, event.commit_id);
    let mut source = stream
        .read(record(
            local_done.source.stream.clone(),
            local_done.source.sequence,
        ))
        .await?;
    let source = source.try_next().await?.unwrap();
    let mut events = stream
        .read(record(
            crate::supervision::streams::path(connection.root())?,
            done_position.sequence,
        ))
        .await?;
    let event = events.try_next().await?.unwrap();
    assert_eq!(source.commit_id, event.commit_id);
    assert_eq!(
        stream
            .tail(crate::supervision::streams::path(connection.root())?)
            .await?,
        done_position.sequence + 1,
        "healthy turn emitted a supervision signal"
    );
    assert_eq!(done_position.sequence, started_position.sequence + 1);
    let measured_reply = || {
        crate::test_support::ScriptedTurn::Events(vec![
            crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Content {
                delta: "done".into(),
            }),
            crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                metadata: serde_json::json!({"usage":{"input_tokens":1,"output_tokens":1}}),
            }),
        ])
    };
    // Descendant observation is enrolled on the same transport pool. The
    // original root capability remains narrow. Selecting a read-only view
    // retains the original caller's command authority, while its raw follows
    // are restricted to the selected observation credential.
    let spawn = async |from: &super::Connection, name: &str| -> Result<crate::agent::AgentPath> {
        provider.extend([measured_reply()]);
        let AgentReply::Spawned { agent, .. } = from
            .execute(&AgentCommand::Spawn {
                parent: from.root().clone(),
                id: crate::agent::IntentId::try_from(name).map_err(|error| {
                    Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
                })?,
                brief: crate::model::ModelContent::Text(format!("{name} task")),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Conflict("spawn reply differs".into()));
        };
        Ok(agent)
    };
    eprintln!("client file journey: spawning observed children");
    let sibling = spawn(&remote_connection, "observed-sibling").await?;
    let child = spawn(&remote_connection, "observed-child").await?;
    let child_connection = remote_connection.select(child.clone()).await?;
    assert_ne!(
        child_connection.session.credential(),
        remote_connection.session.credential()
    );
    let local_child = connection.select(child.clone()).await?;
    assert_ne!(
        local_child.session.credential(),
        connection.session.credential(),
        "local selection opened a new session instead of enrolling its view"
    );
    assert_eq!(local_child.select(sibling.clone()).await?.root(), &sibling);
    eprintln!("client file journey: opening local child supervision");
    let mut local_child_feed = local_child.supervision_own().await?;
    assert_eq!(
        child_connection.select(sibling.clone()).await?.root(),
        &sibling
    );
    assert_eq!(
        child_connection
            .select(connection.root().clone())
            .await?
            .root(),
        connection.root(),
    );
    let sibling_source = crate::supervision::streams::path(&sibling)?;
    assert!(
        child_connection
            .session
            .follow(acyclic_stream::FollowRequest::new(sibling_source, 0))
            .await
            .is_err()
    );
    eprintln!("client file journey: opening remote child supervision");
    let mut child_feed = child_connection.supervision_own().await?;
    let child_conversation = Conversation::open(child_connection.clone());
    // Complete the real mock-model child turn before publishing its pause.
    loop {
        if matches!(child_feed.next().await?.event, Lifecycle::TurnDone { .. }) {
            break;
        }
    }
    // A non-root observer uses each discovering parent's narrow read grant
    // at every depth, while selections retain the original caller identity.
    eprintln!("client file journey: opening subtree supervision");
    let mut subtree = child_connection.supervision().await?;
    eprintln!("client file journey: spawning grandchild");
    let grandchild = spawn(&child_connection, "observed-grandchild").await?;
    let grandchild_connection = child_connection.select(grandchild).await?;
    eprintln!("client file journey: spawning great-grandchild");
    let descendant = spawn(&grandchild_connection, "observed-great-grandchild").await?;
    assert_eq!(
        child_connection.select(descendant.clone()).await?.root(),
        &descendant
    );
    let descendant_connection = grandchild_connection.select(descendant.clone()).await?;
    eprintln!("client file journey: opening descendant supervision");
    let mut descendant_feed = descendant_connection.supervision_own().await?;
    loop {
        if matches!(
            descendant_feed.next().await?.event,
            Lifecycle::TurnDone { .. }
        ) {
            break;
        }
    }
    let descendant_conversation = Conversation::open(descendant_connection.clone());
    descendant_conversation
        .publish_files(
            FileRoot::Private {},
            crate::OperationId::new(),
            vec![ClientFileChange {
                path: "control/pause".into(),
                expected: None,
                text: Some(String::new()),
            }],
        )
        .await?;
    eprintln!("client file journey: awaiting descendant pause");
    let descendant_pause = loop {
        let event = descendant_feed
            .next()
            .await
            .map_err(|error| connection.stream_failure(error))?;
        if matches!(event.event, Lifecycle::Paused { signal: None }) {
            break event;
        }
    };
    eprintln!("client file journey: awaiting subtree pause");
    let subtree_pause = loop {
        let event = subtree.next().await?;
        if event.agent == descendant && matches!(event.event, Lifecycle::Paused { signal: None }) {
            break event;
        }
    };
    assert_eq!(subtree_pause, descendant_pause);
    drop(subtree);
    drop(descendant_conversation);
    drop(descendant_feed);
    drop(descendant_connection);
    drop(grandchild_connection);
    child_conversation
        .publish_files(
            FileRoot::Private {},
            crate::OperationId::new(),
            vec![ClientFileChange {
                path: "control/pause".into(),
                expected: None,
                text: Some(String::new()),
            }],
        )
        .await?;
    eprintln!("client file journey: awaiting child pause");
    let child_pause = loop {
        let event = child_feed.next().await?;
        if matches!(event.event, Lifecycle::Paused { signal: None }) {
            break event;
        }
    };
    eprintln!("client file journey: awaiting local child pause");
    let local_child_pause = loop {
        let event = local_child_feed.next().await?;
        if matches!(event.event, Lifecycle::Paused { signal: None }) {
            break event;
        }
    };
    assert_eq!(local_child_pause, child_pause);
    eprintln!("client file journey: awaiting root tree pause");
    let tree_pause = loop {
        let event = remote_feed.next().await?;
        if event.agent == child && matches!(event.event, Lifecycle::Paused { signal: None }) {
            break event;
        }
    };
    assert_eq!(child_pause, tree_pause);

    // Default status files use the very same remote file path and original
    // streams. A healthy run has no synthetic all-clear files. Follow each
    // committed input cut; no readiness probe or timer is involved.
    eprintln!("client file journey: reading signal files");
    let signal_path = "status/supervision/spend.json".to_owned();
    let initial = remote
        .files(FileRoot::Private {}, vec![signal_path.clone()], None)
        .await?;
    assert!(initial.files[&signal_path].is_none());
    let mut policy = crate::supervision::Policy::default();
    policy.reservation.tokens = Some(1);
    provider.extend([measured_reply()]);
    eprintln!("client file journey: running reserved signal producer");
    let (projected, outcome) = {
        let observing = async {
            let mut cut = initial.cut;
            loop {
                let snapshot = remote
                    .files(FileRoot::Private {}, vec![signal_path.clone()], Some(cut))
                    .await?;
                if snapshot.files[&signal_path].is_some() {
                    break Ok::<_, Error>(snapshot);
                }
                cut = snapshot.cut;
            }
        };
        let running =
            local.run_until_goal(crate::OperationId::new(), "Complete this task.", &policy);
        tokio::pin!(observing, running);
        tokio::select! {
            projected = &mut observing => (projected?, running.await?),
            outcome = &mut running => {
                let outcome = outcome?;
                // A terminal source cut must already contain its signal. Inspect
                // that finite snapshot before awaiting the file follower, so a
                // missing projection fails rather than waiting for another turn.
                let settled = remote
                    .files(FileRoot::Private {}, vec![signal_path.clone()], None)
                    .await?;
                assert!(settled.files[&signal_path].is_some(),
                    "terminal run has no signal file: {outcome:?}; cut {:?}", settled.cut);
                (observing.await?, outcome)
            }
        }
    };
    assert!(matches!(
        outcome,
        crate::local::agent_conversation::RunOutcome::Terminal {
            action: crate::supervision::Action::Pause,
            signal: Some(crate::supervision::Kind::Spend)
        }
    ));
    let value = projected.files[&signal_path].as_ref().unwrap();
    assert!(
        value.original().is_none(),
        "query invented an authored precondition"
    );
    let signal: crate::supervision::Signal =
        serde_json::from_str(&value.text).map_err(|error| {
            Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
        })?;
    assert_eq!(signal.kind, crate::supervision::Kind::Spend);
    assert!(
        matches!(&value.source, crate::client_files::FileSource::Query { input, .. }
        if input.stream == crate::supervision::streams::path(connection.root())?
        && input.sequence < projected.cut.queries[&input.stream])
    );
    if std::env::var_os("ACYCLIC_CAPTURE_CLIENT_FILES").is_some() {
        let command = AgentCommand::ClientFiles {
            agent: connection.root().clone(),
            root: FileRoot::Private {},
            publication: None,
            paths: vec![signal_path.clone()],
            after: None,
        };
        let reply = remote_connection.execute(&command).await?;
        let mut bytes = acyclic_harness_client::PreparedCommand::new(&command)
            .map_err(|error| {
                Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
            })?
            .command()
            .to_vec();
        bytes.push(0);
        bytes.extend(
            acyclic_harness_client::encode_agent_reply(&Ok(reply), &command).map_err(|error| {
                Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
            })?,
        );
        println!(
            "CLIENT_FILES_QUERY_CORPUS {}",
            serde_json::to_string(&bytes).map_err(|error| Error::exchange(
                crate::ExchangeStep::Step("endpoint test operation"),
                error
            ))?
        );
    }
    let selected = local
        .files(FileRoot::Private {}, vec![signal_path.clone()], None)
        .await?;
    let unchanged = remote
        .files(FileRoot::Private {}, vec![signal_path.clone()], None)
        .await?;
    assert_eq!(
        selected.cut.generation, unchanged.cut.generation,
        "reading a default view published an authored generation"
    );
    remote
        .publish_files(
            FileRoot::Private {},
            crate::OperationId::new(),
            vec![ClientFileChange {
                path: signal_path.clone(),
                expected: None,
                text: Some("owner override".into()),
            }],
        )
        .await?;
    let overridden = local
        .files(FileRoot::Private {}, vec![signal_path.clone()], None)
        .await?;
    let authored = overridden.files[&signal_path].as_ref().unwrap();
    assert_eq!(authored.text, "owner override");
    assert!(
        overridden.cut.queries.is_empty(),
        "overridden view still did query work"
    );
    remote
        .publish_files(
            FileRoot::Private {},
            crate::OperationId::new(),
            vec![ClientFileChange {
                path: signal_path.clone(),
                expected: authored.original().cloned(),
                text: None,
            }],
        )
        .await?;
    let revealed = remote
        .files(FileRoot::Private {}, vec![signal_path.clone()], None)
        .await?;
    assert_eq!(
        revealed.files[&signal_path].as_ref().unwrap().text,
        value.text
    );
    assert!(
        revealed.files[&signal_path]
            .as_ref()
            .unwrap()
            .original()
            .is_none()
    );

    drop(child_conversation);
    drop(local_child_feed);
    drop(local_child);
    drop(child_feed);
    drop(child_connection);
    drop(local_feed);
    drop(remote_feed);
    drop(remote_connection);
    drop(connection);
    drop(remote);
    drop(observer);
    drop(local);
    let _ = stop.send(());
    tasks
        .join_next()
        .await
        .ok_or_else(|| Error::Conflict("serving task absent".into()))?
        .map_err(|error| {
            Error::exchange(crate::ExchangeStep::Step("endpoint test operation"), error)
        })??;
    Ok(())
}
