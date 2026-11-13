//! Real effects, actor commits and bindings: names survive a setup failure.
use super::*;
use acyclic_harness_core::{
    filesystem::StockExecutors,
    model::{ModelEvent, ToolCall, ToolResultContent},
    test_support::ScriptedEvent,
};
use acyclic_stream::testing::{Counted, RequestClass};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "spawn_names/send_files.rs"]
mod send_files;

fn call(tool: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            tool.into(),
            tool.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn spawn_call(name: &str, peers: &[&str]) -> ScriptedTurn {
    call(
        "spawn",
        json!({"name": name, "task": "Work on the assigned brief", "role": "worker", "peers": peers}),
    )
}

// Stock templates ask for a retrospective after each settled answer. Keep
// those responses beside the work script so they cannot consume another
// agent's next action or a later setup retry.
fn with_retrospectives(
    turns: impl IntoIterator<Item = ScriptedTurn>,
) -> impl Iterator<Item = ScriptedTurn> {
    turns.into_iter().flat_map(|turn| {
        let feedback =
            matches!(&turn, ScriptedTurn::Text(_)).then(|| ScriptedTurn::Text("none".into()));
        std::iter::once(turn).chain(feedback)
    })
}

async fn fixture(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
)> {
    let (agents, stream, root, model, _) = fixture_with_files(turns, setup_failures).await?;
    Ok((agents, stream, root, model))
}

async fn fixture_with_files(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
    Host,
)> {
    fixture_with_workspace(turns, setup_failures, false).await
}

struct ProjectTasks {
    harness: Arc<acyclic_harness_core::runtime::AgentHarness>,
    model: Arc<ScriptedModelProvider>,
    pause: Option<Arc<send_files::PublicationPause>>,
}

impl acyclic_harness_core::filesystem::TurnContext for ProjectTasks {
    fn prepare<'a>(
        &'a self,
        (executor, limits): (
            acyclic_harness_core::executor::StockExecutor,
            acyclic_harness_core::conversation::Limits,
        ),
        agent: &'a AgentState,
        turn: OperationId,
        _: &'a ModelContent,
        authority: &'a acyclic_harness_core::filesystem::TurnAuthority,
    ) -> BoxProviderFuture<
        'a,
        acyclic_harness_core::Result<acyclic_harness_core::executor::StockExecutor>,
    > {
        Box::pin(async move {
            let entry = WorkspaceTable::primary_in(&agent.volumes)
                .ok_or_else(|| acyclic_harness_core::Error::NotFound("project".into()))?;
            let home = agent
                .volumes
                .iter()
                .find(|binding| binding.role == acyclic_harness_core::agent::WorkspaceRole::Home)
                .and_then(|binding| binding.as_volume())
                .ok_or_else(|| acyclic_harness_core::Error::NotFound("home".into()))?;
            let harness = self.harness.with_workspace(
                None,
                acyclic_harness_core::runtime::ContentBindings {
                    volume: entry.volume()?.clone(),
                    reader: authority.content.reader.clone(),
                    writer: None,
                },
                acyclic_harness_core::runtime::ContentBindings {
                    volume: home.clone(),
                    reader: authority.content.reader.clone(),
                    writer: None,
                },
            )?;
            let executor = if let Some(pause) = &self.pause {
                let mut tools = ToolRegistry::default();
                acyclic_harness_core::tools::bind_agent_tools(
                    &mut tools,
                    agent,
                    (
                        authority.effects.clone().ok_or_else(|| {
                            acyclic_harness_core::Error::Invalid("missing effects".into())
                        })?,
                        authority.children.clone(),
                    ),
                    Arc::new(send_files::PausedPublisher {
                        inner: authority
                            .journal
                            .output_content(turn)?
                            .and_then(|content| content.writer)
                            .ok_or_else(|| {
                                acyclic_harness_core::Error::Invalid(
                                    "missing journal writer".into(),
                                )
                            })?,
                        pause: pause.clone(),
                    }),
                    limits.file_bytes,
                    (
                        &[
                            "spawn".into(),
                            "send".into(),
                            "wait".into(),
                            "archive".into(),
                        ],
                        authority.templates.clone(),
                    ),
                )?;
                acyclic_harness_core::executor::StockExecutor::new(
                    Model::new("test".into(), "text".into(), "1".into())?,
                    self.model.clone(),
                    Default::default(),
                    tools,
                )
                .with_limits(limits)
                .with_compaction_policy(CompactionPolicy::Disabled)
            } else {
                executor
            };
            Tasks::bind(&harness, executor, agent, turn, authority).map(|(executor, _)| executor)
        })
    }
}

async fn fixture_with_workspace(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
    project: bool,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
    Host,
)> {
    fixture_with_pause(turns, setup_failures, project, None).await
}

async fn fixture_with_pause(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
    project: bool,
    pause: Option<Arc<send_files::PublicationPause>>,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
    Host,
)> {
    fixture_with_route(turns, setup_failures, project, pause, None).await
}

async fn fixture_with_route(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
    project: bool,
    pause: Option<Arc<send_files::PublicationPause>>,
    routing: Option<Routing>,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
    Host,
)> {
    let root = AgentPath::root(&id(81));
    let stream = Arc::new(MemoryStream::default());
    let model = Arc::new(ScriptedModelProvider::new(turns));
    let (files, binding) = private_in(&stream, &root).await?;
    files.seed_layer().await?;
    let limits = acyclic_harness_core::conversation::Limits {
        file_bytes: if project {
            LIMITS.maximum_bytes
        } else {
            acyclic_harness_core::conversation::Limits::default().file_bytes
        },
        ..Default::default()
    };
    let harness = acyclic_harness_core::runtime::Bindings {
        scope: acyclic_harness_core::runtime::RuntimeScope::new(
            Capabilities::new([acyclic_harness_core::contract::capability::MODEL_GENERATE]),
            limits,
        )?,
        ..Default::default()
    }
    .build()?;
    let context: Arc<dyn acyclic_harness_core::filesystem::TurnContext> = if project {
        Arc::new(ProjectTasks {
            harness,
            model: model.clone(),
            pause,
        })
    } else {
        Arc::new(Tasks(harness))
    };
    let executors = StockExecutors::new(
        (
            Model::new("test".into(), "text".into(), "1".into())?,
            model.clone(),
        ),
        ToolRegistry::default(),
        (limits, CompactionPolicy::Disabled),
        context,
    )
    .with_models(Arc::new(acyclic_harness_core::model::ModelResolver::new(
        "test/text",
        {
            let model = model.clone();
            move |selection| {
                let (provider, name) = selection.split_once('/').ok_or_else(|| {
                    acyclic_harness_core::Error::Invalid("expected provider/model".into())
                })?;
                Ok((
                    Model::new(provider.into(), name.into(), "1".into())?,
                    model.clone(),
                ))
            }
        },
    )))
    .with_agent_tools(&[
        "spawn".into(),
        "send".into(),
        "wait".into(),
        "archive".into(),
    ])
    .with_templates();
    let remaining = AtomicUsize::new(setup_failures);
    let turns = FilesystemTurnHost::new(
        stream.clone(),
        within(),
        files.clone(),
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        Arc::new(executors),
        LIMITS,
        acyclic_harness_core::approvals::ApprovalPolicy::accept_all("test"),
    )
    .with_binding_fault(move |agent| {
        if agent.parent.is_some()
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(acyclic_harness_core::Error::Invalid(
                "injected child binding failure".into(),
            ));
        }
        Ok(())
    });
    let templates = turns.templates().ok_or("missing creation templates")?;
    let driver = ExecutorDriver::new(turns);
    let agents = match routing {
        None => committing(&stream, &files, driver)?,
        Some(routing) => {
            let volumes: Arc<dyn SpawnVolumes> = Arc::new(
                acyclic_harness_core::filesystem::FilesystemSpawnVolumes::new(files.clone()),
            );
            let handler = AgentHandler::new(
                actor_store(&stream)?,
                driver,
                acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            )?
            .with_spawn_volumes(volumes.clone())
            .with_close()
            .with_routing(routing);
            AgentHost::hosting(
                acyclic_actors::guest::Requests::default(),
                stream.clone(),
                within(),
                Arc::new(handler),
                Arc::new(acyclic_actors::local::SystemClock),
                Some(Arc::new(AgentComposer::new(
                    stream.clone(),
                    Some(volumes),
                    None,
                ))),
            )?
        }
    }
    .with_templates(templates);
    let mut adoption = adopted(root.id());
    adoption.volumes = vec![binding];
    adoption.volumes.push(
        home_binding(
            &files,
            &root,
            adoption.volumes[0].volume()?.provider().clone(),
        )
        .await?,
    );
    if project {
        let volume = VolumeRef::new(
            adoption.volumes[0].volume()?.provider().clone(),
            "send-files-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("send-files".into()),
            &mut None,
        )?;
        files.create_volume(&volume).await?;
        adoption.volumes.push(WorkspaceEntry::initial_volume(
            WorkspaceRole::User,
            "project".into(),
            volume,
        ));
    }
    agents.create(&intent("root"), adoption).await?;
    Ok((agents, stream, root, model, files))
}

async fn home_binding(
    files: &Host,
    agent: &AgentPath,
    provider: acyclic_harness_core::resources::ProviderRef,
) -> Result<WorkspaceEntry> {
    let volume = VolumeRef::new(
        provider,
        "agent-home".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
            agent.id().to_bytes(),
        )),
        &mut None,
    )?;
    files.create_volume(&volume).await?;
    Ok(WorkspaceEntry::initial_volume(
        WorkspaceRole::Home,
        "home".into(),
        volume,
    ))
}
async fn create_peer(
    agents: &AgentHost<MemoryStream>,
    files: &Host,
    root: &AgentPath,
    peer: &AgentPath,
) -> Result {
    let source = agents
        .state(root)
        .await?
        .volumes
        .into_iter()
        .find(|binding| binding.name == PRIVATE_VOLUME)
        .ok_or("root private volume missing")?;
    let volume = VolumeRef::new(
        source.volume()?.provider().clone(),
        "agent-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
            peer.id().to_bytes(),
        )),
        &mut None,
    )?;
    files.create_volume(&volume).await?;
    files
        .create_volume(&acyclic_harness_core::conversation::journal_volume(
            volume.provider().clone(),
            &peer.id(),
        )?)
        .await?;
    let mut adoption = adopted(peer.id());
    adoption.volumes = vec![WorkspaceEntry::initial_volume(
        WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        volume,
    )];
    adoption
        .volumes
        .push(home_binding(files, peer, source.volume()?.provider().clone()).await?);
    agents
        .create(&intent(&format!("create-{}", peer.id())), adoption)
        .await?;
    Ok(())
}

#[tokio::test]
async fn automatic_self_send_is_refused_without_stopping_the_actor() -> Result {
    let own_path = AgentPath::root(&id(81));
    let (agents, stream, root, model) = fixture(
        vec![
            call(
                "send",
                json!({"agent": own_path.as_str(), "message": "self input"}),
            ),
            ScriptedTurn::Text("continued after refusal".into()),
        ],
        0,
    )
    .await?;
    deliver(&agents, &root, requested("start", &root)).await?;
    assert_eq!(
        inbox_tail(&*stream, &root).await?,
        2,
        "no automatic self input appended"
    );
    assert_eq!(agents.state(&root).await?.conversation_tail, 1);
    let request = model.request(1).ok_or("no continuation after refusal")?;
    assert!(request.messages.iter().flat_map(|message| message.content.parts()).any(|part|
        matches!(part, ModelContentPart::ToolResult { name, content: ToolResultContent::Json { value }, .. }
            if name == "send" && value.get("error").is_some())));
    submit(
        &*stream,
        set_peer_membership(&root, &super::super::user(), &intent("off"), false)?,
    )
    .await?;
    model.extend([ScriptedTurn::Text("still hosted".into())]);
    deliver(&agents, &root, requested("after-off", &root)).await?;
    let state = agents.state(&root).await?;
    assert_eq!(state.conversation_tail, 2);
    assert!(!state.closed);
    assert_eq!(model.request_count(), 3);
    Ok(())
}

#[tokio::test]
async fn send_pins_files_written_after_the_child_fork_without_granting_the_sender_volume() -> Result
{
    use acyclic_harness_core::conversation::{
        ContentAccess, ContentGrant, ContentPublisher, ContentResidencyVerifier,
    };
    use acyclic_harness_core::filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, agent_authority,
        agent_journal_authority,
    };

    const PATH: &str = "research/findings/api-evaluation-request-not-snapshotted.md";
    const ORIGINAL: &[u8] = b"The finding written after finding-review forked.\n";
    for (delivery, interrupted_manifest) in [
        ("steer", None),
        ("follow_up", None),
        ("steer", Some(false)),
        ("steer", Some(true)),
    ] {
        let pause = interrupted_manifest
            .map(|manifest| Arc::new(send_files::PublicationPause::new(manifest)));
        let (agents, stream, root, model, files) = fixture_with_pause(
            with_retrospectives([
                spawn_call("finding-review", &[]),
                ScriptedTurn::Text("spawned".into()),
            ])
            .collect(),
            0,
            true,
            pause.clone(),
        )
        .await?;
        deliver(&agents, &root, requested("spawn", &root)).await?;
        let child = named(&agents, &root, "finding-review").await?;
        let sender = agents.state(&root).await?;
        let recipient = agents.state(&child).await?;
        let child_inbox = super::super::values(&*stream, child.inbox()?).await?;
        let mut brief = None;
        for bytes in &child_inbox {
            if let InboxRecord::ForkAdopted(adoption) = InboxRecord::decode(bytes)?
                && adoption.agent == child.id()
            {
                brief = adoption.brief;
                break;
            }
        }
        let brief = brief.ok_or("spawn lost its brief")?;
        let project = WorkspaceTable::primary_in(&sender.volumes)
            .ok_or("sender project")?
            .volume()?;
        let fork = WorkspaceTable::primary_in(&recipient.volumes)
            .ok_or("child project")?
            .volume()?;
        assert_ne!(project, fork);
        let fork_workspace = acyclic_harness_core::filesystem::workspace_ref(
            fork.provider().clone(),
            &fork.storage_name(&mut None)?,
        )?;
        let issuer = AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        )
        .into();
        let (journal, host_authority) = agent_journal_authority(
            &files,
            &issuer,
            &TurnAdmission::LocalRoot,
            &root,
            &sender.volumes,
            &[],
        )
        .await?;
        assert_eq!(brief.volume(), &journal);
        assert!(brief.path().starts_with("inputs/briefs/"));
        let journal_writer = FilesystemContentPublisher::new(
            files.clone(),
            &journal,
            &host_authority.verifier,
            &host_authority.scope,
            LIMITS.maximum_bytes,
            None,
        )?;
        let mut unselected = Vec::new();
        for path in [
            "inputs/messages/unselected/files/0",
            ".system/execution/private-control.json",
        ] {
            unselected.push(
                journal_writer
                    .stage_once(
                        OperationId::new(),
                        path,
                        b"unselected host content",
                        "text/plain",
                        "unselected",
                        ContentAccess::default(),
                    )?
                    .await?
                    .0,
            );
        }
        let authority = agent_authority(
            &issuer,
            &TurnAdmission::LocalRoot,
            &root,
            &sender.volumes,
            &[],
        )
        .await?;
        let publisher = FilesystemContentPublisher::new(
            files.clone(),
            project,
            &authority.verifier,
            &authority.scope,
            LIMITS.maximum_bytes,
            None,
        )?;
        let mut source = publisher
            .stage_once(
                OperationId::new(),
                PATH,
                ORIGINAL,
                "text/markdown",
                "finding.md",
                ContentAccess::default(),
            )?
            .await?
            .0;
        assert!(
            matches!(files.stat(&fork_workspace, None, &format!("/{PATH}")).await,
            Err(error) if error.code() == acyclic_harness_core::wire::ErrorCode::NotFound)
        );

        let before = super::super::values(&*stream, child.inbox()?).await?;
        publisher
            .stage_once(
                OperationId::new(),
                "large.md",
                &vec![b'x'; 32 * 1024],
                "text/markdown",
                "large.md",
                ContentAccess::default(),
            )?
            .await?;
        model.extend(with_retrospectives([
            call(
                "send",
                json!({"agent":"finding-review", "message":"incomplete selection",
                "files":[PATH, "research/findings/missing.md"]}),
            ),
            ScriptedTurn::Text("selection refused".into()),
            call(
                "send",
                json!({"agent":"finding-review", "message":"oversized selection",
                "files":["large.md", "large.md", "large.md"]}),
            ),
            ScriptedTurn::Text("combined selection refused".into()),
        ]));
        deliver(&agents, &root, requested("missing-member", &root)).await?;
        deliver(&agents, &root, requested("combined-budget", &root)).await?;
        assert_eq!(
            super::super::values(&*stream, child.inbox()?).await?,
            before
        );

        model.extend(with_retrospectives([
            call("send", json!({
                "agent":"finding-review",
                "message":format!("One additional candidate is now ready: {PATH}. Please PASS/FAIL it too."),
                "files":[PATH],
                "delivery":delivery,
            })),
            ScriptedTurn::Text("sent".into()),
        ]));
        if let Some(pause) = pause {
            let entered = pause.arm();
            let mut sending = Box::pin(deliver(&agents, &root, requested("send-late-file", &root)));
            tokio::select! {
                reached = entered => reached?,
                result = &mut sending => { result?; return Err("send completed before interruption".into()); }
            }
            drop(sending);
            assert_eq!(
                super::super::values(&*stream, child.inbox()?).await?,
                before
            );
            let replacement = publisher
                .stage_at(
                    OperationId::new(),
                    &source,
                    b"replacement after publication",
                    None,
                    None,
                )?
                .await?
                .0;
            source = replacement;
        }
        deliver(&agents, &root, requested("send-late-file", &root)).await?;
        let inbox = super::super::values(&*stream, child.inbox()?).await?;
        let record = InboxRecord::decode(inbox.last().ok_or("no child message")?)?;
        let message = match &record {
            InboxRecord::Message(message) | InboxRecord::FollowUp(message) => message,
            _ => return Err("no delivered message".into()),
        };
        let manifest = message
            .attachments
            .as_deref()
            .ok_or("send lost selected files")?;
        assert_eq!(message.body.volume(), &journal);
        assert!(message.body.path().starts_with("inputs/messages/"));
        assert_eq!(manifest.volume(), &journal);
        let admitted = agent_authority(
            &issuer,
            &TurnAdmission::LocalRoot,
            &child,
            &recipient.volumes,
            &[brief.clone(), (*message.body).clone(), manifest.clone()],
        )
        .await?;
        let reader = FilesystemContentVerifier::new(
            files.clone(),
            &admitted.verifier,
            &admitted.scope,
            LIMITS.maximum_bytes,
            None,
        )?;
        assert_eq!(
            reader
                .read(&brief, brief.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            b"Work on the assigned brief"
        );
        for file in &unselected {
            let denied = async {
                reader
                    .read(file, file.descriptor().byte_length(), None, None)?
                    .await
            }
            .await
            .expect_err("a message granted unrelated journal content");
            assert_eq!(
                denied.code(),
                acyclic_harness_core::wire::ErrorCode::Unauthorized
            );
        }
        let bytes = reader
            .read(manifest, manifest.descriptor().byte_length(), None, None)?
            .await?;
        let items = acyclic_harness_core::conversation::decode_attachment_manifest(
            manifest, &bytes, None, None,
        )?;
        let [item] = items.as_slice() else {
            return Err("attachment selection changed".into());
        };
        assert_eq!(item.file.volume(), &journal);
        assert_eq!(
            reader
                .read(&item.file, item.file.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            ORIGINAL
        );
        assert!(
            ContentGrant::verify(
                &admitted.verifier,
                &admitted.scope,
                manifest.volume(),
                VolumeOperation::Write,
                &mut None
            )
            .is_err()
        );
        assert!(
            !admitted
                .grants
                .contains(&project.capability(VolumeOperation::Read, &mut None)?)
        );

        // The child reads the pinned copy after its parent's source changes.
        publisher
            .stage_at(
                OperationId::new(),
                &source,
                b"later replacement",
                None,
                None,
            )?
            .await?;
        model.extend(with_retrospectives([ScriptedTurn::Text(
            "reviewed the original finding".into(),
        )]));
        if delivery == "follow_up" {
            model.extend(with_retrospectives([ScriptedTurn::Text(
                "reviewed the follow-up finding".into(),
            )]));
        }
        agents.drain(&child).await?;
        assert_eq!(agents.state(&child).await?.phase, Phase::Idle, "{delivery}");
        let request = model.last_request().ok_or("child did not run")?;
        // Steering pins newly admitted files into the recipient's read-only
        // turn input route, preserving the descriptor instead of the address.
        let projected = request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::File { file, .. }
                    if file.descriptor() == item.file.descriptor() =>
                {
                    Some(file)
                }
                _ => None,
            })
            .ok_or("child did not receive the immutable finding")?;
        // A fresh turn retains the original admitted journal reference. A
        // running turn pins it into its already-bound read-only input route.
        if projected != &item.file {
            assert_eq!(
                projected.volume(),
                &acyclic_harness_core::conversation::journal_volume(
                    project.provider().clone(),
                    &child.id(),
                )?
            );
            assert!(projected.path().starts_with("inputs/turns/"));
        }
        let projected_authority = agent_authority(
            &issuer,
            &TurnAdmission::LocalRoot,
            &child,
            &recipient.volumes,
            std::slice::from_ref(projected),
        )
        .await?;
        let projected_reader = FilesystemContentVerifier::new(
            files.clone(),
            &projected_authority.verifier,
            &projected_authority.scope,
            LIMITS.maximum_bytes,
            None,
        )?;
        assert_eq!(
            projected_reader
                .read(projected, projected.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            ORIGINAL
        );
        assert_eq!(
            reader
                .read(&item.file, item.file.descriptor().byte_length(), None, None)?
                .await?
                .as_ref(),
            ORIGINAL
        );
        assert!(
            matches!(files.stat(&fork_workspace, None, &format!("/{PATH}")).await,
            Err(error) if error.code() == acyclic_harness_core::wire::ErrorCode::NotFound)
        );

        // Replaying the original accepted input neither rereads the changed
        // source nor appends a second delivery to the child's inbox.
        let requests = model.request_count();
        deliver(&agents, &root, requested("send-late-file", &root)).await?;
        assert_eq!(model.request_count(), requests);
        assert_eq!(super::super::values(&*stream, child.inbox()?).await?, inbox);
    }
    Ok(())
}

#[tokio::test]
async fn remote_send_yields_its_original_tool_until_account_admission() -> Result {
    use acyclic_actors::route::{self, NodeId, Paths};
    use acyclic_stream::{CommitMutation, FollowRequest, StreamPath, WhenAbsent};
    use futures::TryStreamExt as _;
    let peer = AgentPath::root(&id(82));
    let routing = Routing {
        paths: Paths::new(StreamPath::new("routing")?),
        node: NodeId::new(b"source")?,
    };
    let destination = NodeId::new(b"destination")?;
    let account = MemoryStream::default();
    let (agents, stream, root, model, _) = fixture_with_route(
        vec![
            call("send", json!({"agent":peer.as_str(),"message":"request"})),
            ScriptedTurn::Text("admitted".into()),
        ],
        0,
        false,
        None,
        Some(routing.clone()),
    )
    .await?;
    // A directory member is known without owning that actor's runtime state.
    submit(
        &*stream,
        create_root(&intent("remote-root"), adopted(peer.id()))?.request,
    )
    .await?;
    assert!(
        !actor_store(&stream)?
            .contains(&agents.actor(&peer)?)
            .await?
    );
    let state = deliver(&agents, &root, requested("send-request", &root)).await?;
    assert!(
        matches!(state.phase, Phase::Waiting { .. }),
        "{:?}",
        model.requests()
    );
    assert_eq!(
        model.request_count(),
        1,
        "pending admission reached the model"
    );
    let outbox = routing.paths.outbox(&routing.node)?;
    assert_eq!(stream.tail(outbox.clone()).await?, 1);
    assert_eq!(stream.tail(peer.inbox()?).await?, 1);
    let record = stream
        .record_at(outbox.clone(), 0)
        .await?
        .ok_or("outbox missing")?;
    let original = route::Publication::decode(&record.value)?.delivery()?;
    let CommitMutation::Append { records, .. } = &original.request.effects[0].mutations[0] else {
        return Err("message missing".into());
    };
    let InboxRecord::Message(message) = InboxRecord::decode(&records[0].value)? else {
        return Err("peer message missing".into());
    };
    let id = message.id;
    assert!(matches!(
        sent(&*stream, &root, &id)
            .await?
            .ok_or("sender receipt missing")?
            .delivery,
        SentDelivery::Pending { from: 0, .. }
    ));
    let mut changes = acyclic_stream::records_only(
        stream
            .follow(FollowRequest {
                when_absent: WhenAbsent::Wait,
                ..FollowRequest::new(routing.paths.outbox_cursor(&routing.node)?, 0)
            })
            .await?,
    );
    let forward = route::forward(
        &*stream,
        &routing.paths,
        &routing.node,
        async |_| Err(route::Error::InvalidRecord),
        async |envelope| {
            envelope
                .publish_relay(&account, &routing.paths, &destination, Vec::new())
                .await
        },
    );
    let acknowledged = changes.try_next();
    futures::pin_mut!(forward, acknowledged);
    match futures::future::select(forward, acknowledged).await {
        futures::future::Either::Right((record, _)) => assert!(record?.is_some()),
        futures::future::Either::Left((result, _)) => {
            return Err(format!("forward stopped: {result:?}").into());
        }
    }
    agents.drain(&root).await?;
    assert_eq!(
        agents.state(&root).await?.phase,
        Phase::Idle,
        "{:?}",
        model.requests()
    );
    let requests = model.requests();
    let results: Vec<_> = requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Json { value },
                ..
            } if name == "send" => Some(value),
            _ => None,
        })
        .collect();
    assert!(!results.is_empty(), "model never observed admission");
    for result in results {
        assert_eq!(result["kind"], "relayed");
        assert_eq!(result["relay_seq"], 0);
        assert_eq!(result["id"], id.as_str());
        assert!(
            result.get("status").is_none(),
            "provisional result reached model history"
        );
    }
    deliver(&agents, &root, requested("send-request", &root)).await?;
    assert_eq!(stream.tail(outbox).await?, 1);
    assert_eq!(account.tail(routing.paths.relay(&destination)?).await?, 1);
    assert_eq!(stream.tail(peer.inbox()?).await?, 1);
    Ok(())
}

#[tokio::test]
async fn peer_send_returns_acceptance_before_receiver_execution() -> Result {
    let peer = AgentPath::root(&id(82));
    let (agents, stream, root, model, files) = fixture_with_files(
        vec![
            call(
                "send",
                json!({"agent": peer.as_str(), "message": "Hello peer"}),
            ),
            ScriptedTurn::Text("sent".into()),
            ScriptedTurn::Text("no follow-up work".into()),
        ],
        0,
    )
    .await?;
    create_peer(&agents, &files, &root, &peer).await?;
    let sender = deliver(&agents, &root, requested("start", &root)).await?;
    assert_eq!(
        sender.phase,
        Phase::Idle,
        "sender requests: {:?}",
        model.requests()
    );
    assert!(matches!(
        answer(&*stream, &root, 0).await?.outcome,
        TurnOutcome::Completed
    ));
    let records = super::super::values(&*stream, peer.inbox()?).await?;
    assert_eq!(records.len(), 2);
    let InboxRecord::Message(message) = InboxRecord::decode(&records[1])? else {
        return Err("peer input missing".into());
    };
    assert_eq!(message.from, Sender::Agent { path: root.clone() });
    let receipt = sent(&*stream, &root, &message.id)
        .await?
        .ok_or("send receipt missing")?;
    assert_eq!(
        receipt.delivery,
        SentDelivery::Complete(acyclic_actors::route::Routed::Delivered(
            acyclic_actors::route::Delivered { inbox: 1 }
        ))
    );
    let requests = model.requests();
    let results: Vec<_> = requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Json { value },
                ..
            } if name == "send" => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(
        agents.state(&peer).await?.conversation_tail,
        0,
        "the receiver has not executed yet"
    );
    let observes =
        |request: &acyclic_harness_core::model::ModelRequest, sender: &AgentPath| {
            request.messages.iter().flat_map(|message| message.content.parts()).any(|part|
            matches!(part, ModelContentPart::Notices { notices } if notices.iter().any(|notice|
                notice.kind == "peer.message" && notice.source == sender.as_str())))
        };
    assert!(requests.iter().all(|request| !observes(request, &root)));
    assert!(
        !results.is_empty(),
        "the sender did not observe its receipt"
    );
    for result in results {
        assert_eq!(result["kind"], "accepted");
        assert_eq!(result["inbox"], 1);
        assert_eq!(result["id"], message.id.as_str());
        assert!(
            result.get("status").is_none(),
            "provisional result reached the model"
        );
    }
    let before_receiver = model.request_count();

    model.extend([
        call(
            "send",
            json!({"agent": root.as_str(), "message": "Hello back", "re": message.id.as_str()}),
        ),
        ScriptedTurn::Text("answered".into()),
        ScriptedTurn::Text("no follow-up work".into()),
    ]);
    agents.drain(&peer).await?;
    let receiver = agents.state(&peer).await?;
    assert_eq!(
        receiver.phase,
        Phase::Idle,
        "receiver requests: {:?}",
        model.requests()
    );
    let reply_turn = answer(&*stream, &peer, 0).await?;
    assert!(
        matches!(&reply_turn.outcome, TurnOutcome::Completed),
        "reply turn: {:?}",
        reply_turn.outcome
    );
    let records = super::super::values(&*stream, root.inbox()?).await?;
    let InboxRecord::Message(reply) =
        InboxRecord::decode(records.last().ok_or("peer reply missing")?)?
    else {
        return Err("peer reply is not a message".into());
    };
    assert_eq!(reply.from, Sender::Agent { path: peer.clone() });
    assert_eq!(reply.re, Some(message.id.clone()));
    assert!(matches!(
        agents.reply(&peer, &message.id, 1).await?,
        Reply::Answered { .. }
    ));
    let receiver_requests = model.requests();
    assert!(
        receiver_requests[before_receiver..]
            .iter()
            .any(|request| observes(request, &root))
    );
    let before_reply = model.request_count();
    model.extend([
        ScriptedTurn::Text("conversation complete".into()),
        ScriptedTurn::Text("no follow-up work".into()),
    ]);
    agents.drain(&root).await?;
    let requests = model.requests();
    assert!(
        requests[before_reply..]
            .iter()
            .any(|request| observes(request, &peer))
    );
    let completed = model.request_count();
    deliver(&agents, &root, requested("start", &root)).await?;
    assert_eq!(
        model.request_count(),
        completed,
        "an exact replay executed the sender again"
    );
    assert_eq!(inbox_tail(&*stream, &peer).await?, 2);
    Ok(())
}

#[tokio::test]
async fn sent_wait_wakes_on_the_original_reply_without_reading_peer_content() -> Result {
    for answered_before_wait in [false, true] {
        let peer = AgentPath::root(&id(82));
        let (agents, stream, root, model, files) = fixture_with_files(
            vec![
                call("send", json!({"agent":peer.as_str(),"message":"request"})),
                ScriptedTurn::Text("send committed".into()),
            ],
            0,
        )
        .await?;
        create_peer(&agents, &files, &root, &peer).await?;
        deliver(&agents, &root, requested("send-request", &root)).await?;
        let records = super::super::values(&*stream, peer.inbox()?).await?;
        let InboxRecord::Message(message) = InboxRecord::decode(&records[1])? else {
            return Err("original send missing".into());
        };
        assert!(
            sent_reply(&stream, &within(), &peer, &message.id)
                .await?
                .is_none(),
            "another sender's intent became an observation capability"
        );
        assert_eq!(
            sent_reply(&stream, &within(), &root, &message.id)
                .await?
                .ok_or("receipt missing")?
                .status,
            SentReplyStatus::Pending
        );
        submit(
            &*stream,
            set_peer_membership(
                &peer,
                &super::super::user(),
                &intent("disable-future-peer-sends"),
                false,
            )?,
        )
        .await?;
        let answer = ScriptedTurn::Text("recipient-only answer content".into());
        if answered_before_wait {
            model.extend([answer.clone()]);
            agents.drain(&peer).await?;
        }
        model.extend([call(
            "wait",
            json!({"sent":[message.id.as_str(),message.id.as_str()]}),
        )]);
        if answered_before_wait {
            model.extend([ScriptedTurn::Text("observed settlement".into())]);
        }
        let state = deliver(&agents, &root, requested("wait-request", &root)).await?;
        if !answered_before_wait {
            assert!(matches!(state.phase, Phase::Waiting { .. }));
            assert_eq!(state.need, 1, "duplicate send IDs increased the quorum");
            let requests = model.request_count();
            agents.drain(&root).await?;
            assert_eq!(
                model.request_count(),
                requests,
                "pending wait ran the model"
            );
            model.extend([answer, ScriptedTurn::Text("observed settlement".into())]);
            agents.drain(&peer).await?;
            agents.drain(&root).await?;
        }
        assert_eq!(agents.state(&root).await?.phase, Phase::Idle);
        let requests = model.requests();
        let resumed = requests.last().ok_or("wait never resumed")?;
        let result = resumed
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::ToolResult {
                    name,
                    content: ToolResultContent::Json { value },
                    ..
                } if name == "wait" => Some(value),
                _ => None,
            })
            .ok_or("wait result missing")?;
        assert_eq!(
            result["sent"]
                .as_array()
                .ok_or("sent results missing")?
                .len(),
            1
        );
        assert_eq!(result["sent"][0]["id"], message.id.as_str());
        assert_eq!(result["sent"][0]["reply"]["kind"], "answered");
        assert!(!serde_json::to_string(resumed)?.contains("recipient-only answer content"));
        let before = model.request_count();
        agents.drain(&root).await?;
        assert_eq!(
            model.request_count(),
            before,
            "a settled watch resumed twice"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sent_wait_counts_each_settlement_once_and_cancel_removes_its_subscriptions() -> Result {
    let peers = [AgentPath::root(&id(82)), AgentPath::root(&id(83))];
    let (agents, stream, root, model, files) = fixture_with_files(
        vec![
            call(
                "send",
                json!({"agent":peers[0].as_str(),"message":"first request"}),
            ),
            call(
                "send",
                json!({"agent":peers[1].as_str(),"message":"second request"}),
            ),
            ScriptedTurn::Text("both sends committed".into()),
        ],
        0,
    )
    .await?;
    for peer in &peers {
        create_peer(&agents, &files, &root, peer).await?;
    }
    deliver(&agents, &root, requested("send-requests", &root)).await?;
    let mut ids = Vec::new();
    for peer in &peers {
        let records = super::super::values(&*stream, peer.inbox()?).await?;
        let InboxRecord::Message(message) = InboxRecord::decode(&records[1])? else {
            return Err("original send missing".into());
        };
        ids.push(message.id);
    }
    model.extend([call("wait", json!({"sent":ids,"need":2}))]);
    deliver(&agents, &root, requested("wait-for-both", &root)).await?;
    assert_eq!(agents.state(&root).await?.need, 2);
    model.extend([ScriptedTurn::Text("first answer".into())]);
    agents.drain(&peers[0]).await?;
    agents.drain(&root).await?;
    assert_eq!(agents.state(&root).await?.need, 1);
    let requests = model.request_count();
    agents.drain(&peers[0]).await?;
    agents.drain(&root).await?;
    assert_eq!(agents.state(&root).await?.need, 1);
    assert_eq!(model.request_count(), requests);
    let waiting = agents.state(&root).await?;
    let input = waiting.turn.as_ref().ok_or("waiting turn missing")?.inbox;
    agents
        .send(
            &root,
            &super::super::user(),
            &InboxRecord::Cancel(Cancellation {
                by: super::super::user(),
                id: intent("cancel-wait"),
                input,
                conversation: waiting.conversation_tail,
            }),
        )
        .await?;
    agents.drain(&root).await?;
    let snapshot = agents.runtime().store().load(&agents.actor(&root)?).await?;
    let subscriptions = snapshot
        .state()
        .ok_or("sender actor missing")?
        .subscriptions();
    assert_eq!(subscriptions.len(), 1, "cancel retained a reply watch");
    assert!(subscriptions.contains_key(INBOX));
    model.extend([ScriptedTurn::Text(
        "second answer after cancellation".into(),
    )]);
    agents.drain(&peers[1]).await?;
    let requests = model.request_count();
    agents.drain(&root).await?;
    assert_eq!(
        model.request_count(),
        requests,
        "late reply resumed a cancelled turn"
    );
    Ok(())
}

async fn named(
    agents: &AgentHost<MemoryStream>,
    parent: &AgentPath,
    name: &str,
) -> Result<AgentPath> {
    agents
        .resolve_spawn_name(parent, name)
        .await?
        .live()
        .ok_or_else(|| format!("missing named child {name}").into())
}

#[tokio::test]
async fn first_turn_failure_after_send_retains_the_spawn_name() -> Result {
    let root = AgentPath::root(&id(81));
    let (agents, stream, _, _) = fixture(
        vec![
            spawn_call("w", &[]),
            ScriptedTurn::Text("spawned".into()),
            call(
                "send",
                json!({"agent": root.as_str(), "message": "committed work"}),
            ),
            ScriptedTurn::Events(vec![ScriptedEvent::Interrupted {
                error: "failure after send".into(),
            }]),
        ],
        0,
    )
    .await?;
    deliver(&agents, &root, requested("start", &root)).await?;
    let child = named(&agents, &root, "w").await?;
    agents.drain(&child).await?;
    let state = agents.state(&child).await?;
    assert_eq!(state.phase, Phase::Idle);
    let entries = super::super::values(&*stream, child.conversation()?).await?;
    assert_eq!(state.conversation_tail - state.conversation_base, 1);
    assert!(matches!(
        ConversationEntry::decode(entries.last().ok_or("missing failed turn")?)?.outcome,
        TurnOutcome::Failed { message } if message.contains("failure after send")
    ));
    assert!(!state.released());
    assert_eq!(named(&agents, &root, "w").await?, child);
    assert!(
        super::super::values(&*stream, root.inbox()?)
            .await?
            .iter()
            .any(|bytes| {
                matches!(InboxRecord::decode(bytes), Ok(InboxRecord::Message(message))
            if message.from == Sender::Agent { path: child.clone() })
            })
    );
    Ok(())
}

#[tokio::test]
async fn waits_report_setup_failure_before_and_after_it_settles() -> Result {
    let (agents, _, root, model) = fixture(
        vec![
            spawn_call("w", &[]),
            call("wait", json!({"agents": ["w"]})),
            ScriptedTurn::Text("observed failure".into()),
            call("wait", json!({"agents": ["w"]})),
            ScriptedTurn::Text("observed released child".into()),
        ],
        1,
    )
    .await?;
    let state = deliver(&agents, &root, requested("start", &root)).await?;
    assert!(matches!(state.phase, Phase::Waiting { .. }));
    let failed = named(&agents, &root, "w").await?;
    agents.drain(&failed).await?;
    agents.drain(&root).await?;
    deliver(&agents, &root, requested("after-failure", &root)).await?;
    let requests = model.requests();
    let results: Vec<_> = requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Json { value },
                ..
            } if name == "wait" => Some(value),
            _ => None,
        })
        .collect();
    assert!(results.len() >= 2);
    for result in results {
        assert_eq!(result["settled"][0]["agent"], failed.as_str());
        assert!(result["settled"][0]["phase"]["done"]["outcome"]["failed"].is_object());
    }
    Ok(())
}

#[tokio::test]
async fn atomic_spawn_retries_setup_and_preserves_references_and_sibling_grants() -> Result {
    let text = || ScriptedTurn::Text("ready".into());
    let (agents, stream, root, model) = fixture(
        with_retrospectives([
            spawn_call("w", &[]),
            spawn_call("s", &["w"]),
            text(),
            spawn_call("w", &[]),
            text(),
            text(),
            call("wait", json!({"agents": ["w"]})),
            call("send", json!({"agent": "w", "message": "parent message"})),
            text(),
            text(),
            call("send", json!({"agent": "w", "message": "sibling message"})),
            text(),
            text(),
            call("archive", json!({"agent": "w"})),
            text(),
        ])
        .collect(),
        1,
    )
    .await?;
    deliver(&agents, &root, requested("first", &root)).await?;
    let failed = named(&agents, &root, "w").await?;
    let sibling = named(&agents, &root, "s").await?;
    // This grant predates the failure and survives the sibling's new identity.
    assert!(
        agents
            .state(&sibling)
            .await?
            .profile
            .unwrap()
            .peers
            .contains("agent.send-sibling:w")
    );
    let counted = Arc::new(Counted::new((*stream).clone()));
    // Newly adopted and checkpointed live agents both cost one head, no data IO.
    for name in ["w", "absent"] {
        let before = counted.requests();
        resolve_spawn_name(&counted, &within(), &root, name).await?;
        let reads = counted.requests().since(before);
        assert_eq!(reads.class(RequestClass::Head), 1);
        assert_eq!(reads.control(), 1);
        assert_eq!(reads.data(), 0);
    }
    agents.drain(&failed).await?;
    let state = agents.state(&failed).await?;
    assert!(state.released());
    assert_eq!(state.conversation_tail, state.conversation_base);
    // Negative control: the old pure derivation still finds the occupied inbox
    // and its admission refuses the retry with exactly the old name-exists error.
    let old = TurnEffect::child(&root, &intent("spawn:w"))?;
    assert_eq!(old, failed);
    let old_admission = match inbox_tail(&*stream, &old).await {
        Ok(_) => Err(acyclic_harness_core::Error::Conflict(
            "an agent with this name exists".into(),
        )),
        Err(error) => Ok(error),
    };
    assert!(
        matches!(old_admission, Err(acyclic_harness_core::Error::Conflict(ref reason)) if reason == "an agent with this name exists")
    );
    // ChildDone wakes the parent, whose scripted next turn retries the same name.
    agents.drain(&root).await?;
    let retry = named(&agents, &root, "w").await?;
    assert_ne!(retry, failed);
    assert_eq!(
        agents.resolve_spawn_name(&root, "w").await?,
        SpawnResolution::Live(1, retry.clone())
    );
    agents.drain(&retry).await?;
    assert_eq!(agents.state(&retry).await?.phase, Phase::Idle);
    assert!(!agents.state(&retry).await?.released());
    deliver(&agents, &root, requested("references", &root)).await?;
    assert!(
        model
            .requests()
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .any(|part| matches!(part,
            ModelContentPart::ToolResult { name, content: ToolResultContent::Json { value }, .. }
            if name == "wait" && value["settled"][0]["agent"] == retry.as_str()))
    );
    agents.drain(&retry).await?;
    agents.drain(&sibling).await?;
    agents.drain(&retry).await?;
    let messages = super::super::values(&*stream, retry.inbox()?)
        .await?
        .into_iter()
        .map(|bytes| InboxRecord::decode(&bytes))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(messages.iter().any(|record| matches!(record, InboxRecord::Message(message) if message.from == Sender::Agent { path: sibling.clone() })));
    let before = counted.requests();
    assert_eq!(
        resolve_spawn_name(&counted, &within(), &root, "s")
            .await?
            .live(),
        Some(sibling)
    );
    let reads = counted.requests().since(before);
    assert_eq!(reads.class(RequestClass::Head), 1);
    assert_eq!(reads.control(), 1);
    assert_eq!(reads.data(), 0);
    deliver(&agents, &root, requested("archive", &root)).await?;
    agents.drain(&retry).await?;
    assert_eq!(
        agents.state(&retry).await?.phase,
        Phase::Done {
            outcome: AgentOutcome::Archived
        }
    );
    assert_eq!(
        agents.resolve_spawn_name(&root, "w").await?.live(),
        Some(retry)
    );
    Ok(())
}

#[tokio::test]
async fn spawn_name_resolver_follows_a_six_deep_chain() -> Result {
    let mut turns = Vec::new();
    for _ in 0..6 {
        turns.extend([spawn_call("w", &[]), ScriptedTurn::Text("delegated".into())]);
    }
    turns.push(ScriptedTurn::Text("leaf".into()));
    let (agents, _, root, _) = fixture(turns, 0).await?;
    deliver(&agents, &root, requested("start", &root)).await?;
    let mut parent = root;
    for _ in 0..6 {
        let child = named(&agents, &parent, "w").await?;
        agents.drain(&child).await?;
        assert_eq!(agents.state(&child).await?.phase, Phase::Idle);
        parent = child;
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn resolver_never_skips_a_live_generation(failures in 0usize..4, fails_after_turn in proptest::bool::ANY) {
        let runtime = acyclic_native_runtime::testing::seeded_runtime(81).unwrap();
        runtime.block_on(async {
            let mut turns = Vec::new();
            for _ in 0..=failures {
                turns.extend([spawn_call("w", &[]), ScriptedTurn::Text("spawned".into())]);
            }
            turns.push(ScriptedTurn::Text("committed turn".into()));
            turns.push(ScriptedTurn::Events(vec![ScriptedEvent::Interrupted { error: "failure after work".into() }]));
            let (agents, stream, root, _) = fixture(turns, failures).await?;
            deliver(&agents, &root, requested("start", &root)).await?;
            for _ in 0..failures {
                let failed = named(&agents, &root, "w").await?;
                agents.drain(&failed).await?;
                assert!(agents.state(&failed).await?.released());
                agents.drain(&root).await?;
            }
            let live = named(&agents, &root, "w").await?;
            agents.drain(&live).await?;
            if fails_after_turn {
                deliver(&agents, &live, requested("fail-after-work", &live)).await?;
                assert_eq!(agents.state(&live).await?.phase, Phase::Idle);
                let entries = super::super::values(&*stream, live.conversation()?).await?;
                assert!(matches!(ConversationEntry::decode(entries.last().ok_or("missing failed turn")?)?.outcome, TurnOutcome::Failed { message } if message.contains("failure after work")));
            }
            assert!(!agents.state(&live).await?.released());
            let counted = Arc::new(Counted::new((*stream).clone()));
            assert_eq!(resolve_spawn_name(&counted, &within(), &root, "w").await?, SpawnResolution::Live(u64::try_from(failures)?, live));
            assert_eq!(counted.requests().class(RequestClass::Head), u64::try_from(failures)? + 1);
            assert_eq!(counted.requests().data(), 0);
            Ok::<_, Box<dyn std::error::Error>>(())
        }).unwrap();
    }
}

#[tokio::test]
async fn explanation_template_restricts_definitions_and_executable_authority() -> Result {
    let root = AgentPath::root(&id(81));
    let (agents, stream, _, model) = fixture(
        vec![
            call(
                "spawn",
                json!({"name":"why", "task":"Explain the current trace", "role":"explanation"}),
            ),
            ScriptedTurn::Text("explanation spawned".into()),
            // A provider can emit a call absent from its definitions. Execution
            // must independently reject it, rather than trusting model behaviour.
            call("archive", json!({"agent":root.as_str()})),
            ScriptedTurn::Text("explanation complete".into()),
        ],
        0,
    )
    .await?;
    deliver(&agents, &root, requested("explain", &root)).await?;
    let explanation = named(&agents, &root, "why").await?;
    agents.drain(&explanation).await?;
    assert!(
        !agents.state(&root).await?.released(),
        "explanation executed archive"
    );
    let requests = model.requests();
    assert!(requests[0].tools.iter().any(|tool| tool.name == "archive"));
    for request in requests.iter().skip(2) {
        assert!(
            request
                .tools
                .iter()
                .all(|tool| matches!(tool.name.as_str(), "send" | "wait"))
        );
        assert!(request.tools.iter().any(|tool| tool.name == "send"));
    }
    let entries = super::super::values(&*stream, explanation.conversation()?).await?;
    assert!(!entries.is_empty());
    assert!(
        requests
            .iter()
            .skip(3)
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .any(|part| matches!(part,
            ModelContentPart::ToolResult { name, content: ToolResultContent::Error { .. }, .. } if name == "archive"))
    );
    Ok(())
}

#[tokio::test]
async fn child_answer_wakes_parent_through_inbox_follow_without_wait() -> Result {
    use acyclic_stream::{FollowRequest, records_only};
    use futures::TryStreamExt as _;

    for failed in [false, true] {
        let (agents, stream, root, model) = fixture(
            with_retrospectives([
                spawn_call("reviewer", &[]),
                ScriptedTurn::Text("delegated".into()),
            ])
            .collect(),
            0,
        )
        .await?;
        deliver(&agents, &root, requested("delegate", &root)).await?;
        let child = named(&agents, &root, "reviewer").await?;
        let before = inbox_tail(&*stream, &root).await?;
        let mut events = records_only(
            stream
                .follow(FollowRequest::new(root.inbox()?, before))
                .await?,
        );
        model.extend(if failed {
            vec![ScriptedTurn::Events(vec![ScriptedEvent::Interrupted {
                error: "review provider failed".into(),
            }])]
        } else {
            with_retrospectives([ScriptedTurn::Text("Review complete".into())]).collect()
        });
        agents.drain(&child).await?;
        assert_eq!(agents.state(&child).await?.phase, Phase::Idle);
        let event = events.try_next().await?.ok_or("completion follow ended")?;
        let InboxRecord::Notice {
            id,
            notice: NoticePayload::Waking(file),
        } = InboxRecord::decode(&event.value)?
        else {
            return Err("child answer did not publish a waking primitive event".into());
        };
        assert!(id.as_str().starts_with("child-settled:"));
        assert_eq!(file.descriptor().media_type(), NOTICE_MEDIA_TYPE);
        let after = inbox_tail(&*stream, &root).await?;
        assert_eq!(after, before + 1);
        agents.drain(&child).await?;
        assert_eq!(
            inbox_tail(&*stream, &root).await?,
            after,
            "replay duplicated completion"
        );

        model.extend(with_retrospectives([ScriptedTurn::Text(
            "Observed reviewer completion".into(),
        )]));
        agents.drain(&root).await?;
        let entry = answer(&*stream, &child, 0).await?;
        let requests = model.requests();
        let notice = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::Notices { notices } => Some(notices),
                _ => None,
            })
            .flatten()
            .find(|notice| notice.kind == "child.settled")
            .ok_or("parent model did not receive completion")?;
        assert_eq!(notice.source, child.as_str());
        assert_eq!(notice.body["output"], serde_json::to_value(&entry.output)?);
        assert_eq!(
            notice.body["outcome"],
            serde_json::to_value(&entry.outcome)?
        );
        assert_eq!(matches!(entry.outcome, TurnOutcome::Failed { .. }), failed);
        assert_eq!(notice.body["branch"], child.as_str());
        assert!(
            !agents.state(&child).await?.closed,
            "completion closed the child"
        );
    }
    Ok(())
}

#[tokio::test]
async fn progress_send_reaches_waiting_parent_and_survives_settlement() -> Result {
    let (agents, stream, root, model) = fixture(
        with_retrospectives([
            spawn_call("worker", &[]),
            ScriptedTurn::Text("delegated".into()),
        ])
        .collect(),
        0,
    )
    .await?;
    deliver(&agents, &root, requested("delegate-progress", &root)).await?;
    let child = named(&agents, &root, "worker").await?;
    model.extend([call(
        "wait",
        json!({"agents": ["worker"], "need": 1, "input": false}),
    )]);
    let waiting = deliver(&agents, &root, requested("await-worker", &root)).await?;
    assert!(matches!(waiting.phase, Phase::Waiting { input: false, .. }));
    let before = inbox_tail(&*stream, &root).await?;
    model.extend([
        call(
            "send",
            json!({"agent": root.as_str(), "message": "Progress: review is in progress"}),
        ),
        ScriptedTurn::Text("work completed".into()),
        ScriptedTurn::Text("none".into()),
    ]);
    agents.drain(&child).await?;
    // Delivery reaches the parent before the child completion event, even
    // though the parent's model is parked in a settlement-only wait.
    let inbox = super::super::values(&*stream, root.inbox()?).await?;
    let mut records = inbox
        .iter()
        .skip(before as usize)
        .map(|bytes| InboxRecord::decode(bytes));
    let InboxRecord::Message(message) = records.next().ok_or("missing progress delivery")?? else {
        return Err("progress did not precede completion".into());
    };
    assert_eq!(
        message.from,
        Sender::Agent {
            path: child.clone()
        }
    );
    assert!(records.any(|record| matches!(
        record,
        Ok(InboxRecord::Notice {
            notice: NoticePayload::Waking(_),
            ..
        })
    )));
    model.extend(with_retrospectives([ScriptedTurn::Text(
        "progress and completion observed".into(),
    )]));
    agents.drain(&root).await?;
    let requests = model.requests();
    let notices: Vec<_> = requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::Notices { notices } => Some(notices),
            _ => None,
        })
        .flatten()
        .collect();
    assert!(notices.iter().any(|notice| notice.kind == "peer.message"
        && notice.source == child.as_str()
        && notice.body["id"] == message.id.as_str()));
    assert!(
        notices
            .iter()
            .any(|notice| notice.kind == "child.settled" && notice.source == child.as_str())
    );
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .any(|part| matches!(part,
            ModelContentPart::ToolResult { name, content: ToolResultContent::Json { value }, .. }
                if name == "wait" && value["settled"][0]["agent"] == child.as_str()))
    );
    Ok(())
}
