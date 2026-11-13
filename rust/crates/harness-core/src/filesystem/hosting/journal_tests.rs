//! Exercise a retained inbox cut against actual actor commits, without a
//! timing hook: the next input settles before the original query resumes.
use super::*;
use crate::{
    agent::{AgentId, Control, InboxRecord, Phase},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{PRIVATE_VOLUME, TurnAuthority},
};
use acyclic_stream::MemoryStream;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Binding = Bound<MemoryStream, acyclic_fs::LogicalObjectStore<acyclic_objects::MemoryObjects>>;

struct NoTurns;
impl TurnExecutors for NoTurns {
    fn executor<'a>(
        &'a self,
        _: &'a crate::agent::AgentState,
        _: OperationId,
        _: &'a crate::model::ModelContent,
        _: &'a TurnAuthority,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Arc<dyn crate::executor::Executor>>>
    {
        Box::pin(async { Err(crate::Error::Unsupported("control-only fixture".into())) })
    }
}

async fn binding() -> TestResult<(Binding, AgentPath)> {
    binding_with(Arc::new(NoTurns), Default::default()).await
}

pub(in crate::filesystem) async fn binding_with(
    executors: Arc<dyn TurnExecutors>,
    approval_policy: crate::approvals::ApprovalPolicy,
) -> TestResult<(Binding, AgentPath)> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let stream = Arc::new(MemoryStream::default());
    let provider = ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "fixture",
            [7; 32],
            Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        Placement {
            filesystem: acyclic_fs::Fs::new(
                StreamAuthorityStore::new(stream),
                acyclic_fs::LogicalObjectStore::new(Arc::new(
                    acyclic_objects::MemoryObjects::memory(),
                )),
                acyclic_fs::EmbeddedCapabilities::MEMORY,
            ),
            provider: provider.clone(),
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        Turns {
            executors,
            limits: TurnLimits {
                maximum_bytes: Some(65_536),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            approval_policy,
        },
    )?;
    let id = AgentId::from_bytes([1; 16]);
    let RootSubmission::Created(root) = bound
        .create_root(
            &IntentId::try_from("root")?,
            RootRequest {
                peer: true,
                agent: id.clone(),
                fresh: vec![WorkspaceEntry::initial_volume(
                    crate::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    VolumeRef::new(
                        provider,
                        "private".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(crate::AgentId::from_bytes(id.to_bytes())),
                        &mut None,
                    )?,
                )],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
    else {
        return Err("uncertain root".into());
    };
    bound.agents().drain(&root).await?;
    Ok((bound, root))
}

#[tokio::test]
async fn independent_review_uses_selected_model_without_opening_worker_context() -> TestResult {
    use crate::{
        approvals::{ApprovalRequest, RuleVerdict},
        conversation::Limits,
        filesystem::{StockExecutors, TurnContext},
        model::{Model, ModelContent, ModelProvider, ModelResolver},
        test_support::{ScriptedModelProvider, ScriptedTurn},
        tool::ToolRegistry,
    };
    struct NoWorkerContext;
    impl TurnContext for NoWorkerContext {
        fn prepare<'a>(
            &'a self,
            _: (crate::executor::StockExecutor, Limits),
            _: &'a crate::agent::AgentState,
            _: OperationId,
            _: &'a ModelContent,
            _: &'a TurnAuthority,
        ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<crate::executor::StockExecutor>>
        {
            Box::pin(async {
                Err(crate::Error::Invalid(
                    "worker context must stay unopened".into(),
                ))
            })
        }
    }
    let (bound, root) = binding().await?;
    let mut agent = bound.agents().state(&root).await?;
    agent.profile = Some(crate::agent::CreationConfig {
        question_plugin: false,
        topology: None,
        continuation: None,
        context: crate::agent::SpawnContext::TopLevel,
        template: None,
        peers: Default::default(),
        model: Some("test/parent-review".into()),
    });
    let reviewer = Arc::new(ScriptedModelProvider::new((0..2).map(|_| {
        ScriptedTurn::Text(r#"{"verdict":"ask","reason":"needs parent action"}"#.into())
    })));
    let selected = (
        Model::new("test".into(), "parent-review".into(), "1".into())?,
        {
            let provider: Arc<dyn ModelProvider> = reviewer.clone();
            provider
        },
    );
    let expected_model = selected.0.clone();
    let resolver = ModelResolver::new("test/worker", move |selection| {
        if selection != "test/parent-review" {
            return Err(crate::Error::Invalid(
                "unexpected review model selection".into(),
            ));
        }
        Ok(selected.clone())
    });
    let worker = Arc::new(ScriptedModelProvider::new([]));
    let executors = StockExecutors::new(
        (
            Model::new("test".into(), "worker".into(), "1".into())?,
            worker.clone(),
        ),
        ToolRegistry::default(),
        (Limits::default(), None::<crate::context::CompactionPolicy>),
        Arc::new(NoWorkerContext),
    )
    .with_models(Arc::new(resolver));
    let storage = crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::from_bytes(agent.path.id_bytes()),
        1_000_000,
    )
    .await?;
    let journal = storage.journal();
    let request =
        ApprovalRequest::new("child-request", br#"{"action":"exact child action"}"#, 4096)?;
    assert!(matches!(
        executors
            .review(
                &agent,
                (journal.as_ref(), None),
                (&agent.path, &request),
                "parent:fixture",
                "TRUSTED_PARENT_POLICY"
            )
            .await,
        Err(crate::Error::Unauthorized(_))
    ));
    for source in [AgentId::from_bytes([31; 16]), AgentId::from_bytes([32; 16])] {
        let child = agent.path.child(&source)?;
        for _ in 0..2 {
            assert_eq!(
                executors
                    .review(
                        &agent,
                        (journal.as_ref(), None),
                        (&child, &request),
                        "parent:fixture",
                        "TRUSTED_PARENT_POLICY"
                    )
                    .await?,
                (RuleVerdict::Ask, Some("needs parent action".into()))
            );
        }
    }
    assert_eq!(worker.request_count(), 0);
    let child = agent.path.child(&AgentId::from_bytes([31; 16]))?;
    assert!(matches!(
        executors
            .review(
                &agent,
                (journal.as_ref(), None),
                (&child, &request),
                "parent:fixture",
                "DIFFERENT_PARENT_POLICY"
            )
            .await,
        Err(crate::Error::Conflict(_))
    ));
    let requests = reviewer.requests();
    assert_eq!(
        requests.len(),
        2,
        "child identities collided or replay dispatched another review"
    );
    assert_eq!(requests[0].model, expected_model);
    assert!(requests[0].tools.is_empty());
    assert!(requests[0].messages.iter().any(|message| {
        message.role == crate::model::ModelRole::System
            && message.content == ModelContent::Text("TRUSTED_PARENT_POLICY".into())
    }));
    assert!(requests[0].messages.iter().any(|message| {
        message.role == crate::model::ModelRole::User
            && message.content == ModelContent::Text(r#"{"action":"exact child action"}"#.into())
    }));
    Ok(())
}

#[tokio::test]
async fn tool_result_reads_keep_the_settled_journal_cut() -> TestResult {
    use crate::{agent::TurnHost, executor::ExecutionJournal};
    let (bound, root) = binding().await?;
    let state = bound.agents().state(&root).await?;
    let journal = bound.turns().query_journal(&state).await?;
    let operation = OperationId::from_bytes([51; 16]);
    let result = journal
        .stage(
            operation,
            "tool:read:result",
            b"original",
            "text/plain",
            None,
        )?
        .await?;
    let output = bound
        .turns()
        .open_control(
            &state,
            &crate::agent::ContextAction::Clear,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?
        .outputs
        .stage(operation, b"{}")
        .await?;
    assert_eq!(result.volume(), output.volume());
    assert_ne!(state.volumes[0].as_volume(), Some(output.volume()));
    let later = journal
        .stage(operation, "tool:later:result", b"later", "text/plain", None)?
        .await?;
    let (bytes, length) = bound
        .read_tool_output(&root, result.path(), Some(&output), 2, 3)
        .await?;
    assert_eq!(bytes, b"igi");
    assert_eq!(length, 8);
    assert!(
        bound
            .read_tool_output(&root, later.path(), Some(&output), 0, 32)
            .await
            .is_err()
    );
    assert!(
        bound
            .read_tool_output(&root, result.path(), None, 0, 32)
            .await
            .is_err()
    );
    assert_eq!(
        bound.tool_resource(&root).await?,
        bound.volumes().tool_resource(output.volume()).await?
    );
    Ok(())
}

#[tokio::test]
async fn workspace_wait_follows_branch_commits_and_checkout_changes() -> TestResult {
    use crate::conversation::{DirectoryQuery, DirectoryTarget};
    use futures::FutureExt as _;
    fn query(after: Option<&crate::resources::GenerationRef>) -> DirectoryQuery<'_> {
        DirectoryQuery {
            target: DirectoryTarget::Private {},
            path: "",
            generation: None,
            after_generation: after,
            after_name: None,
            maximum_entries: 8,
        }
    }
    eprintln!("branch-wait: creating hosted binding");
    let (bound, agent) = binding().await?;
    eprintln!("branch-wait: hosted binding created");
    let state = bound.agents().state(&agent).await?;
    let reference = super::super::super::bounded_io::content_workspace(
        &bound.volumes.provider,
        state.volumes[0].volume()?,
        None,
    )?;
    let root = bound.volumes.open(&reference, None).await?;
    let store = acyclic_fs::StreamGitCompatStore::new(StreamAuthorityStore::new(
        bound.volumes.filesystem.stream_provider(),
    ));
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    let cancellation = acyclic_fs::CancellationToken::new();
    graph
        .register_root(&root, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?;
    eprintln!("branch-wait: project root registered");
    let branch = graph
        .fork(
            &root,
            "watched-branch",
            acyclic_fs::IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    eprintln!("branch-wait: branch forked");
    let repository = acyclic_fs::GitCompatRepository::new(root.id(), store);
    repository
        .register_branch_workspace("watched-branch", branch.id(), None, true)
        .await?;
    eprintln!("branch-wait: branch selected");
    branch
        .write_text("/before-wait", "initial branch file")
        .await?;
    eprintln!("branch-wait: initial branch file published without a pending query");
    let selected = bound.directory_page(&agent, query(None)).await?;
    eprintln!("branch-wait: original branch directory selected");
    let waiting = bound.directory_page(&agent, query(Some(&selected.generation)));
    futures::pin_mut!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    eprintln!("branch-wait: directory pending; publishing branch file");
    branch
        .write_text("/branch-only", "published in the selected branch")
        .await?;
    eprintln!("branch-wait: branch publication completed");
    let changed = waiting
        .now_or_never()
        .ok_or("branch publication did not wake query")??;
    assert!(
        changed
            .entries
            .iter()
            .any(|entry| entry.name == "branch-only")
    );
    let waiting = bound.directory_page(&agent, query(Some(&changed.generation)));
    futures::pin_mut!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    // Switching back changes no volume's publication stream. Only the original
    // repository-state event can wake this already pending directory query.
    repository
        .register_branch_workspace("main", root.id(), None, true)
        .await?;
    let checked_out = waiting
        .now_or_never()
        .ok_or("checkout did not wake query")??;
    assert_ne!(checked_out.generation, changed.generation);
    assert!(
        !checked_out
            .entries
            .iter()
            .any(|entry| entry.name == "branch-only")
    );
    Ok(())
}

#[tokio::test]
async fn workspace_queries_keep_subdirectory_read_grants_without_widening_them() -> TestResult {
    use crate::{
        client_files::ClientFileChange,
        conversation::{DirectoryQuery, DirectoryTarget, VolumeOperation},
    };
    let (mut bound, root) = binding().await?;
    let generation = bound
        .publish_agent_files(
            &root,
            OperationId::from_bytes([73; 16]),
            &[
                ClientFileChange {
                    path: "ui/title".into(),
                    expected: None,
                    text: "scoped title".into(),
                },
                ClientFileChange {
                    path: "outside".into(),
                    expected: None,
                    text: "must not be disclosed".into(),
                },
            ],
        )
        .await?;
    let state = bound.agents().state(&root).await?;
    let private = state.volumes[0].volume()?;
    let journal = crate::conversation::journal_volume(private.provider().clone(), &root.id())?;
    let grants = crate::Capabilities::new([
        private.capability(VolumeOperation::Write, &mut None)?,
        private.directory_read_capability("ui", &mut None)?,
        journal.capability(VolumeOperation::Read, &mut None)?,
        journal.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = bound
        .issuer
        .issue(
            None,
            "ui-reader",
            &grants,
            Some(crate::AgentId::from_bytes(root.id_bytes())),
        )
        .await?;
    bound.admission = TurnAdmission::Admitted {
        audience: bound
            .issuer
            .native()
            .ok_or("native fixture custody")?
            .audience()
            .clone(),
        scope,
    };
    let query = |path| DirectoryQuery {
        target: DirectoryTarget::Private {},
        path,
        generation: Some(&generation),
        after_generation: None,
        after_name: None,
        maximum_entries: 4,
    };
    let page = bound.directory_page(&root, query("ui")).await?;
    assert_eq!(page.entries[0].name, "title");
    let (bytes, _) = bound
        .read_path(
            &root,
            DirectoryTarget::Private {},
            "ui/title",
            &generation,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 64,
            },
        )
        .await?;
    assert_eq!(bytes.as_slice(), b"scoped title");
    for path in ["", "outside", "ui/../outside"] {
        assert!(bound.directory_page(&root, query(path)).await.is_err());
        assert!(
            bound
                .read_path(
                    &root,
                    DirectoryTarget::Private {},
                    path,
                    &generation,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: 64,
                    },
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn call_output_uses_the_journal_without_granting_tools_its_writer() -> TestResult {
    use crate::{
        agent::TurnHost,
        runtime::{ContentBindings, RuntimeScope, ToolContext},
    };
    let (bound, root) = binding().await?;
    let state = bound.agents().state(&root).await?;
    let (scope, verifier) = bound.volume_scope(&root).await?;
    let private = state.volumes[0].volume()?;
    let writer = super::super::FilesystemContentPublisher::new(
        bound.volumes().clone(),
        private,
        &verifier,
        &scope,
        65_536,
        None,
    )?;
    let runtime = crate::runtime::Bindings {
        scope: RuntimeScope::new(
            scope.capabilities().clone(),
            crate::conversation::Limits::default(),
        )?,
        ..crate::runtime::Bindings::local()
    }
    .build()?;
    let turn = OperationId::from_bytes([53; 16]);
    let task = Arc::new(
        runtime
            .agent_context(
                &crate::Capabilities::default(),
                ContentBindings {
                    volume: private.clone(),
                    reader: bound.content_reader(&root).await?,
                    writer: Some(writer),
                },
            )?
            .with_execution_journal(Arc::new(bound.turns().query_journal(&state).await?), turn)?,
    );
    let context = ToolContext::new(task.clone(), turn, "shell")?;
    assert!(
        !task
            .scope()
            .grants()
            .contains(crate::contract::capability::HARNESS_HOST)
    );
    let file = context
        .stage_output_file_once("stdout/0", b"retained", "text/plain", "stdout")
        .await?;
    assert_eq!(
        file.volume(),
        &crate::conversation::journal_volume(private.provider().clone(), &root.id())?
    );
    assert!(
        task.stage_file(
            Some(file.volume()),
            OperationId::new(),
            "ui/title",
            b"forged",
            "text/plain",
            "forged"
        )
        .await
        .is_err()
    );
    assert!(
        context
            .stage_output_file_once("../ui/title", b"forged", "text/plain", "forged")
            .await
            .is_err()
    );
    assert_eq!(
        context.task().read_file(&file, None).await?.as_ref(),
        b"retained"
    );
    assert_eq!(bound.read_file(&root, &file, 0, 8).await?.0, b"retained");
    let mut repinned = file.copy(&mut None)?;
    repinned.pin_version(hex::encode(turn.into_bytes()), &mut None)?;
    let rejected = bound
        .read_file(&root, &repinned, 0, 8)
        .await
        .expect_err("operation identity is not a filesystem generation");
    assert!(matches!(rejected, Error::Turn(cause)
        if matches!(&*cause, crate::Error::Invalid(reason)
            if reason == "file version is not a canonical Filesystem generation")));
    // A later model call reads the advertised address through the ordinary
    // path/range/directory readers, without receiving the host's publisher.
    let reader = ToolContext::new(task.clone(), OperationId::new(), "read")?;
    let address = format!("volume:///output/{}", file.path());
    let (selected, bytes, _) = reader
        .read_tool_path_range(
            &address,
            None,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 3,
            },
            None,
        )
        .await?;
    assert_eq!(bytes.as_ref(), b"ret");
    assert_eq!(selected.volume, *file.volume());
    let parent = file
        .path()
        .rsplit_once('/')
        .ok_or("missing output parent")?
        .0;
    let directory = format!("volume:///output/{parent}");
    let (volume, path) = reader.authorize_tool_directory_read(&directory, None)?;
    let (page, _) = reader
        .list_tool_directory(
            crate::conversation::DirectoryRead {
                volume,
                granted_prefix: &path,
                path: &path,
                generation: Some(&selected.generation),
                after: None,
                maximum_entries: 8,
            },
            None,
        )
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "0");
    assert!(
        reader
            .stage_tool_file_once(
                OperationId::new(),
                &address,
                b"forged",
                "text/plain",
                "forged",
                None,
            )
            .await
            .is_err()
    );
    let hidden_input = bound
        .stage_text(
            &root,
            "message",
            &Sender::Host {
                host: "hidden".into(),
            },
            &IntentId::try_from("hidden")?,
            ("hidden", "host-only input"),
        )
        .await?;
    assert!(reader.read_file(&hidden_input, None).await.is_err());
    let denied = bound
        .read_file(&root, &hidden_input, 0, 8)
        .await
        .expect_err("uncommitted input must not acquire a generic file grant");
    assert!(matches!(denied, Error::Turn(cause)
        if cause.code() == crate::wire::ErrorCode::Unauthorized));
    for hidden in [hidden_input.path(), ".system/effects/hidden"] {
        assert!(
            reader
                .read_tool_path(&format!("volume:///output/{hidden}"), None, None)
                .await
                .is_err()
        );
    }
    assert!(
        reader
            .authorize_tool_directory_read("volume:///output/", None)
            .is_err()
    );
    // Private UI remains independently writable.
    reader
        .stage_tool_file_once(
            OperationId::new(),
            "volume:///private/ui/title",
            b"title",
            "text/plain",
            "title",
            None,
        )
        .await?;
    let replay = context
        .read_output_file_once("stdout/0")
        .await?
        .ok_or("missing original output")?;
    assert_eq!(replay.0, file);
    assert_eq!(replay.1.as_ref(), b"retained");
    let mut source = futures::io::Cursor::new(b"streamed".as_slice());
    let spill = context
        .stage_output_stream_once("spill.log", &mut source, ("text/plain", "spill"))
        .await?;
    assert_eq!(spill.volume(), file.volume());
    let (_, bytes, _) = reader
        .read_tool_path_range(
            &address,
            Some(&selected.generation),
            acyclic_fs::ByteRange {
                offset: 3,
                length: 5,
            },
            None,
        )
        .await?;
    assert_eq!(bytes.as_ref(), b"ained");
    assert_eq!(
        reader
            .read_tool_path(&format!("volume:///output/{}", spill.path()), None, None)
            .await?
            .1
            .as_ref(),
        b"streamed"
    );
    let output = bound
        .turns()
        .open_control(
            &state,
            &crate::agent::ContextAction::Clear,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?
        .outputs
        .stage(turn, b"{}")
        .await?;
    assert_eq!(
        bound
            .read_tool_output(&root, file.path(), Some(&output), 0, 32)
            .await?
            .0,
        b"retained"
    );
    assert_eq!(
        bound
            .read_tool_output(&root, spill.path(), Some(&output), 0, 32)
            .await?
            .0,
        b"streamed"
    );
    Ok(())
}

#[tokio::test]
async fn input_previews_pin_pending_messages_without_reading_the_whole_body() -> TestResult {
    use crate::agent::{InputPreview, InputPreviewStep, Message};
    use futures::StreamExt as _;
    let (bound, root) = binding().await?;
    let by = Sender::Host {
        host: "preview".into(),
    };
    let first = IntentId::try_from("first")?;
    let text = format!("a🙂{}", "unread".repeat(4096));
    let file = bound
        .stage_text(&root, "message", &by, &first, ("preview", &text))
        .await?;
    assert_eq!(
        file.volume(),
        &crate::conversation::journal_volume(bound.volumes().provider.clone(), &root.id(),)?
    );
    assert!(bound.read_file(&root, &file, 0, 3).await.is_err());
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Message(Message {
                from: by.clone(),
                id: first,
                re: None,
                body: Box::new(file),
                attachments: None,
            }),
        )
        .await?;
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Cancel(crate::agent::Cancellation {
                conversation: 0,
                input: 1,
                by: by.clone(),
                id: IntentId::try_from("filtered")?,
            }),
        )
        .await?;
    let original = bound.input_preview_steps(root.clone(), 3, crate::agent::InputSelection::Latest);
    futures::pin_mut!(original);
    assert_eq!(
        original.next().await.transpose()?,
        Some(InputPreviewStep::Progress)
    );
    // A newer accepted prompt must not change the cut retained by the query.
    let later = IntentId::try_from("later")?;
    let file = bound
        .stage_text(&root, "message", &by, &later, ("preview", "newer"))
        .await?;
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::FollowUp(Message {
                from: by.clone(),
                id: later,
                re: None,
                body: Box::new(file),
                attachments: None,
            }),
        )
        .await?;
    assert_eq!(
        original.next().await.transpose()?,
        Some(InputPreviewStep::Selected(Some(InputPreview {
            text: "a".into(),
            bytes: text.len() as u64,
        })))
    );
    assert!(original.next().await.is_none());
    let fresh = bound.input_preview_steps(root, 5, crate::agent::InputSelection::Latest);
    futures::pin_mut!(fresh);
    assert_eq!(
        fresh.next().await.transpose()?,
        Some(InputPreviewStep::Selected(Some(InputPreview {
            text: "newer".into(),
            bytes: 5,
        })))
    );
    Ok(())
}

#[tokio::test]
async fn settled_arrivals_and_archive_do_not_end_a_retained_inbox_cut_early() -> TestResult {
    for archive in [false, true] {
        let (bound, root) = binding().await?;
        let agents = bound.agents();
        let observed = agents.state(&root).await?;
        let cut = crate::agent::inbox_tail(&**agents.provider(), &root).await?;
        let by = Sender::Host {
            host: "query-race".into(),
        };
        assert!(matches!(agents.send(&root, &by, &InboxRecord::Notice {
                id: IntentId::try_from("arrived")?,
                notice: crate::agent::NoticePayload::Quiet(crate::conversation::wire::Notice {
                    source: "test".into(), kind: "test.control".into(),
                    body: serde_json::Value::Null, wake: false, coalesce_key: None,
                }),
            }).await?, Delivered::Committed { inbox } if inbox == cut));
        agents.drain(&root).await?;
        let progress = agents.input_progress(&root, cut).await?;
        assert!(progress.settled);
        assert!(!progress.closed);
        assert!(matches!(agents.state(&root).await?.phase, Phase::Idle));
        if archive {
            agents
                .archive(&root, &by, &IntentId::try_from("archive")?)
                .await?;
            agents.drain(&root).await?;
        }
        // Resume the actual query after both the new input and its actor step.
        // The archive case must re-read the tail after observing closure.
        let (input, tail, _, closed, _) = bound
            .journal_from_input::<Usage>(&observed, cut, cut, 0, 0, (true, Some(0)))
            .await?;
        assert_eq!(input, cut);
        assert!(
            tail > cut,
            "the settled arrival was lost behind the captured tail"
        );
        assert!(
            !closed,
            "settlement was mistaken for an empty closed producer"
        );
        let (_, page, settled, _) = bound.usage_page(&root, cut, 0, false, None).await?;
        assert!(page.is_some() && settled);
        let (_, _, page, settled) = bound.event_page(&root, cut, 0, false).await?;
        assert!(page.is_some() && settled);
        if archive {
            let tail = crate::agent::inbox_tail(&**agents.provider(), &root).await?;
            let (_, page, closed, _) = bound.usage_page(&root, tail, 0, true, Some(0)).await?;
            assert!(page.is_none() && closed);
        }
    }
    Ok(())
}

#[tokio::test]
async fn prompt_history_streams_original_human_messages_in_bounded_ranges() -> TestResult {
    use crate::agent::{InputPreviewStep, InputSelection, Message};
    use crate::model::{ModelContent, ModelContentPart};
    use futures::StreamExt as _;
    let (bound, root) = binding().await?;
    let structured_text = "structured🙂\n\"\\\0".repeat(64);
    for (name, from, content) in [
        (
            "first",
            Sender::User {
                principal: "owner".into(),
            },
            ModelContent::Text("same🙂".into()),
        ),
        (
            "host",
            Sender::Host {
                host: "host".into(),
            },
            ModelContent::Text("host-only".into()),
        ),
        (
            "structured",
            Sender::User {
                principal: "owner".into(),
            },
            ModelContent::Parts(vec![
                ModelContentPart::text(structured_text.clone()),
                ModelContentPart::text("content".into()),
            ]),
        ),
        (
            "second",
            Sender::User {
                principal: "owner".into(),
            },
            ModelContent::Text("same🙂".into()),
        ),
    ] {
        let intent = IntentId::try_from(name)?;
        let body = match &content {
            ModelContent::Text(text) => {
                bound
                    .stage_text(&root, "message", &from, &intent, ("prompt", text))
                    .await?
            }
            _ => bound.stage_prompt(&root, &from, &intent, &content).await?,
        };
        bound
            .agents()
            .send(
                &root,
                &from,
                &InboxRecord::Message(Message {
                    from: from.clone(),
                    id: intent,
                    body: Box::new(body),
                    re: None,
                    attachments: None,
                }),
            )
            .await?;
        if name == "structured" {
            for (maximum, expected) in [(0, ""), (1, "s"), (4, "stru"), (11, "structured")] {
                let mut source =
                    bound.input_preview_steps(root.clone(), maximum, InputSelection::Latest);
                let mut work = 0;
                let preview = loop {
                    let step = source
                        .next()
                        .await
                        .transpose()?
                        .ok_or("structured preview ended before selection")?;
                    match step {
                        InputPreviewStep::Progress => work += 1,
                        InputPreviewStep::Selected(Some(preview)) => {
                            break preview;
                        }
                        _ => panic!("latest structured preview returned a history range"),
                    }
                };
                assert!(work > 1, "structured source work did not yield credit");
                assert_eq!(preview.text, expected);
                assert_eq!(preview.bytes, structured_text.len() as u64 + 7);
            }
        }
    }
    let source = bound.input_preview_steps(root, 4, InputSelection::History);
    futures::pin_mut!(source);
    let mut messages: Vec<(u64, String)> = Vec::new();
    let mut progress = 0;
    while let Some(step) = source.next().await.transpose()? {
        match step {
            InputPreviewStep::Progress => progress += 1,
            InputPreviewStep::MessageRange {
                inbox,
                offset,
                preview,
            } => {
                assert!(preview.text.len() <= 4);
                if offset == 0 {
                    if messages.len() == 1 {
                        assert!(progress > 1, "structured source was not credited in chunks");
                    }
                    messages.push((inbox, String::new()));
                }
                progress = 0;
                let (selected, text) = messages.last_mut().expect("range has an original message");
                assert_eq!(*selected, inbox);
                assert_eq!(text.len() as u64, offset);
                text.push_str(&preview.text);
                assert!(text.len() as u64 <= preview.bytes);
            }
            InputPreviewStep::Selected(None) => break,
            InputPreviewStep::Selected(Some(_)) => panic!("history became a latest-input prefix"),
        }
    }
    assert_eq!(messages.len(), 3);
    assert!(messages[0].0 > messages[1].0);
    assert_eq!(messages[0].1, "same🙂");
    assert_eq!(messages[1].1, format!("{structured_text}content"));
    assert_eq!(messages[2].1, "same🙂");
    Ok(())
}
