//! Exercise a retained inbox cut against actual actor commits, without a
//! timing hook: the next input settles before the original query resumes.
use super::*;
use crate::{
    agent::{AgentId, InboxRecord, Phase},
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
            executors: Arc::new(NoTurns),
            limits: TurnLimits {
                maximum_bytes: Some(65_536),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            approval_policy: Default::default(),
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
async fn waking_notices_steer_without_granting_their_journal_files() -> TestResult {
    use crate::filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, agent_internal::AgentInternalFiles,
        agent_steering::FilesystemBoundary, agent_turns::AdmittedAgent,
    };
    use crate::{
        agent::{Message, ModelBoundary, NoticePayload},
        conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation},
        model::{ModelContent, ModelContentPart, ModelRole},
    };

    let (bound, root) = binding().await?;
    let state = bound.agents().state(&root).await?;
    let by = Sender::Host {
        host: "steering".into(),
    };
    let id = IntentId::try_from("initial")?;
    let body = bound
        .stage_text(&root, "message", &by, &id, ("initial", "prompt"))
        .await?;
    let Delivered::Committed { inbox } = bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Message(Message {
                from: by.clone(),
                id,
                re: None,
                body: Box::new(body),
                attachments: None,
            }),
        )
        .await?
    else {
        return Err("uncertain initial input".into());
    };
    let (journal, host) = bound.turns().journal_authority(&state).await?;
    let (scope, verifier) = bound.volume_scope(&root).await?;
    let private = state.volumes[0].volume()?;
    let files = Arc::new(AgentInternalFiles::new(
        Arc::clone(bound.volumes()),
        journal.clone(),
        ContentGrant::verify(
            &host.verifier,
            &host.scope,
            &journal,
            VolumeOperation::Write,
            &mut None,
        )?,
        private.clone(),
        ContentGrant::verify(
            &verifier,
            &scope,
            private,
            VolumeOperation::Write,
            &mut None,
        )?,
    ));
    let journal_reader = FilesystemContentVerifier::new(
        Arc::clone(bound.volumes()),
        &host.verifier,
        &host.scope,
        crate::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    let writer = FilesystemContentPublisher::new(
        Arc::clone(bound.volumes()),
        private,
        &verifier,
        &scope,
        crate::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    let operation = OperationId::from_bytes([64; 16]);
    let initial_through = acyclic_stream::StreamProviderExt::tail(
        bound.agents().provider().as_ref(),
        state.path.inbox()?,
    )
    .await?;
    let boundary = FilesystemBoundary::new(
        Arc::clone(bound.agents().provider()),
        Arc::clone(bound.volumes()),
        bound.issuer().clone(),
        bound.admission().clone(),
        state.clone(),
        (operation, inbox, initial_through),
        files,
        journal_reader,
        writer,
        None,
    );
    assert!(boundary.messages(operation, 0, None).await?.is_empty());
    boundary.admitted(operation, 0).await?;

    let notice = crate::conversation::wire::Notice {
        source: "records".into(),
        kind: "record.changed".into(),
        body: serde_json::json!({"revision": 1}),
        wake: true,
        coalesce_key: None,
    };
    let record = bound
        .stage_notice(&root, &by, &IntentId::try_from("changed")?, &notice)
        .await?;
    let InboxRecord::Notice {
        notice: NoticePayload::Waking(file),
        ..
    } = &record
    else {
        return Err("waking notice was not staged".into());
    };
    let file = file.as_ref().clone();
    let external = [file.clone()];
    let guest = AdmittedAgent::verify(bound.issuer(), bound.admission(), &root, &state.volumes)?
        .authority(&external)
        .await?;
    let guest_reader = FilesystemContentVerifier::new(
        Arc::clone(bound.volumes()),
        &guest.verifier,
        &guest.scope,
        crate::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    let denied = async {
        guest_reader
            .read(&file, file.descriptor().byte_length(), None, None)?
            .await
    }
    .await
    .expect_err("an exact input grant exposed the host notice file");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    bound.agents().send(&root, &by, &record).await?;
    let messages = boundary.messages(operation, 1, None).await?;
    let [message] = messages.as_slice() else {
        return Err("missing waking notice".into());
    };
    assert_eq!(message.role, ModelRole::User);
    let ModelContent::Part(ModelContentPart::Notices { notices }) = &message.content else {
        return Err("waking notice was not decoded".into());
    };
    assert_eq!(notices.as_slice(), &[notice]);
    boundary.admitted(operation, 1).await?;

    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Message(Message {
                from: by.clone(),
                id: IntentId::try_from("ordinary")?,
                re: None,
                body: Box::new(file),
                attachments: None,
            }),
        )
        .await?;
    let denied = boundary
        .messages(operation, 2, None)
        .await
        .expect_err("an ordinary message acquired the host notice reader");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
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
                    text: Some("scoped title".into()),
                },
                ClientFileChange {
                    path: "outside".into(),
                    expected: None,
                    text: Some("must not be disclosed".into()),
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
    let mut activity_context = context.clone();
    activity_context.output_activity = true;
    let mut log = crate::runtime::ToolOutputLog::new(activity_context.clone());
    let chunk = log.publish_chunk("stderr/0", b"observed").await?;
    assert_eq!(chunk.volume(), file.volume());
    let activity_path = crate::runtime::tool_output_activity_path(turn, context.operation_id());
    let (_, bytes, _) = reader
        .read_tool_path(&format!("volume:///output/{activity_path}"), None, None)
        .await?;
    let activity: crate::runtime::ToolOutputActivity = serde_json::from_slice(&bytes)?;
    assert_eq!(activity.through, 1);
    assert_eq!(log.publish_chunk("stderr/0", b"observed").await?, chunk);
    let second = log.publish_chunk("stderr/1", b"next").await?;
    assert_eq!(log.publish_chunk("stderr/0", b"observed").await?, chunk);
    let third = log.publish_chunk("stderr/2", b"last").await?;
    let mut recovered = crate::runtime::ToolOutputLog::new(activity_context.clone());
    assert_eq!(
        recovered.publish_chunk("stderr/0", b"observed").await?,
        chunk
    );
    assert_eq!(recovered.publish_chunk("stderr/1", b"next").await?, second);
    assert_eq!(recovered.publish_chunk("stderr/2", b"last").await?, third);
    let (_, bytes, _) = reader
        .read_tool_path(&format!("volume:///output/{activity_path}"), None, None)
        .await?;
    let activity: crate::runtime::ToolOutputActivity = serde_json::from_slice(&bytes)?;
    assert_eq!(
        activity.through, 3,
        "chunk replay changed the activity cursor"
    );
    assert!(log.publish_chunk("stderr/0", b"replacement").await.is_err());
    use crate::filesystem::native_process::capture::finish_output;
    use crate::filesystem::{NativeProcessResult, NativeProcessStop, NativeProcessStopKind};
    let mut capture = NativeProcessResult {
        success: false,
        exit_code: None,
        stdout: Vec::new(),
        stderr: Vec::new(),
        stop: Some(NativeProcessStop {
            kind: NativeProcessStopKind::CaptureFailed,
            message: "original cleanup failure".into(),
            cleanup_completed: false,
        }),
    };
    for unresolved in [
        Err(crate::Error::exchange(
            crate::ExchangeStep::Step("capture original output"),
            std::io::Error::other("original capture unavailable"),
        )),
        Ok(None),
        Ok(Some(capture.clone())),
    ] {
        finish_output(&log, &unresolved).await?;
        assert!(
            activity_context
                .read_output_file_once("completed.json")
                .await?
                .is_none(),
            "unsettled capture retired quiet supervision",
        );
    }
    capture
        .stop
        .as_mut()
        .ok_or("missing stop")?
        .cleanup_completed = true;
    finish_output(&log, &Ok(Some(capture.clone()))).await?;
    let terminal = activity_context
        .read_output_file_once("completed.json")
        .await?
        .ok_or("physical completion missing")?;
    let completed: crate::runtime::ToolOutputCompletion = serde_json::from_slice(&terminal.1)?;
    assert_eq!(
        (completed.turn, completed.call),
        (turn, context.operation_id())
    );
    capture.stop = None;
    finish_output(&recovered, &Ok(Some(capture))).await?;
    assert_eq!(
        context
            .read_output_file_once("completed.json")
            .await?
            .ok_or("completion replay missing")?,
        terminal,
        "completion replay changed its original publication",
    );
    assert!(
        task.stage_file(
            Some(chunk.volume()),
            OperationId::new(),
            &activity_path,
            b"forged",
            "application/json",
            "activity.json",
        )
        .await
        .is_err()
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
async fn client_file_wait_ignores_unchanged_publications() -> TestResult {
    use crate::client_files::{ClientFileChange, FileRoot};
    use crate::executor::{ExecutionEvent, ExecutionJournal};
    use acyclic_stream::StreamProviderExt as _;
    let (bound, agent) = binding().await?;
    let root = FileRoot::Private {};
    let paths = vec!["notes.txt".to_owned()];
    bound
        .publish_files(
            &agent,
            root,
            OperationId::from_bytes([71; 16]),
            &[ClientFileChange {
                path: paths[0].clone(),
                expected: None,
                text: Some("before".into()),
            }],
        )
        .await?;
    let original = bound.files(&agent, root, &paths, None).await?;
    let file = original.files[&paths[0]]
        .as_ref()
        .ok_or("missing authored file")?
        .original()
        .ok_or("expected authored source")?
        .clone();
    let state = bound.agents().state(&agent).await?;
    let reference = super::super::super::bounded_io::content_workspace(
        &bound.volumes.provider,
        state.volumes[0].volume()?,
        None,
    )?;
    let workspace = bound.volumes.open(&reference, None).await?;
    let source =
        acyclic_fs::session::changes_path(&bound.volumes.filesystem, workspace.id().volume_id())?;
    let before = bound.agents.provider().tail(source.clone()).await?;
    let waiting = bound.files(&agent, root, &paths, Some(&original.cut));
    futures::pin_mut!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    // Conditional file updates retain a new operation annotation even for the
    // same text. A clean transaction exercises an actually unchanged candidate.
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut transaction = workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::from_bytes([72; 16]),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let published = transaction
        .commit(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await?
        .value;
    assert!(matches!(
        published,
        acyclic_fs::TransactionCommit::Committed(_)
    ));
    let unchanged = bound.files(&agent, root, &paths, None).await?;
    assert!(unchanged.cut.same_inputs(&original.cut));
    assert!(
        bound.agents.provider().tail(source).await? > before,
        "unchanged publication did not exercise the changes producer"
    );
    assert!(
        futures::poll!(waiting.as_mut()).is_pending(),
        "unchanged cut ended the watch"
    );
    bound
        .publish_files(
            &agent,
            root,
            OperationId::from_bytes([73; 16]),
            &[ClientFileChange {
                path: paths[0].clone(),
                expected: Some(file),
                text: Some("after".into()),
            }],
        )
        .await?;
    let changed = waiting.await?;
    assert!(!changed.cut.same_inputs(&original.cut));
    assert_eq!(
        changed.files[&paths[0]]
            .as_ref()
            .ok_or("changed file missing")?
            .text,
        "after"
    );
    // Trace demand is held by this same authenticated file wait, including
    // before the original trace stream has its first publication.
    let journal = bound.turns().query_journal(&state).await?;
    let operation = OperationId::from_bytes([74; 16]);
    let event = ExecutionEvent::Started {
        request_digest: [75; 32],
    };
    journal.append(operation, "start", &event, None)?.await?;
    assert_eq!(
        journal.replay(operation, 0, 1, None)?.await?[0].event,
        event
    );
    let requested = "live/agents/self/trace/0/2.json".to_owned();
    let trace_paths = vec![requested.clone()];
    let before = bound.files(&agent, root, &trace_paths, None).await?;
    assert!(before.files[&requested].is_none());
    assert!(bound.trace_recorder(&agent)?.is_none());
    let mut watching = Box::pin(bound.files(&agent, root, &trace_paths, Some(&before.cut)));
    assert!(futures::poll!(&mut watching).is_pending());
    assert!(bound.trace_recorder(&agent)?.is_some());
    let records = journal.replay(operation, 0, 1, None)?.await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event, event);
    let staged = journal
        .stage(operation, "trace-content", b"raw", "text/plain", None)?
        .await?;
    assert_eq!(journal.load(&staged, None)?.await?.as_ref(), b"raw");
    journal.verify_input_file(&staged, None)?.await?;
    assert_eq!(
        journal
            .load_range(
                &staged,
                acyclic_fs::ByteRange {
                    offset: 1,
                    length: 2
                },
                None
            )?
            .await?
            .as_ref(),
        b"aw"
    );
    let changed = watching.await?;
    assert!(
        bound.trace_recorder(&agent)?.is_none(),
        "returned file wait retained demand"
    );
    let value: serde_json::Value = serde_json::from_str(
        &changed.files[&requested]
            .as_ref()
            .ok_or("trace window absent")?
            .text,
    )?;
    assert_eq!(value["coveredFrom"], 0);
    assert_eq!(value["coveredThrough"], 2);
    let source = crate::client_files::trace::source(&agent)?;
    let through = bound.agents.provider().tail(source.clone()).await?;
    let all_path = format!("live/agents/self/trace/0/{through}.json");
    let completed = bound.files(&agent, root, &[all_path.clone()], None).await?;
    let value: serde_json::Value = serde_json::from_str(
        &completed.files[&all_path]
            .as_ref()
            .ok_or("completed trace absent")?
            .text,
    )?;
    assert_eq!(value["coveredThrough"], through);
    assert!(
        value["mechanisms"]["commit"]["known_bytes"]
            .as_u64()
            .unwrap()
            >= 3
    );
    assert!(value["mechanisms"]["io"]["known_bytes"].as_u64().unwrap() >= 3);
    let stream = acyclic_stream::Stream::new(bound.agents.provider().clone(), source.as_str())?;
    let mut query = acyclic_stream::trace::TraceQuery::new(
        stream.read(0, u32::try_from(through)?).await?,
        Default::default(),
    );
    let mut original = std::collections::BTreeMap::new();
    while let Some(observation) = query.next().await? {
        if let acyclic_stream::trace::Observation::Completed {
            start,
            bytes,
            result,
            ..
        } = observation
        {
            let expected = match start.name.as_str() {
                "harness.execution_replay_page" => (None, "authenticated_execution_prefix"),
                "harness.private_stage" => (Some(3), "authenticated_private_publication"),
                "harness.private_load" => (Some(3), "authenticated_immutable_content"),
                "harness.private_load_range" => (Some(2), "authenticated_immutable_range"),
                "harness.verify_input_file" => (None, "authenticated_original_input_file"),
                _ => continue,
            };
            assert_eq!(start.parent, None);
            assert_eq!(
                start.mechanism,
                match start.name.as_str() {
                    "harness.private_stage" => acyclic_stream::trace::Mechanism::Commit,
                    "harness.verify_input_file" => acyclic_stream::trace::Mechanism::AuthCheck,
                    _ => acyclic_stream::trace::Mechanism::Io,
                }
            );
            assert_eq!(start.guarantee.as_deref(), Some(expected.1));
            assert_eq!(bytes, expected.0);
            assert_eq!(result, acyclic_stream::trace::Outcome::Succeeded);
            assert!(original.insert(start.name, ()).is_none());
        }
    }
    assert_eq!(original.len(), 5);
    Ok(())
}
