//! Original-accounting UI record reads through real Streams.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::{
    CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters,
    demand::{DemandError, DemandSource},
    kernel::{KernelAccounting, MetadataDecodeError, NativeAccounting, RetainedAllocation},
};
use acyclic_harness_core::agent::{
    AgentRecord, Error as AgentError, Phase, RECORD_VERSION, StatusRecord,
};
use acyclic_stream::{
    AppendRequest, HeadProjection, HeadsRequest, IdempotencyKey, MemoryStream,
    NativeStreamProvider, RevisionCut, StreamPath, StreamProviderExt as _,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct NoContent;
impl acyclic_harness_core::conversation::ContentResidencyVerifier for NoContent {}

fn navigation(
    child: &StreamPath,
    ctx: &mut KernelAccounting<'_>,
) -> std::result::Result<acyclic_fs::kernel::OwnedSourceChunk, DemandError> {
    acyclic_fs::kernel::OwnedSourceChunk::produce(ctx, |sink| {
        write!(sink, "/{}/ui", child.as_str()).map_err(DemandError::from)
    })
}

fn namespace(path: &str) -> acyclic_fs::kernel::NamespacePath {
    acyclic_fs::kernel::NamespacePath::from_text_in_profile(
        path,
        acyclic_fs::FilesystemProfile::Portable,
        acyclic_fs::VolumeLimits::default(),
    )
    .expect("test namespace")
}

fn adoption(
    agent: acyclic_harness_core::agent::AgentId,
) -> acyclic_harness_core::agent::ForkAdopted {
    acyclic_harness_core::agent::ForkAdopted {
        agent,
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}

fn accounting(cancellation: &CancellationToken, budget: WorkBudget) -> KernelAccounting<'_> {
    KernelAccounting::Standalone {
        allocations: Default::default(),
        work: WorkCounters::UNCHARGED,
        budget,
        cancellation,
    }
}

fn status(path: &str) -> StatusRecord {
    StatusRecord {
        phase: Phase::WaitingUser { path: path.into() },
        cursor: 3,
        conversation: 2,
        child_settlement: None,
        child_wakes: None,
    }
}

#[tokio::test]
async fn stream_ui_open_pins_real_turn_files_across_later_publications() -> Result {
    use acyclic_harness_core::{
        OperationId,
        agent::{AgentId, ConversationEntry, IntentId, TurnInput, create_root},
        filesystem::MemoryHarnessStorage,
        ui::{StreamUiProvider, View},
    };
    use std::sync::Arc;
    let streams = Arc::new(MemoryStream::default());
    let root = create_root(
        &IntentId::try_from("file-backed".to_owned())?,
        adoption(AgentId::from_bytes([11; 16])),
    )?;
    let agent = root.child;
    streams.commit(root.request).await?;
    let storage = MemoryHarnessStorage::with_stream(
        acyclic_harness_core::AgentId::from_bytes(agent.id().to_bytes()),
        16_384,
        streams.clone(),
    )
    .await?;
    for (sequence, text) in [(0, "original é\nanswer"), (1, "later answer")] {
        let bytes = serde_json::to_vec(
            &serde_json::json!({"text": text, "transcript": [{"ignored": [1, 2, 3]}]}),
        )?;
        let file = storage
            .stage(
                OperationId::new(),
                &format!("turns/{sequence}.json"),
                &bytes,
                "application/json",
                "turn.json",
            )
            .await?;
        let entry = ConversationEntry {
            observed_through: sequence + 1,
            inbox: sequence,
            input: TurnInput::Brief {
                brief: Box::new(file.clone()),
            },
            output: file,
            outcome: Default::default(),
            goal: None,
            targets: None,
            tool_results: None,
            steering: None,
            notices: None,
        };
        streams
            .append(AppendRequest {
                path: agent.conversation()?,
                records: vec![entry.encode()?.into()],
                if_tail: Some(sequence),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
    }
    let source = StreamUiProvider::new(
        streams.clone(),
        storage.content_bindings().reader,
        agent.clone(),
        View::new(1)?,
        [5; 16],
        navigation,
    )
    .into_source();
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &native,
        cancellation: &cancellation,
    };
    let frozen = source.reference(&mut ctx).await?;
    let path = namespace("/turns/0.md");
    let node = source
        .lookup(frozen.clone(), &path, &native, &cancellation)
        .await?
        .ok_or("missing turn page")?;
    let opened = source
        .open_file(frozen.clone(), &path, node.version, &native, &cancellation)
        .await?;
    let before = opened.read_range(0, u64::MAX, &mut ctx)?;
    assert!(std::str::from_utf8(&before.bytes)?.contains("original é\nanswer"));
    assert!(!std::str::from_utf8(&before.bytes)?.contains("later answer"));
    storage
        .stage(
            OperationId::new(),
            "turns/0.json",
            br#"{"text":"replacement at the same path"}"#,
            "application/json",
            "turn.json",
        )
        .await?;
    streams
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![status("new-question.md").encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let refreshed = source.reference(&mut ctx).await?;
    assert_ne!(refreshed, frozen);
    let offset = before.bytes.len().saturating_sub(9) as u64;
    let interval = opened.read_range(offset, 7, &mut ctx)?;
    assert_eq!(
        interval.bytes.as_ref(),
        &before.bytes[offset as usize..(offset as usize + 7).min(before.bytes.len())]
    );
    let repeated = source
        .read_range(
            frozen,
            &path,
            node.version,
            0,
            u64::MAX,
            &native,
            &cancellation,
        )
        .await?;
    assert_eq!(repeated.bytes, before.bytes);
    // One visible turn page may cross the stream request ceiling. Reuse one
    // real immutable output so the proof exercises paging, not file creation.
    let file = storage
        .stage(
            OperationId::new(),
            "turns/boundary.json",
            br#"{"text":"boundary record"}"#,
            "application/json",
            "turn.json",
        )
        .await?;
    let entry = ConversationEntry {
        observed_through: 2,
        inbox: 1,
        input: TurnInput::Brief {
            brief: Box::new(file.clone()),
        },
        output: file,
        outcome: Default::default(),
        goal: None,
        targets: None,
        tool_results: None,
        steering: None,
        notices: None,
    };
    let extra = acyclic_stream::MAX_ITEMS - 1;
    streams
        .append(AppendRequest {
            path: agent.conversation()?,
            records: vec![entry.encode()?.into(); extra],
            if_tail: Some(2),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let wide = StreamUiProvider::new(
        streams,
        storage.content_bindings().reader,
        agent,
        View::new(u32::try_from(extra + 2)?)?,
        [5; 16],
        navigation,
    )
    .into_source();
    let cut = wide.reference(&mut ctx).await?;
    let node = wide
        .lookup(cut.clone(), &path, &native, &cancellation)
        .await?
        .ok_or("missing wide turn page")?;
    let bytes = wide
        .read_range(
            cut,
            &path,
            node.version,
            0,
            u64::MAX,
            &native,
            &cancellation,
        )
        .await?;
    assert_eq!(
        std::str::from_utf8(&bytes.bytes)?
            .matches("boundary record")
            .count(),
        extra
    );
    Ok(())
}

#[tokio::test]
async fn private_agent_fork_binds_child_views_without_replacing_companion_roots() -> Result {
    acyclic_native_runtime::initialize_native_io_services()?;
    use acyclic_fs::{
        Fs, IdempotencyKey as FsKey, LazyWorkspace, LazyWorkspaceError, LocalCoreStateStore,
        PublicationPermit, VolumeLimits,
        demand::{Empty, Routed},
        kernel::{LogicalName, NameEncoding},
    };
    use acyclic_harness_core::{
        Capabilities, OperationId,
        agent::{AgentId, ConversationEntry, IntentId, TurnInput, create_root, spawn},
        conversation::{
            ContentAccess, ContentPublisher, ContentResidencyVerifier, VolumeOperation,
            private_volume,
        },
        core::{AggregateKind, Authority, AuthorityRoot},
        filesystem::{
            FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost,
            MemoryHarnessStorage,
        },
        ui::{StreamUiProvider, View},
    };
    use std::sync::Arc;

    let streams = Arc::new(MemoryStream::default());
    let root = create_root(
        &IntentId::try_from("private-view-parent".to_owned())?,
        adoption(AgentId::from_bytes([21; 16])),
    )?;
    let parent_agent = root.child;
    streams.commit(root.request).await?;
    let child = spawn(
        &parent_agent,
        &IntentId::try_from("private-view-child".to_owned())?,
        1,
        adoption(AgentId::from_bytes([22; 16])),
    )?;
    let child_agent = child.child;
    streams.commit(child.request).await?;
    for (agent, path) in [(&parent_agent, "parent.md"), (&child_agent, "child.md")] {
        streams
            .append(AppendRequest {
                path: agent.status()?,
                records: vec![status(path).encode()?.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
    }
    let fs = Fs::memory();
    let storage = MemoryHarnessStorage::from_providers(
        acyclic_harness_core::AgentId::from_bytes(parent_agent.id().to_bytes()),
        16_384,
        fs.clone(),
        streams.clone(),
    )
    .await?;
    let source =
        |agent, navigation_identity, reader: Arc<dyn ContentResidencyVerifier>| -> Result<_> {
            Ok(Arc::new(Routed::new(
                Empty,
                LogicalName::new(NameEncoding::Utf8, b"ui".to_vec(), 255)?,
                StreamUiProvider::new(
                    streams.clone(),
                    reader,
                    agent,
                    View::new(2)?,
                    navigation_identity,
                    navigation,
                )
                .into_source(),
                VolumeLimits::default(),
            )))
        };
    let parent_source = source(
        parent_agent.clone(),
        [4; 16],
        storage.content_bindings().reader,
    )?;
    let directory = tempfile::tempdir()?;
    let store = LocalCoreStateStore::open_owned(directory.path())?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &native,
        cancellation: &cancellation,
    };
    let workspace = fs
        .open_workspace(
            &storage.volume().storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let parent = LazyWorkspace::bind(workspace, parent_source, store.clone(), &mut ctx).await?;
    let parent_status = parent.read("/ui/status.md", 4096, &mut ctx).await?;
    assert!(std::str::from_utf8(parent_status.bytes.as_slice())?.contains("parent.md"));
    let directory_lookup = parent.lookup("/ui", &mut ctx).await?;
    let directory_identity = parent
        .stable_file_id_for_lookup("/ui", &directory_lookup.0, &mut ctx)
        .await?;
    parent.retire_lookup(directory_lookup, &mut ctx)?;
    parent
        .promote(
            "/ui",
            0,
            FsKey::new(),
            PublicationPermit::Unrestricted,
            &mut ctx,
        )
        .await?;
    parent
        .write(
            "/ui/title",
            acyclic_stream::OwnedBytes::from_static(b"inherited title"),
            FsKey::new(),
            PublicationPermit::Unrestricted,
            &mut ctx,
        )
        .await?;
    parent.hide_source_path("/ui/model", &mut ctx).await?;
    let parent_cut = parent.snapshot(&mut ctx).await?;
    let child_volume = private_volume(storage.volume().provider().clone(), &child_agent.id())?;
    let child_name = child_volume.storage_name(&mut None)?;
    let (inherited, inherited_cut) = parent
        .fork(
            &child_name,
            FsKey::new(),
            PublicationPermit::Unrestricted,
            &mut ctx,
        )
        .await?;
    let original = inherited.state(&mut ctx).await?;
    // Bind the existing fork, rather than creating another volume for the child.
    // Each reader has only its own volume's signed read authority.
    let host = FilesystemHost::new(fs.clone(), storage.volume().provider(), None)?;
    let child_owner = acyclic_harness_core::AgentId::from_bytes(child_agent.id().to_bytes());
    let issuer = AuthorityRoot::new(
        "private-view-child",
        [22; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "private-view-child".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "child",
        &Capabilities::new([
            child_volume.capability(VolumeOperation::Read, &mut None)?,
            child_volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(child_owner),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let child_reader =
        FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 16_384, None)?;
    let child_writer = FilesystemContentPublisher::new(
        host.clone(),
        &child_volume,
        &verifier,
        &scope,
        16_384,
        None,
    )?;
    let (child_output, _) = child_writer
        .stage(
            OperationId::new(),
            "turns/0.json",
            br#"{"text":"child-owned answer"}"#,
            "application/json",
            "turn.json",
            ContentAccess::default(),
        )?
        .await?;
    let parent_output = storage
        .stage(
            OperationId::new(),
            "turns/0.json",
            br#"{"text":"parent-owned answer"}"#,
            "application/json",
            "turn.json",
        )
        .await?;
    assert!(
        child_reader
            .read(&parent_output, 16_384, None, None)?
            .await
            .is_err()
    );
    assert!(storage.read(&child_output).await.is_err());
    for (agent, output) in [(&parent_agent, parent_output), (&child_agent, child_output)] {
        let entry = ConversationEntry {
            observed_through: 1,
            inbox: 0,
            input: TurnInput::Brief {
                brief: Box::new(output.clone()),
            },
            output,
            outcome: Default::default(),
            goal: None,
            targets: None,
            tool_results: None,
            steering: None,
            notices: None,
        };
        streams
            .append(AppendRequest {
                path: agent.conversation()?,
                records: vec![entry.encode()?.into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
    }
    let child_source = source(child_agent.clone(), [4; 16], child_reader.clone())?;
    let child = inherited
        .bind_fork_source(child_source.clone(), &mut ctx)
        .await?;
    let bound = child.state(&mut ctx).await?;
    assert_eq!(bound.fork_origin, original.fork_origin);
    assert_eq!(bound.parent_workspace_id, original.parent_workspace_id);
    assert_eq!(bound.overlay, parent_cut.overlay);
    assert_eq!(bound.shadows, parent_cut.shadows);
    assert_eq!(bound.selected_import_roots, original.selected_import_roots);
    let child_cut = child.snapshot(&mut ctx).await?;
    assert_ne!(child_cut.source.identity, parent_cut.source.identity);
    let projected_reader = FilesystemContentVerifier::new(
        host.for_private_view(&child_volume, child.share(&mut ctx)?)?,
        &verifier,
        &scope,
        16_384,
        None,
    )?;
    let (selected, projected, _) = projected_reader
        .read_private_range(
            &child_volume,
            "",
            "ui/status.md",
            None,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 4096,
            },
            ContentAccess::default(),
        )?
        .await?;
    assert!(std::str::from_utf8(&projected)?.contains("child.md"));
    assert_eq!(selected.generation.as_resource().version(), Some("lazy-v1"));
    let selected: acyclic_harness_core::conversation::FilePathSelection =
        serde_json::from_slice(&serde_json::to_vec(&selected)?)?;
    let directory_lookup = child.lookup("/ui", &mut ctx).await?;
    assert_eq!(
        child
            .stable_file_id_for_lookup("/ui", &directory_lookup.0, &mut ctx)
            .await?,
        directory_identity,
        "rebinding cannot rename an inherited promoted directory"
    );
    child.retire_lookup(directory_lookup, &mut ctx)?;
    let child_page = child.read("/ui/turns/0.md", 4096, &mut ctx).await?;
    assert!(std::str::from_utf8(child_page.bytes.as_slice())?.contains("child-owned answer"));
    assert!(!std::str::from_utf8(child_page.bytes.as_slice())?.contains("parent-owned answer"));
    let parent_page = parent.read("/ui/turns/0.md", 4096, &mut ctx).await?;
    assert!(std::str::from_utf8(parent_page.bytes.as_slice())?.contains("parent-owned answer"));
    let frozen = child
        .read_at_snapshot(&child_cut, "/ui/status.md", None, 4096, &mut ctx)
        .await?
        .0;
    assert!(std::str::from_utf8(frozen.bytes.as_slice())?.contains("child.md"));
    assert_eq!(
        inherited
            .read_at_snapshot(&inherited_cut, "/ui/status.md", None, 4096, &mut ctx)
            .await?
            .0
            .bytes
            .as_slice(),
        parent_status.bytes.as_slice()
    );
    assert_eq!(
        child
            .read("/ui/title", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        b"inherited title"
    );
    assert!(matches!(
        child.stat("/ui/model", &mut ctx).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    let replay = inherited
        .bind_fork_source(child_source.clone(), &mut ctx)
        .await?;
    assert_eq!(
        replay.snapshot(&mut ctx).await?,
        child.snapshot(&mut ctx).await?
    );
    assert!(matches!(
        inherited
            .bind_fork_source(
                source(child_agent.clone(), [5; 16], child_reader)?,
                &mut ctx
            )
            .await,
        Err(LazyWorkspaceError::Concurrent)
    ));
    child.remove("/ui/title", None, &mut ctx).await?;
    let title = child.read("/ui/title", 4096, &mut ctx).await?;
    assert_eq!(
        title.bytes.as_slice(),
        child_agent
            .as_str()
            .rsplit('/')
            .next()
            .ok_or("child id")?
            .as_bytes()
    );
    assert_eq!(
        parent
            .read("/ui/title", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        b"inherited title"
    );
    streams
        .append(AppendRequest {
            path: child_agent.status()?,
            records: vec![status("later-child.md").encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let measured = acyclic_harness_core::filesystem::PreparationIo::from_native_work(&native)?;
    let (reselected, repeated, _) = projected_reader
        .read_private_range(
            &child_volume,
            "",
            "ui/status.md",
            Some(&selected.generation),
            acyclic_fs::ByteRange {
                offset: 1,
                length: 7,
            },
            ContentAccess {
                ceiling: None,
                io: Some(&measured),
            },
        )?
        .await?;
    assert_eq!(reselected, selected);
    assert_eq!(repeated.as_ref(), &projected[1..8]);
    // A finite ordinary caller lends the same ledger and can resume it after
    // native production. Independently retained credit never belongs to its
    // scratch scope, including after all returned values have retired.
    let ordinary = acyclic_harness_core::filesystem::PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024),
        &cancellation,
    )?;
    let (ordinary_selection, ordinary_bytes, ordinary_hold) = projected_reader
        .read_private_range(
            &child_volume,
            "",
            "ui/status.md",
            Some(&selected.generation),
            acyclic_fs::ByteRange {
                offset: 1,
                length: 7,
            },
            ContentAccess {
                ceiling: None,
                io: Some(&ordinary),
            },
        )?
        .await?;
    assert_eq!(ordinary_selection, selected);
    assert_eq!(ordinary_bytes.as_ref(), repeated.as_ref());
    drop((ordinary_selection, ordinary_bytes, ordinary_hold));
    ordinary.remaining()?;
    assert!(
        ordinary.take_owned_custody(1).is_err(),
        "retired ordinary scratch cannot own independent lease credit"
    );
    let (_, current, _) = projected_reader
        .read_private_range(
            &child_volume,
            "",
            "ui/status.md",
            None,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 4096,
            },
            ContentAccess::default(),
        )?
        .await?;
    assert!(std::str::from_utf8(&current)?.contains("later-child.md"));
    assert_eq!(
        child
            .read_at_snapshot(&child_cut, "/ui/status.md", None, 4096, &mut ctx)
            .await?
            .0
            .bytes
            .as_slice(),
        frozen.bytes.as_slice()
    );
    let child_workspace = child.workspace().clone();
    drop(projected_reader);
    drop(replay);
    drop(child);
    drop(inherited);
    drop(parent);
    drop(store);
    let reopened = LazyWorkspace::open(
        child_workspace,
        child_source,
        LocalCoreStateStore::open_owned(directory.path())?,
        &mut ctx,
    )
    .await?;
    let current = reopened.read("/ui/status.md", 4096, &mut ctx).await?;
    assert!(std::str::from_utf8(current.bytes.as_slice())?.contains("later-child.md"));
    assert!(matches!(
        reopened.stat("/ui/model", &mut ctx).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    assert_eq!(
        reopened.state(&mut ctx).await?.fork_origin,
        original.fork_origin
    );
    Ok(())
}

#[tokio::test]
async fn computed_ui_watch_delivers_real_publications_and_physically_stops() -> Result {
    use acyclic_fs::demand::{SourceChange, SourceChangeSink, SourceFence};
    use acyclic_harness_core::{
        agent::{AgentId, IntentId, create_root, spawn},
        ui::{StreamUiProvider, View},
    };
    use acyclic_stream::testing::Counted;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Default)]
    struct Sink {
        changes: Mutex<Vec<SourceChange>>,
        lost: AtomicUsize,
    }
    impl SourceChangeSink for Sink {
        fn source_changed(
            &self,
            changes: &[SourceChange],
            _: &mut KernelAccounting<'_>,
        ) -> std::result::Result<(), DemandError> {
            self.changes.lock().unwrap().extend_from_slice(changes);
            Ok(())
        }
        fn delivery_lost(&self) {
            self.lost.fetch_add(1, Ordering::SeqCst);
        }
    }
    let streams = Arc::new(Counted::new(MemoryStream::default()));
    let root = create_root(
        &IntentId::try_from("watched".to_owned())?,
        adoption(AgentId::from_bytes([9; 16])),
    )?;
    let agent = root.child;
    streams.commit(root.request).await?;
    let source = StreamUiProvider::new(
        streams.clone(),
        Arc::new(NoContent),
        agent.clone(),
        View::new(2)?,
        [4; 16],
        navigation,
    )
    .into_source();
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = native.live_allocation_bytes();
    let sink = Arc::new(Sink::default());
    let before = streams.requests();
    let watch = source
        .watch(sink.clone(), &native, &cancellation)?
        .ok_or("missing watch")?;
    let opening = streams.requests().since(before);
    assert_eq!(opening.control(), 1, "one joint starting cut");
    assert_eq!(opening.data(), 3, "one subscription per input");
    assert_eq!(
        sink.changes.lock().unwrap().len(),
        6,
        "opening invalidates earlier observations"
    );
    sink.changes.lock().unwrap().clear();
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &native,
        cancellation: &cancellation,
    };
    assert_eq!(watch.fence(&mut ctx)?, SourceFence::Unconfirmed);
    assert!(watch.is_exact());
    streams
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![status("reply.md").encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert_eq!(
        *sink.changes.lock().unwrap(),
        [
            SourceChange::Entry(namespace("/index.md")),
            SourceChange::Entry(namespace("/status.md")),
            SourceChange::Entry(namespace("/glyph"))
        ]
    );
    sink.changes.lock().unwrap().clear();
    let child = spawn(
        &agent,
        &IntentId::try_from("watched-child".to_owned())?,
        1,
        adoption(AgentId::from_bytes([10; 16])),
    )?;
    streams.commit(child.request).await?;
    assert_eq!(
        *sink.changes.lock().unwrap(),
        [SourceChange::Entry(namespace("/children"))]
    );
    assert_eq!(sink.lost.load(Ordering::SeqCst), 0);
    watch.stop_accepted_cleanup()?;
    assert!(watch.is_stopped());
    assert!(!watch.is_exact());
    assert_eq!(sink.lost.load(Ordering::SeqCst), 1);
    let delivered = sink.changes.lock().unwrap().len();
    streams
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![status("later.md").encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert_eq!(sink.changes.lock().unwrap().len(), delivered);
    drop(watch);
    assert_eq!(native.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_status_projection_preserves_original_credit_and_cancellation() -> Result {
    let expected = status("reply/étude.md");
    let stream = MemoryStream::default();
    let path = StreamPath::new("computed/status")?;
    stream
        .append(AppendRequest {
            path: path.clone(),
            records: vec![expected.encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let cancellation = CancellationToken::new();
    let mut caller = accounting(&cancellation, WorkBudget::UNBOUNDED);
    drop(RetainedAllocation::claim(0, 0, &mut caller)?);
    let before = caller.remaining()?.peak_allocation_bytes;
    let owner = NativeAccounting::take(&mut caller)?;
    assert!(caller.remaining().is_err(), "caller must remain fenced");
    let output = stream
        .native_heads(
            HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::History,
                paths: [path].into(),
                children: None,
                limit: 1,
                at_least: RevisionCut::default(),
            },
            owner.work.runtime_meter(),
        )
        .await??;
    let record = output.value().heads[0]
        .as_ref()
        .ok_or("head")?
        .last
        .as_ref()
        .ok_or("record")?;
    let decoded = StatusRecord::decode_accounted(
        &record.value,
        &mut KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: &cancellation,
        },
    )?;
    assert_eq!(*decoded, expected);
    owner.restore(&mut caller)?;
    drop(output);
    assert!(caller.remaining()?.peak_allocation_bytes < before);
    drop(decoded);
    assert_eq!(caller.remaining()?.peak_allocation_bytes, before);
    let held = StatusRecord::decode_accounted(&expected.encode()?, &mut caller)?;
    // Cancellation refuses further work, while destroying retained fields
    // still releases their original ownership without another admission.
    cancellation.cancel();
    drop(held);
    assert!(matches!(
        caller.remaining(),
        Err(acyclic_fs::FsError::Cancelled(_))
    ));
    Ok(())
}

#[test]
fn accounted_agent_records_share_version_shape_and_whole_input_validation() -> Result {
    let cancellation = CancellationToken::new();
    let mut caller = accounting(&cancellation, WorkBudget::UNBOUNDED);
    let expected = status("ask.md");
    let bytes = expected.encode()?;
    assert_eq!(
        *StatusRecord::decode_accounted(&bytes, &mut caller)?,
        expected
    );
    // Every trailing marker is refused, including an incomplete integer whose
    // payload is EOF. It cannot be confused with EOF at the next marker.
    for byte in 0..=u8::MAX {
        let mut trailing = bytes.clone();
        trailing.push(byte);
        assert!(StatusRecord::decode(&trailing).is_err());
        assert!(matches!(
            StatusRecord::decode_accounted(&trailing, &mut caller),
            Err(MetadataDecodeError::Codec(AgentError::Record(_)))
        ));
    }
    let mut foreign = (RECORD_VERSION + 1).to_be_bytes().to_vec();
    foreign.push(0xc1); // Malformed payload; version must still win.
    assert!(matches!(
        StatusRecord::decode(&foreign),
        Err(AgentError::UnsupportedRecordVersion(_))
    ));
    assert!(matches!(
        StatusRecord::decode_accounted(&foreign, &mut caller),
        Err(MetadataDecodeError::Codec(
            AgentError::UnsupportedRecordVersion(_)
        ))
    ));
    let malformed = rmp_serde::to_vec_named(&status("../escape.md"))?;
    let mut invalid = RECORD_VERSION.to_be_bytes().to_vec();
    invalid.extend(malformed);
    assert!(StatusRecord::decode(&invalid).is_err());
    assert!(StatusRecord::decode_accounted(&invalid, &mut caller).is_err());
    let mut refused = accounting(
        &cancellation,
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
    );
    assert!(matches!(
        StatusRecord::decode_accounted(&bytes, &mut refused),
        Err(MetadataDecodeError::Work(_))
    ));
    // Dynamic serde values also own their strings; the shared any-value
    // visitor must fund them, just like a statically typed record field.
    let text = "dynamic ".repeat(1024);
    let dynamic = rmp_serde::to_vec(&serde_json::json!({"text": text}))?;
    drop(RetainedAllocation::claim(0, 0, &mut caller)?);
    let baseline = caller.remaining()?.peak_allocation_bytes;
    let mut decoder =
        rmp_serde::Deserializer::from_read_ref(dynamic.as_slice()).with_human_readable();
    let decoded = caller.deserialize::<serde_json::Value, _>(&mut decoder)?;
    assert_eq!(decoded["text"].as_str(), Some(text.as_str()));
    assert!(baseline - caller.remaining()?.peak_allocation_bytes >= text.len() as u64);
    drop(decoded);
    assert_eq!(caller.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}

#[tokio::test]
async fn stream_ui_uses_frozen_inputs_and_direct_child_navigation() -> Result {
    use acyclic_harness_core::{
        agent::{AgentId, IntentId, create_root, spawn},
        ui::{StreamUiProvider, View},
    };
    use acyclic_stream::CommitOutcome;
    use std::sync::Arc;
    let streams = Arc::new(MemoryStream::default());
    let root = create_root(
        &IntentId::try_from("root".to_owned())?,
        adoption(AgentId::from_bytes([1; 16])),
    )?;
    let agent = root.child;
    assert!(matches!(
        streams.commit(root.request).await?,
        CommitOutcome::Committed(_)
    ));
    let child = spawn(
        &agent,
        &IntentId::try_from("child".to_owned())?,
        1,
        adoption(AgentId::from_bytes([2; 16])),
    )?;
    let child_path = child.child;
    assert!(matches!(
        streams.commit(child.request).await?,
        CommitOutcome::Committed(_)
    ));
    let source = StreamUiProvider::new(
        streams.clone(),
        Arc::new(NoContent),
        agent.clone(),
        View::new(2)?,
        [7; 16],
        navigation,
    )
    .into_source();
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &native,
        cancellation: &cancellation,
    };
    let frozen = source.reference(&mut ctx).await?;
    streams
        .append(AppendRequest {
            path: StreamPath::new("unrelated/input")?,
            records: vec![b"unrelated".to_vec().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert_eq!(
        source.reference(&mut ctx).await?,
        frozen,
        "unrelated progress must not invalidate UI inputs"
    );
    let status_path = namespace("/status.md");
    let baseline = native.live_allocation_bytes();
    let old = source
        .lookup(frozen.clone(), &status_path, &native, &cancellation)
        .await?
        .ok_or("status")?;
    assert_eq!(native.live_allocation_bytes(), baseline);
    let mut latest = StatusRecord::decode(
        &streams
            .head(agent.status()?)
            .await?
            .last
            .ok_or("status record")?
            .value,
    )?;
    latest.phase = Phase::WaitingUser {
        path: "reply/étude.md".into(),
    };
    streams
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![latest.encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let next_child = spawn(
        &agent,
        &IntentId::try_from("new-child".to_owned())?,
        1,
        adoption(AgentId::from_bytes([3; 16])),
    )?;
    assert!(matches!(
        streams.commit(next_child.request).await?,
        CommitOutcome::Committed(_)
    ));
    let output = source
        .read_range(
            frozen.clone(),
            &status_path,
            old.version,
            0,
            u64::MAX,
            &native,
            &cancellation,
        )
        .await?;
    assert_eq!(output.bytes.as_slice(), b"Idle\n");
    drop(output);
    let children = source
        .list_page(
            frozen.clone(),
            &namespace("/children"),
            None,
            1,
            &native,
            &cancellation,
        )
        .await?;
    assert_eq!(children.entries.len(), 1);
    let child_link = namespace(&format!("/children/{}", child_path.id()));
    let target = source
        .read_link(
            frozen.clone(),
            &child_link,
            children.entries[0].node.version,
            u64::MAX,
            &native,
            &cancellation,
        )
        .await?;
    assert_eq!(
        target.bytes.as_slice(),
        format!("/{}/ui", child_path.as_str()).as_bytes()
    );
    drop(target);
    drop(children);
    assert!(
        source
            .lookup(
                frozen.clone(),
                &namespace("/children/not-an-agent"),
                &native,
                &cancellation
            )
            .await?
            .is_none()
    );
    assert!(
        source
            .lookup(
                frozen.clone(),
                &namespace("/turns/not-a-page.md"),
                &native,
                &cancellation
            )
            .await?
            .is_none()
    );
    let current = source.reference(&mut ctx).await?;
    assert_ne!(current, frozen);
    let node = source
        .lookup(current.clone(), &status_path, &native, &cancellation)
        .await?
        .ok_or("current status")?;
    let output = source
        .read_range(
            current,
            &status_path,
            node.version,
            0,
            u64::MAX,
            &native,
            &cancellation,
        )
        .await?;
    assert!(std::str::from_utf8(&output.bytes)?.contains("reply/étude.md"));
    // Neither status nor navigation is allowed to read conversation files:
    // NoContent's default read refuses rather than supplying fixture bytes.
    Ok(())
}
