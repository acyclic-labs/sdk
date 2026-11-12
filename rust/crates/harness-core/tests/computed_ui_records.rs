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
