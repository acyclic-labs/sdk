use super::*;

pub(super) struct SnapshotReader {
    storage: Arc<MemoryHarnessStorage>,
    reads: std::sync::atomic::AtomicUsize,
    verifies: std::sync::atomic::AtomicUsize,
    missing: std::sync::atomic::AtomicBool,
}

impl ContentResidencyVerifier for SnapshotReader {
    fn verify<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.verifies.fetch_add(1, Ordering::SeqCst);
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound("snapshot source unavailable".into()));
            }
            if file.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds selected limit".into()));
            }
            file.descriptor()
                .verify(&self.storage.read(file).await?, &mut None)
        })
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        self.reads.fetch_add(1, Ordering::SeqCst);
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound("snapshot source unavailable".into()));
            }
            if file.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds selected limit".into()));
            }
            self.storage.read(file).await
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one authenticated snapshot through actual pipeline/model admission, metadata bounds, capture identity and retained replay"
)]
pub(super) async fn pinned_snapshot_preserves_metadata_and_pins_capture_before_model_admission()
-> Result<()> {
    use acyclic_harness::context::{ContextInput, ContextStage as _, PinnedContextStage};
    use std::collections::BTreeMap;

    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
    let metadata = storage
        .stage(
            OperationId::new(),
            "snapshot/state.txt",
            b"authoritative state",
            "text/plain",
            "state.txt",
        )
        .await?;
    let snapshot = Context {
        messages: vec![message(&storage, ModelRole::System, "pinned instruction").await?],
        metadata: BTreeMap::from([("snapshot.state".into(), metadata.clone())]),
        current_input_index: None,
    };
    let encoded = serde_json::to_vec(
        &serde_json::to_value(&snapshot).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let reference = storage
        .stage(
            OperationId::new(),
            "snapshot/context.json",
            &encoded,
            "application/json",
            "context.json",
        )
        .await?;
    let reader = Arc::new(SnapshotReader {
        storage: storage.clone(),
        reads: std::sync::atomic::AtomicUsize::new(0),
        verifies: std::sync::atomic::AtomicUsize::new(0),
        missing: std::sync::atomic::AtomicBool::new(false),
    });
    let limits = Limits {
        context_messages: 8,
        attachments: 4,
        ..Limits::default()
    };
    let stage = Arc::new(
        PinnedContextStage::capture(
            "pinned-snapshot",
            "1",
            reference.clone(),
            reader.clone(),
            ContextPlacement::Prepend,
            limits,
        )
        .await?,
    );
    assert_eq!(
        stage.contract(None)?.get("reference"),
        Some(&serde_json::to_value(&reference).map_err(|error| Error::Invalid(error.to_string()))?)
    );
    let input = ContextInput {
        input: ModelContent::Text("fresh input".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    let pipeline =
        ContextPipeline::new([stage.clone() as Arc<dyn acyclic_harness::context::ContextStage>]);
    let composed = pipeline.run(&input, limits, None).await?;
    assert_eq!(composed.metadata, snapshot.metadata);
    assert_eq!(composed.current_input_index, Some(1));
    assert_eq!(composed.messages.first(), snapshot.messages.first());
    assert_eq!(
        composed.messages.last().map(|message| &message.content),
        Some(&input.input)
    );
    let verifies = reader.verifies.load(Ordering::SeqCst);
    let mut collision = ContextPipeline::base_context(&input, limits, None)?;
    collision.metadata = snapshot.metadata.clone();
    assert!(matches!(
        stage.apply(&input, collision, limits, None)?.await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(reader.verifies.load(Ordering::SeqCst), verifies);
    let reads = reader.reads.load(Ordering::SeqCst);
    assert!(matches!(
        PinnedContextStage::capture(
            "pinned-snapshot",
            "1",
            reference.clone(),
            reader.clone(),
            ContextPlacement::Prepend,
            Limits {
                render_bytes: 1,
                ..limits
            },
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
    assert!(matches!(
        pipeline
            .run(
                &input,
                Limits {
                    context_messages: 1,
                    ..limits
                },
                None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(reader.verifies.load(Ordering::SeqCst), verifies);

    let narrow_file_bytes = reference.descriptor().byte_length() - 1;
    assert!(
        snapshot
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .chain(snapshot.metadata.values())
            .all(|file| file.descriptor().byte_length() <= narrow_file_bytes)
    );
    let counts_before_narrow = (
        reader.reads.load(Ordering::SeqCst),
        reader.verifies.load(Ordering::SeqCst),
    );
    assert!(matches!(
        pipeline
            .run(
                &input,
                Limits {
                    file_bytes: narrow_file_bytes,
                    ..limits
                },
                None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        (
            reader.reads.load(Ordering::SeqCst),
            reader.verifies.load(Ordering::SeqCst)
        ),
        counts_before_narrow
    );

    let delta = Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: input.input.clone(),
        }],
        metadata: BTreeMap::from([("fresh.state".into(), metadata.clone())]),
        current_input_index: Some(0),
    };
    let continued = ContextPipeline::continue_base(snapshot.clone(), delta.clone(), limits, None)?;
    assert_eq!(continued.metadata.len(), 2);
    assert_eq!(continued.metadata.get("snapshot.state"), Some(&metadata));
    assert_eq!(continued.metadata.get("fresh.state"), Some(&metadata));
    assert_eq!(continued.current_input_index, Some(1));
    assert!(matches!(
        ContextPipeline::continue_base(
            snapshot.clone(),
            delta.clone(),
            Limits {
                attachments: 1,
                ..limits
            },
            None,
        ),
        Err(Error::Invalid(_))
    ));
    let combined_bytes = serde_json::to_vec(&continued)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .len() as u64;
    assert!(
        (serde_json::to_vec(&snapshot)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64)
            < combined_bytes - 1
    );
    assert!(
        (serde_json::to_vec(&delta)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64)
            < combined_bytes - 1
    );
    assert!(matches!(
        ContextPipeline::continue_base(
            snapshot.clone(),
            delta.clone(),
            Limits {
                render_bytes: combined_bytes - 1,
                ..limits
            },
            None,
        ),
        Err(Error::Invalid(_))
    ));
    let mut duplicate = delta;
    duplicate.metadata = snapshot.metadata.clone();
    assert!(matches!(
        ContextPipeline::continue_base(snapshot.clone(), duplicate, limits, None),
        Err(Error::Conflict(_))
    ));

    let (model, _counts) = summary_model(1);
    let snapshot_model = Model::new(
        "synthetic".into(),
        "snapshot-admission".into(),
        "1".into(),
        serde_json::json!({}),
    )?;
    let build = |pipeline| {
        StockExecutor::new(
            snapshot_model.clone(),
            model.clone(),
            pipeline,
            ToolRegistry::default(),
        )
        .with_limits(limits)
        .with_compaction_policy(CompactionPolicy::Disabled)
    };
    let executor = build(pipeline);
    let turn = TurnInput {
        operation_id: OperationId::new(),
        input: input.input.clone(),
        selected_context: None,
        max_steps: 1,
    };
    let journal = storage.journal();
    let output = executor.execute(turn.clone(), journal.as_ref()).await?;
    let records = journal.replay(turn.operation_id, 0, 32, None)?.await?;
    let projection = records
        .iter()
        .find_map(|record| match &record.event {
            acyclic_harness::executor::ExecutionEvent::ContextPrepared {
                step: 0,
                projection,
                ..
            } => Some(projection),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("actual admitted snapshot projection".into()))?;
    let projection_bytes = journal.load(projection, None)?.await?;
    projection
        .descriptor()
        .verify(&projection_bytes, &mut None)?;
    let projection_wire: serde_json::Value = serde_json::from_slice(&projection_bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let prepared: Context = serde_json::from_value(
        projection_wire
            .get("context")
            .cloned()
            .ok_or_else(|| Error::Invalid("prepared projection has no context".into()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(prepared, composed);
    {
        let requests = model.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests.first().map(|request| &request.messages),
            Some(&composed.messages)
        );
    }
    reader.missing.store(true, Ordering::SeqCst);
    let counts = (
        reader.reads.load(Ordering::SeqCst),
        reader.verifies.load(Ordering::SeqCst),
    );
    assert_eq!(
        executor.execute(turn.clone(), journal.as_ref()).await?,
        output
    );
    assert_eq!(
        (
            reader.reads.load(Ordering::SeqCst),
            reader.verifies.load(Ordering::SeqCst)
        ),
        counts
    );
    assert!(matches!(
        executor
            .execute(
                TurnInput {
                    operation_id: OperationId::new(),
                    ..turn.clone()
                },
                journal.as_ref()
            )
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(model.requests().len(), 1);
    reader.missing.store(false, Ordering::SeqCst);
    let changed = storage
        .stage(
            OperationId::new(),
            "snapshot/other-context.json",
            &encoded,
            "application/json",
            "context.json",
        )
        .await?;
    let other_stage = PinnedContextStage::capture(
        "pinned-snapshot",
        "1",
        changed,
        reader,
        ContextPlacement::Prepend,
        limits,
    )
    .await?;
    assert_ne!(stage.contract(None)?, other_stage.contract(None)?);
    let changed_executor = build(ContextPipeline::new([
        Arc::new(other_stage) as Arc<dyn acyclic_harness::context::ContextStage>
    ]));
    assert!(matches!(
        changed_executor.execute(turn, journal.as_ref()).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(model.requests().len(), 1);
    Ok(())
}
