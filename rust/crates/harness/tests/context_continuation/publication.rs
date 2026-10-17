use super::*;

pub(super) async fn stage_fixture_json<T: serde::Serialize>(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
    key: &str,
    value: &T,
) -> Result<FileRef> {
    let value = serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    let bytes = serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stage(operation, &key, &bytes, "application/json", None)?
        .await
}

pub(super) async fn fabricate_checkpoint(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
    event: acyclic_harness::executor::ExecutionEvent,
    mode: usize,
) -> Result<acyclic_harness::executor::ExecutionEvent> {
    use acyclic_harness::context::CanonicalContextCheckpoint;
    use acyclic_harness::executor::ExecutionEvent;
    let ExecutionEvent::ContextCompacted {
        step,
        projection,
        compaction,
        accounting,
    } = event
    else {
        return Err(Error::Invalid("fixture requires compacted context".into()));
    };
    let decode = |bytes: &[u8]| {
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
    };
    let mut wrapper: serde_json::Value = decode(&journal.load(&projection, None)?.await?)?;
    let checkpoint: FileRef = serde_json::from_value(
        wrapper
            .get("checkpoint")
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture checkpoint missing".into()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut envelope: CanonicalContextCheckpoint =
        serde_json::from_slice(&journal.load(&checkpoint, None)?.await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut source: Context = serde_json::from_slice(&journal.load(&envelope.source, None)?.await?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let proof: CompactionReference =
        serde_json::from_slice(&journal.load(&compaction, None)?.await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut summary = proof
        .summary
        .ok_or_else(|| Error::Invalid("fixture summary missing".into()))?;
    if mode == 1 {
        source
            .messages
            .last_mut()
            .ok_or_else(|| Error::Invalid("fixture source empty".into()))?
            .content = ModelContent::Text("fabricated current".into());
    } else {
        summary.output = journal
            .stage(
                operation,
                "poison-summary",
                b"fabricated summary",
                "text/plain",
                None,
            )?
            .await?;
    }
    let (retained, proof) = DurableContextProvider::compact(
        &source,
        proof.maximum_messages as usize,
        Some(summary),
        &proof.retention,
        None,
    )?;
    if mode == 1 {
        envelope.source = stage_fixture_json(journal, operation, "poison-source", &source).await?;
    }
    envelope.retained =
        stage_fixture_json(journal, operation, "poison-retained", &retained).await?;
    envelope.compaction = journal
        .stage(
            operation,
            "poison-proof",
            &proof.encode()?,
            "application/json",
            None,
        )?
        .await?;
    envelope.validate_projection(&source, &retained, &proof, Limits::default(), None)?;
    let checkpoint = journal
        .stage(
            operation,
            "poison-checkpoint",
            &envelope.encode(Limits::default())?,
            "application/json",
            None,
        )?
        .await?;
    *wrapper
        .get_mut("canonical")
        .ok_or_else(|| Error::Invalid("fixture canonical missing".into()))? =
        serde_json::to_value(&envelope.source)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    *wrapper
        .get_mut("checkpoint")
        .ok_or_else(|| Error::Invalid("fixture checkpoint missing".into()))? =
        serde_json::to_value(checkpoint).map_err(|error| Error::Invalid(error.to_string()))?;
    let projection = stage_fixture_json(journal, operation, "poison-projection", &wrapper).await?;
    Ok(ExecutionEvent::ContextCompacted {
        step,
        projection,
        compaction: envelope.compaction,
        accounting,
    })
}

#[tokio::test]
pub(super) async fn canonical_publication_rejects_fabricated_source_and_summary_then_recovers()
-> Result<()> {
    use acyclic_harness::executor::{ExecutionEvent, ModelPurpose};
    for mode in [1, 2] {
        let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
        let (provider, _counts) = summary_model(5);
        let journal = Arc::new(LostCanonicalAck {
            inner: storage.journal(),
            fail: std::sync::atomic::AtomicBool::new(false),
            deny_reads: std::sync::atomic::AtomicBool::new(false),
            tamper: std::sync::atomic::AtomicUsize::new(mode),
        });
        let bundle = storage
            .builder()
            .journal(journal)
            .grant("model:generate")
            .model(
                Model::new(
                    "synthetic".into(),
                    "byte-counter".into(),
                    "1".into(),
                    serde_json::json!({}),
                )?,
                provider.clone(),
            )
            .build()?;
        storage
            .run_prompt(&bundle, &"q".repeat(60_000), Vec::new())
            .await?;
        let operation = OperationId::new();
        let file = storage
            .stage(
                operation,
                "fault/current.txt",
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "current.txt",
            )
            .await?;
        let error = storage
            .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
            .await;
        let expected = if mode == 1 {
            "canonical base and delta"
        } else {
            "output differs from admitted"
        };
        assert!(matches!(error, Err(Error::Conflict(message)) if message.contains(expected)));
        let records = storage.journal().replay(operation, 0, 64, None)?.await?;
        assert!(
            !records
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
        );
        let output = storage
            .run_conversation(&bundle, operation, file, Vec::new(), 1)
            .await?;
        assert_eq!(output.text, "retained summary");
        let records = storage.journal().replay(operation, 0, 64, None)?.await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.event,
                    ExecutionEvent::ModelStarted {
                        purpose: ModelPurpose::Summary,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
                .count(),
            1
        );
        assert_eq!(provider.requests().len(), 3);
        let previous_reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
            storage.journal().as_ref(),
            operation,
            Limits::default(),
            None,
        )
        .await?
        .ok_or_else(|| Error::NotFound("previous published checkpoint".into()))?;
        let (_, previous_retained) = acyclic_harness::executor::load_canonical_checkpoint(
            storage.journal().as_ref(),
            &previous_reference,
            Limits::default(),
            None,
            None,
        )
        .await?;
        assert_checkpoint_metadata_continuation(
            &storage,
            &bundle,
            &previous_reference,
            &previous_retained,
        )
        .await?;
        assert_eq!(provider.remaining(), 0);
    }
    Ok(())
}

pub(super) async fn append_private_checkpoint_metadata(
    storage: &Arc<MemoryHarnessStorage>,
) -> Result<(FileRef, Vec<uuid::Uuid>)> {
    let metadata = storage
        .stage(
            OperationId::new(),
            "fault/metadata.txt",
            b"private timeline metadata",
            "text/plain",
            "metadata.txt",
        )
        .await?;
    let mut history = acyclic_harness::store::StreamAggregate::open(
        storage.stream(),
        (
            storage.conversation().clone(),
            storage.verifier(),
            acyclic_harness::core::SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(StoredContent(storage.clone())));
    let mut metadata_ids = Vec::new();
    for kind in [
        acyclic_harness::conversation::MessageKind::Interaction,
        acyclic_harness::conversation::MessageKind::Permission,
    ] {
        let sequence = history
            .reducer()
            .conversation()
            .ok_or_else(|| Error::NotFound("canonical conversation".into()))?
            .messages()
            .len() as u64
            + 1;
        let id = uuid::Uuid::new_v4();
        metadata_ids.push(id);
        history
            .execute(
                acyclic_harness::core::Command {
                    operation_id: OperationId::new(),
                    idempotency_key: acyclic_harness::IdempotencyKey::new(format!(
                        "cold-metadata:{id}"
                    ))?,
                    expected_revision: history.reducer().revision(),
                    scope: storage.owner_scope().clone(),
                    causal_parent: None,
                    action: acyclic_harness::core::Action::AppendConversationMessage {
                        message: Box::new(acyclic_harness::conversation::ConversationMessage {
                            id,
                            sequence,
                            kind,
                            content: metadata.clone(),
                            attachments:
                                acyclic_harness::conversation::ReferencedAttachments::Inline {
                                    items: Vec::new(),
                                },
                            reply_to: None,
                            tool_call_id: None,
                            extensions: Default::default(),
                        }),
                    },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    Ok((metadata, metadata_ids))
}

pub(super) async fn assert_checkpoint_metadata_continuation(
    storage: &Arc<MemoryHarnessStorage>,
    bundle: &acyclic_harness::Harness,
    previous_reference: &FileRef,
    previous_retained: &Context,
) -> Result<()> {
    use acyclic_harness::executor::ExecutionEvent;
    let (metadata, metadata_ids) = append_private_checkpoint_metadata(storage).await?;
    let next_operation = OperationId::new();
    let next_file = storage
        .stage(
            next_operation,
            "fault/next.txt",
            "q".repeat(60_000).as_bytes(),
            "text/plain",
            "next.txt",
        )
        .await?;
    let next_output = storage
        .run_conversation(bundle, next_operation, next_file, Vec::new(), 1)
        .await?;
    assert_eq!(next_output.text, "retained summary");
    let next_reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        storage.journal().as_ref(),
        next_operation,
        Limits::default(),
        None,
    )
    .await?
    .ok_or_else(|| Error::NotFound("next published checkpoint".into()))?;
    let (next_envelope, _) = acyclic_harness::executor::load_canonical_checkpoint(
        storage.journal().as_ref(),
        &next_reference,
        Limits::default(),
        None,
        None,
    )
    .await?;
    assert_eq!(
        next_envelope.selection.checkpoint.as_ref(),
        Some(previous_reference)
    );
    assert!(
        metadata_ids
            .iter()
            .all(|id| !next_envelope.selection.message_ids.contains(id))
    );
    let source: Context =
        serde_json::from_slice(&storage.journal().load(&next_envelope.source, None)?.await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        source.messages.get(..previous_retained.messages.len()),
        Some(previous_retained.messages.as_slice())
    );
    assert!(
        source
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .all(|file| file != &metadata)
    );
    assert_eq!(
        storage
            .journal()
            .replay(next_operation, 0, 64, None)?
            .await?
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1
    );
    Ok(())
}
