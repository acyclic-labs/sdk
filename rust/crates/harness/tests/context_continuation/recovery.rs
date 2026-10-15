use super::*;

pub(super) struct LostCanonicalAck {
    pub(super) inner: Arc<dyn acyclic_harness::executor::ExecutionJournal>,
    pub(super) fail: std::sync::atomic::AtomicBool,
    pub(super) deny_reads: std::sync::atomic::AtomicBool,
    pub(super) tamper: std::sync::atomic::AtomicUsize,
}

impl acyclic_harness::executor::ExecutionJournal for LostCanonicalAck {
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            Vec<acyclic_harness::executor::ExecutionRecord>,
        >,
    > {
        self.inner.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a acyclic_harness::executor::ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.append(operation, key, event, io)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a acyclic_harness::executor::ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, bool>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let compacted = matches!(
                event,
                acyclic_harness::executor::ExecutionEvent::ContextCompacted { step: 0, .. }
            );
            let mode = if compacted {
                self.tamper.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            let event = if mode == 0 {
                event
            } else {
                fabricate_checkpoint(self.inner.as_ref(), operation, event, mode).await?
            };
            let committed = self
                .inner
                .append_if_tail(operation, tail, key, &event, io)?
                .await?;
            if committed && compacted && self.fail.swap(false, Ordering::SeqCst) {
                Err(Error::Storage(
                    "lost canonical compaction acknowledgement".into(),
                ))
            } else {
                Ok(committed)
            }
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media_type, io)
    }
    fn load<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        self.inner.load(file, io)
    }
    fn verify_input_file<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        if self.deny_reads.load(Ordering::SeqCst) {
            acyclic_harness::conversation::BoundedContentFuture::new(io, || async {
                Err(Error::Unauthorized("retained payload read denied".into()))
            })
        } else {
            self.inner.verify_input_file(file, io)
        }
    }
    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a acyclic_harness::projection::SelectedModelContext,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.verify_selected_context(operation, selected, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
        interaction: &'a acyclic_harness::interaction::Interaction,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            Option<acyclic_harness::interaction::InteractionOutcome>,
        >,
    > {
        self.inner.interaction_outcome(id, io)
    }
}

#[tokio::test]
pub(super) async fn default_canonical_checkpoint_recovers_lost_commit_ack_without_recount_or_stage_replay()
-> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let (provider, counts) = summary_model(4);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let journal = Arc::new(LostCanonicalAck {
        inner: storage.journal(),
        fail: std::sync::atomic::AtomicBool::new(true),
        deny_reads: std::sync::atomic::AtomicBool::new(false),
        tamper: std::sync::atomic::AtomicUsize::new(0),
    });
    let bundle = storage
        .builder()
        .journal(journal.clone())
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
        .context(ContextPipeline::new([Arc::new(InputDependentStage {
            revision: 1,
            calls: calls.clone(),
        })
            as Arc<dyn acyclic_harness::context::ContextStage>]))
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
    assert!(matches!(error, Err(Error::Storage(message)) if message.contains("lost canonical")));
    assert!(
        acyclic_harness::executor::canonical_checkpoint_for_operation(
            storage.journal().as_ref(),
            operation,
            Limits::default(),
            None
        )
        .await?
        .is_some()
    );
    let generated = provider.requests().len();
    let counted = counts.load(Ordering::SeqCst);
    let transformed = calls.load(Ordering::SeqCst);
    assert!(
        storage
            .run_prompt(&bundle, "another turn", Vec::new())
            .await
            .is_err()
    );
    let output = storage
        .run_conversation(&bundle, operation, file, Vec::new(), 1)
        .await?;
    assert_eq!(output.text, "retained summary");
    assert_eq!(provider.requests().len(), generated + 1);
    assert_eq!(counts.load(Ordering::SeqCst), counted);
    assert_eq!(calls.load(Ordering::SeqCst), transformed);
    let records = storage.journal().replay(operation, 0, 64, None)?.await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                acyclic_harness::executor::ExecutionEvent::ContextCompacted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                acyclic_harness::executor::ExecutionEvent::ModelStarted {
                    purpose: acyclic_harness::executor::ModelPurpose::Summary,
                    ..
                }
            ))
            .count(),
        1
    );
    storage
        .run_prompt(&bundle, "after recovery", Vec::new())
        .await?;
    assert_checkpoint_read_admission(&storage, &journal, operation).await?;
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

pub(super) async fn assert_checkpoint_read_admission(
    storage: &MemoryHarnessStorage,
    journal: &LostCanonicalAck,
    operation: OperationId,
) -> Result<()> {
    let limits = Limits::default();
    let reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        journal, operation, limits, None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("recovered checkpoint missing".into()))?;
    let (mut envelope, _) = acyclic_harness::executor::load_canonical_checkpoint(
        journal, &reference, limits, None, None,
    )
    .await?;
    journal.deny_reads.store(true, Ordering::SeqCst);
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &reference, limits, None, None).await,
        Err(Error::Unauthorized(message)) if message.contains("retained payload"))
    );
    journal.deny_reads.store(false, Ordering::SeqCst);
    envelope.operation_id = OperationId::new();
    let unpublished = storage
        .stage(
            envelope.operation_id,
            "fault/unpublished-checkpoint.json",
            &envelope.encode(limits)?,
            "application/json",
            "checkpoint.json",
        )
        .await?;
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &unpublished, limits, None, None).await,
        Err(Error::Conflict(message)) if message.contains("not published"))
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real-journal admission, retry and operation-isolation timeline"
)]
pub(super) async fn local_journal_admits_compaction_only_after_one_settled_summary() -> Result<()> {
    use acyclic_harness::executor::{ExecutionEvent, ModelPurpose};
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let context = journal
        .stage(operation, "temporal-context", b"{\"canonical\":null,\"checkpoint\":null,\"context\":{\"current_input_index\":0,\"messages\":[{\"content\":{\"kind\":\"text\",\"text\":\"source\"},\"role\":\"user\"}],\"metadata\":{}}}", "application/json", None)?
        .await?;
    let compacted = ExecutionEvent::ContextCompacted {
        step: 0,
        projection: context.clone(),
        compaction: context.clone(),
        accounting: context.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 0, &"premature", &compacted, None)?
            .await
            .is_err()
    );
    journal
        .append(
            operation,
            &"started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            &"prepared",
            &ExecutionEvent::ContextPrepared {
                step: 0,
                projection: context.clone(),
                accounting: None,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, &"no-summary", &compacted, None)?
            .await
            .is_err()
    );
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new(
                "synthetic".into(),
                "journal-test".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    let request_ref = journal
        .stage(
            operation,
            "summary-request",
            &request.bytes(),
            "application/json",
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            &"summary-started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: request.request_digest(),
                request: request_ref,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, &"unsettled-summary", &compacted, None)?
            .await
            .is_err()
    );
    let completed = journal
        .stage(
            operation,
            "summary-completed",
            b"{\"kind\":\"completed\",\"metadata\":{}}",
            "application/json",
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            &"summary-completed",
            &ExecutionEvent::Model {
                step: 0,
                purpose: ModelPurpose::Summary,
                event: completed,
            },
            None,
        )?
        .await?;
    assert!(
        !journal
            .append_if_tail(operation, 3, &"stale-tail", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, &"compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, &"compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append(operation, &"duplicate-compacted", &compacted, None)?
            .await
            .is_err()
    );
    let other = OperationId::new();
    journal
        .append(
            other,
            &"started",
            &ExecutionEvent::Started {
                request_digest: [2; 32],
            },
            None,
        )?
        .await?;
    assert_eq!(journal.replay(operation, 0, 64, None)?.await?.len(), 5);
    assert_eq!(journal.replay(other, 0, 64, None)?.await?.len(), 1);
    assert!(
        journal
            .append_if_tail(
                operation,
                4,
                &"compacted",
                &journal
                    .replay(operation, 4, 1, None)?
                    .await?
                    .first()
                    .ok_or_else(|| Error::Invalid("compaction record missing".into()))?
                    .event,
                None
            )?
            .await?
    );
    Ok(())
}
