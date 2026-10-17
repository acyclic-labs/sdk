use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one default-path history, reload, replay and publication qualification sequence"
)]
pub(super) async fn default_canonical_continuation_keeps_stages_fresh_beyond_history_bound()
-> Result<()> {
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
    let (provider, _counts) = summary_model(20);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let model = Model::new(
        "synthetic".into(),
        "byte-counter".into(),
        "1".into(),
        serde_json::json!({}),
    )?;
    let mut pipeline = ContextPipeline::new([Arc::new(InputDependentStage {
        revision: 1,
        calls: calls.clone(),
    })
        as Arc<dyn acyclic_harness::context::ContextStage>]);
    let build = |pipeline| {
        storage
            .builder()
            .model(model.clone(), provider.clone())
            .grant("model:generate")
            .context(pipeline)
            .limits(limits)
            .build()
    };
    let mut bundle = build(pipeline.clone())?;
    let mut operations = Vec::new();
    for turn in 0..10 {
        if turn == 5 {
            pipeline = pipeline.reload(ContextPipeline::new([Arc::new(InputDependentStage {
                revision: 2,
                calls: calls.clone(),
            })
                as Arc<dyn acyclic_harness::context::ContextStage>]))?;
            bundle = build(pipeline.clone())?;
        }
        let operation = OperationId::new();
        let file = storage
            .stage(
                operation,
                &format!("turns/{operation}/input.txt"),
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "input.txt",
            )
            .await?;
        let output = storage
            .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
            .await?;
        operations.push((operation, file, output));
    }
    let generated = assert_fresh_stage_requests(&provider, limits);
    let stage_calls = calls.load(Ordering::SeqCst);
    let (old_operation, old_file, old_output) = operations
        .get(6)
        .ok_or_else(|| Error::Invalid("no archived completed turn".into()))?;
    assert_eq!(
        &storage
            .run_conversation(&bundle, *old_operation, old_file.clone(), Vec::new(), 1)
            .await?,
        old_output
    );
    let (operation, file, expected) = operations
        .last()
        .ok_or_else(|| Error::Invalid("no completed default turn".into()))?;
    assert_eq!(
        &storage
            .run_conversation(&bundle, *operation, file.clone(), Vec::new(), 1)
            .await?,
        expected
    );
    assert_eq!(provider.requests().len(), generated);
    assert_eq!(calls.load(Ordering::SeqCst), stage_calls);

    let journal = storage.journal();
    let reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        journal.as_ref(),
        *operation,
        limits,
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("default path did not publish checkpoint".into()))?;
    let (checkpoint, retained) = acyclic_harness::executor::load_canonical_checkpoint(
        journal.as_ref(),
        &reference,
        limits,
        None,
        None,
    )
    .await?;
    let source: Context = serde_json::from_slice(&storage.read(&checkpoint.source).await?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        source
            .messages
            .iter()
            .all(|message| message.role != ModelRole::System)
    );
    assert!(
        retained
            .messages
            .iter()
            .all(|message| message.role != ModelRole::System)
    );
    assert_eq!(
        (
            checkpoint.selection.conversation_revision,
            checkpoint.selection.message_ids.len()
        ),
        (19, 2)
    );
    assert!(checkpoint.selection.checkpoint.is_some());
    assert_scoped_checkpoint_source(journal.as_ref(), &reference, limits, &retained).await?;
    assert_checkpoint_history_cut(&storage, journal.as_ref(), &reference, limits, &retained)
        .await?;
    assert_checkpoint_tail_source(
        storage.clone(),
        journal.as_ref(),
        &reference,
        limits,
        &retained,
    )
    .await?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one actual checkpoint admission with owner, cursor and encoded-byte negative controls"
)]
pub(super) async fn assert_checkpoint_history_cut(
    storage: &MemoryHarnessStorage,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    expected: &Context,
) -> Result<()> {
    use acyclic_harness::store::HistoryReader;
    use acyclic_harness::{executor::load_canonical_checkpoint_at, runtime::RuntimeScope};

    let history = HistoryReader::new(
        storage.stream(),
        storage.conversation(),
        &(storage.verifier()),
        &mut None,
    )?;
    let cursor = history.pin(0, None).await?;
    let scope = RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?;
    let bytes = MAX_RECORD_BYTES as u64 * 2;
    let (checkpoint, retained, _) = load_canonical_checkpoint_at(
        journal,
        reference,
        limits,
        &scope,
        (&history, &cursor),
        bytes,
        None,
    )
    .await?;
    assert_eq!(&retained, expected);
    let foreign = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    assert!(matches!(
        load_canonical_checkpoint_at(
            foreign.journal().as_ref(),
            reference,
            limits,
            &scope,
            (&history, &cursor),
            bytes,
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let unbound = LostCanonicalAck {
        inner: storage.journal(),
        fail: std::sync::atomic::AtomicBool::new(false),
        deny_reads: std::sync::atomic::AtomicBool::new(false),
        tamper: std::sync::atomic::AtomicUsize::new(0),
    };
    assert!(matches!(
        load_canonical_checkpoint_at(
            &unbound,
            reference,
            limits,
            &scope,
            (&history, &cursor),
            bytes,
            None
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    let (selection, _) = history
        .operation_event(checkpoint.operation_id, bytes, &mut None)
        .await?;
    let selection = selection.ok_or_else(|| Error::Invalid("missing actual selection".into()))?;
    let mut cut = cursor.clone();
    cut.through_revision = selection.revision - 1;
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cut),
            bytes,
            None
        )
        .await,
        Err(Error::Conflict(_))
    ));
    cut = cursor.clone();
    cut.after_revision = selection.revision;
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cut),
            bytes,
            None
        )
        .await,
        Err(Error::Conflict(_))
    ));
    cut = cursor.clone();
    cut.authority.id = "foreign-checkpoint-owner".into();
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cut),
            bytes,
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    cut = cursor.clone();
    cut.through_revision += 1;
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cut),
            bytes,
            None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cursor),
            0,
            None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        load_canonical_checkpoint_at(
            journal,
            reference,
            limits,
            &scope,
            (&history, &cursor),
            1,
            None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

pub(super) struct TailCheckpointRead<'a, P> {
    operation_id: OperationId,
    budget: acyclic_harness::store::HistoryReadLimits,
    journal: &'a dyn acyclic_harness::executor::ExecutionJournal,
    reference: &'a FileRef,
    limits: Limits,
    history: &'a acyclic_harness::store::HistoryReader<P>,
    cursor: &'a acyclic_harness::store::HistoryCursor,
    content: &'a StoredContent,
}

impl<P: StreamProvider> TailCheckpointRead<'_, P> {
    async fn assert_read_controls(
        &self,
        original: &acyclic_harness::runtime::RuntimeScope,
        retained: &Context,
        tail_message: &acyclic_harness::conversation::ConversationMessage,
        imported: &Context,
    ) -> Result<acyclic_harness::runtime::RuntimeScope> {
        use acyclic_harness::executor::load_canonical_checkpoint_through;
        use acyclic_harness::store::HistoryReadLimits;
        use acyclic_harness::{Capabilities, runtime::RuntimeScope};
        let Self {
            operation_id,
            budget,
            journal,
            reference,
            limits,
            history,
            cursor,
            content,
        } = *self;
        let (_, proof_bytes) = history
            .operation_event(operation_id, budget.maximum_bytes, &mut None)
            .await?;
        for (through, allowance) in [
            (18, budget),
            (
                20,
                HistoryReadLimits {
                    maximum_events: 1,
                    ..budget
                },
            ),
            (
                20,
                HistoryReadLimits {
                    maximum_bytes: proof_bytes,
                    ..budget
                },
            ),
            (
                20,
                HistoryReadLimits {
                    maximum_events: 0,
                    ..budget
                },
            ),
        ] {
            assert!(matches!(
                load_canonical_checkpoint_through(
                    journal,
                    reference,
                    (limits, original),
                    (history, cursor),
                    (through, allowance),
                    content,
                    None
                )
                .await,
                Err(Error::Invalid(_))
            ));
        }
        let grants = retained
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .chain(retained.metadata.values())
            .map(|file| file.read_capability(&mut None))
            .collect::<Result<Vec<_>>>()?;
        let denied = RuntimeScope::new(Capabilities::new(grants.clone()), limits)?;
        assert!(matches!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (limits, &denied),
                (history, cursor),
                (20, budget),
                content,
                None
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        let exact = RuntimeScope::new(
            Capabilities::new(
                grants
                    .into_iter()
                    .chain([tail_message.content.read_capability(&mut None)?]),
            ),
            limits,
        )?;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (Limits::default(), &exact),
                (history, cursor),
                (20, budget),
                content,
                None
            )
            .await?
            .1,
            *imported
        );
        Ok(exact)
    }
    async fn assert_later_cut_controls(
        &self,
        reducer: &acyclic_harness::core::Reducer,
        original: &acyclic_harness::runtime::RuntimeScope,
        exact: &acyclic_harness::runtime::RuntimeScope,
        imported: &Context,
        prepared_summary: &acyclic_harness::fork::PreparedSummaryFork,
    ) -> Result<()> {
        use acyclic_harness::executor::load_canonical_checkpoint_through;
        use acyclic_harness::store::HistoryReadLimits;
        let Self {
            journal,
            reference,
            limits,
            history,
            cursor,
            content,
            budget,
            ..
        } = *self;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (limits, exact),
                (history, cursor),
                (20, budget),
                content,
                None
            )
            .await?
            .1,
            *imported
        );
        assert!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (limits, original),
                (history, cursor),
                (
                    21,
                    HistoryReadLimits {
                        maximum_events: 3,
                        ..budget
                    }
                ),
                content,
                None
            )
            .await
            .is_err()
        );
        assert!(matches!(
            acyclic_harness::fork::prepare_summary_fork_context(
                reducer,
                prepared_summary.selection().clone(),
                journal,
                original,
                history,
                cursor,
                content,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(prepared_summary.through_sequence(), 20);
        assert_eq!(prepared_summary.context(), imported);
        let later = history.pin(0, None).await?;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (limits, exact),
                (history, &later),
                (
                    21,
                    HistoryReadLimits {
                        maximum_events: 3,
                        ..budget
                    }
                ),
                content,
                None
            )
            .await?
            .1,
            *imported,
            "canonical interaction record consumes work but is not a model message"
        );
        assert!(matches!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                (limits, exact),
                (history, &later),
                (21, budget),
                content,
                None
            )
            .await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "actual published checkpoint plus exact tail, shared budgets, narrowed grants and ordinary source admission controls"
)]
pub(super) async fn assert_checkpoint_tail_source(
    storage: Arc<MemoryHarnessStorage>,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    retained: &Context,
) -> Result<()> {
    use acyclic_harness::{
        executor::load_canonical_checkpoint_through,
        runtime::RuntimeScope,
        store::{HistoryReadLimits, HistoryReader},
    };

    let history = HistoryReader::new(
        storage.stream(),
        storage.conversation(),
        &(storage.verifier()),
        &mut None,
    )?;
    let cursor = history.pin(0, None).await?;
    let content = StoredContent(storage.clone());
    let budget = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: MAX_RECORD_BYTES as u64 * 4,
    };
    let original = RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?;
    let (checkpoint, imported) = load_canonical_checkpoint_through(
        journal,
        reference,
        (limits, &original),
        (&history, &cursor),
        (20, budget),
        &content,
        None,
    )
    .await?;
    assert_eq!(checkpoint.selection.conversation_revision, 19);
    assert_eq!(imported.current_input_index, None);
    assert_eq!(imported.messages.len(), retained.messages.len() + 1);
    assert_eq!(
        imported.messages.get(..retained.messages.len()),
        Some(retained.messages.as_slice())
    );
    let tail = history
        .conversation_range(&cursor, 19, 20, budget, &mut None)
        .await?;
    assert_eq!(tail.len(), 1);
    let tail_message = tail
        .first()
        .ok_or_else(|| Error::NotFound("actual canonical tail".into()))?;
    assert_eq!(
        tail_message.kind,
        acyclic_harness::conversation::MessageKind::Assistant
    );
    assert_eq!(
        imported.messages.last(),
        Some(&ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![ModelContentPart::File {
                file: tail_message.content.clone(),
                policy: FileProjectionPolicy::BoundedFull,
            }]),
        })
    );
    let read = TailCheckpointRead {
        operation_id: checkpoint.operation_id,
        budget,
        journal,
        reference,
        limits,
        history: &history,
        cursor: &cursor,
        content: &content,
    };
    let exact = read
        .assert_read_controls(&original, retained, tail_message, &imported)
        .await?;
    let (model, _counts) = summary_model(1);
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "checkpoint-and-tail".into(),
            "1".into(),
            serde_json::json!({}),
        )?,
        model.clone(),
        ContextPipeline::new([Arc::new(SourceStage::new(
            "published-summary-and-tail",
            "1",
            Arc::new(imported.clone()),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn acyclic_harness::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_limits(limits);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("fresh input after exact summary and tail".into()),
        selected_context: None,
        max_steps: 1,
    };
    executor.execute(input.clone(), journal).await?;
    {
        let requests = model.requests();
        assert_eq!(requests.len(), 1);
        let messages = &requests
            .first()
            .ok_or_else(|| Error::Invalid("missing admitted request".into()))?
            .messages;
        assert_eq!(
            messages.get(..imported.messages.len()),
            Some(imported.messages.as_slice())
        );
        assert_eq!(
            messages.last().map(|message| &message.content),
            Some(&input.input)
        );
    }
    let mut aggregate = acyclic_harness::store::StreamAggregate::open(
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
    let prepared_summary = acyclic_harness::fork::prepare_summary_fork_context(
        aggregate.reducer(),
        acyclic_harness::fork::SummaryForkSelection {
            checkpoint: reference.clone(),
            limits,
            history_limits: budget,
        },
        journal,
        &original,
        &history,
        &cursor,
        &content,
    )
    .await?;
    assert_eq!(prepared_summary.parent(), aggregate.reducer().authority());
    assert_eq!(prepared_summary.parent_revision(), cursor.through_revision);
    assert_eq!(prepared_summary.through_sequence(), 20);
    assert_eq!(prepared_summary.selection().checkpoint, *reference);
    assert_eq!(prepared_summary.checkpoint(), &checkpoint);
    assert_eq!(prepared_summary.context(), &imported);
    let id = uuid::Uuid::new_v4();
    aggregate
        .execute(
            acyclic_harness::core::Command {
                operation_id: OperationId::new(),
                idempotency_key: acyclic_harness::IdempotencyKey::new(format!(
                    "checkpoint-tail-later:{id}"
                ))?,
                expected_revision: aggregate.reducer().revision(),
                scope: storage.owner_scope().clone(),
                causal_parent: None,
                action: acyclic_harness::core::Action::AppendConversationMessage {
                    message: Box::new(acyclic_harness::conversation::ConversationMessage {
                        id,
                        sequence: 21,
                        kind: acyclic_harness::conversation::MessageKind::Interaction,
                        content: tail_message.content.clone(),
                        attachments: acyclic_harness::conversation::ReferencedAttachments::Inline {
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
    read.assert_later_cut_controls(
        aggregate.reducer(),
        &original,
        &exact,
        &imported,
        &prepared_summary,
    )
    .await?;
    Ok(())
}

pub(super) async fn assert_scoped_checkpoint_source(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    retained: &Context,
) -> Result<()> {
    use acyclic_harness::executor::load_canonical_checkpoint;
    use acyclic_harness::{Capabilities, runtime::RuntimeScope};

    let grants = retained
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(retained.metadata.values())
        .map(|file| file.read_capability(&mut None))
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !grants.is_empty(),
        "published summary must retain file authority"
    );
    let scope = RuntimeScope::new(Capabilities::new(grants.clone()), limits)?;
    let (_, imported) =
        load_canonical_checkpoint(journal, reference, Limits::default(), Some(&scope), None)
            .await?;
    assert_eq!(&imported, retained);
    let denied = scope.narrow(
        scope
            .grants()
            .without(&Capabilities::new([grants.first().cloned().ok_or_else(
                || Error::Invalid("missing retained read grant".into()),
            )?])),
        limits,
    )?;
    assert!(matches!(
        load_canonical_checkpoint(journal, reference, Limits::default(), Some(&denied), None).await,
        Err(Error::Unauthorized(_))
    ));
    let narrow = scope.narrow(
        scope.grants().clone(),
        Limits {
            render_bytes: 1,
            ..limits
        },
    )?;
    assert!(matches!(
        load_canonical_checkpoint(journal, reference, Limits::default(), Some(&narrow), None).await,
        Err(Error::Invalid(_))
    ));

    let (model, _counts) = summary_model(1);
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "checkpoint-import".into(),
            "1".into(),
            serde_json::json!({}),
        )?,
        model.clone(),
        ContextPipeline::new([Arc::new(SourceStage::new(
            "published-summary",
            "1",
            Arc::new(imported),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn acyclic_harness::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_limits(limits);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("new admission after summary import".into()),
        selected_context: None,
        max_steps: 1,
    };
    executor.execute(input.clone(), journal).await?;
    {
        let requests = model.requests();
        assert_eq!(requests.len(), 1);
        let messages = &requests
            .first()
            .ok_or_else(|| Error::Invalid("missing admitted request".into()))?
            .messages;
        assert_eq!(
            messages.get(..retained.messages.len()),
            Some(retained.messages.as_slice())
        );
        assert_eq!(
            messages.last().map(|message| &message.content),
            Some(&input.input)
        );
    }
    let (_, unchanged) =
        load_canonical_checkpoint(journal, reference, Limits::default(), Some(&scope), None)
            .await?;
    assert_eq!(&unchanged, retained);
    Ok(())
}
