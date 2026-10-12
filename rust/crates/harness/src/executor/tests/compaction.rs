use super::*;

#[tokio::test]
async fn default_threshold_compacts_through_ordinary_model_admission() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 65_536,
        requests: Mutex::new(Vec::new()),
        ..CountedModel::default()
    });
    let source = crate::context::Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("keep".into()),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("o".repeat(28_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("a".repeat(28_000)),
            },
        ],
        ..crate::context::Context::default()
    };
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "byte-counter".into(),
            "1".into(),
            json!({}),
        )?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(source),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("c".repeat(5_000)),
        selected_context: None,
        max_steps: 1,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(executor.execute(input, &journal).await?, output);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].max_output_tokens, Some(4_096));
    assert_eq!(requests[1].max_output_tokens, Some(4_096));
    assert_eq!(requests[1].messages.len(), 4);
    assert!(matches!(
        &requests[1].messages[0].content,
        ModelContent::Part(ModelContentPart::File { .. })
    ));
    assert_eq!(
        requests[1].messages[1].content,
        ModelContent::Text("keep".into())
    );
    assert_eq!(
        requests[1].messages.last().map(|message| &message.content),
        Some(&ModelContent::Text("c".repeat(5_000)))
    );
    Ok(())
}

#[tokio::test]
async fn compacted_projection_recovers_before_response_admission() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 65_536,
        ..CountedModel::default()
    });
    let source = crate::context::Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("o".repeat(27_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("a".repeat(27_000)),
            },
        ],
        ..crate::context::Context::default()
    };
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "byte-counter".into(),
            "1".into(),
            json!({}),
        )?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(source),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("current".into()),
        selected_context: None,
        max_steps: 1,
    };
    StockExecution::ordinary(&executor)
        .ensure_started(&journal, &input)
        .await?;
    let original = StockExecution::ordinary(&executor)
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    let counted = provider.accounting_calls.load(Ordering::SeqCst);
    let recovered = StockExecution::ordinary(&executor.clone())
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    assert_eq!(original.bytes(), recovered.bytes());
    assert_eq!(provider.accounting_calls.load(Ordering::SeqCst), counted);
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    let records = journal.0.lock().unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1
    );
    assert!(!records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Response,
            ..
        }
    )));
    Ok(())
}

#[tokio::test]
async fn impossible_default_capacity_fails_before_turn_admission() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 32_768,
        requests: Mutex::new(Vec::new()),
        ..CountedModel::default()
    });
    let executor = StockExecutor::new(
        Model::new("synthetic".into(), "small".into(), "1".into(), json!({}))?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    assert!(
        executor
            .execute(
                TurnInput {
                    operation_id: OperationId::new(),
                    input: ModelContent::Text("current".into()),
                    selected_context: None,
                    max_steps: 1
                },
                &journal
            )
            .await
            .is_err()
    );
    assert!(journal.0.lock().unwrap().is_empty());
    assert!(journal.1.lock().unwrap().is_empty());
    assert!(provider.requests.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn malformed_provider_accounting_rejects_before_projection_or_model_publication() -> Result<()>
{
    for fault in [
        AccountingFault::Digest,
        AccountingFault::MissingMessage,
        AccountingFault::ExtraMessage,
    ] {
        let journal = Journal::default();
        let provider = Arc::new(CountedModel {
            capacity: 65_536,
            accounting_fault: Some(fault),
            ..CountedModel::default()
        });
        let executor = StockExecutor::new(
            Model::new(
                "synthetic".into(),
                "counter".into(),
                "1".into(),
                Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("current".into()),
            selected_context: None,
            max_steps: 1,
        };
        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Invalid(_))
        ));
        let records = journal.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].event, ExecutionEvent::Started { .. }));
        assert!(journal.1.lock().unwrap().is_empty());
        assert!(provider.requests.lock().unwrap().is_empty());
        assert!(provider.dispatches.lock().unwrap().is_empty());
    }
    Ok(())
}

#[test]
fn accounting_wire_rejects_unknown_negative_and_out_of_range_counts() -> Result<()> {
    let valid = json!({
        "capacity": {"context_tokens": 65_536, "output_tokens": 4_096},
        "count": {"request_digest": vec![0_u8; 32], "fixed_tokens": 100, "message_tokens": [10]}
    });
    assert!(serde_json::from_value::<ContextAccounting>(valid.clone()).is_ok());
    for path in ["capacity", "count"] {
        let mut unknown = valid.clone();
        unknown[path]["unknown"] = json!(true);
        assert!(serde_json::from_value::<ContextAccounting>(unknown).is_err());
    }
    for invalid in [json!(-1), json!(4_294_967_296_u64), json!(0.5), Value::Null] {
        let mut fixed = valid.clone();
        fixed["count"]["fixed_tokens"] = invalid.clone();
        assert!(serde_json::from_value::<ContextAccounting>(fixed).is_err());
        let mut message = valid.clone();
        message["count"]["message_tokens"][0] = invalid;
        assert!(serde_json::from_value::<ContextAccounting>(message).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn actual_compacted_accounting_rejects_response_without_repeating_summary() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 65_536,
        accounting_fault: Some(AccountingFault::CompactedPressure),
        ..CountedModel::default()
    });
    let source = Arc::new(ChangingCompactionSource(AtomicUsize::new(0)));
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "counter".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "changing-history",
            "1",
            source.clone(),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("c".repeat(5_000)),
        selected_context: None,
        max_steps: 1,
    };
    for _ in 0..2 {
        assert!(matches!(executor.execute(input.clone(), &journal).await,
                Err(Error::Invalid(message)) if message == "compacted mandatory content exceeds selected model capacity"));
    }
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 0);
    let records = journal.0.lock().unwrap();
    assert!(records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Summary,
            ..
        }
    )));
    assert!(!records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Response,
            ..
        }
    )));
    Ok(())
}

#[tokio::test]
async fn oversized_mandatory_instruction_or_native_media_rejects_before_summary() -> Result<()> {
    for media in [false, true] {
        let journal = Journal::default();
        let operation_id = OperationId::new();
        let mandatory = if media {
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::File {
                    file: journal
                        .stage(operation_id, "media", &vec![1; 50_000], "image/png", None)?
                        .await?,
                    policy: crate::model::FileProjectionPolicy::Native(Box::new(
                        crate::model::NativeMediaPolicy {
                            intent: crate::model::NativeMediaIntent::Image {
                                detail: crate::model::ImageDetail::Auto,
                            },
                            maximum_bytes: 50_000,
                            maximum_work: 1,
                            configuration: None,
                        },
                    )),
                }),
            }
        } else {
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("s".repeat(50_000)),
            }
        };
        let context = crate::context::Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("o".repeat(28_000)),
                },
                mandatory,
            ],
            ..crate::context::Context::default()
        };
        let provider = Arc::new(CountedModel {
            capacity: 65_536,
            ..CountedModel::default()
        });
        let executor = StockExecutor::new(
            Model::new(
                "synthetic".into(),
                "counter".into(),
                "1".into(),
                Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
                "history",
                "1",
                Arc::new(context),
                crate::context::ContextPlacement::Prepend,
            )) as Arc<dyn crate::context::ContextStage>]),
            ToolRegistry::default(),
        );
        let input = TurnInput {
            operation_id,
            input: ModelContent::Text("c".repeat(5_000)),
            selected_context: None,
            max_steps: 1,
        };
        assert!(matches!(executor.execute(input, &journal).await,
                Err(Error::Invalid(message)) if message == "mandatory content exceeds selected model capacity"));
        assert!(provider.requests.lock().unwrap().is_empty());
        assert!(
            !journal
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        );
    }
    Ok(())
}

#[tokio::test]
async fn automatic_summary_reconciles_from_captured_context_without_rereading_sources() -> Result<()>
{
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 65_536,
        interrupt_summary: true,
        ..CountedModel::default()
    });
    let source = Arc::new(ChangingCompactionSource(AtomicUsize::new(0)));
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "byte-counter".into(),
            "1".into(),
            json!({}),
        )?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "changing-history",
            "1",
            source.clone(),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("c".repeat(5_000)),
        selected_context: None,
        max_steps: 1,
    };
    assert!(executor.execute(input.clone(), &journal).await.is_err());
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    let output = executor.clone().execute(input.clone(), &journal).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(executor.execute(input, &journal).await?, output);
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn oversized_summary_input_rejects_before_accounting_or_journal_publication() -> Result<()> {
    let provider = Arc::new(CountedModel {
        capacity: 131_072,
        ..Default::default()
    });
    let base = uncompacted_executor(
        Model::new(
            "test".into(),
            "bounded-summary".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1_024)?;
    let message = |length| ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("x".repeat(length)),
    };
    for (messages, instruction, file_bytes) in [
        (
            vec![message(1), message(1)],
            ModelContent::Text("summary".into()),
            4_096,
        ),
        (
            vec![message(4_097)],
            ModelContent::Text("summary".into()),
            4_096,
        ),
        (
            vec![message(1)],
            ModelContent::Text("x".repeat(4_097)),
            4_096,
        ),
        (vec![message(128)], ModelContent::Text("summary".into()), 64),
    ] {
        let limits = Limits {
            context_messages: 1,
            render_bytes: 4_096,
            file_bytes,
            ..Limits::default()
        };
        let executor = base.clone().with_limits(limits);
        let journal = Journal::default();
        assert!(matches!(
            executor
                .summarize(
                    &journal,
                    OperationId::new(),
                    crate::context::Context {
                        messages,
                        ..Default::default()
                    },
                    instruction,
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert!(journal.0.lock().unwrap().is_empty());
        assert!(journal.1.lock().unwrap().is_empty());
    }
    assert_eq!(provider.accounting_calls.load(Ordering::SeqCst), 0);
    assert!(provider.requests.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn explicit_summary_keeps_capacity_admission_when_compaction_is_disabled() -> Result<()> {
    for (capacity, output, source_length, fault, counts, expected) in [
        (100, 1_024, 1, None, 0, "selected model capacity is invalid"),
        (131_072, 8_192, 1, None, 0, "summary output exceeds"),
        (4_096, 1_024, 5_000, None, 1, "summary input exceeds"),
        (
            131_072,
            1_024,
            1,
            Some(AccountingFault::Digest),
            1,
            "token count does not bind",
        ),
    ] {
        let provider = Arc::new(CountedModel {
            capacity,
            accounting_fault: fault,
            ..Default::default()
        });
        let executor = uncompacted_executor(
            Model::new(
                "test".into(),
                "explicit-summary".into(),
                "1".into(),
                Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_max_output_tokens(output)?;
        let journal = Journal::default();
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("x".repeat(source_length)),
            }],
            ..Default::default()
        };
        let error = executor
            .summarize(
                &journal,
                OperationId::new(),
                source,
                ModelContent::Text("Summarize the preceding context".into()),
            )
            .await
            .err()
            .ok_or_else(|| Error::Invalid("invalid summary was admitted".into()))?;
        assert!(matches!(error, Error::Invalid(ref message) if message.contains(expected)));
        assert_eq!(provider.accounting_calls.load(Ordering::SeqCst), counts);
        assert!(provider.requests.lock().unwrap().is_empty());
        assert!(journal.0.lock().unwrap().is_empty());
        assert!(journal.1.lock().unwrap().is_empty());
    }
    let provider = Arc::new(CountedModel {
        capacity: 131_072,
        ..Default::default()
    });
    let executor = uncompacted_executor(
        Model::new(
            "test".into(),
            "explicit-summary".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1_024)?;
    let journal = Journal::default();
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("bounded source".into()),
        }],
        ..Default::default()
    };
    let operation = OperationId::new();
    let instruction = ModelContent::Text("Summarize the preceding context".into());
    let summary = executor
        .summarize(&journal, operation, source.clone(), instruction.clone())
        .await?;
    assert_eq!(
        journal.load(&summary.output, None)?.await?.as_ref(),
        b"summary"
    );
    let count_calls = provider.accounting_calls.load(Ordering::SeqCst);
    assert_eq!(count_calls, 1);
    assert_eq!(
        executor
            .summarize(&journal, operation, source, instruction)
            .await?,
        summary
    );
    assert_eq!(
        provider.accounting_calls.load(Ordering::SeqCst),
        count_calls
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn summary_reconciles_exact_source_and_stages_durable_output() -> Result<()> {
    let provider = Arc::new(ScriptedModel::recoverable());
    let executor = uncompacted_executor(
        Model::new("test".into(), "summary".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1024)?;
    let journal = Journal::default();
    let operation = OperationId::new();
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("preserve Ã©\\0ðŸ¦€ and uncertainty".into()),
        }],
        metadata: Default::default(),
        current_input_index: None,
    };
    let instruction = ModelContent::Text("Summarize the preceding projection.".into());
    assert!(
        executor
            .summarize(&journal, operation, source.clone(), instruction.clone())
            .await
            .is_err()
    );
    let mut changed = source.clone();
    changed.messages[0].content = ModelContent::Text("different source".into());
    assert!(
        executor
            .summarize(&journal, operation, changed, instruction.clone())
            .await
            .is_err()
    );
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 0);
    let summary = executor
        .summarize(&journal, operation, source.clone(), instruction.clone())
        .await?;
    assert_eq!(
        summary.source_digest,
        crate::contract::canonical_json_digest(&source, u64::MAX, &mut None)?
    );
    assert_eq!(
        journal.load(&summary.output, None)?.await?.as_ref(),
        format!("{}restored", "partial-".repeat(70)).as_bytes()
    );
    assert_eq!(
        executor
            .summarize(&journal, operation, source, instruction)
            .await?,
        summary
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn summary_and_response_reconcile_inside_one_parent_execution() -> Result<()> {
    let provider = Arc::new(ScriptedModel::recoverable());
    let executor = uncompacted_executor(
        Model::new("test".into(), "summary".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1024)?;
    let journal = Journal::default();
    let parent = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("parent response".into()),
        selected_context: None,
        max_steps: 1,
    };
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("history to summarize".into()),
        }],
        metadata: Default::default(),
        current_input_index: None,
    };
    let instruction = ModelContent::Text("summarize history".into());
    assert!(
        executor
            .summarize_in_turn(&journal, &parent, 0, source.clone(), instruction.clone())
            .await
            .is_err()
    );
    let mut changed = source.clone();
    changed.messages[0].content = ModelContent::Text("changed history".into());
    assert!(
        executor
            .summarize_in_turn(&journal, &parent, 0, changed, instruction.clone())
            .await
            .is_err()
    );
    assert!(
        executor
            .summarize_in_turn(
                &journal,
                &parent,
                0,
                source.clone(),
                ModelContent::Text("different instruction".into())
            )
            .await
            .is_err()
    );
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 0);
    let summary = executor
        .summarize_in_turn(&journal, &parent, 0, source.clone(), instruction.clone())
        .await?;
    assert_eq!(summary.operation_id, parent.operation_id);
    assert_eq!(summary.step, 0);
    assert!(
        executor
            .model_step(&journal, &parent, 0, &[])
            .await
            .is_err()
    );
    let response = executor.model_step(&journal, &parent, 0, &[]).await?;
    assert!(matches!(
        response.last(),
        Some(ModelEvent::Completed { .. })
    ));
    assert_eq!(
        executor
            .summarize_in_turn(&journal, &parent, 0, source, instruction)
            .await?,
        summary
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 2);
    let (_, admissions) = replay_execution(
        &journal,
        parent.operation_id,
        3,
        |event| {
            matches!(
                event,
                ExecutionEvent::Started { .. } | ExecutionEvent::ModelStarted { .. }
            )
        },
        None,
    )
    .await?;
    assert_eq!(admissions.len(), 3);
    assert!(matches!(
        admissions.get(1).map(|record| &record.event),
        Some(ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Summary,
            ..
        })
    ));
    assert!(matches!(
        admissions.get(2).map(|record| &record.event),
        Some(ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Response,
            ..
        })
    ));
    Ok(())
}
