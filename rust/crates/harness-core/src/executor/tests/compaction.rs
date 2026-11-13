use super::*;

#[tokio::test]
async fn conservative_admission_never_launches_an_unrequested_summary() -> Result<()> {
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            tool_turn([echo_call("next", json!({}))]),
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Content {
                    delta: "answer".into(),
                }),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({"usage":{"prompt_tokens":15_000,"completion_tokens":1}}),
                }),
            ]),
        ],
        [],
    );
    // The 58k byte bound admits this request but previously started an
    // automatic summary despite the provider's roughly 15k actual input.
    let source = crate::context::Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old".repeat(10_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("answer".repeat(4_666)),
            },
        ],
        ..Default::default()
    };
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "reported-usage".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(source),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        echo_tools(
            Arc::new(FakeTool(AtomicUsize::new(0))),
            json!(true),
            json!(true),
            json!(true),
        )?,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["model:generate", "tool:call:example.echo"]),
            Limits::default(),
        )?,
        allowed()?,
    )?;
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("question".into()),
        selected_context: None,
    };
    let output = executor.execute(input.clone(), journal.as_ref()).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(output.steps, 2);
    assert_eq!(executor.execute(input, journal.as_ref()).await?, output);
    assert_eq!(provider.requests().len(), 2);
    assert!(provider.requests().iter().all(|request| !request.summary));
    Ok(())
}

#[tokio::test]
async fn length_cut_summary_retains_original_usage_without_publishing_a_rewrite() -> Result<()> {
    let (provider, _) = counted_model(
        65_536,
        None,
        [ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "unfinished".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"finish_reason":"length","usage":{"prompt_tokens":15_000,"completion_tokens":8192}}),
            }),
        ])],
        [],
    );
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "summary".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("source".into()),
        }],
        ..Default::default()
    };
    let operation = OperationId::new();
    for _ in 0..2 {
        assert!(
            matches!(executor.summarize(&journal, operation, source.clone(),
            ModelContent::Text("summarize".into())).await,
            Err(Error::Invalid(reason)) if reason == "summary reached its output-token limit; request a shorter summary")
        );
    }
    assert_eq!(
        provider.requests().len(),
        1,
        "replay must not redispatch a cut summary"
    );
    assert!(
        journal
            .1
            .lock()
            .unwrap()
            .keys()
            .all(|key| !key.ends_with(":output"))
    );
    let records = journal.0.lock().unwrap();
    assert!(records.iter().any(|record| matches!(
        &record.event,
        ExecutionEvent::Model { usage: Some(_), .. }
            | ExecutionEvent::ModelInline { usage: Some(_), .. }
    )));
    assert!(
        !records
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
    );
    Ok(())
}

#[tokio::test]
async fn incomplete_summaries_reconcile_until_completion_without_redispatch() -> Result<()> {
    for prefix in ["", "first-"] {
        let observed = if prefix.is_empty() {
            vec![]
        } else {
            vec![ModelEvent::Content {
                delta: prefix.into(),
            }]
        };
        let mut expanded = observed.clone();
        expanded.push(ModelEvent::Content {
            delta: "second-".into(),
        });
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Events(
                observed.iter().cloned().map(ScriptedEvent::Event).collect(),
            )])
            .with_reconciliations([
                ScriptedReconciliation {
                    orphan: false,
                    observed,
                    continuation: Some(vec![ModelEvent::Content {
                        delta: "second-".into(),
                    }]),
                },
                ScriptedReconciliation {
                    orphan: false,
                    observed: expanded,
                    continuation: Some(vec![
                        ModelEvent::Content {
                            delta: "restored".into(),
                        },
                        ModelEvent::Completed {
                            metadata: Value::Null,
                        },
                    ]),
                },
            ]),
        );
        let tools = echo_tools(
            Arc::new(FakeTool(AtomicUsize::new(0))),
            json!(true),
            json!(true),
            json!(true),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let tools = {
            let mut tools = tools;
            tools.register(
                (
                    crate::tools::catalog()
                        .find(|spec| spec.name == "code")
                        .unwrap()
                        .bind()?,
                    None,
                ),
                &mut None,
                None,
            )?;
            tools
        };
        let executor = uncompacted_executor(
            Model::new("test".into(), "summary".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new([
                    "tool:call:code",
                    "tool:call:example.echo",
                    capability::INTERACTION_ROUTE,
                ]),
                Limits::default(),
            )?,
            allowed()?,
        )?;
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let journal = journal.as_ref();
        let operation = OperationId::new();
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            ..Default::default()
        };
        let instruction = ModelContent::Text("Summarize this context".into());
        for _ in 0..2 {
            assert!(
                matches!(executor.summarize(journal, operation, source.clone(), instruction.clone()).await,
                Err(Error::Indeterminate(id)) if id == operation)
            );
        }
        let summary = executor
            .summarize(journal, operation, source.clone(), instruction.clone())
            .await?;
        assert_eq!(
            journal.load(&summary.output, None)?.await?.as_ref(),
            format!("{prefix}second-restored").as_bytes()
        );
        assert_eq!(
            executor
                .summarize(journal, operation, source, instruction)
                .await?,
            summary
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].summary);
        assert_eq!(requests[0].compose_tools, !cfg!(target_arch = "wasm32"));
        assert!(!requests[0].tools.is_empty());
        let (context, summary_instruction) = requests[0].split_summary_instruction()?;
        assert_eq!(context.len(), 1);
        assert_eq!(
            summary_instruction,
            Some(&ModelContent::Text("Summarize this context".into()))
        );
        assert_eq!(provider.reconciliations().len(), 2);
    }
    Ok(())
}

#[test]
fn capacity_rewrite_publishes_its_own_context_occupancy() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for output_capacity in [Some(4096), Some(8192)] {
        let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
            crate::AgentId::new(),
            65_536,
        ))?;
        let journal = storage.journal();
        let journal = journal.as_ref();
        let (provider, _accounting_calls) = counted_model(
            65_536,
            None,
            [
                ScriptedTurn::Text(
                    r#"{"version":1,"drop_through":3,"summary":"MANAGED_SUMMARY"}"#.into(),
                ),
                ScriptedTurn::Text("answer".into()),
            ],
            [],
        );
        let provider = Arc::new(
            Arc::into_inner(provider)
                .expect("unique fixture model")
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 65_536,
                    output_tokens: output_capacity,
                }),
        );
        let source = crate::context::Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("keep".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("o".repeat(25_000)),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("a".repeat(25_000)),
                },
            ],
            ..crate::context::Context::default()
        };
        let executor = StockExecutor::new(
            Model::new("synthetic".into(), "byte-counter".into(), "1".into())?,
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
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("c".repeat(20_000)),
            selected_context: None,
        };
        let output = runtime.block_on(executor.execute(input.clone(), journal))?;
        assert_eq!(output.text, "answer");
        assert_eq!(output.steps, 1, "context management is not a worker step");
        assert_eq!(
            runtime.block_on(executor.execute(input.clone(), journal))?,
            output
        );
        let usage = runtime.block_on(crate::executor::usage_page(
            journal,
            input.operation_id,
            "agents/context-window",
            0,
        ))?;
        let Some(crate::executor::ContextUsage::Counted {
            input_tokens,
            capacity_tokens,
        }) = usage.context
        else {
            return Err(Error::Invalid(
                "rewritten response has no occupancy receipt".into(),
            ));
        };
        assert_eq!(capacity_tokens, 65_536);
        assert!(
            input_tokens < 21_000,
            "occupancy kept the pre-rewrite input count"
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].summary);
        assert!(!requests[1].summary);
        let response = &requests[1];
        assert_eq!(
            provider.context_capacity(&requests[0].model)?.output_tokens,
            output_capacity
        );
        assert_eq!(response.messages.len(), 3);
        assert_eq!(
            response.messages[0].content,
            ModelContent::Text("keep".into())
        );
        let ModelContent::Part(ModelContentPart::File { file, .. }) = &response.messages[1].content
        else {
            panic!("the selected summary follows the leading system message");
        };
        assert_eq!(
            runtime.block_on(journal.load(file, None)?)?.as_ref(),
            b"MANAGED_SUMMARY"
        );
        assert_eq!(
            response.messages.last().map(|message| &message.content),
            Some(&ModelContent::Text("c".repeat(20_000)))
        );
    }
    Ok(())
}

#[tokio::test]
async fn rejected_manager_rewrites_preserve_history_and_never_dispatch_the_worker() -> Result<()> {
    for reply in [
        "not a rewrite",
        r#"{"version":2,"drop_through":2,"summary":"wrong version"}"#,
        r#"{"version":1,"drop_through":0,"summary":"empty range"}"#,
        r#"{"version":1,"drop_through":3,"summary":"unseen source"}"#,
        r#"{"version":1,"drop_through":1,"summary":"insufficient rewrite"}"#,
    ] {
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let (provider, _) = counted_model(32_768, None, [ScriptedTurn::Text(reply.into())], []);
        let source = crate::context::Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("o".repeat(10_000)),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("a".repeat(10_000)),
                },
            ],
            ..Default::default()
        };
        let executor = StockExecutor::new(
            Model::new("fixture".into(), "managed-context".into(), "1".into())?,
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
            input: ModelContent::Text("current".repeat(3_000)),
            selected_context: None,
            max_steps: None,
        };
        for _ in 0..2 {
            assert!(
                matches!(
                    executor
                        .execute_progress(input.clone(), journal.as_ref())
                        .await?,
                    ExecutionProgress::PreDispatchFailed(_)
                ),
                "manager response {reply} must not authorize an unchecked worker request"
            );
        }
        let requests = provider.requests();
        assert_eq!(requests.len(), 1, "replay must not call the manager again");
        assert!(
            requests[0].summary,
            "a rejected rewrite must never dispatch the worker"
        );
        let records = journal.replay(input.operation_id, 0, 128, None)?.await?;
        assert!(
            records
                .iter()
                .all(|record| !matches!(record.event, ExecutionEvent::ContextCompacted { .. })),
            "a rejected rewrite must not replace its source history"
        );
    }
    Ok(())
}

#[test]
fn compacted_projection_recovers_before_response_admission() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for source in ["projected", "selected", "working"] {
        let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
            AgentId::new(),
            65_536,
        ))?;
        let journal = storage.journal();
        let journal = journal.as_ref();
        let (provider, accounting_calls) = counted_model(
            32_768,
            None,
            [ScriptedTurn::Text(
                r#"{"version":1,"drop_through":2,"summary":"retained history"}"#.into(),
            )],
            [],
        );
        let history = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("o".repeat(11_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("a".repeat(11_000)),
            },
        ];
        let pipeline = if source == "projected" {
            ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
                "history",
                "1",
                Arc::new(crate::context::Context {
                    messages: history.clone(),
                    ..Default::default()
                }),
                crate::context::ContextPlacement::Prepend,
            )) as Arc<dyn crate::context::ContextStage>])
        } else {
            ContextPipeline::default()
        };
        let executor = StockExecutor::new(
            Model::new("synthetic".into(), "byte-counter".into(), "1".into())?,
            provider.clone(),
            pipeline,
            ToolRegistry::default(),
        )
        .with_compaction_policy(crate::context::CompactionPolicy::Capacity(
            crate::context::CapacityCompaction::default(),
        ));
        let mut input = TurnInput {
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("current".repeat(2_000)),
            selected_context: None,
        };
        if source == "selected" {
            let mut messages = history.clone();
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: input.input.clone(),
            });
            input.selected_context = Some(SelectedModelContext {
                selection: crate::conversation::ModelContextSelection {
                    conversation_revision: messages.len() as u64,
                    message_ids: (1..=messages.len())
                        .map(|index| uuid::Uuid::from_u128(index as u128))
                        .collect(),
                    checkpoint: None,
                },
                messages,
            });
        }
        let (step, prior) = if source == "working" {
            (1, history)
        } else {
            (0, Vec::new())
        };
        let execution = StockExecution::ordinary(&executor);
        runtime.block_on(execution.ensure_started(journal, &input))?;
        let original =
            runtime.block_on(execution.prepared_response(journal, &input, step, &prior, None))?;
        let counted = accounting_calls.load(Ordering::SeqCst);
        let restarted_executor = executor.clone();
        let restarted = StockExecution::ordinary(&restarted_executor);
        let (recovered, _trace) = acyclic_test_support::differential::observe::<
            _,
            _,
            (&'static str, u64),
        >(
            || (),
            |()| runtime.block_on(restarted.prepared_response(journal, &input, step, &prior, None)),
        );
        let recovered = recovered?;
        assert_eq!(original.bytes(), recovered.bytes(), "{source}");
        assert_eq!(accounting_calls.load(Ordering::SeqCst), counted);
        assert_eq!(provider.request_count(), 1);
        let (_, records) = runtime.block_on(execution.model_records(
            journal,
            input.operation_id,
            step,
            ModelPurpose::Response,
        ))?;
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
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            _trace
                .iter()
                .filter(|(kind, _)| *kind == "clone request messages")
                .count(),
            if source == "projected" { 2 } else { 0 },
            "{source}: recovery copied messages unused by its checkpoint proof"
        );
    }
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
        let (provider, _accounting_calls) = counted_model(65_536, Some(fault), [], []);
        let executor = StockExecutor::new(
            Model::new("synthetic".into(), "counter".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let input = TurnInput {
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("current".into()),
            selected_context: None,
        };
        assert!(matches!(
            executor.execute_progress(input, &journal).await?,
            ExecutionProgress::PreDispatchFailed(_)
        ));
        let records = journal.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].event, ExecutionEvent::Started { .. }));
        assert!(journal.1.lock().unwrap().is_empty());
        assert!(provider.requests().is_empty());
        assert!(provider.dispatches().is_empty());
    }
    Ok(())
}

#[test]
fn accounting_wire_rejects_unknown_negative_and_out_of_range_counts() -> Result<()> {
    let valid = json!({
        "capacity": {"context_tokens": 65_536, "output_tokens": 4_096},
        "count": {"kind": "counted", "request_digest": vec![0_u8; 32], "input_tokens": 110, "message_count": 1}
    });
    assert!(serde_json::from_value::<ContextAccounting>(valid.clone()).is_ok());
    for path in ["capacity", "count"] {
        let mut unknown = valid.clone();
        unknown[path]["unknown"] = json!(true);
        assert!(serde_json::from_value::<ContextAccounting>(unknown).is_err());
    }
    for invalid in [
        json!(-1),
        json!(18_446_744_073_709_551_616_f64),
        json!(0.5),
        Value::Null,
    ] {
        for field in ["input_tokens", "message_count"] {
            let mut count = valid.clone();
            count["count"][field] = invalid.clone();
            assert!(serde_json::from_value::<ContextAccounting>(count).is_err());
        }
    }
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
        let (provider, _accounting_calls) = counted_model(32_768, None, [], []);
        let executor = StockExecutor::new(
            Model::new("synthetic".into(), "counter".into(), "1".into())?,
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
            max_steps: None,
            operation_id,
            input: ModelContent::Text("c".repeat(6_000)),
            selected_context: None,
        };
        assert!(matches!(executor.execute_progress(input, &journal).await?,
                ExecutionProgress::PreDispatchFailed(failure) if failure.message().ends_with("mandatory content exceeds selected model capacity")));
        assert!(provider.requests().is_empty());
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
async fn oversized_summary_input_rejects_before_accounting_or_journal_publication() -> Result<()> {
    let (provider, accounting_calls) = counted_model(131_072, None, [], []);
    let base = uncompacted_executor(
        Model::new("test".into(), "bounded-summary".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
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
            context_messages: std::num::NonZeroUsize::new(1),
            render_bytes: Some(4_096),
            file_bytes: Some(file_bytes),
            ..Limits::default()
        };
        let executor = base.clone().with_limits(limits);
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
        let (generation, _) = storage.list_private_directory("", None, None, 1).await?;
        let journal = storage.journal();
        let journal = journal.as_ref();
        let operation = OperationId::new();
        assert!(matches!(
            executor
                .summarize(
                    journal,
                    operation,
                    crate::context::Context {
                        messages,
                        ..Default::default()
                    },
                    instruction,
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert!(journal.replay(operation, 0, 1, None)?.await?.is_empty());
        assert_eq!(
            storage.list_private_directory("", None, None, 1).await?.0,
            generation,
        );
    }
    assert_eq!(accounting_calls.load(Ordering::SeqCst), 0);
    assert!(provider.requests().is_empty());
    Ok(())
}

#[test]
fn explicit_summary_keeps_capacity_admission_when_compaction_is_disabled() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for (capacity, source_length, fault, counts) in [
        (4_096, 5_000, None, 1),
        (131_072, 1, Some(AccountingFault::Digest), 1),
    ] {
        let (provider, accounting_calls) = counted_model(capacity, fault, [], []);
        let executor = uncompacted_executor(
            Model::new("test".into(), "explicit-summary".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("x".repeat(source_length)),
            }],
            ..Default::default()
        };
        let error = runtime
            .block_on(executor.summarize(
                &journal,
                OperationId::new(),
                source,
                ModelContent::Text("Summarize the preceding context".into()),
            ))
            .err()
            .ok_or_else(|| Error::Invalid("invalid summary was admitted".into()))?;
        assert!(matches!(error, Error::Invalid(_)), "{error}");
        assert_eq!(accounting_calls.load(Ordering::SeqCst), counts);
        assert!(provider.requests().is_empty());
        assert!(journal.0.lock().unwrap().is_empty());
        assert!(journal.1.lock().unwrap().is_empty());
    }
    let (provider, accounting_calls) =
        counted_model(131_072, None, [ScriptedTurn::Text("summary".into())], []);
    let executor = uncompacted_executor(
        Model::new("test".into(), "explicit-summary".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::new(),
        65_536,
    ))?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("bounded source".into()),
        }],
        ..Default::default()
    };
    let operation = OperationId::new();
    let instruction = ModelContent::Text("Summarize the preceding context".into());
    let (summary, _preparations) =
        acyclic_test_support::differential::observe::<_, _, (&'static str, u64)>(
            || (),
            |()| {
                runtime.block_on(executor.summarize(
                    journal,
                    operation,
                    source.clone(),
                    instruction.clone(),
                ))
            },
        );
    let summary = summary?;
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(
        _preparations
            .iter()
            .filter_map(|(operation, _)| {
                matches!(*operation, "prepare request" | "count tokens").then_some(*operation)
            })
            .collect::<Vec<_>>(),
        // The second preparation is the real journal's canonical request
        // verification at ModelStarted. Dispatch reuses capacity admission.
        ["prepare request", "count tokens", "prepare request"],
        "summary dispatch must reuse admission and retain journal verification"
    );
    assert_eq!(
        runtime
            .block_on(journal.load(&summary.output, None)?)?
            .as_ref(),
        b"summary"
    );
    let count_calls = accounting_calls.load(Ordering::SeqCst);
    assert_eq!(count_calls, 1);
    assert_eq!(
        runtime.block_on(executor.summarize(journal, operation, source, instruction))?,
        summary
    );
    assert_eq!(accounting_calls.load(Ordering::SeqCst), count_calls);
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn summary_reconciles_exact_source_and_stages_durable_output() -> Result<()> {
    let provider = Arc::new(recoverable_model(1));
    let executor = uncompacted_executor(
        Model::new("test".into(), "summary".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
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
            .summarize(journal, operation, source.clone(), instruction.clone())
            .await
            .is_err()
    );
    let mut changed = source.clone();
    changed.messages[0].content = ModelContent::Text("different source".into());
    assert!(
        executor
            .summarize(journal, operation, changed, instruction.clone())
            .await
            .is_err()
    );
    assert!(
        executor
            .summarize(
                journal,
                operation,
                source.clone(),
                ModelContent::Text("different instruction".into()),
            )
            .await
            .is_err()
    );
    assert_eq!(provider.reconciliations().len(), 0);
    let summary = executor
        .summarize(journal, operation, source.clone(), instruction.clone())
        .await?;
    assert_eq!(
        summary.source_digest,
        crate::executor::json::canonical_json_digest(&source, None, &mut None)?
    );
    assert_eq!(
        journal.load(&summary.output, None)?.await?.as_ref(),
        format!("{}restored", "partial-".repeat(70)).as_bytes()
    );
    assert_eq!(
        executor
            .summarize(journal, operation, source, instruction)
            .await?,
        summary
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.reconciliations().len(), 1);
    Ok(())
}

#[tokio::test]
async fn summary_and_response_reconcile_inside_one_parent_execution() -> Result<()> {
    let provider = Arc::new(recoverable_model(2));
    let executor = uncompacted_executor(
        Model::new("test".into(), "summary".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let parent = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("parent response".into()),
        selected_context: None,
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
    assert_eq!(provider.reconciliations().len(), 0);
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
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(provider.reconciliations().len(), 2);
    let (_, admissions, _) = replay_execution(
        &journal,
        parent.operation_id,
        Some(3),
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

/// A completion-backed counter can refuse the original input before returning
/// usage. The saved refusal must compact once and admit only the reduced input.
#[tokio::test]
async fn counting_context_refusal_compacts_and_replays_without_recounting() -> Result<()> {
    for irreducible in [false, true] {
        let counted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = counted.clone();
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text(
                r#"{"version":1,"drop_through":3,"summary":"retained history"}"#.into(),
            )])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(4096),
            })
            .with_token_counter(move |request| {
                seen.lock().unwrap().push(request.request_digest());
                if request.bytes().len() > 5_000 {
                    return Err(Error::ModelContextOverflow);
                }
                fixture_token_count(request)
            }),
        );
        let executor = StockExecutor::new(
            Model::new("fixture".into(), "reported-usage".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_compaction_policy(crate::context::CompactionPolicy::Capacity(
            crate::context::CapacityCompaction::default(),
        ));
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text(if irreducible {
                "protected".repeat(2_000)
            } else {
                "question".into()
            }),
            selected_context: None,
        };
        let prior = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old".repeat(500)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("old answer".repeat(150)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("recent answer".repeat(200)),
            },
        ];
        let execution = StockExecution::ordinary(&executor);
        execution.ensure_started(&journal, &input).await?;
        let result = execution
            .prepared_response(&journal, &input, 1, &prior, None)
            .await;
        if irreducible {
            assert!(matches!(result, Err(Error::ModelContextOverflow)));
            assert_eq!(
                counted.lock().unwrap().len(),
                2,
                "an irreducible input must stop at the mandatory subset"
            );
        } else {
            let prepared = result?;
            assert!(prepared.bytes().len() < 5_000);
            let before = counted.lock().unwrap().clone();
            assert!(
                before.len() >= 3,
                "original refusal, mandatory proof, and reduced admission"
            );
            assert!(
                before.iter().skip(1).all(|digest| *digest != before[0]),
                "never retry the refused input"
            );
            let reopened = StockExecution::ordinary(&executor)
                .prepared_response(&journal, &input, 1, &prior, None)
                .await?;
            assert_eq!(reopened.request_digest(), prepared.request_digest());
            assert_eq!(
                *counted.lock().unwrap(),
                before,
                "replay uses the retained verified count"
            );
        }
        assert!(
            provider.request_count() == usize::from(!irreducible),
            "only an admissible summary may generate; the worker remains undispatched"
        );
    }
    Ok(())
}
struct CeilingCounter(crate::test_support::ScriptedModelProvider);

impl ModelProvider for CeilingCounter {
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.0.context_capacity(model)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
        ceiling: u64,
    ) -> crate::model::ModelStream<'a> {
        if request.bytes().len() as u64 <= ceiling {
            crate::model::ModelStream::from_count(async move {
                let bound = u32::try_from(request.bytes().len())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                Ok(crate::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    bound,
                    vec![0; request.request().messages.len()],
                ))
            })
        } else {
            self.0.count_tokens(request, ceiling)
        }
    }

    fn generate<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> crate::model::ModelStream<'a> {
        self.0.generate(request, dispatch)
    }
}

#[tokio::test]
async fn response_reserve_requests_a_tighter_count_before_rewriting() -> Result<()> {
    let provider = Arc::new(CeilingCounter(
        ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(8192),
            },
        ),
    ));
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "ceiling-counter".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(crate::context::Context {
                messages: vec![
                    ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("o".repeat(30_000)),
                    },
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text("a".repeat(30_000)),
                    },
                ],
                ..Default::default()
            }),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: None,
    };
    let output = executor.execute(input.clone(), journal.as_ref()).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(executor.execute(input, journal.as_ref()).await?, output);
    let requests = provider.0.requests();
    assert_eq!(
        requests.len(),
        1,
        "an exact count fits; no manager call is needed"
    );
    assert!(!requests[0].summary);
    let prepared =
        crate::model::PreparedModelRequest::prepare(requests[0].clone(), Limits::default(), None)?;
    assert!(
        (57_345..=65_536).contains(&prepared.bytes().len()),
        "the loose bound must distinguish the two ceilings"
    );
    Ok(())
}

#[tokio::test]
async fn oversized_history_uses_bounded_manager_parts_without_worker_steps_or_replay_collisions()
-> Result<()> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let (provider, _) = counted_model(
        32_768,
        None,
        [
            ScriptedTurn::Text(r#"{"version":1,"drop_through":3,"summary":"FIRST_FACT"}"#.into()),
            ScriptedTurn::Text(
                r#"{"version":1,"drop_through":4,"summary":"FIRST_FACT SECOND_FACT"}"#.into(),
            ),
            ScriptedTurn::Text("answer".into()),
        ],
        [],
    );
    let source = crate::context::Context {
        messages: (0..6)
            .map(|index| ModelMessage {
                role: if index % 2 == 0 {
                    ModelRole::User
                } else {
                    ModelRole::Assistant
                },
                content: ModelContent::Text(format!("FACT_{index} {}", "x".repeat(8_000))),
            })
            .collect(),
        ..Default::default()
    };
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "bounded-manager".into(), "1".into())?,
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
        input: ModelContent::Text("c".repeat(8_000)),
        selected_context: None,
        max_steps: None,
    };
    let output = executor.execute(input.clone(), journal.as_ref()).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(output.steps, 1, "manager parts cannot consume worker steps");
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[..2].iter().all(|request| request.summary));
    assert!(!requests[2].summary);
    assert!(requests[0].messages.iter().all(
        |message| !matches!(&message.content, ModelContent::Text(text) if text.contains("FACT_5"))
    ));
    let rolling = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.file_parts())
        .next()
        .ok_or_else(|| Error::Invalid("missing rolling summary".into()))?
        .0;
    assert_eq!(journal.load(rolling, None)?.await?.as_ref(), b"FIRST_FACT");
    let fresh = StockExecution::ordinary(&executor);
    for purpose in [ModelPurpose::Summary, ModelPurpose::SummaryPart(1)] {
        let (_, records) = fresh
            .model_records(journal.as_ref(), input.operation_id, 0, purpose)
            .await?;
        assert_eq!(records.iter().filter(|record| matches!(record.event, ExecutionEvent::ModelStarted { purpose: recorded, .. } if recorded == purpose)).count(), 1);
        assert!(records.iter().any(|record| matches!(record.event, ExecutionEvent::Model { purpose: recorded, .. } | ExecutionEvent::ModelInline { purpose: recorded, .. } if recorded == purpose)));
    }
    assert_eq!(
        executor.clone().execute(input, journal.as_ref()).await?,
        output
    );
    assert_eq!(
        provider.request_count(),
        3,
        "replay must not dispatch another manager part"
    );
    Ok(())
}

#[tokio::test]
async fn manager_shrinks_source_before_encoding_under_a_finite_file_bound() -> Result<()> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let (provider, _) = counted_model(
        65_536,
        None,
        [ScriptedTurn::Text(
            r#"{"version":1,"drop_through":1,"summary":"retained fact"}"#.into(),
        )],
        [],
    );
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "byte-bounded-manager".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_limits(Limits {
        file_bytes: Some(12_000),
        ..Limits::default()
    });
    let source = crate::context::Context {
        messages: (0..5)
            .map(|index| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!("FACT_{index} {}", "x".repeat(8_000))),
            })
            .collect(),
        current_input_index: Some(4),
        ..Default::default()
    };
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("continue".into()),
        selected_context: None,
        max_steps: None,
    };
    let digest = crate::context::prefix_digest(&source, 4, None)?;
    for _ in 0..2 {
        let execution = StockExecution::ordinary(&executor);
        let (summary, _, more) = execution
            .managed_summary(
                journal.as_ref(),
                (&input, 0),
                &source,
                &crate::context::CapacityCompaction::default(),
                (4, None),
                (0, None, digest),
            )
            .await?;
        assert_eq!(summary.source_messages, 1);
        assert!(more);
    }
    assert_eq!(
        provider.request_count(),
        1,
        "replay reuses the original admitted byte-bounded window"
    );
    assert!(provider.requests()[0].messages.iter().all(
        |message| !matches!(&message.content, ModelContent::Text(text) if text.contains("FACT_1"))
    ));
    Ok(())
}
