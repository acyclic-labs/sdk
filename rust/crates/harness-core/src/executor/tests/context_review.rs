use super::*;
use futures::FutureExt as _;

struct IgnoreDivergence;
impl crate::model::DivergenceSink for IgnoreDivergence {
    fn diverged(&self, _: crate::model::Divergence) {}
}

fn original_metadata(events: &[ModelEvent]) -> &Value {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            ModelEvent::Completed { metadata } => Some(metadata),
            _ => None,
        })
        .expect("the provider completed this request")
}

/// A provider-request count alone misses cancelled preparation. Count the
/// actual source construction, including a control restoring the old caller.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn terminal_replies_prepare_no_review_source_at_any_history_size() -> Result<()> {
    for count in [2, 64] {
        let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "done".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":1024,"completion_tokens":1}}),
            }),
        ])]));
        let stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 8192).await?;
        let journal_owner = storage.journal();
        let journal = journal_owner.as_ref();
        let mut messages: Vec<_> = (0..count)
            .map(|index| ModelMessage {
                role: if index % 2 == 0 {
                    ModelRole::User
                } else {
                    ModelRole::Assistant
                },
                content: ModelContent::Text(format!("Earlier finding {index}.")),
            })
            .collect();
        let input_text = ModelContent::Text("Complete the task.".into());
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: input_text.clone(),
        });
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: input_text,
            selected_context: Some(SelectedModelContext {
                selection: crate::conversation::ModelContextSelection {
                    conversation_revision: messages.len() as u64,
                    message_ids: (1..=messages.len())
                        .map(|index| uuid::Uuid::from_u128(index as u128))
                        .collect(),
                    checkpoint: None,
                },
                messages,
            }),
        };
        let (result, trace) = acyclic_test_support::differential::observe_async::<
            _,
            (&'static str, u64),
        >(stock.execute(input.clone(), journal))
        .await;
        let output = result?;
        assert_eq!(output.text, "done");
        assert!(
            !trace
                .iter()
                .any(|(event, _)| *event == "context review source messages")
        );
        let execution = StockExecution::ordinary(&stock);
        let old_caller = async {
            execution
                .request_context_review(journal, &input, 0, &[], &output.metadata)
                .await?;
            Ok(ExecutionProgress::Ready(()))
        };
        let (result, trace) = acyclic_test_support::differential::observe_async::<
            _,
            (&'static str, u64),
        >(execution.with_context_review(journal, &input, old_caller))
        .await;
        result?;
        let constructed: Vec<_> = trace
            .into_iter()
            .filter(|(event, _)| *event == "context review source messages")
            .collect();
        assert_eq!(
            constructed,
            vec![("context review source messages", count as u64)]
        );
        assert_eq!(
            provider.request_count(),
            1,
            "the negative control wastes preparation without generating another request"
        );
    }
    Ok(())
}

/// A blocked housekeeping request must neither stall the worker nor change
/// its cached prefix. Only a finished, source-bound decision may rewrite it.
#[tokio::test]
async fn model_budget_reviews_context_without_stalling_or_rewriting_live_prefixes() -> Result<()> {
    for (replace, change_source, tokenized) in [
        (false, false, true),
        (true, false, true),
        (true, true, true),
        (true, false, false),
    ] {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let (seen, gate, calls) = (started.clone(), release.clone(), dispatches.clone());
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":4_000,"completion_tokens":1,"prompt_tokens_details":{"cached_tokens":3_800}}}),
            })]),
            ScriptedTurn::Text(json!({
                "version": 1,
                "summary": if replace { Some("Retained decisions and unfinished work.") } else { None },
                "review_at_input_tokens": 8_000,
            }).to_string()),
            ScriptedTurn::Text("work continues".into()),
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Content { delta: "done".into() }),
                ScriptedEvent::Event(ModelEvent::Completed { metadata: json!({"usage":{"prompt_tokens":6_000,"completion_tokens":1}}) }),
            ]),
        ])
        .with_context_capacity(crate::context::ModelContextCapacity { context_tokens: 65_536, output_tokens: Some(4096) })
        .with_token_counter(move |request| if tokenized { fixture_token_count(request) } else { Err(Error::ModelTokenCountUnavailable) })
        .with_dispatch_observer(move |_| {
            let (seen, gate) = (seen.clone(), gate.clone());
            let index = calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if index == 1 {
                    seen.notify_one();
                    gate.notified().await;
                }
                Ok(())
            })
        }));
        let recorded = Arc::new(crate::model::RecordedModel::new(
            provider.clone(),
            Arc::new(crate::model::Recording::default()),
            Arc::new(IgnoreDivergence),
        ));
        let executor = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            recorded,
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let execution = StockExecution::ordinary(&executor);
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let journal_owner = storage.journal();
        let journal = journal_owner.as_ref();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("Continue the task.".into()),
            selected_context: None,
        };
        let mut prior = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old page ".repeat(2_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("Prior findings are recorded.".into()),
            },
        ];
        let initial_prior = prior.clone();
        // An immediately settled run leaves its notification unconsumed. A
        // subsequent review on the same session must use its own lifecycle.
        execution
            .with_context_review(journal, &input, async { Ok(ExecutionProgress::Ready(())) })
            .await?;
        let worker = async {
            let events = execution.model_step(journal, &input, 0, &prior).await?;
            execution
                .request_context_review(journal, &input, 0, &prior, original_metadata(&events))
                .await?;
            started.notified().await;
            let before = provider.request(0).unwrap();
            prior.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("New observation.".into()),
            });
            execution.model_step(journal, &input, 1, &prior).await?;
            let during = provider.request(2).unwrap();
            assert!(during.messages.starts_with(&before.messages));
            assert!(!during.summary);
            release.notify_one();
            execution.review.completed.notified().await;
            if change_source {
                prior[0].content = ModelContent::Text("Changed page ".repeat(2_000));
            }
            prior.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("Use the new observation.".into()),
            });
            let prepared = execution
                .prepared_response(journal, &input, 2, &prior, None)
                .await?;
            let window = prepared.context_window();
            assert_eq!(
                window.is_some(),
                tokenized,
                "a provider without a tokenizer must not invent occupancy"
            );
            if let Some(window) = window {
                assert_eq!(
                    window.input_tokens,
                    fixture_token_count(&prepared)?.input_tokens
                );
                assert_eq!(window.capacity_tokens, 65_536);
            }
            let events = execution.model_step(journal, &input, 2, &prior).await?;
            let after = provider.request(3).unwrap();
            assert!(!after.summary);
            if replace && !change_source {
                let bytes = crate::contract::canonical_json_bytes(&after)?;
                assert!(bytes.len() < crate::contract::canonical_json_bytes(&before)?.len());
                if let Some(window) = window {
                    assert!(
                        window.input_tokens
                            < crate::contract::canonical_json_bytes(&before)?.len() as u64
                    );
                }
                assert!(after.messages.iter().any(|message| {
                    message
                        .content
                        .parts()
                        .iter()
                        .any(|part| matches!(part, ModelContentPart::File { .. }))
                }));
                assert!(after.messages.iter().any(
                    |message| message.content == ModelContent::Text("New observation.".into())
                ));
            } else if !change_source {
                assert!(after.messages.starts_with(&during.messages));
            } else {
                assert!(
                    after
                        .messages
                        .iter()
                        .any(|message| message.content == prior[0].content)
                );
            }
            execution
                .request_context_review(journal, &input, 2, &prior, original_metadata(&events))
                .await?;
            assert!(
                execution
                    .review
                    .requested
                    .notified()
                    .now_or_never()
                    .is_none(),
                "worker usage below the model's chosen budget requested another review"
            );
            // Reopening the exact admitted boundary uses the durable cut and
            // provider completion, not another background or worker request.
            execution.model_step(journal, &input, 2, &prior).await?;
            assert_eq!(provider.request_count(), 4);
            Ok(ExecutionProgress::Ready(()))
        };
        assert!(matches!(
            execution
                .with_context_review(journal, &input, worker)
                .await?,
            ExecutionProgress::Ready(())
        ));
        assert_eq!(
            provider
                .requests()
                .iter()
                .filter(|request| request.summary)
                .count(),
            1
        );
        let reopened = StockExecution::ordinary(&executor);
        let events = reopened
            .model_step(journal, &input, 0, &initial_prior)
            .await?;
        let replay = async {
            reopened
                .request_context_review(
                    journal,
                    &input,
                    0,
                    &initial_prior,
                    original_metadata(&events),
                )
                .await?;
            assert!(
                reopened
                    .review
                    .requested
                    .notified()
                    .now_or_never()
                    .is_none(),
                "reopening a completed review requested another dispatch"
            );
            reopened.model_step(journal, &input, 2, &prior).await?;
            assert_eq!(provider.request_count(), 4);
            Ok(ExecutionProgress::Ready(()))
        };
        reopened
            .with_context_review(journal, &input, replay)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn invalid_context_decisions_preserve_history_and_original_admissions() -> Result<()> {
    for decision in [
        "not JSON".to_owned(),
        json!({"version":2,"summary":"replacement","review_at_input_tokens":500}).to_string(),
        json!({"version":1,"summary":"","review_at_input_tokens":500}).to_string(),
        json!({"version":1,"summary":null,"review_at_input_tokens":250}).to_string(),
        json!({"version":1,"summary":"replacement","review_at_input_tokens":8192}).to_string(),
    ] {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (seen, gate) = (started.clone(), release.clone());
        let calls = AtomicUsize::new(0);
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({"usage":{"prompt_tokens":250,"completion_tokens":1}}),
                })]),
                ScriptedTurn::Text(decision),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 8192,
                output_tokens: Some(512),
            })
            .with_token_counter(fixture_token_count)
            .with_dispatch_observer(move |_| {
                let index = calls.fetch_add(1, Ordering::SeqCst);
                let (seen, gate) = (seen.clone(), gate.clone());
                Box::pin(async move {
                    if index == 1 {
                        seen.notify_one();
                        gate.notified().await;
                    }
                    Ok(())
                })
            }),
        );
        let stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let execution = StockExecution::ordinary(&stock);
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 8192).await?;
        let journal_owner = storage.journal();
        let journal = journal_owner.as_ref();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("Continue.".into()),
            selected_context: None,
        };
        let prior = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("Earlier work.".repeat(32)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("Evidence retained.".into()),
            },
        ];
        let worker = async {
            let events = execution.model_step(journal, &input, 0, &prior).await?;
            execution
                .request_context_review(journal, &input, 0, &prior, original_metadata(&events))
                .await?;
            started.notified().await;
            release.notify_one();
            execution.review.completed.notified().await;
            let result = execution
                .prepared_response(journal, &input, 1, &prior, None)
                .await;
            assert!(
                matches!(result, Err(ref error) if error.code() == crate::wire::ErrorCode::Invalid)
            );
            let (_, records) = execution
                .model_records(journal, input.operation_id, 1, ModelPurpose::Response)
                .await?;
            assert!(
                !records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            );
            assert_eq!(provider.request_count(), 2);
            assert!(provider.request(1).unwrap().summary);
            Ok(ExecutionProgress::Ready(()))
        };
        execution
            .with_context_review(journal, &input, worker)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn worker_settlement_cancels_an_unfinished_context_review() -> Result<()> {
    let started = Arc::new(tokio::sync::Notify::new());
    let never_released = Arc::new(tokio::sync::Notify::new());
    let (seen, gate) = (started.clone(), never_released.clone());
    let calls = AtomicUsize::new(0);
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":250,"completion_tokens":1}}),
            })]),
            ScriptedTurn::Text("unused".into()),
        ])
        .with_dispatch_observer(move |_| {
            let index = calls.fetch_add(1, Ordering::SeqCst);
            let (seen, gate) = (seen.clone(), gate.clone());
            Box::pin(async move {
                if index == 1 {
                    seen.notify_one();
                    gate.notified().await;
                }
                Ok(())
            })
        }),
    );
    let stock = StockExecutor::new(
        Model::new("fixture".into(), "selected".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_automatic_compaction()?;
    let execution = StockExecution::ordinary(&stock);
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 8192).await?;
    let journal_owner = storage.journal();
    let journal = journal_owner.as_ref();
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("Continue.".into()),
        selected_context: None,
    };
    let prior = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("Earlier work.".repeat(32)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("Evidence retained.".into()),
        },
    ];
    let worker = async {
        let events = execution.model_step(journal, &input, 0, &prior).await?;
        execution
            .request_context_review(journal, &input, 0, &prior, original_metadata(&events))
            .await?;
        started.notified().await;
        Ok(ExecutionProgress::Ready(()))
    };
    execution
        .with_context_review(journal, &input, worker)
        .await?;
    let (_, records) = execution
        .model_records(journal, input.operation_id, 0, ModelPurpose::Summary)
        .await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                ExecutionEvent::ModelStopped {
                    worker_paused: false,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(provider.request_count(), 2);
    Ok(())
}
