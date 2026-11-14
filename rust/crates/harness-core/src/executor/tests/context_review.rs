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
    let plugins = crate::plugins::seed().await?;
    for (count, retrospective) in [(2, false), (64, false), (2, true), (64, true)] {
        let provider = Arc::new(ScriptedModelProvider::new(
            (0..1 + usize::from(retrospective)).map(|_| {
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: json!({"usage":{"prompt_tokens":1024,"completion_tokens":1}}),
                    }),
                ])
            }),
        ));
        let mut stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        if retrospective {
            stock = stock.with_plugins(Arc::clone(&plugins))?;
        }
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
        let (journal, _issuer) = crate::filesystem::test_execution_journal().await?;
        let journal = journal.with_bound_selection(input.selected_context.clone().unwrap());
        let journal = &journal;
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
            1 + usize::from(retrospective),
            "the negative control wastes preparation without generating another request"
        );
    }
    Ok(())
}

/// A blocked housekeeping request must neither stall the worker nor change
/// its cached prefix. Only a finished, source-bound decision may rewrite it.
#[tokio::test]
async fn model_budget_reviews_context_without_stalling_or_rewriting_live_prefixes() -> Result<()> {
    struct UnselectedProvider(ScriptedModelProvider);
    impl ModelProvider for UnselectedProvider {
        fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
            self.0.context_capacity(model)
        }

        fn generate<'a>(
            &'a self,
            request: &'a crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> crate::model::ModelStream<'a> {
            self.0.generate(request, dispatch)
        }

        fn reported_input_tokens(&self, _: &Model, _: &Value) -> Result<Option<u64>> {
            Err(Error::Invalid(
                "unselected provider cannot account this call".into(),
            ))
        }
    }
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
        .with_context_capacity(crate::context::ModelContextCapacity { context_tokens: 65_536, output_tokens: Some(4096), output_shares_context: false })
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
        let selected = StockExecutor::from_binding(
            Arc::new(crate::model::ResolvedModel::new((
                Model::new("fixture".into(), "selected".into(), "1".into())?,
                recorded,
            ))),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let selected = selected.into_step_binding(None)?;
        // The parent binding supplies capacity for its durable turn identity,
        // but has no usage counters or scripted requests. Completion accounting,
        // review scheduling and replay use the selection.
        let executor = StockExecutor::from_binding(
            Arc::new(crate::model::ResolvedModel::new((
                Model::new("fixture".into(), "default".into(), "1".into())?,
                Arc::new(UnselectedProvider(ScriptedModelProvider::new([]))),
            ))),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_compaction_policy(crate::context::CompactionPolicy::default())
        .with_automatic_compaction()?;
        let mut execution = StockExecution::ordinary(&executor);
        execution.selected = Some(Arc::clone(&selected));
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
        let mut reopened = StockExecution::ordinary(&executor);
        reopened.selected = Some(selected);
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
async fn reviews_bound_complete_groups_without_stopping_a_fitting_worker() -> Result<()> {
    for partitioned in [true, false] {
        let overflowed = Arc::new(AtomicUsize::new(0));
        let seen = overflowed.clone();
        let response = || {
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":3000,"completion_tokens":1}}),
            })])
        };
        let mut turns = vec![response()];
        if partitioned {
            turns.push(ScriptedTurn::Text(
                json!({
                    "version":1,"summary":null,"review_at_input_tokens":3500,
                })
                .to_string(),
            ));
        }
        turns.push(response());
        let provider = Arc::new(
            ScriptedModelProvider::new(turns)
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 4096,
                    output_tokens: Some(128),
                    output_shares_context: false,
                })
                .with_token_counter(move |request| {
                    if request.bytes().len() > 4096 - 128 {
                        seen.fetch_add(1, Ordering::SeqCst);
                    }
                    fixture_token_count(request)
                }),
        );
        let stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let journal_owner = storage.journal();
        let journal = journal_owner.as_ref();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("Continue.".into()),
            selected_context: None,
        };
        let groups = if partitioned { 4 } else { 1 };
        let mut prior = Vec::new();
        for index in 0..groups {
            prior.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!(
                    "{index}{}",
                    "a".repeat(if partitioned { 640 } else { 2816 })
                )),
            });
            prior.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("b".repeat(64)),
            });
        }
        let execution = StockExecution::ordinary(&stock);
        let worker = async {
            let events = execution.model_step(journal, &input, 0, &prior).await?;
            assert_eq!(
                overflowed.load(Ordering::SeqCst),
                0,
                "the original worker fits"
            );
            execution
                .request_context_review(journal, &input, 0, &prior, original_metadata(&events))
                .await?;
            execution.review.completed.notified().await;
            assert!(
                overflowed.load(Ordering::SeqCst) > 0,
                "the full review does not fit"
            );
            assert_eq!(provider.request_count(), 1 + usize::from(partitioned));
            if partitioned {
                let request = provider.request(1).unwrap();
                assert!(request.summary);
                assert!(
                    request
                        .messages
                        .iter()
                        .any(|m| m.content == prior[0].content)
                );
                assert!(
                    !request
                        .messages
                        .iter()
                        .any(|m| m.content == prior[6].content)
                );
            }
            execution.model_step(journal, &input, 1, &prior).await?;
            let request = provider.last_request().unwrap();
            for message in &prior {
                assert!(
                    request
                        .messages
                        .iter()
                        .any(|m| m.content == message.content)
                );
            }
            Ok(ExecutionProgress::Ready(()))
        };
        execution
            .with_context_review(journal, &input, worker)
            .await?;
        let paid = provider.request_count();
        let reopened = StockExecution::ordinary(&stock);
        let replay = async {
            let events = reopened.model_step(journal, &input, 0, &prior).await?;
            reopened
                .request_context_review(journal, &input, 0, &prior, original_metadata(&events))
                .await?;
            assert_eq!(
                provider.request_count(),
                paid,
                "window replay must not dispatch"
            );
            Ok(ExecutionProgress::Ready(()))
        };
        reopened
            .with_context_review(journal, &input, replay)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn replay_preserves_review_boundaries_skipped_during_active_work() -> Result<()> {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (seen, gate) = (started.clone(), release.clone());
    let dispatches = AtomicUsize::new(0);
    let response = |tokens| {
        ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"usage":{"prompt_tokens":tokens,"completion_tokens":1}}),
        })])
    };
    let decision = |tokens| {
        ScriptedTurn::Text(
            json!({"version":1,"summary":null,"review_at_input_tokens":tokens}).to_string(),
        )
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([
            response(4_000),
            decision(5_000),
            response(9_000),
            response(10_000),
            decision(16_000),
            response(12_000),
        ])
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: 65_536,
            output_tokens: Some(4096),
            output_shares_context: false,
        })
        .with_token_counter(fixture_token_count)
        .with_dispatch_observer(move |_| {
            let (seen, gate) = (seen.clone(), gate.clone());
            let index = dispatches.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if index == 1 {
                    seen.notify_one();
                    gate.notified().await;
                }
                Ok(())
            })
        }),
    );
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "selected".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_automatic_compaction()?;
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal_owner = storage.journal();
    let journal = journal_owner.as_ref();
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("Continue the task.".into()),
        selected_context: None,
    };
    let prior = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("Earlier evidence.".repeat(200)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("Retain the findings.".into()),
        },
    ];
    let execution = StockExecution::ordinary(&executor);
    let worker = async {
        for step in 0..3 {
            let events = execution.model_step(journal, &input, step, &prior).await?;
            execution
                .request_context_review(journal, &input, step, &prior, original_metadata(&events))
                .await?;
            match step {
                0 => started.notified().await,
                1 => {
                    release.notify_one();
                    execution.review.completed.notified().await;
                }
                _ => execution.review.completed.notified().await,
            }
        }
        Ok(ExecutionProgress::Ready(()))
    };
    execution
        .with_context_review(journal, &input, worker)
        .await?;
    assert_eq!(provider.request_count(), 5);
    let reopened = StockExecution::ordinary(&executor);
    let replay = async {
        for step in 0..3 {
            if step > 0 {
                // Live response preparation consumes the preceding decision.
                reopened
                    .prepared_response(journal, &input, step, &prior, None)
                    .await?;
            }
            let events = reopened.model_step(journal, &input, step, &prior).await?;
            reopened
                .request_context_review(journal, &input, step, &prior, original_metadata(&events))
                .await?;
            assert_eq!(
                provider.request_count(),
                5,
                "replay dispatched new paid work"
            );
        }
        let events = reopened.model_step(journal, &input, 3, &prior).await?;
        reopened
            .request_context_review(journal, &input, 3, &prior, original_metadata(&events))
            .await?;
        assert_eq!(provider.request_count(), 6);
        Ok(ExecutionProgress::Ready(()))
    };
    let (result, trace) =
        acyclic_test_support::differential::observe_async::<_, (&'static str, u64)>(
            reopened.with_context_review(journal, &input, replay),
        )
        .await;
    result?;
    assert_eq!(
        trace
            .iter()
            .filter(|(event, _)| *event == "context review source messages")
            .count(),
        2,
        "only the two saved reviews may prepare a source; the skipped boundary and fresh below-budget response prepare none"
    );
    assert_eq!(provider.request_count(), 6);
    Ok(())
}

/// A reviewer's malformed, empty or invalid decision never fails the worker.
/// It is re-asked once with its reason; a second defect keeps the unreviewed
/// context until capacity. The first two payloads are the ones DeepSeek
/// returned in GC-274: a raw newline inside a JSON string, and no text.
#[tokio::test]
async fn defective_context_decisions_reask_once_then_keep_history() -> Result<()> {
    let raw_newline =
        "{\"version\":1,\"summary\":\"line one\nline two\",\"review_at_input_tokens\":500}";
    for decision in [
        raw_newline.to_owned(),
        String::new(),
        "not JSON".to_owned(),
        json!({"version":1,"summary":"x","review_at_input_tokens":500,"quoted_field_name":true})
            .to_string(),
        json!({"version":"quoted_value","summary":"x","review_at_input_tokens":500}).to_string(),
        json!({"version":2,"summary":"replacement","review_at_input_tokens":500}).to_string(),
        json!({"version":1,"summary":"","review_at_input_tokens":500}).to_string(),
        json!({"version":1,"summary":null,"review_at_input_tokens":250}).to_string(),
        json!({"version":1,"summary":"replacement","review_at_input_tokens":8192}).to_string(),
    ] {
        let response = || {
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":250,"completion_tokens":1}}),
            })])
        };
        // No text at all is a completion carrying no content event.
        let reply = |decision: &str| match decision {
            "" => ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            })]),
            text => ScriptedTurn::Text(text.to_owned()),
        };
        let provider = Arc::new(
            ScriptedModelProvider::new([
                response(),
                reply(&decision),
                response(),
                reply(&decision),
                response(),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 8192,
                output_tokens: Some(512),
                output_shares_context: false,
            })
            .with_token_counter(fixture_token_count),
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
            for step in 0..3 {
                let events = execution.model_step(journal, &input, step, &prior).await?;
                let (_, records) = execution
                    .model_records(journal, input.operation_id, step, ModelPurpose::Response)
                    .await?;
                assert!(
                    !records.iter().any(|record| matches!(
                        record.event,
                        ExecutionEvent::ContextCompacted { .. }
                    )),
                    "a rejected decision never rewrites history"
                );
                let requests = provider.request_count();
                execution
                    .request_context_review(
                        journal,
                        &input,
                        step,
                        &prior,
                        original_metadata(&events),
                    )
                    .await?;
                if step < 2 {
                    execution.review.completed.notified().await;
                    assert_eq!(provider.request_count(), requests + 1);
                } else {
                    assert_eq!(
                        provider.request_count(),
                        requests,
                        "after the re-ask, review waits for capacity"
                    );
                }
            }
            for step in 0..3 {
                let (_, rejected) = execution
                    .step_records(journal, input.operation_id, step, false, None, |event| {
                        matches!(
                            event,
                            ExecutionEvent::ModelRejected {
                                purpose: ModelPurpose::Summary,
                                ..
                            }
                        )
                    })
                    .await?;
                assert_eq!(
                    rejected.len(),
                    usize::from(step < 2),
                    "each rejection is journaled once with its reason"
                );
                assert!(rejected.iter().all(|record| matches!(
                    &record.event,
                    ExecutionEvent::ModelRejected { reason, .. } if !reason.is_empty()
                        && !reason.contains("quoted_")
                )));
            }
            let ask = provider.request(1).unwrap();
            let reask = provider.request(3).unwrap();
            assert!(ask.summary && reask.summary);
            let rejected = |request: &crate::model::ModelRequest| {
                format!("{:?}", request.messages).contains("Your previous decision was rejected")
            };
            assert!(!rejected(&ask));
            assert!(rejected(&reask), "the re-ask carries its rejection");
            Ok(ExecutionProgress::Ready(()))
        };
        execution
            .with_context_review(journal, &input, worker)
            .await?;
        assert_eq!(provider.request_count(), 5);
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
