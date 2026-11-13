use super::*;

/// A blocked housekeeping request must neither stall the worker nor change
/// its cached prefix. Only a finished, source-bound decision may rewrite it.
#[tokio::test]
async fn model_budget_reviews_context_without_stalling_or_rewriting_live_prefixes() -> Result<()> {
    for replace in [false, true] {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let (seen, gate, calls) = (started.clone(), release.clone(), dispatches.clone());
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"usage":{"prompt_tokens":160_000,"completion_tokens":1,"prompt_tokens_details":{"cached_tokens":156_800}}}),
            })]),
            ScriptedTurn::Text(json!({
                "summary": if replace { Some("Retained decisions and unfinished work.") } else { None },
                "review_at_input_tokens": 240_000,
            }).to_string()),
            ScriptedTurn::Text("work continues".into()),
            ScriptedTurn::Text("done".into()),
        ])
        .with_context_capacity(crate::context::ModelContextCapacity { context_tokens: 1_048_576, output_tokens: Some(4096) })
        .with_token_counter(fixture_token_count)
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
        let executor = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let execution = StockExecution::ordinary(&executor);
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("Continue the task.".into()),
            selected_context: None,
        };
        let mut prior = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old page ".repeat(20_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("Prior findings are recorded.".into()),
            },
        ];
        let worker = async {
            let events = execution.model_step(&journal, &input, 0, &prior).await?;
            execution
                .request_context_review(&journal, &input, 0, &prior, &events)
                .await?;
            started.notified().await;
            let before = provider.request(0).unwrap();
            prior.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("New observation.".into()),
            });
            execution.model_step(&journal, &input, 1, &prior).await?;
            let during = provider.request(2).unwrap();
            assert!(during.messages.starts_with(&before.messages));
            assert!(!during.summary);
            release.notify_one();
            execution.review.completed.notified().await;
            prior.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("Use the new observation.".into()),
            });
            execution.model_step(&journal, &input, 2, &prior).await?;
            let after = provider.request(3).unwrap();
            assert!(!after.summary);
            if replace {
                let bytes = crate::contract::canonical_json_bytes(&after)?;
                assert!(bytes.len() < crate::contract::canonical_json_bytes(&before)?.len() / 10);
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
            } else {
                assert!(after.messages.starts_with(&during.messages));
            }
            // Reopening the exact admitted boundary uses the durable cut and
            // provider completion, not another background or worker request.
            execution.model_step(&journal, &input, 2, &prior).await?;
            assert_eq!(provider.request_count(), 4);
            Ok(ExecutionProgress::Ready(()))
        };
        assert!(matches!(
            execution
                .with_context_review(&journal, &input, worker)
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
    }
    Ok(())
}
