use super::*;

struct Source(ToolRegistry);

impl crate::tool::ToolSource for Source {
    fn refresh(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(self.0.clone())
    }

    fn capability(&self) -> Option<&str> {
        Some("tools:external")
    }
}

fn catalog(size: usize, executor: Arc<FakeTool>) -> Result<ToolRegistry> {
    let mut tools = ToolRegistry::default();
    for index in 0..size {
        tools.register(
            (
                crate::tool::Tool {
                    hook_names: &[],
                    definition: crate::tool::ToolDefinition {
                        name: format!("example.tool{index:03}"),
                        revision: "1".into(),
                        description: "External action".into(),
                        input_schema: json!({"type":"object"}),
                        output_schema: json!({"type":"object"}),
                        projection_schema: crate::tool::json_projection_schema(
                            json!({"type":"object"}),
                        ),
                    },
                    executor: executor.clone(),
                    projection: Arc::new(Projection),
                },
                None,
            ),
            &mut None,
            None,
        )?;
    }
    Ok(ToolRegistry::default().with_source(Arc::new(Source(tools))))
}

fn total(trace: &[(&'static str, u64)], name: &str) -> (usize, u64) {
    trace
        .iter()
        .filter(|(kind, _)| *kind == name)
        .fold((0, 0), |(count, bytes), (_, value)| {
            (count + 1, bytes + value)
        })
}

#[tokio::test]
async fn recovered_call_count_does_not_multiply_response_or_catalog_reads() -> Result<()> {
    for (events, prompt_bytes, catalog_size) in
        [(1, 128, 8), (16, 128, 8), (1, 4096, 8), (1, 128, 128)]
    {
        let mut small = None;
        for calls in [1, 8] {
            let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
            let tools = catalog(catalog_size, tool.clone())?;
            let mut response: Vec<_> = (0..events)
                .map(|_| ModelEvent::Content { delta: "x".into() })
                .collect();
            response.extend((0..calls).map(|index| {
                ModelEvent::ToolCall(crate::model::ToolCall::new(
                    format!("call-{index}"),
                    "example.tool007".into(),
                    json!({"value":index}),
                ))
            }));
            let provider = Arc::new(ScriptedModelProvider::new([tool_turn(response)]));
            let executor = uncompacted_executor(
                Model::new("fixture".into(), "actions".into(), "1".into())?,
                provider.clone(),
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(
                RuntimeScope::new(Capabilities::new(["tools:external"]), Limits::default())?,
                allowed()?,
            )?;
            let storage =
                crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 1_048_576).await?;
            let journal = storage.journal();
            let input = TurnInput {
                operation_id: OperationId::new(),
                max_steps: Some(1),
                input: ModelContent::Text("p".repeat(prompt_bytes)),
                selected_context: None,
            };
            let (first, fresh) = acyclic_test_support::differential::observe_async::<
                _,
                (&'static str, u64),
            >(executor.execute(input.clone(), journal.as_ref()))
            .await;
            assert!(
                matches!(first, Err(Error::Conflict(message)) if message == "executor step limit reached")
            );
            assert_eq!(tool.0.load(Ordering::SeqCst), calls);
            assert_eq!(total(&fresh, "response request read"), (0, 0));
            assert_eq!(
                total(&fresh, "admitted catalog entries"),
                (1, catalog_size as u64)
            );
            let (replay, trace) = acyclic_test_support::differential::observe_async::<
                _,
                (&'static str, u64),
            >(executor.execute(input.clone(), journal.as_ref()))
            .await;
            assert!(
                matches!(replay, Err(Error::Conflict(message)) if message == "executor step limit reached")
            );
            assert_eq!(
                tool.0.load(Ordering::SeqCst),
                calls,
                "completed calls must not dispatch again"
            );
            assert_eq!(provider.requests().len(), 1);
            assert_eq!(total(&trace, "response request read").0, 1);
            assert_eq!(
                total(&trace, "response observation read").0,
                events + calls + 1
            );
            assert!(total(&trace, "response observation read").1 > 0);
            assert_eq!(
                total(&trace, "admitted catalog entries"),
                (1, catalog_size as u64)
            );
            let request_bytes = total(&trace, "response request read").1;
            if let Some(previous) = small {
                assert_eq!(request_bytes, previous);
            }
            small = Some(request_bytes);

            // Positive control exercises the same real journal reads outside settlement.
            let execution = StockExecution::ordinary(&executor);
            let (control, trace) = acyclic_test_support::differential::observe_async::<
                _,
                (&'static str, u64),
            >(execution.retained_dispatch_step(
                journal.as_ref(),
                input.operation_id,
                0,
                ModelPurpose::Response,
            ))
            .await;
            let control = control?;
            assert_eq!(total(&trace, "response request read"), (1, request_bytes));
            assert_eq!(
                total(&trace, "response observation read").0,
                events + calls + 1
            );
            assert!(total(&trace, "response observation read").1 > 0);
            assert_eq!(control.events.len(), events + calls + 1);
        }
    }
    Ok(())
}
