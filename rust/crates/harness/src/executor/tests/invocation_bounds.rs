use super::*;

#[tokio::test]
async fn saved_invocation_envelope_is_bounded_before_stock_journal_io_or_execution() -> Result<()> {
    for (maximum, render, bare_at_limit, arguments, metadata) in [
        (256, 4096, true, json!({"data":""}), Value::Null),
        (
            256,
            4096,
            false,
            json!({"data":""}),
            json!({"provider_signature":"x".repeat(256)}),
        ),
        (
            4096,
            1024,
            false,
            json!({"data":"x".repeat(600)}),
            json!({"signature":"x".repeat(600)}),
        ),
    ] {
        let operation = OperationId::new();
        let mut invocation = ToolInvocation::for_model_call(
            operation,
            0,
            "call".into(),
            "example.echo".into(),
            arguments,
            None,
        )?;
        if bare_at_limit {
            let overhead = serde_json::to_vec(&invocation)
                .expect("fixture encoding")
                .len();
            invocation.arguments["data"] = json!("x".repeat(maximum as usize - overhead));
            assert_eq!(
                serde_json::to_vec(&invocation)
                    .expect("fixture encoding")
                    .len(),
                maximum as usize
            );
        }
        let saved = crate::tool::RecordedInvocation {
            invocation: &invocation,
            metadata: &metadata,
        };
        assert!(
            serde_json::to_vec(&saved).expect("fixture encoding").len()
                > maximum.min(render) as usize
        );
        let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
        let tools = echo_tools(
            tool.clone(),
            json!({"type":"object"}),
            json!({}),
            crate::tool::json_projection_schema(json!({})),
        )?;
        let mut executor = uncompacted_executor(
            Model::new("fixture".into(), "exact".into(), "1".into(), json!({}))?,
            Arc::new(scripted_model()),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits {
                    file_bytes: maximum,
                    render_bytes: render,
                    ..Limits::default()
                },
            )?,
            None,
        )?;
        // Exercise either side of the effective min(stock, scope) limit.
        if !bare_at_limit {
            executor.limits.file_bytes = maximum;
        }
        let mut prior = vec![ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(
                crate::model::ToolCall::new(
                    invocation.call_id.clone(),
                    invocation.name.clone(),
                    invocation.arguments.clone(),
                )
                .with_metadata(metadata),
            )),
        }];
        let journal = Journal::default();
        let result = StockExecution::ordinary(&executor)
            .settle_tool_call(&journal, operation, 0, invocation, 0, &mut prior)
            .await;
        assert!(matches!(result, Err(Error::Invalid(_))));
        assert_eq!(journal.3.load(Ordering::SeqCst), 0, "no replay I/O");
        assert_eq!(journal.4.load(Ordering::SeqCst), 0, "no artifact reads");
        assert!(
            journal.0.lock().expect("fixture").is_empty(),
            "no journal writes"
        );
        assert!(
            journal.1.lock().expect("fixture").is_empty(),
            "no staged artifact"
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 0, "no execution");
    }
    Ok(())
}
