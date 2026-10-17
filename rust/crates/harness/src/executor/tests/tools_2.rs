use super::*;

#[tokio::test]
async fn stock_tool_input_obeys_narrow_executor_bound_before_journal_io() -> Result<()> {
    let wide = RuntimeScope::new(
        Capabilities::new(["tool:call:example.echo"]),
        Limits::default(),
    )?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "bounded".into(),
        name: "example.echo".into(),
        arguments: json!({"text":"x".repeat(256)}),
    };
    let adapter = Arc::new(FakeTool(AtomicUsize::new(0)));
    crate::tool::ToolExecutor::authorize(adapter.as_ref(), Some(&wide), None, &invocation, None)?;
    crate::executor::json::json_byte_length(&invocation, wide.limits().file_bytes, &mut None)
        .map(|_| ())?;
    let tools = echo_tools(
        adapter.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let narrow = Limits {
        file_bytes: 128,
        ..wide.limits()
    };
    let executor = StockExecutor::new(
        Model::new("test".into(), "scoped".into(), "1".into(), Value::Null)?,
        Arc::new(scripted_model()),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(wide, None)?
    .with_limits(narrow);
    assert!(executor.tool_scope.limits().file_bytes > executor.limits.file_bytes);
    let journal = Journal::default();
    let mut messages = vec![call_message(&invocation)];
    let before = messages.clone();
    assert!(matches!(
        StockExecution::ordinary(&executor)
            .settle_tool_call(
                &journal,
                OperationId::new(),
                0,
                invocation,
                0,
                &mut messages
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(adapter.0.load(Ordering::SeqCst), 0);
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    assert!(journal.0.lock().unwrap().is_empty());
    assert!(journal.1.lock().unwrap().is_empty());
    assert_eq!(messages, before);
    Ok(())
}

#[tokio::test]
async fn policy_denial_returns_to_the_model_before_dispatch() -> Result<()> {
    struct FixedPolicy(bool);
    impl ToolPolicy for FixedPolicy {
        fn identity(&self) -> ComponentIdentity {
            ComponentIdentity {
                name: "test.deny".into(),
                version: "1".into(),
                digest: [1 + u8::from(self.0); 32],
            }
        }
        fn evaluate<'a>(
            &'a self,
            _: &'a ToolInvocation,
            _: &'a RuntimeScope,
            _io: Option<&'a crate::executor::PreparationIo>,
        ) -> Result<
            crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>,
        > {
            let decision = if self.0 {
                ToolPolicyDecision::Deny {
                    reason: "echo is\nforbidden here".into(),
                }
            } else {
                ToolPolicyDecision::Allow
            };
            Ok(crate::store::HistoryContentFuture::Ordinary {
                future: Box::pin(async move { Ok((decision, None)) }),
                frame_bytes: 0,
            })
        }
    }
    // The allowing policy is the negative control: the same call dispatches and no error returns.
    for deny in [true, false] {
        let model = Arc::new(scripted_model());
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let tools = echo_tools(
            tool_executor.clone(),
            json!({"type": "object"}),
            json!({"type": "object"}),
            crate::tool::json_projection_schema(json!({"type": "object"})),
        )?;
        let executor = super::uncompacted_executor(
            Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            Some(Arc::new(FixedPolicy(deny))),
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([29; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        let text = super::tools_1::tool_error_text(&model.requests()[1].messages, "call-1");
        assert_eq!(text.is_some(), deny);
        if let Some(text) = text {
            assert!(text.contains("(unauthorized)") && text.contains("echo is forbidden here"));
        }
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), usize::from(!deny));
        assert_eq!(executor.execute(input, &journal).await?, output);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), usize::from(!deny));
    }
    Ok(())
}
