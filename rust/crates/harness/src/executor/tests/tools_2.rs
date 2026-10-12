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
    crate::tool::ToolExecutor::authorize(adapter.as_ref(), Some(&wide), &invocation)?;
    crate::contract::json_byte_length(&invocation, wide.limits().file_bytes, &mut None)
        .map(|_| ())?;
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type":"object"}),
                    ),
                },
                executor: adapter.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let narrow = Limits {
        file_bytes: 128,
        ..wide.limits()
    };
    let executor = StockExecutor::new(
        Model::new("test".into(), "scoped".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModel::default()),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(wide, None)?
    .with_limits(narrow);
    assert!(executor.tool_scope.limits().file_bytes > executor.limits.file_bytes);
    let journal = Journal::default();
    let mut messages = Vec::new();
    assert!(matches!(
        StockExecution::ordinary(&executor)
            .settle_tool_call(&journal, OperationId::new(), 0, invocation, &mut messages)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(adapter.0.load(Ordering::SeqCst), 0);
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    assert!(journal.0.lock().unwrap().is_empty());
    assert!(journal.1.lock().unwrap().is_empty());
    assert!(messages.is_empty());
    Ok(())
}
