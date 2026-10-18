use super::*;
use crate::executor::observed::ObservedFold;

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
                &mut messages,
                &mut ObservedFold::default(),
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
