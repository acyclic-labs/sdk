use super::*;

#[tokio::test]
async fn stock_driver_retires_only_settled_history_and_restarts_its_offset_on_reentry() -> Result<()>
{
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let (provider, accounting_calls) = counted_model(
        // The whole window; two echoed exchanges overflow it.
        49_152,
        None,
        [
            tool_turn([echo_call("first", json!({"value": "old ".repeat(3_100)}))]),
            tool_turn([echo_call("second", json!({"value": "old ".repeat(3_100)}))]),
            ScriptedTurn::Text(
                r#"{"version":1,"drop_through":4,"summary":"first settled exchange"}"#.into(),
            ),
            tool_turn([echo_call("tail", json!({"value": "new"}))]),
            done_turn(),
        ],
        [],
    );
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "driver-cut".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        allowed()?,
    )?
    .with_compaction_policy(crate::context::CompactionPolicy::Capacity(
        crate::context::CapacityCompaction::default(),
    ));
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
    };
    let mut execution = StockExecution::ordinary(&executor);
    execution
        .execute_progress(&input, journal)
        .await?
        .into_output()?;
    assert!(
        execution.absolute_prior_count(0).await? > 0,
        "the original driver never retired its consumed prefix"
    );
    assert_eq!(
        tool.0.load(Ordering::SeqCst),
        3,
        "retirement invalidated a pending invocation index"
    );
    let dispatched = provider.requests().len();
    execution
        .execute_progress(&input, journal)
        .await?
        .into_output()?;
    assert_eq!(
        provider.requests().len(),
        dispatched,
        "reentry dispatched a settled step twice"
    );
    assert_eq!(tool.0.load(Ordering::SeqCst), 3);
    // Four responses and one summary, including the compacted response count.
    // The pressure, retained subset, summary window and summary admission
    // each need one additional count.
    assert_eq!(dispatched, 5);
    assert_eq!(
        accounting_calls.load(Ordering::SeqCst),
        9,
        "working-base budgeting recounted its already validated source"
    );
    Ok(())
}

#[tokio::test]
async fn stock_driver_refuses_to_summarize_its_only_protected_exchange() -> Result<()> {
    let journal = Journal::default();
    let (provider, _) = counted_model(
        // The whole window; the one echoed exchange overflows it.
        49_152,
        None,
        [
            tool_turn([echo_call("only", json!({"value": "old ".repeat(6_500)}))]),
            done_turn(),
        ],
        [],
    );
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "protected-exchange".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        allowed()?,
    )?
    .with_compaction_policy(crate::context::CompactionPolicy::Capacity(
        crate::context::CapacityCompaction::default(),
    ));
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
    };
    let result = StockExecution::ordinary(&executor)
        .execute_progress(&input, &journal)
        .await;
    // The refusal ends the turn with its settled exchange, whose applied
    // call the next turn replays.
    let Ok(ExecutionProgress::Ended(ended)) = result else {
        return Err(Error::Invalid("the refusal did not end the turn".into()));
    };
    let (error, output) = *ended;
    assert!(
        matches!(error, Error::Invalid(ref reason) if reason == "no complete older compaction source")
    );
    assert_eq!(output.steps, 1);
    assert!(output.transcript.iter().any(|message| {
        message
            .content
            .parts()
            .iter()
            .any(|part| matches!(part, crate::model::ModelContentPart::ToolResult { .. }))
    }));
    assert_eq!(
        provider.requests().len(),
        1,
        "an impossible cut dispatched a summary or response"
    );
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    Ok(())
}
