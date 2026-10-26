use super::*;

#[tokio::test]
async fn stock_driver_retires_only_settled_history_and_restarts_its_offset_on_reentry() -> Result<()>
{
    let mut journal = Journal::default();
    let issuer = crate::core::AuthorityRoot::new(
        "driver-working-cut",
        [23; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    journal.8 = Some(issuer.verifier(&mut None)?);
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            tool_turn([echo_call("first", json!({"value": "old ".repeat(3_100)}))]),
            tool_turn([echo_call("second", json!({"value": "old ".repeat(3_100)}))]),
            ScriptedTurn::Text("bounded summary".into()),
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
        Model::new(
            "fixture".into(),
            "driver-cut".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        None,
    )?
    .with_compaction_policy(crate::context::CompactionPolicy::Threshold(
        crate::context::ThresholdCompaction {
            recent_tokens: 1,
            ..Default::default()
        },
    ));
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 4,
    };
    let execution = StockExecution::ordinary(&executor);
    assert!(matches!(
        execution.execute_progress(&input, &journal).await?,
        ExecutionProgress::Ready(_)
    ));
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
    assert!(matches!(
        execution.execute_progress(&input, &journal).await?,
        ExecutionProgress::Ready(_)
    ));
    assert_eq!(
        provider.requests().len(),
        dispatched,
        "reentry dispatched a settled step twice"
    );
    assert_eq!(tool.0.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn stock_driver_refuses_to_summarize_its_only_protected_exchange() -> Result<()> {
    let journal = Journal::default();
    let (provider, _) = counted_model(
        65_536,
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
        Model::new(
            "fixture".into(),
            "protected-exchange".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        None,
    )?
    .with_compaction_policy(crate::context::CompactionPolicy::Threshold(
        crate::context::ThresholdCompaction {
            recent_tokens: 1,
            ..Default::default()
        },
    ));
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 2,
    };
    let result = StockExecution::ordinary(&executor)
        .execute_progress(&input, &journal)
        .await;
    assert!(
        matches!(result, Err(Error::Invalid(ref reason)) if reason == "mandatory content leaves no compaction source")
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "an impossible cut dispatched a summary or response"
    );
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    Ok(())
}
