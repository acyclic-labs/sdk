use super::*;
use futures::FutureExt as _;

fn input() -> TurnInput {
    TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("live attempt".into()),
        selected_context: None,
        max_steps: 4,
    }
}

#[tokio::test]
async fn a_live_model_retry_is_rejected_before_replay_or_dispatch() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("independent".into()),
            ScriptedTurn::Text("recovered".into()),
        ])
        .with_token_counter(fixture_token_count)
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: None,
        }]),
    );
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let input = input();
    let mut first = Box::pin(executor.execute(input.clone(), &journal));
    assert!(first.as_mut().now_or_never().is_none());
    assert_eq!(provider.requests().len(), 1);
    let before = journal.0.lock().unwrap().clone();
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        executor.model_step(&journal, &input, 0, &[]).await,
        Err(Error::Conflict(_))
    ));
    for inside_turn in [false, true] {
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: crate::model::ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            metadata: Default::default(),
            current_input_index: Some(0),
        };
        let instruction = ModelContent::Text("summarize".into());
        let result = if inside_turn {
            executor
                .summarize_in_turn(&journal, &input, 0, source, instruction)
                .await
        } else {
            executor
                .summarize(&journal, input.operation_id, source, instruction)
                .await
        };
        assert!(matches!(result, Err(Error::Conflict(_))));
    }
    assert_eq!(*journal.0.lock().unwrap(), before);
    assert_eq!(provider.requests().len(), 1);
    let mut independent = input.clone();
    independent.operation_id = OperationId::new();
    assert_eq!(
        executor.execute(independent, &journal).await?.text,
        "independent"
    );
    drop(first);
    assert_eq!(executor.execute(input, &journal).await?.text, "recovered");
    assert_eq!(provider.requests().len(), 3);
    Ok(())
}

struct PendingTool(AtomicUsize);
impl crate::tool::ToolExecutor for PendingTool {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn a_live_tool_retry_cannot_settle_or_repeat_the_effect() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(scripted_model());
    let tool = Arc::new(PendingTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
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
    )?;
    let input = input();
    let mut first = Box::pin(executor.execute(input.clone(), &journal));
    assert!(first.as_mut().now_or_never().is_none());
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    let before = journal.0.lock().unwrap().clone();
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(*journal.0.lock().unwrap(), before);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    drop(first);
    assert!(matches!(
        executor.execute_progress(input.clone(), &journal).await?,
        ExecutionProgress::Failed(ToolFailureKind::Interrupted)
    ));
    let settled = journal.0.lock().unwrap().clone();
    assert_eq!(
        settled
            .iter()
            .filter(|record| matches!(
                record.event,
                ExecutionEvent::ToolFailed {
                    reason: ToolFailureKind::Interrupted,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(matches!(
        executor.execute_progress(input, &journal).await?,
        ExecutionProgress::Failed(ToolFailureKind::Interrupted)
    ));
    assert_eq!(*journal.0.lock().unwrap(), settled);
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}
