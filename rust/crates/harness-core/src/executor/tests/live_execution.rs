use super::*;
use futures::FutureExt as _;

fn input() -> TurnInput {
    TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("live attempt".into()),
        selected_context: None,
    }
}

#[tokio::test]
async fn a_live_model_retry_is_rejected_before_replay_or_dispatch() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("independent".into()),
        ])
        .with_token_counter(fixture_token_count)
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: None,
        }]),
    );
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into())?,
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
    let before_retry = journal.0.lock().unwrap().clone();
    // Dropping the live guard permits reconciliation, but this direct executor
    // has no retained task fence authorizing another model dispatch.
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Indeterminate(operation)) if operation == input.operation_id
    ));
    assert_eq!(*journal.0.lock().unwrap(), before_retry);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
struct PendingTool {
    calls: AtomicUsize,
    result: Mutex<Option<ToolResult>>,
}
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl crate::tool::ToolExecutor for PendingTool {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(self.result.lock().unwrap().clone()) })
    }
}

#[tokio::test]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
async fn a_live_tool_retry_cannot_settle_or_repeat_the_effect() -> Result<()> {
    let (journal, _issuer) = super::publication_observation::journal().await?;
    let provider = Arc::new(scripted_model());
    let tool = Arc::new(PendingTool {
        calls: AtomicUsize::new(0),
        result: Mutex::new(None),
    });
    let tools = echo_tools(
        tool.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into())?,
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
    )?;
    let input = input();
    let mut first = Box::pin(executor.execute(input.clone(), &journal));
    assert!(first.as_mut().now_or_never().is_none());
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let before = journal.replay(input.operation_id, 0, 100, None)?.await?;
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        journal.replay(input.operation_id, 0, 100, None)?.await?,
        before
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    drop(first);
    assert!(matches!(
        executor.execute_progress(input.clone(), &journal).await,
        Err(Error::Indeterminate(operation)) if operation == input.operation_id
    ));
    // Neither cancellation nor an inconclusive reconciliation authorizes a
    // terminal result. The original serialized invocation remains recoverable.
    assert_eq!(
        journal.replay(input.operation_id, 0, 100, None)?.await?,
        before
    );
    assert!(matches!(
        executor.execute_progress(input.clone(), &journal).await,
        Err(Error::Indeterminate(operation)) if operation == input.operation_id
    ));
    assert_eq!(
        journal.replay(input.operation_id, 0, 100, None)?.await?,
        before
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    // A later outcome of that exact invocation can still settle normally.
    *tool.result.lock().unwrap() = Some(ToolResult {
        value: json!({"value":"hello"}),
    });
    assert_eq!(
        executor.execute(input.clone(), &journal).await?.text,
        "done"
    );
    let completed = journal.replay(input.operation_id, 0, 100, None)?.await?;
    assert_eq!(&completed[..before.len()], before.as_slice());
    assert!(
        completed
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
    );
    assert!(
        !completed
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. }))
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}
