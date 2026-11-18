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
    // Dropping the live guard permits reconciliation. Nothing may dispatch
    // the uncertain request again, so the unreconciled attempt settles.
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::ModelInterrupted(_))
    ));
    assert_eq!(*journal.0.lock().unwrap(), before_retry);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
type PendingOutcome = fn(OperationId) -> Result<Option<ToolResult>>;

#[cfg(not(target_arch = "wasm32"))]
struct PendingTool {
    retire_on_drop: bool,
    outcome: PendingOutcome,
    calls: AtomicUsize,
    accepted: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}
#[cfg(not(target_arch = "wasm32"))]
impl crate::tool::ToolExecutor for PendingTool {
    fn cancellable_on_drop(&self) -> bool {
        self.retire_on_drop
    }
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(accepted) = self.accepted.lock().unwrap().take() {
            accepted.send(()).unwrap();
        }
        Box::pin(std::future::pending())
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move { (self.outcome)(invocation.operation_id) })
    }
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn a_live_tool_retry_requires_retirement_before_settlement() -> Result<()> {
    let (journal, _issuer) = super::publication_observation::journal().await?;
    let provider = Arc::new(scripted_model());
    let (accepted, acceptance) = tokio::sync::oneshot::channel();
    let tool = Arc::new(PendingTool {
        retire_on_drop: false,
        outcome: |_| Ok(None),
        calls: AtomicUsize::new(0),
        accepted: Mutex::new(Some(accepted)),
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
    tokio::select! {
        outcome = &mut first => panic!("execution ended before tool acceptance: {outcome:?}"),
        accepted = acceptance => accepted.expect("tool acceptance producer was dropped"),
    }
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
    pending_redelivery(&journal, (tool, provider, executor, input), true).await
}

/// Stop the original attempt while its tool outcome is still pending.
#[cfg(not(target_arch = "wasm32"))]
async fn start_pending_call(
    journal: &dyn ExecutionJournal,
    retire_on_drop: bool,
    outcome: PendingOutcome,
) -> Result<(
    Arc<PendingTool>,
    Arc<ScriptedModelProvider>,
    StockExecutor,
    TurnInput,
)> {
    let provider = Arc::new(scripted_model());
    let (accepted, acceptance) = tokio::sync::oneshot::channel();
    let tool = Arc::new(PendingTool {
        retire_on_drop,
        outcome,
        calls: AtomicUsize::new(0),
        accepted: Mutex::new(Some(accepted)),
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
    let mut first = Box::pin(executor.execute(input.clone(), journal));
    tokio::select! {
        outcome = &mut first => panic!("execution ended before tool acceptance: {outcome:?}"),
        accepted = acceptance => accepted.expect("tool acceptance producer was dropped"),
    }
    drop(first);
    Ok((tool, provider, executor, input))
}

#[cfg(not(target_arch = "wasm32"))]
async fn fenced_pending_call(
    dispatched: bool,
) -> Result<(
    Journal,
    Arc<PendingTool>,
    Arc<ScriptedModelProvider>,
    StockExecutor,
    TurnInput,
)> {
    let journal = Journal::default();
    let (tool, provider, executor, input) =
        start_pending_call(&journal, false, |_| Ok(None)).await?;
    // The incarnation dies with its producer; the next one reopens the cut.
    let reopened = journal.reopen_before_tool_completion("call-1");
    if dispatched {
        for record in reopened.0.lock().unwrap().iter_mut() {
            if let ExecutionEvent::ToolStarted { publication, .. } = &mut record.event {
                *publication = Some([7; 32]);
            }
        }
    }
    Ok((reopened, tool, provider, executor, input))
}

/// The fenced twin of the live retry above: a call that ran only inside its
/// dead owner can no longer settle, so an inconclusive reconciliation is the
/// model's interrupted failure, the effect never runs again, and the turn
/// continues instead of staying indeterminate.
#[cfg(not(target_arch = "wasm32"))]
async fn pending_redelivery(
    journal: &dyn ExecutionJournal,
    (tool, provider, executor, input): (
        Arc<PendingTool>,
        Arc<ScriptedModelProvider>,
        StockExecutor,
        TurnInput,
    ),
    unknown: bool,
) -> Result<()> {
    let settled = executor.execute(input.clone(), journal).await?;
    assert_eq!(settled.text, "done");
    let text = super::tools_1::tool_error_text(&provider.requests()[1].messages, "call-1")
        .expect("the next model request carries the call's failure");
    assert!(
        text.starts_with(ToolFailureKind::Interrupted.message()),
        "{text}"
    );
    assert_eq!(
        text.contains("operation outcome is indeterminate"),
        unknown,
        "{text}"
    );
    assert_eq!(text.contains("may still complete"), unknown, "{text}");
    assert!(
        journal
            .replay(input.operation_id, 0, 100, None)?
            .await?
            .iter()
            .any(|record| matches!(
                &record.event,
                ExecutionEvent::ToolFailed { call_id, reason: ToolFailureKind::Interrupted, .. }
                    if call_id == "call-1"
            ))
    );
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        1,
        "the effect never runs again"
    );
    assert_eq!(settled, executor.execute(input, journal).await?);
    Ok(())
}

/// Replay a still-pending outcome on the same owner and after a restart.
/// The real journal, claim and stock settlement engine run under seeded I/O
/// scheduling; no uncertain tool effect is executed a second time.
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn seeded_pending_redelivery_settles_without_failing_the_turn() -> std::result::Result<(), String> {
    use acyclic_native_runtime::testing::{
        Schedule, seeded_runtime,
        sweep::{seeds, sweep},
    };
    sweep(
        "harness-pending-redelivery",
        seeds(0..8).map_err(|error| error.to_string())?,
        |seed| {
            seeded_runtime(seed).map_err(|error| error.to_string())?.block_on(async {
            let (schedule, mut boundaries) = Schedule::seeded(seed);
            let history = async {
                let (reopened, tool, provider, executor, input) = fenced_pending_call(false).await?;
                pending_redelivery(&reopened, (tool, provider, executor, input), false).await?;
                let (reopened, tool, provider, executor, input) = fenced_pending_call(true).await?;
                pending_redelivery(&reopened, (tool, provider, executor, input), true).await?;
                let outcomes: [PendingOutcome; 4] = [
                    |_| Ok(None),
                    |operation| Err(Error::Indeterminate(operation)),
                    |_| Err(Error::Storage("reconciliation transport lost".into())),
                    |operation_id| Err(Error::CommandIndeterminate {
                        operation_id,
                        cause: crate::CommandIndeterminacy {
                            command: "pending".into(),
                            reason: crate::CommandIndeterminacyReason::Failed {
                                code: "STORAGE".into(),
                                error: "command outcome unknown".into(),
                            },
                        },
                    }),
                ];
                let (journal, _issuer) = super::publication_observation::journal().await?;
                for retired in [false, true] {
                    for outcome in outcomes {
                        let pending = start_pending_call(&journal, retired, outcome).await?;
                        pending_redelivery(&journal, pending, !retired).await?;
                    }
                }
                Ok::<(), Error>(())
            };
            let mut execution = Box::pin(schedule.scope(history));
            let result = loop {
                tokio::select! {
                    result = &mut execution => break result,
                    boundary = boundaries.recv() => boundary.expect("schedule boundary").release(),
                }
            };
            drop(execution);
            loop {
                tokio::select! {
                    () = schedule.drained() => break,
                    boundary = boundaries.recv() => boundary.expect("drain boundary").release(),
                }
            }
            result.map_err(|error| error.to_string())
        })
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// An effect published only under its call's operation: the first run never
/// lands it, and running it again is that one effect.
#[cfg(not(target_arch = "wasm32"))]
struct KeyedTool {
    calls: AtomicUsize,
    reconciles: AtomicUsize,
    effects: AtomicUsize,
    accepted: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}
#[cfg(not(target_arch = "wasm32"))]
impl crate::tool::ToolExecutor for KeyedTool {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(accepted) = self.accepted.lock().unwrap().take() {
                accepted.send(()).unwrap();
            }
            return Box::pin(std::future::pending());
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"value":"hello"}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
    fn reexecutes_fenced(&self) -> bool {
        true
    }
}

/// A fenced start of a keyed effect runs again, once, after reconciliation
/// finds no receipt; an unfenced effect is never repeated.
#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn a_fenced_keyed_call_runs_its_one_effect_again() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(scripted_model());
    let (accepted, acceptance) = tokio::sync::oneshot::channel();
    let tool = Arc::new(KeyedTool {
        calls: AtomicUsize::new(0),
        reconciles: AtomicUsize::new(0),
        effects: AtomicUsize::new(0),
        accepted: Mutex::new(Some(accepted)),
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
    tokio::select! {
        outcome = &mut first => panic!("execution ended before tool acceptance: {outcome:?}"),
        accepted = acceptance => accepted.expect("tool acceptance producer was dropped"),
    }
    drop(first);
    let reopened = journal.reopen_before_tool_completion("call-1");
    let settled = executor.execute(input.clone(), &reopened).await?;
    assert_eq!(settled.text, "done");
    assert!(reopened.0.lock().unwrap().iter().any(|record| matches!(
        &record.event,
        ExecutionEvent::ToolCompleted { call_id, .. } if call_id == "call-1"
    )));
    assert_eq!(settled, executor.execute(input, &reopened).await?);
    assert_eq!(
        (
            tool.calls.load(Ordering::SeqCst),
            tool.reconciles.load(Ordering::SeqCst),
            tool.effects.load(Ordering::SeqCst),
        ),
        (2, 1, 1),
        "one fenced reconciliation, one effect"
    );
    Ok(())
}
