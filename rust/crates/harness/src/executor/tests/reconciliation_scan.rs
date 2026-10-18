//! Review P1 (#899): a settled call without a start (a policy denial) stopped reconciliation
//! before a later call with an uncertain outcome, so the task stayed indeterminate forever.
use super::*;

/// How one call in the single model response settles.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// The policy denies it: a settled failure without a start.
    Deny,
    /// No tool is bound: a settled failure without a start.
    Ghost,
    /// The tool returns: a settled success.
    Ok,
    /// The tool fails definitively: a settled failure with a start.
    Fail,
    /// The response is lost after the effect: started, unsettled until reconciled.
    Lost,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Ghost => "ghost",
            Self::Ok => "ok",
            Self::Fail => "fail",
            Self::Lost => "lost",
        }
    }
}

fn mode_of(invocation: &ToolInvocation) -> &str {
    invocation.arguments["mode"].as_str().unwrap_or_default()
}

/// Counts executions and reconciliations per call.
#[derive(Default)]
struct ModeTool(Mutex<HashMap<String, (usize, usize)>>);

impl ToolExecutor for ModeTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.0
            .lock()
            .unwrap()
            .entry(invocation.call_id.clone())
            .or_default()
            .0 += 1;
        Box::pin(async move {
            match mode_of(&invocation) {
                "fail" => Err(Error::Invalid("tool refused".into())),
                "lost" => Err(Error::Cancelled),
                _ => Ok(ToolResult {
                    value: invocation.arguments,
                }),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.0
            .lock()
            .unwrap()
            .entry(invocation.call_id.clone())
            .or_default()
            .1 += 1;
        Box::pin(async move {
            Ok(Some(ToolResult {
                value: invocation.arguments,
            }))
        })
    }
}

struct DenyMode;

impl ToolPolicy for DenyMode {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.deny-mode".into(),
            version: "1".into(),
            digest: [7; 32],
        }
    }

    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _: &'a RuntimeScope,
        _: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        let decision = if mode_of(invocation) == "deny" {
            ToolPolicyDecision::Deny {
                reason: "denied".into(),
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

fn settlements(journal: &Journal) -> HashMap<String, usize> {
    let mut settled = HashMap::new();
    for record in journal.0.lock().unwrap().iter() {
        if let ExecutionEvent::ToolCompleted { call_id, .. }
        | ExecutionEvent::ToolFailed { call_id, .. } = &record.event
        {
            *settled.entry(call_id.clone()).or_default() += 1;
        }
    }
    settled
}

/// Runs one response of `modes` to completion the way the task runtime does: an ordinary run,
/// and after every indeterminate one, a reconciliation-only pass before the retry. Every pass
/// must settle the uncertain call, and the turn must finish within one retry per lost call.
async fn drive(modes: &[Mode]) -> Result<()> {
    let calls = modes.iter().enumerate().map(|(index, mode)| {
        ModelEvent::ToolCall(crate::model::ToolCall::new(
            format!("call-{index}"),
            match mode {
                Mode::Ghost => "example.ghost".into(),
                _ => "example.echo".into(),
            },
            json!({"mode": mode.name()}),
        ))
    });
    let model = Arc::new(
        ScriptedModelProvider::new([tool_turn(calls), done_turn()])
            .with_token_counter(fixture_token_count),
    );
    let tool = Arc::new(ModeTool::default());
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        echo_tools(
            tool.clone(),
            json!({"type":"object"}),
            json!({"type":"object"}),
            crate::tool::json_projection_schema(json!({"type":"object"})),
        )?,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        Some(Arc::new(DenyMode)),
    )?;
    let reconciling = executor.clone().into_reconciliation();
    let input = TurnInput {
        operation_id: OperationId::from_bytes([47; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 2,
    };
    let journal = Journal::default();
    let lost = modes
        .iter()
        .filter(|mode| matches!(mode, Mode::Lost))
        .count();
    let mut retries = 0;
    let output = loop {
        match executor.execute(input.clone(), &journal).await {
            Ok(output) => break output,
            Err(Error::Indeterminate(_)) => {}
            Err(error) => return Err(error),
        }
        retries += 1;
        assert!(retries <= lost, "{modes:?}: retry made no progress");
        let before = settlements(&journal).len();
        match reconciling.execute(input.clone(), &journal).await {
            Ok(_) | Err(Error::Indeterminate(_)) => {}
            Err(error) => return Err(error),
        }
        assert_eq!(
            settlements(&journal).len(),
            before + 1,
            "{modes:?}: reconciliation did not settle the uncertain call"
        );
    };
    assert_eq!(output.text, "done");
    assert_eq!(retries, lost, "{modes:?}");
    let settled = settlements(&journal);
    let counts = tool.0.lock().unwrap();
    for (index, mode) in modes.iter().enumerate() {
        let call = format!("call-{index}");
        assert_eq!(
            settled.get(&call),
            Some(&1),
            "{modes:?}: {call} settles once"
        );
        let expected = match mode {
            Mode::Deny | Mode::Ghost => (0, 0),
            Mode::Ok | Mode::Fail => (1, 0),
            Mode::Lost => (1, 1),
        };
        let observed = counts.get(&call).copied().unwrap_or_default();
        assert_eq!(observed, expected, "{modes:?}: {call} runs once");
    }
    drop(counts);
    let requests = model.requests();
    assert_eq!(requests.len(), 2, "{modes:?}");
    for index in 0..modes.len() {
        let call = format!("call-{index}");
        assert!(
            requests[1].messages.iter().any(|message| matches!(
                &message.content,
                ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) if *call_id == call
            )),
            "{modes:?}: {call} reaches the model"
        );
    }
    // Replay reproduces the turn without dispatch.
    assert_eq!(executor.execute(input, &journal).await?, output);
    assert_eq!(model.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn denied_call_does_not_block_reconciling_a_later_uncertain_call() -> Result<()> {
    drive(&[Mode::Deny, Mode::Lost]).await?;
    drive(&[Mode::Ghost, Mode::Lost]).await
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]

    /// Every combination of settled-without-start, settled-with-start, settled success,
    /// uncertain and unstarted calls in one response is reconciled exactly once, in order.
    #[test]
    fn every_call_in_a_response_reconciles_exactly_once(
        modes in proptest::collection::vec(
            proptest::prop_oneof![
                proptest::strategy::Just(Mode::Deny),
                proptest::strategy::Just(Mode::Ghost),
                proptest::strategy::Just(Mode::Ok),
                proptest::strategy::Just(Mode::Fail),
                proptest::strategy::Just(Mode::Lost),
            ],
            1..=5,
        )
    ) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(drive(&modes))
            .unwrap();
    }
}
