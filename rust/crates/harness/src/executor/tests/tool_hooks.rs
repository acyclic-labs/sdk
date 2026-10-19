use super::*;
use crate::hooks::{BoundHooks, HookDiagnostic, HookEvent, HookInput, HookOutput};
use crate::task_volume::TaskVolume;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::atomic::AtomicBool;

struct HookBackend {
    files: TaskVolume<MemoryStream>,
    journal: Arc<Journal>,
    operation: OperationId,
    cut: u8,
    interrupted: AtomicBool,
    pre: AtomicUsize,
    post: AtomicUsize,
    stop: AtomicUsize,
    stop_blocks: AtomicUsize,
    reason: String,
    replacement: i64,
    denied: bool,
    explicit_denial: AtomicBool,
    ask: bool,
}

impl HookBackend {
    fn interrupt(&self, cut: u8) -> Result<()> {
        if self.cut == cut && !self.interrupted.swap(true, Ordering::SeqCst) {
            return Err(Error::Indeterminate(self.operation));
        }
        Ok(())
    }
}

impl ToolExecutor for HookBackend {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async { panic!("hook binding cannot execute a tool") })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { panic!("hook binding cannot reconcile a tool") })
    }

    fn run_hooks<'a>(
        &'a self,
        _: (&'a crate::hooks::Hooks, [u8; 32]),
        event: HookEvent,
        input: HookInput<'a>,
        _: &'a ToolRegistry,
        context: crate::runtime::ToolContext,
        retained_only: bool,
    ) -> BoxFuture<'a, Result<Option<HookOutput>>> {
        Box::pin(async move {
            if event == HookEvent::PostToolUse {
                // Test-provider leaf lock; released before any volume/provider IO.
                let completed = self
                    .journal
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }));
                if !completed {
                    return Err(Error::Invalid(
                        "post-tool effect has no retained completion".into(),
                    ));
                }
                if !retained_only {
                    assert!(matches!(
                        input,
                        HookInput::Tool {
                            response: Some(_),
                            ..
                        }
                    ));
                }
                self.interrupt(2)?;
            }
            let result = crate::hooks::effects::run_once::<_, _, _, _, _, true>(
                &self.files,
                (
                    TaskId::from_bytes(context.task().id().into_bytes()),
                    context.operation_id(),
                    event.name(),
                ),
                65_536,
                !retained_only,
                || std::future::ready(Ok((Vec::new(), None))),
                || async {
                    if event == HookEvent::PreToolUse {
                        self.pre.fetch_add(1, Ordering::SeqCst);
                        Ok(HookOutput {
                            blocked: (self.denied && self.explicit_denial.load(Ordering::SeqCst))
                                .then(|| self.reason.clone()),
                            approval: self.ask.then(|| self.reason.clone()),
                            updated_input: Some(json!({"value":self.replacement})),
                            additional_context: vec![self.reason.clone()],
                            diagnostics: vec![HookDiagnostic {
                                cause: crate::ProcessError::NotFound {
                                    exit_code: None,
                                    message: self.reason.clone(),
                                },
                                blocking: self.denied
                                    && !self.explicit_denial.load(Ordering::SeqCst),
                            }],
                            ..Default::default()
                        })
                    } else if event == HookEvent::Stop {
                        let attempt = self.stop.fetch_add(1, Ordering::SeqCst);
                        Ok(HookOutput {
                            blocked: (attempt < self.stop_blocks.load(Ordering::SeqCst))
                                .then(|| self.reason.clone()),
                            ..Default::default()
                        })
                    } else {
                        self.post.fetch_add(1, Ordering::SeqCst);
                        Ok(HookOutput {
                            additional_context: vec![format!("post:{}", self.reason)],
                            ..Default::default()
                        })
                    }
                },
            )
            .await?;
            self.interrupt(match event {
                HookEvent::PreToolUse => 1,
                HookEvent::Stop => 4,
                _ => 3,
            })?;
            Ok(result)
        })
    }
}

async fn fixture(
    cut: u8,
    reason: String,
    replacement: i64,
    denied: bool,
    ask: bool,
) -> Result<(
    StockExecutor,
    Arc<ScriptedModelProvider>,
    Arc<FakeTool>,
    Arc<HookBackend>,
)> {
    let operation = OperationId::from_bytes([19; 16]);
    let hooks = Arc::new(HookBackend {
        files: TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("tool-hooks")?,
        )
        .await?,
        journal: Arc::new(Journal::default()),
        operation,
        cut,
        interrupted: AtomicBool::new(false),
        pre: AtomicUsize::new(0),
        post: AtomicUsize::new(0),
        stop: AtomicUsize::new(0),
        stop_blocks: AtomicUsize::new(0),
        reason,
        replacement,
        denied,
        explicit_denial: AtomicBool::new(false),
        ask,
    });
    let scope = RuntimeScope::new(
        Capabilities::new([
            "task:spawn:context-probe@1",
            "tool:call:example.echo",
            "model:generate",
            capability::INTERACTION_ROUTE,
        ]),
        Limits::default(),
    )?;
    let mut context = capture_admitted_context(scope.clone()).await?;
    Arc::make_mut(&mut context.harness).workspace_tools = Some(hooks.clone());
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let provider = Arc::new(scripted_model());
    let mut rules = crate::hooks::Hooks::default();
    rules.add_json(
        br#"{"hooks":{
        "PreToolUse":[{"hooks":[{"type":"command","command":"configured","args":[]}]}],
        "PostToolUse":[{"hooks":[{"type":"command","command":"configured","args":[]}]}]
    }}"#,
    )?;
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "hooks".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(scope, None)?
    .with_task_context(&context, operation)?
    .with_hooks(Some(Arc::new(BoundHooks {
        rules,
        manifest: [1; 32],
    })));
    Ok((executor, provider, tool, hooks))
}

fn input() -> TurnInput {
    TurnInput {
        operation_id: OperationId::from_bytes([19; 16]),
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 4,
    }
}

#[tokio::test]
async fn session_only_hooks_preserve_the_existing_tool_execution_identity() -> Result<()> {
    let (executor, _, _, _) = fixture(0, "context".into(), 42, false, false).await?;
    let baseline = executor.clone().with_hooks(None);
    let mut rules = crate::hooks::Hooks::default();
    rules.add_json(
        br#"{"hooks":{"SessionStart":[{"hooks":[{
        "type":"command","command":"configured","args":[]
    }]}]}}"#,
    )?;
    let session_only = executor.clone().with_hooks(Some(Arc::new(BoundHooks {
        rules,
        manifest: [2; 32],
    })));
    assert!(session_only.hooks.is_none());
    let digest = StockExecution::ordinary(&baseline).request_digest(&input())?;
    assert_eq!(
        StockExecution::ordinary(&session_only).request_digest(&input())?,
        digest
    );
    assert_ne!(
        StockExecution::ordinary(&executor).request_digest(&input())?,
        digest
    );
    Ok(())
}

/// Integers that tool artifacts represent exactly; larger magnitudes are
/// rejected below, before any tool dispatch.
const EXACT_JSON_INTEGER: std::ops::Range<i64> = -(1 << 53) + 1..(1 << 53);

#[tokio::test]
async fn inexact_hook_replacements_are_rejected_before_dispatch() -> Result<()> {
    for replacement in [1 << 53, i64::MIN] {
        let (executor, _, tool, hooks) =
            fixture(0, "inexact".into(), replacement, false, false).await?;
        let result = executor.execute(input(), hooks.journal.as_ref()).await;
        assert!(
            matches!(&result, Err(Error::Invalid(message)) if message.contains("inexact number")),
            "{result:?}"
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 0);
    }
    // Control: the largest exact magnitude is dispatched.
    let (executor, _, tool, hooks) =
        fixture(0, "exact".into(), EXACT_JSON_INTEGER.end - 1, false, false).await?;
    executor.execute(input(), hooks.journal.as_ref()).await?;
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn provider_requests_preserve_guard_results_across_every_tool_hook_cut() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::process_io)?;
    proptest!(|(cut in 0u8..4, reason in "[a-z]{1,32}", replacement in EXACT_JSON_INTEGER, denied in any::<bool>())| {
        runtime.block_on(async {
            let (executor, provider, tool, hooks) = fixture(cut, reason.clone(), replacement, denied, false).await?;
            let first = executor.execute(input(), hooks.journal.as_ref()).await;
            if cut == 1 || !denied && cut > 1 {
                prop_assert!(matches!(first, Err(Error::Indeterminate(_))), "{first:?}");
            } else {
                prop_assert!(first.is_ok(), "{first:?}");
            }
            let result = executor.execute(input(), hooks.journal.as_ref()).await?;
            prop_assert_eq!(executor.execute(input(), hooks.journal.as_ref()).await?, result);
            prop_assert_eq!(provider.requests().len(), 2);
            prop_assert_eq!(tool.0.load(Ordering::SeqCst), usize::from(!denied));
            prop_assert_eq!(hooks.pre.load(Ordering::SeqCst), 1);
            prop_assert_eq!(hooks.post.load(Ordering::SeqCst), usize::from(!denied));
            let requests = provider.requests();
            let prefix = executor.completed_tool_prefix(hooks.journal.as_ref(), &input(), 0, None).await?;
            prop_assert_eq!(&prefix.request().messages, &requests[1].messages);
            prop_assert!(requests[1].messages.iter().any(|message|
                message.role == ModelRole::System && message.content == ModelContent::Text(reason.clone())));
            if !denied {
                prop_assert!(requests[1].messages.iter().any(|message|
                    message.content == ModelContent::Text(format!("post:{reason}"))),
                    "post-tool feedback is missing");
                let retained = {
                    let records = hooks.journal.0.lock().unwrap();
                    records.iter().find_map(|record| match &record.event {
                        ExecutionEvent::ToolStarted { invocation, .. } => Some(invocation.clone()), _ => None,
                    }).unwrap()
                };
                let invocation = load_json::<crate::tool::RecordedInvocation>(
                    hooks.journal.as_ref(), &retained, None).await?;
                prop_assert_eq!(invocation.0.invocation.arguments, json!({"value":replacement}));
            }
            Ok(())
        })?;
    });
    Ok(())
}

#[tokio::test]
async fn completion_order_detector_rejects_the_original_post_before_commit_path() -> Result<()> {
    let (executor, _, _, hooks) = fixture(0, "feedback".into(), 42, false, false).await?;
    let invocation = ToolInvocation::for_model_call(
        input().operation_id,
        0,
        "call-1".into(),
        "example.echo".into(),
        json!({"value":42}),
        None,
    )?;
    let result = ToolResult {
        value: json!({"value":42}),
    };
    let projection = json!({"kind":"json", "value":result.value});
    assert!(matches!(StockExecution::ordinary(&executor)
        .complete_with_post_hook::<false>(hooks.journal.as_ref(), (input().operation_id,0),
            &invocation, &result, &projection).await,
        Err(Error::Invalid(message)) if message == "post-tool effect has no retained completion"));
    Ok(())
}

#[tokio::test]
async fn hook_approvals_use_the_existing_interaction_before_tool_admission() -> Result<()> {
    for approved in [false, true] {
        let (executor, provider, tool, hooks) =
            fixture(0, "Approve changed arguments".into(), 42, false, true).await?;
        for _ in 0..2 {
            assert!(matches!(
                executor
                    .execute_progress(input(), hooks.journal.as_ref())
                    .await?,
                ExecutionProgress::Pending(_)
            ));
        }
        assert_eq!(hooks.pre.load(Ordering::SeqCst), 1);
        assert_eq!(tool.0.load(Ordering::SeqCst), 0);
        assert!(
            !hooks
                .journal
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
        );
        {
            // Test journal interaction lock is a leaf, never held across IO.
            let mut requests = hooks.journal.2.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let (_, outcome) = requests.values_mut().next().unwrap();
            *outcome = Some(if approved {
                InteractionOutcome::Approved {}
            } else {
                InteractionOutcome::Denied {}
            });
        }
        let progress = executor
            .execute_progress(input(), hooks.journal.as_ref())
            .await?;
        assert_eq!(matches!(progress, ExecutionProgress::Ready(_)), approved);
        assert_eq!(
            matches!(progress, ExecutionProgress::Rejected(_)),
            !approved
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), usize::from(approved));
        assert_eq!(provider.requests().len(), if approved { 2 } else { 1 });
    }
    let (executor, _, tool, hooks) =
        fixture(0, "deny takes precedence".into(), 42, true, true).await?;
    executor.execute(input(), hooks.journal.as_ref()).await?;
    assert_eq!(tool.0.load(Ordering::SeqCst), 0);
    assert!(hooks.journal.2.lock().unwrap().is_empty());
    Ok(())
}

proptest! {
    // Hook context gathered per call moves after every result of the step, and
    // each group keeps its order. Negative control: the gathered order puts a
    // system message between two results whenever a non-final call has context.
    #[test]
    fn step_hook_context_follows_every_result(contexts in prop::collection::vec(0usize..3, 1..6)) {
        let mut step = Vec::new();
        let (mut results, mut notes) = (Vec::new(), Vec::new());
        for (call, count) in contexts.iter().enumerate() {
            let result = ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Text(format!("result {call}")),
            };
            results.push(result.clone());
            step.push(result);
            for note in 0..*count {
                let context = ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!("context {call}.{note}")),
                };
                notes.push(context.clone());
                step.push(context);
            }
        }
        let interrupted = step
            .windows(2)
            .any(|pair| pair[0].role == ModelRole::System && pair[1].role == ModelRole::Tool);
        prop_assert_eq!(
            interrupted,
            contexts[..contexts.len() - 1].iter().any(|count| *count > 0)
        );
        let mut step = results.clone();
        super::super::turn::append_step_context(&mut step, &mut notes.clone(), None).unwrap();
        results.extend(notes);
        prop_assert_eq!(step, results);
    }
}

#[test]
fn hook_free_steps_allocate_no_sorting_scratch() {
    for broken in [false, true] {
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Text("result".into()),
            };
            512
        ];
        let mut context = Vec::new();
        let measured = acyclic_test_support::measure(|| {
            if broken {
                // Restore the original production stable sort. Even an already
                // ordered step allocated uncharged scratch in the native path.
                messages.sort_by_key(|message| message.role != ModelRole::Tool);
            } else {
                super::super::turn::append_step_context(&mut messages, &mut context, None).unwrap();
            }
        });
        assert_eq!(
            (measured.count_total, measured.bytes_total) == (0, 0),
            !broken
        );
    }
}

/// Exhaust every Stop disposition within a bounded turn, including denial at
/// the final step. Replay must retain each disposition without invoking again.
#[tokio::test]
async fn stop_denials_never_settle_success_at_any_step_budget() -> Result<()> {
    for cut in [0, 4] {
        for budget in 1..=4 {
            for blocks in 0..=budget {
                let (mut executor, _, _, hooks) =
                    fixture(cut, "continue working".into(), 42, false, false).await?;
                hooks.stop_blocks.store(blocks as usize, Ordering::SeqCst);
                executor.provider = Arc::new(ScriptedModelProvider::new(
                    (0..budget).map(|step| ScriptedTurn::Text(format!("answer {step}"))),
                ));
                let mut rules = crate::hooks::Hooks::default();
                rules.add_json(br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"configured","args":[]}]}]}}"#)?;
                executor = executor.with_hooks(Some(Arc::new(BoundHooks {
                    rules,
                    manifest: [3; 32],
                })));
                let mut input = input();
                input.max_steps = budget;
                if cut == 4 {
                    assert!(matches!(
                        executor
                            .execute(input.clone(), hooks.journal.as_ref())
                            .await,
                        Err(Error::Indeterminate(_))
                    ));
                    assert_eq!(hooks.stop.load(Ordering::SeqCst), 1);
                }
                for _ in 0..2 {
                    let result = executor
                        .execute(input.clone(), hooks.journal.as_ref())
                        .await;
                    if blocks == budget {
                        assert!(
                            matches!(result, Err(Error::Conflict(ref reason))
                        if reason == "executor step limit reached"),
                            "Stop denial escaped budget {budget}: {result:?}"
                        );
                    } else {
                        assert_eq!(result?.text, format!("answer {blocks}"));
                    }
                    assert_eq!(
                        hooks.stop.load(Ordering::SeqCst),
                        (blocks + 1).min(budget) as usize
                    );
                }
            }
        }
    }
    Ok(())
}

/// A schema/capability rejection precedes PreToolUse. Its final failure is
/// replayable without inventing an effect claim, while strict effect replay
/// still rejects a missing claim for a tool that was actually dispatched.
#[tokio::test]
async fn completed_prefix_replays_pre_hook_admission_failures_without_starting_hooks() -> Result<()>
{
    for missing in [true, false] {
        let (mut executor, provider, tool, hooks) =
            fixture(0, "unused".into(), 42, false, false).await?;
        executor.tools = if missing {
            ToolRegistry::default()
        } else {
            echo_tools(
                tool.clone(),
                json!({"type":"array"}),
                json!({"type":"object"}),
                crate::tool::json_projection_schema(json!({"type":"object"})),
            )?
        };
        executor.execute(input(), hooks.journal.as_ref()).await?;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(tool.0.load(Ordering::SeqCst), 0);
        assert_eq!(hooks.pre.load(Ordering::SeqCst), 0);
        assert_eq!(hooks.post.load(Ordering::SeqCst), 0);
        let prefix = executor
            .completed_tool_prefix(hooks.journal.as_ref(), &input(), 0, None)
            .await?;
        assert_eq!(&prefix.request().messages, &requests[1].messages);
        executor.execute(input(), hooks.journal.as_ref()).await?;
        assert_eq!(provider.requests().len(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn explicit_guard_denials_replay_the_exact_model_context() -> Result<()> {
    let (executor, provider, tool, hooks) =
        fixture(0, "explicit refusal".into(), 42, true, false).await?;
    hooks.explicit_denial.store(true, Ordering::SeqCst);
    executor.execute(input(), hooks.journal.as_ref()).await?;
    let requests = provider.requests();
    let prefix = executor
        .completed_tool_prefix(hooks.journal.as_ref(), &input(), 0, None)
        .await?;
    assert_eq!(&prefix.request().messages, &requests[1].messages);
    executor.execute(input(), hooks.journal.as_ref()).await?;
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(tool.0.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.pre.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.post.load(Ordering::SeqCst), 0);
    Ok(())
}
