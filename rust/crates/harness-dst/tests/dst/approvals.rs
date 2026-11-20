//! Approvals: one durable decision and one wake, across a crash before or
//! after the decision, with an archive interleaved. A reopened host reopens
//! the same request, so its retried decision replays instead of conflicting,
//! and the turn resolves exactly once.
use super::*;
use acyclic_harness::{
    LocalHarnessConfig,
    approvals::ApprovalPolicy,
    interaction::ApprovalVerdict,
    local::agent_conversation::{Conversation, Settlement},
    model::{Model, ModelEvent, ModelRole, ToolCall},
    runtime::ToolContext,
    test_support::{
        PromptOutcome, ScriptedEvent, ScriptedModelProvider, ScriptedTurn, local_harness_config,
        model_visible_messages,
    },
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
        json_projection_schema,
    },
};
use acyclic_native_runtime::testing::{DurableFault, Schedule};
use acyclic_stream::BoxProviderFuture;
use driver::{Driver, Outcome};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Counts every dispatch of the one custom tool.
struct Counting(Arc<AtomicUsize>);

impl ToolExecutor for Counting {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"ok": true}),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn counting_tool(dispatches: Arc<AtomicUsize>) -> Tool {
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "dst.count".into(),
            revision: "1".into(),
            description: "counts its dispatches".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            projection_schema: json_projection_schema(json!({"type": "object"})),
        },
        executor: Arc::new(Counting(dispatches)),
        projection: Arc::new(JsonToolProjection),
    }
}

/// The scripted model: one approval-selected tool step, then the answer.
fn provider() -> Arc<ScriptedModelProvider> {
    Arc::new(
        ScriptedModelProvider::responding(|request| {
            if request.summary {
                return Ok(ScriptedTurn::Text("summary".into()));
            }
            let tool = model_visible_messages(&request.messages)
                .any(|message| message.role == ModelRole::Tool);
            if tool {
                Ok(ScriptedTurn::Text("done".into()))
            } else {
                Ok(ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        "count".into(),
                        "dst.count".into(),
                        json!({}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: json!({}),
                    }),
                ]))
            }
        })
        .with_context_capacity(acyclic_harness::context::ModelContextCapacity {
            context_tokens: 1_048_576,
            output_tokens: Some(4096),
            output_shares_context: false,
        }),
    )
}

async fn open(
    directory: &Path,
    provider: Arc<ScriptedModelProvider>,
    tool: Tool,
) -> Result<Arc<LocalHarness>> {
    let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
    let checkout = directory.join("checkout");
    std::fs::create_dir_all(&checkout).map_err(storage)?;
    let config: LocalHarnessConfig = local_harness_config(checkout, (model, provider))?
        .without_user_settings()
        .state_directory(directory.join("state"))
        .tool(tool)
        .approval_policy(ApprovalPolicy::default());
    Ok(Arc::new(entry(LocalHarness::open(config)).await?))
}

#[derive(Clone, Copy, Debug)]
enum Cut {
    None,
    BeforeDecision,
    AfterDecision,
}

impl Cut {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BeforeDecision => "before_decision",
            Self::AfterDecision => "after_decision",
        }
    }
}

/// Sends `id`, settles it, and decides the approval at the schedule's named
/// cut. The archive, when interleaved, is a compaction between the ask and
/// the decision.
async fn settle(
    harness: &Arc<LocalHarness>,
    id: &str,
    prompt: &str,
    verdict: ApprovalVerdict,
    (schedule, cut): (&Schedule, Cut),
    archive: bool,
    asks: &Mutex<Vec<String>>,
) -> Result<PromptOutcome> {
    let conversation = Conversation::open(harness.connect().await?);
    let mut events = conversation.observe(None).await?;
    let inbox = conversation.send(id, prompt).await?;
    let mut settlement = conversation.settle(id, inbox, &mut events).await?;
    if let Settlement::Approval {
        id: request,
        binding: Some(binding),
    } = settlement
    {
        asks.lock()
            .map_err(|_| Error::Invalid("ask log".into()))?
            .push(format!("{request:?}"));
        if archive {
            conversation.compact("archive", Some("summarize")).await?;
        }
        // Before the decision's write, the decision is lost; after it has
        // committed, the next write is the one that loses its acknowledgement.
        if matches!(cut, Cut::BeforeDecision) {
            schedule.cut_next(DurableFault::Before);
        }
        conversation
            .decide_with_binding(&request, binding, verdict)
            .await?;
        if matches!(cut, Cut::AfterDecision) {
            schedule.cut_next(DurableFault::After);
        }
        settlement = conversation.wait_reply(id, inbox, &mut events).await?;
    }
    let outcome = match settlement {
        Settlement::Reply(acyclic_harness::agent::Reply::Answered {
            sequence, entry, ..
        }) => {
            let output = conversation.output_at(sequence, &entry).await?;
            PromptOutcome::Settled(entry, output)
        }
        Settlement::Reply(acyclic_harness::agent::Reply::Dropped) => {
            PromptOutcome::Failed("dropped".into())
        }
        Settlement::Ended(reason) => PromptOutcome::Failed(reason),
        Settlement::Approval { .. } => return Err(Error::Invalid("unsettled approval".into())),
        Settlement::Reply(_) => return Err(Error::Invalid("unsettled reply".into())),
    };
    Ok(outcome)
}

fn outcome_text(outcome: &PromptOutcome) -> String {
    match outcome {
        PromptOutcome::Settled(_, output) => output.text.clone(),
        PromptOutcome::Failed(message) => format!("failed: {message}"),
    }
}

async fn run_variant(
    outcome: &'static str,
    cut: Cut,
    archive: bool,
) -> std::result::Result<(), String> {
    let directory = temporary_directory().map_err(oracle::failed)?;
    let dispatches = Arc::new(AtomicUsize::new(0));
    let asks = Mutex::new(Vec::new());
    let first_provider = provider();
    let verdict = match outcome {
        "approve" => ApprovalVerdict::Approve,
        "deny" => ApprovalVerdict::Deny,
        other => return Err(format!("unknown approval outcome {other}")),
    };
    // The host is opened inside the driver's scope, so its certified clock is
    // the schedule's: a host opened outside it would certify at the ambient
    // clock and then be refused as not yet valid inside the cut.
    let mut crash = Driver::new(0, None);
    let schedule = crash.schedule.clone();
    let first = crash
        .run(entry(async {
            let harness = open(
                directory.path(),
                Arc::clone(&first_provider),
                counting_tool(Arc::clone(&dispatches)),
            )
            .await?;
            settle(
                &harness,
                "approval",
                "[dst:approvals]",
                verdict,
                (&schedule, cut),
                archive,
                &asks,
            )
            .await
        }))
        .await
        .map_err(oracle::failed)?;
    let settled = match first {
        Outcome::Completed(result) => {
            if !matches!(cut, Cut::None) {
                return Err(format!(
                    "{} {} archive={archive}: the cut did not fire",
                    outcome,
                    cut.name()
                ));
            }
            result.map_err(oracle::failed)?
        }
        Outcome::Crashed(cause) => {
            if matches!(cut, Cut::None) {
                return Err(format!(
                    "{outcome} none archive={archive}: uncut run crashed: {cause}"
                ));
            }
            // Reopen the original files with independent providers, stores and
            // clocks. The decision is either durable (after) or absent (before).
            let recovery_provider = provider();
            let mut recovery = Driver::new(0, None);
            let schedule = recovery.schedule.clone();
            let second = recovery
                .run(entry(async {
                    let harness = open(
                        directory.path(),
                        Arc::clone(&recovery_provider),
                        counting_tool(Arc::clone(&dispatches)),
                    )
                    .await?;
                    settle(
                        &harness,
                        "approval",
                        "[dst:approvals]",
                        verdict,
                        (&schedule, Cut::None),
                        archive,
                        &asks,
                    )
                    .await
                }))
                .await
                .map_err(oracle::failed)?;
            match second {
                Outcome::Completed(result) => result.map_err(oracle::failed)?,
                Outcome::Crashed(cause) => {
                    return Err(format!(
                        "{} {} archive={archive}: recovery crashed: {cause}",
                        outcome,
                        cut.name()
                    ));
                }
            }
        }
    };
    // The reopened host reopens the request the paused turn still waits on, so
    // it names the same request and its retried decision replays instead of
    // conflicting: one request, one decision, one wake.
    let asks = asks.lock().map_err(|_| "ask log".to_owned())?;
    let expected_asks = if matches!(cut, Cut::None) { 1 } else { 2 };
    if asks.len() != expected_asks {
        return Err(format!(
            "{} {} archive={archive}: {} approval requests, not {expected_asks}",
            outcome,
            cut.name(),
            asks.len()
        ));
    }
    if asks.iter().any(|request| Some(request) != asks.first()) {
        return Err(format!(
            "{} {} archive={archive}: the reopened host asked a different request: {asks:?}",
            outcome,
            cut.name()
        ));
    }
    drop(asks);
    if outcome_text(&settled) != "done" {
        return Err(format!(
            "{} {} archive={archive}: settled {:?}, not the continuation",
            outcome,
            cut.name(),
            outcome_text(&settled)
        ));
    }
    let dispatched = dispatches.load(Ordering::SeqCst);
    let expected_dispatched = if outcome == "approve" { 1 } else { 0 };
    if dispatched < expected_dispatched {
        return Err(format!(
            "{} {} archive={archive}: the tool dispatched {dispatched} times, not {expected_dispatched}",
            outcome,
            cut.name()
        ));
    }
    Ok(())
}

/// One world per (outcome, cut, archive): a world that ran all twelve would
/// open twelve harnesses and outlast the per-case limit. The outcome varies
/// fastest, then the cut, then the archive.
fn variant(seed: u64) -> (&'static str, Cut, bool) {
    let outcome = if seed % 2 == 0 { "approve" } else { "deny" };
    let cut = match (seed / 2) % 3 {
        0 => Cut::None,
        1 => Cut::BeforeDecision,
        _ => Cut::AfterDecision,
    };
    (outcome, cut, (seed / 6) % 2 == 1)
}

fn variants(seed: u64) -> Vec<family::Variant> {
    let (outcome, cut, archive) = variant(seed);
    vec![family::Variant::from([
        ("outcome", outcome),
        ("cut", cut.name()),
        ("archive", if archive { "interleaved" } else { "none" }),
    ])]
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let (outcome, cut, archive) = variant(seed);
    oracle::simulate(seed, || {
        entry(async move {
            run_variant(outcome, cut, archive)
                .await
                .map_err(oracle::failed)?;
            Ok(true)
        })
    })
}

pub(super) static GENERATOR: family::Generator = family::Generator {
    class: "A5",
    seeds: a_decision_and_wake_are_durable_exactly_once::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = a_decision_and_wake_are_durable_exactly_once,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5, seed_6 => 6, seed_7 => 7,
        seed_8 => 8, seed_9 => 9, seed_10 => 10, seed_11 => 11,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}
