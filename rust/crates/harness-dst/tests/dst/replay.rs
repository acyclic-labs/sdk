//! Replay is free: a repeated `TurnInput` dispatches no model request and no
//! tool, whatever the first turn did. The same tail is repeated in about sixty
//! executor tests; this world covers the features those tests name: plain
//! tools, code mode, summaries, length stops, refusals, interrupted calls and
//! every approval outcome.
use super::*;
use acyclic_harness::{
    LocalHarnessConfig,
    approvals::ApprovalPolicy,
    interaction::ApprovalVerdict,
    local::{
        WorkspaceMounting,
        agent_conversation::{Conversation, Settlement},
    },
    model::{Model, ModelEvent, ModelRole, ToolCall},
    runtime::ToolContext,
    test_support::{
        PromptOutcome, ScriptedEvent, ScriptedModelProvider, ScriptedTurn, local_harness_config,
        model_visible_messages, prompt_settled,
    },
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
        json_projection_schema,
    },
};
use acyclic_stream::BoxProviderFuture;
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

/// One feature a repeated input must not re-run.
#[derive(Clone, Copy, Debug)]
enum Feature {
    Plain,
    Code,
    LengthStop,
    Refusal,
    Interrupted,
    Approve,
    Deny,
}

impl Feature {
    fn name(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Code => "code",
            Self::LengthStop => "length_stop",
            Self::Refusal => "refusal",
            Self::Interrupted => "interrupted",
            Self::Approve => "approve",
            Self::Deny => "deny",
        }
    }

    fn approval(self) -> bool {
        matches!(self, Self::Approve | Self::Deny)
    }
}

/// The scripted model: one tool step, then the answer, except where the
/// feature names another shape.
fn provider(feature: Feature) -> Arc<ScriptedModelProvider> {
    let seen = Arc::new(Mutex::new(0usize));
    Arc::new(
        ScriptedModelProvider::responding(move |request| {
            let messages: Vec<_> = model_visible_messages(&request.messages).collect();
            let tagged = messages
                .iter()
                .rev()
                .filter(|message| message.role == ModelRole::User)
                .any(|message| {
                    serde_json::to_string(&message.content)
                        .unwrap_or_default()
                        .contains("[dst:replay]")
                });
            if !tagged {
                return Ok(ScriptedTurn::Text("untagged".into()));
            }
            let mut seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let turn = match feature {
                Feature::LengthStop if *seen == 0 => {
                    *seen += 1;
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::Content {
                            delta: "partial".into(),
                        }),
                        ScriptedEvent::Event(ModelEvent::Completed {
                            metadata: json!({"finish_reason": "length"}),
                        }),
                    ])
                }
                Feature::LengthStop if *seen == 1 => {
                    *seen += 1;
                    ScriptedTurn::Text("continued".into())
                }
                Feature::Interrupted if *seen == 0 => {
                    *seen += 1;
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                            "interrupted".into(),
                            "dst.count".into(),
                            json!({}),
                        ))),
                        ScriptedEvent::Interrupted {
                            error: "connection lost after the call".into(),
                        },
                    ])
                }
                _ if *seen == 0 => {
                    *seen += 1;
                    let call = match feature {
                        Feature::Code => ToolCall::new(
                            "code".into(),
                            "code".into(),
                            json!({"code": "export default 1;"}),
                        ),
                        Feature::Refusal => ToolCall::new(
                            "missing".into(),
                            "read".into(),
                            json!({"path": "missing.txt"}),
                        ),
                        _ => ToolCall::new("count".into(), "dst.count".into(), json!({})),
                    };
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::ToolCall(call)),
                        ScriptedEvent::Event(ModelEvent::Completed {
                            metadata: json!({}),
                        }),
                    ])
                }
                _ => ScriptedTurn::Text("done".into()),
            };
            Ok(turn)
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
    policy: ApprovalPolicy,
) -> Result<Arc<LocalHarness>> {
    let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
    let checkout = directory.join("checkout");
    std::fs::create_dir_all(&checkout).map_err(storage)?;
    let config: LocalHarnessConfig = local_harness_config(checkout, (model, provider))?
        .without_user_settings()
        .workspace_mounting(WorkspaceMounting::Physical)
        .state_directory(directory.join("state"))
        .tool(tool)
        .approval_policy(policy);
    Ok(Arc::new(entry(LocalHarness::open(config)).await?))
}

/// Sends `id` and settles it, deciding any approval the feature names.
async fn settle(
    harness: &Arc<LocalHarness>,
    id: &str,
    prompt: &str,
    verdict: Option<ApprovalVerdict>,
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
        let verdict = verdict.ok_or_else(|| Error::Invalid("unexpected approval".into()))?;
        conversation
            .decide_with_binding(&request, binding, verdict)
            .await?;
        settlement = conversation.wait_reply(id, inbox, &mut events).await?;
    }
    match settlement {
        Settlement::Reply(acyclic_harness::agent::Reply::Answered {
            sequence, entry, ..
        }) => {
            let output = conversation.output_at(sequence, &entry).await?;
            Ok(PromptOutcome::Settled(entry, output))
        }
        Settlement::Reply(acyclic_harness::agent::Reply::Dropped) => {
            Ok(PromptOutcome::Failed("dropped".into()))
        }
        Settlement::Ended(reason) => Ok(PromptOutcome::Failed(reason)),
        Settlement::Approval { .. } => Err(Error::Invalid("unsettled approval".into())),
        Settlement::Reply(_) => Err(Error::Invalid("unsettled reply".into())),
    }
}

fn outcome_text(outcome: &PromptOutcome) -> String {
    match outcome {
        PromptOutcome::Settled(_, output) => output.text.clone(),
        PromptOutcome::Failed(message) => format!("failed: {message}"),
    }
}

fn world(feature: Feature) -> std::result::Result<(), String> {
    oracle::simulate(0, move || {
        entry(async move {
            let directory = temporary_directory().map_err(oracle::failed)?;
            let dispatches = Arc::new(AtomicUsize::new(0));
            let provider = provider(feature);
            let policy = if feature.approval() {
                ApprovalPolicy::default()
            } else {
                ApprovalPolicy::accept_all("dst")
            };
            let harness = open(
                directory.path(),
                Arc::clone(&provider),
                counting_tool(Arc::clone(&dispatches)),
                policy,
            )
            .await
            .map_err(oracle::failed)?;
            let verdict = match feature {
                Feature::Approve => Some(ApprovalVerdict::Approve),
                Feature::Deny => Some(ApprovalVerdict::Deny),
                _ => None,
            };
            let first = settle(&harness, "replay", "[dst:replay]", verdict)
                .await
                .map_err(oracle::failed)?;
            let requests = provider.request_count();
            if requests == 0 {
                return Err(format!(
                    "{}: the first input dispatched no model request",
                    feature.name()
                ));
            }
            let tools = dispatches.load(Ordering::SeqCst);
            let second = settle(&harness, "replay", "[dst:replay]", verdict)
                .await
                .map_err(oracle::failed)?;
            if provider.request_count() != requests {
                return Err(format!(
                    "{}: the repeated input dispatched {} model requests, not 0",
                    feature.name(),
                    provider.request_count() - requests
                ));
            }
            if dispatches.load(Ordering::SeqCst) != tools {
                return Err(format!(
                    "{}: the repeated input dispatched {} tools, not 0",
                    feature.name(),
                    dispatches.load(Ordering::SeqCst) - tools
                ));
            }
            if outcome_text(&first) != outcome_text(&second) {
                return Err(format!(
                    "{}: the repeated input answered {:?} after {:?}",
                    feature.name(),
                    outcome_text(&second),
                    outcome_text(&first)
                ));
            }
            Ok(())
        })
    })
}

#[test]
fn a_repeated_input_dispatches_nothing() -> std::result::Result<(), String> {
    for feature in [
        Feature::Plain,
        Feature::Code,
        Feature::LengthStop,
        Feature::Refusal,
        Feature::Interrupted,
        Feature::Approve,
        Feature::Deny,
    ] {
        world(feature)?;
    }
    Ok(())
}

/// A summary request replays exactly: the same id returns the same checkpoint
/// without a second model request.
#[test]
fn a_repeated_summary_dispatches_nothing() -> std::result::Result<(), String> {
    oracle::simulate(0, || {
        entry(async move {
            let directory = temporary_directory().map_err(oracle::failed)?;
            let provider = Arc::new(ScriptedModelProvider::responding(|request| {
                if request.summary {
                    Ok(ScriptedTurn::Text("summary".into()))
                } else {
                    Ok(ScriptedTurn::Text("done".into()))
                }
            }));
            let harness = open(
                directory.path(),
                Arc::clone(&provider),
                counting_tool(Arc::new(AtomicUsize::new(0))),
                ApprovalPolicy::accept_all("dst"),
            )
            .await
            .map_err(oracle::failed)?;
            let conversation = Conversation::open(harness.connect().await.map_err(oracle::failed)?);
            let first = conversation
                .compact("summary", Some("summarize"))
                .await
                .map_err(oracle::failed)?;
            let requests = provider.request_count();
            let second = conversation
                .compact("summary", Some("summarize"))
                .await
                .map_err(oracle::failed)?;
            if first != second {
                return Err(format!(
                    "the repeated summary settled at {second}, not {first}"
                ));
            }
            if provider.request_count() != requests {
                return Err(format!(
                    "the repeated summary dispatched {} model requests, not 0",
                    provider.request_count() - requests
                ));
            }
            Ok(())
        })
    })
}
