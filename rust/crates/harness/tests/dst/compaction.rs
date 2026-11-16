//! Compaction: a rewrite may only drop a complete older prefix, must retain
//! every call with its result, and must leave the history unchanged when the
//! summary cannot be produced or the boundary is not complete.
use super::*;
use acyclic_harness::{
    AgentId, OperationId,
    context::{Context, ContextPipeline, ContextSummary, ModelContextCapacity},
    conversation::wire::Notice,
    executor::StockExecutor,
    filesystem::MemoryHarnessStorage,
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelRole, ToolCall,
        ToolResultContent,
    },
    test_support::{ScriptedEvent, compact_projection_for_test, prefix_digest_for_test},
    tool::ToolRegistry,
};
use family::{Generator, Variant};
use serde_json::json;

/// The three trigger conditions: the first complete boundary, an early cut
/// that a byte overestimate would choose, and the last complete boundary.
const TRIGGERS: [&str; 3] = ["step0", "overestimate", "at_threshold"];

/// What sits immediately after the cut.
const STRADDLES: [&str; 4] = ["none", "tool_pending", "spawn_pending", "child_notice"];

/// What the summary produces.
const OUTCOMES: [&str; 4] = ["applied", "length_cut", "error", "rate_limited"];

pub(super) const GENERATOR: Generator = Generator {
    class: "D6",
    seeds: every_compaction_preserves_tool_exchanges_and_the_unchanged_source::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_compaction_preserves_tool_exchanges_and_the_unchanged_source,
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

fn trigger(seed: u64) -> &'static str {
    TRIGGERS[usize::try_from(seed % TRIGGERS.len() as u64).unwrap_or(0)]
}

fn outcome(seed: u64) -> &'static str {
    OUTCOMES[usize::try_from((seed / TRIGGERS.len() as u64) % OUTCOMES.len() as u64).unwrap_or(0)]
}

fn variants(seed: u64) -> Vec<Variant> {
    STRADDLES
        .iter()
        .map(|straddle| {
            Variant::from([
                ("trigger", trigger(seed)),
                ("straddle", *straddle),
                ("outcome", outcome(seed)),
            ])
        })
        .collect()
}

fn text(role: ModelRole, body: &str) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(body.into()),
    }
}

fn call(id: &str, name: &str) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
            id.into(),
            name.into(),
            json!({"path": "a.txt"}),
        ))),
    }
}

fn result(id: &str, name: &str) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: id.into(),
            name: name.into(),
            content: ToolResultContent::Json {
                value: json!({"ok": true}),
            },
        }),
    }
}

fn notice() -> ModelMessage {
    ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Part(ModelContentPart::Notices {
            notices: vec![Notice {
                source: "agents/child".into(),
                kind: "child.settled".into(),
                body: json!({"child": "agents/child"}),
                wake: true,
                coalesce_key: None,
            }],
        }),
    }
}

/// The history every world compacts. The straddle state is message two, so a
/// cut at three or six always leaves it on the summarized side.
fn history(straddle: &str) -> Context {
    let messages = vec![
        text(ModelRole::System, "instructions"),
        text(ModelRole::User, "older input"),
        match straddle {
            "tool_pending" => call("pending", "read"),
            "spawn_pending" => call("pending", "spawn"),
            "child_notice" => notice(),
            _ => text(ModelRole::User, "after the cut"),
        },
        text(ModelRole::User, "older input two"),
        text(ModelRole::Assistant, "older answer two"),
        text(ModelRole::User, "latest older input"),
        call("old", "read"),
        result("old", "read"),
        text(ModelRole::User, "current input"),
    ];
    Context {
        messages,
        current_input_index: Some(8),
        metadata: Default::default(),
    }
}

fn cut(trigger: &str, context: &Context) -> usize {
    match trigger {
        // The earliest complete older prefix.
        "step0" => 3,
        // The same early prefix, though the history is much larger.
        "overestimate" => 3,
        // The last complete boundary before the current input.
        _ => context.messages.len() - 3,
    }
}

fn prefix(context: &Context, through: usize) -> Context {
    let mut prefix = context.clone();
    prefix.messages.truncate(through);
    prefix.current_input_index = prefix
        .current_input_index
        .filter(|index| (*index as usize) < through);
    prefix
}

fn provider(outcome: &str) -> ScriptedModelProvider {
    let reply = match outcome {
        "length_cut" => ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "unfinished".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"finish_reason":"length"}),
            }),
        ]),
        "error" => ScriptedTurn::Events(vec![ScriptedEvent::Error {
            error: "provider internal error".into(),
        }]),
        "rate_limited" => ScriptedTurn::Events(vec![ScriptedEvent::Refused {
            status: 429,
            error: "rate limited".into(),
        }]),
        _ => ScriptedTurn::Text("SUMMARY".into()),
    };
    ScriptedModelProvider::new([reply]).with_context_capacity(ModelContextCapacity {
        context_tokens: 131_072,
        output_tokens: Some(4_096),
        output_shares_context: false,
    })
}

fn executor(provider: Arc<ScriptedModelProvider>) -> std::result::Result<StockExecutor, String> {
    Ok(StockExecutor::new(
        Model::new("fixture".into(), "compaction".into(), "1".into()).map_err(oracle::failed)?,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled))
}

async fn summary(
    provider: Arc<ScriptedModelProvider>,
    source: Context,
) -> std::result::Result<ContextSummary, String> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
        .await
        .map_err(oracle::failed)?;
    executor(provider)?
        .summarize(
            storage.journal().as_ref(),
            OperationId::new(),
            source,
            ModelContent::Text("summarize the retained history".into()),
        )
        .await
        .map_err(oracle::failed)
}

/// A real summary output, reused when the source prefix itself is incomplete
/// and therefore cannot be sent to a summary model.
async fn summary_output() -> std::result::Result<acyclic_harness::conversation::FileRef, String> {
    let source = Context {
        messages: vec![
            text(ModelRole::System, "instructions"),
            text(ModelRole::User, "older input"),
        ],
        current_input_index: None,
        metadata: Default::default(),
    };
    summary(Arc::new(provider("applied")), source)
        .await
        .map(|summary| summary.output)
}

/// A call and its result are both present, or neither is.
fn paired(context: &Context) -> std::result::Result<(), String> {
    let mut pending = std::collections::BTreeSet::new();
    for message in &context.messages {
        for part in message.content.parts() {
            match part {
                ModelContentPart::ToolCall(call) => {
                    pending.insert(call.call_id.as_str());
                }
                ModelContentPart::ToolResult { call_id, .. } => {
                    if !pending.remove(call_id.as_str()) {
                        return Err(format!("result {call_id} has no earlier call"));
                    }
                }
                _ => {}
            }
        }
    }
    if !pending.is_empty() {
        return Err(format!("calls without results: {pending:?}"));
    }
    Ok(())
}

fn compacted(context: &Context, summary: ContextSummary) -> std::result::Result<Context, String> {
    compact_projection_for_test(context, context.messages.len(), Some(summary))
        .map(|(retained, _)| retained)
        .map_err(oracle::failed)
}

fn manual_summary(
    context: &Context,
    through: usize,
    output: acyclic_harness::conversation::FileRef,
) -> std::result::Result<ContextSummary, String> {
    Ok(ContextSummary {
        operation_id: OperationId::new(),
        step: 0,
        source_messages: u32::try_from(through).map_err(oracle::failed)?,
        source_digest: prefix_digest_for_test(context, through).map_err(oracle::failed)?,
        output,
    })
}

fn check(
    trigger: &'static str,
    straddle: &'static str,
    outcome: &'static str,
) -> std::result::Result<(), String> {
    oracle::simulate(0, move || {
        entry(async move {
            let source = history(straddle);
            let applied = matches!(outcome, "applied");
            let pending = matches!(straddle, "tool_pending" | "spawn_pending");
            // A pending call can only be retained by refusing the rewrite. The
            // applied case puts it inside the summarized prefix; a failed
            // summary keeps the cut before it so the provider fault is the
            // only reason no rewrite happens.
            let through = if pending && !applied {
                2
            } else {
                cut(trigger, &source)
            };
            if through < 2 || through >= source.messages.len() {
                return Err(format!("trigger {trigger} chose an invalid cut {through}"));
            }
            let provider = Arc::new(provider(outcome));
            let summary = if applied && pending {
                // The prefix contains the unsettled call, so no summary model
                // may be asked to read it. Bind a real output to that prefix.
                manual_summary(&source, through, summary_output().await?)?
            } else {
                match summary(Arc::clone(&provider), prefix(&source, through)).await {
                    Ok(summary) => summary,
                    Err(error) if !applied => {
                        let expected = match outcome {
                            "length_cut" => "output-token limit",
                            "error" => "provider internal error",
                            "rate_limited" => "rate limited",
                            _ => "",
                        };
                        if !error.contains(expected) {
                            return Err(format!(
                                "{trigger}/{straddle}/{outcome}: failed for {error:?}, expected {expected:?}"
                            ));
                        }
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(format!(
                            "{trigger}/{straddle}/{outcome}: summary failed: {error}"
                        ));
                    }
                }
            };
            if summary.source_messages as usize != through {
                return Err(format!(
                    "{trigger}/{straddle}/{outcome}: summary covers {} of {through}",
                    summary.source_messages
                ));
            }
            match compacted(&source, summary) {
                Ok(retained) => {
                    if pending {
                        return Err(format!(
                            "{trigger}/{straddle}/{outcome}: compaction applied over an unsettled call"
                        ));
                    }
                    paired(&retained)?;
                    if !retained.messages.iter().any(|message| {
                        message
                            .content
                            .parts()
                            .iter()
                            .any(|part| matches!(part, ModelContentPart::File { .. }))
                    }) {
                        return Err(format!(
                            "{trigger}/{straddle}/{outcome}: compaction lost its summary"
                        ));
                    }
                    Ok(())
                }
                Err(error) => {
                    if pending && error.contains("splits a tool exchange") {
                        // The only safe rewrite is the one already in hand.
                        return Ok(());
                    }
                    Err(format!(
                        "{trigger}/{straddle}/{outcome}: compaction refused: {error}"
                    ))
                }
            }
        })
    })
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let trigger = trigger(seed);
    let outcome = outcome(seed);
    for straddle in STRADDLES {
        check(trigger, straddle, outcome)?;
    }
    Ok(true)
}
