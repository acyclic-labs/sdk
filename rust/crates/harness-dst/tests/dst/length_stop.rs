//! D4 length stop: a length stop never settles a turn. The turn continues
//! with the byte-exact reasoning the provider already emitted, the output
//! capacity never consumes the context reservation, and a length-cut
//! background call degrades to absent.
use super::*;
use acyclic_harness::{
    AgentId, OperationId,
    context::{Context, ContextPipeline, ModelContextCapacity},
    executor::StockExecutor,
    filesystem::MemoryHarnessStorage,
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelRole, ReasoningBlock,
        ReasoningKind, ReasoningOrigin, ReasoningOutcome, ToolCall,
    },
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn, model_visible_messages},
    tool::ToolRegistry,
};
use family::{Generator, Variant};
use oracle::{Prompted, World, done, tag};
use serde_json::json;
use std::sync::Mutex;

const FILES: [(&str, &str); 1] = [("a.txt", "alpha\n")];

/// The content a length stop leaves behind, the call it cuts, and the
/// capacity it advertises.
const CONTENTS: [&str; 4] = ["text", "reasoning_only", "partial_tool_call", "empty"];
const CALLS: [&str; 4] = ["turn", "final_report", "summary", "context_review"];
const CAPACITIES: [&str; 2] = ["output_eq_context", "output_lt_context"];

const SEEDS: &[u64] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

/// One world per (call, capacity, content): a world that reached every
/// content would run four harnesses and outlast the per-case limit.
pub(super) const GENERATOR: Generator = Generator {
    class: "D4",
    seeds: SEEDS,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = a_length_stop_continues_the_turn_with_its_reasoning,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5, seed_6 => 6, seed_7 => 7,
        seed_8 => 8, seed_9 => 9, seed_10 => 10, seed_11 => 11,
        seed_12 => 12, seed_13 => 13, seed_14 => 14, seed_15 => 15,
        seed_16 => 16, seed_17 => 17, seed_18 => 18, seed_19 => 19,
        seed_20 => 20, seed_21 => 21, seed_22 => 22, seed_23 => 23,
        seed_24 => 24, seed_25 => 25, seed_26 => 26, seed_27 => 27,
        seed_28 => 28, seed_29 => 29, seed_30 => 30, seed_31 => 31,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn call_kind(seed: u64) -> &'static str {
    seeded_variant(&CALLS, seed)
}

fn capacity(seed: u64) -> &'static str {
    seeded_variant(&CAPACITIES, seed / CALLS.len() as u64)
}

fn content(seed: u64) -> &'static str {
    seeded_variant(&CONTENTS, seed / (CALLS.len() * CAPACITIES.len()) as u64)
}

fn variants(seed: u64) -> Vec<Variant> {
    vec![Variant::from([
        ("content", content(seed)),
        ("call", call_kind(seed)),
        ("capacity", capacity(seed)),
    ])]
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    check(seed, call_kind(seed), capacity(seed), content(seed))?;
    Ok(true)
}

fn check(
    seed: u64,
    call: &'static str,
    capacity: &'static str,
    content: &'static str,
) -> std::result::Result<(), String> {
    match call {
        "turn" => turn_world(seed, capacity, content),
        "final_report" => report_world(seed, capacity, content),
        "summary" => summary_check(seed, capacity, content),
        _ => review_world(seed, capacity, content),
    }
}

fn context_capacity(name: &str) -> ModelContextCapacity {
    match name {
        // The published output maximum is the whole window, so it must never
        // be reserved ahead of input: the response shares the context.
        "output_eq_context" => ModelContextCapacity {
            context_tokens: 1_048_576,
            output_tokens: Some(1_048_576),
            output_shares_context: true,
        },
        _ => ModelContextCapacity {
            context_tokens: 1_048_576,
            output_tokens: Some(4096),
            output_shares_context: false,
        },
    }
}

fn reasoning() -> ReasoningBlock {
    ReasoningBlock {
        kind: ReasoningKind::Readable,
        text: Some("reasoning text".into()),
        origin: ReasoningOrigin {
            provider: "scripted".into(),
            model: "dst".into(),
            credential_scope: None,
        },
        continuity: b"reasoning-continuity".to_vec(),
        outcome: ReasoningOutcome::PreservedReadable,
    }
}

fn length_stop(content: &str) -> ScriptedTurn {
    let mut events = Vec::new();
    match content {
        "text" => events.push(ScriptedEvent::Event(ModelEvent::Content {
            delta: "reasoning text".into(),
        })),
        "reasoning_only" => events.push(ScriptedEvent::Event(ModelEvent::ReasoningBlock {
            block: reasoning(),
        })),
        "partial_tool_call" => events.push(ScriptedEvent::Event(ModelEvent::ToolCall(
            ToolCall::new("partial".into(), "read".into(), json!({"path": "a.txt"})),
        ))),
        _ => {}
    }
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: json!({"finish_reason": "length", "usage": {"completion_tokens": 8192}}),
    }));
    ScriptedTurn::Events(events)
}

fn continuation() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content {
            delta: "continued".into(),
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"finish": "stop"}),
        }),
    ])
}

/// The answer a length stop's continuation settles with: the text the stop
/// already emitted, when it left text, then the continuation's own text.
fn answer(content: &str) -> &'static str {
    if content == "text" {
        "reasoning textcontinued"
    } else {
        "continued"
    }
}

/// A turn step that reads the world's file and reports its input usage, so a
/// context review is admitted at the next boundary.
fn tool_turn(id: &str) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            "read".into(),
            json!({"path": "a.txt"}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"usage": {"prompt_tokens": 100, "completion_tokens": 1}}),
        }),
    ])
}

fn tool_call(id: &str, name: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            name.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        }),
    ])
}

/// The one model service of every D4 world. A turn request is selected by its
/// newest user brief; a summary request by its instruction, which names the
/// context manager or the reviewer.
fn service(
    call: &'static str,
    capacity: &'static str,
    content: &'static str,
) -> ScriptedModelProvider {
    let seen = Mutex::new(std::collections::BTreeMap::<String, usize>::new());
    ScriptedModelProvider::responding(move |request| {
        let messages: Vec<_> = model_visible_messages(&request.messages).collect();
        if request.summary {
            let instruction = messages
                .last()
                .map(|message| serde_json::to_string(&message.content).unwrap_or_default())
                .unwrap_or_default();
            let mut seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let count = seen.entry("summary".to_owned()).or_insert(0);
            let turn = if instruction.contains("You manage this worker's context outside its turns")
            {
                if *count == 0 {
                    length_stop(content)
                } else {
                    ScriptedTurn::Text(
                        json!({"version": 1, "summary": null, "review_at_input_tokens": 1000})
                            .to_string(),
                    )
                }
            } else {
                length_stop(content)
            };
            *count += 1;
            return Ok(turn);
        }
        let found = messages
            .iter()
            .rev()
            .filter(|message| message.role == ModelRole::User)
            .find_map(|message| {
                let body = serde_json::to_string(&message.content).unwrap_or_default();
                ["d4", "d4-report", "check"].into_iter().find(|name| {
                    oracle::appears(&tag(name))
                        .iter()
                        .any(|needle| body.contains(needle))
                })
            });
        let Some(name) = found else {
            return Ok(ScriptedTurn::Text("untagged".into()));
        };
        let mut seen = seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = seen.entry(name.to_owned()).or_insert(0);
        let turn = match name {
            "check" => ScriptedTurn::Text(done("check")),
            "d4-report" => match *count {
                0 => length_stop(content),
                _ => continuation(),
            },
            _ => match call {
                "turn" => match *count {
                    0 => length_stop(content),
                    1 => continuation(),
                    _ => ScriptedTurn::Text(done("d4")),
                },
                "final_report" => match *count {
                    0 => tool_call(
                        "d4-spawn",
                        "spawn",
                        json!({"name": "leaf", "task": tag("d4-report"), "role": "worker"}),
                    ),
                    1 => tool_call("d4-wait", "wait", json!({"agents": ["leaf"]})),
                    _ => ScriptedTurn::Text(done("d4")),
                },
                _ => match *count {
                    0..=2 => tool_turn(&format!("d4-read-{count}")),
                    _ => continuation(),
                },
            },
        };
        *count += 1;
        Ok(turn)
    })
    .with_context_capacity(context_capacity(capacity))
}

/// The assistant parts a length stop leaves for the request that continues
/// it: the text it emitted, its reasoning block, or nothing when it emitted
/// neither (an empty stop, or a tool call the stop cut).
fn carried_parts(content: &str) -> Vec<ModelContentPart> {
    match content {
        "text" => vec![ModelContentPart::Text {
            text: "reasoning text".into(),
            native: None,
        }],
        "reasoning_only" => vec![ModelContentPart::Reasoning { block: reasoning() }],
        _ => Vec::new(),
    }
}

/// The request that continued a length-stopped turn carries the stop's content
/// byte-exactly, and nothing else: the harness appends an empty text after a
/// carried block, so trailing empty text is not content.
fn carried(
    requests: &[acyclic_harness::model::ModelRequest],
    content: &str,
) -> std::result::Result<(), String> {
    let expected = carried_parts(content);
    requests
        .iter()
        .find(|request| {
            let mut parts: Vec<ModelContentPart> = model_visible_messages(&request.messages)
                .filter(|message| message.role == ModelRole::Assistant)
                .flat_map(|message| message.content.parts().iter().cloned())
                .collect();
            if parts.is_empty() {
                return false;
            }
            while matches!(
                parts.last(),
                Some(ModelContentPart::Text { text, .. }) if text.is_empty()
            ) {
                parts.pop();
            }
            parts == expected
        })
        .map(|_| ())
        .ok_or_else(|| format!("no request continued the length stop with {expected:?}"))
}

fn turn_world(
    seed: u64,
    capacity: &'static str,
    content: &'static str,
) -> std::result::Result<(), String> {
    oracle::simulate(seed, move || {
        entry(async move {
            let d4 = ("d4", tag("d4"));
            let check = ("check", tag("check"));
            let mut world = World::new(seed, &FILES, service("turn", capacity, content))
                .map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![d4], vec![check]])
                .await
                .map_err(oracle::failed)?;
            let Some((Prompted::Settled(text), _)) = incarnation.prompts.first() else {
                return Err(format!(
                    "the length-stopped turn did not settle: {:?}",
                    incarnation.prompts
                ));
            };
            if *text != answer(content) {
                return Err(format!(
                    "the turn settled with {text:?}, not the continuation"
                ));
            }
            carried(&incarnation.requests, content)?;
            Ok(())
        })
    })
}

/// The child's final message is its report to its parent, so a length stop on
/// the report turn must continue and the parent must read the continuation.
fn report_world(
    seed: u64,
    capacity: &'static str,
    content: &'static str,
) -> std::result::Result<(), String> {
    oracle::simulate(seed, move || {
        entry(async move {
            let d4 = ("d4", tag("d4"));
            let check = ("check", tag("check"));
            let mut world = World::new(seed, &FILES, service("final_report", capacity, content))
                .map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![d4], vec![check]])
                .await
                .map_err(oracle::failed)?;
            if !incarnation
                .prompts
                .iter()
                .any(|(outcome, _)| *outcome == Prompted::Settled(done("d4")))
            {
                return Err(format!(
                    "the root turn did not finish: {:?}",
                    incarnation.prompts
                ));
            }
            let results = oracle::wait_results(&incarnation.requests)?;
            let answer = results
                .iter()
                .find_map(|(_, value)| {
                    value.get("settled")?.as_array()?.iter().find_map(|entry| {
                        if entry.get("name")?.as_str()? != "leaf" {
                            return None;
                        }
                        entry.get("answer")?.as_str().map(str::to_owned)
                    })
                })
                .ok_or_else(|| format!("the parent never saw the child's report: {results:?}"))?;
            if answer != self::answer(content) {
                return Err(format!(
                    "the parent read {answer:?}, not the child's continuation"
                ));
            }
            carried(&incarnation.requests, content)?;
            Ok(())
        })
    })
}

/// A length-cut summary publishes no rewrite: the call degrades to absent and
/// is never re-dispatched.
fn summary_check(
    seed: u64,
    capacity: &'static str,
    content: &'static str,
) -> std::result::Result<(), String> {
    oracle::simulate(seed, move || {
        entry(async move {
            let provider = Arc::new(service("summary", capacity, content));
            let executor = StockExecutor::new(
                Model::new("scripted".into(), "dst".into(), "1".into()).map_err(oracle::failed)?,
                provider.clone(),
                ContextPipeline::default(),
                ToolRegistry::default(),
            );
            let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
                .await
                .map_err(oracle::failed)?;
            let source = Context {
                messages: vec![
                    ModelMessage {
                        role: ModelRole::System,
                        content: ModelContent::Text("instructions".into()),
                    },
                    ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("older input".into()),
                    },
                ],
                current_input_index: None,
                metadata: Default::default(),
            };
            let result = executor
                .summarize(
                    storage.journal().as_ref(),
                    OperationId::new(),
                    source,
                    ModelContent::Text("summarize the retained history".into()),
                )
                .await;
            match result {
                Err(Error::Invalid(reason))
                    if reason
                        == "summary reached its output-token limit; request a shorter summary" => {}
                other => {
                    return Err(format!(
                        "a length-cut summary did not degrade to absent: {other:?}"
                    ));
                }
            }
            if provider.request_count() != 1 {
                return Err(format!(
                    "the length-cut summary was dispatched {} times",
                    provider.request_count()
                ));
            }
            Ok(())
        })
    })
}

/// A length-cut review is rejected and re-asked at the next boundary; the
/// worker's turn still settles with its continuation.
fn review_world(
    seed: u64,
    capacity: &'static str,
    content: &'static str,
) -> std::result::Result<(), String> {
    oracle::simulate(seed, move || {
        entry(async move {
            let d4 = ("d4", tag("d4"));
            let check = ("check", tag("check"));
            let mut world = World::new(seed, &FILES, service("context_review", capacity, content))
                .map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![d4], vec![check]])
                .await
                .map_err(oracle::failed)?;
            let Some((Prompted::Settled(text), _)) = incarnation.prompts.first() else {
                return Err(format!(
                    "the reviewed turn did not settle: {:?}",
                    incarnation.prompts
                ));
            };
            if *text != "continued" {
                return Err(format!(
                    "the reviewed turn settled with {text:?}, not the continuation"
                ));
            }
            let rejected = incarnation.requests.iter().any(|request| {
                request.summary
                    && request.messages.last().is_some_and(|message| {
                        let body = serde_json::to_string(&message.content).unwrap_or_default();
                        body.contains("You manage this worker's context outside its turns")
                            && body.contains(
                                "Your previous decision was rejected: summary reached its \
                                 output-token limit",
                            )
                    })
            });
            if !rejected {
                return Err("the length-cut review was not rejected and re-asked".into());
            }
            Ok(())
        })
    })
}
