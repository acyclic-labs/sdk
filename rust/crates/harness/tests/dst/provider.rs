//! Provider requests: the canonical history only grows, except when a context
//! rewrite is applied, and a length stop continues the turn instead of
//! settling it.
use super::*;
use acyclic_harness::{
    model::{
        Model, ModelContentPart, ModelEvent, ModelRequest, ModelRole, ReasoningBlock,
        ReasoningKind, ReasoningOrigin, ReasoningOutcome,
    },
    test_support::{ScriptedEvent, ScriptedTurn},
};
use family::{Generator, Variant};
use oracle::{Prompted, World, done, tag};
use serde_json::json;
use sha2::Digest as _;
use std::sync::Mutex;

const DIALECTS: [&str; 3] = ["chat_completions", "responses", "messages"];
const TRANSITIONS: [&str; 9] = [
    "tool_result",
    "parallel_results",
    "child_notice_mid_turn",
    "compaction_applied",
    "background_summary",
    "context_review",
    "reopen",
    "length_continuation",
    "retry_after",
];
const CONTENTS: [&str; 4] = ["text", "reasoning_only", "partial_tool_call", "empty"];
const CALLS: [&str; 4] = ["turn", "final_report", "summary", "context_review"];
const CAPACITIES: [&str; 2] = ["output_eq_context", "output_lt_context"];

pub(super) const PREFIX_GENERATOR: Generator = Generator {
    class: "D3",
    seeds: prefix_stability_across_every_transition::VALUES,
    variants: prefix_variants,
    world: prefix_world,
};

pub(super) const LENGTH_GENERATOR: Generator = Generator {
    class: "D4",
    seeds: length_stop_continues_or_degrades::VALUES,
    variants: length_variants,
    world: length_world,
};

acyclic_test_support::cases! {
    module = prefix_stability_across_every_transition,
    values: u64,
    runner = super::prefix_case,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4,
        seed_5 => 5, seed_6 => 6, seed_7 => 7, seed_8 => 8, seed_9 => 9,
        seed_10 => 10, seed_11 => 11, seed_12 => 12, seed_13 => 13, seed_14 => 14,
        seed_15 => 15, seed_16 => 16, seed_17 => 17, seed_18 => 18, seed_19 => 19,
        seed_20 => 20, seed_21 => 21, seed_22 => 22, seed_23 => 23, seed_24 => 24,
        seed_25 => 25, seed_26 => 26,
    ],
}

acyclic_test_support::cases! {
    module = length_stop_continues_or_degrades,
    values: u64,
    runner = super::length_case,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5, seed_6 => 6, seed_7 => 7,
    ],
}

fn prefix_case(seed: u64) -> std::result::Result<(), String> {
    PREFIX_GENERATOR.run(seed)
}

fn length_case(seed: u64) -> std::result::Result<(), String> {
    LENGTH_GENERATOR.run(seed)
}

fn transition(seed: u64) -> &'static str {
    TRANSITIONS
        .get(usize::try_from(seed).unwrap_or(0) % TRANSITIONS.len())
        .copied()
        .unwrap_or("tool_result")
}

fn dialect(seed: u64) -> &'static str {
    DIALECTS
        .get(usize::try_from(seed / TRANSITIONS.len() as u64).unwrap_or(0) % DIALECTS.len())
        .copied()
        .unwrap_or("chat_completions")
}

fn prefix_variants(seed: u64) -> Vec<Variant> {
    vec![Variant::from([
        ("transition", transition(seed)),
        ("dialect", dialect(seed)),
    ])]
}

fn call(seed: u64) -> (u64, &'static str) {
    let call = CALLS
        .get(usize::try_from(seed).unwrap_or(0) % CALLS.len())
        .copied()
        .unwrap_or("turn");
    (seed, call)
}

fn capacity(seed: u64) -> &'static str {
    CAPACITIES
        .get(usize::try_from(seed / CALLS.len() as u64).unwrap_or(0) % CAPACITIES.len())
        .copied()
        .unwrap_or("output_lt_context")
}

fn length_variants(seed: u64) -> Vec<Variant> {
    let (_, call) = call(seed);
    let capacity = capacity(seed);
    CONTENTS
        .iter()
        .map(|content| {
            Variant::from([
                ("content", *content),
                ("call", call),
                ("capacity", capacity),
            ])
        })
        .collect()
}

fn request_tag(request: &ModelRequest, tags: &[&'static str]) -> Option<&'static str> {
    let message = request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == ModelRole::User)?;
    let body = serde_json::to_string(&message.content).unwrap_or_default();
    tags.iter().copied().find(|candidate| {
        let text = tag(candidate);
        body.contains(&text)
            || body.contains(&format!(
                "\"sha256\":{}",
                serde_json::to_string(&sha2::Sha256::digest(text.as_bytes())).unwrap_or_default()
            ))
    })
}

fn assistant_since_prompt(request: &ModelRequest) -> usize {
    let start = request
        .messages
        .iter()
        .rposition(|message| message.role == ModelRole::User)
        .unwrap_or(0);
    request.messages[start..]
        .iter()
        .filter(|message| message.role == ModelRole::Assistant)
        .count()
}

fn reasoning(model: &str, continuity: &[u8]) -> ModelEvent {
    ModelEvent::ReasoningBlock {
        block: ReasoningBlock {
            kind: ReasoningKind::Readable,
            text: Some("reasoning retained".into()),
            origin: ReasoningOrigin {
                provider: "scripted".into(),
                model: model.into(),
                credential_scope: None,
            },
            continuity: continuity.to_vec(),
            outcome: ReasoningOutcome::PreservedReadable,
        },
    }
}

fn length_stop(content: &str, model: &str) -> ScriptedTurn {
    let completed = ScriptedEvent::Event(ModelEvent::Completed {
        metadata: json!({"finish_reason":"length","usage":{"completion_tokens":8192}}),
    });
    let mut events = match content {
        "text" => vec![ScriptedEvent::Event(ModelEvent::Content {
            delta: "partial text".into(),
        })],
        "reasoning_only" => vec![ScriptedEvent::Event(reasoning(model, b"exact reasoning"))],
        "partial_tool_call" => vec![ScriptedEvent::Event(ModelEvent::ToolCall(
            oracle::call("cut-call", "read", json!({"path":"missing.txt"})),
        ))],
        _ => Vec::new(),
    };
    events.push(completed);
    ScriptedTurn::Events(events)
}

fn text_with_usage(text: String, input_tokens: u64) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content { delta: text }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"finish_reason":"stop","usage":{"prompt_tokens":input_tokens,"completion_tokens":1}}),
        }),
    ])
}

fn tool_with_usage() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
            "d4-read",
            "read",
            json!({"path":"a.txt"}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"finish_reason":"tool_calls","usage":{"prompt_tokens":100,"completion_tokens":1}}),
        }),
    ])
}

fn manager_rewrite(request: &ModelRequest) -> ScriptedTurn {
    let drop_through = request.messages.len().saturating_sub(1).max(1);
    ScriptedTurn::Text(
        json!({"version":1,"drop_through":drop_through,"summary":"compacted history"}).to_string(),
    )
}

fn review_decision() -> ScriptedTurn {
    ScriptedTurn::Text(
        json!({"version":1,"summary":null,"review_at_input_tokens":999_999}).to_string(),
    )
}

fn prefix(message: &str) -> std::result::Result<(), String> {
    Err(message.into())
}

fn stable(first: &ModelRequest, next: &ModelRequest) -> std::result::Result<(), String> {
    if first.model != next.model {
        return prefix("the request's model selection changed between steps");
    }
    if first.tools != next.tools || first.sampling != next.sampling || first.summary != next.summary {
        return prefix("the request's tools, sampling or purpose changed between steps");
    }
    if next.messages.len() < first.messages.len()
        || next.messages[..first.messages.len()] != first.messages
    {
        return prefix("the next request did not extend the previous request byte-for-byte");
    }
    Ok(())
}

fn history(requests: &[ModelRequest], tag: &str) -> Vec<&ModelRequest> {
    requests
        .iter()
        .filter(|request| request_tag(request, &[tag]) == Some(tag) && !request.summary)
        .collect()
}

fn assert_history(requests: &[ModelRequest], tag: &str) -> std::result::Result<(), String> {
    let requests = history(requests, tag);
    for pair in requests.windows(2) {
        stable(pair[0], pair[1])?;
    }
    Ok(())
}

fn assert_reasoning(request: &ModelRequest, model: &str) -> std::result::Result<(), String> {
    let found = request.messages.iter().flat_map(|message| message.content.parts());
    for part in found {
        if let ModelContentPart::Reasoning { block } = part
            && block.continuity == b"exact reasoning"
            && block.origin.model == model
        {
            return Ok(());
        }
    }
    prefix("the continued request lost its byte-exact reasoning block")
}

fn has_file(request: &ModelRequest) -> bool {
    request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::Assistant)
        .flat_map(|message| message.content.parts())
        .any(|part| matches!(part, ModelContentPart::File { .. }))
}

fn d3_service(transition: &'static str, dialect: &'static str) -> ScriptedModelProvider {
    let failed = Mutex::new(false);
    let responder = move |request: &ModelRequest| {
        if request.summary {
            return Ok(match transition {
                "compaction_applied" | "background_summary" => manager_rewrite(request),
                "context_review" => review_decision(),
                _ => ScriptedTurn::Text("unexpected summary request".into()),
            });
        }
        let Some(tag) = request_tag(request, &["d3-parent", "d3-child"]) else {
            return Ok(ScriptedTurn::Text("untagged request".into()));
        };
        let step = assistant_since_prompt(request);
        let answer = match transition {
            "tool_result" => {
                if step == 0 {
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                            "d3-read",
                            "read",
                            json!({"path":"a.txt"}),
                        ))),
                        ScriptedEvent::Event(ModelEvent::Completed {
                            metadata: json!({"finish_reason":"tool_calls"}),
                        }),
                    ])
                } else {
                    ScriptedTurn::Text(done(tag))
                }
            }
            "parallel_results" => {
                if step == 0 {
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                            "d3-read-a",
                            "read",
                            json!({"path":"a.txt"}),
                        ))),
                        ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                            "d3-read-b",
                            "read",
                            json!({"path":"b.txt"}),
                        ))),
                        ScriptedEvent::Event(ModelEvent::Completed {
                            metadata: json!({"finish_reason":"tool_calls"}),
                        }),
                    ])
                } else {
                    ScriptedTurn::Text(done(tag))
                }
            }
            "child_notice_mid_turn" if tag == "d3-child" => ScriptedTurn::Text(done(tag)),
            "child_notice_mid_turn" => match step {
                0 => ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                        "d3-spawn",
                        "spawn",
                        json!({"task": tag("d3-child"), "name": "worker"}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: json!({"finish_reason":"tool_calls"}),
                    }),
                ]),
                1 => ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                        "d3-read",
                        "read",
                        json!({"path":"a.txt"}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: json!({"finish_reason":"tool_calls"}),
                    }),
                ]),
                _ => ScriptedTurn::Text(done(tag)),
            },
            "context_review" => match step {
                0 => tool_with_usage(),
                _ => text_with_usage(done(tag), 100),
            },
            "length_continuation" => {
                if step == 0 {
                    length_stop("reasoning_only", dialect)
                } else {
                    ScriptedTurn::Text(done(tag))
                }
            }
            "retry_after" => {
                let mut failed = failed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if !*failed {
                    *failed = true;
                    ScriptedTurn::Events(vec![ScriptedEvent::Refused {
                        status: 429,
                        error: "rate limited".into(),
                    }])
                } else {
                    ScriptedTurn::Text(done(tag))
                }
            }
            _ => ScriptedTurn::Text(done(tag)),
        };
        Ok(answer)
    };
    ScriptedModelProvider::responding(responder)
}

fn provider(model: &str) -> Model {
    Model::new(
        model.into(),
        format!("dst-{model}"),
        "1".into(),
    )
    .expect("valid dst model")
}

fn prefix_world(seed: u64) -> std::result::Result<bool, String> {
    let transition = transition(seed);
    let dialect = dialect(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let service = d3_service(transition, dialect);
            let model = provider(dialect);
            let files = [("a.txt", "alpha\n"), ("b.txt", "beta\n")];
            let mut world =
                World::new_with_model(seed, &files, service, model).map_err(oracle::failed)?;
            if transition == "reopen" {
                let first = vec![vec![("d3-parent", tag("d3-parent"))]];
                let opener = world
                    .incarnation(None, &first)
                    .await
                    .map_err(oracle::failed)?;
                if opener.prompts.first().map(|(outcome, _)| outcome)
                    != Some(&Prompted::Settled(done("d3-parent")))
                {
                    return prefix("the first incarnation did not settle");
                }
                let next = vec![vec![("d3-next", tag("d3-next"))]];
                let reopened = world
                    .incarnation(None, &next)
                    .await
                    .map_err(oracle::failed)?;
                let previous = opener.requests.last().ok_or("the first turn sent no request")?;
                let request = reopened.requests.first().ok_or("the reopen sent no request")?;
                stable(previous, request)?;
                return Ok(true);
            }
            let prompt = if transition == "compaction_applied" || transition == "background_summary"
            {
                format!("{} large context {}", tag("d3-parent"), "x".repeat(12_000))
            } else {
                tag("d3-parent")
            };
            let prompts = vec![vec![("d3-parent", prompt)]];
            let incarnation = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            if incarnation.prompts.first().map(|(outcome, _)| outcome)
                != Some(&Prompted::Settled(done("d3-parent")))
            {
                return prefix(&format!(
                    "{transition} did not settle its turn: {:?}",
                    incarnation.prompts
                ));
            }
            let summaries: Vec<_> = incarnation
                .requests
                .iter()
                .filter(|request| request.summary)
                .collect();
            match transition {
                "compaction_applied" => {
                    if summaries.is_empty() {
                        return prefix("compaction sent no summary request");
                    }
                    let compacted = incarnation
                        .requests
                        .iter()
                        .find(|request| !request.summary)
                        .ok_or("compaction dispatched no worker")?;
                    if !has_file(compacted) {
                        return prefix("the request after compaction does not carry its summary");
                    }
                }
                "background_summary" => {
                    if summaries.is_empty() {
                        return prefix("background summary was never dispatched");
                    }
                    let compacted = incarnation
                        .requests
                        .iter()
                        .find(|request| !request.summary)
                        .ok_or("background summary dispatched no worker")?;
                    let first = summaries[0]
                        .messages
                        .first()
                        .ok_or("the summary request has no header")?;
                    let header = compacted
                        .messages
                        .first()
                        .ok_or("the worker request has no header")?;
                    if first != header {
                        return prefix("the background summary did not reuse the agent header");
                    }
                }
                "context_review" => {
                    if summaries.is_empty() {
                        return prefix("context review was never dispatched");
                    }
                    let worker = incarnation
                        .requests
                        .iter()
                        .find(|request| {
                            !request.summary
                                && request_tag(request, &["d3-parent"]) == Some("d3-parent")
                        })
                        .ok_or("context review dispatched no worker")?;
                    let source = &summaries[0].messages;
                    let common = source.len().saturating_sub(1).min(worker.messages.len());
                    if source[..common] != worker.messages[..common] {
                        return prefix("the context review did not reuse the agent prefix");
                    }
                }
                "length_continuation" => {
                    assert_history(&incarnation.requests, "d3-parent")?;
                    let continued = incarnation
                        .requests
                        .iter()
                        .filter(|request| !request.summary)
                        .last()
                        .ok_or("length continuation sent no request")?;
                    assert_reasoning(continued, dialect)?;
                }
                "retry_after" => {
                    let requests = history(&incarnation.requests, "d3-parent");
                    if requests.len() < 2 || requests[0].messages != requests[1].messages {
                        return prefix("the Retry-After retry changed the request bytes");
                    }
                }
                _ => assert_history(&incarnation.requests, "d3-parent")?,
            }
            Ok(true)
        })
    })
}

struct LengthState {
    step: usize,
}

fn d4_service(call: &'static str, content: &'static str) -> ScriptedModelProvider {
    let state = Mutex::new(LengthState { step: 0 });
    let responder = move |request: &ModelRequest| {
        if request.summary {
            return Ok(length_stop(content, "dst"));
        }
        let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let answer = match call {
            "context_review" => match state.step {
                0 => {
                    state.step += 1;
                    tool_with_usage()
                }
                _ => {
                    state.step += 1;
                    text_with_usage(done("d4"), 100)
                }
            },
            "final_report" => match state.step {
                0 => {
                    state.step += 1;
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(ModelEvent::ToolCall(oracle::call(
                            "d4-read",
                            "read",
                            json!({"path":"a.txt"}),
                        ))),
                        ScriptedEvent::Event(ModelEvent::Completed {
                            metadata: json!({"finish_reason":"tool_calls"}),
                        }),
                    ])
                }
                1 => {
                    state.step += 1;
                    length_stop(content, "dst")
                }
                _ => {
                    state.step += 1;
                    ScriptedTurn::Text(done("d4"))
                }
            },
            _ => match state.step {
                0 => {
                    state.step += 1;
                    length_stop(content, "dst")
                }
                _ => {
                    state.step += 1;
                    ScriptedTurn::Text(done("d4"))
                }
            },
        };
        Ok(answer)
    };
    ScriptedModelProvider::responding(responder)
}

fn d4_capacity(capacity: &str) -> acyclic_harness::context::ModelContextCapacity {
    match capacity {
        "output_eq_context" => acyclic_harness::context::ModelContextCapacity {
            context_tokens: 4096,
            output_tokens: Some(4096),
            output_shares_context: true,
        },
        _ => acyclic_harness::context::ModelContextCapacity {
            context_tokens: 8192,
            output_tokens: Some(2048),
            output_shares_context: false,
        },
    }
}

fn contains_reasoning(request: &ModelRequest) -> bool {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .any(|part| matches!(part, ModelContentPart::Reasoning { .. }))
}

fn length_world(seed: u64) -> std::result::Result<bool, String> {
    let (_, call) = call(seed);
    let capacity = capacity(seed);
    let contents = CONTENTS;
    oracle::simulate(seed, move || {
        entry(async move {
            let files = [("a.txt", "alpha\n")];
            let mut requests = Vec::new();
            for content in contents {
                let service =
                    d4_service(call, content).with_context_capacity(d4_capacity(capacity));
                let mut world =
                    World::new(seed, &files, service).map_err(oracle::failed)?;
                let prompt = if call == "summary" {
                    format!("d4-content:{content} {} {}", tag("d4"), "x".repeat(12_000))
                } else {
                    format!("d4-content:{content} {}", tag("d4"))
                };
                let groups = vec![vec![("d4", prompt.clone())]];
                let incarnation = world
                    .incarnation(None, &groups)
                    .await
                    .map_err(oracle::failed)?;
                let summary_requests: Vec<_> = incarnation
                    .requests
                    .iter()
                    .filter(|request| request.summary)
                    .collect();
                match call {
                    "summary" => {
                        if summary_requests.is_empty() {
                            return prefix("a length-cut summary was never requested");
                        }
                        // The cut summary authorises no rewrite: no request may
                        // carry a summary file and the agent stays open.
                        if incarnation.requests.iter().any(has_file) {
                            return prefix("a length-cut summary published a rewrite");
                        }
                        let after = vec![vec![("d4-after", tag("d4-after"))]];
                        let reopened = world
                            .incarnation(None, &after)
                            .await
                            .map_err(oracle::failed)?;
                        if reopened.prompts.is_empty() {
                            return prefix("the worker died after its cut summary");
                        }
                    }
                    "context_review" => {
                        if summary_requests.is_empty() {
                            return prefix("the length-cut context review was never requested");
                        }
                        if incarnation.prompts.first().map(|(outcome, _)| outcome)
                            != Some(&Prompted::Settled(done("d4")))
                        {
                            return prefix("a length-cut context review ended the worker");
                        }
                        if incarnation.requests.iter().any(has_file) {
                            return prefix("a length-cut context review published a rewrite");
                        }
                    }
                    _ => {
                        if incarnation.prompts.first().map(|(outcome, _)| outcome)
                            != Some(&Prompted::Settled(done("d4")))
                        {
                            return prefix(&format!(
                                "{call} did not continue after a length stop: {:?}",
                                incarnation.prompts
                            ));
                        }
                        if content == "reasoning_only"
                            && !incarnation.requests.iter().any(contains_reasoning)
                        {
                            return prefix("reasoning did not round-trip into the continuation");
                        }
                        if content == "partial_tool_call"
                            && incarnation.requests.iter().any(|request| {
                                request.messages.iter().any(|message| {
                                    serde_json::to_string(&message.content)
                                        .unwrap_or_default()
                                        .contains("cut-call")
                                })
                            })
                        {
                            return prefix("a length-cut tool call was admitted");
                        }
                    }
                }
                requests.extend(incarnation.requests);
            }
            if requests.is_empty() {
                return prefix("the length world dispatched no request");
            }
            Ok(true)
        })
    })
}
