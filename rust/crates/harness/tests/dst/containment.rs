//! Containment: a fault at one agent reaches only that agent, as a result its
//! model sees or a failed turn; the owner keeps accepting work.
use super::*;
use acyclic_harness::{
    model::{ModelRequest, ModelRole, ToolResultContent},
    test_support::ScriptedEvent,
};
use family::{Generator, Variant};
use oracle::{Plan, Prompted, World, call, done, service, settlements, tag};
use serde_json::{Value, json};

/// Every origin, in seed order.
const ORIGINS: [&str; 10] = [
    "tool_error",
    "rate_limit",
    "transport",
    "protective_bound",
    "internal_error",
    "custody_conflict",
    "malformed_output",
    "subscription_end",
    "child_settles_during_turn",
    "interaction_cancelled",
];

/// The three depths a fault can be injected at.
const LOCATIONS: [&str; 3] = ["root", "child", "grandchild"];

const ROOT_TAG: &str = "f1-root";
const MID_TAG: &str = "f1-mid";
const TARGET_TAG: &str = "f1-target";
const SETTLER_TAG: &str = "f1-settler";
const CHECK_TAG: &str = "f1-check";
const FILE_BYTES: u64 = 64 * 1024;

/// One world per (origin, location).
pub(super) const GENERATOR: Generator = Generator {
    class: "F1",
    seeds: a_fault_reaches_only_its_agent_and_the_owner_keeps_working::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = a_fault_reaches_only_its_agent_and_the_owner_keeps_working,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4,
        seed_5 => 5, seed_6 => 6, seed_7 => 7, seed_8 => 8, seed_9 => 9,
        seed_10 => 10, seed_11 => 11, seed_12 => 12, seed_13 => 13, seed_14 => 14,
        seed_15 => 15, seed_16 => 16, seed_17 => 17, seed_18 => 18, seed_19 => 19,
        seed_20 => 20, seed_21 => 21, seed_22 => 22, seed_23 => 23, seed_24 => 24,
        seed_25 => 25, seed_26 => 26, seed_27 => 27, seed_28 => 28, seed_29 => 29,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn origin(seed: u64) -> &'static str {
    ORIGINS
        .get(usize::try_from(seed % ORIGINS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("tool_error")
}

fn location(seed: u64) -> &'static str {
    LOCATIONS
        .get(usize::try_from((seed / ORIGINS.len() as u64) % LOCATIONS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("root")
}

fn variants(seed: u64) -> Vec<Variant> {
    vec![Variant::from([
        ("origin", origin(seed)),
        ("location", location(seed)),
    ])]
}

fn tool_level(origin: &str) -> bool {
    matches!(
        origin,
        "tool_error" | "protective_bound" | "custody_conflict" | "malformed_output"
    )
}

fn model_fault(origin: &str) -> Option<ScriptedEvent> {
    match origin {
        "rate_limit" => Some(ScriptedEvent::Refused {
            status: 429,
            error: "rate limited".into(),
        }),
        "transport" => Some(ScriptedEvent::Interrupted {
            error: "connection reset".into(),
        }),
        "internal_error" => Some(ScriptedEvent::Error {
            error: "provider internal error".into(),
        }),
        "subscription_end" => Some(ScriptedEvent::Error {
            error: "stream unavailable".into(),
        }),
        "interaction_cancelled" => Some(ScriptedEvent::Interrupted {
            error: "interaction cancelled".into(),
        }),
        _ => None,
    }
}

fn fault_plan(origin: &str, tag: &'static str) -> Plan {
    let (steps, fail_once) = match origin {
        "tool_error" => (
            vec![vec![call(
                &format!("{tag}-read"),
                "read",
                json!({"path": "missing.txt"}),
            )]],
            None,
        ),
        "malformed_output" => (
            vec![vec![
                call(&format!("{tag}-unknown"), "no_such_tool", json!({})),
                call(&format!("{tag}-invalid"), "write", json!({"bogus": 1})),
            ]],
            None,
        ),
        "protective_bound" => (
            vec![vec![call(
                &format!("{tag}-bound"),
                "write",
                json!({
                    "path": "bound.txt",
                    "content": "x".repeat((FILE_BYTES as usize) * 2),
                    "ifVersion": "absent",
                }),
            )]],
            None,
        ),
        "custody_conflict" => (
            vec![
                vec![call(
                    &format!("{tag}-conflict-1"),
                    "write",
                    json!({
                        "path": "conflict.txt",
                        "content": "one",
                        "ifVersion": "absent",
                    }),
                )],
                vec![call(
                    &format!("{tag}-conflict-2"),
                    "write",
                    json!({
                        "path": "conflict.txt",
                        "content": "two",
                        "ifVersion": "absent",
                    }),
                )],
            ],
            None,
        ),
        _ => (vec![Vec::new()], model_fault(origin)),
    };
    Plan {
        tag,
        steps,
        fail_once: fail_once.map(|event| (0, event)),
    }
}

fn spawn_call(id: &str, name: &str, brief: &'static str) -> acyclic_harness::model::ToolCall {
    call(
        id,
        "spawn",
        json!({"name": name, "task": tag(brief), "role": "worker"}),
    )
}

fn wait_call(id: &str, name: &str) -> acyclic_harness::model::ToolCall {
    call(id, "wait", json!({"agents": [name]}))
}

/// `tag` spawns a child briefed `child` and waits for it.
fn spawn_plan(tag: &'static str, child: &'static str) -> Plan {
    Plan {
        tag,
        steps: vec![
            vec![spawn_call(&format!("{tag}-spawn"), "target", child)],
            vec![wait_call(&format!("{tag}-wait"), "target")],
        ],
        fail_once: None,
    }
}

/// `tag` spawns a settler that faults, does ordinary work while it settles,
/// then waits for it. The settlement is the turn's mid-flight fault.
fn parent_plan(tag: &'static str) -> Plan {
    Plan {
        tag,
        steps: vec![
            vec![spawn_call(&format!("{tag}-spawn"), "settler", SETTLER_TAG)],
            vec![call(&format!("{tag}-work"), "read", json!({"path": "marker.txt"}))],
            vec![wait_call(&format!("{tag}-wait"), "settler")],
        ],
        fail_once: None,
    }
}

fn plans(origin: &str, location: &str) -> Vec<Plan> {
    let mut plans = Vec::new();
    match location {
        "root" => plans.push(if origin == "child_settles_during_turn" {
            parent_plan(ROOT_TAG)
        } else {
            fault_plan(origin, ROOT_TAG)
        }),
        "child" => {
            plans.push(spawn_plan(ROOT_TAG, TARGET_TAG));
            plans.push(if origin == "child_settles_during_turn" {
                parent_plan(TARGET_TAG)
            } else {
                fault_plan(origin, TARGET_TAG)
            });
        }
        _ => {
            plans.push(spawn_plan(ROOT_TAG, MID_TAG));
            plans.push(spawn_plan(MID_TAG, TARGET_TAG));
            plans.push(if origin == "child_settles_during_turn" {
                parent_plan(TARGET_TAG)
            } else {
                fault_plan(origin, TARGET_TAG)
            });
        }
    }
    if origin == "child_settles_during_turn" {
        plans.push(Plan {
            tag: SETTLER_TAG,
            steps: vec![Vec::new()],
            fail_once: Some((
                0,
                ScriptedEvent::Error {
                    error: "settling child failed".into(),
                },
            )),
        });
    }
    plans.push(Plan {
        tag: CHECK_TAG,
        steps: Vec::new(),
        fail_once: None,
    });
    plans
}

/// The faulted turn's own outcome. A tool-level fault reaches the model,
/// which finishes the turn. A model-level fault either fails the turn naming
/// its cause or, when the harness retries it, completes it with the plan's
/// answer; nothing else is a contained fault.
fn faulted(origin: &str, outcome: Option<&Prompted>) -> std::result::Result<(), String> {
    if tool_level(origin) {
        return match outcome {
            Some(Prompted::Settled(text)) if *text == done(ROOT_TAG) => Ok(()),
            other => Err(format!("{origin} ended its turn as {other:?}")),
        };
    }
    let cause = match origin {
        "rate_limit" => "rate limited",
        "transport" => "connection reset",
        "internal_error" => "provider internal error",
        "subscription_end" => "stream unavailable",
        "interaction_cancelled" => "interaction cancelled",
        _ => return Ok(()),
    };
    match outcome {
        Some(Prompted::Failed(message)) if message.contains(cause) => Ok(()),
        Some(Prompted::Settled(text)) if *text == done(ROOT_TAG) => Ok(()),
        other => Err(format!("{origin} ended its turn as {other:?}")),
    }
}

/// Whether `request` carries `brief` as its newest user input.
fn briefed(request: &ModelRequest, brief: &'static str) -> bool {
    let needles = oracle::appears(&tag(brief));
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == ModelRole::User)
        .and_then(|message| serde_json::to_string(&message.content).ok())
        .is_some_and(|body| needles.iter().any(|needle| body.contains(needle)))
}

/// Every child entry a wait result in the history returned.
fn wait_entries(requests: &[ModelRequest]) -> std::result::Result<Vec<Value>, String> {
    let mut entries = Vec::new();
    for request in requests {
        for settled in settlements(request)? {
            if settled.call.name != "wait" {
                continue;
            }
            let ToolResultContent::Json { value } = settled.result else {
                return Err(format!("wait returned {} not JSON", settled.rendered()));
            };
            if let Some(settled) = value.get("settled").and_then(Value::as_array) {
                entries.extend(settled.iter().cloned());
            }
        }
    }
    Ok(entries)
}

fn named<'a>(entries: &'a [Value], name: &str) -> Vec<&'a Value> {
    entries
        .iter()
        .filter(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
        .collect()
}

fn failed(entry: &Value) -> bool {
    entry.get("failed").is_some()
}

fn checked(
    origin: &str,
    location: &str,
    incarnation: &oracle::Incarnation,
) -> std::result::Result<(), String> {
    oracle::all_settled(&incarnation.requests)?;
    let outcomes: Vec<_> = incarnation
        .prompts
        .iter()
        .map(|(outcome, _)| outcome)
        .collect();
    if location == "root" && origin != "child_settles_during_turn" {
        faulted(origin, outcomes.first().copied())?;
    } else if outcomes.first() != Some(&&Prompted::Settled(done(ROOT_TAG))) {
        return Err(format!("{origin} at {location} reached the owner: {outcomes:?}"));
    }
    if outcomes.last() != Some(&&Prompted::Settled(done(CHECK_TAG))) {
        return Err(format!(
            "{origin} at {location} stopped the owner: {outcomes:?}"
        ));
    }

    let target_brief = if location == "root" {
        ROOT_TAG
    } else {
        TARGET_TAG
    };
    if !incarnation
        .requests
        .iter()
        .any(|request| briefed(request, target_brief))
    {
        return Err(format!("{origin} at {location} never reached its target"));
    }
    if origin == "child_settles_during_turn"
        && !incarnation
            .requests
            .iter()
            .any(|request| briefed(request, SETTLER_TAG))
    {
        return Err(format!(
            "{origin} at {location} never reached its settler"
        ));
    }

    let entries = wait_entries(&incarnation.requests)?;
    if origin == "child_settles_during_turn" {
        let settler = named(&entries, "settler");
        if settler.is_empty() || settler.iter().any(|entry| !failed(entry)) {
            return Err(format!(
                "{origin} at {location} did not contain the settler failure: {entries:?}"
            ));
        }
        for name in ["target", "mid"] {
            if named(&entries, name).iter().any(|entry| failed(entry)) {
                return Err(format!(
                    "{origin} at {location} reached {name}: {entries:?}"
                ));
            }
        }
    } else if location == "grandchild" {
        let middle = named(&entries, "mid");
        if middle.is_empty() || middle.iter().any(|entry| failed(entry)) {
            return Err(format!(
                "{origin} at {location} reached its parent: {entries:?}"
            ));
        }
    }

    if tool_level(origin) {
        let prefix = if location == "root" {
            format!("{ROOT_TAG}-")
        } else {
            format!("{TARGET_TAG}-")
        };
        let faulted: Vec<_> = incarnation
            .requests
            .iter()
            .flat_map(|request| settlements(request).unwrap_or_default())
            .filter(|settled| {
                settled.call.call_id.starts_with(&prefix)
                    && settled.call.name != "spawn"
                    && settled.call.name != "wait"
                    && !settled.succeeded()
            })
            .collect();
        if faulted.is_empty() {
            return Err(format!("{origin} at {location} never became a tool error"));
        }
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let origin = origin(seed);
    let location = location(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let mut world = World::new(
                seed,
                &[("marker.txt", "marker\n")],
                service(plans(origin, location)),
            )
            .map_err(oracle::failed)?;
            if origin == "protective_bound" {
                world = world.with_limits(acyclic_harness::conversation::Limits {
                    file_bytes: Some(FILE_BYTES),
                    ..Default::default()
                });
            }
            let prompts = [
                vec![("f1", tag(ROOT_TAG))],
                vec![("next", tag(CHECK_TAG))],
            ];
            let incarnation = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            checked(origin, location, &incarnation)?;
            Ok(true)
        })
    })
}
