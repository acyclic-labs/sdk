//! Containment: a fault at one agent reaches only that agent, as a result its
//! model sees or a failed turn; the owner keeps accepting work.
use super::*;
use acyclic_harness::{
    model::{ModelRequest, ToolResultContent},
    test_support::ScriptedEvent,
};
use family::{Generator, Variant};
use oracle::{Plan, Prompted, World, call, done, service, settlements, tag};
use serde_json::{Value, json};

const ROOT_TAG: &str = "f1-root";
const MID_TAG: &str = "f1-mid";
const TARGET_TAG: &str = "f1-target";
const SETTLER_TAG: &str = "f1-settler";
const CHECK_TAG: &str = "f1-check";

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
        seed_25 => 25, seed_26 => 26,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn origin(seed: u64) -> &'static str {
    match seed % 9 {
        0 => "tool_error",
        1 => "rate_limit",
        2 => "transport",
        3 => "protective_bound",
        4 => "internal_error",
        5 => "custody_conflict",
        6 => "malformed_output",
        7 => "subscription_end",
        _ => "child_settles_during_turn",
    }
}

fn location(seed: u64) -> &'static str {
    match (seed / 9) % 3 {
        0 => "root",
        1 => "child",
        _ => "grandchild",
    }
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
        "tool_error" | "custody_conflict" | "malformed_output"
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
        "protective_bound" => Some(ScriptedEvent::Error {
            error: "protective bound exceeded".into(),
        }),
        "internal_error" => Some(ScriptedEvent::Error {
            error: "provider internal error".into(),
        }),
        "subscription_end" => Some(ScriptedEvent::Error {
            error: "stream unavailable".into(),
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

/// `tag` spawns the named child briefed `child` and waits for it.
fn spawn_plan(tag: &'static str, name: &str, child: &'static str) -> Plan {
    Plan {
        tag,
        steps: vec![
            vec![spawn_call(&format!("{tag}-spawn"), name, child)],
            vec![wait_call(&format!("{tag}-wait"), name)],
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
            vec![call(
                &format!("{tag}-work"),
                "read",
                json!({"path": "missing.txt"}),
            )],
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
            plans.push(spawn_plan(ROOT_TAG, "target", TARGET_TAG));
            plans.push(if origin == "child_settles_during_turn" {
                parent_plan(TARGET_TAG)
            } else {
                fault_plan(origin, TARGET_TAG)
            });
        }
        _ => {
            plans.push(spawn_plan(ROOT_TAG, "mid", MID_TAG));
            plans.push(spawn_plan(MID_TAG, "target", TARGET_TAG));
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

/// The faulted turn's own outcome, for the agent tagged `tag`. A tool-level
/// fault reaches the model, and a failed child reaches its parent, which
/// finishes the turn. A model-level fault either fails the turn naming its
/// cause or, when the harness retries it, completes it with the plan's
/// answer; nothing else is a contained fault.
fn faulted(origin: &str, tag: &str, outcome: Option<&Prompted>) -> std::result::Result<(), String> {
    let cause = match origin {
        "rate_limit" => "rate limited",
        "transport" => "connection reset",
        "protective_bound" => "protective bound exceeded",
        "internal_error" => "provider internal error",
        "subscription_end" => "stream unavailable",
        _ => "",
    };
    match outcome {
        Some(Prompted::Failed(message)) if !cause.is_empty() && message.contains(cause) => Ok(()),
        Some(Prompted::Settled(text)) if *text == done(tag) => Ok(()),
        other => Err(format!("{origin} ended {tag}'s turn as {other:?}")),
    }
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

/// How a wait entry reports its child's turn ended.
fn reported(entry: &Value) -> Option<Prompted> {
    match entry.get("failed").and_then(Value::as_str) {
        Some(message) => Some(Prompted::Failed(message.into())),
        None => entry
            .get("answer")
            .and_then(Value::as_str)
            .map(|answer| Prompted::Settled(answer.into())),
    }
}

/// Every reported outcome of `name` passes `expected`, and there is one.
fn each_reported(
    entries: &[Value],
    name: &str,
    expected: impl Fn(Option<&Prompted>) -> std::result::Result<(), String>,
) -> std::result::Result<(), String> {
    let settled = named(entries, name);
    if settled.is_empty() {
        return Err(format!("{name} never settled: {entries:?}"));
    }
    settled
        .into_iter()
        .try_for_each(|entry| expected(reported(entry).as_ref()))
}

/// The settlements a faulted world's parents received: the target's settlement names
/// its own fault, a grandchild's middle finished its turn, and a settler's
/// failure reached only the target that spawned it.
fn child_outcomes(
    origin: &str,
    location: &str,
    entries: &[Value],
) -> std::result::Result<(), String> {
    if location != "root" {
        each_reported(entries, "target", |outcome| {
            faulted(origin, TARGET_TAG, outcome)
        })?;
    }
    if location == "grandchild" {
        each_reported(entries, "mid", |outcome| faulted("", MID_TAG, outcome))?;
    }
    if origin == "child_settles_during_turn" {
        each_reported(entries, "settler", |outcome| match outcome {
            Some(Prompted::Failed(message)) if message.contains("settling child failed") => Ok(()),
            other => Err(format!("settler ended its turn as {other:?}")),
        })?;
    }
    Ok(())
}

/// Checks the fault stayed at its target and the owner kept working.
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
    if location == "root" {
        faulted(origin, ROOT_TAG, outcomes.first().copied())?;
    } else if outcomes.first() != Some(&&Prompted::Settled(done(ROOT_TAG))) {
        return Err(format!(
            "{origin} at {location} reached the owner: {outcomes:?}"
        ));
    }
    if outcomes.last() != Some(&&Prompted::Settled(done(CHECK_TAG))) {
        return Err(format!(
            "{origin} at {location} stopped the owner: {outcomes:?}"
        ));
    }

    let entries = wait_entries(&incarnation.requests)?;
    child_outcomes(origin, location, &entries)
        .map_err(|error| format!("{origin} at {location}: {error}"))?;

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
            let mut world =
                World::new(seed, &[], service(plans(origin, location))).map_err(oracle::failed)?;
            let prompts = [vec![("f1", tag(ROOT_TAG))], vec![("next", tag(CHECK_TAG))]];
            let incarnation = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            checked(origin, location, &incarnation)?;
            Ok(true)
        })
    })
}

/// Negative control: a child settlement that names the wrong outcome fails
/// the oracle even though its parent continues.
#[test]
fn wrong_child_outcomes_fail_the_oracle() {
    let answered = |name: &str, tag: &str| json!({"name": name, "answer": done(tag)});
    let failed =
        |name: &str, message: &str| json!({"name": name, "answer": null, "failed": message});
    for (origin, location, entries) in [
        ("tool_error", "child", vec![answered("target", TARGET_TAG)]),
        (
            "transport",
            "child",
            vec![failed("target", "connection reset")],
        ),
        (
            "rate_limit",
            "grandchild",
            vec![answered("target", TARGET_TAG), answered("mid", MID_TAG)],
        ),
    ] {
        assert_eq!(child_outcomes(origin, location, &entries), Ok(()));
    }
    for (origin, location, entries) in [
        // A tool error ended the child's turn instead of reaching its model.
        ("tool_error", "child", vec![failed("target", "read failed")]),
        // A model fault reported under an unrelated cause.
        (
            "transport",
            "child",
            vec![failed("target", "unrelated failure")],
        ),
        // The child settled with another agent's answer.
        ("rate_limit", "child", vec![answered("target", MID_TAG)]),
        // The fault escaped to the middle agent.
        (
            "transport",
            "grandchild",
            vec![
                failed("target", "connection reset"),
                failed("mid", "connection reset"),
            ],
        ),
        // The target never settled.
        ("transport", "child", Vec::new()),
    ] {
        assert!(
            child_outcomes(origin, location, &entries).is_err(),
            "{origin} at {location} accepted {entries:?}"
        );
    }
}
