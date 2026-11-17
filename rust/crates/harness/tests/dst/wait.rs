//! Wait report fidelity: `wait` returns only when its need is met, its
//! timeout fires, or with a named reason; every settled child it reports has
//! the phase, outcome and answer that child actually reached.
use super::*;
use acyclic_harness::{
    model::{ModelRequest, ToolCall, ToolResultContent},
    test_support::ScriptedEvent,
};
use family::{Generator, Variant};
use oracle::{Plan, World, call, done, service, settlements, tag};
use serde_json::{Value, json};

const FORMS: [&str; 4] = ["no_agents", "named", "quorum_need", "timeout"];
const STATES: [&str; 8] = [
    "starting",
    "running",
    "idle",
    "settled_ok",
    "settled_failed",
    "merged",
    "archived",
    "stopped",
];
const RELATIONS: [&str; 3] = ["child", "grandchild", "same_parent_other_runtime"];

const ALL_SEEDS: &[u64] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73,
    74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
];

pub(super) const GENERATOR: Generator = Generator {
    class: "C3",
    seeds: ALL_SEEDS,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = wait_child,
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

acyclic_test_support::cases! {
    module = wait_grandchild,
    values: u64,
    runner = super::run,
    cases = [
        seed_32 => 32, seed_33 => 33, seed_34 => 34, seed_35 => 35,
        seed_36 => 36, seed_37 => 37, seed_38 => 38, seed_39 => 39,
        seed_40 => 40, seed_41 => 41, seed_42 => 42, seed_43 => 43,
        seed_44 => 44, seed_45 => 45, seed_46 => 46, seed_47 => 47,
        seed_48 => 48, seed_49 => 49, seed_50 => 50, seed_51 => 51,
        seed_52 => 52, seed_53 => 53, seed_54 => 54, seed_55 => 55,
        seed_56 => 56, seed_57 => 57, seed_58 => 58, seed_59 => 59,
        seed_60 => 60, seed_61 => 61, seed_62 => 62, seed_63 => 63,
    ],
}

acyclic_test_support::cases! {
    module = wait_same_parent,
    values: u64,
    runner = super::run,
    cases = [
        seed_64 => 64, seed_65 => 65, seed_66 => 66, seed_67 => 67,
        seed_68 => 68, seed_69 => 69, seed_70 => 70, seed_71 => 71,
        seed_72 => 72, seed_73 => 73, seed_74 => 74, seed_75 => 75,
        seed_76 => 76, seed_77 => 77, seed_78 => 78, seed_79 => 79,
        seed_80 => 80, seed_81 => 81, seed_82 => 82, seed_83 => 83,
        seed_84 => 84, seed_85 => 85, seed_86 => 86, seed_87 => 87,
        seed_88 => 88, seed_89 => 89, seed_90 => 90, seed_91 => 91,
        seed_92 => 92, seed_93 => 93, seed_94 => 94, seed_95 => 95,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn form(seed: u64) -> &'static str {
    FORMS[usize::try_from(seed % 4).unwrap_or(0)]
}

fn state(seed: u64) -> &'static str {
    STATES[usize::try_from((seed / 4) % 8).unwrap_or(0)]
}

fn relation(seed: u64) -> &'static str {
    RELATIONS[usize::try_from((seed / 32) % 3).unwrap_or(0)]
}

fn variants(seed: u64) -> Vec<Variant> {
    vec![Variant::from([
        ("child_state", state(seed)),
        ("form", form(seed)),
        ("relation", relation(seed)),
    ])]
}

fn settled(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: Vec::new(),
        fail_once: None,
    }
}

fn failed(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: Vec::new(),
        fail_once: Some((
            0,
            ScriptedEvent::Interrupted {
                error: "child turn failed".into(),
            },
        )),
    }
}

fn parked(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: vec![vec![call(
            &format!("{tag_name}-park"),
            "wait",
            json!({"input": true, "timeout_ms": 100000}),
        )]],
        fail_once: None,
    }
}

fn writer(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: vec![vec![call(
            &format!("{tag_name}-write"),
            "write",
            json!({"path": "merged.txt", "content": "merged\n", "ifVersion": "absent"}),
        )]],
        fail_once: None,
    }
}

fn spawn(id: &str, name: &str, brief: &'static str) -> ToolCall {
    call(
        id,
        "spawn",
        json!({"name": name, "task": tag(brief), "role": "worker"}),
    )
}

fn wait(id: &str, names: &[&str], need: Option<u32>, timeout: Option<u64>) -> ToolCall {
    let mut value = json!({"agents": names});
    if let Some(need) = need {
        value["need"] = json!(need);
    }
    if let Some(timeout) = timeout {
        value["timeout_ms"] = json!(timeout);
    }
    call(id, "wait", value)
}

fn input_wait(id: &str) -> ToolCall {
    call(id, "wait", json!({"input": true}))
}

fn release(id: &str, target: &str) -> ToolCall {
    call(id, "send", json!({"agent": target, "message": "release"}))
}

fn merge(id: &str, target: &str) -> ToolCall {
    call(
        id,
        "fs",
        json!({"command": format!("merge agents/{target}")}),
    )
}

fn archive(id: &str, target: &str) -> ToolCall {
    call(id, "archive", json!({"agent": target}))
}

fn tool(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: vec![vec![call(
            &format!("{tag_name}-read"),
            "read",
            json!({"path": "marker.txt"}),
        )]],
        fail_once: None,
    }
}

fn child_plan(tag_name: &'static str, state: &str, form: &str) -> Plan {
    match (state, form) {
        ("running", "named") => tool(tag_name),
        ("running", _) | ("archived", _) | ("stopped", _) => parked(tag_name),
        ("settled_failed", _) => failed(tag_name),
        ("merged", _) => writer(tag_name),
        _ => settled(tag_name),
    }
}

fn needs_sibling(form: &str) -> bool {
    form == "quorum_need"
}

/// The wait in the requested form. A running child is released first for the
/// named form; the quorum form observes it through a settled sibling, and the
/// timeout form returns on its deadline.
fn form_wait(form: &str, id: &str, target: &str) -> ToolCall {
    match form {
        "no_agents" => call(id, "wait", json!({"timeout_ms": 1})),
        "quorum_need" => wait(id, &[target, "sibling"], Some(1), None),
        "timeout" => wait(id, &[target], None, Some(1)),
        _ => wait(id, &[target], None, None),
    }
}

/// The steps after the target and its optional sibling are spawned.
fn observe_steps(form: &str, state: &str, prefix: &str) -> Vec<Vec<ToolCall>> {
    let wait_id = format!("{prefix}-wait");
    match state {
        "merged" => vec![
            vec![wait(&format!("{prefix}-settle"), &["target"], None, None)],
            vec![merge(&format!("{prefix}-merge"), "target")],
            vec![form_wait(form, &wait_id, "target")],
        ],
        "archived" | "stopped" => vec![
            vec![archive(&format!("{prefix}-archive"), "target")],
            vec![form_wait(form, &wait_id, "target")],
        ],
        _ => vec![vec![form_wait(form, &wait_id, "target")]],
    }
}

fn spawn_steps(form: &str, state: &str, prefix: &str) -> Vec<ToolCall> {
    let mut step = vec![spawn(
        &format!("{prefix}-spawn-target"),
        "target",
        "c3-target",
    )];
    if needs_sibling(form) {
        step.push(spawn(
            &format!("{prefix}-spawn-sibling"),
            "sibling",
            "c3-sibling",
        ));
    }
    step
}

fn plans(seed: u64) -> Vec<Plan> {
    let form = form(seed);
    let state = state(seed);
    let relation = relation(seed);
    let mut plans = vec![
        child_plan("c3-target", state, form),
        settled("c3-sibling"),
        Plan {
            tag: "c3-mid",
            steps: {
                let mut steps = vec![spawn_steps(form, state, "mid")];
                steps.extend(observe_steps(form, state, "mid"));
                steps
            },
            fail_once: None,
        },
        Plan {
            tag: "c3-root",
            steps: root_steps(form, state, relation),
            fail_once: None,
        },
        Plan {
            tag: "c3-reopen",
            steps: reopen_steps(form, state),
            fail_once: None,
        },
        Plan {
            tag: "c3-reopen-spawn",
            steps: {
                let mut steps = vec![spawn_steps(form, state, "reopen")];
                if relation == "same_parent_other_runtime" && state != "running" {
                    steps.extend(observe_steps(form, state, "reopen"));
                }
                steps.push(vec![call(
                    "reopen-read",
                    "read",
                    json!({"path": "marker.txt"}),
                )]);
                steps
            },
            fail_once: None,
        },
    ];
    plans.push(settled("c3-sibling"));
    plans
}

fn root_steps(form: &str, state: &str, relation: &str) -> Vec<Vec<ToolCall>> {
    match relation {
        "grandchild" => vec![
            vec![spawn("root-spawn-mid", "mid", "c3-mid")],
            vec![call(
                "root-send-mid",
                "send",
                json!({"agent": "mid", "message": "wake"}),
            )],
            vec![wait("root-join-mid", &["mid"], None, None)],
        ],
        "same_parent_other_runtime" => vec![spawn_steps(form, state, "root")],
        _ => {
            let mut steps = vec![spawn_steps(form, state, "root")];
            steps.extend(observe_steps(form, state, "root"));
            steps
        }
    }
}

fn reopen_steps(form: &str, state: &str) -> Vec<Vec<ToolCall>> {
    vec![vec![form_wait(form, "reopen-wait", "target")]]
}

fn wait_results(requests: &[ModelRequest]) -> std::result::Result<Vec<(String, Value)>, String> {
    let mut results = Vec::new();
    for request in requests {
        for settled in settlements(request)? {
            if settled.call.name != "wait" {
                continue;
            }
            let ToolResultContent::Json { value } = settled.result else {
                return Err(format!("wait returned {} not JSON", settled.rendered()));
            };
            results.push((settled.call.call_id.clone(), value));
        }
    }
    Ok(results)
}

fn result<'a>(results: &'a [(String, Value)], id: &str) -> std::result::Result<&'a Value, String> {
    results
        .iter()
        .find(|(call, _)| call == id)
        .map(|(_, value)| value)
        .ok_or_else(|| {
            format!(
                "wait {id} never returned: {:?}",
                results.iter().map(|(id, _)| id).collect::<Vec<_>>()
            )
        })
}

fn reason(value: &Value) -> &str {
    match value.get("reason") {
        Some(Value::String(reason)) => reason,
        Some(reason) => reason.get("kind").and_then(Value::as_str).unwrap_or(""),
        None => "",
    }
}

fn field_entry<'a>(value: &'a Value, field: &str, name: &str) -> Option<&'a Value> {
    value
        .get(field)?
        .as_array()?
        .iter()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
}

fn is_settled(entry: &Value) -> bool {
    match entry.get("phase") {
        Some(Value::String(phase)) => phase == "idle",
        Some(phase) => phase.get("done").is_some(),
        None => false,
    }
}

fn outcome(entry: &Value) -> Option<&str> {
    entry.get("phase")?.get("done")?.get("outcome")?.as_str()
}

fn checked(seed: u64, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let form = form(seed);
    let state = state(seed);
    let relation = relation(seed);
    let results = wait_results(requests)?;
    let id = match relation {
        "grandchild" => "mid-wait",
        "same_parent_other_runtime" => "reopen-wait",
        _ => "root-wait",
    };
    let value = result(&results, id)?;
    if reason(value).is_empty() {
        return Err(format!("wait {id} returned no reason: {value}"));
    }
    if form == "no_agents" {
        if !matches!(reason(value), "input" | "deadline") {
            return Err(format!("no-agent wait returned {}: {value}", reason(value)));
        }
        if !value
            .get("unsettled")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return Err(format!("no-agent wait reported children: {value}"));
        }
        return Ok(());
    }
    let settled = field_entry(value, "settled", "target");
    let unsettled = field_entry(value, "unsettled", "target");
    match state {
        "starting" => {
            if let Some(entry) = unsettled {
                if entry.get("phase").and_then(Value::as_str) != Some("spawning") {
                    return Err(format!("wait {id} did not report spawning: {entry}"));
                }
            } else if settled.is_none() {
                return Err(format!(
                    "wait {id} reported neither spawning nor settled: {value}"
                ));
            }
        }
        "running" => match form {
            "timeout" | "quorum_need" => {
                let entry = unsettled
                    .ok_or_else(|| format!("wait {id} settled a running child: {value}"))?;
                if is_settled(entry) {
                    return Err(format!("wait {id} reported running target done: {entry}"));
                }
            }
            _ => {
                let entry = settled
                    .ok_or_else(|| format!("wait {id} did not settle released target: {value}"))?;
                if !is_settled(entry) {
                    return Err(format!("wait {id} reported target not settled: {entry}"));
                }
            }
        },
        "settled_failed" => {
            let entry =
                settled.ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_settled(entry) || entry.get("failed").is_none() {
                return Err(format!("wait {id} hid target's failure: {entry}"));
            }
        }
        "merged" => {
            let entry =
                settled.ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_settled(entry) || entry.get("failed").is_some() {
                return Err(format!("wait {id} reported target wrong: {entry}"));
            }
            let merged = requests
                .iter()
                .flat_map(|request| settlements(request).unwrap_or_default())
                .any(|settled| {
                    settled.call.name == "fs"
                        && settled.succeeded()
                        && settled
                            .call
                            .arguments
                            .get("command")
                            .and_then(Value::as_str)
                            .is_some_and(|command| command.starts_with("merge "))
                });
            if !merged {
                return Err(format!("wait {id} never merged the target"));
            }
        }
        "archived" | "stopped" => {
            let entry =
                settled.ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_settled(entry) || outcome(entry) != Some("archived") {
                return Err(format!(
                    "wait {id} did not report an archived target: {entry}"
                ));
            }
        }
        _ => {
            let entry =
                settled.ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_settled(entry) || entry.get("failed").is_some() {
                return Err(format!("wait {id} reported target wrong: {entry}"));
            }
            if entry.get("answer").and_then(Value::as_str) != Some(done("c3-target").as_str()) {
                return Err(format!("wait {id} reported the wrong answer: {entry}"));
            }
        }
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let form = form(seed);
    let relation = relation(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            if relation == "same_parent_other_runtime" {
                let mut world =
                    World::new(seed, &[("marker.txt", "marker\n")], service(plans(seed)))
                        .map_err(oracle::failed)?
                        .completing();
                let spawn_prompt = vec![("root", tag("c3-reopen-spawn"))];
                let spawned = world
                    .incarnation(None, &[spawn_prompt])
                    .await
                    .map_err(oracle::failed)?;
                oracle::all_settled(&spawned.requests)?;
                if !spawned.prompts.iter().any(|(outcome, _)| {
                    *outcome == oracle::Prompted::Settled(done("c3-reopen-spawn"))
                }) {
                    return Err(format!(
                        "the spawn turn did not finish: {:?}",
                        spawned.prompts
                    ));
                }
                let mut recovery = vec![vec![("reopen", tag("c3-reopen"))]];
                if form == "no_agents" {
                    recovery[0].push(("wake", "wake".to_owned()));
                }
                let reopened = world
                    .incarnation(None, &recovery)
                    .await
                    .map_err(oracle::failed)?;
                oracle::all_settled(&reopened.requests)?;
                if !reopened
                    .prompts
                    .iter()
                    .any(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c3-reopen")))
                {
                    return Err(format!(
                        "the reopened root turn did not finish: {:?}",
                        reopened.prompts
                    ));
                }
                checked(seed, &reopened.requests)?;
                return Ok(true);
            }
            let mut world = World::new(seed, &[("marker.txt", "marker\n")], service(plans(seed)))
                .map_err(oracle::failed)?;
            let mut prompts = vec![vec![("root", tag("c3-root"))]];
            if form == "no_agents" && relation != "grandchild" {
                prompts[0].push(("wake", "wake".to_owned()));
            }
            let first = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&first.requests)?;
            if !first
                .prompts
                .iter()
                .any(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c3-root")))
            {
                return Err(format!("the root turn did not finish: {:?}", first.prompts));
            }
            checked(seed, &first.requests)?;
            Ok(true)
        })
    })
}
