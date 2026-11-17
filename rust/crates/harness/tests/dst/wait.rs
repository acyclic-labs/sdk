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

/// One world per variant: 4 forms x 8 states x 3 relations.
pub(super) const GENERATOR: Generator = Generator {
    class: "C3",
    seeds: every_wait_reports_the_state_it_returned_at::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_wait_reports_the_state_it_returned_at,
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
        seed_32 => 32, seed_33 => 33, seed_34 => 34, seed_35 => 35,
        seed_36 => 36, seed_37 => 37, seed_38 => 38, seed_39 => 39,
        seed_40 => 40, seed_41 => 41, seed_42 => 42, seed_43 => 43,
        seed_44 => 44, seed_45 => 45, seed_46 => 46, seed_47 => 47,
        seed_48 => 48, seed_49 => 49, seed_50 => 50, seed_51 => 51,
        seed_52 => 52, seed_53 => 53, seed_54 => 54, seed_55 => 55,
        seed_56 => 56, seed_57 => 57, seed_58 => 58, seed_59 => 59,
        seed_60 => 60, seed_61 => 61, seed_62 => 62, seed_63 => 63,
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
    RELATIONS[usize::try_from(seed / 32).unwrap_or(0)]
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
            ScriptedEvent::Error {
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

/// The child plan for `state`.
fn child_plan(tag_name: &'static str, state: &str) -> Plan {
    match state {
        "running" => parked(tag_name),
        "settled_failed" | "archived" | "stopped" => failed(tag_name),
        _ => settled(tag_name),
    }
}

/// The wait that observes `state` in `form`. A running or starting child is
/// observed through a settled sibling's quorum, so no timer is needed.
fn state_wait(form: &str, id: &str, state: &str, target: &str, sibling: &str) -> ToolCall {
    match (form, state) {
        ("no_agents", _) => input_wait(id),
        (_, "running" | "starting") => wait(id, &[sibling, target], Some(1), None),
        ("quorum_need", _) => wait(id, &[target], Some(1), None),
        ("timeout", _) => wait(id, &[target], None, Some(1)),
        _ => wait(id, &[target], None, None),
    }
}

fn needs_sibling(state: &str) -> bool {
    matches!(state, "running" | "starting")
}

fn plans(seed: u64) -> Vec<Plan> {
    let form = form(seed);
    let state = state(seed);
    let relation = relation(seed);
    let plans = vec![
        child_plan("c3-target", state),
        settled("c3-sibling"),
        settled("c3-wake"),
        Plan {
            tag: "c3-mid",
            steps: vec![
                vec![spawn("mid-spawn-target", "target", "c3-target")],
                vec![state_wait(form, "mid-wait", state, "target", "sibling")],
            ],
            fail_once: None,
        },
        Plan {
            tag: "c3-root",
            steps: root_steps(form, state, relation),
            fail_once: None,
        },
        Plan {
            tag: "c3-reopen",
            steps: vec![vec![state_wait(
                form,
                "reopen-wait",
                state,
                "target",
                "sibling",
            )]],
            fail_once: None,
        },
    ];
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
        "same_parent_other_runtime" => vec![vec![
            spawn("root-spawn-target", "target", "c3-target"),
            spawn("root-spawn-sibling", "sibling", "c3-sibling"),
        ]],
        _ => {
            let mut step = vec![spawn("root-spawn-target", "target", "c3-target")];
            if needs_sibling(state) {
                step.push(spawn("root-spawn-sibling", "sibling", "c3-sibling"));
            }
            vec![
                step,
                vec![state_wait(form, "root-wait", state, "target", "sibling")],
            ]
        }
    }
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

fn is_done(entry: &Value) -> bool {
    entry
        .get("phase")
        .and_then(|phase| phase.get("done"))
        .is_some()
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
        if reason(value) != "input" {
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
    match state {
        "starting" if relation != "same_parent_other_runtime" => {
            if let Some(entry) = field_entry(value, "unsettled", "target") {
                if entry.get("phase").and_then(Value::as_str) != Some("spawning") {
                    return Err(format!("wait {id} did not report spawning: {entry}"));
                }
            } else if field_entry(value, "settled", "target").is_none() {
                return Err(format!("wait {id} reported neither spawning nor settled: {value}"));
            }
        }
        "running" => {
            let entry = field_entry(value, "unsettled", "target")
                .ok_or_else(|| format!("wait {id} settled a running child: {value}"))?;
            if is_done(entry) {
                return Err(format!("wait {id} reported running target done: {entry}"));
            }
        }
        "settled_failed" | "archived" | "stopped" => {
            let entry = field_entry(value, "settled", "target")
                .ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_done(entry) || entry.get("failed").is_none() {
                return Err(format!("wait {id} hid target's failure: {entry}"));
            }
        }
        _ => {
            let entry = field_entry(value, "settled", "target")
                .ok_or_else(|| format!("wait {id} did not settle target: {value}"))?;
            if !is_done(entry) || entry.get("failed").is_some() {
                return Err(format!("wait {id} reported target wrong: {entry}"));
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
            let mut world = World::new(seed, &[("marker.txt", "marker\n")], service(plans(seed)))
                .map_err(oracle::failed)?;
            let mut prompts = vec![vec![("root", tag("c3-root"))]];
            if form == "no_agents" && relation != "grandchild" {
                prompts[0].push(("wake", tag("c3-wake")));
            }
            let first = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&first.requests)?;
            if !first
                .prompts
                .first()
                .is_some_and(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c3-root")))
            {
                return Err(format!("the root turn did not finish: {:?}", first.prompts));
            }
            if relation != "same_parent_other_runtime" {
                checked(seed, &first.requests)?;
                return Ok(true);
            }
            let mut recovery = vec![vec![("reopen", tag("c3-reopen"))]];
            if form == "no_agents" {
                recovery[0].push(("wake", tag("c3-wake")));
            }
            let reopened = world
                .incarnation(None, &recovery)
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&reopened.requests)?;
            if !reopened
                .prompts
                .first()
                .is_some_and(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c3-reopen")))
            {
                return Err(format!(
                    "the reopened root turn did not finish: {:?}",
                    reopened.prompts
                ));
            }
            checked(seed, &reopened.requests)?;
            Ok(true)
        })
    })
}
