//! Child start and idle: every successful spawn starts its child, a child
//! with prepared context issues its request or settles with an error, and a
//! stuck child is visible to its parent.
use super::*;
use acyclic_harness::{
    model::{ModelRequest, ToolResultContent},
    test_support::ScriptedEvent,
};
use family::{Generator, Variant};
use oracle::{Plan, World, call, done, service, settlements, tag};
use serde_json::json;

/// The points a child can stall at.
const PHASES: [&str; 5] = [
    "spawn_to_start",
    "after_context_prepared",
    "after_background_summary",
    "first_tool",
    "after_failure",
];
/// The faults that can hide a stall.
const FAULTS: [&str; 4] = [
    "none",
    "crash_actor_state",
    "undecided_failure",
    "shared_activation_shard",
];
/// The spawn topology.
const TOPOLOGIES: [&str; 3] = ["child", "grandchild", "batch_spawn"];

/// One world per fault, each covering every phase and topology.
pub(super) const GENERATOR: Generator = Generator {
    class: "C4",
    seeds: every_spawned_child_starts_and_every_prepared_context_requests::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_spawned_child_starts_and_every_prepared_context_requests,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn fault(seed: u64) -> &'static str {
    FAULTS
        .get(usize::try_from(seed % FAULTS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("none")
}

fn variants(seed: u64) -> Vec<Variant> {
    let fault = fault(seed);
    let mut variants = Vec::new();
    for phase in PHASES {
        for topology in TOPOLOGIES {
            variants.push(Variant::from([
                ("phase", phase),
                ("fault", fault),
                ("topology", topology),
            ]));
        }
    }
    variants
}

fn spawn(id: &str, name: &str, brief: &'static str) -> acyclic_harness::model::ToolCall {
    call(
        id,
        "spawn",
        json!({"name": name, "task": tag(brief), "role": "worker"}),
    )
}

fn wait(id: &str, names: &[&str]) -> acyclic_harness::model::ToolCall {
    call(id, "wait", json!({"agents": names}))
}

fn settled(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: Vec::new(),
        fail_once: None,
    }
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

fn failed(tag_name: &'static str, undecided: bool) -> Plan {
    if undecided {
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
    } else {
        Plan {
            tag: tag_name,
            steps: vec![vec![call(
                &format!("{tag_name}-missing"),
                "read",
                json!({"path": "missing.txt"}),
            )]],
            fail_once: None,
        }
    }
}

const BATCH: usize = 8;

fn batch_names() -> Vec<String> {
    (0..BATCH).map(|index| format!("b{index}")).collect()
}

fn root_plan(_fault: &str) -> Plan {
    let mut spawns = vec![
        spawn("c4-spawn-start", "leaf-start", "c4-start"),
        spawn("c4-spawn-tool", "leaf-tool", "c4-tool"),
        spawn("c4-spawn-fail", "leaf-fail", "c4-fail"),
        spawn("c4-spawn-mid", "mid", "c4-mid"),
    ];
    for name in batch_names() {
        spawns.push(spawn(&format!("c4-spawn-{name}"), &name, "c4-batch"));
    }
    let mut waits = vec![wait(
        "c4-wait-leaf",
        &["leaf-start", "leaf-tool", "leaf-fail"],
    )];
    waits.push(wait("c4-wait-mid", &["mid"]));
    let batch_names = batch_names();
    let batch: Vec<&str> = batch_names.iter().map(String::as_str).collect();
    waits.push(wait("c4-wait-batch", &batch));
    Plan {
        tag: "c4-root",
        steps: vec![spawns, waits],
        fail_once: None,
    }
}

fn mid_plan() -> Plan {
    Plan {
        tag: "c4-mid",
        steps: vec![
            vec![
                spawn("c4-spawn-g-start", "g-start", "c4-g-start"),
                spawn("c4-spawn-g-tool", "g-tool", "c4-g-tool"),
                spawn("c4-spawn-g-fail", "g-fail", "c4-g-fail"),
            ],
            vec![wait(
                "c4-wait-g",
                &["g-start", "g-tool", "g-fail"],
            )],
        ],
        fail_once: None,
    }
}

fn check_plan() -> Plan {
    let mut names = vec!["leaf-start", "leaf-tool", "leaf-fail", "mid"];
    let batch = batch_names();
    names.extend(batch.iter().map(String::as_str));
    Plan {
        tag: "c4-check",
        steps: vec![vec![wait("c4-check-wait", &names)]],
        fail_once: None,
    }
}

fn plans(fault: &str) -> Vec<Plan> {
    let undecided = fault == "undecided_failure";
    vec![
        root_plan(fault),
        mid_plan(),
        check_plan(),
        settled("c4-start"),
        tool("c4-tool"),
        failed("c4-fail", undecided),
        settled("c4-batch"),
        settled("c4-g-start"),
        tool("c4-g-tool"),
        failed("c4-g-fail", undecided),
        Plan {
            tag: "done",
            steps: Vec::new(),
            fail_once: None,
        },
    ]
}

fn wait_results(
    requests: &[ModelRequest],
) -> std::result::Result<Vec<(String, serde_json::Value)>, String> {
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

fn settled_names(results: &[(String, serde_json::Value)]) -> Vec<String> {
    results
        .iter()
        .flat_map(|(_, value)| {
            value
                .get("settled")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|entry| entry.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn checked(fault: &str, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let results = wait_results(requests)?;
    let names = settled_names(&results);
    // Every spawned child starts and settles: the parent's wait names it.
    let mut expected = vec![
        "leaf-start".to_owned(),
        "leaf-tool".to_owned(),
        "leaf-fail".to_owned(),
        "g-start".to_owned(),
        "g-tool".to_owned(),
        "g-fail".to_owned(),
    ];
    expected.extend(batch_names());
    for name in expected {
        if !names.iter().any(|found| found == &name) {
            return Err(format!(
                "{fault}: child {name} never started; settled {names:?}"
            ));
        }
    }
    // The first tool ran and its result reached the child's model.
    let tool_result = requests
        .iter()
        .flat_map(|request| settlements(request).unwrap_or_default())
        .any(|settled| settled.call.name == "read" && settled.succeeded());
    if !tool_result {
        return Err(format!("{fault}: the first tool never settled"));
    }
    // A failed child is visible to its parent: either the turn failed and the
    // wait reports it, or the tool error reached the child's model.
    let failed_visible = results.iter().any(|(_, value)| {
        value
            .get("settled")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry.get("name").and_then(serde_json::Value::as_str) == Some("leaf-fail")
                        && entry.get("failed").is_some()
                })
            })
    });
    let tool_error = requests
        .iter()
        .flat_map(|request| settlements(request).unwrap_or_default())
        .any(|settled| settled.call.name == "read" && !settled.succeeded());
    if fault == "undecided_failure" {
        if !failed_visible {
            return Err(format!(
                "{fault}: the parent never saw the failed child: {results:?}"
            ));
        }
    } else if !tool_error {
        return Err(format!("{fault}: the child's failure never reached its model"));
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let fault = fault(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let files = [("marker.txt", "marker\n")];
            if fault == "crash_actor_state" {
                let crash = oracle::Crash {
                    seed,
                    files: &files,
                    plans: || plans(fault),
                    prompts: vec![vec![("root", tag("c4-root"))]],
                    // The spawn step commits before the crash; recovery must
                    // resume every child it started.
                    window: (1, 2),
                    mode: oracle::crash_mode("crash_after", seed).ok_or("crash mode")?,
                };
                let (crashed, recovered) = crash
                    .recover(vec![vec![("check", tag("c4-check"))]])
                    .await?;
                oracle::all_settled(&recovered.requests)?;
                if !recovered
                    .prompts
                    .first()
                    .is_some_and(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c4-check")))
                {
                    return Err(format!(
                        "after {crashed}: the reopened root did not finish: {:?}",
                        recovered.prompts
                    ));
                }
                checked(fault, &recovered.requests)
                    .map_err(|report| format!("after {crashed}: {report}"))?;
                return Ok(crashed.in_window);
            }
            let mut world = World::new(seed, &files, service(plans(fault)))
                .map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![("root", tag("c4-root"))]])
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&incarnation.requests)?;
            if !incarnation
                .prompts
                .first()
                .is_some_and(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c4-root")))
            {
                return Err(format!("the root turn did not finish: {:?}", incarnation.prompts));
            }
            checked(fault, &incarnation.requests)?;
            Ok(true)
        })
    })
}
