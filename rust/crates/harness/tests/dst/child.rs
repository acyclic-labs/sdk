//! Child start and idle: every successful spawn starts its child, a child
//! with prepared context issues its request or settles with an error, and a
//! stuck child is visible to its parent.
use super::*;
use acyclic_harness::model::{ModelRequest, ToolResultContent};
use family::{Generator, Variant};
use oracle::{Plan, World, call, done, service, settlements, tag};
use serde_json::json;

const PHASES: [&str; 5] = [
    "spawn_to_start",
    "after_context_prepared",
    "after_background_summary",
    "first_tool",
    "after_failure",
];
const FAULTS: [&str; 4] = [
    "none",
    "crash_actor_state",
    "undecided_failure",
    "shared_activation_shard",
];
const TOPOLOGIES: [&str; 3] = ["child", "grandchild", "batch_spawn"];

const ALL_SEEDS: &[u64] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

pub(super) const GENERATOR: Generator = Generator {
    class: "C4",
    seeds: ALL_SEEDS,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = child_none,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_4 => 4, seed_8 => 8],
}

acyclic_test_support::cases! {
    module = child_crash,
    values: u64,
    runner = super::run,
    cases = [seed_1 => 1, seed_5 => 5, seed_9 => 9],
}

acyclic_test_support::cases! {
    module = child_undecided,
    values: u64,
    runner = super::run,
    cases = [seed_2 => 2, seed_6 => 6, seed_10 => 10],
}

acyclic_test_support::cases! {
    module = child_shard,
    values: u64,
    runner = super::run,
    cases = [seed_3 => 3, seed_7 => 7, seed_11 => 11],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn fault(seed: u64) -> &'static str {
    FAULTS[usize::try_from(seed % 4).unwrap_or(0)]
}

fn topology(seed: u64) -> &'static str {
    TOPOLOGIES[usize::try_from((seed / 4) % 3).unwrap_or(0)]
}

fn variants(seed: u64) -> Vec<Variant> {
    let fault = fault(seed);
    let topology = topology(seed);
    PHASES
        .iter()
        .map(|phase| {
            Variant::from([
                ("phase", *phase),
                ("fault", fault),
                ("topology", topology),
            ])
        })
        .collect()
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

fn failed(tag_name: &'static str) -> Plan {
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

fn names(topology: &str) -> Vec<&'static str> {
    match topology {
        "grandchild" => vec!["g-start"],
        "batch_spawn" => vec!["b0", "b1"],
        _ => vec!["leaf-start", "leaf-tool", "leaf-fail"],
    }
}

fn root_steps(topology: &str) -> Vec<Vec<acyclic_harness::model::ToolCall>> {
    match topology {
        "grandchild" => vec![
            vec![spawn("c4-spawn-mid", "mid", "c4-mid")],
            vec![wait("c4-wait-mid", &["mid"])],
        ],
        "batch_spawn" => vec![
            vec![
                spawn("c4-spawn-b0", "b0", "c4-batch"),
                spawn("c4-spawn-b1", "b1", "c4-batch"),
            ],
            vec![wait("c4-wait-batch", &["b0", "b1"])],
        ],
        _ => vec![
            vec![
                spawn("c4-spawn-start", "leaf-start", "c4-start"),
                spawn("c4-spawn-tool", "leaf-tool", "c4-tool"),
                spawn("c4-spawn-fail", "leaf-fail", "c4-fail"),
            ],
            vec![wait(
                "c4-wait-leaf",
                &["leaf-start", "leaf-tool", "leaf-fail"],
            )],
        ],
    }
}

fn check_steps(topology: &str) -> Vec<Vec<acyclic_harness::model::ToolCall>> {
    vec![vec![wait("c4-check-wait", &names(topology))]]
}

fn plans(seed: u64) -> Vec<Plan> {
    let topology = topology(seed);
    let mut plans = vec![
        Plan {
            tag: "c4-root",
            steps: root_steps(topology),
            fail_once: None,
        },
        Plan {
            tag: "c4-check",
            steps: check_steps(topology),
            fail_once: None,
        },
        Plan {
            tag: "c4-mid",
            steps: vec![
                vec![spawn("c4-spawn-g-start", "g-start", "c4-g-start")],
                vec![wait("c4-wait-g", &["g-start"])],
            ],
            fail_once: None,
        },
        settled("c4-start"),
        tool("c4-tool"),
        failed("c4-fail"),
        settled("c4-batch"),
        settled("c4-g-start"),
        Plan {
            tag: "done",
            steps: Vec::new(),
            fail_once: None,
        },
    ];
    plans.push(settled("c4-batch"));
    plans
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

fn checked(seed: u64, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let fault = fault(seed);
    let topology = topology(seed);
    let results = wait_results(requests)?;
    let found = settled_names(&results);
    for name in names(topology) {
        if !found.iter().any(|settled| settled == name) {
            return Err(format!(
                "{fault}/{topology}: child {name} never started; settled {found:?}"
            ));
        }
    }
    if topology == "child" && fault != "crash_actor_state" {
        let tool_result = requests
            .iter()
            .flat_map(|request| settlements(request).unwrap_or_default())
            .any(|settled| settled.call.name == "read" && settled.succeeded());
        if !tool_result {
            return Err(format!("{fault}: the first tool never settled"));
        }
        let tool_error = requests
            .iter()
            .flat_map(|request| settlements(request).unwrap_or_default())
            .any(|settled| settled.call.name == "read" && !settled.succeeded());
        if !tool_error {
            return Err(format!("{fault}: the child's failure never reached its model"));
        }
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let fault = fault(seed);
    let topology = topology(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let files = [("marker.txt", "marker\n")];
            if fault == "crash_actor_state" {
                let crash = oracle::Crash {
                    seed,
                    files: &files,
                    plans: || plans(seed),
                    prompts: vec![vec![("root", tag("c4-root"))]],
                    window: (1, usize::MAX),
                    mode: oracle::crash_mode("crash_after", seed).ok_or("crash mode")?,
                };
                let (crashed, recovered) = crash
                    .recover(vec![vec![("check", tag("c4-check"))]])
                    .await?;
                oracle::all_settled(&recovered.requests)?;
                if !recovered
                    .prompts
                    .iter()
                    .any(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c4-check")))
                {
                    return Err(format!(
                        "after {crashed}: the reopened root did not finish: {:?}",
                        recovered.prompts
                    ));
                }
                checked(seed, &recovered.requests)
                    .map_err(|report| format!("after {crashed}: {report}"))?;
                return Ok(crashed.in_window);
            }
            let mut world = World::new(seed, &files, service(plans(seed)))
                .map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![("root", tag("c4-root"))]])
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&incarnation.requests)?;
            if !incarnation
                .prompts
                .iter()
                .any(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c4-root")))
            {
                return Err(format!("the root turn did not finish: {:?}", incarnation.prompts));
            }
            checked(seed, &incarnation.requests)?;
            Ok(true)
        })
    })
}
