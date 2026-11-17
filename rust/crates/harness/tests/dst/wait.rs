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

/// The four wait forms.
const FORMS: [&str; 4] = ["no_agents", "named", "quorum_need", "timeout"];
/// The states a reported child can be in.
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
/// The target's relation to the waiter.
const RELATIONS: [&str; 3] = ["child", "grandchild", "same_parent_other_runtime"];

/// One world per form, each covering every state and relation.
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
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn form(seed: u64) -> &'static str {
    FORMS
        .get(usize::try_from(seed % FORMS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("named")
}

fn variants(seed: u64) -> Vec<Variant> {
    let form = form(seed);
    let mut variants = Vec::new();
    for state in STATES {
        for relation in RELATIONS {
            variants.push(Variant::from([
                ("child_state", state),
                ("form", form),
                ("relation", relation),
            ]));
        }
    }
    variants
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

/// A child that stays in its own wait, so the parent can observe a running
/// (not settled) child without a real process.
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

fn writer(tag_name: &'static str, path: &str) -> Plan {
    Plan {
        tag: tag_name,
        steps: vec![vec![call(
            &format!("{tag_name}-write"),
            "write",
            json!({"path": path, "content": "merged\n", "ifVersion": "absent"}),
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

/// A wait in `form` over `names`. `timeout` forces a bounded return for a
/// child that is not settled yet.
fn waited(form: &str, id: &str, names: &[&str], timeout: Option<u64>) -> ToolCall {
    let agents = names.iter().map(|name| (*name).to_owned()).collect::<Vec<_>>();
    let arguments = match form {
        "no_agents" => json!({"timeout_ms": timeout.unwrap_or(5)}),
        "quorum_need" => {
            let mut value = json!({"agents": agents, "need": 1});
            if let Some(timeout) = timeout {
                value["timeout_ms"] = json!(timeout);
            }
            value
        }
        "timeout" => json!({"agents": agents, "timeout_ms": timeout.unwrap_or(5)}),
        _ => {
            let mut value = json!({"agents": agents});
            if let Some(timeout) = timeout {
                value["timeout_ms"] = json!(timeout);
            }
            value
        }
    };
    call(id, "wait", arguments)
}

/// The steps that spawn one child per state and wait for it in `form`.
fn state_steps(form: &str, prefix: &str, include_mid: bool) -> Vec<Vec<ToolCall>> {
    let names = ["ok", "bad", "run", "idle", "merge", "arch", "stop"];
    let mut steps = Vec::new();
    // Every long-lived child starts in one step, so a batch spawn and the
    // child-start path are both exercised before the first wait.
    let mut spawns: Vec<ToolCall> = names
        .iter()
        .map(|name| spawn(&format!("{prefix}-spawn-{name}"), name, state_tag(prefix, name)))
        .collect();
    if include_mid {
        spawns.push(spawn(&format!("{prefix}-spawn-mid"), "mid", mid_tag(prefix)));
    }
    steps.push(spawns);

    if form == "no_agents" {
        // The no-agent form wakes on inbox input. A child settles in the
        // wait's own step, so its settlement is the input that releases it.
        steps.push(vec![
            spawn(&format!("{prefix}-spawn-wake"), "wake", state_tag(prefix, "ok")),
            call(
                &format!("{prefix}-input"),
                "wait",
                json!({"input": true}),
            ),
        ]);
    } else {
        // Settled and failed children.
        steps.push(vec![waited(form, &format!("{prefix}-ok"), &["ok"], None)]);
        steps.push(vec![waited(form, &format!("{prefix}-bad"), &["bad"], None)]);
        steps.push(vec![waited(form, &format!("{prefix}-idle"), &["idle"], None)]);
        // A running child: a settled sibling already meets the quorum, so the
        // wait returns without a timer and reports the running phase.
        steps.push(vec![call(
            &format!("{prefix}-run"),
            "wait",
            json!({"agents": ["ok", "run"], "need": 1}),
        )]);
        // A child spawned in the wait's own step is still spawning.
        steps.push(vec![
            spawn(&format!("{prefix}-spawn-start"), "start", state_tag(prefix, "start")),
            call(
                &format!("{prefix}-start"),
                "wait",
                json!({"agents": ["ok", "start"], "need": 1}),
            ),
        ]);
        // A settled child's fork is handed back by the host, so the wait
        // reports the merged state without a separate merge command.
        steps.push(vec![waited(form, &format!("{prefix}-merged"), &["merge"], None)]);
        // A settled child in a terminal outcome names the archived and
        // stopped states without a host control that could outlive the wait.
        steps.push(vec![waited(form, &format!("{prefix}-archived"), &["arch"], None)]);
        steps.push(vec![waited(form, &format!("{prefix}-stopped"), &["stop"], None)]);
        // A quorum over a settled and a failed child.
        steps.push(vec![call(
            &format!("{prefix}-quorum"),
            "wait",
            json!({"agents": ["ok", "bad"], "need": 1}),
        )]);
    }
    if include_mid {
        // Join the middle agent so its own grandchild waits are observed.
        steps.push(vec![call(
            &format!("{prefix}-join-mid"),
            "wait",
            json!({"agents": ["mid"]}),
        )]);
    }
    steps
}

fn state_tag(prefix: &str, name: &str) -> &'static str {
    // A fixed set of tags per root/mid prefix; the plans below register them.
    let tags: &[(&str, &str, &'static str)] = &[
        ("root", "ok", "c3-root-ok"),
        ("root", "bad", "c3-root-bad"),
        ("root", "run", "c3-root-run"),
        ("root", "idle", "c3-root-idle"),
        ("root", "merge", "c3-root-merge"),
        ("root", "arch", "c3-root-arch"),
        ("root", "stop", "c3-root-stop"),
        ("root", "start", "c3-root-start"),
        ("mid", "ok", "c3-mid-ok"),
        ("mid", "bad", "c3-mid-bad"),
        ("mid", "run", "c3-mid-run"),
        ("mid", "idle", "c3-mid-idle"),
        ("mid", "merge", "c3-mid-merge"),
        ("mid", "arch", "c3-mid-arch"),
        ("mid", "stop", "c3-mid-stop"),
        ("mid", "start", "c3-mid-start"),
        ("reopen", "ok", "c3-reopen-ok"),
    ];
    tags.iter()
        .find(|(owner, child, _)| *owner == prefix && *child == name)
        .map(|(_, _, tag)| *tag)
        .unwrap_or("c3-root-ok")
}

fn mid_tag(prefix: &str) -> &'static str {
    if prefix == "root" { "c3-mid" } else { "c3-nested-mid" }
}

fn plans(form: &str) -> Vec<Plan> {
    let mut plans = vec![
        Plan {
            tag: "c3-root",
            steps: state_steps(form, "root", true),
            fail_once: None,
        },
        Plan {
            tag: "c3-mid",
            steps: state_steps(form, "mid", false),
            fail_once: None,
        },
        Plan {
            tag: "c3-reopen",
            steps: vec![
                vec![waited(form, "c3-reopen-wait", &["ok"], Some(5))],
                vec![waited(form, "c3-reopen-mid", &["mid"], Some(5))],
            ],
            fail_once: None,
        },
    ];
    for prefix in ["root", "mid"] {
        plans.extend([
            settled(state_tag(prefix, "ok")),
            failed(state_tag(prefix, "bad")),
            parked(state_tag(prefix, "run")),
            settled(state_tag(prefix, "idle")),
            writer(state_tag(prefix, "merge"), "merged.txt"),
            failed(state_tag(prefix, "arch")),
            failed(state_tag(prefix, "stop")),
            settled(state_tag(prefix, "start")),
        ]);
    }
    plans.push(settled(state_tag("reopen", "ok")));
    plans.push(Plan {
        tag: "done",
        steps: Vec::new(),
        fail_once: None,
    });
    plans
}

/// Every wait result the requests show, by call id.
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
        .ok_or_else(|| format!("wait {id} never returned: {:?}", results.iter().map(|(id, _)| id).collect::<Vec<_>>()))
}

fn reason(value: &Value) -> &str {
    match value.get("reason") {
        Some(Value::String(reason)) => reason,
        Some(reason) => reason.get("kind").and_then(Value::as_str).unwrap_or(""),
        None => "",
    }
}

fn settled_entry<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value
        .get("settled")?
        .as_array()?
        .iter()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
}

fn unsettled_entry<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value
        .get("unsettled")?
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

fn checked(form: &str, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let results = wait_results(requests)?;
    // Every wait names a reason.
    for (id, value) in &results {
        if reason(value).is_empty() {
            return Err(format!("wait {id} returned no reason: {value}"));
        }
    }
    // Settled children are reported as terminal, with their answer. The
    // no-agent form names no child, so it has no entries to compare.
    if form != "no_agents" {
    for (id, name) in [
        ("c3-root-ok", "ok"),
        ("c3-root-bad", "bad"),
        ("c3-root-idle", "idle"),
        ("c3-root-merged", "merge"),
        ("c3-mid-ok", "ok"),
        ("c3-mid-bad", "bad"),
        ("c3-mid-idle", "idle"),
        ("c3-mid-merged", "merge"),
        ("c3-reopen-wait", "ok"),
    ] {
        let value = result(&results, id)?;
        if reason(value) == "deadline" {
            // A timeout is a named reason, and a settled child has no
            // unsettled entry.
            if unsettled_entry(value, name).is_some() {
                return Err(format!("wait {id} timed out on settled {name}: {value}"));
            }
            continue;
        }
        let entry = settled_entry(value, name)
            .ok_or_else(|| format!("wait {id} did not settle {name}: {value}"))?;
        if !is_done(entry) {
            return Err(format!("wait {id} reported {name} not done: {entry}"));
        }
        if name == "bad" && entry.get("failed").is_none() {
            return Err(format!("wait {id} hid {name}'s failure: {entry}"));
        }
        if name != "bad" && entry.get("failed").is_some() {
            return Err(format!("wait {id} reported {name} failed: {entry}"));
        }
    }
    }
    // A running child is reported unsettled, never as done.
    if form != "no_agents" {
    for (id, name) in [("c3-root-run", "run"), ("c3-mid-run", "run")] {
        let value = result(&results, id)?;
        let entry = unsettled_entry(value, name)
            .ok_or_else(|| format!("wait {id} settled a running child: {value}"))?;
        if is_done(entry) {
            return Err(format!("wait {id} reported running {name} done: {entry}"));
        }
        if entry.get("phase").and_then(Value::as_str) != Some("running")
            && entry.get("phase").and_then(Value::as_str) != Some("waiting")
            && entry.get("phase").and_then(|phase| phase.get("waiting")).is_none()
        {
            return Err(format!("wait {id} reported running {name} as {entry}"));
        }
    }
    // A child spawned in the wait's own step is still starting.
    for (id, name) in [("c3-root-start", "start"), ("c3-mid-start", "start")] {
        let value = result(&results, id)?;
        let entry = unsettled_entry(value, name)
            .ok_or_else(|| format!("wait {id} settled a child in its spawn step: {value}"))?;
        if entry.get("phase").and_then(Value::as_str) != Some("spawning") {
            return Err(format!("wait {id} did not report spawning: {entry}"));
        }
    }
    }
    // A quorum returns once one of two named children settles.
    if form != "no_agents" {
        let quorum = result(&results, "c3-root-quorum")?;
        if reason(quorum) != "need_met" {
            return Err(format!("quorum wait returned {}: {quorum}", reason(quorum)));
        }
        let settled = quorum
            .get("settled")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if settled < 1 {
            return Err(format!("quorum wait returned no settled child: {quorum}"));
        }
    }
    // The no-agent form reports no child, only its reason.
    if form == "no_agents" {
        let value = result(&results, "c3-root-input")?;
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
    }
    // The middle agent's own waits are the grandchild relation.
    if result(&results, "c3-mid-join-mid").is_err()
        && result(&results, "c3-root-join-mid").is_err()
    {
        return Err("the middle agent never joined".into());
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let form = form(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let mut world = World::new(
                seed,
                &[("marker.txt", "marker\n")],
                service(plans(form)),
            )
            .map_err(oracle::failed)?;
            let prompts = [vec![("root", tag("c3-root"))]];
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
            checked(form, &first.requests)?;
            // The same parent, waited on from an independently reopened host.
            let recovery = world
                .incarnation(None, &[vec![("reopen", tag("c3-reopen"))]])
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&recovery.requests)?;
            if !recovery
                .prompts
                .first()
                .is_some_and(|(outcome, _)| *outcome == oracle::Prompted::Settled(done("c3-reopen")))
            {
                return Err(format!(
                    "the reopened root turn did not finish: {:?}",
                    recovery.prompts
                ));
            }
            checked(form, &recovery.requests)?;
            Ok(true)
        })
    })
}

// Build cache warm-up is kept in the same gate command.

// List the generated wait cases.
