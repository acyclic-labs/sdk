//! Refusals: every tool failure names the cause the world actually produced,
//! and a successful nested call stays successful.
use super::*;
use acyclic_harness::model::ToolResultContent;
use family::{Generator, Variant};
use oracle::{Plan, Settled, World, call, service, settlements, tag};
use serde_json::json;

const FILES: [(&str, &str); 3] = [
    ("present.txt", "alpha\nbeta\n"),
    ("edit.txt", "base\n"),
    ("exists.txt", "original\n"),
];

/// One refusal this world drives, with the tool whose call produces it.
type Case = (&'static str, &'static str, &'static str);

/// Every refusal this world drives, in call order.
const CASES: [Case; 5] = [
    ("not-found", "read", "not_found"),
    ("edit-missing", "edit", "edit_text_missing"),
    ("past-eof", "read", "past_eof"),
    ("version-conflict", "write", "version_conflict"),
    ("nested-success", "code", "nested_success"),
];

pub(super) const GENERATOR: Generator = Generator {
    class: "R1",
    seeds: every_refusal_names_its_true_cause::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_refusal_names_its_true_cause,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

/// The case one world drives; a world stays well inside the per-case limit.
fn cases(seed: u64) -> &'static [Case] {
    match seed {
        0 => &CASES[..1],
        1 => &CASES[1..2],
        2 => &CASES[2..3],
        3 => &CASES[3..4],
        4 => &CASES[4..],
        _ => &[],
    }
}

fn variants(seed: u64) -> Vec<Variant> {
    cases(seed)
        .iter()
        .map(|(_, tool, cause)| Variant::from([("cause", *cause), ("tool", *tool)]))
        .collect()
}

fn call_for((id, tool, _): &Case) -> acyclic_harness::model::ToolCall {
    match *id {
        "not-found" => call(id, tool, json!({"path": "missing.txt"})),
        "edit-missing" => call(
            id,
            tool,
            json!({"path": "edit.txt", "old_text": "absent", "new_text": "changed"}),
        ),
        "past-eof" => call(
            id,
            tool,
            json!({"path": "present.txt", "selection": {"kind": "lines", "offset": 100, "limit": 10}}),
        ),
        "version-conflict" => call(
            id,
            tool,
            json!({"path": "exists.txt", "content": "replacement\n", "ifVersion": "absent"}),
        ),
        "nested-success" => call(
            id,
            tool,
            json!({"code": "export default await tools.read({path: 'present.txt'});"}),
        ),
        _ => unreachable!("every case names its call"),
    }
}

fn plan(seed: u64) -> Plan {
    Plan {
        tag: "r1",
        steps: vec![cases(seed).iter().map(call_for).collect()],
        fail_once: None,
    }
}

/// The cause the settled result must name, or the success it must keep.
fn check(cause: &str, settled: &Settled) -> std::result::Result<(), String> {
    let id = &settled.call.call_id;
    let refused = |needle: &str| match &settled.result {
        ToolResultContent::Error { message } if message.contains(needle) => Ok(()),
        ToolResultContent::Error { message } => {
            Err(format!("{id} refused without naming {needle:?}: {message}"))
        }
        other => Err(format!("{id} was not refused: {other:?}")),
    };
    match cause {
        "not_found" => refused("does not exist"),
        "edit_text_missing" => refused("exact edit text is missing"),
        "version_conflict" => refused("already exists"),
        "past_eof" | "nested_success" => {
            if settled.succeeded() {
                Ok(())
            } else {
                Err(format!(
                    "{id} ({cause}) was refused: {}",
                    settled.rendered()
                ))
            }
        }
        _ => Err(format!("{id} has no oracle for cause {cause}")),
    }
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    oracle::simulate(seed, move || {
        entry(async move {
            let mut world =
                World::new(seed, &FILES, service(vec![plan(seed)])).map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &[vec![("r1", tag("r1"))]])
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&incarnation.requests)?;
            let request = incarnation.requests.last().ok_or("no model request")?;
            let settled = settlements(request)?;
            for (id, _, cause) in cases(seed) {
                let result = settled
                    .iter()
                    .find(|settled| settled.call.call_id == *id)
                    .ok_or_else(|| format!("{id} never settled"))?;
                check(cause, result)?;
            }
            Ok(true)
        })
    })
}
