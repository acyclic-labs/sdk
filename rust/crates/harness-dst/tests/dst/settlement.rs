//! Settlement: every admitted call settles exactly once, the model sees that
//! settlement paired with its call, and the call's effect matches it.
use super::*;
use acyclic_harness::test_support::ScriptedEvent;
use family::{Generator, Variant};
use oracle::{Plan, Prompted, Settled, World, call, done, service, settlements, tag};
use serde_json::json;

const FILES: [(&str, &str); 3] = [
    ("a.txt", "alpha\n"),
    ("c.txt", "base\n"),
    ("d.txt", "doomed\n"),
];

/// Faults this world drives, in seed order.
const FAULTS: [&str; 6] = [
    "none",
    "crash_before",
    "crash_after",
    "crash_torn",
    "redeliver",
    "stream_interrupted",
];

/// Entries one step of this world admits together.
const ENTRIES: [&str; 4] = ["read", "write", "edit", "remove"];

/// One world per fault; every world admits every entry.
pub(super) const GENERATOR: Generator = Generator {
    class: "A1",
    seeds: every_admitted_call_settles_exactly_once_whatever_the_fault::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_admitted_call_settles_exactly_once_whatever_the_fault,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5,
    ],
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
    ENTRIES
        .iter()
        .map(|entry| Variant::from([("entry", *entry), ("fault", fault(seed))]))
        .collect()
}

fn step() -> Vec<acyclic_harness::model::ToolCall> {
    let calls = [
        call("a1-read", "read", json!({"path": "a.txt"})),
        call(
            "a1-write",
            "write",
            json!({"path": "b.txt", "content": "written\n", "ifVersion": "absent"}),
        ),
        call(
            "a1-edit",
            "edit",
            json!({"path": "c.txt", "old_text": "base", "new_text": "base+"}),
        ),
        call(
            "a1-remove",
            "remove",
            json!({"path": "d.txt", "ifVersion": "any"}),
        ),
    ];
    calls.into_iter().collect()
}

fn plans(interrupt: bool) -> Vec<Plan> {
    vec![
        Plan {
            tag: "a1",
            steps: vec![step()],
            fail_once: interrupt.then(|| {
                (
                    0,
                    ScriptedEvent::Interrupted {
                        error: "connection lost after the calls".into(),
                    },
                )
            }),
        },
        Plan {
            tag: "check",
            steps: vec![
                ["b.txt", "c.txt", "d.txt"]
                    .iter()
                    .enumerate()
                    .map(|(index, path)| {
                        call(&format!("check-{index}"), "read", json!({"path": path}))
                    })
                    .collect(),
            ],
            fail_once: None,
        },
    ]
}

fn prompt(name: &str) -> (&'static str, String) {
    let id = if name == "a1" { "a1" } else { "check" };
    (id, tag(name))
}

/// Effects equal visible successes: the edit applied once per successful
/// edit, the file written iff a write succeeded, removed iff a remove did.
fn effects_match(history: &[Settled], step_ran: bool) -> std::result::Result<(), String> {
    let successes = |name: &str| {
        history
            .iter()
            .filter(|settled| settled.call.name == name && settled.succeeded())
            .count()
    };
    let check = |path: &str| {
        history
            .iter()
            .rev()
            .find(|settled| settled.call.name == "read" && settled.path() == Some(path))
            .ok_or_else(|| format!("the check never read {path}"))
    };
    // Every entry must have worked when the step's turn finished: a world
    // where all four tools fail would otherwise match unchanged files.
    let read = history
        .iter()
        .any(|settled| settled.path() == Some("a.txt") && settled.succeeded());
    for (entry, worked) in [
        ("read", read),
        ("write", successes("write") > 0),
        ("edit", successes("edit") > 0),
        ("remove", successes("remove") > 0),
    ] {
        if step_ran && !worked {
            return Err(format!("no {entry} call succeeded: {history:?}"));
        }
    }
    let edited = check("c.txt")?.rendered();
    let applied = oracle::edits(&edited).ok_or_else(|| format!("c.txt lost its base: {edited}"))?;
    if applied != successes("edit") {
        return Err(format!(
            "c.txt carries {applied} edits but the model saw {} successful edit settlements",
            successes("edit")
        ));
    }
    let written = check("b.txt")?;
    if written.succeeded() != (successes("write") > 0) {
        return Err(format!(
            "b.txt present={} but the model saw {} successful writes",
            written.succeeded(),
            successes("write")
        ));
    }
    let removed = !check("d.txt")?.succeeded();
    if removed != (successes("remove") > 0) {
        return Err(format!(
            "d.txt removed={removed} but the model saw {} successful removes",
            successes("remove")
        ));
    }
    Ok(())
}

fn checked(incarnation: &oracle::Incarnation) -> std::result::Result<(), String> {
    oracle::all_settled(&incarnation.requests)?;
    let Some((Prompted::Settled(text), _)) = incarnation.prompts.last() else {
        return Err(format!(
            "the owner did not continue after the fault: {:?}",
            incarnation.prompts.last()
        ));
    };
    if *text != done("check") {
        return Err(format!("check answered {text:?}"));
    }
    let last = incarnation
        .requests
        .last()
        .ok_or("the check reached no model")?;
    let step_ran = incarnation
        .prompts
        .iter()
        .any(|(outcome, _)| *outcome == Prompted::Settled(done("a1")));
    effects_match(&settlements(last)?, step_ran)
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let fault = fault(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let a1 = prompt("a1");
            let check = prompt("check");
            if let Some(mode) = oracle::crash_mode(fault, seed) {
                // The window opens at the call step's dispatch and closes at
                // the dispatch that carries its settlements.
                let crash = oracle::Crash {
                    seed,
                    files: &FILES,
                    plans: || plans(false),
                    prompts: vec![vec![a1.clone()]],
                    at: oracle::CrashAt::Window(0, 1),
                    mode,
                };
                // The client redelivers its unanswered prompt, then continues.
                let (crashed, recovered) = crash
                    .recover(vec![vec![a1.clone()], vec![check.clone()]])
                    .await?;
                checked(&recovered).map_err(|report| format!("after {crashed}: {report}"))?;
                return Ok(crashed.in_window);
            }
            let plans = plans(fault == "stream_interrupted");
            let mut world = World::new(seed, &FILES, service(plans)).map_err(oracle::failed)?;
            let prompts: Vec<_> = if fault == "redeliver" {
                vec![vec![a1.clone()], vec![a1.clone()], vec![check.clone()]]
            } else {
                vec![vec![a1.clone()], vec![check.clone()]]
            };
            let incarnation = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            if fault == "redeliver" {
                let (first, second) = (incarnation.prompts.first(), incarnation.prompts.get(1));
                if first.map(|(outcome, _)| outcome) != second.map(|(outcome, _)| outcome)
                    || first.map(|(_, requests)| requests) != second.map(|(_, requests)| requests)
                {
                    return Err(format!(
                        "redelivered prompt answered {second:?} after {first:?}"
                    ));
                }
            }
            if fault == "none"
                && !matches!(incarnation.prompts.first(), Some((Prompted::Settled(text), _)) if *text == done("a1"))
            {
                return Err(format!(
                    "the uncut call step did not settle: {:?}",
                    incarnation.prompts
                ));
            }
            checked(&incarnation)?;
            Ok(true)
        })
    })
}
