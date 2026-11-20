//! Redelivery: a message delivered any number of times, in any order relative
//! to others, leaves the state of one delivery, whether its turn is running,
//! settled, or failed after partial effects.
use super::*;
use acyclic_harness::test_support::ScriptedEvent;
use family::{Generator, Variant};
use oracle::{Incarnation, Plan, World, call, done, service, settlements, tag};
use serde_json::json;

const FILES: [(&str, &str); 1] = [("c.txt", "base\n")];

/// One world per (multiplicity, target) pair.
pub(super) const GENERATOR: Generator = Generator {
    class: "E2",
    seeds: a_redelivered_message_leaves_the_state_of_one_delivery::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = a_redelivered_message_leaves_the_state_of_one_delivery,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4,
        seed_5 => 5, seed_6 => 6, seed_7 => 7, seed_8 => 8,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

/// Every (multiplicity, target) pair.
const VARIANTS: [(&str, &str); 9] = [
    ("duplicate", "settled"),
    ("reorder", "settled"),
    ("redeliver_after_failure", "settled"),
    ("duplicate", "running"),
    ("redeliver_after_failure", "running"),
    ("duplicate", "failed_with_partial_effects"),
    ("reorder", "failed_with_partial_effects"),
    ("redeliver_after_failure", "failed_with_partial_effects"),
    ("reorder", "running"),
];

fn plans() -> Vec<Plan> {
    let edit = || {
        vec![call(
            "e2-edit",
            "edit",
            json!({"path": "c.txt", "old_text": "base", "new_text": "base+"}),
        )]
    };
    vec![
        Plan {
            tag: "m1",
            steps: vec![edit()],
            fail_once: None,
        },
        Plan {
            tag: "partial",
            steps: vec![edit(), Vec::new()],
            // The answer after the edit fails: the turn ends with its
            // effect applied, and no redelivery may apply it again.
            fail_once: Some((
                1,
                ScriptedEvent::Error {
                    error: "provider internal error".into(),
                },
            )),
        },
        Plan {
            tag: "m2",
            steps: Vec::new(),
            fail_once: None,
        },
        Plan {
            tag: "check",
            steps: vec![vec![call("check-c", "read", json!({"path": "c.txt"}))]],
            fail_once: None,
        },
    ]
}

fn message(tag_name: &'static str) -> (&'static str, String) {
    (tag_name, tag(tag_name))
}

/// The redelivered message's answers equal its first, it started no further
/// model work, and its edit applied once per success the model saw.
fn one_delivery(
    incarnation: &Incarnation,
    copies: &[usize],
    first_only: bool,
) -> std::result::Result<(), String> {
    oracle::all_settled(&incarnation.requests)?;
    let answers: Vec<_> = copies
        .iter()
        .map(|index| incarnation.prompts.get(*index))
        .collect();
    if let Some(first) = answers.first().copied().flatten() {
        for answer in &answers {
            let Some(answer) = answer else {
                return Err("a redelivery was never answered".into());
            };
            if answer.0 != first.0 || (!first_only && answer.1 != first.1) {
                return Err(format!("redelivery answered {answer:?} after {first:?}"));
            }
        }
    }
    let last = incarnation.prompts.last().map(|(outcome, _)| outcome);
    if last != Some(&oracle::Prompted::Settled(done("check"))) {
        return Err(format!(
            "the owner did not continue: {:?}",
            incarnation.prompts
        ));
    }
    let history = settlements(incarnation.requests.last().ok_or("no request")?)?;
    let read = history
        .iter()
        .rev()
        .find(|settled| settled.call.name == "read")
        .ok_or("the check never read c.txt")?
        .rendered();
    let applied = oracle::edits(&read).ok_or_else(|| format!("c.txt lost its base: {read}"))?;
    let successes = history
        .iter()
        .filter(|settled| settled.call.name == "edit" && settled.succeeded())
        .count();
    if applied != successes || applied > 1 {
        return Err(format!(
            "c.txt carries {applied} edits; the model saw {successes} successful edits"
        ));
    }
    Ok(())
}

fn variant(seed: u64) -> (&'static str, &'static str) {
    VARIANTS
        .get(usize::try_from(seed).unwrap_or(0) % VARIANTS.len())
        .copied()
        .unwrap_or(("duplicate", "settled"))
}

fn variants(seed: u64) -> Vec<Variant> {
    let (multiplicity, target) = variant(seed);
    vec![Variant::from([
        ("message", "send_message"),
        ("multiplicity", multiplicity),
        ("target", target),
    ])]
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let (multiplicity, target) = variant(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let m = message(if target == "failed_with_partial_effects" {
                "partial"
            } else {
                "m1"
            });
            let (m2, check) = (message("m2"), message("check"));
            let together = target == "running";
            if multiplicity == "redeliver_after_failure" {
                // The first delivery's host crashes anywhere in its turn; the
                // client redelivers to the reopened host.
                let crash = oracle::Crash {
                    seed,
                    files: &FILES,
                    plans,
                    prompts: vec![vec![m.clone()]],
                    at: oracle::CrashAt::Window(0, usize::MAX),
                    mode: oracle::crash_mode("crash_after", seed).ok_or("crash mode")?,
                };
                let resend = if together {
                    vec![vec![m.clone(), m.clone()], vec![check.clone()]]
                } else {
                    vec![vec![m.clone()], vec![check.clone()]]
                };
                let (crashed, recovered) = crash.recover(resend).await?;
                let copies: &[usize] = if together { &[0, 1] } else { &[0] };
                one_delivery(&recovered, copies, true)
                    .map_err(|report| format!("after {crashed}: {report}"))?;
                return Ok(crashed.in_window);
            }
            let (groups, copies) = match (multiplicity, together) {
                ("duplicate", true) => (vec![vec![m.clone(), m.clone()]], vec![0, 1]),
                ("duplicate", false) => (vec![vec![m.clone()], vec![m.clone()]], vec![0, 1]),
                (_, true) => (vec![vec![m.clone(), m2.clone(), m.clone()]], vec![0, 2]),
                (_, false) => (
                    vec![vec![m.clone()], vec![m2.clone()], vec![m.clone()]],
                    vec![0, 2],
                ),
            };
            let mut groups = groups;
            groups.push(vec![check.clone()]);
            let mut world = World::new(seed, &FILES, service(plans())).map_err(oracle::failed)?;
            let incarnation = world
                .incarnation(None, &groups)
                .await
                .map_err(oracle::failed)?;
            if target == "failed_with_partial_effects"
                && !matches!(
                    incarnation.prompts.first(),
                    Some((oracle::Prompted::Failed(_), _))
                )
            {
                return Err(format!(
                    "the partial turn did not fail: {:?}",
                    incarnation.prompts
                ));
            }
            // A reordered copy follows another message's model work.
            one_delivery(&incarnation, &copies, together || multiplicity == "reorder")?;
            Ok(true)
        })
    })
}
