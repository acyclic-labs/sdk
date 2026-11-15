//! Containment: a fault at one agent reaches only that agent, as a result its
//! model sees or a failed turn; the owner keeps accepting work.
use super::*;
use acyclic_harness::test_support::ScriptedEvent;
use coverage::Coverage;
use oracle::{Plan, Prompted, World, call, done, service, settlements, tag};
use serde_json::json;

/// Origins this world injects, in seed order.
const ORIGINS: [&str; 5] = [
    "tool_error",
    "malformed_output",
    "rate_limit",
    "transport",
    "internal_error",
];

pub(super) const SEEDS: std::ops::Range<u64> = 0..5;

fn plan(origin: &str) -> Plan {
    let (steps, fail_once) = match origin {
        "tool_error" => (
            vec![vec![call(
                "f1-read",
                "read",
                json!({"path": "missing.txt"}),
            )]],
            None,
        ),
        "malformed_output" => (
            vec![vec![
                call("f1-unknown", "no_such_tool", json!({})),
                call("f1-invalid", "write", json!({"bogus": 1})),
            ]],
            None,
        ),
        "rate_limit" => (
            vec![Vec::new()],
            Some((
                0,
                ScriptedEvent::Refused {
                    status: 429,
                    error: "rate limited".into(),
                },
            )),
        ),
        "transport" => (
            vec![Vec::new()],
            Some((
                0,
                ScriptedEvent::Interrupted {
                    error: "connection reset".into(),
                },
            )),
        ),
        // A local failure inside the model stream, which leaves the attempt
        // to reconciliation rather than settling it as a provider outcome.
        _ => (
            vec![Vec::new()],
            Some((
                0,
                ScriptedEvent::Error {
                    error: "provider internal error".into(),
                },
            )),
        ),
    };
    Plan {
        tag: "f1",
        steps,
        fail_once,
    }
}

/// The faulted turn's own outcome. A tool-level fault reaches the model,
/// which finishes the turn. A model-level fault (every one here is
/// transient) either fails the turn naming its cause or, when the harness
/// retries it, completes it with the plan's answer; nothing else is a
/// contained fault.
fn faulted(origin: &str, outcome: Option<&Prompted>) -> std::result::Result<(), String> {
    let cause = match origin {
        "rate_limit" => "rate limited",
        "transport" => "connection reset",
        "internal_error" => "provider internal error",
        _ => return Ok(()),
    };
    match outcome {
        Some(Prompted::Failed(message)) if message.contains(cause) => Ok(()),
        Some(Prompted::Settled(text)) if *text == done("f1") => Ok(()),
        other => Err(format!("{origin} ended its turn as {other:?}")),
    }
}

#[test]
fn a_misreported_model_fault_is_not_contained() {
    // Planted control: wrong outcomes must fail the containment check.
    let wrong = [
        ("internal_error", Some(Prompted::Settled("other".into()))),
        (
            "transport",
            Some(Prompted::Failed("connection interrupted".into())),
        ),
        ("rate_limit", Some(Prompted::Failed("unrelated".into()))),
        ("transport", Some(Prompted::Settled("other".into()))),
        ("internal_error", None),
    ];
    for (origin, outcome) in wrong {
        assert!(
            faulted(origin, outcome.as_ref()).is_err(),
            "{origin} {outcome:?}"
        );
    }
    assert!(
        faulted(
            "internal_error",
            Some(&Prompted::Failed("provider internal error".into()))
        )
        .is_ok()
    );
}

pub(super) fn world(seed: u64, coverage: &mut Coverage) -> std::result::Result<(), String> {
    let origin = ORIGINS
        .get(usize::try_from(seed % ORIGINS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("tool_error");
    oracle::simulate(seed, move || {
        entry(async move {
            let plans = vec![
                plan(origin),
                Plan {
                    tag: "next",
                    steps: Vec::new(),
                    fail_once: None,
                },
            ];
            let mut world = World::new(seed, &[], service(plans)).map_err(oracle::failed)?;
            let prompts = [vec![("f1", tag("f1"))], vec![("next", tag("next"))]];
            let incarnation = world
                .incarnation(None, &prompts)
                .await
                .map_err(oracle::failed)?;
            oracle::all_settled(&incarnation.requests)?;
            let outcomes: Vec<_> = incarnation
                .prompts
                .iter()
                .map(|(outcome, _)| outcome)
                .collect();
            if outcomes.last() != Some(&&Prompted::Settled(done("next"))) {
                return Err(format!("{origin} reached the owner: {outcomes:?}"));
            }
            faulted(origin, outcomes.first().copied())?;
            if matches!(origin, "tool_error" | "malformed_output") {
                // A tool-level fault is the model's to read and correct.
                if outcomes.first() != Some(&&Prompted::Settled(done("f1"))) {
                    return Err(format!(
                        "{origin} ended the turn instead of reaching its model: {outcomes:?}"
                    ));
                }
                let history = settlements(incarnation.requests.last().ok_or("no request")?)?;
                // Only the faulted calls ever ran in this world.
                if history.is_empty() || history.iter().any(oracle::Settled::succeeded) {
                    return Err(format!(
                        "{origin} did not reach its model as a tool error: {history:?}"
                    ));
                }
            }
            Ok(())
        })
    })?;
    coverage.cover("F1", &[("origin", origin), ("location", "root")]);
    Ok(())
}
