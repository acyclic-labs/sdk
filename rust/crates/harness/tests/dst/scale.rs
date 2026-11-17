//! J1: durable work per step is a function of that step's delta, not of the
//! history before it. A long turn writes one equal file per step; the durable
//! bytes between two ordinary dispatches must stay flat.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, World, call, done, service, settlements, simulate, tag};

const STEPS: usize = 8;
const FILE_BYTES: usize = 7 * 1024;

pub(super) const GENERATOR: Generator = Generator {
    class: "J1",
    seeds: flat_durable_work_per_step::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = flat_durable_work_per_step,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn plan() -> Plan {
    Plan {
        tag: "scale",
        steps: (0..STEPS)
            .map(|step| {
                // The first step admits two calls together, so a summary cut
                // must wait for both results before it drops the exchange.
                let calls = if step == 0 { 2 } else { 1 };
                (0..calls)
                    .map(|part| {
                        call(
                            &format!("s{step}-{part}"),
                            "write",
                            serde_json::json!({
                                "path": format!("step-{step}-{part}.txt"),
                                "content": "x".repeat(FILE_BYTES),
                            }),
                        )
                    })
                    .collect()
            })
            .collect(),
        fail_once: None,
    }
}

/// Every call the model saw settled, across the whole turn. A summary may
/// drop older calls from later requests, so the last request alone is partial.
fn settled_calls(requests: &[acyclic_harness::model::ModelRequest]) -> Vec<oracle::Settled> {
    let mut seen = std::collections::BTreeMap::new();
    for request in requests {
        if let Ok(settled) = settlements(request) {
            for settled in settled {
                seen.insert(settled.call.call_id.clone(), settled);
            }
        }
    }
    seen.into_values().collect()
}

/// Durable bytes between each pair of ordinary model dispatches. Summary
/// requests are not work steps, so they do not open or close an interval.
fn per_step(
    writes: &[DurableWrite],
    dispatched_at: &[usize],
    requests: &[acyclic_harness::model::ModelRequest],
) -> Vec<usize> {
    let ordinary: Vec<usize> = requests
        .iter()
        .zip(dispatched_at)
        .filter(|(request, _)| !request.summary)
        .map(|(_, &at)| at)
        .collect();
    ordinary
        .windows(2)
        .map(|window| {
            let (start, end) = (window[0], window[1]);
            writes[start..end]
                .iter()
                .map(|write| write.requested)
                .sum()
        })
        .collect()
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    simulate(seed, || {
        entry(async move {
            let mut world = World::new(seed, &[], service(vec![plan()])).map_err(oracle::failed)?;
            let prompt = tag("scale");
            let incarnation = world
                .incarnation_recorded(None, &[vec![("scale", prompt)]])
                .await
                .map_err(oracle::failed)?;
            if incarnation.crashed.is_some() {
                return Err("the scale world crashed".into());
            }
            match incarnation.prompts.first() {
                Some((oracle::Prompted::Settled(text), _)) if text.contains(&done("scale")) => {}
                Some((oracle::Prompted::Settled(text), _)) => {
                    return Err(format!("the scale prompt settled without its answer: {text}"));
                }
                Some((oracle::Prompted::Failed(message), _)) => {
                    return Err(format!("the scale prompt failed: {message}"));
                }
                None => return Err("the scale world settled no prompt".into()),
            }
            if !incarnation.requests.iter().any(|request| request.summary) {
                return Err("the scale world never reached the context manager".into());
            }
            let settled = settled_calls(&incarnation.requests);
            let expected_writes = STEPS + 1;
            if settled.len() < expected_writes {
                return Err(format!(
                    "the scale world settled {} of {expected_writes} writes",
                    settled.len()
                ));
            }
            if let Some(failed) = settled.iter().find(|settled| !settled.succeeded()) {
                return Err(format!(
                    "the scale world failed a write: {}",
                    failed.rendered()
                ));
            }
            let steps = per_step(
                &incarnation.writes,
                &incarnation.dispatched_at,
                &incarnation.requests,
            );
            if steps.len() < STEPS {
                return Err(format!(
                    "the scale world dispatched {} of {STEPS} steps",
                    steps.len()
                ));
            }
            // Skip the first interval's setup. Every later step writes the same
            // bytes, so its durable work must not grow with the step index.
            let steady = &steps[1..];
            let first = steady.first().copied().unwrap_or(0);
            let bound = first.saturating_mul(2).saturating_add(64 * 1024);
            if let Some((index, step)) = steady.iter().enumerate().find(|(_, step)| **step > bound) {
                return Err(format!(
                    "durable bytes per step grew with history: step {index}, first {first}, step {step}, all {steps:?}"
                ));
            }
            Ok(true)
        })
    })
}

fn variants(_seed: u64) -> Vec<Variant> {
    [Variant::from([
        ("resource", "durable_log_bytes"),
        ("driver", "steps"),
    ])]
    .into_iter()
    .collect()
}
