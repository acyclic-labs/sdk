//! J1: durable work per step is a function of that step's delta, not of the
//! history before it. A long turn writes one equal file per step; the durable
//! operations between two dispatches must stay flat.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, World, call, service, simulate, tag};

const STEPS: usize = 8;
const FILE_BYTES: usize = 8 * 1024;

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
                vec![call(
                    &format!("s{step}"),
                    "write",
                    serde_json::json!({
                        "path": format!("step-{step}.txt"),
                        "content": "x".repeat(FILE_BYTES),
                    }),
                )]
            })
            .collect(),
        fail_once: None,
    }
}

/// Durable operations between each pair of model dispatches. The first
/// dispatch includes host setup, so only the steady steps are compared.
fn per_step(dispatched_at: &[usize]) -> Vec<usize> {
    dispatched_at
        .windows(2)
        .map(|window| window[1].saturating_sub(window[0]))
        .collect()
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    simulate(seed, || {
        entry(async move {
            let mut world = World::new(seed, &[], service(vec![plan()])).map_err(oracle::failed)?;
            let prompt = tag("scale");
            let incarnation = world
                .incarnation(None, &[vec![("scale", prompt)]])
                .await
                .map_err(oracle::failed)?;
            if incarnation.crashed.is_some() {
                return Err("the scale world crashed".into());
            }
            if let Some((prompted, _)) = incarnation.prompts.first()
                && let oracle::Prompted::Failed(message) = prompted
            {
                return Err(format!("the scale prompt failed: {message}"));
            }
            if !incarnation.requests.iter().any(|request| request.summary) {
                return Err("the scale world never reached the context manager".into());
            }
            let steps = per_step(&incarnation.dispatched_at);
            if steps.len() < STEPS {
                return Err(format!(
                    "the scale world dispatched {} of {STEPS} steps",
                    steps.len()
                ));
            }
            // Skip the first dispatch's setup. Every later step writes the same
            // file, so its durable work must not grow with the step index.
            let steady = &steps[1..];
            let first = steady.first().copied().unwrap_or(0);
            let last = steady.last().copied().unwrap_or(0);
            let bound = first.saturating_mul(2).saturating_add(64);
            if last > bound {
                return Err(format!(
                    "durable operations per step grew with history: first {first}, last {last}, all {steps:?}"
                ));
            }
            Ok(true)
        })
    })
}

fn variants(_seed: u64) -> Vec<Variant> {
    ["durable_log_bytes", "index_keys", "transcript_projection"]
        .into_iter()
        .map(|resource| Variant::from([("resource", resource), ("driver", "steps")]))
        .collect()
}
