//! Determinism: every class world replays from its seed to the same trace,
//! and erasing the scheduler's entropy source changes that trace.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, Prompted, World, call, failed, service, settlements, tag};
use serde_json::json;

/// The X1 world dimension, in `classes.json` order.
const WORLDS: [&str; 37] = [
    "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3", "C4", "C5", "D1", "D2", "D3", "D4", "D5",
    "D6", "E1", "E2", "E3", "F1", "F2", "G1", "H1", "H2", "H3", "H4", "H5", "H6", "I1", "I2", "J1",
    "J2", "O1", "R1", "S1", "T1",
];

const FILES: [(&str, &str); 1] = [("base.txt", "base\n")];

/// One seed per class world. A world runs the same production turn twice and
/// the entropy-erased control once. The class generators own the per-class
/// worlds; this generic replay does not claim their variants.
pub(super) const GENERATOR: Generator = Generator {
    class: "X1",
    seeds: every_class_world_replays_byte_identically::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_class_world_replays_byte_identically,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0,
        seed_1 => 1,
        seed_2 => 2,
        seed_3 => 3,
        seed_4 => 4,
        seed_5 => 5,
        seed_6 => 6,
        seed_7 => 7,
        seed_8 => 8,
        seed_9 => 9,
        seed_10 => 10,
        seed_11 => 11,
        seed_12 => 12,
        seed_13 => 13,
        seed_14 => 14,
        seed_15 => 15,
        seed_16 => 16,
        seed_17 => 17,
        seed_18 => 18,
        seed_19 => 19,
        seed_20 => 20,
        seed_21 => 21,
        seed_22 => 22,
        seed_23 => 23,
        seed_24 => 24,
        seed_25 => 25,
        seed_26 => 26,
        seed_27 => 27,
        seed_28 => 28,
        seed_29 => 29,
        seed_30 => 30,
        seed_31 => 31,
        seed_32 => 32,
        seed_33 => 33,
        seed_34 => 34,
        seed_35 => 35,
        seed_36 => 36,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

/// The generic replay does not run the per-class generator worlds or a
/// cross-process peer, so it declares no X1 variants; the coverage gate counts
/// them as uncovered.
fn variants(_seed: u64) -> Vec<Variant> {
    Vec::new()
}

fn class(seed: u64) -> &'static str {
    let index = match usize::try_from(seed % WORLDS.len() as u64) {
        Ok(index) => index,
        Err(_) => return WORLDS[0],
    };
    match WORLDS.get(index) {
        Some(class) => class,
        None => WORLDS[0],
    }
}

fn plans(class: &'static str) -> Vec<Plan> {
    let content = format!("{class}\n");
    vec![Plan {
        tag: class,
        steps: vec![vec![
            call(
                "x1-write",
                "write",
                json!({
                    "path": "out.txt",
                    "content": content,
                    "ifVersion": "absent",
                }),
            ),
            call(
                "x1-read",
                "read",
                json!({
                    "path": "out.txt",
                }),
            ),
        ]],
        fail_once: None,
    }]
}

struct Replay {
    digest: blake3::Hash,
    trace: serde_json::Value,
}

async fn trace(
    world_seed: u64,
    variation: u64,
    class: &'static str,
    erase_entropy: bool,
) -> std::result::Result<Replay, String> {
    let mut world = if erase_entropy {
        World::without_entropy(world_seed, &FILES, service(plans(class)))
    } else {
        World::new(world_seed, &FILES, service(plans(class)))
    }
    .map_err(failed)?;
    let prompts = [vec![(class, format!("{} seed:{variation}", tag(class)))]];
    let incarnation = world.incarnation(None, &prompts).await.map_err(failed)?;
    if !matches!(incarnation.prompts.as_slice(), [(Prompted::Settled(_), _)]) {
        return Err(format!(
            "X1 {class} seed {world_seed} prompt did not settle: {:?}",
            incarnation.prompts
        ));
    }
    let settled = incarnation.requests.iter().find_map(|request| {
        settlements(request)
            .ok()
            .filter(|calls| calls.len() == 2 && calls.iter().all(|call| call.succeeded()))
    });
    if settled.is_none() {
        let observed = incarnation
            .requests
            .iter()
            .map(|request| settlements(request).map(|calls| calls.len()))
            .collect::<Vec<_>>();
        return Err(format!(
            "X1 {class} seed {world_seed} did not settle both tool calls: {observed:?}"
        ));
    }
    // Every replayed input stays in the digest. The explicit record tree only
    // makes the first differing field nameable when two runs disagree.
    let trace = serde_json::json!({
        "prompts": format!("{:?}", incarnation.prompts),
        "requests": serde_json::to_value(&incarnation.requests).map_err(failed)?,
        "dispatched_at": incarnation.dispatched_at,
        "durable": incarnation.durable,
        "crashed": format!("{:?}", incarnation.crashed),
    });
    let digest = blake3::hash(&serde_json::to_vec(&trace).map_err(failed)?);
    Ok(Replay { digest, trace })
}

fn short(value: &serde_json::Value) -> String {
    let text = value.to_string();
    let mut shown: String = text.chars().take(240).collect();
    if shown.len() < text.len() {
        shown.push('…');
    }
    shown
}

fn first_difference(left: &serde_json::Value, right: &serde_json::Value) -> String {
    fn walk(path: &str, left: &serde_json::Value, right: &serde_json::Value) -> String {
        match (left, right) {
            (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
                let keys = left
                    .keys()
                    .chain(right.keys())
                    .collect::<std::collections::BTreeSet<_>>();
                for key in keys {
                    let path = format!("{path}.{key}");
                    match (left.get(key), right.get(key)) {
                        (Some(left), Some(right)) if left != right => {
                            return walk(&path, left, right);
                        }
                        (Some(left), None) => {
                            return format!("{path}: {} != <missing>", short(left));
                        }
                        (None, Some(right)) => {
                            return format!("{path}: <missing> != {}", short(right));
                        }
                        _ => {}
                    }
                }
                format!("{path}: objects differ")
            }
            (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
                for (index, (left, right)) in left.iter().zip(right).enumerate() {
                    if left != right {
                        return walk(&format!("{path}[{index}]"), left, right);
                    }
                }
                format!("{path}.len: {} != {}", left.len(), right.len())
            }
            _ => format!("{path}: {} != {}", short(left), short(right)),
        }
    }
    walk("trace", left, right)
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let class = class(seed);
    let world_seed = seed % WORLDS.len() as u64;
    oracle::simulate(seed, || {
        entry(async move {
            let first = trace(world_seed, seed, class, false).await?;
            let second = trace(world_seed, seed, class, false).await?;
            if first.digest != second.digest {
                return Err(format!(
                    "X1 {class} seed {seed} diverged in-process at {}: {} != {}",
                    first_difference(&first.trace, &second.trace),
                    first.digest,
                    second.digest,
                ));
            }
            let erased = trace(world_seed, seed, class, true).await?;
            if first.digest == erased.digest {
                return Err(format!(
                    "X1 {class} seed {seed} entropy erasure did not change the trace"
                ));
            }
            println!("DST_X1_DIGEST={class} {}", first.digest);
            Ok(true)
        })
    })
}

#[test]
fn two_incarnations_reopen_the_same_state() -> std::result::Result<(), String> {
    oracle::simulate(0, || {
        entry(async move {
            let plans = vec![
                Plan {
                    tag: "write-state",
                    steps: vec![vec![call(
                        "write-state",
                        "write",
                        json!({
                            "path": "kept.txt",
                            "content": "kept\n",
                            "ifVersion": "absent",
                        }),
                    )]],
                    fail_once: None,
                },
                Plan {
                    tag: "read-state",
                    steps: vec![vec![call(
                        "read-state",
                        "read",
                        json!({ "path": "kept.txt" }),
                    )]],
                    fail_once: None,
                },
            ];
            let mut world = World::new(0, &FILES, service(plans)).map_err(failed)?;
            let first = world
                .incarnation(None, &[vec![("write-state", tag("write-state"))]])
                .await
                .map_err(failed)?;
            if !matches!(first.prompts.as_slice(), [(Prompted::Settled(_), _)]) {
                return Err(format!(
                    "first incarnation did not settle: {:?}",
                    first.prompts
                ));
            }
            let second = world
                .incarnation(None, &[vec![("read-state", tag("read-state"))]])
                .await
                .map_err(failed)?;
            if !matches!(second.prompts.as_slice(), [(Prompted::Settled(_), _)]) {
                return Err(format!(
                    "second incarnation did not reopen the same state: {:?}",
                    second.prompts
                ));
            }
            Ok(())
        })
    })
}

#[test]
fn seed_1000_maps_to_a_world() -> std::result::Result<(), String> {
    let class = class(1000);
    if class != "A2" {
        return Err(format!("X1 seed 1000 mapped to {class}, not A2"));
    }
    let world_seed = 1000 % WORLDS.len() as u64;
    oracle::simulate(1000, || {
        entry(async move {
            // A single replay proves the mapping reaches a world without the
            // index panic; the default seeds own the in-process comparison.
            trace(world_seed, 1000, class, false).await?;
            Ok(())
        })
    })
}
