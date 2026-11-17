//! Determinism: every class world replays from its seed to the same trace,
//! and erasing the scheduler's entropy source changes that trace.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, World, call, failed, service, tag};
use serde_json::json;

/// The X1 world dimension, in `classes.json` order.
const WORLDS: [&str; 37] = [
    "A1",
    "A2",
    "A3",
    "B1",
    "B2",
    "B3",
    "C1",
    "C2",
    "C3",
    "C4",
    "C5",
    "D1",
    "D2",
    "D3",
    "D4",
    "D5",
    "D6",
    "E1",
    "E2",
    "E3",
    "F1",
    "F2",
    "G1",
    "H1",
    "H2",
    "H3",
    "H4",
    "H5",
    "H6",
    "I1",
    "I2",
    "J1",
    "J2",
    "O1",
    "R1",
    "S1",
    "T1",
];

const FILES: [(&str, &str); 1] = [("base.txt", "base\n")];

/// One seed per class world. A world runs the same production turn twice and
/// the entropy-erased control once, then reports all three X1 run variants.
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

fn variants(seed: u64) -> Vec<Variant> {
    let class = usize::try_from(seed)
        .ok()
        .and_then(|index| WORLDS.get(index).copied());
    let Some(class) = class else {
        return Vec::new();
    };
    ["same_process", "cross_process", "entropy_erased"]
        .into_iter()
        .map(|run| Variant::from([("run", run), ("world", class)]))
        .collect()
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
            call("x1-read", "read", json!({
                "path": "out.txt",
            })),
        ]],
        fail_once: None,
    }]
}

struct Replay {
    digest: blake3::Hash,
    trace: serde_json::Value,
}

async fn trace(
    seed: u64,
    class: &'static str,
    erase_entropy: bool,
) -> std::result::Result<Replay, String> {
    let mut world = if erase_entropy {
        World::without_entropy(seed, &FILES, service(plans(class)))
    } else {
        World::new(seed, &FILES, service(plans(class)))
    }
    .map_err(failed)?;
    let prompts = [vec![(class, tag(class))]];
    let incarnation = world
        .incarnation(None, &prompts)
        .await
        .map_err(failed)?;
    // Every replayed input stays in the digest, including the exact durable
    // count at each dispatch. The count after the last dispatch is not a replay
    // input: independent host stores finish their own writes after the model
    // has settled, and their interleaving is not the seed's to reproduce.
    let trace = serde_json::json!({
        "prompts": format!("{:?}", incarnation.prompts),
        "requests": serde_json::to_value(&incarnation.requests).map_err(failed)?,
        "dispatched_at": incarnation.dispatched_at,
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
                let keys = left.keys().chain(right.keys()).collect::<std::collections::BTreeSet<_>>();
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
    let class = usize::try_from(seed)
        .ok()
        .and_then(|index| WORLDS.get(index).copied())
        .ok_or_else(|| format!("X1 seed {seed} names no world"))?;
    oracle::simulate(seed, || {
        entry(async move {
            let first = trace(seed, class, false).await?;
            let second = trace(seed, class, false).await?;
            if first.digest != second.digest {
                return Err(format!(
                    "X1 {class} seed {seed} diverged in-process at {}: {} != {}",
                    first_difference(&first.trace, &second.trace),
                    first.digest,
                    second.digest,
                ));
            }
            let erased = trace(seed, class, true).await?;
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
