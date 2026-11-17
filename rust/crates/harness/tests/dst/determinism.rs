//! Determinism: every class world replays from its seed to the same trace,
//! in this process and in a fresh one, and erasing the scheduler's entropy
//! source changes that trace.
use super::*;
use family::{GENERATORS, Generator, Variant};

/// The X1 world dimension, in `classes.json` order.
const WORLDS: [&str; 37] = [
    "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3", "C4", "C5", "D1", "D2", "D3", "D4", "D5",
    "D6", "E1", "E2", "E3", "F1", "F2", "G1", "H1", "H2", "H3", "H4", "H5", "H6", "I1", "I2", "J1",
    "J2", "O1", "R1", "S1", "T1",
];

/// One seed per class world. A world replays the class generator twice, then
/// the entropy-erased control, and a fresh process replays it once more.
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

/// The class world a schedule seed selects. The seed itself stays the
/// schedule seed, so a deeper sweep can run any class at any seed.
fn class(seed: u64) -> &'static str {
    let index = usize::try_from(seed % WORLDS.len() as u64).unwrap_or_default();
    WORLDS.get(index).copied().unwrap_or("A1")
}

fn generator(class: &str) -> std::result::Result<&'static Generator, String> {
    GENERATORS
        .iter()
        .copied()
        .find(|generator| generator.class == class)
        .ok_or_else(|| format!("X1 class {class} has no generator"))
}

fn variants(seed: u64) -> Vec<Variant> {
    let class = class(seed);
    ["same_process", "cross_process", "entropy_erased"]
        .into_iter()
        .map(|run| Variant::from([("run", run), ("world", class)]))
        .collect()
}

fn digest(trace: &[serde_json::Value]) -> blake3::Hash {
    blake3::hash(&serde_json::to_vec(trace).unwrap_or_default())
}

fn trace(
    seed: u64,
    erase_entropy: bool,
) -> std::result::Result<(bool, Vec<serde_json::Value>), String> {
    let generator = generator(class(seed))?;
    let run = || (generator.world)(seed);
    let (result, trace) = if erase_entropy {
        oracle::record(|| oracle::erase_entropy(run))
    } else {
        oracle::record(run)
    };
    Ok((result?, trace))
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

fn child_digest(seed: u64) -> std::result::Result<blake3::Hash, String> {
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let output = std::process::Command::new(executable)
        .args([
            "--exact",
            "dst::determinism::x1_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("DST_X1_CHILD", "1")
        .env("DST_X1_SEED", seed.to_string())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "fresh-process replay failed: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let marker = format!("DST_X1_DIGEST={} ", class(seed));
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(&marker))
        .ok_or_else(|| format!("fresh-process replay printed no digest: {stdout}"))
        .and_then(|digest| {
            blake3::Hash::from_hex(digest.trim()).map_err(|error| error.to_string())
        })
}

/// The fresh-process half of an X1 replay. The parent passes the schedule
/// seed; this process runs the same class world once and prints its digest.
#[test]
fn x1_child() {
    if std::env::var_os("DST_X1_CHILD").is_none() {
        return;
    }
    let seed = std::env::var("DST_X1_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or_default();
    world(seed).unwrap_or_else(|error| panic!("{error}"));
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let class = class(seed);
    if std::env::var_os("DST_X1_CHILD").is_some() {
        let (_, trace) = trace(seed, false)?;
        println!("DST_X1_DIGEST={class} {}", digest(&trace));
        return Ok(true);
    }
    let (reached, first) = trace(seed, false)?;
    let (_, second) = trace(seed, false)?;
    if digest(&first) != digest(&second) {
        return Err(format!(
            "X1 {class} seed {seed} diverged in-process at {}: {} != {}",
            first_difference(
                &serde_json::Value::Array(first.clone()),
                &serde_json::Value::Array(second.clone()),
            ),
            digest(&first),
            digest(&second),
        ));
    }
    let (_, erased) = trace(seed, true)?;
    if digest(&first) == digest(&erased) {
        return Err(format!(
            "X1 {class} seed {seed} entropy erasure did not change the trace"
        ));
    }
    let fresh = child_digest(seed)?;
    if fresh != digest(&first) {
        return Err(format!(
            "X1 {class} seed {seed} diverged in a fresh process: {} != {}",
            digest(&first),
            fresh,
        ));
    }
    println!("DST_X1_DIGEST={class} {}", digest(&first));
    Ok(reached)
}
