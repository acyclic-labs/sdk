//! Determinism: every class world replays from its seed to the same trace,
//! and erasing the scheduler's entropy source changes that trace.
use super::*;
use family::{Generator, Variant};
use oracle::failed;

/// The X1 world dimension, in `classes.json` order.
const WORLDS: [&str; 5] = ["A1", "E2", "F1", "I1", "R1"];

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
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn variants(seed: u64) -> Vec<Variant> {
    let class = WORLDS[usize::try_from(seed % WORLDS.len() as u64).unwrap_or(0)];
    ["same_process", "cross_process", "entropy_erased"]
        .into_iter()
        .map(|run| Variant::from([("run", run), ("world", class)]))
        .collect()
}

/// Replays the class's own generator world and hashes every durable byte it
/// retired. The generator owns its runtime, so this is not nested in one.
fn replay_generator(seed: u64, class: &'static str) -> std::result::Result<[u8; 32], String> {
    let generator = family::GENERATORS
        .iter()
        .find(|generator| generator.class == class)
        .ok_or_else(|| format!("X1 class {class} has no generator"))?;
    oracle::reset_world_digests();
    (generator.world)(seed)?;
    let digests = oracle::take_world_digests();
    if digests.is_empty() {
        return Err(format!("X1 {class} seed {seed} retired no world"));
    }
    let mut hasher = blake3::Hasher::new();
    for digest in digests {
        hasher.update(&digest);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Replays the same class world in a separate process and returns its digest.
fn cross_process_digest(seed: u64, class: &'static str) -> std::result::Result<[u8; 32], String> {
    let executable = std::env::current_exe().map_err(failed)?;
    let key = std::thread::current().name().unwrap_or("dst").to_owned();
    let output = std::process::Command::new(executable)
        .args([
            "--exact",
            "dst::determinism::x1_cross_process_child",
            "--nocapture",
        ])
        .env("DST_X1_CHILD_SEED", seed.to_string())
        .env("DST_X1_CHILD_CLASS", class)
        .env("DST_WORLD_KEY", key)
        .output()
        .map_err(failed)?;
    if !output.status.success() {
        return Err(format!(
            "X1 {class} seed {seed} child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("DST_X1_CHILD_DIGEST="))
        .ok_or_else(|| format!("X1 {class} seed {seed} child printed no digest"))?;
    let digest = blake3::Hash::from_hex(line.trim()).map_err(failed)?;
    Ok(*digest.as_bytes())
}

/// The child half of the cross-process variant. The isolated runner also runs
/// it as a case; without the parent's environment it has nothing to replay.
#[test]
fn x1_cross_process_child() {
    let Some(seed) = std::env::var("DST_X1_CHILD_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
    else {
        return;
    };
    let Ok(class) = std::env::var("DST_X1_CHILD_CLASS") else {
        return;
    };
    let Some(class) = WORLDS.iter().find(|world| **world == class).copied() else {
        panic!("child class {class} is not a world");
    };
    let digest = replay_generator(seed, class).unwrap_or_else(|error| panic!("{error}"));
    println!("DST_X1_CHILD_DIGEST={}", hex(digest));
}

fn hex(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    // The world is the seed's class slot; the scheduler seed stays the full
    // requested seed so a deeper sweep can walk past the class count.
    let class = WORLDS[usize::try_from(seed % WORLDS.len() as u64).unwrap_or(0)];
    let first = replay_generator(seed, class)?;
    let second = replay_generator(seed, class)?;
    if first != second {
        return Err(format!(
            "X1 {class} seed {seed} diverged in-process: {} != {}",
            hex(first),
            hex(second)
        ));
    }
    let child = cross_process_digest(seed, class)?;
    if first != child {
        return Err(format!(
            "X1 {class} seed {seed} diverged across processes: {} != {}",
            hex(first),
            hex(child)
        ));
    }
    // The entropy-erased control replays the same class generator with the
    // scheduler's source erased; a digest that still matches proves the seed
    // never reached it.
    let erased = oracle::erase_entropy(|| replay_generator(seed, class))?;
    if first == erased {
        return Err(format!(
            "X1 {class} seed {seed} entropy erasure did not change the trace"
        ));
    }
    println!("DST_X1_DIGEST={class} {}", hex(first));
    Ok(true)
}
