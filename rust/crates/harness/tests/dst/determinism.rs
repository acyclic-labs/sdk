//! Determinism: every class world that owns a replay trace produces the same
//! trace in a fresh process, and erasing the scheduler's entropy source
//! changes that trace.
use super::*;
use family::{GENERATORS, Generator, Variant};

/// The X1 world dimension, limited to the class generators that drive the
/// shared world harness and therefore produce a replay trace.
const WORLDS: [&str; 5] = ["A1", "E2", "F1", "I1", "R1"];

/// One seed per replayed class world. Each world runs the class generator in
/// two fresh processes, one seeded and one entropy-erased.
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
    ["cross_process", "entropy_erased"]
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

struct Child {
    reached: bool,
    digest: blake3::Hash,
}

fn child(seed: u64, erase_entropy: bool) -> std::result::Result<Child, String> {
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
        .env("DST_X1_ERASE", if erase_entropy { "1" } else { "0" })
        .env(
            "DST_X1_THREAD",
            std::thread::current().name().unwrap_or("dst"),
        )
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "fresh-process replay failed: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ));
    }
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let marker = format!("DST_X1_DIGEST={} ", class(seed));
    let digest = output
        .lines()
        .find_map(|line| {
            line.find(&marker)
                .map(|index| &line[index + marker.len()..])
        })
        .ok_or_else(|| format!("fresh-process replay printed no digest: {output}"))
        .and_then(|digest| {
            blake3::Hash::from_hex(digest.trim()).map_err(|error| error.to_string())
        })?;
    let reached = output
        .lines()
        .any(|line| line.contains("DST_X1_REACHED=1"));
    Ok(Child { reached, digest })
}

/// The fresh-process half of an X1 replay. The parent passes the schedule
/// seed and whether to erase entropy; this process runs the class world once
/// and prints its digest.
#[test]
fn x1_child() {
    if std::env::var_os("DST_X1_CHILD").is_none() {
        return;
    }
    let seed = std::env::var("DST_X1_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or_default();
    let erase = std::env::var("DST_X1_ERASE").is_ok_and(|erase| erase == "1");
    let (reached, trace) = trace(seed, erase).unwrap_or_else(|error| panic!("{error}"));
    println!(
        "DST_X1_DIGEST={} {}",
        class(seed),
        digest(&trace)
    );
    if reached {
        println!("DST_X1_REACHED=1");
    }
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let class = class(seed);
    let first = child(seed, false)?;
    let second = child(seed, false)?;
    if first.digest != second.digest {
        return Err(format!(
            "X1 {class} seed {seed} diverged in a fresh process: {} != {}",
            first.digest, second.digest,
        ));
    }
    let erased = child(seed, true)?;
    if first.digest == erased.digest {
        return Err(format!(
            "X1 {class} seed {seed} entropy erasure did not change the trace"
        ));
    }
    println!("DST_X1_DIGEST={class} {}", first.digest);
    Ok(first.reached)
}
