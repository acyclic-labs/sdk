//! Determinism: every class world replays from its seed to the same trace,
//! and erasing the scheduler's entropy source changes that trace.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, World, call, failed, service, tag};
use serde_json::json;

/// The X1 world dimension, in `classes.json` order.
const WORLDS: [&str; 8] = ["A1", "D6", "E2", "F1", "G1", "H2", "I1", "R1"];

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

async fn trace(
    seed: u64,
    class: &'static str,
    erase_entropy: bool,
) -> std::result::Result<blake3::Hash, String> {
    let mut world = if erase_entropy {
        World::without_entropy(seed, &FILES, service(plans(class)))
    } else {
        World::new(seed, &FILES, service(plans(class)))
    }
    .map_err(failed)?;
    let prompts = [vec![(class, tag(class))]];
    let incarnation = world.incarnation(None, &prompts).await.map_err(failed)?;
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
    Ok(blake3::hash(&serde_json::to_vec(&trace).map_err(failed)?))
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
    let class = std::env::var("DST_X1_CHILD_CLASS").expect("child class");
    let class = WORLDS
        .iter()
        .find(|world| **world == class)
        .copied()
        .expect("child class is a world");
    let digest = replay_generator(seed, class).expect("child replay");
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
    // The entropy-erased control replays the same production turn with the
    // scheduler's source erased; a trace that still matches proves the seed
    // never reached it.
    oracle::simulate(seed, || {
        entry(async move {
            let on = trace(seed, class, false).await?;
            let off = trace(seed, class, true).await?;
            if on == off {
                return Err(format!(
                    "X1 {class} seed {seed} entropy erasure did not change the trace"
                ));
            }
            Ok(())
        })
    })?;
    println!("DST_X1_DIGEST={class} {}", hex(first));
    Ok(true)
}
