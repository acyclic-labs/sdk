//! Publication: every committed version of a finding file is visible through
//! the same live directory and range queries a client uses, at every moment,
//! once per current path, after any writer and after a reopen.
use super::*;
use family::{Generator, Variant};
use oracle::{Plan, Prompted, World, call, done, failed, service, simulate, tag};
use serde_json::json;

/// One root world per writer: write a new finding, edit a committed one, or
/// remove one, then fold the current committed set. The fold is the same live
/// directory/range query a client uses, so it sees committed versions only and
/// never waits for the subtree to settle.
pub(super) const GENERATOR: Generator = Generator {
    class: "I1",
    seeds: a_finding_file_is_published_once_by_every_writer::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = a_finding_file_is_published_once_by_every_writer,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

/// The writer this seed drives.
fn writer(seed: u64) -> &'static str {
    ["write", "edit", "remove"]
        .get(usize::try_from(seed % 3).unwrap_or(0))
        .copied()
        .unwrap_or("write")
}

fn variants(seed: u64) -> Vec<Variant> {
    vec![Variant::from([
        ("writer", writer(seed)),
        ("form", "relative"),
        ("agent_state", "settled"),
        ("topology", "root"),
    ])]
}

fn plan(seed: u64) -> Plan {
    let step = match writer(seed) {
        "write" => call(
            "i1-write",
            "write",
            json!({"path": "findings/b.md", "content": "keep\n", "ifVersion": "absent"}),
        ),
        "edit" => call(
            "i1-edit",
            "edit",
            json!({"path": "findings/a.md", "old_text": "first", "new_text": "second"}),
        ),
        _ => call(
            "i1-remove",
            "remove",
            json!({"path": "findings/remove.md", "ifVersion": "any"}),
        ),
    };
    Plan {
        tag: "i1",
        steps: vec![vec![step]],
        fail_once: None,
    }
}

/// A finding longer than several read ranges, which the fold must read to its
/// whole length.
fn large() -> String {
    "large finding\n".repeat(8 * 1024)
}

/// The fold the writer's committed version leaves behind.
fn expected(seed: u64) -> Vec<(String, Vec<u8>)> {
    let files: &[(&str, &[u8])] = match writer(seed) {
        "write" => &[
            ("a.md", b"first\n"),
            ("b.md", b"keep\n"),
            ("remove.md", b"remove\n"),
        ],
        "edit" => &[("a.md", b"second\n"), ("remove.md", b"remove\n")],
        _ => &[("a.md", b"first\n")],
    };
    let mut files: Vec<_> = files
        .iter()
        .map(|(name, body)| ((*name).to_owned(), body.to_vec()))
        .chain([("large.md".to_owned(), large().into_bytes())])
        .collect();
    files.sort();
    files
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    simulate(seed, move || {
        entry(async move {
            let large = large();
            let mut world = World::new(
                seed,
                &[
                    ("findings/a.md", "first\n"),
                    ("findings/large.md", &large),
                    ("findings/remove.md", "remove\n"),
                ],
                service(vec![plan(seed)]),
            )
            .map_err(failed)?;
            let incarnation = world
                .incarnation(None, &[vec![("i1", tag("i1"))]])
                .await
                .map_err(failed)?;
            if !matches!(
                incarnation.prompts.as_slice(),
                [(Prompted::Settled(text), _)] if *text == done("i1")
            ) {
                return Err(format!(
                    "i1 prompt did not settle: {:?}",
                    incarnation.prompts
                ));
            }
            // One entry per page, so every later page continues the listing.
            let findings = world.findings(1).await.map_err(failed)?;
            let expected = expected(seed);
            if findings != expected {
                return Err(format!("published findings {findings:?} != {expected:?}"));
            }
            Ok(true)
        })
    })
}
