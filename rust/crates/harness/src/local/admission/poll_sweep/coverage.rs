//! Boundary coverage derived from each run: required kinds, paired hooks,
//! and a cut at every recorded boundary.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn permute<T>(pairs: &mut [T], seed: u64) {
    let mut random = blake3::Hasher::new_derive_key("acyclic.harness.test.cut-order.v1");
    random.update(&seed.to_le_bytes());
    let mut random = random.finalize_xof();
    for upper in (1..pairs.len()).rev() {
        let mut draw = [0; 8];
        random.fill(&mut draw);
        pairs.swap(
            upper,
            (u64::from_le_bytes(draw) % (upper as u64 + 1)) as usize,
        );
    }
}

pub(super) fn check_interleaved(order: impl IntoIterator<Item = Scenario>) -> Result<()> {
    let mut runs = BTreeMap::new();
    let mut previous = None;
    for scenario in order {
        if previous != Some(scenario) {
            *runs.entry(scenario as u8).or_insert(0) += 1;
            previous = Some(scenario);
        }
    }
    if runs.len() != 3 || runs.values().any(|runs| *runs < 2) {
        return Err(Error::Storage(
            "the matrix still groups cuts by scenario".into(),
        ));
    }
    Ok(())
}

/// Opening and closing boundary kinds. Each closer follows its own opener, so
/// at every prefix of a run the closers never outnumber their openers, and the
/// two counts are equal once the run ends. A hook omitted on either side breaks
/// the pairing however many boundaries the run crosses.
const PAIRS: [(&str, &str); 6] = [
    ("blocking:submitted", "blocking:completed"),
    ("native:submitted", "native:completed"),
    ("task:submitted", "task:completed"),
    ("stream:read:blocked", "stream:read:completed"),
    ("model:dispatch", "model:accepted"),
    ("tool:dispatch", "tool:accepted"),
];

/// The boundary kinds each scenario must reach. They are what the scenario is
/// for (a model turn, a tool effect, a blocked read), not how often an
/// implementation happens to cross them.
fn required(scenario: Scenario) -> impl Iterator<Item = &'static str> {
    let tool = scenario == Scenario::EffectAndContinuation;
    PAIRS
        .into_iter()
        .filter(move |(open, _)| tool || !open.starts_with("tool:"))
        .flat_map(|(open, close)| [open, close])
}

/// A fixture manifest for the permutation tests; the real sweep checks the
/// interleaving of its own recorded cuts.
fn manifest_pairs() -> Vec<(Scenario, usize)> {
    [
        (Scenario::Text, 300),
        (Scenario::EffectAndContinuation, 800),
        (Scenario::Compaction, 600),
    ]
    .into_iter()
    .flat_map(|(scenario, total)| (1..=total).map(move |cut| (scenario, cut)))
    .collect()
}

#[test]
fn seed_711_interleaves_the_manifest_and_rejects_the_grouped_control() -> Result<()> {
    let mut pairs = manifest_pairs();
    assert!(check_interleaved(pairs.iter().map(|(scenario, _)| *scenario)).is_err());
    permute(&mut pairs, 711);
    check_interleaved(pairs.into_iter().map(|(scenario, _)| scenario))
}

/// Checks a run's recorded boundaries against what the run itself implies:
/// every kind the scenario exists to exercise was reached, and every opening
/// boundary is closed by its own kind, in order.
pub(super) fn check_boundaries(scenario: Scenario, actual: &[&str]) -> Result<()> {
    let seen: BTreeSet<&str> = actual.iter().copied().collect();
    let missing: Vec<_> = required(scenario)
        .filter(|kind| !seen.contains(kind))
        .collect();
    if !missing.is_empty() {
        return Err(Error::Storage(format!(
            "{scenario:?} never reached boundary kinds {missing:?}"
        )));
    }
    for (open, close) in PAIRS {
        let mut depth = 0usize;
        for (index, name) in actual.iter().enumerate() {
            if *name == open {
                depth += 1;
            } else if *name == close {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    Error::Storage(format!(
                        "{scenario:?} boundary {} is {close} without an open {open}",
                        index + 1
                    ))
                })?;
            }
        }
        if depth != 0 {
            return Err(Error::Storage(format!(
                "{scenario:?} left {depth} {open} boundaries without {close}"
            )));
        }
    }
    Ok(())
}

/// Every recorded boundary is a cancellation point exactly once.
pub(super) fn check_cuts(recorded: usize, cuts: impl IntoIterator<Item = usize>) -> Result<()> {
    let cuts: Vec<_> = cuts.into_iter().collect();
    if recorded == 0
        || cuts.len() != recorded
        || cuts.into_iter().collect::<BTreeSet<_>>() != (1..=recorded).collect()
    {
        return Err(Error::Storage(
            "the real sweep omitted or duplicated a required cut".into(),
        ));
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn broken_real_sweep_cannot_drop_or_repeat_a_cut(total in 1usize..4096, omitted in 0usize..4096) {
        let omitted = 1 + omitted % total;
        proptest::prop_assert!(check_cuts(total, 1..=total).is_ok());
        // The replay planner deliberately missing a real cut, or repeating another in its place.
        proptest::prop_assert!(check_cuts(total, (1..=total).filter(|cut| *cut != omitted)).is_err());
        if total > 1 {
            let repeated = if omitted == 1 { 2 } else { 1 };
            let cuts: Vec<_> = (1..=total).map(|cut| if cut == omitted { repeated } else { cut }).collect();
            proptest::prop_assert!(check_cuts(total, cuts).is_err());
        }
    }

    #[test]
    fn seeded_permutation_preserves_every_manifest_pair(seed in proptest::prelude::any::<u64>()) {
        let expected = manifest_pairs();
        let mut first = expected.clone();
        let mut replay = expected.clone();
        permute(&mut first, seed);
        permute(&mut replay, seed);
        proptest::prop_assert_eq!(&first, &replay);
        first.sort_unstable_by_key(|(scenario, cut)| (*scenario as u8, *cut));
        proptest::prop_assert_eq!(first, expected);
    }
}

#[test]
fn omitted_actual_hook_and_cut_are_rejected() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let observed = runtime.block_on(case(Scenario::Text, None, &[], None))?;
    let actual = observed.points;
    check_boundaries(Scenario::Text, &actual)?;
    // Each real hook kind omitted once, at its first occurrence.
    for kind in required(Scenario::Text) {
        let omitted = actual.iter().position(|name| *name == kind).unwrap();
        let mut broken = actual.clone();
        broken.remove(omitted);
        assert!(
            check_boundaries(Scenario::Text, &broken).is_err(),
            "missing real {kind} hook went undetected"
        );
    }
    // A run that never reaches a kind its scenario exists to exercise.
    let unreached: Vec<_> = actual
        .iter()
        .copied()
        .filter(|name| !name.starts_with("model:"))
        .collect();
    assert!(
        check_boundaries(Scenario::Text, &unreached).is_err(),
        "unreached model turn went undetected"
    );
    let omitted = actual
        .iter()
        .position(|name| *name == "task:submitted")
        .unwrap();
    assert!(
        check_cuts(
            actual.len(),
            (1..=actual.len()).filter(|cut| *cut != omitted + 1)
        )
        .is_err(),
        "missing real replay went undetected"
    );
    Ok(())
}
