//! Independent, reviewed boundary counts and complete cut plans.
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

fn manifest_pairs() -> Vec<(Scenario, usize)> {
    [
        Scenario::Text,
        Scenario::EffectAndContinuation,
        Scenario::Compaction,
    ]
    .into_iter()
    .flat_map(|scenario| {
        let total: usize = expected(scenario)
            .unwrap()
            .iter()
            .filter(|(name, _)| name.as_str() != "stream:read:ready")
            .map(|(_, count)| *count)
            .sum();
        (1..=total).map(move |cut| (scenario, cut))
    })
    .collect()
}

#[test]
fn seed_711_interleaves_the_actual_manifest_and_rejects_the_grouped_control() -> Result<()> {
    let mut pairs = manifest_pairs();
    assert!(check_interleaved(pairs.iter().map(|(scenario, _)| *scenario)).is_err());
    permute(&mut pairs, 711);
    check_interleaved(pairs.into_iter().map(|(scenario, _)| scenario))
}

fn expected(scenario: Scenario) -> Result<BTreeMap<String, usize>> {
    // Text/EffectAndContinuation/Compaction verify 14/36/27 retained files.
    // Reopening each already authenticated generation cost eight ready authority
    // reads. Reuse removes only those reads; content and descriptor checks remain.
    // Counts include HEAD-write verification now traced on main. Independent
    // old/new scenario traces derive the ready pins:
    // Text 2098 - 14*8 = 1986; Effect 5587 - 36*8 = 5299;
    // Compaction 3805 - 27*8 = 3589. Every other boundary count is unchanged.
    let bytes: &[u8] = match scenario {
        Scenario::Text => include_bytes!("coverage/text.json"),
        Scenario::EffectAndContinuation => include_bytes!("coverage/effect_goal.json"),
        Scenario::Compaction => include_bytes!("coverage/compaction.json"),
    };
    let mut expected: BTreeMap<String, usize> = serde_json::from_slice(bytes).map_err(storage)?;
    // The initial context now probes two optional skill roots once per project
    // generation. Derive that incremental cost from independent filesystem
    // operations; never copy a count from the run being checked.
    *expected.get_mut("stream:read:ready").unwrap() += skill_reads::cold_root_reads(scenario)?;
    Ok(expected)
}

pub(super) fn check_boundaries(
    scenario: Scenario,
    actual: &[&str],
    ready_reads: usize,
) -> Result<()> {
    let mut counts = BTreeMap::new();
    for name in actual {
        *counts.entry((*name).to_owned()).or_insert(0) += 1;
    }
    counts.insert("stream:read:ready".into(), ready_reads);
    let expected = expected(scenario)?;
    if counts != expected {
        return Err(Error::Storage(format!(
            "{scenario:?} independent boundary coverage: expected {expected:?}, actual {counts:?}"
        )));
    }
    Ok(())
}

pub(super) fn check_cuts(
    scenario: Scenario,
    recorded: usize,
    cuts: impl IntoIterator<Item = usize>,
) -> Result<()> {
    let total: usize = expected(scenario)?
        .iter()
        .filter(|(name, _)| name.as_str() != "stream:read:ready")
        .map(|(_, count)| *count)
        .sum();
    let cuts: Vec<_> = cuts.into_iter().collect();
    if recorded != total
        || cuts.len() != total
        || cuts.into_iter().collect::<BTreeSet<_>>() != (1..=total).collect()
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
    fn broken_real_sweep_cannot_drop_a_cut(which in 0u8..3, omitted in 0usize..4096) {
        let scenario = match which { 0 => Scenario::Text, 1 => Scenario::EffectAndContinuation, _ => Scenario::Compaction };
        let total = expected(scenario).unwrap().iter().filter(|(name, _)| name.as_str() != "stream:read:ready").map(|(_, count)| *count).sum::<usize>();
        let omitted = 1 + omitted % total;
        // This is the actual replay planner, deliberately missing a real cut.
        proptest::prop_assert!(check_cuts(scenario, total, (1..=total).filter(|cut| *cut != omitted)).is_err());
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
    check_boundaries(Scenario::Text, &actual, observed.ready_reads)?;
    let omitted = actual
        .iter()
        .position(|name| *name == "task:submitted")
        .unwrap();
    let mut broken = actual.clone();
    broken.remove(omitted);
    assert!(
        check_boundaries(Scenario::Text, &broken, observed.ready_reads).is_err(),
        "missing real hook went undetected"
    );
    assert!(
        check_cuts(
            Scenario::Text,
            actual.len(),
            (1..=actual.len()).filter(|cut| *cut != omitted + 1)
        )
        .is_err(),
        "missing real replay went undetected"
    );
    Ok(())
}
