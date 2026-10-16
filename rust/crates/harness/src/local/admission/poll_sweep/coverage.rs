//! Independent, reviewed boundary counts and complete cut plans.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn expected(scenario: Scenario) -> Result<BTreeMap<String, usize>> {
    let name = match scenario {
        Scenario::Text => "text",
        Scenario::EffectAndContinuation => "effect_goal",
    };
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "src/local/admission/poll_sweep/coverage/{name}.json"
    ));
    serde_json::from_slice(&std::fs::read(path).map_err(storage)?).map_err(storage)
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
    fn broken_real_sweep_cannot_drop_a_cut(which in proptest::bool::ANY, omitted in 0usize..4096) {
        let scenario = if which { Scenario::Text } else { Scenario::EffectAndContinuation };
        let total = expected(scenario).unwrap().iter().filter(|(name, _)| name.as_str() != "stream:read:ready").map(|(_, count)| *count).sum::<usize>();
        let omitted = 1 + omitted % total;
        // This is the actual replay planner, deliberately missing a real cut.
        proptest::prop_assert!(check_cuts(scenario, total, (1..=total).filter(|cut| *cut != omitted)).is_err());
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
