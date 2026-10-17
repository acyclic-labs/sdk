use super::subset;
use proptest::prelude::*;
use std::{
    cell::Cell,
    hash::{Hash, Hasher},
};

struct Counted<'a> {
    value: &'a str,
    comparisons: &'a Cell<usize>,
}

impl PartialEq for Counted<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.comparisons.set(self.comparisons.get() + 1);
        self.value == other.value
    }
}
impl Eq for Counted<'_> {}
impl Hash for Counted<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

#[test]
fn inherited_claim_membership_uses_linear_comparison_work() {
    for count in [16, 256, 4096, 10_000] {
        let values = (0..count)
            .map(|i| format!("inherited-group-{i}"))
            .collect::<Vec<_>>();
        let comparisons = Cell::new(0);
        let claims = || {
            values.iter().map(|value| Counted {
                value,
                comparisons: &comparisons,
            })
        };
        assert!(subset(claims(), claims().rev()));
        assert!(
            comparisons.get() <= 16 * count,
            "{} comparisons for {count} claims",
            comparisons.get()
        );
    }
}

#[test]
fn linear_work_bound_detects_the_quadratic_scan() {
    let count = 1024;
    let values = (0..count)
        .map(|i| format!("inherited-group-{i}"))
        .collect::<Vec<_>>();
    let comparisons = Cell::new(0);
    let claims = values
        .iter()
        .map(|value| Counted {
            value,
            comparisons: &comparisons,
        })
        .collect::<Vec<_>>();
    assert!(claims.iter().rev().all(|claim| claims.contains(claim)));
    assert!(comparisons.get() > 16 * count);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn borrowed_membership_matches_subset_definition(
        parent in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 0..64),
        child in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 0..64),
        keep in prop::collection::vec(any::<bool>(), 0..64),
    ) {
        prop_assert_eq!(subset(parent.iter().map(Vec::as_slice), child.iter().map(Vec::as_slice)), child.iter().all(|claim| parent.contains(claim)));
        let inherited = parent.iter().zip(keep).filter_map(|(claim, keep)| keep.then_some(claim.as_slice()));
        prop_assert!(subset(parent.iter().map(Vec::as_slice), inherited));
    }
}
