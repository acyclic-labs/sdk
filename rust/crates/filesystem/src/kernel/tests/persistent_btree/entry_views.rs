use super::*;
use crate::{CancellationToken, WorkBudget};
use proptest::prelude::*;

fn same_selection(
    mut actual: Iter<'_, u8>,
    values: &[u8],
    selections: &[(bool, usize)],
) -> proptest::test_runner::TestCaseResult {
    let mut expected = values.iter();
    for &(back, skip) in selections {
        let (actual_value, expected_value) = if back {
            (actual.nth_back(skip), expected.nth_back(skip))
        } else {
            (actual.nth(skip), expected.nth(skip))
        };
        prop_assert_eq!(actual_value, expected_value);
        prop_assert_eq!(actual.len(), expected.len());
        prop_assert_eq!(actual.size_hint(), expected.size_hint());
        prop_assert_eq!(actual.clone().count(), expected.clone().count());
        prop_assert_eq!(actual.clone().last(), expected.clone().last());
    }
    prop_assert_eq!(
        actual.copied().collect::<Vec<_>>(),
        expected.copied().collect::<Vec<_>>()
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn borrowed_views_preserve_slice_selection_and_retire_standalone_funding(
        values in prop::collection::vec(any::<u8>(), 0..65),
        selections in prop::collection::vec(
            (any::<bool>(), prop_oneof![0_usize..80, Just(usize::MAX)]), 0..17,
        ),
    ) {
        same_selection(Entries::Plain(&values).iter(), &values, &selections)?;
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut owned = Owned::allocate(values.len(), &mut accounting).unwrap();
        owned.value.extend_from_slice(&values);
        let (shared, logical_bytes) = share_values(owned, |_| 0, &mut accounting).unwrap();
        prop_assert_eq!(shared.bytes, logical_bytes);
        same_selection(Entries::Shared(&shared.value).iter(), &values, &selections)?;
        shared.retire(&mut accounting).unwrap();
        prop_assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    }

    #[test]
    fn shared_partitions_preserve_slice_results_probes_and_charges(
        mut values in prop::collection::vec(any::<u8>(), 0..65),
        cursor in any::<u8>(),
        inclusive in any::<bool>(),
        start in 0_usize..70,
        end in 0_usize..70,
    ) {
        values.sort_unstable();
        let cancellation = CancellationToken::new();
        let mut baseline = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut expected_probes = Vec::new();
        let expected = crate::kernel::search::partition_point_with_accounting(
            &values, &mut baseline, |value| {
                expected_probes.push(*value);
                *value < cursor || (inclusive && *value == cursor)
            },
        ).unwrap();
        let mut candidate = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut owned = Owned::allocate(values.len(), &mut candidate).unwrap();
        owned.value.extend_from_slice(&values);
        let (shared, _) = share_values(owned, |_| 0, &mut candidate).unwrap();
        let before = candidate.spent();
        let mut actual_probes = Vec::new();
        let actual = Entries::Shared(&shared.value).partition(&mut candidate, |value| {
            actual_probes.push(*value);
            *value < cursor || (inclusive && *value == cursor)
        }).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(actual_probes, expected_probes);
        prop_assert_eq!(candidate.spent().items_examined - before.items_examined,
            baseline.spent().items_examined);
        prop_assert_eq!(Entries::Shared(&shared.value).slice(start..end)
            .map(|view| view.iter().copied().collect::<Vec<_>>()),
            values.get(start..end).map(|slice| slice.to_vec()));
        let mut baseline = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut expected_probes = Vec::new();
        let expected = crate::kernel::search::binary_search_with_accounting(
            &values, &mut baseline, |value| {
                expected_probes.push(*value);
                value.cmp(&cursor)
            },
        ).unwrap();
        let before = candidate.spent();
        let mut actual_probes = Vec::new();
        let actual = Entries::Shared(&shared.value).search(0, &mut candidate, |value| {
            actual_probes.push(*value);
            value.cmp(&cursor)
        }).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(actual_probes, expected_probes);
        prop_assert_eq!(candidate.spent().items_examined - before.items_examined,
            baseline.spent().items_examined);
        shared.retire(&mut candidate).unwrap();
        prop_assert_eq!(candidate.standalone_parts().unwrap().0.live_bytes(), 0);
    }
}
