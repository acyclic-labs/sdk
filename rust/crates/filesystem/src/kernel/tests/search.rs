use super::*;
use proptest::prelude::*;

fn sorted() -> impl Strategy<Value = Vec<u16>> {
    prop::collection::vec(0_u16..64, 0..80).prop_map(|mut items| {
        items.sort_unstable();
        items
    })
}

fn legacy_probe_indices(items: &[u16], key: u16) -> Vec<usize> {
    // Frozen pre-PR decision tree, independent of production search/accounting.
    let (mut low, mut high) = (0, items.len());
    let mut probes = Vec::new();
    while low < high {
        let middle = low + (high - low) / 2;
        probes.push(middle);
        match items[middle].cmp(&key) {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => break,
        }
    }
    probes
}

fn compare_known_prefix<const SKIP_FIRST: bool, const UNCHARGED_PROBE: bool>(
    items: &[u16],
    key: u16,
    floor: usize,
) -> proptest::test_runner::TestCaseResult {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let mut calls = 0;
    let mut actual = Vec::new();
    let mut compare = |item: &u16| {
        calls += 1;
        actual.push(
            items
                .iter()
                .position(|slot| std::ptr::eq(slot, item))
                .unwrap(),
        );
        item.cmp(&key)
    };
    if UNCHARGED_PROBE && let Some(first) = items.first() {
        compare(first);
    }
    let offset = floor + usize::from(SKIP_FIRST && floor < items.len());
    let found = binary_search_from_with_accounting(items, offset, &mut accounting, compare)
        .map_err(|error| TestCaseError::fail(error.to_string()))?;
    prop_assert_eq!(
        accounting.spent().items_examined,
        calls,
        "charge every actual comparison"
    );
    let mut baseline = Vec::new();
    let mut baseline_accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let _ = binary_search_with_accounting(items, &mut baseline_accounting, |item| {
        baseline.push(
            items
                .iter()
                .position(|slot| std::ptr::eq(slot, item))
                .unwrap(),
        );
        item.cmp(&key)
    })
    .unwrap();
    let legacy = legacy_probe_indices(items, key);
    prop_assert_eq!(
        &baseline,
        &legacy,
        "unoptimized traversal retains the old search"
    );
    let expected: Vec<_> = legacy
        .into_iter()
        .filter(|index| *index >= offset)
        .collect();
    prop_assert_eq!(
        actual,
        expected,
        "actual probes are exactly the unknown baseline probes"
    );
    match (found, items.binary_search(&key)) {
        (Ok(index), Ok(_)) => prop_assert_eq!(items.get(index), Some(&key)),
        (Err(index), Err(expected)) => prop_assert_eq!(index, expected),
        (found, expected) => prop_assert!(false, "{found:?} != {expected:?}"),
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn known_prefix_matches_std_and_charges_every_comparison(items in sorted(), key in 0_u16..70, floor in any::<usize>()) {
        let floor = floor % (items.partition_point(|item| *item < key) + 1);
        compare_known_prefix::<false, false>(&items, key, floor)?;
    }

    #[test]
    fn binary_search_matches_std(items in sorted(), key in 0_u16..70) {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let found = binary_search_with_accounting(&items, &mut accounting, |item| item.cmp(&key))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        match (found, items.binary_search(&key)) {
            (Ok(found), Ok(_)) => prop_assert_eq!(items.get(found), Some(&key)),
            (Err(counted), Err(expected)) => prop_assert_eq!(counted, expected),
            (counted, expected) => prop_assert!(false, "{counted:?} != {expected:?}"),
        }
    }

    #[test]
    fn partition_point_matches_std(
        items in sorted(),
        key in 0_u16..70,
        inclusive in any::<bool>(),
    ) {
        let before = |item: &u16| if inclusive { *item <= key } else { *item < key };
        let cancellation = crate::CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let index = partition_point_with_accounting(&items, &mut accounting, before)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(index, items.partition_point(before));
    }
}

#[test]
fn known_prefix_oracle_detects_a_skipped_slot_and_an_uncharged_comparison() {
    compare_known_prefix::<false, false>(&[1, 2, 3], 1, 0).unwrap();
    assert!(compare_known_prefix::<true, false>(&[1, 2, 3], 1, 0).is_err());
    let error = compare_known_prefix::<false, true>(&[1, 2, 3], 1, 0).unwrap_err();
    assert!(error.to_string().contains("charge every actual comparison"));
}

#[test]
fn empty_known_prefix_retains_admission_without_comparisons() -> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNCHARGED,
        &cancellation,
    );
    let mut calls = 0;
    assert_eq!(
        binary_search_from_with_accounting::<u8>(&[], 0, &mut accounting, |_| {
            calls += 1;
            Ordering::Equal
        })?,
        Err(0)
    );
    cancellation.cancel();
    assert!(
        binary_search_from_with_accounting::<u8>(&[], 0, &mut accounting, |_| {
            calls += 1;
            Ordering::Equal
        })
        .is_err()
    );
    assert_eq!(accounting.spent(), crate::WorkCounters::UNCHARGED);
    assert_eq!(calls, 0);
    Ok(())
}

#[test]
fn known_prefix_denies_each_probe_before_its_comparator() -> Result<(), crate::FsError> {
    let items: Vec<_> = (0..64).collect();
    for key in 0..=64 {
        for floor in 0..=key {
            let cancellation = crate::CancellationToken::new();
            let mut unlimited = crate::kernel::allocation::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let expected = binary_search_from_with_accounting(
                &items,
                floor as usize,
                &mut unlimited,
                |item| item.cmp(&key),
            )?;
            let required = unlimited.spent().items_examined;
            for budget in 0..=required {
                let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
                    crate::WorkBudget {
                        items_examined: budget,
                        ..crate::WorkBudget::UNBOUNDED
                    },
                    &cancellation,
                );
                let mut calls = 0;
                let found = binary_search_from_with_accounting(
                    &items,
                    floor as usize,
                    &mut accounting,
                    |item| {
                        calls += 1;
                        item.cmp(&key)
                    },
                );
                assert_eq!(calls, budget);
                assert_eq!(accounting.spent().items_examined, calls);
                if budget == required {
                    assert_eq!(found?, expected);
                } else {
                    assert!(found.is_err());
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_known_prefix_preserves_completed_comparisons_on_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let mut calls = 0;
    let found = binary_search_from_with_accounting(&[1, 2, 3], 0, &mut accounting, |item| {
        calls += 1;
        cancellation.cancel();
        item.cmp(&3)
    });
    assert!(found.is_err());
    assert_eq!(calls, 1);
    assert_eq!(original.receipt().items_examined, calls);
    assert!(
        binary_search_from_with_accounting::<u8>(&[], 0, &mut accounting, |_| {
            calls += 1;
            Ordering::Equal
        })
        .is_err()
    );
    assert_eq!(calls, 1);
    Ok(())
}

#[test]
fn accounting_denies_a_probe_before_calling_its_comparator() -> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget {
            items_examined: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let mut calls = 0;
    let result = binary_search_with_accounting(&[1, 2, 3], &mut accounting, |item| {
        calls += 1;
        item.cmp(&1)
    });
    assert!(result.is_err());
    assert_eq!(calls, 1);
    assert_eq!(accounting.spent().items_examined, 1);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_search_keeps_completed_probes_when_the_next_probe_is_cancelled()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let mut calls = 0;
    let result = binary_search_with_accounting(&[1, 2, 3], &mut accounting, |item| {
        calls += 1;
        cancellation.cancel();
        item.cmp(&1)
    });
    assert!(result.is_err());
    assert_eq!(calls, 1);
    assert_eq!(original.receipt().items_examined, 1);
    Ok(())
}

/// Follows the ordinary whole-slice binary-search decision tree. Slots before
/// `known_before` are already known to compare Less, so those comparisons can
/// be inferred. Every actual probe is a subsequence of the baseline probes.
fn binary_search_from_with_accounting<T>(
    items: &[T],
    known_before: usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    binary_search_indexed_with_accounting(items.len(), known_before, accounting, |index| {
        items
            .get(index)
            .map(&mut compare)
            .ok_or(crate::FsError::InvalidDiff)
    })
}

/// The same probe plan for any rank-addressable collection. Navigation to a
/// rank examines integer topology only; payload comparison follows admission.
fn binary_search_indexed_with_accounting(
    len: usize,
    known_before: usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    compare: impl FnMut(usize) -> Result<Ordering, crate::FsError>,
) -> Result<Result<usize, usize>, crate::FsError> {
    struct Indexed<'a, 'b, C> {
        accounting: &'a mut crate::kernel::allocation::KernelAccounting<'b>,
        compare: C,
    }
    impl<C: FnMut(usize) -> Result<Ordering, crate::FsError>>
        acyclic_collections::page::search::Work<usize> for Indexed<'_, '_, C>
    {
        type Error = crate::FsError;
        fn check(&mut self) -> Result<(), Self::Error> {
            self.accounting.check()
        }
        fn invalid_prefix(&self) -> Self::Error {
            crate::FsError::InvalidDiff
        }
        fn compare(&mut self, index: &usize) -> Result<Ordering, Self::Error> {
            let delta = crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            self.accounting.admit(delta)?;
            let ordering = (self.compare)(*index)?;
            self.accounting.record_completed(delta)?;
            Ok(ordering)
        }
    }
    acyclic_collections::page::search::binary_search_indexed(
        len,
        known_before,
        &mut Indexed {
            accounting,
            compare,
        },
    )
}
