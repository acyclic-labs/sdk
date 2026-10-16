use super::*;
use proptest::prelude::*;

fn sorted() -> impl Strategy<Value = Vec<u16>> {
    prop::collection::vec(0_u16..64, 0..80).prop_map(|mut items| {
        items.sort_unstable();
        items
    })
}

fn compare_galloping<const SKIP_FIRST: bool, const UNCHARGED_PROBE: bool>(
    items: &[u16],
    key: u16,
) -> proptest::test_runner::TestCaseResult {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let mut calls = 0;
    let mut compare = |item: &u16| {
        calls += 1;
        item.cmp(&key)
    };
    if UNCHARGED_PROBE && let Some(first) = items.first() {
        compare(first);
    }
    let offset = usize::from(SKIP_FIRST && !items.is_empty());
    let found =
        galloping_search_with_accounting(items.get(offset..).unwrap(), &mut accounting, compare)
            .map_err(|error| TestCaseError::fail(error.to_string()))?
            .map(|index| index + offset)
            .map_err(|index| index + offset);
    prop_assert_eq!(
        accounting.spent().items_examined,
        calls,
        "charge every actual comparison"
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
    fn galloping_matches_std_and_charges_every_comparison(items in sorted(), key in 0_u16..70) {
        compare_galloping::<false, false>(&items, key)?;
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
fn galloping_oracle_detects_a_skipped_slot_and_an_uncharged_comparison() {
    compare_galloping::<false, false>(&[1, 2, 3], 1).unwrap();
    assert!(compare_galloping::<true, false>(&[1, 2, 3], 1).is_err());
    let error = compare_galloping::<false, true>(&[1, 2, 3], 1).unwrap_err();
    assert!(error.to_string().contains("charge every actual comparison"));
}

#[test]
fn empty_galloping_retains_admission_without_comparisons() -> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNCHARGED,
        &cancellation,
    );
    let mut calls = 0;
    assert_eq!(
        galloping_search_with_accounting::<u8>(&[], &mut accounting, |_| {
            calls += 1;
            Ordering::Equal
        })?,
        Err(0)
    );
    cancellation.cancel();
    assert!(
        galloping_search_with_accounting::<u8>(&[], &mut accounting, |_| {
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
fn galloping_denies_each_probe_before_its_comparator() -> Result<(), crate::FsError> {
    let items: Vec<_> = (0..64).collect();
    for key in 0..=64 {
        let cancellation = crate::CancellationToken::new();
        let mut unlimited = crate::kernel::allocation::KernelAccounting::standalone(
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let expected =
            galloping_search_with_accounting(&items, &mut unlimited, |item| item.cmp(&key))?;
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
            let found = galloping_search_with_accounting(&items, &mut accounting, |item| {
                calls += 1;
                item.cmp(&key)
            });
            assert_eq!(calls, budget);
            assert_eq!(accounting.spent().items_examined, calls);
            if budget == required {
                assert_eq!(found?, expected);
            } else {
                assert!(found.is_err());
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_galloping_preserves_completed_comparisons_on_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let mut calls = 0;
    let found = galloping_search_with_accounting(&[1, 2, 3], &mut accounting, |item| {
        calls += 1;
        cancellation.cancel();
        item.cmp(&3)
    });
    assert!(found.is_err());
    assert_eq!(calls, 1);
    assert_eq!(original.receipt().items_examined, calls);
    assert!(
        galloping_search_with_accounting::<u8>(&[], &mut accounting, |_| {
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
