use super::*;
use proptest::prelude::*;

fn sorted() -> impl Strategy<Value = Vec<u16>> {
    prop::collection::vec(0_u16..64, 0..80).prop_map(|mut items| {
        items.sort_unstable();
        items
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn binary_search_matches_std(items in sorted(), key in 0_u16..70) {
        let counted = counted_binary_search(&items, |item| item.cmp(&key));
        match (counted.0, items.binary_search(&key)) {
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
        let (index, _) = counted_partition_point(&items, before);
        prop_assert_eq!(index, items.partition_point(before));
    }
}

#[test]
fn accounting_denies_a_probe_before_calling_its_comparator()
-> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget { items_examined: 1, ..crate::WorkBudget::UNBOUNDED }, &cancellation,
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
    let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
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
