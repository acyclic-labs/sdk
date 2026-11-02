use super::*;
use proptest::prelude::*;

fn selection_path(components: &[u16]) -> NamespacePath {
    let limits = crate::model::VolumeLimits::default();
    NamespacePath::new(
        components
            .iter()
            .map(|component| {
                LogicalName::new(NameEncoding::Utf8, format!("{component:04x}").into_bytes())
                    .unwrap()
            })
            .collect(),
        limits,
    )
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn selected_paths_preserve_order_overlap_and_original_work_bounds(
        components in prop::collection::vec(prop::collection::vec(0..32_u16, 1..4), 0..64),
        count in 128..257_u16,
    ) {
        let cancellation = CancellationToken::new();
        let budget = crate::performance::work_budget(100_000, 8 * 1024 * 1024);
        let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let baseline = work.live_allocation_bytes();
        let paths = components.iter().map(|names| selection_path(names)).collect::<Vec<_>>();
        let mut expected = paths.iter().collect::<Vec<_>>();
        expected.sort_unstable();
        let overlapping = expected.windows(2).any(|pair| pair[1].is_within(pair[0]));
        let mut receipt = GenerationReceipt::from_work(&work).unwrap();
        let selected = select_materialization_paths(&paths, &mut receipt);
        match selected {
            Ok(selected) => {
                prop_assert!(!overlapping);
                prop_assert_eq!(selected, expected);
            }
            Err(error) => {
                prop_assert!(overlapping);
                prop_assert!(matches!(error, MaterializeError::OverlappingPaths));
            }
        }
        drop(receipt);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);

        // The old reverse insertion needs n(n-1)/2 comparisons and n(n+1)/2
        // pointer copies. This original item/copy credit cannot fund it.
        let paths = (0..count).rev().map(|key| selection_path(&[key])).collect::<Vec<_>>();
        let levels = u64::from(usize::BITS - paths.len().leading_zeros());
        let bound = 3 * u64::from(count) * levels + 2 * u64::from(count - 1);
        prop_assert!(u64::from(count) * u64::from(count - 1) / 2 > bound);
        let bounded = work.native_volume_work(WorkBudget {
            items_examined: bound,
            bytes_copied: 128 * 1024,
            ..budget
        }).unwrap();
        let mut receipt = GenerationReceipt::from_work(&bounded).unwrap();
        let before = work.receipt();
        let selected = select_materialization_paths(&paths, &mut receipt).unwrap();
        prop_assert!(selected.windows(2).all(|pair| pair[0] < pair[1]));
        prop_assert_eq!(selected.len(), paths.len());
        prop_assert!(work.receipt().items_examined - before.items_examined <= bound);
        prop_assert!(work.receipt().bytes_copied - before.bytes_copied <= 128 * 1024);
        drop((selected, receipt, bounded));
        prop_assert_eq!(work.live_allocation_bytes(), baseline);

        // Refusal and cancellation retire the same selection backing. Neither
        // can reach the destination preparation following selection.
        for copied in [0, paths.len() as u64 * std::mem::size_of::<&NamespacePath>() as u64] {
            let denied = work.native_volume_work(WorkBudget {
                items_examined: 0,
                bytes_copied: copied,
                ..budget
            }).unwrap();
            let mut receipt = GenerationReceipt::from_work(&denied).unwrap();
            prop_assert!(matches!(
                select_materialization_paths(&paths, &mut receipt),
                Err(MaterializeError::Work(_)),
            ));
            drop((receipt, denied));
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
        }
        let mut receipt = GenerationReceipt::from_work(&work).unwrap();
        cancellation.cancel();
        prop_assert!(matches!(
            select_materialization_paths(&paths, &mut receipt),
            Err(MaterializeError::Cancelled(_)),
        ));
        drop(receipt);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
