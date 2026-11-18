use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn capture_ordering_preserves_original_credit_and_refuses_unfunded_swaps(
        input in prop::collection::vec(any::<u16>(), 0..257),
    ) {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(
            crate::performance::work_budget(100_000, 8 * 1024 * 1024),
            &cancellation,
        ).unwrap();
        let baseline = original.live_allocation_bytes().map_err(|error| TestCaseError::fail(error.to_string()))?;
        let scope = CaptureWorkScope::new(&original).unwrap();
        let mut values = capture_vec(input.len(), &scope.native).unwrap();
        values.extend_from_slice(&input);
        let mut expected = input;
        expected.sort_unstable();
        let allocation = original.live_allocation_bytes().map_err(|error| TestCaseError::fail(error.to_string()))?;
        capture_sort_by(&mut values, Ord::cmp, &scope.native).unwrap();
        prop_assert_eq!(&values, &expected);
        prop_assert_eq!(original.live_allocation_bytes(), Ok(allocation));

        // Already ordered input needs one adjacent pass and no swaps, unlike
        // rebuilding and extracting a heap for the same canonical sequence.
        let before = original.receipt().map_err(|error| TestCaseError::fail(error.to_string()))?;
        capture_sort_by(&mut values, Ord::cmp, &scope.native).unwrap();
        let after = original.receipt().map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(after.items_examined - before.items_examined, values.len().saturating_sub(1) as u64);
        prop_assert_eq!(after.bytes_copied, before.bytes_copied);
        drop((values, scope));
        prop_assert_eq!(original.live_allocation_bytes(), Ok(baseline));

        // Restoring the capture adapter's uncharged swap would sort this pair
        // successfully instead of refusing before changing its original bytes.
        let denied = original.native_volume_work(WorkBudget {
            items_examined: 100,
            bytes_copied: 0,
            ..crate::performance::work_budget(100_000, 8 * 1024 * 1024)
        }).unwrap();
        let mut pair = [2_u16, 1];
        let error = capture_sort_by(&mut pair, Ord::cmp, &denied).unwrap_err();
        prop_assert!(matches!(error, CaptureError::Kernel(error) if matches!(
            *error,
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)),
        )));
        prop_assert_eq!(pair, [2, 1]);
        drop(denied);
        prop_assert_eq!(original.live_allocation_bytes(), Ok(baseline));

        // Even an empty selection observes the same original cancellation;
        // no comparison or extraction is needed to reach this terminal.
        cancellation.cancel();
        for values in [&mut [][..], &mut pair[..]] {
            let error = capture_sort_by(values, Ord::cmp, &original).unwrap_err();
            prop_assert!(matches!(error, CaptureError::Kernel(error) if matches!(*error, crate::FsError::Cancelled(_))));
        }
        prop_assert_eq!(original.live_allocation_bytes(), Ok(baseline));
    }
}
