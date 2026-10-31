use super::*;
use crate::CancellationToken;

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn eviction_bounds_backing_and_destroys_values_before_original_funding()
-> Result<(), Box<dyn std::error::Error>> {
    struct Value<'a> {
        original: &'a crate::NativeMaterializationWork,
        drops: &'a std::cell::Cell<usize>,
        minimum: &'a std::cell::Cell<u64>,
    }
    impl Drop for Value<'_> {
        fn drop(&mut self) {
            assert!(self.original.live_allocation_bytes() >= self.minimum.get());
            self.drops.set(self.drops.get() + 1);
        }
    }
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut node_bytes = 0;
    let minimum = std::cell::Cell::new(baseline);
    let drops = std::cell::Cell::new(0);
    let mut index = RecentIndex::<_, 32, 2>::default();
    for number in 0_u8..64 {
        index.get_or_try_insert_with(
            [number; 32],
            &mut KernelAccounting::native(&original, &cancel),
            |_| {
                Ok(Value {
                    original: &original,
                    drops: &drops,
                    minimum: &minimum,
                })
            },
        )?;
        if number == 0 {
            node_bytes = original.live_allocation_bytes() - baseline;
            minimum.set(baseline + node_bytes);
        }
        assert_eq!(index.complete(), number < 4);
        // Four leaves across two windows need at most two branch nodes.
        assert!(original.live_allocation_bytes() <= baseline + 6 * node_bytes);
        assert!(
            index
                .get(
                    [number; 32],
                    &mut KernelAccounting::native(&original, &cancel)
                )?
                .is_some()
        );
        if number >= 4 {
            assert!(
                index
                    .get([0; 32], &mut KernelAccounting::native(&original, &cancel))?
                    .is_none()
            );
            assert!(
                !index.complete(),
                "evicted facts were treated as verified absence"
            );
        }
        let before = original.receipt();
        let mut constructed = false;
        index.get_or_try_insert_with(
            [number; 32],
            &mut KernelAccounting::native(&original, &cancel),
            |_| {
                constructed = true;
                Err(FsError::InvalidDiff)
            },
        )?;
        assert!(!constructed);
        assert_eq!(
            original.receipt().allocation_operations,
            before.allocation_operations
        );
    }
    assert_eq!(64 - drops.get(), 4);
    drop(index);
    assert_eq!(drops.get(), 64);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn refused_rotation_keeps_all_facts_and_complete_coverage() -> Result<(), Box<dyn std::error::Error>>
{
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let mut index = RecentIndex::<_, 32, 2>::default();
    for number in 0_u8..4 {
        index.get_or_try_insert_with(
            [number; 32],
            &mut KernelAccounting::native(&original, &cancel),
            |_| Ok(number),
        )?;
    }
    let baseline = original.live_allocation_bytes();
    let attempt = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: 1,
            ..crate::performance::BOUNDARY_BUDGET
        },
        &cancel,
    )
    .map_err(|(error, _)| error)?;
    let attempt_baseline = attempt.live_allocation_bytes();
    assert!(
        index
            .get_or_try_insert_with(
                [4; 32],
                &mut KernelAccounting::native(&attempt, &cancel),
                |_| Ok(4)
            )
            .is_err()
    );
    assert_eq!(attempt.live_allocation_bytes(), attempt_baseline);
    assert!(
        index
            .get_or_try_insert_with(
                [4; 32],
                &mut KernelAccounting::native(&original, &cancel),
                |_| Err(FsError::InvalidDiff)
            )
            .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert!(index.complete());
    for number in 0_u8..4 {
        assert_eq!(
            index.get(
                [number; 32],
                &mut KernelAccounting::native(&original, &cancel)
            )?,
            Some(&number)
        );
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        index
            .get_or_try_insert_with(
                [4; 32],
                &mut KernelAccounting::native(&original, &cancelled),
                |_| Ok(4)
            )
            .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert!(index.complete());
    index.get_or_try_insert_with(
        [4; 32],
        &mut KernelAccounting::native(&original, &cancel),
        |_| Ok(4),
    )?;
    assert!(!index.complete());
    assert_eq!(
        index.get([0; 32], &mut KernelAccounting::native(&original, &cancel))?,
        None
    );
    assert_eq!(
        index.get([1; 32], &mut KernelAccounting::native(&original, &cancel))?,
        None
    );
    for number in 2_u8..5 {
        assert_eq!(
            index.get(
                [number; 32],
                &mut KernelAccounting::native(&original, &cancel)
            )?,
            Some(&number)
        );
    }
    Ok(())
}
