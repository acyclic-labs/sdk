use super::*;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

fn observe(
    raw: &[Patch],
    final_size: u64,
    sort: impl FnOnce(&mut Owned<Vec<u64>>, &mut KernelAccounting<'_>) -> Result<(), FsError>,
) -> (Result<Vec<Patch>, String>, WorkCounters) {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let result = normalize_raw_patches(raw, final_size, &mut accounting, sort)
        .map(|patches| {
            let value = patches.value.clone();
            patches.retire(&mut accounting).unwrap();
            value
        })
        .map_err(|error| format!("{error:?}"));
    (result, accounting.spent())
}

fn compare(
    raw: &[Patch],
    final_size: u64,
    sort: impl FnOnce(&mut Owned<Vec<u64>>, &mut KernelAccounting<'_>) -> Result<(), FsError>,
) -> TestCaseResult {
    let (actual, mut receipt) = observe(raw, final_size, sort);
    let (expected, old) = observe(raw, final_size, sort_all_coordinates);
    prop_assert_eq!(actual, expected);
    let active = raw
        .iter()
        .filter(|patch| patch.offset.min(final_size) < patch.end.min(final_size))
        .count();
    if active == 1 {
        receipt.items_examined += 32;
        receipt.bytes_copied += 128;
        receipt.allocation_operations += 1;
    }
    prop_assert_eq!(receipt, old);
    Ok(())
}

#[test]
fn comparison_detects_skipping_sort_for_unordered_patches() {
    let raw = [
        Patch {
            offset: 10,
            end: 20,
            kind: ExtentKind::AllocatedZero,
        },
        Patch {
            offset: 0,
            end: 5,
            kind: ExtentKind::Hole,
        },
    ];
    compare(&raw, 20, sort_patch_coordinates).unwrap();
    assert!(compare(&raw, 20, |_, _| Ok(())).is_err());
}

proptest! {
    // Clipping, overlap priority, coalescing and every remaining charge match
    // the full-radix implementation; exactly one active pair saves 8 passes.
    #[test]
    fn already_ordered_pair_preserves_normalization_and_exact_remaining_work(
        ranges in prop::collection::vec((0_u64..129, 0_u64..129, any::<bool>()), 0..9),
        final_size in 0_u64..129,
    ) {
        let raw: Vec<_> = ranges.into_iter().map(|(a, b, allocated)| Patch {
            offset: a.min(b), end: a.max(b),
            kind: if allocated { ExtentKind::AllocatedZero } else { ExtentKind::Hole },
        }).collect();
        compare(&raw, final_size, sort_patch_coordinates)?;
    }
}
