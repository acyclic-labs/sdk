use super::*;
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_point::Owned;
use proptest::prelude::*;

#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "owned_loom.rs"]
mod loom_tests;

fn page(
    count: usize,
    accounting: &mut KernelAccounting<'_>,
) -> (OwnedPage<FileTableFormat>, KernelOwnedAllocation) {
    let mut records = Owned::<Vec<crate::kernel::FileRecord>>::allocate(count, accounting).unwrap();
    records
        .value
        .extend((0..count).map(|key| crate::kernel::FileRecord {
            file_id: FileId::from_bytes([key as u8; 16]),
            kind: FileKind::Fifo,
            link_count: 1,
            metadata: ObjectId {
                kind: ObjectKind::Metadata,
                digest: crate::Digest::ZERO,
            },
            payload: crate::kernel::FilePayload::Empty,
        }));
    (
        OwnedPage {
            page: PageLease::Owned(Page::Leaf(records.value)),
            logical_bytes: records.bytes,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        },
        records.allocation,
    )
}

fn key() -> DecodedCacheKey {
    DecodedCacheKey::new::<Page<FileTableFormat>>(
        ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: crate::Digest::ZERO,
        },
        DecodeLimits::default(),
    )
}

fn check_owned<const LEGACY: bool>(
    count: usize,
    accounting: &mut KernelAccounting<'_>,
) -> proptest::test_runner::TestCaseResult {
    let input = page(count, accounting);
    let PageLease::Owned(Page::Leaf(records)) = &input.0.page else {
        unreachable!()
    };
    let address = records.as_ptr();
    let bytes = (count * size_of::<crate::kernel::FileRecord>()) as u64;
    let before = accounting.spent();
    // Forcing the default cache hooks replays the old allocate-and-share path.
    let result = if LEGACY {
        admit_decoded_with_accounting(
            &OwnedReadObjectStore::default(),
            key(),
            input,
            accounting,
            None,
        )
    } else {
        admit_decoded_with_accounting(
            &MemoryObjectStore::default(),
            key(),
            input,
            accounting,
            None,
        )
    };
    let (page, allocation) = result.map_err(|error| TestCaseError::fail(error.to_string()))?;
    prop_assert!(matches!(page.page, PageLease::Owned(_)));
    prop_assert_eq!(page.logical_bytes, bytes);
    prop_assert_eq!(
        accounting.spent(),
        before,
        "no shared owner or payload copy is needed"
    );
    let ((page, actual), allocation) = page
        .into_owned_with_accounting(allocation, accounting)
        .map_err(|error| TestCaseError::fail(error.to_string()))?;
    let Page::Leaf(records) = &page else {
        unreachable!()
    };
    prop_assert_eq!(
        records.as_ptr(),
        address,
        "mutation takes the original buffer"
    );
    prop_assert_eq!(actual, bytes);
    prop_assert_eq!(
        accounting.spent(),
        before,
        "taking ownership needs no clone"
    );
    Owned::new(page, allocation, actual)
        .retire(accounting)
        .unwrap();
    Ok(())
}

fn exact_budget(count: usize) -> WorkBudget {
    WorkBudget {
        allocation_operations: u64::from(count != 0),
        peak_allocation_bytes: (count * size_of::<crate::kernel::FileRecord>()) as u64,
        ..WorkBudget::UNBOUNDED
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn uncached_pages_keep_their_buffer_custody_and_exact_receipt(count in 0_usize..65) {
        let cancellation = CancellationToken::new();
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        check_owned::<false>(count, &mut KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, exact_budget(count), &cancellation))?;
        prop_assert_eq!(allocations.live_bytes(), 0);
        prop_assert_eq!(work.allocation_operations, u64::from(count != 0));
        prop_assert_eq!(work.peak_allocation_bytes, exact_budget(count).peak_allocation_bytes);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
            let resource = original.native_volume_work(exact_budget(count)).unwrap();
            let baseline = original.live_allocation_bytes();
            check_owned::<false>(count, &mut KernelAccounting::native(&resource, &cancellation))?;
            prop_assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
}

#[test]
fn exact_ownership_oracle_rejects_the_old_shared_owner_path() {
    let cancellation = CancellationToken::new();
    for count in [0, 1, 64] {
        let mut accounting = KernelAccounting::standalone(exact_budget(count), &cancellation);
        assert!(check_owned::<true>(count, &mut accounting).is_err());
        #[cfg(not(target_arch = "wasm32"))]
        {
            let original =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .unwrap();
            let resource = original.native_volume_work(exact_budget(count)).unwrap();
            let baseline = original.live_allocation_bytes();
            assert!(
                check_owned::<true>(
                    count,
                    &mut KernelAccounting::native(&resource, &cancellation)
                )
                .is_err()
            );
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
}

#[test]
fn standalone_handoff_keeps_the_existing_cancellation_checkpoints() {
    // This internal transfer performs no new work. The read/decoder owns the
    // cancellation checkpoints; the original shared-owner handoff has none.
    for legacy in [false, true] {
        let cancellation = CancellationToken::new();
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let input = page(2, &mut accounting);
        cancellation.cancel();
        let (page, allocation) = if legacy {
            admit_decoded_with_accounting(
                &OwnedReadObjectStore::default(),
                key(),
                input,
                &mut accounting,
                None,
            )
        } else {
            admit_decoded_with_accounting(
                &MemoryObjectStore::default(),
                key(),
                input,
                &mut accounting,
                None,
            )
        }
        .unwrap();
        let bytes = page.logical_bytes;
        Owned::new(page, allocation, bytes)
            .retire(&mut accounting)
            .unwrap();
        assert_eq!(allocations.live_bytes(), 0);
    }
}

#[test]
fn uncached_admission_rechecks_a_tightened_budget_before_handoff() {
    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    let input = page(
        2,
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
    );
    let budget = WorkBudget {
        peak_allocation_bytes: input.0.logical_bytes - 1,
        ..WorkBudget::UNBOUNDED
    };
    let result = admit_decoded_with_accounting(
        &MemoryObjectStore::default(),
        key(),
        input,
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            budget,
            &cancellation,
        ),
        None,
    );
    assert!(matches!(result, Err(crate::FsError::Work(_))));
    assert_eq!(allocations.live_bytes(), 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn uncached_native_admission_keeps_identity_and_cancellation_checks() {
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let other =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = original.live_allocation_bytes();
    let input = page(2, &mut KernelAccounting::native(&original, &cancellation));
    let result = admit_decoded_with_accounting(
        &MemoryObjectStore::default(),
        key(),
        input,
        &mut KernelAccounting::native(&other, &cancellation),
        None,
    );
    assert!(matches!(
        result,
        Err(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::InvalidLayout
        ))
    ));
    assert_eq!(original.live_allocation_bytes(), baseline);
    let input = page(2, &mut KernelAccounting::native(&original, &cancellation));
    cancellation.cancel();
    assert!(
        admit_decoded_with_accounting(
            &MemoryObjectStore::default(),
            key(),
            input,
            &mut KernelAccounting::native(&original, &cancellation),
            None
        )
        .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn uncached_native_handoff_cannot_change_the_allocation_resource() {
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let resource = original.native_volume_work(WorkBudget::UNBOUNDED).unwrap();
    let baseline = original.live_allocation_bytes();
    for (source, destination) in [(&original, &resource), (&resource, &original)] {
        let input = page(2, &mut KernelAccounting::native(source, &cancellation));
        let result = admit_decoded_with_accounting(
            &MemoryObjectStore::default(),
            key(),
            input,
            &mut KernelAccounting::native(destination, &cancellation),
            None,
        );
        assert!(matches!(
            result,
            Err(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::InvalidLayout
            ))
        ));
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    let input = page(2, &mut KernelAccounting::native(&original, &cancellation));
    let explicit = CancellationToken::new();
    explicit.cancel();
    assert!(
        admit_decoded_with_accounting(
            &MemoryObjectStore::default(),
            key(),
            input,
            &mut KernelAccounting::native(&original, &explicit),
            None
        )
        .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
}
