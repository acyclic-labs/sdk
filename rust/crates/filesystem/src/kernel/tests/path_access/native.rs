#[tokio::test]
async fn observer_cache_and_cold_frame_overlap_survives_unwind_without_phantom_retention()
-> Result<(), Box<dyn std::error::Error>> {
    struct PanicAfterAdmission<'a>(&'a AtomicBool);
    impl Future for PanicAfterAdmission<'_> {
        type Output = Result<crate::OperationReceipt<()>, PathLookupFailure>;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            panic!("cold backend unwind after the canonical physical frame allocation");
        }
    }
    impl Drop for PanicAfterAdmission<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let store = MemoryObjectStore::default();
    let custody = super::super::ProofDataCustody::default();
    let (cache, setup) = setup_cache(&store, 64, WorkBudget::UNBOUNDED, Some(&custody))?;
    let dropped = AtomicBool::new(false);
    let observer = |event| {
        if matches!(event, crate::heap_future::HeapFutureEvent::Released(_)) {
            assert!(
                dropped.load(Ordering::Acquire),
                "whole box dies before frame retirement"
            );
        }
        custody.record_heap_future_event(event)
    };
    let cancellation = CancellationToken::new();
    let unwind = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        (crate::heap_future::in_heap_measured(
            |_| PanicAfterAdmission(&dropped),
            WorkBudget::UNBOUNDED,
            &cancellation,
            PathLookupError::Tree(TreeReadError::Cancelled),
            Some(&observer),
        ))
        .await
    }))
    .await;
    assert!(unwind.is_err());
    let frame_bytes = u64::try_from(size_of::<PanicAfterAdmission<'_>>())?;
    let completed = custody.take_completed_data_work();
    assert_eq!(completed.allocation_operations, 2);
    assert_eq!(
        completed.peak_allocation_bytes,
        setup.peak_allocation_bytes + frame_bytes
    );
    assert_eq!(
        custody.retained_bytes(),
        0,
        "cache and frames are not retained proof keys"
    );
    drop(cache);
    assert_eq!(custody.take_completed_data_work(), WorkCounters::default());
    let next_observer = |event| custody.record_heap_future_event(event);
    let next = (crate::heap_future::in_heap_measured(
        |_| {
            std::future::ready(Ok::<_, PathLookupFailure>(crate::OperationReceipt {
                value: (),
                work: WorkCounters::default(),
            }))
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
        PathLookupError::Tree(TreeReadError::Cancelled),
        Some(&next_observer),
    ))
    .await?;
    assert_eq!(
        custody.take_completed_data_work(),
        next.work,
        "dead cache backing is not carried forward"
    );
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_path_frame_refusal_preserves_successful_prefix_without_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let live_before = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let failure = lookup_path_async_with_accounting(
        &store,
        &generation,
        &path,
        config(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await
    .expect_err("a refused original frame cannot start a backend read");
    assert!(matches!(
        failure,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded {
                counter: "allocation_operations",
                observed: 2,
                maximum: 1,
            }
        ))
    ));
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(
        original.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert!(original.receipt().items_examined > before.items_examined);
    assert_eq!(original.live_allocation_bytes(), live_before);
    Ok(())
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn original_path_item_effect_runs_without_root_loan_and_survives_late_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let inspected = examine_item(&mut accounting, None, || original.receipt())?;
    assert_eq!(
        inspected, before,
        "the actual inspection precedes the item completion"
    );
    let mut expected = before;
    expected.items_examined += 1;
    assert_eq!(original.receipt(), expected);
    let failure = examine_item(&mut accounting, None, || cancellation.cancel());
    assert!(
        failure.is_err(),
        "late cancellation refuses only after recording the actual item"
    );
    expected.items_examined += 1;
    assert_eq!(original.receipt(), expected);
    Ok(())
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn original_path_slot_reserve_failure_keeps_actual_cause_and_retires_only_claimed_backing()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let live_before = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let count = (isize::MAX as usize)
        .checked_add(1)
        .ok_or("reserve bound overflowed")?;
    let failure = reserve_slots::<u8>(count, &mut accounting, None)
        .err()
        .ok_or("an impossible physical reserve unexpectedly succeeded")?;
    assert!(matches!(
        failure,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(_),)
    ));
    let mut expected = before;
    expected.allocation_operations += 1;
    expected.peak_allocation_bytes = expected
        .peak_allocation_bytes
        .max(live_before + count as u64);
    assert_eq!(original.receipt(), expected);
    assert_eq!(original.live_allocation_bytes(), live_before);
    Ok(())
}
