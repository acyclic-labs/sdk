use super::*;

fn handoff_model<const EARLY_RETIREMENT: bool>() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let resource = original.native_volume_work(exact_budget(1)).unwrap();
        // Keep root/resource storage owners until join. The synchronization
        // exercised here is the production crate::sync leaf/resource mutexes
        // and cancellation atomics, rather than Arc reference-count uniqueness.
        let baseline = original.live_allocation_bytes();
        let input = page(1, &mut KernelAccounting::native(&resource, &cancellation));
        let charged = original.receipt();
        let worker_cancellation = cancellation.clone();
        let worker_original = original.clone();
        let worker_resource = resource.clone();
        let worker = loom::thread::spawn(move || {
            let result = admit_decoded_with_accounting(
                &MemoryObjectStore::default(),
                key(),
                input,
                &mut KernelAccounting::native(&worker_resource, &worker_cancellation),
                None,
            );
            match result {
                Ok((page, allocation)) => {
                    let bytes = page.logical_bytes;
                    let mut allocation = Some(allocation);
                    if EARLY_RETIREMENT {
                        // Broken lifetime: retire funding while the real
                        // production result still retains its decoded buffer.
                        drop(allocation.take());
                    }
                    let funded = worker_original.live_allocation_bytes() == baseline + bytes;
                    drop(page);
                    drop(allocation);
                    funded
                }
                Err(error) => {
                    assert!(matches!(
                        error,
                        FsError::Cancelled(_)
                            | FsError::NativeKernel(
                                crate::NativeTreeMaterializationError::Cancelled
                            )
                    ));
                    true // Rejection must also retire the original input.
                }
            }
        });
        cancellation.cancel();
        assert!(
            worker.join().unwrap(),
            "payload must keep its original funding"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(
            original.receipt(),
            charged,
            "handoff/retirement adds no work"
        );
    });
}

#[test]
fn native_handoff_keeps_funding_until_payload_destruction_across_cancellation() {
    handoff_model::<false>();
}

#[test]
#[should_panic(expected = "payload must keep its original funding")]
fn early_funding_retirement_is_detected() {
    handoff_model::<true>();
}
