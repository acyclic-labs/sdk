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

fn shared_lease_model<const DETACH_CUSTODY: bool>() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let resource = original.native_volume_work(WorkBudget::UNBOUNDED).unwrap();
        let baseline = original.live_allocation_bytes();
        let input = page(1, &mut KernelAccounting::native(&resource, &cancellation));

        // The default cache hooks return the decoded page without retaining it.
        // Only the returned lease can keep its native custody alive.
        let (decoded, empty) = admit_decoded_with_accounting(
            &OwnedReadObjectStore::default(),
            key(),
            input,
            &mut KernelAccounting::native(&resource, &cancellation),
            None,
        )
        .unwrap();
        drop(empty);
        assert_eq!(decoded.logical_bytes, 0);
        let funded = original.live_allocation_bytes();
        let mut lease = decoded.page;
        if DETACH_CUSTODY {
            let PageLease::Shared {
                _native_custody, ..
            } = &mut lease
            else {
                unreachable!()
            };
            drop(_native_custody.take());
        }
        let reader_work = original.clone();
        let reader = loom::thread::spawn(move || {
            // Lock order: one production allocation-resource mutex at a time.
            // A cancellation must not retire funding under a borrowed page.
            let PageRef::Leaf(records) = lease.view() else {
                unreachable!()
            };
            assert_eq!(records.len(), 1);
            assert_eq!(
                reader_work.live_allocation_bytes(),
                funded,
                "shared lease lost its original funding"
            );
            drop(lease);
        });
        cancellation.cancel();
        reader.join().unwrap();
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}

#[test]
fn shared_page_lease_keeps_custody_after_its_outer_owner_is_consumed() {
    shared_lease_model::<false>();
}

#[test]
#[should_panic(expected = "shared lease lost its original funding")]
fn detached_shared_page_custody_is_detected() {
    shared_lease_model::<true>();
}
