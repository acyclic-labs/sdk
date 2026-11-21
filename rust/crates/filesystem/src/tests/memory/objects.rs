use super::*;
use crate::performance::WorkBudget;
use crate::storage::ObjectFailure;

#[tokio::test]
async fn object_store_verifies_identity_bounds_and_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"abcdef"),
    };
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"abcdef")),
        WorkBudget::UNBOUNDED
    )?;
    assert_eq!(
        provider_receipt!(store, read(object_id, 6), WorkBudget::UNBOUNDED)?.value,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        provider_receipt!(store, read(object_id, 5), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));

    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        object_probes: 1,
        backend_read_operations: 1,
        object_bytes_read: 6,
        bytes_hashed: 6,
        ..crate::performance::work_budget(10_000, 65_536)
    };
    let mut context = AuthorityWork::new(budget, &cancellation);
    context.accounting().charge_items(1)?;
    let continuation = crate::kernel::allocation::retained::OwnedAccounting::take_branch(
        &mut context.accounting(),
        &cancellation,
    )?;
    let suspended = context.spent().unwrap();
    assert!(matches!(
        store.read(object_id, 6, &mut context).await,
        Err(ObjectStoreError::AccountingHandoffActive)
    ));
    assert!(matches!(
        crate::async_storage::read_many_sequential(
            &store,
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 6,
            }],
            &mut context,
        )
        .await,
        Err(ObjectStoreError::AccountingHandoffActive)
    ));
    assert_eq!(context.spent().unwrap(), suspended);

    continuation.restore(&mut context.accounting())?;
    let before_read = context.spent().unwrap();
    let read = store.read(object_id, 6, &mut context).await?;
    assert_eq!(read.bytes.as_ref(), b"abcdef");
    assert_eq!(read.retention, ObjectReadRetention::Shared);
    let completed = context.spent().unwrap();
    assert_eq!(completed.object_probes, 1);
    assert_eq!(completed.backend_read_operations, 1);
    assert_eq!(completed.object_bytes_read, 6);
    assert_eq!(completed.bytes_hashed, 6);
    assert_eq!(
        completed.allocation_operations,
        before_read.allocation_operations
    );
    assert_eq!(
        completed.peak_allocation_bytes,
        before_read.peak_allocation_bytes
    );
    assert!(matches!(
        store.read(object_id, 6, &mut context).await,
        Err(ObjectStoreError::Work(WorkError::BudgetExceeded { .. }))
    ));
    assert_eq!(context.spent().unwrap(), completed);

    let continuation = crate::kernel::allocation::retained::OwnedAccounting::take_branch(
        &mut context.accounting(),
        &cancellation,
    )?;
    let suspended = context.spent().unwrap();
    cancellation.cancel();
    assert!(matches!(
        crate::kernel::read_object_with_accounting(
            &store,
            object_id,
            6,
            &mut context.accounting(),
            &cancellation,
        )
        .await,
        Err(FsError::AccountingHandoffActive)
    ));
    assert!(matches!(
        crate::kernel::read_objects_with_accounting(
            &store,
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 6,
            }],
            &mut context.accounting(),
            &cancellation,
        )
        .await,
        Err(FsError::AccountingHandoffActive)
    ));
    assert_eq!(context.spent().unwrap(), suspended);
    continuation.restore(&mut context.accounting())?;
    let cancelled = context.spent().unwrap();
    assert!(matches!(
        store.read(object_id, 6, &mut context).await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(context.spent().unwrap(), cancelled);
    assert_eq!(read.bytes.as_ref(), b"abcdef");
    Ok(())
}

#[tokio::test]
async fn object_batch_preserves_order_and_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"first"),
    };
    let second = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"second"),
    };
    provider_receipt!(
        store,
        put(first, OwnedBytes::from_static(b"first")),
        WorkBudget::UNBOUNDED
    )?;
    provider_receipt!(
        store,
        put(second, OwnedBytes::from_static(b"second")),
        WorkBudget::UNBOUNDED
    )?;
    let requests = [
        crate::storage::ObjectReadRequest {
            object_id: second,
            maximum_bytes: 6,
        },
        crate::storage::ObjectReadRequest {
            object_id: first,
            maximum_bytes: 5,
        },
        crate::storage::ObjectReadRequest {
            object_id: second,
            maximum_bytes: 6,
        },
    ];
    let receipt = provider_receipt!(store, read_many(&requests), WorkBudget::UNBOUNDED)?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.clone())
            .collect::<Vec<_>>(),
        vec![
            OwnedBytes::from_static(b"second"),
            OwnedBytes::from_static(b"first"),
            OwnedBytes::from_static(b"second"),
        ]
    );

    let cancellation = CancellationToken::new();
    let earlier_bytes = 31;
    let result_bytes = (requests.len() * size_of::<ObjectRead>()) as u64;
    for shortfall in [0, 1] {
        let budget = WorkBudget {
            peak_allocation_bytes: earlier_bytes + result_bytes - shortfall,
            ..crate::performance::work_budget(10_000, 65_536)
        };
        let mut context = AuthorityWork::new(budget, &cancellation);
        let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(
            earlier_bytes as usize,
            &mut context.accounting(),
        )?;
        earlier.value.extend([7; 31]);
        let result =
            crate::async_storage::read_many_sequential(&store, &requests, &mut context).await;
        if shortfall == 0 {
            let (values, allocation) = result?;
            assert_eq!(values.len(), 3);
            assert_eq!(values[0].bytes.as_ref(), b"second");
            assert_eq!(values[1].bytes.as_ref(), b"first");
            assert_eq!(values[2].bytes.as_ref(), b"second");
            assert_eq!(
                context.spent().unwrap().peak_allocation_bytes,
                earlier_bytes + result_bytes
            );
            assert_eq!(context.spent().unwrap().backend_read_operations, 3);
            assert_eq!(
                context.allocations.live_bytes(),
                earlier_bytes + result_bytes
            );
            assert!(allocation.is_none());
            drop(values);
            drop(allocation);
            context.allocations.release_owned(result_bytes)?;
        } else {
            assert!(matches!(
                result,
                Err(ObjectStoreError::Work(WorkError::BudgetExceeded {
                    counter: crate::WorkCounter::PeakAllocationBytes,
                    ..
                }))
            ));
            assert_eq!(context.spent().unwrap().backend_read_operations, 0);
            assert_eq!(
                context.spent().unwrap().peak_allocation_bytes,
                earlier_bytes
            );
        }
        assert_eq!(earlier.value, [7; 31]);
        assert_eq!(context.allocations.live_bytes(), earlier_bytes);
        earlier.retire(&mut context.accounting())?;
        assert_eq!(context.allocations.live_bytes(), 0);
    }

    assert!(matches!(
        provider_receipt!(store, read_many(&[]), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::EmptyReadBatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_store_reauthenticates_shared_bytes_after_memory_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"authentic"),
    };
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"authentic")),
        WorkBudget::UNBOUNDED
    )?;
    let mut state = store
        .objects
        .write()
        .map_err(|_| "memory object lock poisoned")?;
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    *state
        .objects
        .get_or_try_insert_with(object_id.index_key(), &mut accounting, |_| {
            Err(ObjectStoreError::Missing.into())
        })? = OwnedBytes::from_static(b"corrupted");
    drop(state);
    assert!(matches!(
        provider_receipt!(store, read(object_id, 64), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        provider_receipt!(store, contains(object_id), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = provider_receipt!(
        store,
        read_many(&[crate::storage::ObjectReadRequest {
            object_id,
            maximum_bytes: 64,
        }]),
        WorkBudget::UNBOUNDED
    )
    .err()
    .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure.error, ObjectStoreError::Corrupt));

    Ok(())
}
