use super::*;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{AuthorityStoreError, ObjectKind, ObjectStoreError, object_digest};
use acyclic_stream::OwnedBytes;

#[tokio::test]
async fn owned_read_conformance_keeps_each_live_batch_buffer_funded()
-> Result<(), Box<dyn std::error::Error>> {
    let store = crate::test_support::OwnedReadObjectStore::default();
    let cancellation = CancellationToken::new();
    let bytes = b"independent read backing";
    let object_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(bytes),
    };
    let budget = crate::performance::BOUNDARY_BUDGET;
    store
        .put(
            object_id,
            OwnedBytes::copy_from_slice(bytes),
            &mut AuthorityWork::new(budget, &cancellation),
        )
        .await?;
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: bytes.len() as u64,
    }; 2];
    let output_bytes = (2 * size_of::<crate::ObjectRead>() + 2 * bytes.len()) as u64;
    for shortfall in [0, 1] {
        let mut work = AuthorityWork::new(
            WorkBudget {
                peak_allocation_bytes: 31 + output_bytes - shortfall,
                ..budget
            },
            &cancellation,
        );
        let mut earlier = work.vec_owned::<u8>(31)?;
        earlier.value.extend([7; 31]);
        let result = store.read_many(&requests, &mut work).await;
        if shortfall == 0 {
            let (values, allocation) = result?;
            assert_eq!(values.len(), 2);
            for read in &values {
                assert_eq!(read.bytes.as_ref(), bytes);
                assert_eq!(
                    read.retention,
                    crate::ObjectReadRetention::Owned {
                        logical_bytes: bytes.len() as u64
                    }
                );
            }
            assert_eq!(
                work.spent().unwrap().peak_allocation_bytes,
                31 + output_bytes
            );
            assert_eq!(work.spent().unwrap().bytes_copied, 2 * bytes.len() as u64);
            assert_eq!(work.allocations.live_bytes(), 31 + output_bytes);
            assert!(allocation.is_none());
            drop(values);
            drop(allocation);
            work.allocations.release_owned(output_bytes)?;
        } else {
            assert!(matches!(
                result,
                Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }))
            ));
            assert_eq!(work.spent().unwrap().bytes_copied, bytes.len() as u64);
        }
        assert_eq!(earlier.value, [7; 31]);
        assert_eq!(work.allocations.live_bytes(), 31);
        earlier.retire(&mut work.accounting())?;
        assert_eq!(work.allocations.live_bytes(), 0);
    }
    let mut work = AuthorityWork::new(budget, &cancellation);
    let earlier = work.vec_owned::<u8>(31)?;
    let read = store.read(object_id, bytes.len() as u64, &mut work).await?;
    assert_eq!(read.bytes.as_ref(), bytes);
    assert_eq!(work.allocations.live_bytes(), 31 + bytes.len() as u64);
    drop(read);
    work.allocations.release_owned(bytes.len() as u64)?;
    let before = work.spent().unwrap();
    work.budget.bytes_copied = before.bytes_copied;
    assert!(matches!(
        store.read(object_id, bytes.len() as u64, &mut work).await,
        Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        }))
    ));
    assert_eq!(work.spent().unwrap().bytes_copied, before.bytes_copied);
    assert_eq!(work.allocations.live_bytes(), 31);
    earlier.retire(&mut work.accounting())?;
    assert_eq!(work.allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn default_guarded_fork_preserves_owner_rejection_and_rejects_managed_permits()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fork = WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([0x91; 16]),
        creation: ProposedCommit {
            publication: None,
            operation_id: OperationId::from_bytes([0x92; 16]),
            fingerprint: Digest::from_bytes([0x93; 32]),
            payload: bytes::Bytes::from_static(b"creation"),
        },
        retention: AuthorityId::from_bytes([0x94; 16]),
        retained: ProposedCommit {
            publication: None,
            operation_id: OperationId::from_bytes([0x95; 16]),
            fingerprint: Digest::from_bytes([0x96; 32]),
            payload: bytes::Bytes::from_static(b"retention"),
        },
    };
    let baseline = MemoryAuthorityStore::default();
    let mut prefix = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let mut baseline_guard = |_: WorkBudget| {
        std::future::ready(Err(crate::OperationFailure::new(
            crate::FsError::WorkspaceDeleted,
            WorkCounters::UNCHARGED,
        )))
    };
    let rejected = baseline
        .fork_workspace(
            fork.clone(),
            PublicationPermit::Unrestricted,
            &mut prefix,
            Some(&mut &mut baseline_guard),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        rejected.into_error(crate::FsError::from),
        crate::FsError::WorkspaceDeleted
    ));
    let guard_work = WorkCounters {
        backend_read_operations: 1,
        authority_bytes_read: 13,
        ..WorkCounters::default()
    };
    for managed in [false, true] {
        let store = MemoryAuthorityStore::default();
        let mut calls = 0;
        let mut guard = |_: WorkBudget| {
            calls += 1;
            std::future::ready(Err(crate::OperationFailure::new(
                crate::FsError::WorkspaceDeleted,
                guard_work,
            )))
        };
        let permit = if managed {
            PublicationPermit::Reservation {
                operation_id: [0xb5; 16],
                gate_tail: 1,
                expected: Head::genesis(Epoch::GENESIS),
            }
        } else {
            PublicationPermit::Unrestricted
        };
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let failure = store
            .fork_workspace(fork.clone(), permit, &mut work, Some(&mut &mut guard))
            .await
            .unwrap_err();
        let failure = failure.into_error(crate::FsError::from);
        if managed {
            assert!(matches!(
                failure,
                crate::FsError::Authority(AuthorityStoreError::RejectedStatic(_))
            ));
            assert_eq!(work.spent().unwrap(), WorkCounters::default());
            assert_eq!(calls, 0);
            assert!(matches!(
                store
                    .head(
                        fork.retention,
                        &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel)
                    )
                    .await,
                Err(AuthorityStoreError::Missing)
            ));
        } else {
            assert!(matches!(failure, crate::FsError::WorkspaceDeleted));
            assert_eq!(
                work.spent().unwrap(),
                prefix.spent().unwrap().checked_add(guard_work)?
            );
            assert_eq!(calls, 1);
        }
        assert!(matches!(
            store
                .head(
                    fork.destination,
                    &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel)
                )
                .await,
            Err(AuthorityStoreError::Missing)
        ));
    }
    Ok(())
}

#[cfg(feature = "conformance")]
#[tokio::test]
async fn object_store_adapters_preserve_dispatch_bytes_and_errors()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::testing::Counted;
    use std::sync::Arc;

    let inner = Arc::new(Counted::new(MemoryObjectStore::default()));
    let objects = Arc::new(crate::CachedObjectStore::with_default_options(
        inner.clone(),
    ));
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let bytes = OwnedBytes::from_static(b"original");
    let id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(&bytes),
    };

    // Dispatch records one request even if its original future is dropped.
    let before = inner.requests().data();
    let pending = objects.put_stored(id, bytes.clone(), &mut work);
    assert_eq!(inner.requests().data(), before + 1);
    drop(pending);
    assert!(!inner.inner().contains(id, &mut work).await?);

    // Negative control: an extra async frame postpones the same admission.
    let before = inner.requests().data();
    let deferred = async { objects.put_stored(id, bytes.clone(), &mut work).await };
    assert_eq!(inner.requests().data(), before);
    deferred.await?;
    assert_eq!(inner.requests().data(), before + 1);

    let before = inner.requests().data();
    let original = objects.read_stored(id, 8, &mut work);
    assert_eq!(inner.requests().data(), before + 1);
    let read = original.await?;
    assert_eq!(read.bytes.as_ref(), bytes.as_ref());
    assert_eq!(read.bytes.as_ptr(), bytes.as_ptr());
    assert!(matches!(
        objects.read_stored(id, 7, &mut work).await,
        Err(ObjectStoreError::TooLarge {
            observed: 8,
            maximum: 7
        })
    ));
    cancel.cancel();
    assert!(matches!(
        objects.read_stored(id, 8, &mut work).await,
        Err(ObjectStoreError::Cancelled)
    ));
    Ok(())
}
