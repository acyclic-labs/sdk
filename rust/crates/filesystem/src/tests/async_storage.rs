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
            assert_eq!(work.spent().peak_allocation_bytes, 31 + output_bytes);
            assert_eq!(work.spent().bytes_copied, 2 * bytes.len() as u64);
            assert_eq!(work.allocations.live_bytes(), 31 + output_bytes);
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
            assert_eq!(work.spent().bytes_copied, bytes.len() as u64);
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
    let before = work.spent();
    work.budget.bytes_copied = before.bytes_copied;
    assert!(matches!(
        store.read(object_id, bytes.len() as u64, &mut work).await,
        Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        }))
    ));
    assert_eq!(work.spent().bytes_copied, before.bytes_copied);
    assert_eq!(work.allocations.live_bytes(), 31);
    earlier.retire(&mut work.accounting())?;
    assert_eq!(work.allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn memory_stores_expose_the_complete_async_contract() -> Result<(), Box<dyn std::error::Error>>
{
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let objects = MemoryObjectStore::default();
    let mut ids = Vec::new();
    for bytes in [b"first".as_slice(), b"second".as_slice()] {
        let id = ObjectId {
            storage: None,
            kind: ObjectKind::BlobChunk,
            digest: object_digest(bytes),
        };
        objects
            .put(id, OwnedBytes::copy_from_slice(bytes), &mut work)
            .await?;
        ids.push(id);
    }
    assert!(objects.contains(ids[0], &mut work).await?);
    assert_eq!(
        objects.read(ids[0], 5, &mut work).await?.bytes.as_ref(),
        b"first"
    );
    let requests = [
        ObjectReadRequest {
            object_id: ids[1],
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: ids[0],
            maximum_bytes: 5,
        },
    ];
    for values in [
        objects.read_many(&requests, &mut work).await?.0,
        read_many_sequential(&objects, &requests, &mut work)
            .await?
            .0,
    ] {
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].bytes.as_ref(), b"second");
        assert_eq!(values[1].bytes.as_ref(), b"first");
    }
    let authorities = MemoryAuthorityStore::default();
    let authority = AuthorityId::from_bytes([0x41; 16]);
    let created = authorities
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let CreateAuthorityOutcome::Created(head) = created else {
        return Err("authority existed".into());
    };
    assert_eq!(authorities.head(authority, &mut work).await?, head);
    let operation = OperationId::from_bytes([0x42; 16]);
    let appended = authorities
        .compare_and_append(
            GuardedAppend {
                authority_id: authority,
                epoch: Epoch::GENESIS,
                expected: head,
                commit: ProposedCommit {
                    publication: None,
                    operation_id: operation,
                    fingerprint: Digest::from_bytes([0x43; 32]),
                    payload: bytes::Bytes::from_static(b"payload"),
                },
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(appended, AppendOutcome::Committed(_)));
    assert_eq!(
        authorities
            .replay(
                authority,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 7
                },
                &mut work
            )
            .await?
            .0
            .len(),
        1
    );
    assert!(
        authorities
            .find_operation(authority, operation, &mut work)
            .await?
            .is_some()
    );
    let current = authorities.head(authority, &mut work).await?;
    assert!(
        matches!(authorities.fence(authority, current, &mut work).await?, FenceOutcome::Advanced(head) if head.epoch.get() == 2)
    );
    Ok(())
}

#[tokio::test]
async fn adapters_reject_cancellation_and_sequential_read_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
    let authority = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0x51; 16]);
    let head = Head::genesis(Epoch::GENESIS);
    let operation = OperationId::from_bytes([0x52; 16]);
    let request = GuardedAppend {
        authority_id: id,
        epoch: Epoch::GENESIS,
        expected: head,
        commit: ProposedCommit {
            publication: None,
            operation_id: operation,
            fingerprint: Digest::from_bytes([0x53; 32]),
            payload: bytes::Bytes::from_static(b"cancelled"),
        },
        permit: PublicationPermit::Unrestricted,
    };
    for result in [
        authority
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await
            .map(|_| ())
            .map_err(|error| error.into_error(std::convert::identity)),
        authority.head(id, &mut work).await.map(|_| ()),
        authority
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map(|_| ())
            .map_err(|error| error.into_error(std::convert::identity)),
        authority
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 1,
                },
                &mut work,
            )
            .await
            .map(|_| ()),
        authority.fence(id, head, &mut work).await.map(|_| ()),
        authority
            .find_operation(id, operation, &mut work)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(result, Err(AuthorityStoreError::Cancelled)));
    }
    assert_eq!(work.spent(), WorkCounters::default());
    let objects = MemoryObjectStore::default();
    let object_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(b"cancelled"),
    };
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 9,
    }];
    for result in [
        objects
            .put(object_id, OwnedBytes::from_static(b"cancelled"), &mut work)
            .await,
        objects.read(object_id, 9, &mut work).await.map(|_| ()),
        objects.read_many(&requests, &mut work).await.map(|_| ()),
        objects.contains(object_id, &mut work).await.map(|_| ()),
    ] {
        assert!(matches!(result, Err(ObjectStoreError::Cancelled)));
    }
    assert_eq!(work.spent(), WorkCounters::default());
    let live = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &live);
    assert!(matches!(
        read_many_sequential(&objects, &[], &mut work).await,
        Err(ObjectStoreError::EmptyReadBatch)
    ));
    assert!(matches!(
        read_many_sequential(&objects, &requests, &mut work).await,
        Err(ObjectStoreError::Missing)
    ));
    let mut denied = AuthorityWork::new(
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
        &live,
    );
    assert!(matches!(
        read_many_sequential(&objects, &requests, &mut denied).await,
        Err(ObjectStoreError::Work(_))
    ));
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
            assert_eq!(work.spent(), WorkCounters::default());
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
            assert_eq!(work.spent(), prefix.spent().checked_add(guard_work)?);
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
