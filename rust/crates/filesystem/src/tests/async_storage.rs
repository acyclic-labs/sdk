use super::*;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{AuthorityStoreError, ObjectKind, ObjectStoreError, object_digest};
use acyclic_stream::OwnedBytes;

enum ScriptedRead {
    CancelAfterSuccess(CancellationToken),
    FailWithOverflowingWork,
}

impl AsyncObjectStore for ScriptedRead {
    async fn put(
        &self,
        _: ObjectId,
        _: OwnedBytes,
        _: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::Rejected(
            "unsupported scripted operation".into(),
        ))
    }
    async fn read(
        &self,
        _: ObjectId,
        _: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        match self {
            Self::CancelAfterSuccess(cancel) => {
                cancel.cancel();
                work.work = work.work.checked_add(WorkCounters {
                    backend_read_operations: 1,
                    output_bytes: 5,
                    peak_allocation_bytes: 5,
                    ..WorkCounters::default()
                })?;
                Ok(ObjectRead {
                    bytes: OwnedBytes::from_static(b"owned"),
                    retention: crate::storage::ObjectReadRetention::Owned { logical_bytes: 5 },
                })
            }
            Self::FailWithOverflowingWork => {
                work.work = work.work.checked_add(WorkCounters {
                    items_examined: u64::MAX,
                    ..WorkCounters::default()
                })?;
                Err(ObjectStoreError::Missing)
            }
        }
    }
    async fn read_many(
        &self,
        _: &[ObjectReadRequest],
        _: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        Err(ObjectStoreError::Rejected(
            "unsupported scripted operation".into(),
        ))
    }
    async fn contains(
        &self,
        _: ObjectId,
        _: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        Err(ObjectStoreError::Rejected(
            "unsupported scripted operation".into(),
        ))
    }
}

#[tokio::test]
async fn sequential_batch_preserves_cancellation_and_nested_overflow_work()
-> Result<(), Box<dyn std::error::Error>> {
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, b"owned"),
    };
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 5,
    }; 2];
    let stopped = CancellationToken::new();
    stopped.cancel();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &stopped);
    assert!(matches!(
        read_many_sequential(&ScriptedRead::FailWithOverflowingWork, &requests, &mut work).await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(work.spent(), WorkCounters::default());
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    assert!(matches!(
        read_many_sequential(
            &ScriptedRead::CancelAfterSuccess(cancel.clone()),
            &requests,
            &mut work
        )
        .await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(work.spent().backend_read_operations, 1);
    assert!(work.spent().peak_allocation_bytes >= 5);
    let live = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &live);
    assert!(matches!(
        read_many_sequential(
            &ScriptedRead::FailWithOverflowingWork,
            &requests[..1],
            &mut work
        )
        .await,
        Err(ObjectStoreError::Work(_))
    ));
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
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, bytes),
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
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, b"cancelled"),
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
