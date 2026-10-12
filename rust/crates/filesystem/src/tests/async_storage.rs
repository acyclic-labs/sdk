use super::*;
use acyclic_stream::OwnedBytes;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{AuthorityStoreError, ObjectKind, ObjectStoreError, object_digest};

enum ScriptedRead {
    CancelAfterSuccess(CancellationToken),
    FailWithOverflowingWork,
}

struct ScriptedAsyncObjectStore {
    read: ScriptedRead,
}

impl ScriptedAsyncObjectStore {
    fn unsupported<T>() -> ObjectResult<T> {
        Err(crate::storage::ObjectFailure::before_work(
            ObjectStoreError::Rejected("unsupported scripted operation".to_owned()),
        ))
    }
}

impl AsyncObjectStore for ScriptedAsyncObjectStore {
    fn put(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> {
        std::future::ready(Self::unsupported())
    }

    fn read(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> {
        let result = match &self.read {
            ScriptedRead::CancelAfterSuccess(cancellation) => {
                cancellation.cancel();
                Ok(crate::OperationReceipt {
                    value: ObjectRead {
                        bytes: OwnedBytes::from_static(b"owned"),
                        retention: crate::storage::ObjectReadRetention::Owned { logical_bytes: 5 },
                    },
                    work: crate::performance::WorkCounters {
                        backend_read_operations: 1,
                        output_bytes: 5,
                        peak_allocation_bytes: 5,
                        ..crate::performance::WorkCounters::default()
                    },
                })
            }
            ScriptedRead::FailWithOverflowingWork => Err(crate::storage::ObjectFailure::new(
                ObjectStoreError::Missing,
                crate::performance::WorkCounters {
                    items_examined: u64::MAX,
                    ..crate::performance::WorkCounters::default()
                },
            )),
        };
        std::future::ready(result)
    }

    fn read_many(
        &self,
        _requests: &[ObjectReadRequest],
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>> {
        std::future::ready(Self::unsupported())
    }

    fn contains(
        &self,
        _object_id: ObjectId,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>> {
        std::future::ready(Self::unsupported())
    }
}

#[tokio::test]
async fn sequential_async_batch_preserves_precancel_midbatch_and_nested_overflow_work()
-> Result<(), Box<dyn std::error::Error>> {
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, b"owned"),
    };
    let requests = [
        ObjectReadRequest {
            object_id,
            maximum_bytes: 5,
        },
        ObjectReadRequest {
            object_id,
            maximum_bytes: 5,
        },
    ];

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let pre_cancelled = (read_many_sequential_async(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::FailWithOverflowingWork,
        },
        &requests,
        WorkBudget::UNBOUNDED,
        &cancelled,
    )).await
    .err()
    .ok_or("pre-cancelled scripted batch succeeded")?;
    assert!(matches!(pre_cancelled.error, ObjectStoreError::Cancelled));
    assert_eq!(
        *pre_cancelled.work,
        crate::performance::WorkCounters::default()
    );

    let midbatch = CancellationToken::new();
    let failure = (read_many_sequential_async(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::CancelAfterSuccess(midbatch.clone()),
        },
        &requests,
        WorkBudget::UNBOUNDED,
        &midbatch,
    )).await
    .err()
    .ok_or("mid-batch cancellation succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert!(failure.work.peak_allocation_bytes >= 5);

    let overflow = (read_many_sequential_async(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::FailWithOverflowingWork,
        },
        &requests[..1],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )).await
    .err()
    .ok_or("overflowing nested work succeeded")?;
    assert!(matches!(overflow.error, ObjectStoreError::Work(_)));
    Ok(())
}

#[tokio::test]
async fn memory_stores_expose_the_complete_async_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let objects = MemoryObjectStore::default();
    let first_bytes = OwnedBytes::from_static(b"first");
    let second_bytes = OwnedBytes::from_static(b"second");
    let first = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
    };
    let second = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
    };
    (AsyncObjectStore::put(
        &objects,
        first,
        first_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (AsyncObjectStore::put(
        &objects,
        second,
        second_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(
        (AsyncObjectStore::contains(
            &objects,
            first,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
    );
    assert_eq!(
        (AsyncObjectStore::read(
            &objects,
            first,
            5,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes.as_ref(),
        first_bytes.as_ref()
    );
    let requests = [
        ObjectReadRequest {
            object_id: second,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: first,
            maximum_bytes: 5,
        },
    ];
    let batched = (AsyncObjectStore::read_many(
        &objects,
        &requests,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(batched.value.len(), 2);
    let sequential = (read_many_sequential_async(
        &objects,
        &requests,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(sequential.value.len(), 2);

    let authorities = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([0x41; 16]);
    let created = (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_authority(&authorities, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert_eq!(
        (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&authorities, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?
        .value,
        head
    );
    let operation_id = OperationId::from_bytes([0x42; 16]);
    let proposed = ProposedCommit {
        operation_id,
        fingerprint: Digest::from_bytes([0x43; 32]),
        payload: bytes::Bytes::from_static(b"payload"),
    };
    let appended = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposed, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = AsyncAuthorityStore::compare_and_append(&authorities, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    assert!(matches!(appended.value, AppendOutcome::Committed(_)));
    assert_eq!(
        (async { let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 1,
                payload_bytes: 7,
            }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = AsyncAuthorityStore::replay(&authorities, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?
        .value
        .len(),
        1
    );
    assert!(
        (async { let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&authorities, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?
        .value
        .is_some()
    );
    let current = (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&authorities, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?
    .value;
    let fenced = (async { let __authority_args = (authority_id, current, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fence(&authorities, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?;
    assert!(matches!(
        fenced.value,
        FenceOutcome::Advanced(head) if head.epoch.get() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn async_adapters_and_explicit_sequential_reads_fail_closed_before_or_at_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let authority = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([0x51; 16]);
    let head = Head::genesis(Epoch::GENESIS);
    let operation_id = OperationId::from_bytes([0x52; 16]);
    let commit = ProposedCommit {
        operation_id,
        fingerprint: Digest::from_bytes([0x53; 32]),
        payload: bytes::Bytes::from_static(b"cancelled"),
    };
    let authority_failures = [
        (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_authority(&authority, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&authority, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, Epoch::GENESIS, head, commit, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = AsyncAuthorityStore::compare_and_append(&authority, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 1,
                payload_bytes: 1,
            }, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = AsyncAuthorityStore::replay(&authority, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fence(&authority, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&authority, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
    ];
    for failure in authority_failures {
        let failure = failure.err().ok_or("cancelled authority call succeeded")?;
        assert!(matches!(failure.error, AuthorityStoreError::Cancelled));
        assert_eq!(*failure.work, crate::performance::WorkCounters::default());
    }

    let objects = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, b"cancelled"),
    };
    let request = ObjectReadRequest {
        object_id,
        maximum_bytes: 9,
    };
    let object_failures = [
        (AsyncObjectStore::put(
            &objects,
            object_id,
            OwnedBytes::from_static(b"cancelled"),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )).await
        .map(|_| ()),
        (AsyncObjectStore::read(
            &objects,
            object_id,
            9,
            WorkBudget::UNBOUNDED,
            &cancelled,
        )).await
        .map(|_| ()),
        (AsyncObjectStore::read_many(
            &objects,
            std::slice::from_ref(&request),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )).await
        .map(|_| ()),
        (AsyncObjectStore::contains(
            &objects,
            object_id,
            WorkBudget::UNBOUNDED,
            &cancelled,
        )).await
        .map(|_| ()),
    ];
    for failure in object_failures {
        let failure = failure.err().ok_or("cancelled object call succeeded")?;
        assert!(matches!(failure.error, ObjectStoreError::Cancelled));
        assert_eq!(*failure.work, crate::performance::WorkCounters::default());
    }

    let active = CancellationToken::new();
    let empty = (read_many_sequential_async(
        &objects,
        &[],
        WorkBudget::UNBOUNDED,
        &active,
    )).await
    .err()
    .ok_or("empty sequential batch succeeded")?;
    assert!(matches!(empty.error, ObjectStoreError::Rejected(_)));
    let missing = (read_many_sequential_async(
        &objects,
        std::slice::from_ref(&request),
        WorkBudget::UNBOUNDED,
        &active,
    )).await
    .err()
    .ok_or("missing sequential batch succeeded")?;
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.items_examined = 0;
    let over_budget = (read_many_sequential_async(
        &objects,
        std::slice::from_ref(&request),
        denied,
        &active,
    )).await
    .err()
    .ok_or("over-budget sequential batch succeeded")?;
    assert!(matches!(over_budget.error, ObjectStoreError::Work(_)));
    Ok(())
}

#[tokio::test]
async fn default_guarded_fork_preserves_typed_guard_failure_and_retention_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fork = WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([0x91; 16]),
        creation: ProposedCommit {
            operation_id: OperationId::from_bytes([0x92; 16]),
            fingerprint: Digest::from_bytes([0x93; 32]),
            payload: bytes::Bytes::from_static(b"creation"),
        },
        retention: AuthorityId::from_bytes([0x94; 16]),
        retained: ProposedCommit {
            operation_id: OperationId::from_bytes([0x95; 16]),
            fingerprint: Digest::from_bytes([0x96; 32]),
            payload: bytes::Bytes::from_static(b"retention"),
        },
    };
    let baseline_store = MemoryAuthorityStore::default();
    let mut baseline_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let result = crate::async_storage::first_record(&baseline_store, fork.retention, fork.retained.clone(), &mut baseline_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity));
    let baseline = baseline_work.receipt(result)?;
    let guard_work = WorkCounters {
        backend_read_operations: 1,
        authority_bytes_read: 13,
        ..WorkCounters::default()
    };
    let mut calls = 0;
    let mut guard = |_budget: WorkBudget| {
        calls += 1;
        std::future::ready(Err(crate::performance::OperationFailure::new(
            crate::facade::FsError::WorkspaceDeleted,
            guard_work,
        )))
    };
    let store = MemoryAuthorityStore::default();
    let failure = (async { let __authority_args = (fork.clone(), PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fork_workspace(&store, __authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).await
        .err().ok_or("guarded fork ignored rejection")?;
    assert!(matches!(failure.error, crate::facade::FsError::WorkspaceDeleted));
    assert_eq!(*failure.work, baseline.work.checked_add(guard_work)?);
    assert_eq!(calls, 1);
    let missing = (async { let __authority_args = (fork.destination, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&store, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
        .err().ok_or("guard rejection published destination")?;
    assert!(matches!(missing.error, AuthorityStoreError::Missing));
    Ok(())
}

#[tokio::test]
async fn default_guarded_fork_rejects_managed_permit_before_retention_or_guard()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let cancellation = CancellationToken::new();
    let commit = ProposedCommit {
        operation_id: OperationId::from_bytes([0xb1; 16]),
        fingerprint: Digest::from_bytes([0xb2; 32]),
        payload: bytes::Bytes::from_static(b"record"),
    };
    let fork = WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([0xb3; 16]),
        creation: commit.clone(),
        retention: AuthorityId::from_bytes([0xb4; 16]),
        retained: commit,
    };
    let mut calls = 0;
    let mut guard = |_budget: WorkBudget| {
        calls += 1;
        std::future::ready(Ok(crate::facade::FsReceipt {
            value: (), work: WorkCounters::default(),
        }))
    };
    let failure = (async { let __authority_args = (fork.clone(), PublicationPermit::Reservation {
            operation_id: [0xb5; 16],
            gate_tail: 1,
            expected: Head::genesis(Epoch::GENESIS),
        }, WorkBudget::UNBOUNDED, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fork_workspace(&store, __authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).await
        .err().ok_or("managed fork permit was silently stripped")?;
    assert!(matches!(
        failure.error,
        crate::facade::FsError::Authority(AuthorityStoreError::Rejected(_))
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(calls, 0);
    for authority in [fork.retention, fork.destination] {
        let failure = (async { let __authority_args = (authority, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&store, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
            .err().ok_or("managed rejection created authority")?;
        assert!(matches!(failure.error, AuthorityStoreError::Missing));
    }
    Ok(())
}
