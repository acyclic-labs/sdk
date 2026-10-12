use super::*;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{AuthorityStoreError, ObjectKind, ObjectStoreError, object_digest};
use acyclic_stream::OwnedBytes;

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
    async fn put(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let _budget = context.work.remaining(context.budget)?;
        let _cancellation = context.cancellation;
        let result = ({
        std::future::ready(Self::unsupported())
    }).await;
        context.accept(result)
    }

    async fn read(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let _budget = context.read_budget()?;
        let _cancellation = context.cancellation;
        let result = ({
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
    })
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        _requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let _budget = context.work.remaining(context.budget)?;
        let _cancellation = context.cancellation;
        let result = ({
        std::future::ready(Self::unsupported())
    }).await;
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(
        &self,
        _object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let _budget = context.work.remaining(context.budget)?;
        let _cancellation = context.cancellation;
        let result = ({
        std::future::ready(Self::unsupported())
    }).await;
        context.accept(result)
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
    let pre_cancelled = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
        let result = read_many_sequential(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::FailWithOverflowingWork,
        },
        &requests,
            &mut work,
        )
        .await
        .map(|(values, _)| values);
        work.receipt(result)
    }
    .err()
    .ok_or("pre-cancelled scripted batch succeeded")?;
    assert!(matches!(pre_cancelled.error, ObjectStoreError::Cancelled));
    assert_eq!(
        *pre_cancelled.work,
        crate::performance::WorkCounters::default()
    );

    let midbatch = CancellationToken::new();
    let failure = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &midbatch);
        let result = read_many_sequential(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::CancelAfterSuccess(midbatch.clone()),
        },
        &requests,
            &mut work,
        )
        .await
        .map(|(values, _)| values);
        work.receipt(result)
    }
    .err()
    .ok_or("mid-batch cancellation succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert!(failure.work.peak_allocation_bytes >= 5);

    let overflow = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let result = read_many_sequential(
        &ScriptedAsyncObjectStore {
            read: ScriptedRead::FailWithOverflowingWork,
        },
        &requests[..1],
            &mut work,
        )
        .await
        .map(|(values, _)| values);
        work.receipt(result)
    }
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
    (async {
        let __object_args = (
            first,
            first_bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::put(
        &objects,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    })
    .await?;
    (async {
        let __object_args = (
            second,
            second_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::put(
        &objects,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }).await?;
    assert!(
        (async {
            let __object_args = (first, WorkBudget::UNBOUNDED, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result =
                AsyncObjectStore::contains(
            &objects, __object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }).await?
        .value
    );
    assert_eq!(
        (async {
            let __object_args = (first, 5, WorkBudget::UNBOUNDED, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = AsyncObjectStore::read(
            &objects,
                __object_args.0,
                __object_args.1,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }).await?
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
    let batched = (async {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::read_many(
        &objects, __object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }).await?;
    assert_eq!(batched.value.len(), 2);
    let sequential = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let result = read_many_sequential(&objects, &requests, &mut work)
            .await
            .map(|(values, _)| values);
        work.receipt(result)
    }?;
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
    let appended = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposed, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = AsyncAuthorityStore::compare_and_append(&authorities, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
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
        (async { let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&authorities, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?
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
        (async { let __authority_args = (authority_id, Epoch::GENESIS, head, commit, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = AsyncAuthorityStore::compare_and_append(&authority, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 1,
                payload_bytes: 1,
            }, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = AsyncAuthorityStore::replay(&authority, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fence(&authority, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
        .map(|_| ()),
        (async { let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancelled,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&authority, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
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
        (async {
            let __object_args = (
                object_id,
                OwnedBytes::from_static(b"cancelled"),
                WorkBudget::UNBOUNDED,
                &cancelled,
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = AsyncObjectStore::put(
            &objects,
                __object_args.0,
                __object_args.1,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        })
        .await
        .map(|_| ()),
        (async {
            let __object_args = (object_id, 9,
            WorkBudget::UNBOUNDED,
            &cancelled,
        );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = AsyncObjectStore::read(
                &objects,
                __object_args.0,
                __object_args.1,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }).await
        .map(|_| ()),
        (async {
            let __object_args = (
                std::slice::from_ref(&request),
                WorkBudget::UNBOUNDED,
                &cancelled,
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result =
                AsyncObjectStore::read_many(&objects, __object_args.0, &mut __object_work)
                    .await
                    .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        })
        .await
        .map(|_| ()),
        (async {
            let __object_args = (object_id, WorkBudget::UNBOUNDED,
            &cancelled,
        );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result =
                AsyncObjectStore::contains(&objects, __object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }).await
        .map(|_| ()),
    ];
    for failure in object_failures {
        let failure = failure.err().ok_or("cancelled object call succeeded")?;
        assert!(matches!(failure.error, ObjectStoreError::Cancelled));
        assert_eq!(*failure.work, crate::performance::WorkCounters::default());
    }

    let active = CancellationToken::new();
    let empty = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &active);
        let result = read_many_sequential(
        &objects,
        &[], &mut work)
            .await
            .map(|(values, _)| values);
        work.receipt(result)
    }
    .err()
    .ok_or("empty sequential batch succeeded")?;
    assert!(matches!(empty.error, ObjectStoreError::Rejected(_)));
    let missing = {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &active);
        let result = read_many_sequential(
        &objects,
        std::slice::from_ref(&request), &mut work)
            .await
            .map(|(values, _)| values);
        work.receipt(result)
    }
    .err()
    .ok_or("missing sequential batch succeeded")?;
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.items_examined = 0;
    let over_budget = {
        let mut work = crate::AuthorityWork::new(denied, &active);
        let result = read_many_sequential(
        &objects,
        std::slice::from_ref(&request), &mut work)
            .await
            .map(|(values, _)| values);
        work.receipt(result)
    }
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
