use super::*;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::foundation::Digest;
use crate::storage::FenceOutcome;
use acyclic_stream::OwnedBytes;

#[tokio::test]
async fn authority_is_fenced_and_replay_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([1; 16]);
    let created = ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    let commit = ProposedCommit {
        operation_id: OperationId::from_bytes([2; 16]),
        fingerprint: Digest::from_bytes([3; 32]),
        payload: bytes::Bytes::from_static(b"payload"),
    };
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, head, commit, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::Committed(_)
    ));
    let current = ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
    let fenced = ({ let __authority_args = (authority_id, current, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let page = ({ let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
            records: 1,
            payload_bytes: 7,
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    assert_eq!(page.value.len(), 1);
    Ok(())
}

#[tokio::test]
async fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"abcdef"),
    };
    {
        let __object_args = (
            object_id,
            OwnedBytes::from_static(b"abcdef"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(
        {
            let __object_args = (
                object_id,
                6,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?
        .value,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                5,
                WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_batch_is_ordered_deduplicated_by_storage_and_one_backend_read()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"first"),
    };
    let second = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"second"),
    };
    {
        let __object_args = (
            first,
            OwnedBytes::from_static(b"first"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    {
        let __object_args = (
            second,
            OwnedBytes::from_static(b"second"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    let receipt = {
        let __object_args = (
            &[
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
            ], WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }?;
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
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.object_probes, 3);
    assert_eq!(receipt.work.items_returned, 3);
    assert!(matches!(
        {
            let __object_args = (&[], WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store
                .read_many(__object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
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
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"authentic"),
    };
    {
        let __object_args = (
            object_id,
            OwnedBytes::from_static(b"authentic"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    let mut state = store.objects.write().map_err(|_| "memory object lock poisoned")?;
    let index = state.objects.binary_search_by_key(&object_id, |(id, _)| *id)
        .map_err(|_| "resident object is missing")?;
    state.objects[index].1 = OwnedBytes::from_static(b"corrupted");
    drop(state);
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                64,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = {
        let __object_args = (
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 64,
            }], WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .err()
        .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(batch_failure.work.object_probes, 1);
    assert_eq!(batch_failure.work.backend_read_operations, 1);
    assert_eq!(batch_failure.work.object_bytes_read, 9);
    Ok(())
}

#[tokio::test]
async fn authority_payload_and_replay_bounds_preserve_forward_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::new(2)?;
    let authority_id = AuthorityId::from_bytes([11; 16]);
    let head = match ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?
        .value
    {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    let commit = ProposedCommit {
        operation_id: OperationId::from_bytes([12; 16]),
        fingerprint: Digest::from_bytes([13; 32]),
        payload: bytes::Bytes::from_static(b"ab"),
    };
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, head, commit, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::Committed(_)
    ));
    assert!(matches!(
        ({ let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 1,
                payload_bytes: 1,
            }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayRecordTooLarge {
                observed: 2,
                maximum: 1,
            },
            ..
        })
    ));
    let oversized_cancellation = crate::CancellationToken::new();
    let mut oversized_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &oversized_cancellation);
    let oversized_head = store.head(authority_id, &mut oversized_work).await?;
    let oversized = ProposedCommit {
        operation_id: OperationId::from_bytes([14; 16]),
        fingerprint: Digest::from_bytes([15; 32]),
        payload: bytes::Bytes::from_static(b"abc"),
    };
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, oversized_head, oversized, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::PayloadTooLarge {
                observed: 3,
                maximum: 2,
            },
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn replay_stops_at_the_payload_bound_after_contiguous_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([16; 16]);
    let created = ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let CreateAuthorityOutcome::Created(mut head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    for (operation, fingerprint, payload) in [
        ([17; 16], [18; 32], bytes::Bytes::from_static(b"aa")),
        ([19; 16], [20; 32], bytes::Bytes::from_static(b"bb")),
    ] {
        let appended = ({ let __authority_args = (authority_id, Epoch::GENESIS, head, ProposedCommit {
                operation_id: OperationId::from_bytes(operation),
                fingerprint: Digest::from_bytes(fingerprint),
                payload,
            }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
        let AppendOutcome::Committed(commit) = appended.value else {
            return Err("authority append did not commit".into());
        };
        head = Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        };
    }
    let replay = ({ let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
            records: 2,
            payload_bytes: 3,
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    assert_eq!(replay.value.len(), 1);
    assert_eq!(replay.value[0].payload.as_ref(), b"aa");
    assert_eq!(replay.work.authority_records_read, 1);
    assert_eq!(replay.work.authority_bytes_read, 2);
    Ok(())
}

#[tokio::test]
async fn authority_outcomes_are_exactly_idempotent_conflicted_and_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        MemoryAuthorityStore::new(0),
        Err(AuthorityStoreError::PayloadTooLarge {
            observed: 0,
            maximum: 0,
        })
    ));
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([21; 16]);
    assert!(matches!(
        ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let created = ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let CreateAuthorityOutcome::Created(genesis) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?
            .value,
        CreateAuthorityOutcome::Existing(actual) if actual == genesis
    ));
    let operation_id = OperationId::from_bytes([22; 16]);
    let fingerprint = Digest::from_bytes([23; 32]);
    let proposed = ProposedCommit {
        operation_id,
        fingerprint,
        payload: bytes::Bytes::from_static(b"durable"),
    };
    let committed = ({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, proposed.clone(), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    let AppendOutcome::Committed(durable) = committed.value else {
        return Err("first append was not committed".into());
    };
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, proposed.clone(), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::AlreadyCommitted(actual) if actual == durable
    ));
    assert!(matches!(
        ({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, ProposedCommit {
                    fingerprint: Digest::from_bytes([24; 32]),
                    ..proposed
                }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } if committed_fingerprint == fingerprint
    ));
    assert_eq!(
        ({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        Some(durable.clone())
    );
    assert!(
        ({ let __authority_args = (authority_id, OperationId::from_bytes([25; 16]), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value
            .is_none()
    );
    let current = ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
    let stale = ({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, ProposedCommit {
            operation_id: OperationId::from_bytes([26; 16]),
            fingerprint: Digest::from_bytes([27; 32]),
            payload: bytes::Bytes::from_static(b"stale"),
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    assert!(matches!(
        stale.value,
        AppendOutcome::Conflict { actual } if actual == current
    ));
    let fenced = ({ let __authority_args = (authority_id, current, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let repeated_fence = ({ let __authority_args = (authority_id, current, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
    assert_eq!(
        repeated_fence.value,
        FenceOutcome::Conflict {
            actual: fenced_head
        }
    );
    let wrong_epoch = ({ let __authority_args = (authority_id, Epoch::GENESIS, fenced_head, ProposedCommit {
            operation_id: OperationId::from_bytes([28; 16]),
            fingerprint: Digest::from_bytes([29; 32]),
            payload: bytes::Bytes::from_static(b"fenced"),
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    assert!(matches!(
        wrong_epoch.value,
        AppendOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));
    for invalid in [
        ReplayLimit {
            records: 0,
            payload_bytes: 1,
        },
        ReplayLimit {
            records: 1,
            payload_bytes: 0,
        },
    ] {
        assert!(matches!(
            ({ let __authority_args = (authority_id, Sequence::GENESIS, invalid, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }),
            Err(AuthorityFailure {
                error: AuthorityStoreError::InvalidReplayLimit,
                ..
            })
        ));
    }
    assert!(matches!(
        ({ let __authority_args = (authority_id, Sequence::new(2), ReplayLimit {
                records: 1,
                payload_bytes: 16,
            }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayCursorAhead { .. },
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_admission_rejects_size_identity_absence_and_batch_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        MemoryObjectStore::new(0),
        Err(ObjectStoreError::TooLarge {
            observed: 0,
            maximum: 0,
        })
    ));
    let store = MemoryObjectStore::new(4)?;
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"data"),
    };
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                OwnedBytes::from_static(b"oversized"),
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                OwnedBytes::from_static(b"nope"),
            WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(
        !{
            let __object_args = (
                object_id,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(matches!(
        {
            let __object_args = (
                object_id,
                4,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    {
        let __object_args = (
            object_id,
            OwnedBytes::from_static(b"data"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    {
        let __object_args = (
            object_id,
            OwnedBytes::from_static(b"data"),
            WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert!(matches!(
        {
            let __object_args = (
                &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 3,
            }],
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store
                .read_many(__object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge {
                observed: 4,
                maximum: 3,
            },
            ..
        })
    ));
    let missing = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: Digest::from_bytes([31; 32]),
    };
    assert!(matches!(
        {
            let __object_args = (
                &[crate::storage::ObjectReadRequest {
                object_id: missing,
                maximum_bytes: 4,
            }],
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store
                .read_many(__object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        {
            let __object_args = (object_id, denied, &crate::CancellationToken::new());
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_authority_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_records_appended: 32,
        authority_bytes_read: 131_072,
        authority_bytes_written: 131_072,
        backend_read_operations: 32,
        backend_write_operations: 32,
        bytes_hashed: 131_072,
        bytes_copied: 131_072,
        items_examined: 128,
        allocation_operations: 32,
        peak_allocation_bytes: 131_072,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_authority_fixture()
-> Result<(MemoryAuthorityStore, AuthorityId, DurableCommit), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let budget = native_authority_budget();
    let CreateAuthorityOutcome::Created(head) =
        ({ let __authority_args = (id, Epoch::GENESIS, budget, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?.value
    else { return Err("native fixture authority already exists".into()); };
    let AppendOutcome::Committed(commit) = ({ let __authority_args = (id, Epoch::GENESIS, head, ProposedCommit {
            operation_id: OperationId::from_bytes([0xe2; 16]),
            fingerprint: Digest::from_bytes([0xe3; 32]),
            payload: bytes::Bytes::from(vec![0x5a; 8_192]),
        }, budget, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?.value else { return Err("native fixture did not append".into()); };
    Ok((store, id, commit))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authority_checks_both_cancellation_roots_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {

    use crate::materializer::NativeMaterializationWork;
    let (store, id, committed) = native_authority_fixture().await?;
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let active = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &root)
        .map_err(|(error, _)| error)?;
    let head = ({ let __authority_args = (id, native_authority_budget(), &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
    let before = work.receipt();
    supplied.cancel();
    for cancellation in [&supplied, &active] {
        if std::ptr::eq(cancellation, &active) { root.cancel(); }
        assert!(({ let __authority_args = (id, &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&store, __authority_args.0, &mut __authority_work).await; __authority_result }).is_err());
        assert!(({ let __authority_args = (id, Sequence::GENESIS, ReplayLimit { records: 1, payload_bytes: 8_192 }, &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.3, __authority_args.4); let __authority_result = AsyncAuthorityStore::replay(&store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }).is_err());
        assert!(({ let __authority_args = (id, head, &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fence(&store, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result }).is_err());
        assert!(({ let __authority_args = (id, committed.operation_id, &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&store, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }).is_err());
        assert_eq!(work.receipt(), before);
    }
    assert_eq!(({ let __authority_args = (id, native_authority_budget(), &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value, head);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_object_budget() -> WorkBudget {
    WorkBudget {
        object_probes: 128, object_bytes_read: 131_072, object_bytes_written: 131_072,
        bytes_hashed: 131_072, bytes_copied: 131_072,
        backend_read_operations: 32, backend_write_operations: 32,
        allocation_operations: 32, peak_allocation_bytes: 131_072,
        items_examined: 128, items_returned: 128,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_object_put_denies_table_allocation_before_backend_write()
-> Result<(), Box<dyn std::error::Error>> {

    use crate::materializer::NativeMaterializationWork;
    let cancellation = crate::CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1, // Only the original meter's own allocation.
        ..native_object_budget()
    };
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let store = MemoryObjectStore::default();
    let object = crate::storage::HashedObject::new(crate::ObjectKind::BlobChunk, OwnedBytes::from_static(b"body"));
    let result = {
        let __object_args = (object.clone(), &work, &cancellation);
        let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::put_hashed(&store, __object_args.0, &mut __object_work).await;
        __object_result
    };
    assert!(matches!(
        result,
        Err(ObjectStoreError::Native(
            crate::materializer::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.receipt().bytes_hashed, 0);
    assert_eq!(work.receipt().object_bytes_written, 0);
    assert!(
        !{
            let __object_args = (
                object.object_id(),
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?.value);
    assert!(store.objects.read().map_err(|_| "object table lock poisoned")?.objects.is_empty());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_object_intrinsic_payload_lives_until_last_resident_alias()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncObjectStore;
    use crate::materializer::NativeMaterializationWork;
    use std::sync::Arc;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_object_budget(), &cancellation).map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    let mut allocation = work.claim(4, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(4)?;
    allocation.resize_retained(bytes.capacity() as u64)?;
    bytes.extend_from_slice(b"body");
    let object = crate::storage::HashedObject::from_native_vec(
        crate::ObjectKind::BlobChunk, &mut bytes, &mut allocation, &work, &cancellation,
    )?;
    assert!(bytes.is_empty());
    assert_eq!(allocation.bytes(), 0);
    assert_eq!(work.receipt().bytes_hashed, 4 + OBJECT_DIGEST_ENVELOPE_BYTES);
    let id = object.object_id();
    let store = Arc::new(MemoryObjectStore::default());
    (async {
        let __object_args = (object, &work, &cancellation);
        let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::put_hashed(&store, __object_args.0, &mut __object_work).await;
        __object_result
    }).await?;
    assert_eq!(work.receipt().bytes_hashed, 4 + OBJECT_DIGEST_ENVELOPE_BYTES);
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert_eq!(work.receipt().object_bytes_written, 4);
    let body_live = work.live_allocation_bytes();
    let view = {
        let __object_args = (
            id,
            4,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(&*store,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let reader = NativeMaterializationWork::new(native_object_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let reader_before = reader.receipt();
    let native_view = {
        let __object_args = (id, 4, &reader, &cancellation);
        let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(
            &*store,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_result
    }?;
    assert_eq!(
        reader.receipt().allocation_operations,
        reader_before.allocation_operations
    );
    assert_eq!(work.live_allocation_bytes(), body_live);
    assert_eq!(native_view.bytes.as_ptr(), view.bytes.as_ptr());
    let alias_before = work.receipt();
    let alias = native_view.bytes.clone();
    assert_eq!(work.receipt(), alias_before);
    assert_eq!(alias.as_ptr(), view.bytes.as_ptr());
    {
        let __object_args = (
            id,
            OwnedBytes::from_static(b"body"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = // A duplicate ordinary input leaves the original resident carrier intact.
    AsyncObjectStore::put(&*store, __object_args.0, __object_args.1, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(work.live_allocation_bytes(), body_live);
    drop(native_view);
    // Table growth moves the carrier without replacing its payload controls.
    let second = crate::storage::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"second"),
    );
    {
        let __object_args = (second, &work, &cancellation);
        let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::put_hashed(&*store, __object_args.0, &mut __object_work).await;
        __object_result
    }?;
    let resident = {
        let __object_args = (
            id,
            4,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(&*store,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }?.value;
    assert_eq!(resident.bytes.as_ptr(), alias.as_ptr());
    drop(resident);
    // Removing the map's alias cannot release custody retained by either read.
    let mut state = store.objects.write().map_err(|_| "object table lock poisoned")?;
    let index = state.objects.binary_search_by_key(&id, |(id, _)| *id)
        .map_err(|_| "resident object is missing")?;
    drop(state.objects.remove(index));
    drop(state);
    assert!(work.live_allocation_bytes() > baseline);
    drop(store);
    assert!(work.live_allocation_bytes() < body_live);
    assert!(work.live_allocation_bytes() > baseline);
    assert_eq!(view.bytes.as_ref(), b"body");
    assert_eq!(alias.as_ptr(), view.bytes.as_ptr());
    drop(view);
    assert!(work.live_allocation_bytes() > baseline);
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}



#[cfg(not(target_arch = "wasm32"))]
fn native_mutation_request(authority: AuthorityId, expected: Head) -> crate::storage::GuardedAppend {
    crate::storage::GuardedAppend {
        authority_id: authority, epoch: expected.epoch, expected,
        commit: ProposedCommit {
            operation_id: OperationId::from_bytes([0x91; 16]),
            fingerprint: Digest::from_bytes([0x92; 32]),
            payload: bytes::Bytes::from_static(b"canonical configuration bytes"),
        },
        permit: crate::storage::PublicationPermit::Unrestricted,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_first_record_is_atomic_exact_and_ordinary_byte_compatible()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let native = std::sync::Arc::new(MemoryAuthorityStore::default());
    let ordinary = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x90; 16]);
    let proposed = native_mutation_request(id, Head::genesis(Epoch::GENESIS)).commit;
    assert!((async { let __authority_args = (id, proposed.clone(), &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&native, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }).await?);
    let created = work.receipt();
    assert_eq!(created.backend_write_operations, 1);
    assert_eq!(created.authority_records_appended, 1);
    assert_eq!(created.allocation_operations, 4); // Root, history vector, two real map growth attempts.
    let state = NativeMaterializationWork::accounting_state_bytes();
    let retained = (size_of::<(AuthorityId, AuthorityState)>()
        + size_of::<DurableCommit>() + size_of::<(OperationId, (usize, KernelOwnedAllocation))>()) as u64;
    assert_eq!(created.peak_allocation_bytes, state + retained);
    let ordinary_receipt = (async { let __authority_args = (id, proposed.clone(), native_authority_budget(), &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&ordinary, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    assert!(ordinary_receipt.value);
    assert_eq!(ordinary_receipt.work.backend_write_operations, 1);
    let limit = ReplayLimit { records: 1, payload_bytes: 1024 };
    assert_eq!(({ let __authority_args = (id, Sequence::GENESIS, limit, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = AsyncAuthorityStore::replay(native.as_ref(), __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?.value,({ let __authority_args = (id, Sequence::GENESIS, limit, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =
        AsyncAuthorityStore::replay(ordinary.as_ref(), __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?.value);
    assert!((async { let __authority_args = (id, proposed.clone(), &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&native, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }).await?);
    assert_eq!(work.receipt().backend_write_operations, created.backend_write_operations);
    assert_eq!(work.receipt().allocation_operations, created.allocation_operations);
    for changed in [
        ProposedCommit { payload: bytes::Bytes::from_static(b"different configuration bytes"), ..proposed.clone() },
        ProposedCommit { operation_id: OperationId::from_bytes([0x93; 16]), ..proposed.clone() },
        ProposedCommit { fingerprint: Digest::from_bytes([0x94; 32]), ..proposed },
    ] {
        assert!(!(async { let __authority_args = (id, changed, &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&native, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }).await?);
    }
    assert_eq!(work.receipt().backend_write_operations, 1);
    // Persistent table/history buffers retain their original root through state drop.
    let live = work.allocations(|allocations, _, _, _| {
        Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
    }).map(|(value, _allocation)| value)?;
    assert_eq!(live, state + retained);
    drop(native);
    assert_eq!(work.allocations(|allocations, _, _, _| {
        Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
    }).map(|(value, _allocation)| value)?, state);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum OriginalMutationOwnerFailure {
    Authority(AuthorityStoreError),
    Owner(u8),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<AuthorityStoreError> for OriginalMutationOwnerFailure {
    fn from(error: AuthorityStoreError) -> Self { Self::Authority(error) }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for OriginalMutationOwnerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authority(error) => std::fmt::Display::fmt(error, formatter),
            Self::Owner(code) => write!(formatter, "owner rejection {code}"),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::error::Error for OriginalMutationOwnerFailure {}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_owner_failure_wins_after_planning_outside_every_lock()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x96; 16]);
    let head = Head::genesis(Epoch::GENESIS);({ let __authority_args = (id, head.epoch, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
    AsyncAuthorityStore::create_authority(store.as_ref(), __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let callback_work = work.clone();
    let callback_store = store.clone();
    let callback_cancellation = cancellation.clone();
    let mut guard = move |original: &NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(original.shares_ledger(&callback_work));
        let store = callback_store.clone();
        let cancellation = callback_cancellation.clone();
        let original = original.clone();
        async move {
            let actual =({ let __authority_args = (id, &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_result }).expect("owner callback must hold no provider or root lock");
            assert_eq!(actual, head);
            original.charge(WorkCounters { items_examined: 5, ..WorkCounters::default() })
                .expect("owner work admitted");
            Err::<(), _>(OriginalMutationOwnerFailure::Owner(0xa7))
        }
    };
    assert!(matches!((async { let __authority_args = (native_mutation_request(id, head), &work, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await, Err(OriginalMutationOwnerFailure::Owner(0xa7))));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value, head);
    let prefix = work.receipt();
    assert!(prefix.bytes_hashed > 0);
    assert!(prefix.items_examined >= 5);
    assert_eq!(prefix.allocation_operations, 3);
    assert_eq!(prefix.backend_write_operations, 0);
    assert_eq!(prefix.authority_records_appended, 0);
    assert_eq!(work.allocations(|allocations, _, _, _| {
        Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
    }).map(|(value, _allocation)| value)?, NativeMaterializationWork::accounting_state_bytes());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_owner_revalidates_head_permit_and_cancellation_at_publication()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x97; 16]);
    let head = Head::genesis(Epoch::GENESIS);({ let __authority_args = (id, head.epoch, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
    AsyncAuthorityStore::create_authority(store.as_ref(), __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &root)
        .map_err(|(error, _)| error)?;
    let callback_store = store.clone();
    let callback_supplied = supplied.clone();
    let mut guard = move |original: &NativeMaterializationWork| {
        let store = callback_store.clone();
        let supplied = callback_supplied.clone();
        let original = original.clone();
        async move {({ let __authority_args = (id, head, &original, &supplied,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result =
            AsyncAuthorityStore::fence(store.as_ref(), __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result }).map(|_| ())
        }
    };
    let (outcome, allocation) = (async { let __authority_args = (native_mutation_request(id, head), &work, &supplied, &mut guard,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await?;
    assert!(matches!(outcome, AppendOutcome::Fenced { actual_epoch } if actual_epoch.get() == 2));
    assert_eq!(allocation.bytes(), 0);
    assert_eq!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value.sequence, Sequence::GENESIS);
    let actual =({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
    let mut request = native_mutation_request(id, actual);
    request.permit = crate::storage::PublicationPermit::Reservation {
        operation_id: request.commit.operation_id.into_bytes(), gate_tail: 7, expected: actual,
    };
    let prefix = work.receipt();
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    let mut forbidden = move |_: &NativeMaterializationWork| {
        callback_called.store(true, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!((async { let __authority_args = (request, &work, &supplied, &mut forbidden,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await, Err(AuthorityStoreError::RejectedStatic(_))));
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(work.receipt(), prefix);
    let callback_supplied = supplied.clone();
    let mut cancel = move |_: &NativeMaterializationWork| {
        callback_supplied.cancel();
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!((async { let __authority_args = (native_mutation_request(id, actual), &work, &supplied, &mut cancel,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await, Err(AuthorityStoreError::Cancelled)));
    assert_eq!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value, actual);
    assert_eq!(work.receipt().backend_write_operations, prefix.backend_write_operations);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_first_record_and_append_denial_never_create_a_partial_authority()
-> Result<(), Box<dyn std::error::Error>> {

    use crate::materializer::NativeMaterializationWork;
    let id = AuthorityId::from_bytes([0x98; 16]);
    let store = MemoryAuthorityStore::default();
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let active = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
        ..native_authority_budget()
    }, &root).map_err(|(error, _)| error)?;
    let request = native_mutation_request(id, Head::genesis(Epoch::GENESIS));
    assert!(matches!(({ let __authority_args = (id, request.commit.clone(), &work, &supplied,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }), Err(AuthorityStoreError::NativeOriginal(_))));
    assert!(matches!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }), Err(AuthorityFailure { error: AuthorityStoreError::Missing, .. })));
    assert_eq!(work.receipt().allocation_operations, 1);
    assert_eq!(work.receipt().backend_write_operations, 0);
    for cancel_root in [false, true] {
        if cancel_root { root.cancel(); } else { supplied.cancel(); }
        let cancellation = if cancel_root { &active } else { &supplied };
        let prefix = work.receipt();
        assert!(({ let __authority_args = (id, request.commit.clone(), &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }).is_err());
        assert!(({ let __authority_args = (request.clone(), &work, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }).is_err());
        assert_eq!(work.receipt(), prefix);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct OpaqueImmediateAuthority(MemoryAuthorityStore);

#[cfg(not(target_arch = "wasm32"))]
impl crate::AsyncAuthorityStore for OpaqueImmediateAuthority {
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
let value = crate::AsyncAuthorityStore::create_authority(&self.0, authority, epoch, work, None::<&mut ()>).await.map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::compare_and_append(&self.0, request, work, None::<&mut ()>).await.map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::head(&self.0, authority, work).await
}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::replay(&self.0, authority, after, limit, work).await
}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::fence(&self.0, authority, expected, work).await
}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
    > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::find_operation(&self.0, authority, operation, work).await
}
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn opaque_immediate_native_mutations_reject_before_io_and_preserve_ordinary_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(OpaqueImmediateAuthority::default());
    let id = AuthorityId::from_bytes([0x99; 16]);
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let prefix = work.receipt();
    let request = native_mutation_request(id, Head::genesis(Epoch::GENESIS));
    assert!(matches!((async { let __authority_args = (id, request.commit.clone(), &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!((async { let __authority_args = (request.clone(), &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    let mut guard = move |_: &NativeMaterializationWork| {
        callback_called.store(true, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!((async { let __authority_args = (request.clone(), &work, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(work.receipt(), prefix);
    assert!(matches!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure { error: AuthorityStoreError::Missing, .. })));
    let ordinary = (async { let __authority_args = (id, request.commit, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    assert!(ordinary.value);
    assert_eq!(ordinary.work.backend_write_operations, 2);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_guarded_first_record_seals_atomic_creation_and_successful_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x9a; 16]);
    let proposed = native_mutation_request(id, Head::genesis(Epoch::GENESIS)).commit;
    let budget = native_authority_budget();
    let cancellation = crate::CancellationToken::new();
    let mut failed = |remaining: WorkBudget| {
        assert!(remaining.bytes_hashed < budget.bytes_hashed);
        let store = store.clone();
        async move {
        assert!(matches!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }),
            Err(AuthorityFailure { error: AuthorityStoreError::Missing, .. })));
        Err::<crate::facade::FsReceipt<()>, _>(
            crate::performance::OperationFailure::new(
                crate::facade::FsError::Cancelled(crate::CancellationError),
                WorkCounters { items_examined: 3, ..WorkCounters::default() },
            ),
        )
        }
    };
    let failure = (async { let __authority_args = (id, proposed.clone(), budget, &cancellation, &mut failed,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).await.err().ok_or("ordinary guard rejection did not win")?;
    assert!(matches!(failure.error, crate::facade::FsError::Cancelled(_)));
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    assert_eq!(failure.work.items_examined, 3);
    assert!(matches!(({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }),
        Err(AuthorityFailure { error: AuthorityStoreError::Missing, .. })));
    let mut calls = 0;
    let mut successful = |_: WorkBudget| {
        calls += 1;
        let store = store.clone();
        async move {
        // Also exercises recovery's unlocked seal; no synchronous mutex can
        // remain held when the actual caller performs its own head proof.
        let _ =({ let __authority_args = (id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::head(store.as_ref(), __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) });
        Ok::<_, crate::performance::OperationFailure<crate::facade::FsError>>(
            crate::facade::FsReceipt { value: (), work: WorkCounters::default() },
        )
        }
    };
    let first = (async { let __authority_args = (id, proposed.clone(), budget, &cancellation, &mut successful,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).await?;
    assert!(first.value);
    assert_eq!(first.work.backend_write_operations, 1);
    let retry = (async { let __authority_args = (id, proposed, budget, &cancellation, &mut successful,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).await?;
    assert!(retry.value);
    assert_eq!(retry.work.backend_write_operations, 0);
    assert_eq!(calls, 2);
    Ok(())
}
