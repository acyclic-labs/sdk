use super::*;
use crate::foundation::Digest;
use crate::storage::FenceOutcome;

#[test]
fn authority_is_fenced_and_replay_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([1; 16]);
    let created = store.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    let commit = ProposedCommit {
        operation_id: OperationId::from_bytes([2; 16]),
        fingerprint: Digest::from_bytes([3; 32]),
        payload: Bytes::from_static(b"payload"),
    };
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                head,
                commit,
                WorkBudget::UNBOUNDED,
            )?
            .value,
        AppendOutcome::Committed(_)
    ));
    let current = store.head(authority_id, WorkBudget::UNBOUNDED)?.value;
    let fenced = store.fence(authority_id, current, WorkBudget::UNBOUNDED)?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let page = store.replay(
        authority_id,
        Sequence::GENESIS,
        ReplayLimit {
            records: 1,
            payload_bytes: 7,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(page.value.len(), 1);
    Ok(())
}

#[test]
fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"abcdef"),
    };
    store.put(
        object_id,
        Bytes::from_static(b"abcdef"),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        store.read(object_id, 6, WorkBudget::UNBOUNDED)?.value,
        ObjectRead {
            bytes: Bytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        store.read(object_id, 5, WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    Ok(())
}

#[test]
fn object_batch_is_ordered_deduplicated_by_storage_and_one_backend_read()
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
    store.put(first, Bytes::from_static(b"first"), WorkBudget::UNBOUNDED)?;
    store.put(second, Bytes::from_static(b"second"), WorkBudget::UNBOUNDED)?;
    let receipt = store.read_many(
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
        ],
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.clone())
            .collect::<Vec<_>>(),
        vec![
            Bytes::from_static(b"second"),
            Bytes::from_static(b"first"),
            Bytes::from_static(b"second"),
        ]
    );
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.object_probes, 3);
    assert_eq!(receipt.work.items_returned, 3);
    assert!(matches!(
        store.read_many(&[], WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Rejected(_),
            ..
        })
    ));
    Ok(())
}

#[test]
fn object_store_reauthenticates_shared_bytes_after_memory_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"authentic"),
    };
    store.put(
        object_id,
        Bytes::from_static(b"authentic"),
        WorkBudget::UNBOUNDED,
    )?;
    store
        .objects
        .write()
        .map_err(|_| "memory object lock poisoned")?
        .insert(object_id, Bytes::from_static(b"corrupted"));
    assert!(matches!(
        store.read(object_id, 64, WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        store.contains(object_id, WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = store
        .read_many(
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 64,
            }],
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(batch_failure.work.object_probes, 1);
    assert_eq!(batch_failure.work.backend_read_operations, 1);
    assert_eq!(batch_failure.work.object_bytes_read, 9);
    Ok(())
}

#[test]
fn authority_payload_and_replay_bounds_preserve_forward_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::new(2)?;
    let authority_id = AuthorityId::from_bytes([11; 16]);
    let head = match store
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
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
        payload: Bytes::from_static(b"ab"),
    };
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                head,
                commit,
                WorkBudget::UNBOUNDED,
            )?
            .value,
        AppendOutcome::Committed(_)
    ));
    assert!(matches!(
        store.replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: 1,
            },
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayRecordTooLarge {
                observed: 2,
                maximum: 1,
            },
            ..
        })
    ));
    let oversized = ProposedCommit {
        operation_id: OperationId::from_bytes([14; 16]),
        fingerprint: Digest::from_bytes([15; 32]),
        payload: Bytes::from_static(b"abc"),
    };
    assert!(matches!(
        store.compare_and_append(
            authority_id,
            Epoch::GENESIS,
            store.head(authority_id, WorkBudget::UNBOUNDED)?.value,
            oversized,
            WorkBudget::UNBOUNDED,
        ),
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

#[test]
fn replay_stops_at_the_payload_bound_after_contiguous_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([16; 16]);
    let created = store.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(mut head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    for (operation, fingerprint, payload) in [
        ([17; 16], [18; 32], Bytes::from_static(b"aa")),
        ([19; 16], [20; 32], Bytes::from_static(b"bb")),
    ] {
        let appended = store.compare_and_append(
            authority_id,
            Epoch::GENESIS,
            head,
            ProposedCommit {
                operation_id: OperationId::from_bytes(operation),
                fingerprint: Digest::from_bytes(fingerprint),
                payload,
            },
            WorkBudget::UNBOUNDED,
        )?;
        let AppendOutcome::Committed(commit) = appended.value else {
            return Err("authority append did not commit".into());
        };
        head = Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        };
    }
    let replay = store.replay(
        authority_id,
        Sequence::GENESIS,
        ReplayLimit {
            records: 2,
            payload_bytes: 3,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(replay.value.len(), 1);
    assert_eq!(replay.value[0].payload.as_ref(), b"aa");
    assert_eq!(replay.work.authority_records_read, 1);
    assert_eq!(replay.work.authority_bytes_read, 2);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn authority_outcomes_are_exactly_idempotent_conflicted_and_fenced()
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
        store.head(authority_id, WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let created = store.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(genesis) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        store
            .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
            .value,
        CreateAuthorityOutcome::Existing(actual) if actual == genesis
    ));
    let operation_id = OperationId::from_bytes([22; 16]);
    let fingerprint = Digest::from_bytes([23; 32]);
    let proposed = ProposedCommit {
        operation_id,
        fingerprint,
        payload: Bytes::from_static(b"durable"),
    };
    let committed = store.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        genesis,
        proposed.clone(),
        WorkBudget::UNBOUNDED,
    )?;
    let AppendOutcome::Committed(durable) = committed.value else {
        return Err("first append was not committed".into());
    };
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                genesis,
                proposed.clone(),
                WorkBudget::UNBOUNDED,
            )?
            .value,
        AppendOutcome::AlreadyCommitted(actual) if actual == durable
    ));
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                genesis,
                ProposedCommit {
                    fingerprint: Digest::from_bytes([24; 32]),
                    ..proposed
                },
                WorkBudget::UNBOUNDED,
            )?
            .value,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } if committed_fingerprint == fingerprint
    ));
    assert_eq!(
        store
            .find_operation(authority_id, operation_id, WorkBudget::UNBOUNDED)?
            .value,
        Some(durable.clone())
    );
    assert!(
        store
            .find_operation(
                authority_id,
                OperationId::from_bytes([25; 16]),
                WorkBudget::UNBOUNDED,
            )?
            .value
            .is_none()
    );
    let current = store.head(authority_id, WorkBudget::UNBOUNDED)?.value;
    let stale = store.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        genesis,
        ProposedCommit {
            operation_id: OperationId::from_bytes([26; 16]),
            fingerprint: Digest::from_bytes([27; 32]),
            payload: Bytes::from_static(b"stale"),
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        stale.value,
        AppendOutcome::Conflict { actual } if actual == current
    ));
    let fenced = store.fence(authority_id, current, WorkBudget::UNBOUNDED)?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let repeated_fence = store.fence(authority_id, current, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        repeated_fence.value,
        FenceOutcome::Conflict {
            actual: fenced_head
        }
    );
    let wrong_epoch = store.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        fenced_head,
        ProposedCommit {
            operation_id: OperationId::from_bytes([28; 16]),
            fingerprint: Digest::from_bytes([29; 32]),
            payload: Bytes::from_static(b"fenced"),
        },
        WorkBudget::UNBOUNDED,
    )?;
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
            store.replay(
                authority_id,
                Sequence::GENESIS,
                invalid,
                WorkBudget::UNBOUNDED,
            ),
            Err(AuthorityFailure {
                error: AuthorityStoreError::InvalidReplayLimit,
                ..
            })
        ));
    }
    assert!(matches!(
        store.replay(
            authority_id,
            Sequence::new(2),
            ReplayLimit {
                records: 1,
                payload_bytes: 16,
            },
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Rejected(_),
            ..
        })
    ));
    Ok(())
}

#[test]
fn object_admission_rejects_size_identity_absence_and_batch_bounds()
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
        store.put(
            object_id,
            Bytes::from_static(b"oversized"),
            WorkBudget::UNBOUNDED,
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        store.put(
            object_id,
            Bytes::from_static(b"nope"),
            WorkBudget::UNBOUNDED,
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(!store.contains(object_id, WorkBudget::UNBOUNDED)?.value);
    assert!(matches!(
        store.read(object_id, 4, WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    store.put(
        object_id,
        Bytes::from_static(b"data"),
        WorkBudget::UNBOUNDED,
    )?;
    store.put(
        object_id,
        Bytes::from_static(b"data"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        store.read_many(
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 3,
            }],
            WorkBudget::UNBOUNDED,
        ),
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
        store.read_many(
            &[crate::storage::ObjectReadRequest {
                object_id: missing,
                maximum_bytes: 4,
            }],
            WorkBudget::UNBOUNDED,
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        store.contains(object_id, denied),
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn defensive_memory_state_failures_are_typed_and_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let authority_id = AuthorityId::from_bytes([40; 16]);
    let operation_id = OperationId::from_bytes([41; 16]);
    let proposed = ProposedCommit {
        operation_id,
        fingerprint: Digest::from_bytes([42; 32]),
        payload: Bytes::from_static(b"payload"),
    };

    let corrupt_index = MemoryAuthorityStore::default();
    corrupt_index
        .authorities
        .lock()
        .map_err(|_| "authority lock poisoned")?
        .insert(
            authority_id,
            AuthorityState {
                epoch: Some(Epoch::GENESIS),
                commits: Vec::new(),
                operations: HashMap::from([(operation_id, 0)]),
            },
        );
    assert!(matches!(
        corrupt_index.compare_and_append(
            authority_id,
            Epoch::GENESIS,
            Head::genesis(Epoch::GENESIS),
            proposed.clone(),
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Corrupt(_),
            ..
        })
    ));

    let missing_epoch = MemoryAuthorityStore::default();
    missing_epoch
        .authorities
        .lock()
        .map_err(|_| "authority lock poisoned")?
        .insert(authority_id, AuthorityState::default());
    assert!(matches!(
        missing_epoch.head(authority_id, WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    assert!(matches!(
        missing_epoch.fence(
            authority_id,
            Head::genesis(Epoch::GENESIS),
            WorkBudget::UNBOUNDED
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));

    let exhausted_epoch = MemoryAuthorityStore::default();
    exhausted_epoch
        .authorities
        .lock()
        .map_err(|_| "authority lock poisoned")?
        .insert(
            authority_id,
            AuthorityState {
                epoch: Some(Epoch::new(u64::MAX)?),
                ..AuthorityState::default()
            },
        );
    assert!(matches!(
        exhausted_epoch.fence(
            authority_id,
            Head::genesis(Epoch::new(u64::MAX)?),
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::EpochExhausted,
            ..
        })
    ));

    let exhausted_sequence = MemoryAuthorityStore::default();
    let prior = DurableCommit {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(u64::MAX),
        operation_id: OperationId::from_bytes([43; 16]),
        fingerprint: Digest::from_bytes([44; 32]),
        previous_digest: Digest::ZERO,
        digest: Digest::from_bytes([45; 32]),
        payload: Bytes::from_static(b"prior"),
    };
    exhausted_sequence
        .authorities
        .lock()
        .map_err(|_| "authority lock poisoned")?
        .insert(
            authority_id,
            AuthorityState {
                epoch: Some(Epoch::GENESIS),
                commits: vec![prior.clone()],
                operations: HashMap::new(),
            },
        );
    let exhausted_head = Head {
        epoch: Epoch::GENESIS,
        sequence: prior.sequence,
        digest: prior.digest,
    };
    assert!(matches!(
        exhausted_sequence.compare_and_append(
            authority_id,
            Epoch::GENESIS,
            exhausted_head,
            proposed,
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Rejected(_),
            ..
        })
    ));

    let objects = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"authentic"),
    };
    objects
        .objects
        .write()
        .map_err(|_| "object lock poisoned")?
        .insert(object_id, Bytes::from_static(b"corrupt"));
    let failure = objects
        .put(
            object_id,
            Bytes::from_static(b"authentic"),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("conflicting existing object unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(failure.work.object_bytes_written, 9);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn poisoned_memory_locks_reject_every_surface_with_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let authority_id = AuthorityId::from_bytes([46; 16]);
    let operation_id = OperationId::from_bytes([47; 16]);
    let authority = MemoryAuthorityStore::default();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = authority
            .authorities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison authority lock"));
    }));
    assert!(poisoned.is_err());
    let proposed = ProposedCommit {
        operation_id,
        fingerprint: Digest::from_bytes([48; 32]),
        payload: Bytes::from_static(b"payload"),
    };
    let authority_failures = [
        authority
            .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("poisoned authority create succeeded")?,
        authority
            .head(authority_id, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("poisoned authority head succeeded")?,
        authority
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposed,
                WorkBudget::UNBOUNDED,
            )
            .err()
            .ok_or("poisoned authority append succeeded")?,
        authority
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 1,
                },
                WorkBudget::UNBOUNDED,
            )
            .err()
            .ok_or("poisoned authority replay succeeded")?,
        authority
            .fence(
                authority_id,
                Head::genesis(Epoch::GENESIS),
                WorkBudget::UNBOUNDED,
            )
            .err()
            .ok_or("poisoned authority fence succeeded")?,
        authority
            .find_operation(authority_id, operation_id, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("poisoned authority operation lookup succeeded")?,
    ];
    for (failure, expected_backend_operations) in
        authority_failures.into_iter().zip([0, 1, 1, 1, 1, 1])
    {
        assert!(matches!(failure.error, AuthorityStoreError::Rejected(_)));
        assert_eq!(
            failure.work.backend_read_operations + failure.work.backend_write_operations,
            expected_backend_operations
        );
    }

    let objects = MemoryObjectStore::default();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = objects
            .objects
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison object lock"));
    }));
    assert!(poisoned.is_err());
    let bytes = Bytes::from_static(b"object");
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, &bytes),
    };
    let put = objects
        .put(object_id, bytes, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("poisoned object put succeeded")?;
    assert!(matches!(put.error, ObjectStoreError::Corrupt));
    assert_eq!(put.work.backend_write_operations, 1);
    let read = objects
        .read(object_id, 6, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("poisoned object read succeeded")?;
    assert!(matches!(read.error, ObjectStoreError::Corrupt));
    assert_eq!(read.work.backend_read_operations, 1);
    let batch = objects
        .read_many(
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 6,
            }],
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("poisoned object batch read succeeded")?;
    assert!(matches!(batch.error, ObjectStoreError::Corrupt));
    assert_eq!(batch.work.backend_read_operations, 1);
    let contains = objects
        .contains(object_id, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("poisoned object contains succeeded")?;
    assert!(matches!(contains.error, ObjectStoreError::Corrupt));
    assert_eq!(contains.work.backend_read_operations, 1);
    Ok(())
}
