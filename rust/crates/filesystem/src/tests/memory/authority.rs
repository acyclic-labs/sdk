use super::*;

#[tokio::test]
async fn authority_is_fenced_and_replay_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([1; 16]);
    let created = provider!(
        store,
        create_authority(authority_id, Epoch::GENESIS),
        WorkBudget::UNBOUNDED
    )?;
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    let commit = ProposedCommit {
        publication: None,
        operation_id: OperationId::from_bytes([2; 16]),
        fingerprint: Digest::from_bytes([3; 32]),
        payload: bytes::Bytes::from_static(b"payload"),
    };
    assert!(matches!(
        provider!(
            store,
            compare_and_append(authority_id, Epoch::GENESIS, head, commit),
            WorkBudget::UNBOUNDED
        )?
        .value,
        AppendOutcome::Committed(_)
    ));
    let current = provider!(store, head(authority_id), WorkBudget::UNBOUNDED)?.value;
    let fenced = provider!(store, fence(authority_id, current), WorkBudget::UNBOUNDED)?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let page = provider!(
        store,
        replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: 7,
            }
        ),
        WorkBudget::UNBOUNDED
    )?;
    assert_eq!(page.value.len(), 1);
    Ok(())
}

#[tokio::test]
async fn authority_payload_and_replay_bounds_preserve_forward_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::new(2)?;
    let authority_id = AuthorityId::from_bytes([11; 16]);
    let head = match provider!(
        store,
        create_authority(authority_id, Epoch::GENESIS),
        WorkBudget::UNBOUNDED
    )?
    .value
    {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    let commit = ProposedCommit {
        publication: None,
        operation_id: OperationId::from_bytes([12; 16]),
        fingerprint: Digest::from_bytes([13; 32]),
        payload: bytes::Bytes::from_static(b"ab"),
    };
    assert!(matches!(
        provider!(
            store,
            compare_and_append(authority_id, Epoch::GENESIS, head, commit),
            WorkBudget::UNBOUNDED
        )?
        .value,
        AppendOutcome::Committed(_)
    ));
    assert!(matches!(
        provider!(
            store,
            replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 1,
                }
            ),
            WorkBudget::UNBOUNDED
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
        publication: None,
        operation_id: OperationId::from_bytes([14; 16]),
        fingerprint: Digest::from_bytes([15; 32]),
        payload: bytes::Bytes::from_static(b"abc"),
    };
    assert!(matches!(
        provider!(
            store,
            compare_and_append(
                authority_id,
                Epoch::GENESIS,
                provider!(store, head(authority_id), WorkBudget::UNBOUNDED)?.value,
                oversized
            ),
            WorkBudget::UNBOUNDED
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

#[tokio::test]
async fn replay_stops_at_the_payload_bound_after_contiguous_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([16; 16]);
    let created = provider!(
        store,
        create_authority(authority_id, Epoch::GENESIS),
        WorkBudget::UNBOUNDED
    )?;
    let CreateAuthorityOutcome::Created(mut head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    for (operation, fingerprint, payload) in [
        ([17; 16], [18; 32], bytes::Bytes::from_static(b"aa")),
        ([19; 16], [20; 32], bytes::Bytes::from_static(b"bb")),
    ] {
        let appended = provider!(
            store,
            compare_and_append(
                authority_id,
                Epoch::GENESIS,
                head,
                ProposedCommit {
                    publication: None,
                    operation_id: OperationId::from_bytes(operation),
                    fingerprint: Digest::from_bytes(fingerprint),
                    payload,
                }
            ),
            WorkBudget::UNBOUNDED
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
    let replay = provider!(
        store,
        replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 2,
                payload_bytes: 3,
            }
        ),
        WorkBudget::UNBOUNDED
    )?;
    assert_eq!(replay.value.len(), 1);
    assert_eq!(replay.value[0].payload.as_ref(), b"aa");

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
        provider!(store, head(authority_id), WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let created = provider!(
        store,
        create_authority(authority_id, Epoch::GENESIS),
        WorkBudget::UNBOUNDED
    )?;
    let CreateAuthorityOutcome::Created(genesis) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        provider!(store, create_authority(authority_id, Epoch::GENESIS), WorkBudget::UNBOUNDED)?
            .value,
        CreateAuthorityOutcome::Existing(actual) if actual == genesis
    ));
    let operation_id = OperationId::from_bytes([22; 16]);
    let fingerprint = Digest::from_bytes([23; 32]);
    let proposed = ProposedCommit {
        publication: None,
        operation_id,
        fingerprint,
        payload: bytes::Bytes::from_static(b"durable"),
    };
    let committed = provider!(
        store,
        compare_and_append(authority_id, Epoch::GENESIS, genesis, proposed.clone()),
        WorkBudget::UNBOUNDED
    )?;
    let AppendOutcome::Committed(durable) = committed.value else {
        return Err("first append was not committed".into());
    };
    assert!(matches!(
        provider!(store, compare_and_append(authority_id, Epoch::GENESIS, genesis, proposed.clone()), WorkBudget::UNBOUNDED)?
            .value,
        AppendOutcome::AlreadyCommitted(actual) if actual == durable
    ));
    assert!(matches!(
        provider!(store, compare_and_append(authority_id, Epoch::GENESIS, genesis, ProposedCommit {
                    publication: None,
                    fingerprint: Digest::from_bytes([24; 32]),
                    ..proposed
                }), WorkBudget::UNBOUNDED)?
            .value,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } if committed_fingerprint == fingerprint
    ));
    assert_eq!(
        provider!(
            store,
            find_operation(authority_id, operation_id),
            WorkBudget::UNBOUNDED
        )?
        .value,
        Some(durable.clone())
    );
    assert!(
        provider!(
            store,
            find_operation(authority_id, OperationId::from_bytes([25; 16])),
            WorkBudget::UNBOUNDED
        )?
        .value
        .is_none()
    );
    let current = provider!(store, head(authority_id), WorkBudget::UNBOUNDED)?.value;
    let stale = provider!(
        store,
        compare_and_append(
            authority_id,
            Epoch::GENESIS,
            genesis,
            ProposedCommit {
                publication: None,
                operation_id: OperationId::from_bytes([26; 16]),
                fingerprint: Digest::from_bytes([27; 32]),
                payload: bytes::Bytes::from_static(b"stale"),
            }
        ),
        WorkBudget::UNBOUNDED
    )?;
    assert!(matches!(
        stale.value,
        AppendOutcome::Conflict { actual } if actual == current
    ));
    let fenced = provider!(store, fence(authority_id, current), WorkBudget::UNBOUNDED)?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let repeated_fence = provider!(store, fence(authority_id, current), WorkBudget::UNBOUNDED)?;
    assert_eq!(
        repeated_fence.value,
        FenceOutcome::Conflict {
            actual: fenced_head
        }
    );
    let wrong_epoch = provider!(
        store,
        compare_and_append(
            authority_id,
            Epoch::GENESIS,
            fenced_head,
            ProposedCommit {
                publication: None,
                operation_id: OperationId::from_bytes([28; 16]),
                fingerprint: Digest::from_bytes([29; 32]),
                payload: bytes::Bytes::from_static(b"fenced"),
            }
        ),
        WorkBudget::UNBOUNDED
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
            provider!(
                store,
                replay(authority_id, Sequence::GENESIS, invalid),
                WorkBudget::UNBOUNDED
            ),
            Err(AuthorityFailure {
                error: AuthorityStoreError::InvalidReplayLimit,
                ..
            })
        ));
    }
    assert!(matches!(
        provider!(
            store,
            replay(
                authority_id,
                Sequence::new(2),
                ReplayLimit {
                    records: 1,
                    payload_bytes: 16,
                }
            ),
            WorkBudget::UNBOUNDED
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayCursorAhead { after, head },
            ..
        }) if after == Sequence::new(2) && head == current.sequence
    ));
    Ok(())
}
