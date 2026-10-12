use super::*;
use bytes::Bytes;

#[tokio::test]
async fn authority_is_fenced_and_replay_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();

    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([1; 16]);
    let created = store
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let head = match created {
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
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: commit,
                    permit: crate::PublicationPermit::Unrestricted
                },
                &mut crate::AuthorityWork::new(B, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?,
        AppendOutcome::Committed(_)
    ));
    let current = store
        .head(authority_id, &mut crate::AuthorityWork::new(B, &cancel))
        .await?;
    let fenced = store
        .fence(
            authority_id,
            current,
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    let FenceOutcome::Advanced(fenced_head) = fenced else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let page = store
        .replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: 7,
            },
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await
        .map(|(value, _allocation)| value)?;
    assert_eq!(page.len(), 1);
    Ok(())
}

#[tokio::test]
async fn authority_payload_and_replay_bounds_preserve_forward_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();

    let store = MemoryAuthorityStore::new(2)?;
    let authority_id = AuthorityId::from_bytes([11; 16]);
    let head = match store
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?
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
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: commit,
                    permit: crate::PublicationPermit::Unrestricted
                },
                &mut crate::AuthorityWork::new(B, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?,
        AppendOutcome::Committed(_)
    ));
    assert!(matches!(
        store
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 1,
                },
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await
            .map(|(value, _allocation)| value),
        Err(AuthorityStoreError::ReplayRecordTooLarge {
            observed: 2,
            maximum: 1,
        })
    ));
    let oversized = ProposedCommit {
        operation_id: OperationId::from_bytes([14; 16]),
        fingerprint: Digest::from_bytes([15; 32]),
        payload: Bytes::from_static(b"abc"),
    };
    assert!(matches!(
        store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: store
                        .head(authority_id, &mut crate::AuthorityWork::new(B, &cancel))
                        .await?,
                    commit: oversized,
                    permit: crate::PublicationPermit::Unrestricted
                },
                &mut crate::AuthorityWork::new(B, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::PayloadTooLarge {
            observed: 3,
            maximum: 2,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn replay_stops_at_the_payload_bound_after_contiguous_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();
    let mut replay_work = crate::AuthorityWork::new(B, &cancel);

    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([16; 16]);
    let created = store
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let CreateAuthorityOutcome::Created(mut head) = created else {
        return Err("authority unexpectedly existed".into());
    };
    for (operation, fingerprint, payload) in [
        ([17; 16], [18; 32], Bytes::from_static(b"aa")),
        ([19; 16], [20; 32], Bytes::from_static(b"bb")),
    ] {
        let appended = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: ProposedCommit {
                        operation_id: OperationId::from_bytes(operation),
                        fingerprint: Digest::from_bytes(fingerprint),
                        payload,
                    },
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut crate::AuthorityWork::new(B, &cancel),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?;
        let AppendOutcome::Committed(commit) = appended else {
            return Err("authority append did not commit".into());
        };
        head = Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        };
    }
    let replay = store
        .replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 2,
                payload_bytes: 3,
            },
            &mut replay_work,
        )
        .await
        .map(|(value, _allocation)| value)?;
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].payload.as_ref(), b"aa");
    assert_eq!(replay_work.spent().authority_records_read, 1);
    assert_eq!(replay_work.spent().authority_bytes_read, 2);
    Ok(())
}

#[tokio::test]
async fn authority_outcomes_are_exactly_idempotent_conflicted_and_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();

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
        store
            .head(authority_id, &mut crate::AuthorityWork::new(B, &cancel))
            .await,
        Err(AuthorityStoreError::Missing)
    ));
    let created = store
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let CreateAuthorityOutcome::Created(genesis) = created else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        store
            .create_authority(authority_id, Epoch::GENESIS, &mut crate::AuthorityWork::new(B, &cancel), None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity))?
            ,
        CreateAuthorityOutcome::Existing(actual) if actual == genesis
    ));
    let operation_id = OperationId::from_bytes([22; 16]);
    let fingerprint = Digest::from_bytes([23; 32]);
    let proposed = ProposedCommit {
        operation_id,
        fingerprint,
        payload: Bytes::from_static(b"durable"),
    };
    let committed = store
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: proposed.clone(),
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let AppendOutcome::Committed(durable) = committed else {
        return Err("first append was not committed".into());
    };
    assert!(matches!(
        store
            .compare_and_append(crate::GuardedAppend { authority_id: authority_id, epoch: Epoch::GENESIS, expected: genesis, commit: proposed.clone(), permit: crate::PublicationPermit::Unrestricted }, &mut crate::AuthorityWork::new(B, &cancel), None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity))?
            ,
        AppendOutcome::AlreadyCommitted(actual) if actual == durable
    ));
    assert!(matches!(
        store
            .compare_and_append(crate::GuardedAppend { authority_id: authority_id, epoch: Epoch::GENESIS, expected: genesis, commit: ProposedCommit {
                    fingerprint: Digest::from_bytes([24; 32]),
                    ..proposed
                }, permit: crate::PublicationPermit::Unrestricted }, &mut crate::AuthorityWork::new(B, &cancel), None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity))?
            ,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } if committed_fingerprint == fingerprint
    ));
    assert_eq!(
        store
            .find_operation(
                authority_id,
                operation_id,
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await?,
        Some(durable.clone())
    );
    assert!(
        store
            .find_operation(
                authority_id,
                OperationId::from_bytes([25; 16]),
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await?
            .is_none()
    );
    let current = store
        .head(authority_id, &mut crate::AuthorityWork::new(B, &cancel))
        .await?;
    let stale = store
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([26; 16]),
                    fingerprint: Digest::from_bytes([27; 32]),
                    payload: Bytes::from_static(b"stale"),
                },
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(
        stale,
        AppendOutcome::Conflict { actual } if actual == current
    ));
    let fenced = store
        .fence(
            authority_id,
            current,
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    let FenceOutcome::Advanced(fenced_head) = fenced else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let repeated_fence = store
        .fence(
            authority_id,
            current,
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    assert_eq!(
        repeated_fence,
        FenceOutcome::Conflict {
            actual: fenced_head
        }
    );
    let wrong_epoch = store
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: fenced_head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([28; 16]),
                    fingerprint: Digest::from_bytes([29; 32]),
                    payload: Bytes::from_static(b"fenced"),
                },
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut crate::AuthorityWork::new(B, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(
        wrong_epoch,
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
            store
                .replay(
                    authority_id,
                    Sequence::GENESIS,
                    invalid,
                    &mut crate::AuthorityWork::new(B, &cancel)
                )
                .await
                .map(|(value, _allocation)| value),
            Err(AuthorityStoreError::InvalidReplayLimit)
        ));
    }
    assert!(matches!(
        store
            .replay(
                authority_id,
                Sequence::new(2),
                ReplayLimit {
                    records: 1,
                    payload_bytes: 16,
                },
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await
            .map(|(value, _allocation)| value),
        Err(AuthorityStoreError::ReplayCursorAhead { .. })
    ));
    Ok(())
}
