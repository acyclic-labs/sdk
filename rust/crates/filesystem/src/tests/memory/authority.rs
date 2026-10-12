use super::*;

#[tokio::test]
async fn authority_is_fenced_and_replay_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let authority_id = AuthorityId::from_bytes([1; 16]);
    let created = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
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
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority_id,
                        epoch: Epoch::GENESIS,
                        expected: head,
                        commit: commit,
                        permit: crate::PublicationPermit::Unrestricted,
                    },
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })?
        .value,
        AppendOutcome::Committed(_)
    ));
    let current = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store.head(authority_id, &mut __authority_work).await;
        __authority_work.receipt(__authority_result)
    })?
    .value;
    let fenced = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .fence(authority_id, current, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let page = ({
        let authority_input_2 = ReplayLimit {
            records: 1,
            payload_bytes: 7,
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .replay(
                authority_id,
                Sequence::GENESIS,
                authority_input_2,
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
    assert_eq!(page.value.len(), 1);
    Ok(())
}

#[tokio::test]
async fn authority_payload_and_replay_bounds_preserve_forward_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::new(2)?;
    let authority_id = AuthorityId::from_bytes([11; 16]);
    let head = match ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?
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
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority_id,
                        epoch: Epoch::GENESIS,
                        expected: head,
                        commit: commit,
                        permit: crate::PublicationPermit::Unrestricted,
                    },
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })?
        .value,
        AppendOutcome::Committed(_)
    ));
    assert!(matches!(
        ({
            let authority_input_2 = ReplayLimit {
                records: 1,
                payload_bytes: 1,
            };
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .replay(
                    authority_id,
                    Sequence::GENESIS,
                    authority_input_2,
                    &mut __authority_work,
                )
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayRecordTooLarge {
                observed: 2,
                maximum: 1,
            },
            ..
        })
    ));
    let oversized_cancellation = crate::CancellationToken::new();
    let mut oversized_work =
        crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &oversized_cancellation);
    let oversized_head = store.head(authority_id, &mut oversized_work).await?;
    let oversized = ProposedCommit {
        operation_id: OperationId::from_bytes([14; 16]),
        fingerprint: Digest::from_bytes([15; 32]),
        payload: bytes::Bytes::from_static(b"abc"),
    };
    assert!(matches!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority_id,
                        epoch: Epoch::GENESIS,
                        expected: oversized_head,
                        commit: oversized,
                        permit: crate::PublicationPermit::Unrestricted,
                    },
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        }),
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
    let created = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
    let CreateAuthorityOutcome::Created(mut head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    for (operation, fingerprint, payload) in [
        ([17; 16], [18; 32], bytes::Bytes::from_static(b"aa")),
        ([19; 16], [20; 32], bytes::Bytes::from_static(b"bb")),
    ] {
        let appended = ({
            let authority_input_3 = ProposedCommit {
                operation_id: OperationId::from_bytes(operation),
                fingerprint: Digest::from_bytes(fingerprint),
                payload,
            };
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority_id,
                        epoch: Epoch::GENESIS,
                        expected: head,
                        commit: authority_input_3,
                        permit: crate::PublicationPermit::Unrestricted,
                    },
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })?;
        let AppendOutcome::Committed(commit) = appended.value else {
            return Err("authority append did not commit".into());
        };
        head = Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        };
    }
    let replay = ({
        let authority_input_2 = ReplayLimit {
            records: 2,
            payload_bytes: 3,
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .replay(
                authority_id,
                Sequence::GENESIS,
                authority_input_2,
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
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
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store.head(authority_id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let created = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
    let CreateAuthorityOutcome::Created(genesis) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        ({ let authority_cancellation = &crate::CancellationToken::new();
         let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation); let __authority_result = store.create_authority(authority_id, Epoch::GENESIS, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?
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
    let committed = ({
        let authority_input_3 = proposed.clone();
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: genesis,
                    commit: authority_input_3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
    let AppendOutcome::Committed(durable) = committed.value else {
        return Err("first append was not committed".into());
    };
    assert!(matches!(
        ({ let authority_input_3 = proposed.clone();
        let authority_cancellation = &crate::CancellationToken::new();
         let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: authority_id, epoch: Epoch::GENESIS, expected: genesis, commit: authority_input_3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::AlreadyCommitted(actual) if actual == durable
    ));
    assert!(matches!(
        ({ let authority_input_3 = ProposedCommit {
                    fingerprint: Digest::from_bytes([24; 32]),
                    ..proposed
                };
        let authority_cancellation = &crate::CancellationToken::new();
         let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation); let __authority_result = store.compare_and_append(crate::GuardedAppend { authority_id: authority_id, epoch: Epoch::GENESIS, expected: genesis, commit: authority_input_3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
            .value,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } if committed_fingerprint == fingerprint
    ));
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .find_operation(authority_id, operation_id, &mut __authority_work)
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })?
        .value,
        Some(durable.clone())
    );
    assert!(
        ({
            let authority_input_1 = OperationId::from_bytes([25; 16]);
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .find_operation(authority_id, authority_input_1, &mut __authority_work)
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })?
        .value
        .is_none()
    );
    let current = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store.head(authority_id, &mut __authority_work).await;
        __authority_work.receipt(__authority_result)
    })?
    .value;
    let stale = ({
        let authority_input_3 = ProposedCommit {
            operation_id: OperationId::from_bytes([26; 16]),
            fingerprint: Digest::from_bytes([27; 32]),
            payload: bytes::Bytes::from_static(b"stale"),
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: genesis,
                    commit: authority_input_3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
    assert!(matches!(
        stale.value,
        AppendOutcome::Conflict { actual } if actual == current
    ));
    let fenced = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .fence(authority_id, current, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    let repeated_fence = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .fence(authority_id, current, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })?;
    assert_eq!(
        repeated_fence.value,
        FenceOutcome::Conflict {
            actual: fenced_head
        }
    );
    let wrong_epoch = ({
        let authority_input_3 = ProposedCommit {
            operation_id: OperationId::from_bytes([28; 16]),
            fingerprint: Digest::from_bytes([29; 32]),
            payload: bytes::Bytes::from_static(b"fenced"),
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: fenced_head,
                    commit: authority_input_3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
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
            ({
                let authority_cancellation = &crate::CancellationToken::new();

                let mut __authority_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
                let __authority_result = store
                    .replay(
                        authority_id,
                        Sequence::GENESIS,
                        invalid,
                        &mut __authority_work,
                    )
                    .await
                    .map(|(value, _allocation)| value);
                __authority_work.receipt(__authority_result)
            }),
            Err(AuthorityFailure {
                error: AuthorityStoreError::InvalidReplayLimit,
                ..
            })
        ));
    }
    assert!(matches!(
        ({
            let authority_input_1 = Sequence::new(2);
            let authority_input_2 = ReplayLimit {
                records: 1,
                payload_bytes: 16,
            };
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store
                .replay(
                    authority_id,
                    authority_input_1,
                    authority_input_2,
                    &mut __authority_work,
                )
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayCursorAhead { .. },
            ..
        })
    ));
    Ok(())
}
