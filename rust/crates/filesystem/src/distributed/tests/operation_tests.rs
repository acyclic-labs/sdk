use super::*;

fn fork() -> WorkspaceForkCommit {
    WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([71; 16]),
        creation: proposed(72),
        retention: AuthorityId::from_bytes([73; 16]),
        retained: proposed(74),
    }
}

#[tokio::test]
async fn operation_locators_cover_first_records_and_atomic_fork_replay() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream);
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        let id = AuthorityId::from_bytes([70; 16]);
        assert!(
            store
                .create_first_record(id, proposed(70), &mut work, None::<&mut ()>)
                .await?
        );
        let located = store
            .located_operation(id, proposed(70).operation_id, &mut work)
            .await?
            .ok_or("first-record operation locator is missing")?;
        assert_eq!(located.0.sequence, Sequence::new(1));
        assert_eq!(located.0.fingerprint, proposed(70).fingerprint);
        assert!(matches!(
            store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: id,
                        epoch: Epoch::GENESIS,
                        expected: Head::genesis(Epoch::GENESIS),
                        commit: proposed(70),
                        permit: PublicationPermit::Unrestricted,
                    },
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            FsAppendOutcome::AlreadyCommitted(_)
        ));

        let fork = fork();
        for _ in 0..2 {
            assert_eq!(
                store
                    .fork_workspace(
                        fork.clone(),
                        PublicationPermit::Unrestricted,
                        &mut work,
                        None::<&mut ()>
                    )
                    .await?,
                WorkspaceForkOutcome::Committed
            );
        }
        let retained = store
            .located_operation(fork.retention, fork.retained.operation_id, &mut work)
            .await?
            .ok_or("retention operation locator is missing")?;
        let created = store
            .located_operation(fork.destination, fork.creation.operation_id, &mut work)
            .await?
            .ok_or("destination operation locator is missing")?;
        assert_eq!(
            retained.1, created.1,
            "both authorities must publish atomically"
        );
        assert_eq!(retained.0.sequence, Sequence::new(1));
        assert_eq!(created.0.sequence, Sequence::new(1));
        let mut changed = fork.clone();
        changed.creation.fingerprint = Digest::from_bytes([75; 32]);
        assert_eq!(
            store
                .fork_workspace(
                    changed,
                    PublicationPermit::Unrestricted,
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            WorkspaceForkOutcome::CreationRejected
        );
        let mut changed = fork;
        changed.retained.fingerprint = Digest::from_bytes([76; 32]);
        assert_eq!(
            store
                .fork_workspace(
                    changed,
                    PublicationPermit::Unrestricted,
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            WorkspaceForkOutcome::RetentionConflict
        );
        original.receipt().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_atomic_fork_never_publishes_its_other_authority_or_locator() -> TestResult {
    for native in [false, true] {
        for retention_conflict in [false, true] {
            let stream = Arc::new(MemoryStream::default());
            let store = StreamAuthorityStore::new(stream);
            let token = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(BUDGET, &token)
                .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(BUDGET, &token)
            };
            let fork = fork();
            let (occupied, untouched, operation) = if retention_conflict {
                (fork.retention, fork.destination, fork.creation.operation_id)
            } else {
                (fork.destination, fork.retention, fork.retained.operation_id)
            };
            assert!(
                store
                    .create_first_record(occupied, proposed(77), &mut work, None::<&mut ()>)
                    .await?
            );
            assert_eq!(
                store
                    .fork_workspace(
                        fork,
                        PublicationPermit::Unrestricted,
                        &mut work,
                        None::<&mut ()>
                    )
                    .await?,
                if retention_conflict {
                    WorkspaceForkOutcome::RetentionConflict
                } else {
                    WorkspaceForkOutcome::CreationRejected
                }
            );
            assert!(matches!(store.head(untouched, &mut work).await,
                Err(error) if authority_missing(&error)));
            let path = work.operation_path(untouched, operation)?;
            assert!(matches!(store.tail(path, &mut work).await,
                Err(error) if authority_missing(&error)));
            assert_eq!(
                store.head(occupied, &mut work).await?.sequence,
                Sequence::new(1)
            );
            original.receipt().verify(BUDGET)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_attempts_admit_only_one_semantic_operation() -> TestResult {
    for different_fingerprint in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(BUDGET, &token);
        let id = AuthorityId::from_bytes([78; 16]);
        store
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let request = crate::GuardedAppend {
            authority_id: id,
            epoch: Epoch::GENESIS,
            expected: store.head(id, &mut work).await?,
            commit: proposed(79),
            permit: PublicationPermit::Unrestricted,
        };
        let mut winner = request.clone();
        if different_fingerprint {
            winner.commit.fingerprint = Digest::from_bytes([80; 32]);
        }
        let mut spy = ReleaseSpy::new(stream);
        spy.before_commit = Some(winner);
        let racing = StreamAuthorityStore::new(Arc::new(spy));
        let result = racing
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await?;
        assert!(
            if different_fingerprint {
                matches!(result, FsAppendOutcome::IdempotencyConflict { .. })
            } else {
                matches!(
                    result,
                    FsAppendOutcome::Committed(_) | FsAppendOutcome::AlreadyCommitted(_)
                )
            },
            "concurrent semantic operation returned {result:?}"
        );
        assert_eq!(store.head(id, &mut work).await?.sequence, Sequence::new(1));
    }
    Ok(())
}

#[tokio::test]
async fn durable_operation_lookup_is_constant_work_for_long_histories() -> TestResult {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([81; 16]);
    assert!(
        store
            .create_first_record(id, proposed(1), &mut work, None::<&mut ()>)
            .await?
    );
    for operation in 2..65 {
        let expected = store.head(id, &mut work).await?;
        assert!(matches!(
            store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: id,
                        epoch: Epoch::GENESIS,
                        expected,
                        commit: proposed(operation),
                        permit: PublicationPermit::Unrestricted,
                    },
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            FsAppendOutcome::Committed(_)
        ));
    }
    // Check an append identity before creation's identity so the old stable-key
    // lookup fails on missing semantic replay before any accounting comparison.
    for operation in [32, 64, 1] {
        let mut lookup = AuthorityWork::new(BUDGET, &token);
        let found = store
            .find_operation(id, proposed(operation).operation_id, &mut lookup)
            .await?
            .ok_or("durable operation was not found")?;
        assert_eq!(found.operation_id, proposed(operation).operation_id);
        let (records, backend_reads) = if operation == 1 { (1, 4) } else { (2, 5) };
        assert_eq!(lookup.spent().authority_records_read, records);
        assert_eq!(lookup.spent().backend_read_operations, backend_reads);
    }
    Ok(())
}

fn lease(workspace: crate::WorkspaceId, id: u8) -> PublicationPermit {
    PublicationPermit::Lease {
        authority_id: volume_authority_id(workspace.volume_id()).into_bytes(),
        workspace_id: workspace.into_bytes(),
        lease_id: crate::OperationLeaseId::from_bytes([id; 16]).into_bytes(),
        expires_at_millis: u64::MAX,
    }
}

async fn open_lease(
    stream: &MemoryStream,
    permit: PublicationPermit,
    work: &mut AuthorityWork<'_>,
) -> Result<StreamPath, Box<dyn std::error::Error>> {
    let PublicationPermit::Lease {
        workspace_id,
        lease_id,
        expires_at_millis,
        ..
    } = permit
    else {
        return Err("expected lease permit".into());
    };
    let path = work.lease_path(
        crate::WorkspaceId::from_bytes(workspace_id),
        crate::OperationLeaseId::from_bytes(lease_id),
        expires_at_millis,
    )?;
    stream
        .append(AppendRequest {
            path: path.clone(),
            if_tail: Some(0),
            records: vec![acyclic_stream::OwnedBytes::from_static(b"open")],
            idempotency_key: Some(stream_key(b"open-lease", path.as_str().as_bytes())?),
        })
        .await?;
    Ok(path)
}

#[tokio::test]
async fn a_missing_lease_cannot_cache_a_rejected_append_attempt() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        let workspace = crate::WorkspaceId::from_bytes([82; 16]);
        let id = volume_authority_id(workspace.volume_id());
        store
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let request = crate::GuardedAppend {
            authority_id: id,
            epoch: Epoch::GENESIS,
            expected: store.head(id, &mut work).await?,
            commit: proposed(83),
            permit: lease(workspace, 84),
        };
        assert!(matches!(
            store
                .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::Fenced { .. }
        ));
        assert!(
            store
                .find_operation(id, request.commit.operation_id, &mut work)
                .await?
                .is_none()
        );
        open_lease(&stream, request.permit, &mut work).await?;
        assert!(matches!(
            store
                .compare_and_append(request, &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::Committed(_)
        ));
        original.receipt().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn a_closed_lease_attempt_does_not_poison_a_new_lease_for_the_same_operation() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let workspace = crate::WorkspaceId::from_bytes([85; 16]);
    let id = volume_authority_id(workspace.volume_id());
    store
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let first = lease(workspace, 86);
    let path = open_lease(&stream, first, &mut work).await?;
    let request = crate::GuardedAppend {
        authority_id: id,
        epoch: Epoch::GENESIS,
        expected: store.head(id, &mut work).await?,
        commit: proposed(87),
        permit: first,
    };
    let mut spy = ReleaseSpy::new(stream.clone());
    spy.before_lease_close = Some(path);
    let racing = StreamAuthorityStore::new(Arc::new(spy));
    assert!(matches!(
        racing
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::Fenced { .. }
    ));
    assert_eq!(store.head(id, &mut work).await?, request.expected);
    assert!(
        store
            .find_operation(id, request.commit.operation_id, &mut work)
            .await?
            .is_none()
    );
    let mut retry = request;
    retry.permit = lease(workspace, 88);
    open_lease(&stream, retry.permit, &mut work).await?;
    assert!(matches!(
        store
            .compare_and_append(retry, &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn fork_lease_rejection_and_closed_lease_replay_preserve_atomic_publication() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        let workspace = crate::WorkspaceId::from_bytes([89; 16]);
        let permit = lease(workspace, 90);
        let mut fork = fork();
        fork.lineage = Some(crate::GenerationForkSource {
            authority: volume_authority_id(workspace.volume_id()),
            generation: GenerationId::new(Digest::from_bytes([91; 32])),
            lineage: crate::GenerationFork::Independent,
        });
        assert_eq!(
            store
                .fork_workspace(fork.clone(), permit, &mut work, None::<&mut ()>)
                .await?,
            WorkspaceForkOutcome::CreationRejected
        );
        for authority in [fork.retention, fork.destination] {
            assert!(matches!(store.head(authority, &mut work).await,
                Err(error) if authority_missing(&error)));
        }
        let path = open_lease(&stream, permit, &mut work).await?;
        assert_eq!(
            store
                .fork_workspace(fork.clone(), permit, &mut work, None::<&mut ()>)
                .await?,
            WorkspaceForkOutcome::Committed
        );
        stream
            .append(AppendRequest {
                path: path.clone(),
                if_tail: Some(1),
                records: vec![acyclic_stream::OwnedBytes::from_static(b"closed")],
                idempotency_key: Some(stream_key(b"close-lease", path.as_str().as_bytes())?),
            })
            .await?;
        assert_eq!(
            store
                .fork_workspace(fork, permit, &mut work, None::<&mut ()>)
                .await?,
            WorkspaceForkOutcome::Committed
        );
        original.receipt().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn lost_fork_acknowledgement_recovers_both_original_locators() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let mut spy = ReleaseSpy::new(stream.clone());
    spy.lose_ack = true;
    let lossy = StreamAuthorityStore::new(Arc::new(spy));
    let fork = fork();
    assert!(
        lossy
            .fork_workspace(
                fork.clone(),
                PublicationPermit::Unrestricted,
                &mut work,
                None::<&mut ()>
            )
            .await
            .is_err()
    );
    let store = StreamAuthorityStore::new(stream);
    assert_eq!(
        store
            .fork_workspace(
                fork.clone(),
                PublicationPermit::Unrestricted,
                &mut work,
                None::<&mut ()>
            )
            .await?,
        WorkspaceForkOutcome::Committed
    );
    let retained = store
        .located_operation(fork.retention, fork.retained.operation_id, &mut work)
        .await?
        .ok_or("lost fork retention locator is missing")?;
    let created = store
        .located_operation(fork.destination, fork.creation.operation_id, &mut work)
        .await?
        .ok_or("lost fork destination locator is missing")?;
    assert_eq!(retained.1, created.1);
    Ok(())
}

#[tokio::test]
async fn separately_created_matching_records_are_not_an_atomic_fork() -> TestResult {
    for native in [false, true] {
        let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        let fork = fork();
        for (authority, commit) in [
            (fork.retention, fork.retained.clone()),
            (fork.destination, fork.creation.clone()),
        ] {
            assert!(
                store
                    .create_first_record(authority, commit, &mut work, None::<&mut ()>)
                    .await?
            );
        }
        let retained = store
            .located_operation(fork.retention, fork.retained.operation_id, &mut work)
            .await?
            .ok_or("separate retention record is missing")?;
        let created = store
            .located_operation(fork.destination, fork.creation.operation_id, &mut work)
            .await?
            .ok_or("separate creation record is missing")?;
        assert_ne!(retained.1, created.1);
        assert_eq!(
            store
                .fork_workspace(
                    fork,
                    PublicationPermit::Unrestricted,
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            WorkspaceForkOutcome::CreationRejected
        );
        original.receipt().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn lost_first_record_acknowledgement_recovers_its_atomic_locator() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let mut spy = ReleaseSpy::new(stream.clone());
    spy.lose_ack = true;
    let lossy = StreamAuthorityStore::new(Arc::new(spy));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([95; 16]);
    let commit = proposed(96);
    assert!(
        lossy
            .create_first_record(id, commit.clone(), &mut work, None::<&mut ()>)
            .await
            .is_err()
    );
    let store = StreamAuthorityStore::new(stream);
    assert!(
        store
            .create_first_record(id, commit.clone(), &mut work, None::<&mut ()>)
            .await?
    );
    let recovered = store
        .find_operation(id, commit.operation_id, &mut work)
        .await?
        .ok_or("lost first-record locator is missing")?;
    assert_eq!(recovered.sequence, Sequence::new(1));
    assert_eq!(recovered.fingerprint, commit.fingerprint);
    assert_eq!(store.head(id, &mut work).await?.sequence, Sequence::new(1));
    Ok(())
}

#[tokio::test]
async fn operation_locators_reject_corruption_and_separately_published_records() -> TestResult {
    for invalid in 0..7_u8 {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(BUDGET, &token);
        let id = AuthorityId::from_bytes([92; 16]);
        assert!(
            store
                .create_first_record(id, proposed(93), &mut work, None::<&mut ()>)
                .await?
        );
        let first = store
            .find_operation(id, proposed(93).operation_id, &mut work)
            .await?
            .ok_or("first operation is missing")?;
        let mut sequence = 1_u64;
        let mut digest = first.digest;
        if invalid == 6 {
            let mut record = crate::streams_record::BorrowedStreamsDurableRecord {
                epoch: Epoch::GENESIS,
                sequence: Sequence::new(2),
                operation_id: proposed(94).operation_id,
                fingerprint: proposed(94).fingerprint,
                previous_digest: first.digest,
                digest: Digest::ZERO,
                payload: b"publication",
            };
            record.digest = work.durable_digest(id, &record)?;
            let encoded = StreamsDurableRecord::encode_borrowed_with(
                &record,
                STREAM_RECORD_LIMIT,
                |domain, fields| work.stream_bytes(domain, fields),
            )
            .map_err(AuthorityStoreError::DurableCodec)??;
            stream
                .append(AppendRequest {
                    path: work.path(id, "/records", None)?,
                    if_tail: Some(2),
                    records: vec![encoded],
                    idempotency_key: Some(stream_key(b"separate-record", &[invalid])?),
                })
                .await?;
            sequence = 2;
            digest = record.digest;
        }
        if invalid == 1 {
            sequence = 0;
        }
        if invalid == 2 {
            sequence = 3;
        }
        if invalid == 3 {
            digest = Digest::ZERO;
        }
        let path = work.operation_path(id, proposed(94).operation_id)?;
        let value = if invalid == 0 {
            acyclic_stream::OwnedBytes::from_static(b"bad")
        } else {
            work.stream_bytes(
                OPERATION_LOCATOR_DOMAIN,
                &[&sequence.to_le_bytes(), digest.as_bytes()],
            )?
        };
        let records = if invalid == 5 {
            vec![value.clone(), value]
        } else {
            vec![value]
        };
        stream
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutations: vec![CommitMutation::Append { path, records }],
                idempotency_key: stream_key(b"forged-locator", &[invalid])?,
            })
            .await?;
        assert!(
            matches!(
                store
                    .find_operation(id, proposed(94).operation_id, &mut work)
                    .await,
                Err(AuthorityStoreError::Corrupt(_) | AuthorityStoreError::CorruptStatic(_))
            ),
            "corrupt locator case {invalid} was accepted"
        );
    }
    Ok(())
}

#[tokio::test]
async fn refused_or_cancelled_forks_publish_neither_records_nor_locators() -> TestResult {
    for native in [false, true] {
        for cancel in [false, true] {
            let stream = Arc::new(MemoryStream::default());
            let store = StreamAuthorityStore::new(stream);
            let token = CancellationToken::new();
            let mut budget = BUDGET;
            if !cancel {
                budget.authority_records_appended = 0;
            }
            let original = crate::NativeMaterializationWork::new(budget, &token)
                .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(budget, &token)
            };
            if cancel {
                token.cancel();
            }
            let fork = fork();
            assert!(
                store
                    .fork_workspace(
                        fork.clone(),
                        PublicationPermit::Unrestricted,
                        &mut work,
                        None::<&mut ()>
                    )
                    .await
                    .is_err()
            );
            let fresh = CancellationToken::new();
            let mut inspection = AuthorityWork::new(BUDGET, &fresh);
            for (authority, operation) in [
                (fork.retention, fork.retained.operation_id),
                (fork.destination, fork.creation.operation_id),
            ] {
                assert!(matches!(store.head(authority, &mut inspection).await,
                    Err(error) if authority_missing(&error)));
                let path = inspection.operation_path(authority, operation)?;
                assert!(matches!(store.tail(path, &mut inspection).await,
                    Err(error) if authority_missing(&error)));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn operation_index_shares_its_parent_across_authorities_at_capacity() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::new(acyclic_stream::MemoryLimits {
            // Two common authority prefixes, four paths per authority, and
            // one common operation-index prefix plus one locator per authority.
            paths: 2 + 8 * 4 + 1 + 8,
            ..acyclic_stream::MemoryLimits::default()
        }));
        let store = StreamAuthorityStore::new(stream);
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        for number in 0..8 {
            let id = AuthorityId::from_bytes([number; 16]);
            // Operation identities are scoped to their authority, including in
            // the shared index: all eight authorities deliberately reuse one.
            let commit = proposed(0);
            assert!(
                store
                    .create_first_record(id, commit.clone(), &mut work, None::<&mut ()>)
                    .await?
            );
            let located = store
                .located_operation(id, commit.operation_id, &mut work)
                .await?
                .ok_or("capacity-admitted authority lost its operation")?;
            assert_eq!(located.0.operation_id, commit.operation_id);
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_operation_recovery_needs_no_separate_locator_record() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream);
        let token = CancellationToken::new();
        let budget = WorkBudget {
            authority_records_appended: 5,
            ..BUDGET
        };
        let original =
            crate::NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(budget, &token)
        };
        let authority = AuthorityId::from_bytes([81; 16]);
        let first = proposed(82);
        assert!(
            store
                .create_first_record(authority, first.clone(), &mut work, None::<&mut ()>)
                .await?
        );
        let recovered = store
            .located_operation(authority, first.operation_id, &mut work)
            .await?
            .ok_or("initial operation was not recovered")?;
        assert_eq!(recovered.0.sequence, Sequence::new(1));
        assert_eq!(recovered.0.fingerprint, first.fingerprint);
        assert_eq!(recovered.0.payload, first.payload);
        assert!(matches!(
            store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority,
                        epoch: Epoch::GENESIS,
                        expected: Head::genesis(Epoch::GENESIS),
                        commit: first,
                        permit: PublicationPermit::Unrestricted,
                    },
                    &mut work,
                    None::<&mut ()>
                )
                .await?,
            FsAppendOutcome::AlreadyCommitted(_)
        ));
        assert!(
            store
                .located_operation(authority, proposed(83).operation_id, &mut work)
                .await?
                .is_none()
        );
    }
    Ok(())
}
