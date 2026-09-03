use super::*;
use crate::storage::FenceOutcome;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn proposed(byte: u8, payload: &'static [u8]) -> ProposedCommit {
    ProposedCommit {
        operation_id: OperationId::from_bytes([byte; 16]),
        fingerprint: Digest::from_bytes([byte; 32]),
        payload: Bytes::from_static(payload),
    }
}

fn assert_chain_boundaries(
    first: &DurableCommit,
    second: &DurableCommit,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(!replay_chain_is_discontinuous(
        second,
        Sequence::new(2),
        first.digest
    ));
    assert!(replay_chain_is_discontinuous(
        second,
        Sequence::new(1),
        first.digest
    ));
    assert!(replay_chain_is_discontinuous(
        second,
        Sequence::new(2),
        Digest::ZERO
    ));
    assert!(terminal_matches_head(
        second,
        Head {
            epoch: second.epoch,
            sequence: second.sequence,
            digest: second.digest,
        }
    ));
    for mismatched in [
        Head {
            epoch: second.epoch,
            sequence: first.sequence,
            digest: second.digest,
        },
        Head {
            epoch: second.epoch,
            sequence: second.sequence,
            digest: first.digest,
        },
    ] {
        assert!(!terminal_matches_head(second, mismatched));
    }
    let mut newer_epoch = second.clone();
    newer_epoch.epoch = Epoch::new(2)?;
    assert!(!terminal_matches_head(
        &newer_epoch,
        Head {
            epoch: Epoch::GENESIS,
            sequence: second.sequence,
            digest: second.digest,
        }
    ));
    Ok(())
}

fn assert_replay_boundaries(
    store: &LocalAuthorityStore,
    authority: AuthorityId,
    second: &DurableCommit,
) -> Result<(), Box<dyn std::error::Error>> {
    for limit in [
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
            store.replay(authority, Sequence::GENESIS, limit, WorkBudget::UNBOUNDED),
            Err(AuthorityFailure {
                error: AuthorityStoreError::InvalidReplayLimit,
                ..
            })
        ));
    }
    assert!(
        store
            .replay(
                authority,
                Sequence::new(2),
                ReplayLimit {
                    records: 1,
                    payload_bytes: 3,
                },
                WorkBudget::UNBOUNDED,
            )?
            .value
            .is_empty()
    );
    assert!(matches!(
        store.replay(
            authority,
            Sequence::new(3),
            ReplayLimit {
                records: 1,
                payload_bytes: 3
            },
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Rejected(_),
            ..
        })
    ));
    assert_eq!(
        store
            .replay(
                authority,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 3
                },
                WorkBudget::UNBOUNDED,
            )?
            .value
            .len(),
        1
    );
    let tail = store.replay(
        authority,
        Sequence::new(1),
        ReplayLimit {
            records: 1,
            payload_bytes: 3,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(tail.value, vec![second.clone()]);
    assert_eq!(tail.work.backend_read_operations, 3);
    assert_eq!(tail.work.authority_records_read, 2);
    Ok(())
}

#[test]
fn commit_read_accounting_and_head_order_are_exact() {
    assert_eq!(DEFAULT_MAX_PAYLOAD_BYTES, 16_777_216);
    assert_eq!(LocalAuthorityConfig::default().checkpoint_pages, 1_000);
    assert_eq!(
        commit_read_work(17),
        WorkCounters {
            backend_read_operations: 1,
            authority_records_read: 1,
            authority_bytes_read: 17,
            bytes_hashed: 17 + AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES,
            bytes_copied: 17,
            allocation_operations: 1,
            peak_allocation_bytes: 17,
            ..WorkCounters::default()
        }
    );
    assert_eq!(
        commit_read_work(0),
        WorkCounters {
            backend_read_operations: 1,
            authority_records_read: 1,
            bytes_hashed: AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES,
            ..WorkCounters::default()
        }
    );

    let genesis = Head::genesis(Epoch::GENESIS);
    let next_sequence = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(1),
        digest: Digest::from_bytes([1; 32]),
    };
    let next_epoch = Head {
        epoch: Epoch::new(2).unwrap_or(Epoch::GENESIS),
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([2; 32]),
    };
    assert!(!head_is_newer(genesis, genesis));
    assert!(head_is_newer(next_sequence, genesis));
    assert!(!head_is_newer(genesis, next_sequence));
    assert!(head_is_newer(next_epoch, next_sequence));
    assert!(!head_is_newer(next_sequence, next_epoch));
    assert!(sqlite_is_corrupt(ErrorCode::DatabaseCorrupt));
    assert!(sqlite_is_corrupt(ErrorCode::NotADatabase));
    assert!(!sqlite_is_corrupt(ErrorCode::CannotOpen));
    for code in [rusqlite::ffi::SQLITE_CORRUPT, rusqlite::ffi::SQLITE_NOTADB] {
        assert!(matches!(
            map_sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None
            )),
            AuthorityStoreError::Corrupt(_)
        ));
    }
    assert!(matches!(
        map_sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            None
        )),
        AuthorityStoreError::Io(_)
    ));
}

#[test]
fn configuration_and_payload_boundaries_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    for config in [
        LocalAuthorityConfig {
            max_payload_bytes: 0,
            checkpoint_pages: 1,
        },
        LocalAuthorityConfig {
            max_payload_bytes: 1,
            checkpoint_pages: 0,
        },
    ] {
        assert!(matches!(
            LocalAuthorityStore::open(directory.path(), config),
            Err(AuthorityStoreError::Rejected(_))
        ));
    }

    let store = LocalAuthorityStore::open(
        directory.path(),
        LocalAuthorityConfig {
            max_payload_bytes: 3,
            checkpoint_pages: 1,
        },
    )?;
    let authority = AuthorityId::from_bytes([31; 16]);
    let created = store.create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    assert!(matches!(
        store.compare_and_append(
            authority,
            Epoch::GENESIS,
            head,
            proposed(32, b"1234"),
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::PayloadTooLarge {
                observed: 4,
                maximum: 3
            },
            ..
        })
    ));
    let committed = store.compare_and_append(
        authority,
        Epoch::GENESIS,
        head,
        proposed(33, b"123"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(committed.value, AppendOutcome::Committed(_)));

    let older_epoch_authority = AuthorityId::from_bytes([36; 16]);
    store.create_authority(older_epoch_authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    assert!(matches!(
        store.create_authority(older_epoch_authority, Epoch::new(2)?, WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Corrupt(_),
            ..
        })
    ));

    let larger_directory = tempdir()?;
    let larger = LocalAuthorityStore::open(
        larger_directory.path(),
        LocalAuthorityConfig {
            max_payload_bytes: 4,
            checkpoint_pages: 1,
        },
    )?;
    let larger_authority = AuthorityId::from_bytes([37; 16]);
    let created =
        larger.create_authority(larger_authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(larger_head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    larger.compare_and_append(
        larger_authority,
        Epoch::GENESIS,
        larger_head,
        proposed(38, b"1234"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(larger.head(larger_authority, WorkBudget::UNBOUNDED).is_ok());
    drop(larger);
    let smaller = LocalAuthorityStore::open(
        larger_directory.path(),
        LocalAuthorityConfig {
            max_payload_bytes: 3,
            checkpoint_pages: 1,
        },
    )?;
    assert!(matches!(
        smaller.head(larger_authority, WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Corrupt(_),
            ..
        })
    ));
    Ok(())
}

#[test]
fn replay_cursor_record_and_payload_boundaries_are_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let store = LocalAuthorityStore::open(
        directory.path(),
        LocalAuthorityConfig {
            max_payload_bytes: 3,
            checkpoint_pages: 1,
        },
    )?;
    let authority = AuthorityId::from_bytes([34; 16]);
    let created = store.create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    let first = store.compare_and_append(
        authority,
        Epoch::GENESIS,
        head,
        proposed(35, b"123"),
        WorkBudget::UNBOUNDED,
    )?;
    let AppendOutcome::Committed(first) = first.value else {
        return Err("first append did not commit".into());
    };
    let second = store.compare_and_append(
        authority,
        Epoch::GENESIS,
        Head {
            epoch: first.epoch,
            sequence: first.sequence,
            digest: first.digest,
        },
        proposed(36, b"456"),
        WorkBudget::UNBOUNDED,
    )?;
    let AppendOutcome::Committed(second) = second.value else {
        return Err("second append did not commit".into());
    };
    assert_chain_boundaries(&first, &second)?;

    assert_replay_boundaries(&store, authority, &second)?;
    Ok(())
}

#[test]
fn local_authority_reopens_replays_fences_and_deduplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority = AuthorityId::from_bytes([1; 16]);
    let committed;
    {
        let store = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
        let created = store.create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
        assert_eq!(
            created.work,
            WorkCounters {
                backend_read_operations: 1,
                backend_write_operations: 1,
                durability_operations: 1,
                items_examined: 1,
                bytes_encoded: 104,
                bytes_hashed: 104,
                ..WorkCounters::default()
            }
        );
        let CreateAuthorityOutcome::Created(head) = created.value else {
            return Err("authority unexpectedly existed".into());
        };
        let candidate = proposed(2, b"one");
        let append = store.compare_and_append(
            authority,
            Epoch::GENESIS,
            head,
            candidate.clone(),
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(
            append.work,
            WorkCounters {
                authority_records_appended: 1,
                authority_bytes_written: 3,
                backend_read_operations: 2,
                backend_write_operations: 2,
                durability_operations: 1,
                bytes_hashed: 107 + AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES,
                bytes_copied: 3,
                bytes_encoded: 159,
                items_examined: 2,
                ..WorkCounters::default()
            }
        );
        committed = match append.value {
            AppendOutcome::Committed(commit) => commit,
            other => return Err(format!("unexpected append: {other:?}").into()),
        };
        assert!(matches!(
            store
                .compare_and_append(
                    authority,
                    Epoch::GENESIS,
                    head,
                    candidate,
                    WorkBudget::UNBOUNDED,
                )?
                .value,
            AppendOutcome::AlreadyCommitted(_)
        ));
        let current = store.head(authority, WorkBudget::UNBOUNDED)?.value;
        let fenced = store.fence(authority, current, WorkBudget::UNBOUNDED)?;
        let FenceOutcome::Advanced(fenced_head) = fenced.value else {
            return Err("fresh local fence conflicted".into());
        };
        assert_eq!(fenced_head.epoch, Epoch::new(2)?);
        assert_eq!(fenced_head.digest, committed.digest);
        assert_eq!(fenced.work.authority_records_appended, 1);
        assert_eq!(fenced.work.backend_write_operations, 1);
        assert_eq!(fenced.work.durability_operations, 1);
        assert_eq!(fenced.work.bytes_encoded, 80);
        assert_eq!(fenced.work.bytes_hashed, 362);
    }
    let reopened = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    assert_eq!(
        reopened.head(authority, WorkBudget::UNBOUNDED)?.value.epoch,
        Epoch::new(2)?
    );
    let replay = reopened.replay(
        authority,
        Sequence::GENESIS,
        ReplayLimit {
            records: 8,
            payload_bytes: 64,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(replay.value, vec![committed]);
    assert_eq!(replay.work.backend_read_operations, 2);
    assert_eq!(replay.work.authority_records_read, 1);
    assert_eq!(replay.work.authority_bytes_read, 3);
    assert_eq!(replay.work.bytes_copied, 3);
    assert_eq!(replay.work.allocation_operations, 1);
    assert_eq!(replay.work.peak_allocation_bytes, 3);
    Ok(())
}

#[test]
fn replay_limits_and_head_checksum_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority = AuthorityId::from_bytes([3; 16]);
    let store = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let created = store.create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    let _receipt = store.compare_and_append(
        authority,
        Epoch::GENESIS,
        head,
        proposed(4, b"payload"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        store.replay(
            authority,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: 1,
            },
            WorkBudget::UNBOUNDED,
        ),
        Err(AuthorityFailure {
            error: AuthorityStoreError::ReplayRecordTooLarge { .. },
            ..
        })
    ));
    store.connection()?.execute(
        "UPDATE authority SET head_checksum = zeroblob(32) WHERE authority_id = ?1",
        params![authority.into_bytes().as_slice()],
    )?;
    assert!(matches!(
        store.head(authority, WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Corrupt(_),
            ..
        })
    ));
    Ok(())
}

#[test]
fn local_notifications_persist_and_coalesce_without_becoming_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority = AuthorityId::from_bytes([8; 16]);
    let newer = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(9),
        digest: Digest::from_bytes([9; 32]),
    };
    {
        let store = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
        let published =
            NotificationStore::publish(&store, authority, newer, WorkBudget::UNBOUNDED)?;
        assert_eq!(
            published.work,
            WorkCounters {
                backend_read_operations: 1,
                backend_write_operations: 1,
                durability_operations: 1,
                items_examined: 1,
                bytes_encoded: 80,
                bytes_hashed: 80,
                ..WorkCounters::default()
            }
        );
        let stale = NotificationStore::publish(
            &store,
            authority,
            Head {
                epoch: Epoch::GENESIS,
                sequence: Sequence::new(8),
                digest: Digest::from_bytes([8; 32]),
            },
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(
            stale.work,
            WorkCounters {
                backend_read_operations: 1,
                items_examined: 1,
                ..WorkCounters::default()
            }
        );
        assert!(matches!(
            store.head(authority, WorkBudget::UNBOUNDED),
            Err(AuthorityFailure {
                error: AuthorityStoreError::Missing,
                ..
            })
        ));
    }
    let reopened = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let polled = NotificationStore::poll_after(
        &reopened,
        authority,
        Head::genesis(Epoch::GENESIS),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        polled,
        crate::OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        } if value == newer
    ));
    assert_eq!(
        polled.work,
        WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            bytes_hashed: 80,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn authority_listing_is_snapshot_ordered_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    for value in [7_u8, 2, 9] {
        store.create_authority(
            AuthorityId::from_bytes([value; 16]),
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
        )?;
    }
    assert!(store.list_authorities(2, WorkBudget::UNBOUNDED).is_err());
    let listed = store.list_authorities(3, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        listed.value,
        vec![
            AuthorityId::from_bytes([2; 16]),
            AuthorityId::from_bytes([7; 16]),
            AuthorityId::from_bytes([9; 16]),
        ]
    );
    assert_eq!(
        listed.work,
        WorkCounters {
            backend_read_operations: 2,
            authority_records_read: 3,
            items_examined: 3,
            items_returned: 3,
            allocation_operations: 1,
            peak_allocation_bytes: 48,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn independent_connections_serialize_compare_and_append_without_split_brain()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority = AuthorityId::from_bytes([21; 16]);
    let bootstrap = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let head = match bootstrap
        .create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value
    {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => {
            return Err("authority unexpectedly existed".into());
        }
    };
    drop(bootstrap);
    let first = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let second = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let barrier = Arc::new(Barrier::new(2));
    let (first_outcome, second_outcome) = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first_thread = scope.spawn(move || {
            first_barrier.wait();
            first
                .compare_and_append(
                    authority,
                    Epoch::GENESIS,
                    head,
                    proposed(22, b"first"),
                    WorkBudget::UNBOUNDED,
                )
                .map(|receipt| receipt.value)
        });
        let second_barrier = Arc::clone(&barrier);
        let second_thread = scope.spawn(move || {
            second_barrier.wait();
            second
                .compare_and_append(
                    authority,
                    Epoch::GENESIS,
                    head,
                    proposed(23, b"second"),
                    WorkBudget::UNBOUNDED,
                )
                .map(|receipt| receipt.value)
        });
        (first_thread.join(), second_thread.join())
    });
    let first_outcome = first_outcome.map_err(|_| "first writer panicked")??;
    let second_outcome = second_outcome.map_err(|_| "second writer panicked")??;
    assert!(matches!(
        (&first_outcome, &second_outcome),
        (AppendOutcome::Committed(_), AppendOutcome::Conflict { .. })
            | (AppendOutcome::Conflict { .. }, AppendOutcome::Committed(_))
    ));
    let reopened = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let replay = reopened.replay(
        authority,
        Sequence::GENESIS,
        ReplayLimit {
            records: 2,
            payload_bytes: 32,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(replay.value.len(), 1);
    assert_eq!(
        reopened
            .head(authority, WorkBudget::UNBOUNDED)?
            .value
            .digest,
        replay.value[0].digest
    );
    Ok(())
}

#[test]
fn independent_connections_compare_and_fence_admit_exactly_one_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority = AuthorityId::from_bytes([0xf1; 16]);
    let bootstrap = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let initial = match bootstrap
        .create_authority(authority, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value
    {
        CreateAuthorityOutcome::Created(head) => head,
        CreateAuthorityOutcome::Existing(_) => return Err("authority unexpectedly existed".into()),
    };
    drop(bootstrap);
    let first = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let second = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let barrier = Arc::new(Barrier::new(2));
    let (first_outcome, second_outcome) = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first_thread = scope.spawn(move || {
            first_barrier.wait();
            first
                .fence(authority, initial, WorkBudget::UNBOUNDED)
                .map(|receipt| receipt.value)
        });
        let second_barrier = Arc::clone(&barrier);
        let second_thread = scope.spawn(move || {
            second_barrier.wait();
            second
                .fence(authority, initial, WorkBudget::UNBOUNDED)
                .map(|receipt| receipt.value)
        });
        (first_thread.join(), second_thread.join())
    });
    let first_outcome = first_outcome.map_err(|_| "first fence panicked")??;
    let second_outcome = second_outcome.map_err(|_| "second fence panicked")??;
    let advanced = match (first_outcome, second_outcome) {
        (FenceOutcome::Advanced(advanced), FenceOutcome::Conflict { actual })
        | (FenceOutcome::Conflict { actual }, FenceOutcome::Advanced(advanced))
            if actual == advanced =>
        {
            advanced
        }
        outcomes => return Err(format!("invalid fence race outcomes: {outcomes:?}").into()),
    };
    assert_eq!(advanced.epoch, Epoch::new(2)?);
    assert_eq!(advanced.sequence, initial.sequence);
    assert_eq!(advanced.digest, initial.digest);

    let reopened = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;
    let committed = reopened.compare_and_append(
        authority,
        advanced.epoch,
        advanced,
        proposed(0xf2, b"owner"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(committed.value, AppendOutcome::Committed(_)));
    let stale = reopened.compare_and_append(
        authority,
        initial.epoch,
        initial,
        proposed(0xf3, b"stale"),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        stale.value,
        AppendOutcome::Fenced { actual_epoch } if actual_epoch == advanced.epoch
    ));
    Ok(())
}
