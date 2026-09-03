//! Durable local authority log backed by one transactional `SQLite` WAL.
//!
//! `SQLite` owns physical segmentation, crash recovery, checkpoints, and indexed
//! lookup. Rows remain canonical hash-chained authority facts; `SQLite` is only
//! the local implementation of the public authority-store contract.

use crate::foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, Digest, DurableCommit, Epoch, Head,
    OperationId, ProposedCommit, Sequence, authority_commit_digest,
};
use crate::local::create_directories_durable;
use crate::notification::{
    NotificationError, NotificationPoll, NotificationResult, NotificationStore,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityReceipt, AuthorityResult, AuthorityStore,
    AuthorityStoreError, CreateAuthorityOutcome, ObjectStoreError, ReplayLimit,
};
use bytes::Bytes;
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// Default maximum canonical authority payload: 16 MiB.
pub const DEFAULT_MAX_PAYLOAD_BYTES: u32 = 16 * 1024 * 1024;
/// Default `SQLite` automatic-checkpoint interval.
pub const DEFAULT_CHECKPOINT_PAGES: u32 = 1_000;

const SCHEMA_VERSION: i64 = 1;
const HEAD_DOMAIN: &[u8] = b"acyclic-fs-local-head-v1\0";
const HINT_DOMAIN: &[u8] = b"acyclic-fs-local-hint-v1\0";

/// Bounded local authority configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalAuthorityConfig {
    /// Maximum canonical payload accepted for one authority fact.
    pub max_payload_bytes: u32,
    /// Positive `SQLite` WAL pages between automatic checkpoints.
    pub checkpoint_pages: u32,
}

impl Default for LocalAuthorityConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            checkpoint_pages: DEFAULT_CHECKPOINT_PAGES,
        }
    }
}

/// Process-safe durable authority store with indexed replay and idempotency.
pub struct LocalAuthorityStore {
    connection: Mutex<Connection>,
    config: LocalAuthorityConfig,
}

impl LocalAuthorityStore {
    /// Opens or creates the durable authority database below the supplied root.
    ///
    /// WAL mode and full synchronous durability are mandatory. Failure to
    /// establish either contract rejects the backend.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, filesystem, schema, or `SQLite` failure.
    pub fn open(
        root: impl AsRef<Path>,
        config: LocalAuthorityConfig,
    ) -> Result<Self, AuthorityStoreError> {
        if config.max_payload_bytes == 0 || config.checkpoint_pages == 0 {
            return Err(AuthorityStoreError::Rejected(
                "authority payload and checkpoint bounds must be positive".to_owned(),
            ));
        }
        let root = root.as_ref();
        let directory = root.join("authority");
        create_directories_durable(root, &directory).map_err(map_object_error)?;
        let connection = Connection::open(directory.join("events.sqlite3")).map_err(map_sqlite)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(map_sqlite)?;
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(map_sqlite)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(AuthorityStoreError::Rejected(format!(
                "SQLite refused WAL mode and selected {journal_mode}"
            )));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(map_sqlite)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(map_sqlite)?;
        connection
            .pragma_update(None, "wal_autocheckpoint", config.checkpoint_pages)
            .map_err(map_sqlite)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS authority (
                    authority_id BLOB PRIMARY KEY NOT NULL CHECK(length(authority_id) = 16),
                    genesis_epoch BLOB NOT NULL CHECK(length(genesis_epoch) = 8),
                    epoch BLOB NOT NULL CHECK(length(epoch) = 8),
                    sequence BLOB NOT NULL CHECK(length(sequence) = 8),
                    digest BLOB NOT NULL CHECK(length(digest) = 32),
                    head_checksum BLOB NOT NULL CHECK(length(head_checksum) = 32)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS authority_commit (
                    authority_id BLOB NOT NULL,
                    sequence BLOB NOT NULL CHECK(length(sequence) = 8),
                    epoch BLOB NOT NULL CHECK(length(epoch) = 8),
                    operation_id BLOB NOT NULL CHECK(length(operation_id) = 16),
                    fingerprint BLOB NOT NULL CHECK(length(fingerprint) = 32),
                    previous_digest BLOB NOT NULL CHECK(length(previous_digest) = 32),
                    digest BLOB NOT NULL CHECK(length(digest) = 32),
                    payload BLOB NOT NULL,
                    PRIMARY KEY(authority_id, sequence),
                    UNIQUE(authority_id, operation_id),
                    FOREIGN KEY(authority_id) REFERENCES authority(authority_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS authority_commit_operation
                    ON authority_commit(authority_id, operation_id);
                CREATE TABLE IF NOT EXISTS authority_hint (
                    authority_id BLOB PRIMARY KEY NOT NULL CHECK(length(authority_id) = 16),
                    epoch BLOB NOT NULL CHECK(length(epoch) = 8),
                    sequence BLOB NOT NULL CHECK(length(sequence) = 8),
                    digest BLOB NOT NULL CHECK(length(digest) = 32),
                    checksum BLOB NOT NULL CHECK(length(checksum) = 32)
                ) WITHOUT ROWID;",
            )
            .map_err(map_sqlite)?;
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(map_sqlite)?;
        if version == 0 {
            connection
                .pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(map_sqlite)?;
        } else if version != SCHEMA_VERSION {
            return Err(AuthorityStoreError::Corrupt(format!(
                "unsupported local authority schema version {version}"
            )));
        }
        Ok(Self {
            connection: Mutex::new(connection),
            config,
        })
    }

    /// Lists every local authority identity under one explicit result bound.
    ///
    /// This is an indexed physical projection used by local maintenance such
    /// as authenticated garbage collection; authority rows remain the truth.
    ///
    /// # Errors
    ///
    /// Rejects a zero or insufficient bound, malformed identities, `SQLite`
    /// failure, allocation failure, or work outside `budget`.
    pub fn list_authorities(
        &self,
        maximum_authorities: u32,
        budget: WorkBudget,
    ) -> AuthorityResult<Vec<AuthorityId>> {
        if maximum_authorities == 0 {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::Rejected(
                    "authority listing bound must be positive".to_owned(),
                ),
            ));
        }
        let mut connection = self.connection().map_err(AuthorityFailure::before_work)?;
        let count_work = WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        admit(count_work, budget)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, count_work))?;
        let count: i64 = transaction
            .query_row("SELECT COUNT(*) FROM authority", [], |row| row.get(0))
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, count_work))?;
        let count = u32::try_from(count).map_err(|_| {
            AuthorityFailure::new(
                AuthorityStoreError::Corrupt("authority count is invalid".to_owned()),
                count_work,
            )
        })?;
        if count > maximum_authorities {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Rejected(format!(
                    "authority count {count} exceeds bound {maximum_authorities}"
                )),
                count_work,
            ));
        }
        let retained = u64::from(count).saturating_mul(16);
        let work = count_work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                authority_records_read: u64::from(count),
                items_examined: u64::from(count),
                items_returned: u64::from(count),
                allocation_operations: u64::from(count != 0),
                peak_allocation_bytes: retained,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), count_work))?;
        admit(work, budget)?;
        let authorities = read_authority_ids(&transaction, count, count_work, work)?;
        transaction
            .commit()
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))?;
        Ok(AuthorityReceipt {
            value: authorities,
            work,
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, AuthorityStoreError> {
        self.connection
            .lock()
            .map_err(|_| AuthorityStoreError::Rejected("local authority mutex poisoned".to_owned()))
    }
}

fn read_authority_ids(
    transaction: &rusqlite::Transaction<'_>,
    count: u32,
    prior_work: WorkCounters,
    work: WorkCounters,
) -> Result<Vec<AuthorityId>, AuthorityFailure> {
    let mut statement = transaction
        .prepare("SELECT authority_id FROM authority ORDER BY authority_id")
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, prior_work))?;
    let mut rows = statement
        .query([])
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, prior_work))?;
    let expected = usize::try_from(count).unwrap_or(usize::MAX);
    let mut authorities = Vec::new();
    authorities.try_reserve_exact(expected).map_err(|_| {
        AuthorityFailure::new(
            AuthorityStoreError::Rejected("authority listing allocation failed".to_owned()),
            prior_work,
        )
    })?;
    while let Some(row) = rows
        .next()
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, work))?
    {
        let bytes = row
            .get_ref(0)
            .map_err(map_sqlite)
            .and_then(|value| {
                value
                    .as_blob()
                    .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
            })
            .map_err(|error| AuthorityFailure::new(error, work))?;
        authorities.push(AuthorityId::from_bytes(decode_array(
            bytes,
            "authority id",
            work,
        )?));
    }
    if authorities.len() != expected {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt("authority snapshot count changed".to_owned()),
            work,
        ));
    }
    Ok(authorities)
}

impl AuthorityStore for LocalAuthorityStore {
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        let mut work = WorkCounters {
            backend_write_operations: 1,
            durability_operations: 1,
            bytes_encoded: 104,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        let mut connection = self.connection().map_err(AuthorityFailure::before_work)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))?;
        let head = Head::genesis(genesis_epoch);
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO authority
                 (authority_id, genesis_epoch, epoch, sequence, digest, head_checksum)
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5)",
                params![
                    authority_id.into_bytes().as_slice(),
                    encode_u64(genesis_epoch.get()).as_slice(),
                    encode_u64(Sequence::GENESIS.get()).as_slice(),
                    Digest::ZERO.as_bytes().as_slice(),
                    head_checksum(authority_id, genesis_epoch, head)
                        .as_bytes()
                        .as_slice(),
                ],
            )
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))?;
        let loaded = read_head(&transaction, authority_id, self.config, budget, work)?;
        work = loaded.work;
        transaction
            .commit()
            .map_err(|error| indeterminate("create authority", error))
            .map_err(|error| AuthorityFailure::new(error, work))?;
        if loaded.value.epoch < genesis_epoch {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Corrupt(
                    "stored genesis epoch is older than requested".to_owned(),
                ),
                work,
            ));
        }
        Ok(AuthorityReceipt {
            value: if inserted == 1 {
                CreateAuthorityOutcome::Created(loaded.value)
            } else {
                CreateAuthorityOutcome::Existing(loaded.value)
            },
            work,
        })
    }

    fn head(&self, authority_id: AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        let connection = self.connection().map_err(AuthorityFailure::before_work)?;
        read_head(
            &connection,
            authority_id,
            self.config,
            budget,
            WorkCounters::default(),
        )
    }

    #[allow(clippy::too_many_lines)]
    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        proposed: ProposedCommit,
        budget: WorkBudget,
    ) -> AuthorityResult<AppendOutcome> {
        let payload_bytes = u64::try_from(proposed.payload.len()).unwrap_or(u64::MAX);
        if payload_bytes > u64::from(self.config.max_payload_bytes) {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::PayloadTooLarge {
                    observed: payload_bytes,
                    maximum: u64::from(self.config.max_payload_bytes),
                },
            ));
        }
        let mut connection = self.connection().map_err(AuthorityFailure::before_work)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)
            .map_err(AuthorityFailure::before_work)?;
        let loaded = read_head(
            &transaction,
            authority_id,
            self.config,
            budget,
            WorkCounters::default(),
        )?;
        let mut work = loaded.work;
        let actual = loaded.value;
        if epoch != actual.epoch {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                },
                work,
            });
        }
        let existing = read_operation(
            &transaction,
            authority_id,
            proposed.operation_id,
            self.config,
            budget,
            work,
        )?;
        work = existing.work;
        if let Some(existing) = existing.value {
            let value = if existing.fingerprint == proposed.fingerprint {
                AppendOutcome::AlreadyCommitted(existing)
            } else {
                AppendOutcome::IdempotencyConflict {
                    committed_fingerprint: existing.fingerprint,
                }
            };
            return Ok(AuthorityReceipt { value, work });
        }
        if expected != actual {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Conflict { actual },
                work,
            });
        }
        let sequence = actual
            .sequence
            .checked_next()
            .map_err(|_| AuthorityFailure::new(AuthorityStoreError::SequenceExhausted, work))?;
        let digest = authority_commit_digest(
            authority_id,
            epoch,
            sequence,
            proposed.operation_id,
            proposed.fingerprint,
            actual.digest,
            &proposed.payload,
        );
        let commit = DurableCommit {
            epoch,
            sequence,
            operation_id: proposed.operation_id,
            fingerprint: proposed.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: proposed.payload,
        };
        work = work
            .checked_add(WorkCounters {
                authority_records_appended: 1,
                authority_bytes_written: payload_bytes,
                backend_write_operations: 2,
                durability_operations: 1,
                bytes_encoded: payload_bytes.saturating_add(156),
                bytes_hashed: payload_bytes.saturating_add(AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES),
                bytes_copied: payload_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        admit(work, budget)?;
        transaction
            .execute(
                "INSERT INTO authority_commit
                 (authority_id, sequence, epoch, operation_id, fingerprint,
                  previous_digest, digest, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    authority_id.into_bytes().as_slice(),
                    encode_u64(commit.sequence.get()).as_slice(),
                    encode_u64(commit.epoch.get()).as_slice(),
                    commit.operation_id.into_bytes().as_slice(),
                    commit.fingerprint.as_bytes().as_slice(),
                    commit.previous_digest.as_bytes().as_slice(),
                    commit.digest.as_bytes().as_slice(),
                    commit.payload.as_ref(),
                ],
            )
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))?;
        let next_head = Head {
            epoch,
            sequence,
            digest,
        };
        update_head(&transaction, authority_id, next_head, work)?;
        transaction
            .commit()
            .map_err(|error| indeterminate("append", error))
            .map_err(|error| AuthorityFailure::new(error, work))?;
        Ok(AuthorityReceipt {
            value: AppendOutcome::Committed(commit),
            work,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::InvalidReplayLimit,
            ));
        }
        let connection = self.connection().map_err(AuthorityFailure::before_work)?;
        let summary =
            read_head_summary(&connection, authority_id, budget, WorkCounters::default())?;
        if after > summary.value.sequence {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Rejected(format!(
                    "replay cursor {} exceeds head {}",
                    after.get(),
                    summary.value.sequence.get()
                )),
                summary.work,
            ));
        }
        let mut statement = connection
            .prepare(
                "SELECT epoch, sequence, operation_id, fingerprint, previous_digest, digest, payload
                 FROM authority_commit
                 WHERE authority_id = ?1 AND sequence > ?2
                 ORDER BY sequence LIMIT ?3",
            )
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, summary.work))?;
        let mut rows = statement
            .query(params![
                authority_id.into_bytes().as_slice(),
                encode_u64(after.get()).as_slice(),
                i64::from(limit.records),
            ])
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, summary.work))?;
        let mut commits = Vec::new();
        commits
            .try_reserve_exact(usize::try_from(limit.records).unwrap_or(usize::MAX))
            .map_err(|_| {
                AuthorityFailure::new(
                    AuthorityStoreError::Rejected("replay allocation failed".to_owned()),
                    summary.work,
                )
            })?;
        let mut work = summary.work;
        let mut payload_total = 0_u64;
        let mut expected_sequence = after;
        let mut expected_previous = if after == Sequence::GENESIS {
            Digest::ZERO
        } else {
            let predecessor = read_digest(&connection, authority_id, after, budget, work)?;
            work = predecessor.work;
            predecessor.value
        };
        while let Some(row) = rows
            .next()
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))?
        {
            let payload = row
                .get_ref(6)
                .map_err(map_sqlite)
                .and_then(|value| {
                    value
                        .as_blob()
                        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
                })
                .map_err(|error| AuthorityFailure::new(error, work))?;
            let payload_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
            let next_total = payload_total.checked_add(payload_bytes).ok_or_else(|| {
                AuthorityFailure::new(AuthorityStoreError::Work(crate::WorkError::Overflow), work)
            })?;
            if next_total > limit.payload_bytes {
                if commits.is_empty() {
                    return Err(AuthorityFailure::new(
                        AuthorityStoreError::ReplayRecordTooLarge {
                            observed: payload_bytes,
                            maximum: limit.payload_bytes,
                        },
                        work,
                    ));
                }
                break;
            }
            let next_work = work
                .checked_add(commit_read_work(payload_bytes))
                .map_err(|error| AuthorityFailure::new(error.into(), work))?;
            admit(next_work, budget)?;
            let commit = decode_commit_row(row, payload, next_work)?;
            validate_commit(authority_id, &commit, next_work)?;
            let next_sequence = expected_sequence.checked_next().map_err(|_| {
                AuthorityFailure::new(AuthorityStoreError::SequenceExhausted, next_work)
            })?;
            if replay_chain_is_discontinuous(&commit, next_sequence, expected_previous) {
                return Err(AuthorityFailure::new(
                    AuthorityStoreError::Corrupt(
                        "authority replay chain is discontinuous".to_owned(),
                    ),
                    next_work,
                ));
            }
            expected_sequence = commit.sequence;
            expected_previous = commit.digest;
            payload_total = next_total;
            work = next_work;
            commits.push(commit);
        }
        Ok(AuthorityReceipt {
            value: commits,
            work,
        })
    }

    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
    ) -> AuthorityResult<crate::storage::FenceOutcome> {
        let mut connection = self.connection().map_err(AuthorityFailure::before_work)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)
            .map_err(AuthorityFailure::before_work)?;
        let loaded = read_head(
            &transaction,
            authority_id,
            self.config,
            budget,
            WorkCounters::default(),
        )?;
        if loaded.value != expected {
            return Ok(AuthorityReceipt {
                value: crate::storage::FenceOutcome::Conflict {
                    actual: loaded.value,
                },
                work: loaded.work,
            });
        }
        let epoch_value = expected.epoch.get().checked_add(1).ok_or_else(|| {
            AuthorityFailure::new(AuthorityStoreError::EpochExhausted, loaded.work)
        })?;
        let epoch = Epoch::new(epoch_value)
            .map_err(|_| AuthorityFailure::new(AuthorityStoreError::EpochExhausted, loaded.work))?;
        let head = Head {
            epoch,
            sequence: loaded.value.sequence,
            digest: loaded.value.digest,
        };
        let work = loaded
            .work
            .checked_add(WorkCounters {
                authority_records_appended: 1,
                backend_write_operations: 1,
                durability_operations: 1,
                bytes_encoded: 80,
                bytes_hashed: 104,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), loaded.work))?;
        admit(work, budget)?;
        update_head(&transaction, authority_id, head, work)?;
        transaction
            .commit()
            .map_err(|error| indeterminate("fence", error))
            .map_err(|error| AuthorityFailure::new(error, work))?;
        Ok(AuthorityReceipt {
            value: crate::storage::FenceOutcome::Advanced(head),
            work,
        })
    }

    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let connection = self.connection().map_err(AuthorityFailure::before_work)?;
        read_operation(
            &connection,
            authority_id,
            operation_id,
            self.config,
            budget,
            WorkCounters::default(),
        )
    }
}

impl NotificationStore for LocalAuthorityStore {
    fn publish(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
    ) -> NotificationResult<()> {
        let read_work = WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            ..WorkCounters::default()
        };
        read_work.verify(budget).map_err(|error| {
            crate::OperationFailure::before_work(NotificationError::Work(error))
        })?;
        let connection = self
            .connection()
            .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, read_work))?;
        if let Some(current) = read_hint(&connection, authority_id, read_work)?
            && !head_is_newer(head, current)
        {
            return Ok(crate::OperationReceipt {
                value: (),
                work: read_work,
            });
        }
        let work = read_work
            .checked_add(WorkCounters {
                backend_write_operations: 1,
                durability_operations: 1,
                bytes_encoded: 80,
                bytes_hashed: 80,
                ..WorkCounters::default()
            })
            .map_err(|error| {
                crate::OperationFailure::new(NotificationError::Work(error), read_work)
            })?;
        work.verify(budget).map_err(|error| {
            crate::OperationFailure::new(NotificationError::Work(error), read_work)
        })?;
        connection
            .execute(
                "INSERT INTO authority_hint(authority_id, epoch, sequence, digest, checksum)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(authority_id) DO UPDATE SET
                    epoch = excluded.epoch,
                    sequence = excluded.sequence,
                    digest = excluded.digest,
                    checksum = excluded.checksum",
                params![
                    authority_id.into_bytes().as_slice(),
                    encode_u64(head.epoch.get()).as_slice(),
                    encode_u64(head.sequence.get()).as_slice(),
                    head.digest.as_bytes().as_slice(),
                    hint_checksum(authority_id, head).as_bytes().as_slice(),
                ],
            )
            .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?;
        Ok(crate::OperationReceipt { value: (), work })
    }

    fn poll_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
    ) -> NotificationResult<NotificationPoll> {
        let work = WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            bytes_hashed: 80,
            ..WorkCounters::default()
        };
        work.verify(budget).map_err(|error| {
            crate::OperationFailure::before_work(NotificationError::Work(error))
        })?;
        let connection = self
            .connection()
            .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?;
        let value = read_hint(&connection, authority_id, work)?
            .filter(|head| head_is_newer(*head, after))
            .map_or(NotificationPoll::Unchanged, NotificationPoll::Advanced);
        Ok(crate::OperationReceipt { value, work })
    }
}

fn read_hint(
    connection: &Connection,
    authority_id: AuthorityId,
    work: WorkCounters,
) -> Result<Option<Head>, crate::OperationFailure<NotificationError>> {
    let values = connection
        .query_row(
            "SELECT epoch, sequence, digest, checksum
             FROM authority_hint WHERE authority_id = ?1",
            params![authority_id.into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?;
    let Some(values) = values else {
        return Ok(None);
    };
    let epoch = u64::from_be_bytes(
        values
            .0
            .as_slice()
            .try_into()
            .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?,
    );
    let head =
        Head {
            epoch: Epoch::new(epoch)
                .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?,
            sequence: Sequence::new(u64::from_be_bytes(values.1.as_slice().try_into().map_err(
                |_| crate::OperationFailure::new(NotificationError::Unavailable, work),
            )?)),
            digest: Digest::from_bytes(
                values.2.as_slice().try_into().map_err(|_| {
                    crate::OperationFailure::new(NotificationError::Unavailable, work)
                })?,
            ),
        };
    let checksum = Digest::from_bytes(
        values
            .3
            .as_slice()
            .try_into()
            .map_err(|_| crate::OperationFailure::new(NotificationError::Unavailable, work))?,
    );
    if checksum != hint_checksum(authority_id, head) {
        return Err(crate::OperationFailure::new(
            NotificationError::Unavailable,
            work,
        ));
    }
    Ok(Some(head))
}

fn head_is_newer(candidate: Head, current: Head) -> bool {
    candidate.epoch > current.epoch
        || (candidate.epoch == current.epoch && candidate.sequence > current.sequence)
}

fn read_head(
    connection: &Connection,
    authority_id: AuthorityId,
    config: LocalAuthorityConfig,
    budget: WorkBudget,
    prior: WorkCounters,
) -> AuthorityResult<Head> {
    let summary = read_head_summary(connection, authority_id, budget, prior)?;
    if summary.value.sequence == Sequence::GENESIS {
        return Ok(summary);
    }
    let commit = read_commit_by_sequence(
        connection,
        authority_id,
        summary.value.sequence,
        config,
        budget,
        summary.work,
    )?;
    if !terminal_matches_head(&commit.value, summary.value) {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt(
                "authority head does not match terminal commit".to_owned(),
            ),
            commit.work,
        ));
    }
    Ok(AuthorityReceipt {
        value: summary.value,
        work: commit.work,
    })
}

fn read_head_summary(
    connection: &Connection,
    authority_id: AuthorityId,
    budget: WorkBudget,
    prior: WorkCounters,
) -> AuthorityResult<Head> {
    let work = prior
        .checked_add(WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            bytes_hashed: 104,
            ..WorkCounters::default()
        })
        .map_err(|error| AuthorityFailure::new(error.into(), prior))?;
    admit(work, budget)?;
    let values = connection
        .query_row(
            "SELECT genesis_epoch, epoch, sequence, digest, head_checksum
             FROM authority WHERE authority_id = ?1",
            params![authority_id.into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            },
        )
        .optional()
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, work))?
        .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, work))?;
    let genesis = decode_epoch(&values.0, "genesis epoch", work)?;
    let head = Head {
        epoch: decode_epoch(&values.1, "head epoch", work)?,
        sequence: Sequence::new(decode_u64(&values.2, "head sequence", work)?),
        digest: decode_digest(&values.3, "head digest", work)?,
    };
    if decode_digest(&values.4, "head checksum", work)?
        != head_checksum(authority_id, genesis, head)
    {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt("authority head checksum mismatch".to_owned()),
            work,
        ));
    }
    Ok(AuthorityReceipt { value: head, work })
}

fn read_operation(
    connection: &Connection,
    authority_id: AuthorityId,
    operation_id: OperationId,
    config: LocalAuthorityConfig,
    budget: WorkBudget,
    prior: WorkCounters,
) -> AuthorityResult<Option<DurableCommit>> {
    let query_work = prior
        .checked_add(WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| AuthorityFailure::new(error.into(), prior))?;
    admit(query_work, budget)?;
    let sequence = connection
        .query_row(
            "SELECT sequence FROM authority_commit
             WHERE authority_id = ?1 AND operation_id = ?2",
            params![
                authority_id.into_bytes().as_slice(),
                operation_id.into_bytes().as_slice(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, query_work))?;
    let Some(sequence) = sequence else {
        return Ok(AuthorityReceipt {
            value: None,
            work: query_work,
        });
    };
    let sequence = Sequence::new(decode_u64(&sequence, "operation sequence", query_work)?);
    let commit = read_commit_by_sequence(
        connection,
        authority_id,
        sequence,
        config,
        budget,
        query_work,
    )?;
    Ok(AuthorityReceipt {
        value: Some(commit.value),
        work: commit.work,
    })
}

fn read_commit_by_sequence(
    connection: &Connection,
    authority_id: AuthorityId,
    sequence: Sequence,
    config: LocalAuthorityConfig,
    budget: WorkBudget,
    prior: WorkCounters,
) -> AuthorityResult<DurableCommit> {
    let mut statement = connection
        .prepare(
            "SELECT epoch, sequence, operation_id, fingerprint, previous_digest, digest, payload
             FROM authority_commit WHERE authority_id = ?1 AND sequence = ?2",
        )
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, prior))?;
    let mut rows = statement
        .query(params![
            authority_id.into_bytes().as_slice(),
            encode_u64(sequence.get()).as_slice(),
        ])
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, prior))?;
    let row = rows
        .next()
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, prior))?
        .ok_or_else(|| {
            AuthorityFailure::new(
                AuthorityStoreError::Corrupt("authority commit is absent".to_owned()),
                prior,
            )
        })?;
    let payload = row
        .get_ref(6)
        .map_err(map_sqlite)
        .and_then(|value| {
            value
                .as_blob()
                .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
        })
        .map_err(|error| AuthorityFailure::new(error, prior))?;
    let payload_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    if payload_bytes > u64::from(config.max_payload_bytes) {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt("stored authority payload exceeds its bound".to_owned()),
            prior,
        ));
    }
    let work = prior
        .checked_add(commit_read_work(payload_bytes))
        .map_err(|error| AuthorityFailure::new(error.into(), prior))?;
    admit(work, budget)?;
    let commit = decode_commit_row(row, payload, work)?;
    validate_commit(authority_id, &commit, work)?;
    Ok(AuthorityReceipt {
        value: commit,
        work,
    })
}

fn read_digest(
    connection: &Connection,
    authority_id: AuthorityId,
    sequence: Sequence,
    budget: WorkBudget,
    prior: WorkCounters,
) -> AuthorityResult<Digest> {
    let work = prior
        .checked_add(WorkCounters {
            backend_read_operations: 1,
            authority_records_read: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| AuthorityFailure::new(error.into(), prior))?;
    admit(work, budget)?;
    let bytes = connection
        .query_row(
            "SELECT digest FROM authority_commit WHERE authority_id = ?1 AND sequence = ?2",
            params![
                authority_id.into_bytes().as_slice(),
                encode_u64(sequence.get()).as_slice(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, work))?
        .ok_or_else(|| {
            AuthorityFailure::new(
                AuthorityStoreError::Corrupt("replay predecessor is absent".to_owned()),
                work,
            )
        })?;
    Ok(AuthorityReceipt {
        value: decode_digest(&bytes, "predecessor digest", work)?,
        work,
    })
}

fn replay_chain_is_discontinuous(
    commit: &DurableCommit,
    expected_sequence: Sequence,
    expected_previous: Digest,
) -> bool {
    commit.sequence != expected_sequence || commit.previous_digest != expected_previous
}

fn terminal_matches_head(commit: &DurableCommit, head: Head) -> bool {
    commit.epoch <= head.epoch && commit.sequence == head.sequence && commit.digest == head.digest
}

fn decode_commit_row(
    row: &rusqlite::Row<'_>,
    payload: &[u8],
    work: WorkCounters,
) -> Result<DurableCommit, AuthorityFailure> {
    let bytes = |index: usize, field: &'static str| -> Result<Vec<u8>, AuthorityFailure> {
        row.get(index)
            .map_err(map_sqlite)
            .map_err(|error| AuthorityFailure::new(error, work))
            .and_then(|value: Vec<u8>| {
                if value.is_empty() {
                    Err(AuthorityFailure::new(
                        AuthorityStoreError::Corrupt(format!("{field} is empty")),
                        work,
                    ))
                } else {
                    Ok(value)
                }
            })
    };
    Ok(DurableCommit {
        epoch: decode_epoch(&bytes(0, "commit epoch")?, "commit epoch", work)?,
        sequence: Sequence::new(decode_u64(
            &bytes(1, "commit sequence")?,
            "commit sequence",
            work,
        )?),
        operation_id: OperationId::from_bytes(decode_array::<16>(
            &bytes(2, "operation id")?,
            "operation id",
            work,
        )?),
        fingerprint: decode_digest(&bytes(3, "fingerprint")?, "fingerprint", work)?,
        previous_digest: decode_digest(&bytes(4, "previous digest")?, "previous digest", work)?,
        digest: decode_digest(&bytes(5, "commit digest")?, "commit digest", work)?,
        payload: Bytes::copy_from_slice(payload),
    })
}

fn validate_commit(
    authority_id: AuthorityId,
    commit: &DurableCommit,
    work: WorkCounters,
) -> Result<(), AuthorityFailure> {
    let digest = authority_commit_digest(
        authority_id,
        commit.epoch,
        commit.sequence,
        commit.operation_id,
        commit.fingerprint,
        commit.previous_digest,
        &commit.payload,
    );
    if digest != commit.digest {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt("authority commit digest mismatch".to_owned()),
            work,
        ));
    }
    Ok(())
}

fn update_head(
    transaction: &rusqlite::Transaction<'_>,
    authority_id: AuthorityId,
    head: Head,
    work: WorkCounters,
) -> Result<(), AuthorityFailure> {
    let genesis = transaction
        .query_row(
            "SELECT genesis_epoch FROM authority WHERE authority_id = ?1",
            params![authority_id.into_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, work))?;
    let genesis = decode_epoch(&genesis, "genesis epoch", work)?;
    let updated = transaction
        .execute(
            "UPDATE authority SET epoch = ?2, sequence = ?3, digest = ?4, head_checksum = ?5
             WHERE authority_id = ?1",
            params![
                authority_id.into_bytes().as_slice(),
                encode_u64(head.epoch.get()).as_slice(),
                encode_u64(head.sequence.get()).as_slice(),
                head.digest.as_bytes().as_slice(),
                head_checksum(authority_id, genesis, head)
                    .as_bytes()
                    .as_slice(),
            ],
        )
        .map_err(map_sqlite)
        .map_err(|error| AuthorityFailure::new(error, work))?;
    if updated != 1 {
        return Err(AuthorityFailure::new(
            AuthorityStoreError::Corrupt("authority head update matched no row".to_owned()),
            work,
        ));
    }
    Ok(())
}

fn commit_read_work(payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        backend_read_operations: 1,
        authority_records_read: 1,
        authority_bytes_read: payload_bytes,
        bytes_hashed: payload_bytes.saturating_add(AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES),
        bytes_copied: payload_bytes,
        allocation_operations: u64::from(payload_bytes != 0),
        peak_allocation_bytes: payload_bytes,
        ..WorkCounters::default()
    }
}

fn head_checksum(authority_id: AuthorityId, genesis: Epoch, head: Head) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HEAD_DOMAIN);
    hasher.update(&authority_id.into_bytes());
    hasher.update(&genesis.get().to_le_bytes());
    hasher.update(&head.epoch.get().to_le_bytes());
    hasher.update(&head.sequence.get().to_le_bytes());
    hasher.update(head.digest.as_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn hint_checksum(authority_id: AuthorityId, head: Head) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HINT_DOMAIN);
    hasher.update(&authority_id.into_bytes());
    hasher.update(&head.epoch.get().to_le_bytes());
    hasher.update(&head.sequence.get().to_le_bytes());
    hasher.update(head.digest.as_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(
    bytes: &[u8],
    field: &'static str,
    work: WorkCounters,
) -> Result<u64, AuthorityFailure> {
    Ok(u64::from_be_bytes(decode_array(bytes, field, work)?))
}

fn decode_epoch(
    bytes: &[u8],
    field: &'static str,
    work: WorkCounters,
) -> Result<Epoch, AuthorityFailure> {
    Epoch::new(decode_u64(bytes, field, work)?)
        .map_err(|_| AuthorityFailure::new(AuthorityStoreError::Corrupt(field.to_owned()), work))
}

fn decode_digest(
    bytes: &[u8],
    field: &'static str,
    work: WorkCounters,
) -> Result<Digest, AuthorityFailure> {
    Ok(Digest::from_bytes(decode_array(bytes, field, work)?))
}

fn decode_array<const N: usize>(
    bytes: &[u8],
    field: &'static str,
    work: WorkCounters,
) -> Result<[u8; N], AuthorityFailure> {
    bytes.try_into().map_err(|_| {
        AuthorityFailure::new(
            AuthorityStoreError::Corrupt(format!("{field} has the wrong byte length")),
            work,
        )
    })
}

fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), AuthorityFailure> {
    work.verify(budget)
        .map_err(|error| AuthorityFailure::before_work(error.into()))
}

fn map_object_error(error: ObjectStoreError) -> AuthorityStoreError {
    match error {
        ObjectStoreError::Io(error) => AuthorityStoreError::Io(error),
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlite(error: rusqlite::Error) -> AuthorityStoreError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _) if sqlite_is_corrupt(failure.code) => {
            AuthorityStoreError::Corrupt(error.to_string())
        }
        _ => AuthorityStoreError::Io(std::io::Error::other(error.to_string())),
    }
}

fn sqlite_is_corrupt(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase)
}

#[allow(clippy::needless_pass_by_value)]
fn indeterminate(operation: &'static str, error: rusqlite::Error) -> AuthorityStoreError {
    AuthorityStoreError::Indeterminate {
        operation,
        source: std::io::Error::other(error.to_string()),
    }
}

#[cfg(test)]
#[path = "tests/local_authority.rs"]
mod tests;
