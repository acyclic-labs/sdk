//! Deterministic in-memory backends used by conformance tests and ephemeral deployments.

use crate::foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, DurableCommit, Epoch, Head, OperationId,
    ProposedCommit, Sequence, authority_commit_digest,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityReceipt, AuthorityResult, AuthorityStore,
    AuthorityStoreError, CreateAuthorityOutcome, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure,
    ObjectId, ObjectRead, ObjectReadRetention, ObjectReceipt, ObjectResult, ObjectStore,
    ObjectStoreError, ReplayLimit, object_digest,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Mutex, RwLock};

/// Default maximum immutable object size admitted by the memory backend.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Default)]
struct AuthorityState {
    epoch: Option<Epoch>,
    commits: Vec<DurableCommit>,
    operations: HashMap<OperationId, usize>,
}

impl AuthorityState {
    fn head(&self) -> Result<Head, AuthorityStoreError> {
        let epoch = self.epoch.ok_or(AuthorityStoreError::Missing)?;
        Ok(self.commits.last().map_or_else(
            || Head::genesis(epoch),
            |commit| Head {
                epoch,
                sequence: commit.sequence,
                digest: commit.digest,
            },
        ))
    }

    fn resolve_operation(
        &self,
        commit: &ProposedCommit,
        base: WorkCounters,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<AppendOutcome>> {
        let Some(index) = self.operations.get(&commit.operation_id) else {
            return Ok(AuthorityReceipt {
                value: None,
                work: base,
            });
        };
        let durable = self.commits.get(*index).ok_or_else(|| {
            AuthorityFailure::new(
                AuthorityStoreError::Corrupt("operation index outside commit history".to_owned()),
                base,
            )
        })?;
        let work = base
            .checked_add(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: u64::try_from(durable.payload.len()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), base))?;
        admit_authority(work, budget)?;
        let outcome = if durable.fingerprint == commit.fingerprint {
            AppendOutcome::AlreadyCommitted(durable.clone())
        } else {
            AppendOutcome::IdempotencyConflict {
                committed_fingerprint: durable.fingerprint,
            }
        };
        Ok(AuthorityReceipt {
            value: Some(outcome),
            work,
        })
    }
}

/// Deterministic single-process authority store.
pub struct MemoryAuthorityStore {
    maximum_payload_bytes: u64,
    authorities: Mutex<HashMap<AuthorityId, AuthorityState>>,
}

impl MemoryAuthorityStore {
    /// Creates a deterministic authority store with a hard per-record payload bound.
    ///
    /// # Errors
    ///
    /// Rejects a zero bound before allocating authority state.
    pub fn new(maximum_payload_bytes: u64) -> Result<Self, AuthorityStoreError> {
        if maximum_payload_bytes == 0 {
            return Err(AuthorityStoreError::PayloadTooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            maximum_payload_bytes,
            authorities: Mutex::new(HashMap::new()),
        })
    }
}

impl Default for MemoryAuthorityStore {
    fn default() -> Self {
        Self {
            maximum_payload_bytes: 16 * 1024 * 1024,
            authorities: Mutex::new(HashMap::new()),
        }
    }
}

impl AuthorityStore for MemoryAuthorityStore {
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::before_work(poisoned_authority()))?;
        if let Some(existing) = authorities.get(&authority_id) {
            let work = authority_read_work();
            admit_authority(work, budget)?;
            let head = existing
                .head()
                .map_err(|error| AuthorityFailure::new(error, work))?;
            return Ok(AuthorityReceipt {
                value: CreateAuthorityOutcome::Existing(head),
                work,
            });
        }
        let work = authority_write_work();
        admit_authority(work, budget)?;
        let state = AuthorityState {
            epoch: Some(genesis_epoch),
            ..AuthorityState::default()
        };
        let head = state
            .head()
            .map_err(|error| AuthorityFailure::new(error, work))?;
        authorities.insert(authority_id, state);
        Ok(AuthorityReceipt {
            value: CreateAuthorityOutcome::Created(head),
            work,
        })
    }

    fn head(&self, authority_id: AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        let work = authority_read_work();
        admit_authority(work, budget)?;
        let value = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::new(poisoned_authority(), work))?
            .get(&authority_id)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, work))?
            .head()
            .map_err(|error| AuthorityFailure::new(error, work))?;
        Ok(AuthorityReceipt { value, work })
    }

    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
    ) -> AuthorityResult<AppendOutcome> {
        let payload_bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
        if payload_bytes > self.maximum_payload_bytes {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::PayloadTooLarge {
                    observed: payload_bytes,
                    maximum: self.maximum_payload_bytes,
                },
            ));
        }
        let read_work = authority_read_work();
        admit_authority(read_work, budget)?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::new(poisoned_authority(), read_work))?;
        let state = authorities
            .get_mut(&authority_id)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, read_work))?;
        let actual = state
            .head()
            .map_err(|error| AuthorityFailure::new(error, read_work))?;
        if epoch != actual.epoch {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                },
                work: read_work,
            });
        }
        let existing = state.resolve_operation(&commit, read_work, budget)?;
        if let Some(outcome) = existing.value {
            return Ok(AuthorityReceipt {
                value: outcome,
                work: existing.work,
            });
        }
        if expected != actual {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Conflict { actual },
                work: read_work,
            });
        }
        let write_work = read_work
            .checked_add(WorkCounters {
                authority_records_appended: 1,
                authority_bytes_written: payload_bytes,
                backend_write_operations: 1,
                bytes_hashed: payload_bytes.saturating_add(AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        admit_authority(write_work, budget)?;
        let sequence = actual.sequence.checked_next().map_err(|error| {
            AuthorityFailure::new(
                AuthorityStoreError::Rejected(format!(
                    "authority sequence cannot advance: {error}"
                )),
                read_work,
            )
        })?;
        let digest = authority_commit_digest(
            authority_id,
            epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            actual.digest,
            &commit.payload,
        );
        let durable = DurableCommit {
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: commit.payload,
        };
        let index = state.commits.len();
        state.operations.insert(durable.operation_id, index);
        state.commits.push(durable.clone());
        Ok(AuthorityReceipt {
            value: AppendOutcome::Committed(durable),
            work: write_work,
        })
    }

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
        let base = authority_read_work();
        admit_authority(base, budget)?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::new(poisoned_authority(), base))?;
        let state = authorities
            .get(&authority_id)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, base))?;
        if after
            > state
                .head()
                .map_err(|error| AuthorityFailure::new(error, base))?
                .sequence
        {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Rejected(
                    "replay cursor is ahead of authority head".to_owned(),
                ),
                base,
            ));
        }
        let (start, end, payload_bytes) = bounded_page_bounds(&state.commits, after, limit)
            .map_err(|error| AuthorityFailure::new(error, base))?;
        let work = base
            .checked_add(WorkCounters {
                authority_records_read: u64::try_from(end - start).unwrap_or(u64::MAX),
                authority_bytes_read: payload_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), base))?;
        admit_authority(work, budget)?;
        Ok(AuthorityReceipt {
            value: state.commits[start..end].to_vec(),
            work,
        })
    }

    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
    ) -> AuthorityResult<crate::storage::FenceOutcome> {
        let read_work = authority_read_work();
        admit_authority(read_work, budget)?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::new(poisoned_authority(), read_work))?;
        let state = authorities
            .get_mut(&authority_id)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, read_work))?;
        let actual = state
            .head()
            .map_err(|error| AuthorityFailure::new(error, read_work))?;
        if actual != expected {
            return Ok(AuthorityReceipt {
                value: crate::storage::FenceOutcome::Conflict { actual },
                work: read_work,
            });
        }
        let write_work = read_work
            .checked_add(WorkCounters {
                authority_records_appended: 1,
                backend_write_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        admit_authority(write_work, budget)?;
        let next =
            actual.epoch.get().checked_add(1).ok_or_else(|| {
                AuthorityFailure::new(AuthorityStoreError::EpochExhausted, read_work)
            })?;
        state.epoch = Some(Epoch::new(next).map_err(|error| {
            AuthorityFailure::new(AuthorityStoreError::Rejected(error.to_string()), read_work)
        })?);
        let value = state
            .head()
            .map_err(|error| AuthorityFailure::new(error, write_work))?;
        Ok(AuthorityReceipt {
            value: crate::storage::FenceOutcome::Advanced(value),
            work: write_work,
        })
    }

    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let base = authority_read_work();
        admit_authority(base, budget)?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityFailure::new(poisoned_authority(), base))?;
        let state = authorities
            .get(&authority_id)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, base))?;
        let value = state
            .operations
            .get(&operation_id)
            .and_then(|index| state.commits.get(*index))
            .cloned();
        let work = match &value {
            Some(commit) => base
                .checked_add(WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: u64::try_from(commit.payload.len()).unwrap_or(u64::MAX),
                    ..WorkCounters::default()
                })
                .map_err(|error| AuthorityFailure::new(error.into(), base))?,
            None => base,
        };
        admit_authority(work, budget)?;
        Ok(AuthorityReceipt { value, work })
    }
}

impl crate::async_storage::ImmediateAuthorityStore for MemoryAuthorityStore {}

fn poisoned_authority() -> AuthorityStoreError {
    AuthorityStoreError::Rejected("authority mutex poisoned".to_owned())
}

fn authority_read_work() -> WorkCounters {
    WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::default()
    }
}

fn authority_write_work() -> WorkCounters {
    WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::default()
    }
}

fn admit_authority(work: WorkCounters, budget: WorkBudget) -> Result<(), AuthorityFailure> {
    work.verify(budget)
        .map_err(|error| AuthorityFailure::before_work(error.into()))
}

fn bounded_page_bounds(
    commits: &[DurableCommit],
    after: Sequence,
    limit: ReplayLimit,
) -> Result<(usize, usize, u64), AuthorityStoreError> {
    let start = usize::try_from(after.get()).map_err(|_| {
        AuthorityStoreError::Rejected("replay cursor does not fit process".to_owned())
    })?;
    let maximum_records = usize::try_from(limit.records).unwrap_or(usize::MAX);
    let mut bytes = 0_u64;
    let mut record_count = 0_usize;
    for commit in commits.iter().skip(start) {
        if record_count >= maximum_records {
            break;
        }
        let payload_bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
        let next = bytes.checked_add(payload_bytes).ok_or_else(|| {
            AuthorityStoreError::Rejected("replay byte accounting overflowed".to_owned())
        })?;
        if next > limit.payload_bytes {
            if record_count == 0 {
                return Err(AuthorityStoreError::ReplayRecordTooLarge {
                    observed: payload_bytes,
                    maximum: limit.payload_bytes,
                });
            }
            break;
        }
        bytes = next;
        record_count = record_count.saturating_add(1);
    }
    let end = start.checked_add(record_count).ok_or_else(|| {
        AuthorityStoreError::Rejected("replay record accounting overflowed".to_owned())
    })?;
    Ok((start, end, bytes))
}

/// Deterministic immutable-object store.
pub struct MemoryObjectStore {
    maximum_object_bytes: u64,
    objects: RwLock<HashMap<ObjectId, Bytes>>,
}

impl MemoryObjectStore {
    /// Creates a bounded memory object store.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError::TooLarge`] when the configured maximum is zero.
    pub fn new(maximum_object_bytes: u64) -> Result<Self, ObjectStoreError> {
        if maximum_object_bytes == 0 {
            return Err(ObjectStoreError::TooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            maximum_object_bytes,
            objects: RwLock::new(HashMap::new()),
        })
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self {
            maximum_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            objects: RwLock::new(HashMap::new()),
        }
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put(&self, object_id: ObjectId, bytes: Bytes, budget: WorkBudget) -> ObjectResult<()> {
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > self.maximum_object_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                observed: length,
                maximum: self.maximum_object_bytes,
            }));
        }
        let work = WorkCounters {
            object_probes: 1,
            backend_write_operations: 1,
            object_bytes_written: length,
            bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure {
                error: ObjectStoreError::DigestMismatch,
                work: Box::new(WorkCounters {
                    object_probes: 1,
                    backend_write_operations: 1,
                    bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                    ..WorkCounters::default()
                }),
            });
        }
        let mut objects = self.objects.write().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(work),
        })?;
        if let Some(existing) = objects.get(&object_id) {
            if existing != &bytes {
                return Err(ObjectFailure {
                    error: ObjectStoreError::Corrupt,
                    work: Box::new(work),
                });
            }
            return Ok(ObjectReceipt { value: (), work });
        }
        objects.insert(object_id, bytes);
        Ok(ObjectReceipt { value: (), work })
    }

    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        let probe = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        probe
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let objects = self.objects.read().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(probe),
        })?;
        let bytes = objects.get(&object_id).ok_or(ObjectFailure {
            error: ObjectStoreError::Missing,
            work: Box::new(probe),
        })?;
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > maximum_bytes {
            return Err(ObjectFailure {
                error: ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum_bytes,
                },
                work: Box::new(probe),
            });
        }
        let work = probe
            .checked_add(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(probe),
            })?;
        work.verify(budget).map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(probe),
        })?;
        if object_digest(object_id.kind, bytes) != object_id.digest {
            return Err(ObjectFailure {
                error: ObjectStoreError::Corrupt,
                work: Box::new(work),
            });
        }
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes: bytes.clone(),
                retention: ObjectReadRetention::Shared,
            },
            work,
        })
    }

    fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>> {
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let item_count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
        let vector_bytes =
            item_count.saturating_mul(u64::try_from(size_of::<ObjectRead>()).unwrap_or(u64::MAX));
        let mut work = WorkCounters {
            object_probes: item_count,
            backend_read_operations: 1,
            items_examined: item_count,
            allocation_operations: 1,
            peak_allocation_bytes: vector_bytes,
            ..WorkCounters::default()
        };
        let mut admission = work;
        admission.items_returned = item_count;
        admission
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let objects = self.objects.read().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(work),
        })?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::new(
                ObjectStoreError::Rejected("object batch result allocation failed".to_owned()),
                work,
            )
        })?;
        for request in requests {
            let bytes = objects.get(&request.object_id).ok_or(ObjectFailure {
                error: ObjectStoreError::Missing,
                work: Box::new(work),
            })?;
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if length > request.maximum_bytes {
                return Err(ObjectFailure {
                    error: ObjectStoreError::TooLarge {
                        observed: length,
                        maximum: request.maximum_bytes,
                    },
                    work: Box::new(work),
                });
            }
            let next = work
                .checked_add(WorkCounters {
                    object_bytes_read: length,
                    bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                    ..WorkCounters::default()
                })
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            next.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            work = next;
            if object_digest(request.object_id.kind, bytes) != request.object_id.digest {
                return Err(ObjectFailure {
                    error: ObjectStoreError::Corrupt,
                    work: Box::new(work),
                });
            }
            values.push(ObjectRead {
                bytes: bytes.clone(),
                retention: ObjectReadRetention::Shared,
            });
        }
        work.items_returned = item_count;
        Ok(ObjectReceipt {
            value: values,
            work,
        })
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        let probe = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        probe
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let objects = self.objects.read().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(probe),
        })?;
        let Some(bytes) = objects.get(&object_id) else {
            return Ok(ObjectReceipt {
                value: false,
                work: probe,
            });
        };
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let work = probe
            .checked_add(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(probe),
            })?;
        work.verify(budget).map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(probe),
        })?;
        if object_digest(object_id.kind, bytes) != object_id.digest {
            return Err(ObjectFailure {
                error: ObjectStoreError::Corrupt,
                work: Box::new(work),
            });
        }
        Ok(ObjectReceipt { value: true, work })
    }
}

impl crate::async_storage::ImmediateObjectStore for MemoryObjectStore {}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;
