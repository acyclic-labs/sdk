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
    ObjectStoreError, ObjectWrite, ReplayLimit, object_digest,
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

    fn head_accounted(
        &self, authority_id: AuthorityId, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Head, AuthorityMemoryFailure> {
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?
            .head().map_err(|error| accounting.failure(error))
    }

    fn replay_accounted(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Vec<DurableCommit>, AuthorityMemoryFailure> {
        accounting.check()?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(accounting.failure(AuthorityStoreError::InvalidReplayLimit));
        }
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        let state = authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let head = state.head().map_err(|error| accounting.failure(error))?;
        if after > head.sequence {
            return Err(accounting.failure(AuthorityStoreError::ReplayCursorAhead {
                after, head: head.sequence,
            }));
        }
        let (start, end, payload_bytes) =
            bounded_page_bounds(&state.commits, after, limit, accounting)?;
        accounting.charge(WorkCounters {
            authority_records_read: u64::try_from(end - start).unwrap_or(u64::MAX),
            authority_bytes_read: payload_bytes,
            ..WorkCounters::default()
        })?;
        accounting.clone_page(&state.commits[start..end])
    }

    fn fence_accounted(
        &self, authority_id: AuthorityId, expected: Head,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<crate::storage::FenceOutcome, AuthorityMemoryFailure> {
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        let state = authorities.get_mut(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head().map_err(|error| accounting.failure(error))?;
        if actual != expected {
            return Ok(crate::storage::FenceOutcome::Conflict { actual });
        }
        let delta = WorkCounters {
            authority_records_appended: 1,
            backend_write_operations: 1,
            ..WorkCounters::default()
        };
        accounting.admit(delta)?;
        let next = actual.epoch.get().checked_add(1)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::EpochExhausted))?;
        let epoch = Epoch::new(next)
            .map_err(|_| accounting.failure(AuthorityStoreError::EpochExhausted))?;
        accounting.check()?;
        accounting.record(delta)?;
        state.epoch = Some(epoch);
        Ok(crate::storage::FenceOutcome::Advanced(
            state.head().map_err(|error| accounting.failure(error))?,
        ))
    }

    fn find_operation_accounted(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityMemoryFailure> {
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        let state = authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        accounting.examine()?;
        let value = state.operations.get(&operation_id)
            .and_then(|index| state.commits.get(*index));
        if let Some(commit) = value {
            accounting.charge(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: u64::try_from(commit.payload.len()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            })?;
        }
        accounting.check()?;
        Ok(value.cloned())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn with_native_authority<T>(
        &self, work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        operation: impl FnOnce(&mut AuthorityAccounting<'_>) -> Result<T, AuthorityMemoryFailure>,
    ) -> Result<(T, crate::materializer::NativeAllocation), AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        work.admit(WorkCounters::default())?;
        work.with_owned_allocations(|allocations, counters, budget, original_cancellation| {
            let mut accounting = AuthorityAccounting {
                work: WorkCounters::default(),
                budget,
                cancellation: Some(cancellation),
                native: Some(NativeAuthorityAccounting {
                    allocations, counters, original_cancellation,
                }),
            };
            operation(&mut accounting).map_err(|failure| failure.error)
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
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let value = self.head_accounted(authority_id, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
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
        let existing = state.resolve_operation(&commit, read_work, budget)?;
        if let Some(outcome) = existing.value {
            return Ok(AuthorityReceipt {
                value: outcome,
                work: existing.work,
            });
        }
        if epoch != actual.epoch {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                },
                work: read_work,
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
            settled_at_micros: None,
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
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit, budget: WorkBudget,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let value = self.replay_accounted(authority_id, after, limit, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
    }

    fn fence(
        &self, authority_id: AuthorityId, expected: Head, budget: WorkBudget,
    ) -> AuthorityResult<crate::storage::FenceOutcome> {
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let value = self.fence_accounted(authority_id, expected, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
    }

    fn find_operation(
        &self, authority_id: AuthorityId, operation_id: OperationId, budget: WorkBudget,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let value = self.find_operation_accounted(authority_id, operation_id, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
    }
}

impl crate::async_storage::ImmediateAuthorityStore for MemoryAuthorityStore {
    #[cfg(not(target_arch = "wasm32"))]
    fn head_with_native_work(
        &self, authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Head, AuthorityStoreError> {
        self.with_native_authority(work, cancellation, |accounting| {
            self.head_accounted(authority_id, accounting)
        }).map(|(head, _)| head)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        self.with_native_authority(work, cancellation, |accounting| {
            self.replay_accounted(authority_id, after, limit, accounting)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(
        &self, authority_id: AuthorityId, expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::storage::FenceOutcome, AuthorityStoreError> {
        self.with_native_authority(work, cancellation, |accounting| {
            self.fence_accounted(authority_id, expected, accounting)
        }).map(|(outcome, _)| outcome)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn find_operation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Option<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        self.with_native_authority(work, cancellation, |accounting| {
            self.find_operation_accounted(authority_id, operation_id, accounting)
        })
    }
}

/// Kept inline until an ordinary receipt is requested. Original native failures
/// must not allocate the ordinary receipt's counters box.
struct AuthorityMemoryFailure {
    error: AuthorityStoreError,
    work: WorkCounters,
}

impl AuthorityMemoryFailure {
    fn new(error: AuthorityStoreError, work: WorkCounters) -> Self { Self { error, work } }
    fn before_work(error: AuthorityStoreError) -> Self { Self::new(error, WorkCounters::default()) }
}

impl From<AuthorityMemoryFailure> for AuthorityFailure {
    fn from(failure: AuthorityMemoryFailure) -> Self {
        Self::new(failure.error, failure.work)
    }
}

/// One canonical memory producer, projected either into ordinary receipts or
/// directly into the original native ledger borrowed for this synchronous leaf.
struct AuthorityAccounting<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&'a crate::CancellationToken>,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<NativeAuthorityAccounting<'a>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeAuthorityAccounting<'a> {
    allocations: &'a mut crate::kernel::allocation::AllocationLedger,
    counters: &'a mut WorkCounters,
    original_cancellation: &'a crate::CancellationToken,
}

impl AuthorityAccounting<'_> {
    fn ordinary(budget: WorkBudget) -> Self {
        Self {
            work: WorkCounters::default(), budget, cancellation: None,
            #[cfg(not(target_arch = "wasm32"))]
            native: None,
        }
    }

    fn failure(&self, error: AuthorityStoreError) -> AuthorityMemoryFailure {
        AuthorityMemoryFailure::new(error, self.work)
    }

    fn work_failure(&self, error: crate::WorkError) -> AuthorityMemoryFailure {
        #[cfg(not(target_arch = "wasm32"))]
        if self.native.is_some() {
            return self.failure(AuthorityStoreError::NativeOriginal(
                crate::materializer::NativeTreeMaterializationError::Work(error),
            ));
        }
        AuthorityMemoryFailure::before_work(error.into())
    }

    fn check(&self) -> Result<(), AuthorityMemoryFailure> {
        if let Some(cancellation) = self.cancellation {
            cancellation.check().map_err(|_| self.failure(AuthorityStoreError::Cancelled))?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            native.original_cancellation.check()
                .map_err(|_| self.failure(AuthorityStoreError::Cancelled))?;
            native.counters.verify(self.budget).map_err(|error| self.work_failure(error))?;
        }
        Ok(())
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), AuthorityMemoryFailure> {
        self.check()?;
        let base = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = &self.native { *native.counters } else { self.work }
            #[cfg(target_arch = "wasm32")]
            { self.work }
        };
        let next = base.checked_add(delta).map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if self.native.is_some() { return self.work_failure(error); }
            self.failure(error.into())
        })?;
        next.verify(self.budget).map_err(|error| self.work_failure(error))
    }

    fn record(&mut self, delta: WorkCounters) -> Result<(), AuthorityMemoryFailure> {
        let next = self.work.checked_add(delta).map_err(|error| self.work_failure(error))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &mut self.native {
            let root_next = native.counters.checked_add(delta)
                .map_err(|error| AuthorityMemoryFailure::new(
                    AuthorityStoreError::NativeOriginal(
                        crate::materializer::NativeTreeMaterializationError::Work(error),
                    ), self.work,
                ))?;
            *native.counters = root_next;
        }
        self.work = next;
        Ok(())
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), AuthorityMemoryFailure> {
        self.admit(delta)?;
        self.record(delta)
    }

    fn examine(&mut self) -> Result<(), AuthorityMemoryFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.native.is_some() {
            return self.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() });
        }
        self.check()
    }

    fn clone_page(&mut self, commits: &[DurableCommit]) -> Result<Vec<DurableCommit>, AuthorityMemoryFailure> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        let admitted = if let Some(native) = &mut self.native {
            native.allocations.claim_elements::<DurableCommit>(
                commits.len(), native.counters, self.budget,
            ).map_err(|error| AuthorityMemoryFailure::new(
                AuthorityStoreError::NativeOriginal(error.into_work_or(
                    crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
                )),
                self.work,
            ))?
        } else { 0 };
        let mut page = Vec::new();
        page.try_reserve_exact(commits.len())
            .map_err(|error| self.failure(AuthorityStoreError::MemoryReplayAllocationFailed(error)))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &mut self.native {
            let actual = page.capacity().checked_mul(size_of::<DurableCommit>())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| AuthorityMemoryFailure::new(
                    AuthorityStoreError::NativeOriginal(
                        crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
                    ), self.work,
                ))?;
            if actual > admitted {
                native.allocations.record_owned_bytes(actual - admitted, native.counters)
                    .map_err(|error| AuthorityMemoryFailure::new(
                        AuthorityStoreError::NativeOriginal(error.into_work_or(
                            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
                        )), self.work,
                    ))?;
                native.counters.verify(self.budget).map_err(|error| AuthorityMemoryFailure::new(
                    AuthorityStoreError::NativeOriginal(
                        crate::materializer::NativeTreeMaterializationError::Work(error),
                    ), self.work,
                ))?;
            }
        }
        for commit in commits {
            self.examine()?;
            // Only inline record metadata is copied; Bytes shares its existing owner.
            #[cfg(not(target_arch = "wasm32"))]
            if self.native.is_some() {
                self.charge(WorkCounters {
                    bytes_copied: size_of::<DurableCommit>() as u64,
                    ..WorkCounters::default()
                })?;
            }
            page.push(commit.clone());
        }
        Ok(page)
    }
}

fn poisoned_authority() -> AuthorityStoreError {
    AuthorityStoreError::MemoryAuthorityPoisoned
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
    commits: &[DurableCommit], after: Sequence, limit: ReplayLimit,
    accounting: &mut AuthorityAccounting<'_>,
) -> Result<(usize, usize, u64), AuthorityMemoryFailure> {
    let start = usize::try_from(after.get()).map_err(|_| accounting.failure(
        AuthorityStoreError::ReplayCursorProcessOverflow { after },
    ))?;
    let maximum_records = usize::try_from(limit.records).unwrap_or(usize::MAX);
    let mut bytes = 0_u64;
    let mut record_count = 0_usize;
    for commit in commits.iter().skip(start).take(maximum_records) {
        accounting.examine()?;
        let payload_bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
        let next = bytes.checked_add(payload_bytes).ok_or_else(|| accounting.failure(
            AuthorityStoreError::Work(crate::WorkError::Overflow),
        ))?;
        if next > limit.payload_bytes {
            if record_count == 0 {
                return Err(accounting.failure(AuthorityStoreError::ReplayRecordTooLarge {
                    observed: payload_bytes, maximum: limit.payload_bytes,
                }));
            }
            break;
        }
        bytes = next;
        record_count = record_count.saturating_add(1);
    }
    let end = start.checked_add(record_count).ok_or_else(|| accounting.failure(
        AuthorityStoreError::Work(crate::WorkError::Overflow),
    ))?;
    Ok((start, end, bytes))
}

/// Deterministic immutable-object store.
pub struct MemoryObjectStore {
    maximum_object_bytes: u64,
    objects: RwLock<MemoryObjectState>,
}

// The backing vector is destroyed before its original capacity guard.
struct MemoryObjectState {
    objects: Vec<(ObjectId, Bytes)>,
    #[cfg(not(target_arch = "wasm32"))]
    table_custody: Option<crate::materializer::NativeAllocation>,
}

impl MemoryObjectState {
    fn new() -> Self {
        Self {
            objects: Vec::new(),
            #[cfg(not(target_arch = "wasm32"))]
            table_custody: None,
        }
    }

    fn get(&self, id: &ObjectId) -> Option<&Bytes> {
        self.objects.binary_search_by_key(id, |(id, _)| *id)
            .ok().map(|index| &self.objects[index].1)
    }
}

enum MemoryObjectAccounting<'a> {
    Portable {
        work: WorkCounters, budget: WorkBudget,
        _lifetime: std::marker::PhantomData<&'a ()>,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        work: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

impl MemoryObjectAccounting<'_> {
    fn portable(budget: WorkBudget) -> Self {
        Self::Portable {
            work: WorkCounters::default(), budget, _lifetime: std::marker::PhantomData,
        }
    }

    fn check(&self) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { work, cancellation } = self {
            cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
            work.admit(WorkCounters::default())?;
        }
        Ok(())
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), ObjectStoreError> {
        self.check()?;
        match self {
            Self::Portable { work, budget, .. } => work.checked_add(delta)?.verify(*budget).map_err(Into::into),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.admit(delta).map_err(Into::into),
        }
    }

    fn record(&mut self, delta: WorkCounters) -> Result<(), ObjectStoreError> {
        match self {
            Self::Portable { work, .. } => { *work = work.checked_add(delta)?; Ok(()) }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.record_admitted(delta).map_err(Into::into),
        }
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), ObjectStoreError> {
        self.admit(delta)?;
        self.record(delta)
    }

    fn portable_work(&self) -> WorkCounters {
        match self {
            Self::Portable { work, .. } => *work,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => WorkCounters::default(),
        }
    }
}

struct MemoryObjectReads {
    reads: Vec<ObjectRead>,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<crate::materializer::NativeAllocation>,
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
            objects: RwLock::new(MemoryObjectState::new()),
        })
    }

    fn put_admitted(
        &self, writes: &[ObjectWrite], hashed: bool,
        accounting: &mut MemoryObjectAccounting<'_>,
    ) -> Result<(), ObjectStoreError> {
        if writes.is_empty() { return Err(ObjectStoreError::EmptyWriteBatch); }
        accounting.check()?;
        for write in writes {
            let length = u64::try_from(write.bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
            if length > self.maximum_object_bytes {
                return Err(ObjectStoreError::TooLarge { observed: length, maximum: self.maximum_object_bytes });
            }
            if !hashed {
                let hash = WorkCounters {
                    bytes_hashed: length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                        .ok_or(crate::WorkError::Overflow)?,
                    ..WorkCounters::default()
                };
                accounting.admit(hash)?;
                let digest = object_digest(write.object_id.kind, &write.bytes);
                accounting.record(hash)?;
                if digest != write.object_id.digest { return Err(ObjectStoreError::DigestMismatch); }
            }
        }
        let mut state = self.objects.write().map_err(|_| ObjectStoreError::Corrupt)?;
        let mut missing = 0_usize;
        let mut written = 0_u64;
        for (index, write) in writes.iter().enumerate() {
            accounting.charge(WorkCounters { object_probes: 1, ..WorkCounters::default() })?;
            if let Some(existing) = state.get(&write.object_id) {
                if existing != &write.bytes { return Err(ObjectStoreError::Corrupt); }
                continue;
            }
            let mut duplicate = false;
            for earlier in &writes[..index] {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if earlier.object_id == write.object_id {
                    if earlier.bytes != write.bytes { return Err(ObjectStoreError::Corrupt); }
                    duplicate = true;
                    break;
                }
            }
            if !duplicate {
                missing = missing.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                written = written.checked_add(u64::try_from(write.bytes.len()).map_err(|_| crate::WorkError::Overflow)?)
                    .ok_or(crate::WorkError::Overflow)?;
            }
        }
        let needed = state.objects.len().checked_add(missing).ok_or(crate::WorkError::Overflow)?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut replacement_custody = None;
        let mut replacement = None;
        if needed > state.objects.capacity() {
            let capacity = needed.max(state.objects.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?);
            let requested = capacity.checked_mul(size_of::<(ObjectId, Bytes)>())
                .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
            let allocation = WorkCounters {
                allocation_operations: 1, peak_allocation_bytes: requested,
                ..WorkCounters::default()
            };
            accounting.admit(allocation)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let MemoryObjectAccounting::Native { work, .. } = accounting {
                replacement_custody = Some(work.claim(requested)?);
            }
            if matches!(accounting, MemoryObjectAccounting::Portable { .. }) {
                accounting.record(allocation)?;
            }
            // Declare after the guard so allocator failure frees the backing first.
            let mut table = Vec::new();
            table.try_reserve_exact(capacity).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
            let actual = table.capacity().checked_mul(size_of::<(ObjectId, Bytes)>())
                .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(custody) = &mut replacement_custody { custody.resize_retained(actual)?; }
            if actual > requested && matches!(accounting, MemoryObjectAccounting::Portable { .. }) {
                accounting.charge(WorkCounters { peak_allocation_bytes: actual, ..WorkCounters::default() })?;
            }
            replacement = Some(table);
        }
        let write = WorkCounters {
            backend_write_operations: 1, object_bytes_written: written,
            ..WorkCounters::default()
        };
        let copy = WorkCounters {
            bytes_copied: if replacement.is_some() {
                state.objects.len().checked_mul(size_of::<(ObjectId, Bytes)>())
                    .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?
            } else { 0 },
            ..WorkCounters::default()
        };
        accounting.admit(write.checked_add(copy)?)?;
        accounting.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
        if let Some(mut table) = replacement {
            table.append(&mut state.objects);
            let old = std::mem::replace(&mut state.objects, table);
            drop(old);
            #[cfg(not(target_arch = "wasm32"))]
            { state.table_custody = replacement_custody; }
        }
        accounting.record(copy)?;
        for entry in writes {
            if let Err(index) = state.objects.binary_search_by_key(&entry.object_id, |(id, _)| *id) {
                let shift = state.objects.len().checked_sub(index)
                    .and_then(|count| count.checked_mul(size_of::<(ObjectId, Bytes)>()))
                    .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
                let copied = WorkCounters { bytes_copied: shift, ..WorkCounters::default() };
                let written = WorkCounters {
                    object_bytes_written: u64::try_from(entry.bytes.len()).map_err(|_| crate::WorkError::Overflow)?,
                    ..WorkCounters::default()
                };
                accounting.admit(copied.checked_add(written)?)?;
                state.objects.insert(index, (entry.object_id, entry.bytes.clone()));
                accounting.record(copied.checked_add(written)?)?;
            }
        }
        accounting.check()
    }

    fn read_admitted(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        mut charge: impl FnMut(WorkCounters) -> Result<(), ObjectStoreError>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        })?;
        let objects = self.objects.read().map_err(|_| ObjectStoreError::Corrupt)?;
        let bytes = objects.get(&object_id).ok_or(ObjectStoreError::Missing)?;
        let length = u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
        if length > maximum_bytes {
            return Err(ObjectStoreError::TooLarge { observed: length, maximum: maximum_bytes });
        }
        charge(WorkCounters {
            object_bytes_read: length,
            bytes_hashed: length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::default()
        })?;
        if object_digest(object_id.kind, bytes) != object_id.digest {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(ObjectRead { bytes: bytes.clone(), retention: ObjectReadRetention::Shared })
    }

    fn read_many_admitted(
        &self, requests: &[crate::storage::ObjectReadRequest],
        accounting: &mut MemoryObjectAccounting<'_>,
    ) -> Result<MemoryObjectReads, ObjectStoreError> {
        if requests.is_empty() { return Err(ObjectStoreError::EmptyReadBatch); }
        accounting.check()?;
        let requested = requests.len().checked_mul(size_of::<ObjectRead>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        let allocation = WorkCounters {
            allocation_operations: 1, peak_allocation_bytes: requested,
            ..WorkCounters::default()
        };
        accounting.admit(allocation)?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = if let MemoryObjectAccounting::Native { work, .. } = accounting {
            Some(work.claim(requested)?)
        } else { None };
        if matches!(accounting, MemoryObjectAccounting::Portable { .. }) {
            accounting.record(allocation)?;
        }
        let mut reads = Vec::new();
        reads.try_reserve_exact(requests.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual = reads.capacity().checked_mul(size_of::<ObjectRead>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut custody { custody.resize_retained(actual)?; }
        if actual > requested && matches!(accounting, MemoryObjectAccounting::Portable { .. }) {
            accounting.charge(WorkCounters { peak_allocation_bytes: actual, ..WorkCounters::default() })?;
        }
        accounting.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        let state = self.objects.read().map_err(|_| ObjectStoreError::Corrupt)?;
        for request in requests {
            accounting.charge(WorkCounters {
                object_probes: 1, items_examined: 1, ..WorkCounters::default()
            })?;
            let bytes = state.get(&request.object_id).ok_or(ObjectStoreError::Missing)?;
            let length = u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
            if length > request.maximum_bytes {
                return Err(ObjectStoreError::TooLarge { observed: length, maximum: request.maximum_bytes });
            }
            let read = WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                    .ok_or(crate::WorkError::Overflow)?,
                ..WorkCounters::default()
            };
            accounting.admit(read)?;
            let digest = object_digest(request.object_id.kind, bytes);
            accounting.record(read)?;
            if digest != request.object_id.digest { return Err(ObjectStoreError::Corrupt); }
            reads.push(ObjectRead { bytes: bytes.clone(), retention: ObjectReadRetention::Shared });
        }
        accounting.charge(WorkCounters {
            items_returned: u64::try_from(reads.len()).map_err(|_| crate::WorkError::Overflow)?,
            ..WorkCounters::default()
        })?;
        accounting.check()?;
        Ok(MemoryObjectReads {
            reads,
            #[cfg(not(target_arch = "wasm32"))]
            custody,
        })
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self {
            maximum_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            objects: RwLock::new(MemoryObjectState::new()),
        }
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put(&self, object_id: ObjectId, bytes: Bytes, budget: WorkBudget) -> ObjectResult<()> {
        let mut accounting = MemoryObjectAccounting::portable(budget);
        let result = self.put_admitted(&[ObjectWrite { object_id, bytes }], false, &mut accounting);
        result.map(|value| ObjectReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }

    fn put_many(&self, writes: &[ObjectWrite], budget: WorkBudget) -> ObjectResult<()> {
        let mut accounting = MemoryObjectAccounting::portable(budget);
        let result = self.put_admitted(writes, false, &mut accounting);
        result.map(|value| ObjectReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }

    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        let mut work = WorkCounters::default();
        let result = self.read_admitted(object_id, maximum_bytes, |delta| {
            work.charge(&delta, &budget).map_err(ObjectStoreError::from)
        });
        result.map(|value| ObjectReceipt { value, work })
            .map_err(|error| ObjectFailure::new(error, work))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        let read = self.read_admitted(object_id, maximum_bytes, |delta| {
            cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
            work.charge(delta).map_err(ObjectStoreError::from)
        })?;
        crate::async_storage::NativeObjectRead::shared(read)
    }

    fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut accounting = MemoryObjectAccounting::portable(budget);
        let result = self.read_many_admitted(requests, &mut accounting);
        result.map(|value| ObjectReceipt { value: value.reads, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(
        &self, requests: &[crate::storage::ObjectReadRequest],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectReadBatch, ObjectStoreError> {
        let result = self.read_many_admitted(
            requests, &mut MemoryObjectAccounting::Native { work, cancellation },
        )?;
        let MemoryObjectReads { reads, custody } = result;
        let Some(custody) = custody else {
            drop(reads);
            return Err(ObjectStoreError::Corrupt);
        };
        crate::async_storage::NativeObjectReadBatch::new(reads, custody)
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

impl crate::async_storage::ImmediateObjectStore for MemoryObjectStore {
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self, object_id: ObjectId, bytes: Bytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_admitted(&[ObjectWrite { object_id, bytes }], false,
            &mut MemoryObjectAccounting::Native { work, cancellation })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        self.put_admitted(&[ObjectWrite { object_id, bytes }], true,
            &mut MemoryObjectAccounting::Native { work, cancellation })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_admitted(writes, false, &mut MemoryObjectAccounting::Native { work, cancellation })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self, _scope: crate::async_storage::PublicationScope<'_>,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        let accounting = MemoryObjectAccounting::Native { work, cancellation };
        accounting.check()?;
        Ok(crate::PublicationHold::none())
    }

    /// Like the ordinary PR237 preparation default, this in-memory provider
    /// supplies no hosted or historical custody and creates no archive receipt.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self, _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof, _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let accounting = MemoryObjectAccounting::Native { work: original, cancellation };
        accounting.check()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;
