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
    operations: HashMap<OperationId, MemoryOperationIndex>,
    // Physical history is destroyed before its guard. HashMap bucket layout is
    // excluded; each compiled typed entry retains only its logical envelope.
    #[cfg(not(target_arch = "wasm32"))]
    commits_custody: Option<crate::materializer::NativeAllocation>,
    _entry_envelope: MemoryEntryEnvelope,
}

impl AuthorityState {
    fn head(&self) -> Result<Head, AuthorityStoreError> {
        let epoch = self.epoch.ok_or(AuthorityStoreError::Missing)?;
        Ok(self.commits.last().map_or_else(
            || Head::genesis(epoch),
            |commit| Head { epoch, sequence: commit.sequence, digest: commit.digest },
        ))
    }

    fn operation(&self, operation_id: OperationId) -> Option<usize> {
        self.operations.get(&operation_id).map(|operation| operation.index)
    }

    fn resolve_operation(
        &self, commit: &ProposedCommit, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<AppendOutcome>, AuthorityMemoryFailure> {
        accounting.examine()?;
        let Some(index) = self.operation(commit.operation_id) else { return Ok(None); };
        let durable = self.commits.get(index).ok_or_else(|| accounting.failure(
            accounting.static_corrupt("operation index outside commit history"),
        ))?;
        accounting.charge(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: u64::try_from(durable.payload.len()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })?;
        Ok(Some(if durable.fingerprint == commit.fingerprint {
            AppendOutcome::AlreadyCommitted(durable.clone())
        } else {
            AppendOutcome::IdempotencyConflict { committed_fingerprint: durable.fingerprint }
        }))
    }
}

#[derive(Default)]
struct MemoryEntryEnvelope {
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<crate::materializer::NativeAllocation>,
}

struct MemoryOperationIndex {
    index: usize,
    _envelope: MemoryEntryEnvelope,
}

struct AuthorityBuffer<T> {
    values: Vec<T>,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<crate::materializer::NativeAllocation>,
}

enum MemoryAppendPlan {
    Resolved(AppendOutcome),
    Publish {
        durable: DurableCommit,
        commits: Option<AuthorityBuffer<DurableCommit>>,
        operation: MemoryEntryEnvelope,
    },
}

enum MemoryFirstRecordPlan {
    Resolved(bool),
    Publish { expected: Head, state: AuthorityState },
}

fn append_write_work(payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: 1, authority_bytes_written: payload_bytes,
        backend_write_operations: 1, ..WorkCounters::default()
    }
}

fn metadata_copy_work<T>(
    count: usize, accounting: &AuthorityAccounting<'_>,
) -> Result<WorkCounters, AuthorityMemoryFailure> {
    let bytes = count.checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?;
    Ok(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })
}


fn install_authority_commits(state: &mut AuthorityState, mut buffer: AuthorityBuffer<DurableCommit>) {
    buffer.values.append(&mut state.commits);
    drop(std::mem::replace(&mut state.commits, buffer.values));
    #[cfg(not(target_arch = "wasm32"))]
    { state.commits_custody = buffer.custody; }
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

    fn validate_payload(
        &self, commit: &ProposedCommit, accounting: &AuthorityAccounting<'_>,
    ) -> Result<u64, AuthorityMemoryFailure> {
        accounting.check()?;
        let bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
        if bytes > self.maximum_payload_bytes {
            return Err(accounting.failure(AuthorityStoreError::PayloadTooLarge {
                observed: bytes, maximum: self.maximum_payload_bytes,
            }));
        }
        Ok(bytes)
    }

    fn validate_permit(
        permit: crate::storage::PublicationPermit, accounting: &AuthorityAccounting<'_>,
    ) -> Result<(), AuthorityMemoryFailure> {
        if permit != crate::storage::PublicationPermit::Unrestricted {
            return Err(accounting.failure(accounting.static_rejected(
                "authority backend does not support atomic publication permits",
            )));
        }
        Ok(())
    }

    fn durable_accounted(
        &self, authority: AuthorityId, actual: Head, commit: ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<DurableCommit, AuthorityMemoryFailure> {
        let sequence = actual.sequence.checked_next().map_err(|_| accounting.failure(
            accounting.static_rejected("authority sequence cannot advance"),
        ))?;
        let hashed = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX)
            .checked_add(AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES)
            .ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?;
        accounting.charge(WorkCounters { bytes_hashed: hashed, ..WorkCounters::default() })?;
        let digest = authority_commit_digest(
            authority, actual.epoch, sequence, commit.operation_id, commit.fingerprint,
            actual.digest, &commit.payload,
        );
        Ok(DurableCommit {
            epoch: actual.epoch, sequence, operation_id: commit.operation_id,
            fingerprint: commit.fingerprint, previous_digest: actual.digest,
            digest, payload: commit.payload,
        })
    }

    fn create_accounted(
        &self, authority: AuthorityId, epoch: Epoch, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<CreateAuthorityOutcome, AuthorityMemoryFailure> {
        accounting.check()?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        if let Some(state) = authorities.get(&authority) {
            accounting.charge(authority_read_work())?;
            return state.head().map(CreateAuthorityOutcome::Existing)
                .map_err(|error| accounting.failure(error));
        }
        accounting.admit(authority_write_work())?;
        let envelope = accounting.logical_entry::<(AuthorityId, AuthorityState)>()?;
        accounting.reserve_map_entry(&mut authorities)?;
        accounting.charge(authority_write_work())?;
        authorities.insert(authority, AuthorityState {
            epoch: Some(epoch), _entry_envelope: envelope, ..AuthorityState::default()
        });
        Ok(CreateAuthorityOutcome::Created(Head::genesis(epoch)))
    }

    fn first_record_matches(
        first: &DurableCommit, commit: &ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<bool, AuthorityMemoryFailure> {
        accounting.charge(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: u64::try_from(first.payload.len()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })?;
        Ok(first.sequence == Sequence::new(1)
            && first.operation_id == commit.operation_id
            && first.fingerprint == commit.fingerprint
            && first.payload == commit.payload)
    }

    fn plan_first_record(
        &self, authority: AuthorityId, commit: &ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<MemoryFirstRecordPlan, AuthorityMemoryFailure> {
        let payload_bytes = self.validate_payload(commit, accounting)?;
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        if let Some(state) = authorities.get(&authority) {
            if let Some(first) = state.commits.first() {
                return Self::first_record_matches(first, commit, accounting)
                    .map(MemoryFirstRecordPlan::Resolved);
            }
            state.head().map_err(|error| accounting.failure(error))?;
            if !state.operations.is_empty() {
                return Err(accounting.failure(accounting.static_corrupt("operation index outside commit history")));
            }
        }
        let expected = match authorities.get(&authority) {
            Some(state) => state.head().map_err(|error| accounting.failure(error))?,
            None => Head::genesis(Epoch::GENESIS),
        };
        accounting.admit(append_write_work(payload_bytes))?;
        let new_authority = !authorities.contains_key(&authority);
        let envelope = if new_authority {
            accounting.logical_entry::<(AuthorityId, AuthorityState)>()?
        } else { MemoryEntryEnvelope::default() };
        let operation = accounting.logical_entry::<(OperationId, MemoryOperationIndex)>()?;
        let mut commits = accounting.allocate_vector::<DurableCommit>(1)?;
        let mut operations = HashMap::new();
        accounting.reserve_map_entry(&mut operations)?;
        if new_authority { accounting.reserve_map_entry(&mut authorities)?; }
        let durable = self.durable_accounted(authority, expected, commit.clone(), accounting)?;
        operations.insert(durable.operation_id, MemoryOperationIndex { index: 0, _envelope: operation });
        accounting.charge(metadata_copy_work::<DurableCommit>(1, accounting)?)?;
        commits.values.push(durable);
        Ok(MemoryFirstRecordPlan::Publish {
            expected,
            state: AuthorityState {
                epoch: Some(expected.epoch), commits: commits.values, operations,
                #[cfg(not(target_arch = "wasm32"))]
                commits_custody: commits.custody,
                _entry_envelope: envelope,
            },
        })
    }

    fn publish_first_record(
        &self, authority: AuthorityId, commit: &ProposedCommit, plan: MemoryFirstRecordPlan,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<bool, AuthorityMemoryFailure> {
        accounting.check()?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        if let Some(existing) = authorities.get(&authority) {
            if let Some(first) = existing.commits.first() {
                return Self::first_record_matches(first, commit, accounting);
            }
        }
        let MemoryFirstRecordPlan::Publish { expected, mut state } = plan else { return Ok(false); };
        let actual = match authorities.get(&authority) {
            Some(existing) => existing.head().map_err(|error| accounting.failure(error))?,
            None => Head::genesis(Epoch::GENESIS),
        };
        if actual != expected { return Ok(false); }
        let write = append_write_work(commit.payload.len() as u64);
        accounting.admit(write)?;
        if !authorities.contains_key(&authority) { accounting.reserve_map_entry(&mut authorities)?; }
        accounting.charge(write)?;
        // Creation and its exact initial append are one locked publication.
        if let Some(existing) = authorities.get_mut(&authority) {
            state._entry_envelope = std::mem::take(&mut existing._entry_envelope);
            *existing = state;
        } else {
            authorities.insert(authority, state);
        }
        Ok(true)
    }

    fn first_record_accounted(
        &self, authority: AuthorityId, commit: ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<bool, AuthorityMemoryFailure> {
        let plan = self.plan_first_record(authority, &commit, accounting)?;
        self.publish_first_record(authority, &commit, plan, accounting)
    }

    fn plan_append(
        &self, request: &crate::storage::GuardedAppend, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<MemoryAppendPlan, AuthorityMemoryFailure> {
        self.validate_payload(&request.commit, accounting)?;
        Self::validate_permit(request.permit, accounting)?;
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        accounting.examine()?;
        let state = authorities.get_mut(&request.authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head().map_err(|error| accounting.failure(error))?;
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? {
            return Ok(MemoryAppendPlan::Resolved(outcome));
        }
        if request.epoch != actual.epoch {
            return Ok(MemoryAppendPlan::Resolved(AppendOutcome::Fenced { actual_epoch: actual.epoch }));
        }
        if request.expected != actual {
            return Ok(MemoryAppendPlan::Resolved(AppendOutcome::Conflict { actual }));
        }
        accounting.admit(append_write_work(request.commit.payload.len() as u64))?;
        let commits = accounting.replacement(&state.commits)?;
        let operation = accounting.logical_entry::<(OperationId, MemoryOperationIndex)>()?;
        accounting.reserve_map_entry(&mut state.operations)?;
        let copied_records = if commits.is_some() {
            state.commits.len().checked_add(1).ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?
        } else { 1 };
        let copied = metadata_copy_work::<DurableCommit>(copied_records, accounting)?;
        accounting.admit(copied)?;
        let durable = self.durable_accounted(request.authority_id, actual, request.commit.clone(), accounting)?;
        Ok(MemoryAppendPlan::Publish { durable, commits, operation })
    }

    fn publish_append(
        &self, request: &crate::storage::GuardedAppend, plan: MemoryAppendPlan,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<AppendOutcome, AuthorityMemoryFailure> {
        accounting.check()?;
        if let MemoryAppendPlan::Resolved(outcome) = plan { return Ok(outcome); }
        let MemoryAppendPlan::Publish { durable, commits, operation } = plan else { unreachable!() };
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(poisoned_authority()))?;
        Self::validate_permit(request.permit, accounting)?;
        let state = authorities.get_mut(&request.authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head().map_err(|error| accounting.failure(error))?;
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? { return Ok(outcome); }
        if request.epoch != actual.epoch { return Ok(AppendOutcome::Fenced { actual_epoch: actual.epoch }); }
        if request.expected != actual { return Ok(AppendOutcome::Conflict { actual }); }
        let copied_records = if commits.is_some() {
            state.commits.len().checked_add(1).ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?
        } else { 1 };
        let copied = metadata_copy_work::<DurableCommit>(copied_records, accounting)?;
        let write = append_write_work(durable.payload.len() as u64);
        accounting.admit(write.checked_add(copied).map_err(|error| accounting.work_failure(error))?)?;
        accounting.record(write)?;
        if let Some(buffer) = commits {
            install_authority_commits(state, buffer);
        }
        let index = state.commits.len();
        state.operations.insert(durable.operation_id, MemoryOperationIndex { index, _envelope: operation });
        state.commits.push(durable.clone());
        accounting.record(copied)?;
        Ok(AppendOutcome::Committed(durable))
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
        let value = state.operation(operation_id).and_then(|index| state.commits.get(index));
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
        let mut accounting = AuthorityAccounting::native(work, cancellation)?;
        let value = operation(&mut accounting).map_err(|failure| failure.error)?;
        let guard = match accounting.output_custody.take() {
            Some(guard) => guard,
            None => work.with_owned_allocations(|_, _, _, _| Ok::<_, AuthorityStoreError>(()))?.1,
        };
        Ok((value, guard))
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
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let value = self.create_accounted(authority_id, genesis_epoch, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
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
        let request = crate::storage::GuardedAppend {
            authority_id, epoch, expected, commit,
            permit: crate::storage::PublicationPermit::Unrestricted,
        };
        let mut accounting = AuthorityAccounting::ordinary(budget);
        let plan = self.plan_append(&request, &mut accounting)?;
        let value = self.publish_append(&request, plan, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
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
    async fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> AuthorityResult<bool> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget);
        accounting.cancellation = Some(cancellation);
        let value = self.first_record_accounted(authority, commit, &mut accounting)?;
        Ok(AuthorityReceipt { value, work: accounting.work })
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> crate::facade::FsResult<bool>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        let mut accounting = AuthorityAccounting::ordinary(budget);
        accounting.cancellation = Some(cancellation);
        let plan = self.plan_first_record(authority, &commit, &mut accounting)
            .map_err(AuthorityMemoryFailure::into_fs)?;
        if !matches!(&plan, MemoryFirstRecordPlan::Resolved(false)) {
            let live = accounting.allocations.live_bytes();
            let mut remaining = accounting.work.remaining(budget)
                .map_err(|error| accounting.work_failure(error).into_fs())?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(live)
                .ok_or_else(|| accounting.failure(crate::WorkError::Overflow.into()).into_fs())?;
            let receipt = guard(remaining).await.map_err(|mut failure| {
                failure.work.peak_allocation_bytes = live
                    .checked_add(failure.work.peak_allocation_bytes).unwrap_or(u64::MAX);
                failure.map_with_prior_work(accounting.work, std::convert::identity)
            })?;
            let mut spent = receipt.work;
            spent.peak_allocation_bytes = live.checked_add(spent.peak_allocation_bytes)
                .ok_or_else(|| accounting.failure(crate::WorkError::Overflow.into()).into_fs())?;
            accounting.record(spent).map_err(AuthorityMemoryFailure::into_fs)?;
            accounting.work.verify(budget)
                .map_err(|error| accounting.failure(error.into()).into_fs())?;
        }
        let value = self.publish_first_record(authority, &commit, plan, &mut accounting)
            .map_err(AuthorityMemoryFailure::into_fs)?;
        Ok(crate::facade::FsReceipt { value, work: accounting.work })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_with_native_work(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<bool, AuthorityStoreError> {
        self.with_native_authority(original, cancellation, |accounting| {
            self.first_record_accounted(authority, commit, accounting)
        }).map(|(created, _)| created)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_work(
        &self, request: crate::storage::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), AuthorityStoreError> {
        self.with_native_authority(original, cancellation, |accounting| {
            let plan = self.plan_append(&request, accounting)?;
            self.publish_append(&request, plan, accounting)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: crate::storage::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), E>
    where
        F: FnMut(crate::NativeMaterializationWork) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: From<AuthorityStoreError> + Send,
    {
        let mut accounting = AuthorityAccounting::native(original, cancellation).map_err(E::from)?;
        let plan = self.plan_append(&request, &mut accounting).map_err(|failure| E::from(failure.error))?;
        // The complete original root is aliased, never replaced or reduced.
        // Every store and ledger loan above ended before calling the owner.
        guard(original.clone()).await?;
        let value = self.publish_append(&request, plan, &mut accounting)
            .map_err(|failure| E::from(failure.error))?;
        let (_, allocation) = original.with_owned_allocations(|_, _, _, _| {
            Ok::<_, AuthorityStoreError>(())
        }).map_err(E::from)?;
        Ok((value, allocation))
    }

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
    fn into_fs(self) -> crate::performance::OperationFailure<crate::facade::FsError> {
        crate::performance::OperationFailure::new(self.error.into(), self.work)
    }
}

impl From<AuthorityMemoryFailure> for AuthorityFailure {
    fn from(failure: AuthorityMemoryFailure) -> Self {
        Self::new(failure.error, failure.work)
    }
}

/// Both projections run the same producer. Root loans are brief accounting
/// leaves; no root mutex is retained while acquiring the authority mutex.
struct AuthorityAccounting<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    allocations: crate::kernel::allocation::AllocationLedger,
    cancellation: Option<&'a crate::CancellationToken>,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<&'a crate::materializer::NativeMaterializationWork>,
    #[cfg(not(target_arch = "wasm32"))]
    output_custody: Option<crate::materializer::NativeAllocation>,
}

impl<'a> AuthorityAccounting<'a> {
    fn ordinary(budget: WorkBudget) -> Self {
        Self {
            work: WorkCounters::default(), budget,
            allocations: crate::kernel::allocation::AllocationLedger::default(),
            cancellation: None,
            #[cfg(not(target_arch = "wasm32"))]
            native: None,
            #[cfg(not(target_arch = "wasm32"))]
            output_custody: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(
        original: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    ) -> Result<Self, AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        original.admit(WorkCounters::default())?;
        let mut accounting = Self::ordinary(WorkBudget::UNBOUNDED);
        accounting.native = Some(original);
        accounting.cancellation = Some(cancellation);
        Ok(accounting)
    }

    fn failure(&self, error: AuthorityStoreError) -> AuthorityMemoryFailure {
        AuthorityMemoryFailure::new(error, self.work)
    }

    fn static_corrupt(&self, message: &'static str) -> AuthorityStoreError {
        #[cfg(not(target_arch = "wasm32"))]
        if self.native.is_some() { return AuthorityStoreError::CorruptStatic(message); }
        AuthorityStoreError::Corrupt(message.to_owned())
    }

    fn static_rejected(&self, message: &'static str) -> AuthorityStoreError {
        #[cfg(not(target_arch = "wasm32"))]
        if self.native.is_some() { return AuthorityStoreError::RejectedStatic(message); }
        AuthorityStoreError::Rejected(message.to_owned())
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
        if let Some(native) = self.native {
            native.admit(WorkCounters::default())
                .map_err(|error| self.failure(error.into()))?;
        }
        Ok(())
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), AuthorityMemoryFailure> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = self.native {
            return native.admit(delta).map_err(|error| self.failure(error.into()));
        }
        self.work.checked_add(delta).map_err(|error| self.work_failure(error))?
            .verify(self.budget).map_err(|error| self.work_failure(error))
    }

    fn record(&mut self, delta: WorkCounters) -> Result<(), AuthorityMemoryFailure> {
        let next = self.work.checked_add(delta).map_err(|error| self.work_failure(error))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = self.native {
            native.record_admitted(delta).map_err(|error| self.failure(error.into()))?;
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

    fn logical_entry<T>(&mut self) -> Result<MemoryEntryEnvelope, AuthorityMemoryFailure> {
        self.check()?;
        let bytes = size_of::<T>() as u64;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = self.native {
            let (_, guard) = native.with_owned_allocations(|allocations, counters, budget, _| {
                allocations.claim_bytes(bytes, 0, counters, budget).map_err(|error| {
                    AuthorityStoreError::NativeOriginal(error.into_work_or(
                        crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
                    ))
                })
            }).map_err(|error| self.failure(error))?;
            return Ok(MemoryEntryEnvelope { _custody: Some(guard) });
        }
        self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
            .map_err(|error| self.work_failure(error.into_work_or(crate::WorkError::Overflow)))?;
        Ok(MemoryEntryEnvelope::default())
    }

    fn reserve_map_entry<K: Eq + std::hash::Hash, V>(
        &mut self, map: &mut HashMap<K, V>,
    ) -> Result<(), AuthorityMemoryFailure> {
        if map.len() < map.capacity() { return Ok(()); }
        // This is the actual fallible growth API, not an invented per-insert
        // operation or a fabricated bucket-capacity byte claim.
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        map.try_reserve(1)
            .map_err(|error| self.failure(AuthorityStoreError::MemoryReplayAllocationFailed(error)))
    }

    fn allocate_vector<T>(&mut self, capacity: usize) -> Result<AuthorityBuffer<T>, AuthorityMemoryFailure> {
        self.check()?;
        let requested = capacity.checked_mul(size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        // Guard is declared before the vector, including allocator failure paths.
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = if let Some(native) = self.native {
            if requested == 0 { None } else {
                Some(native.claim(requested).map_err(|error| self.failure(error.into()))?)
            }
        } else { None };
        #[cfg(not(target_arch = "wasm32"))]
        let portable = self.native.is_none();
        #[cfg(target_arch = "wasm32")]
        let portable = true;
        if portable {
            self.allocations.claim_bytes(requested, u64::from(requested != 0), &mut self.work, self.budget)
                .map_err(|error| self.work_failure(error.into_work_or(crate::WorkError::Overflow)))?;
        }
        let mut values = Vec::new();
        values.try_reserve_exact(capacity)
            .map_err(|error| self.failure(AuthorityStoreError::MemoryReplayAllocationFailed(error)))?;
        let actual = values.capacity().checked_mul(size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(guard) = &mut custody {
            guard.reconcile_capacity(actual).map_err(|error| self.failure(error.into()))?;
        }
        if portable && actual > requested {
            self.allocations.record_owned_bytes(actual - requested, &mut self.work)
                .map_err(|error| self.work_failure(error.into_work_or(crate::WorkError::Overflow)))?;
            self.work.verify(self.budget).map_err(|error| self.work_failure(error))?;
        }
        Ok(AuthorityBuffer {
            values,
            #[cfg(not(target_arch = "wasm32"))]
            custody,
        })
    }

    fn replacement<T>(&mut self, values: &Vec<T>) -> Result<Option<AuthorityBuffer<T>>, AuthorityMemoryFailure> {
        if values.len() < values.capacity() { return Ok(None); }
        let capacity = values.len().checked_add(1)
            .and_then(|needed| values.capacity().checked_mul(2).map(|double| needed.max(double)))
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        self.allocate_vector(capacity).map(Some)
    }

    fn clone_page(&mut self, commits: &[DurableCommit]) -> Result<Vec<DurableCommit>, AuthorityMemoryFailure> {
        let mut page = self.allocate_vector::<DurableCommit>(commits.len())?;
        for commit in commits {
            self.examine()?;
            #[cfg(not(target_arch = "wasm32"))]
            if self.native.is_some() {
                self.charge(WorkCounters { bytes_copied: size_of::<DurableCommit>() as u64, ..WorkCounters::default() })?;
            }
            page.values.push(commit.clone());
        }
        #[cfg(not(target_arch = "wasm32"))]
        { self.output_custody = page.custody.take(); }
        Ok(page.values)
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
