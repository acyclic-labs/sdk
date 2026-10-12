//! Deterministic in-memory backends used by conformance tests and ephemeral deployments.

use crate::foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, DurableCommit, Epoch, Head, OperationId,
    ProposedCommit, Sequence, authority_commit_digest,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityResult, AuthorityStoreError, CreateAuthorityOutcome, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure,
    ObjectId, ObjectRead, ObjectReadRetention, ObjectResult, ObjectStoreError, ObjectWrite, ReplayLimit, object_digest,
};
use acyclic_stream::OwnedBytes;
use crate::kernel::allocation::KernelOwnedAllocation;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Mutex, RwLock};

/// Default maximum immutable object size admitted by the memory backend.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

struct AuthorityState {
    epoch: Epoch,
    commits: Vec<DurableCommit>,
    operations: HashMap<OperationId, (usize, KernelOwnedAllocation)>,
    publication_gate_tail: u64,
    publication_reservation: Option<crate::PublicationReservation>,
    publication_proofs: HashMap<u64, (crate::PublicationReservation, KernelOwnedAllocation)>,
    commits_custody: KernelOwnedAllocation,
    _entry_envelope: KernelOwnedAllocation,
}

impl Default for AuthorityState {
    fn default() -> Self {
        Self {
            epoch: Epoch::GENESIS, commits: Vec::new(), operations: HashMap::new(),
            publication_gate_tail: 0, publication_reservation: None,
            publication_proofs: HashMap::new(),
            commits_custody: KernelOwnedAllocation::Standalone,
            _entry_envelope: KernelOwnedAllocation::Standalone,
        }
    }
}

impl AuthorityState {
    fn head(&self) -> Head {
        let epoch = self.epoch;
        self.commits.last().map_or_else(
            || Head::genesis(epoch),
            |commit| Head { epoch, sequence: commit.sequence, digest: commit.digest },
        )
    }

    fn resolve_operation(
        &self, commit: &ProposedCommit, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<AppendOutcome>, (AuthorityStoreError, WorkCounters)> {
        accounting.examine()?;
        let Some(&(index, _)) = self.operations.get(&commit.operation_id) else { return Ok(None); };
        let durable = &self.commits[index];
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

enum MemoryAppendPlan {
    Resolved(AppendOutcome),
    Publish {
        durable: DurableCommit,
        commits: Option<(Vec<DurableCommit>, KernelOwnedAllocation)>,
        operation: KernelOwnedAllocation,
    },
}

enum MemoryFirstRecordPlan {
    Resolved(bool),
    Publish(AuthorityState),
}

enum MemoryCreatePlan {
    Existing(Head),
    Publish(KernelOwnedAllocation),
}

fn append_write_work(payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: 1, authority_bytes_written: payload_bytes,
        backend_write_operations: 1, ..WorkCounters::default()
    }
}

fn metadata_copy_work(
    count: usize, width: usize, accounting: &AuthorityAccounting<'_>,
) -> Result<WorkCounters, (AuthorityStoreError, WorkCounters)> {
    let bytes = count.checked_mul(width)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?;
    Ok(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })
}

/// Deterministic single-process authority store.
pub struct MemoryAuthorityStore {
    maximum_payload_bytes: u64,
    authorities: Mutex<HashMap<AuthorityId, AuthorityState>>,
}

impl MemoryAuthorityStore {
    /// Creates a deterministic authority store with a hard per-record payload bound.
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
    ) -> Result<u64, (AuthorityStoreError, WorkCounters)> {
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
    ) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        if matches!(permit, crate::storage::PublicationPermit::Lease { .. }) {
            return Err(accounting.failure(accounting.static_rejected(
                "authority backend does not support atomic publication leases",
            )));
        }
        Ok(())
    }

    fn publication_permit_matches(state: &AuthorityState, permit: crate::PublicationPermit) -> bool {
        match (state.publication_reservation, permit) {
            (None, crate::PublicationPermit::Unrestricted) => true,
            (Some(proof), crate::PublicationPermit::Reservation { operation_id, gate_tail, expected }) =>
                proof.operation_id == OperationId::from_bytes(operation_id)
                    && proof.gate_tail == gate_tail && proof.expected == expected,
            _ => false,
        }
    }

    fn durable_accounted(
        &self, authority: AuthorityId, actual: Head, commit: ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<DurableCommit, (AuthorityStoreError, WorkCounters)> {
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
            digest, payload: commit.payload, settled_at_micros: None,
        })
    }

    fn create_accounted(
        &self, authority: AuthorityId, epoch: Epoch, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<CreateAuthorityOutcome, (AuthorityStoreError, WorkCounters)> {
        loop {
            let plan = self.plan_create(authority, accounting)?;
            if let Some(value) = self.publish_create(authority, epoch, plan, accounting)? {
                return Ok(value);
            }
        }
    }

    fn plan_create(
        &self, authority: AuthorityId, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<MemoryCreatePlan, (AuthorityStoreError, WorkCounters)> {
        accounting.check()?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        if let Some(state) = authorities.get(&authority) {
            accounting.charge(authority_read_work())?;
            return Ok(MemoryCreatePlan::Existing(state.head()));
        }
        accounting.admit(authority_write_work())?;
        let envelope = accounting.claim(size_of::<(AuthorityId, AuthorityState)>() as u64, 0)?;
        accounting.reserve_map_entry(&mut authorities)?;
        Ok(MemoryCreatePlan::Publish(envelope))
    }

    fn publish_create(
        &self, authority: AuthorityId, epoch: Epoch, plan: MemoryCreatePlan,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<CreateAuthorityOutcome>, (AuthorityStoreError, WorkCounters)> {
        accounting.check()?;
        if let MemoryCreatePlan::Existing(head) = plan {
            return Ok(Some(CreateAuthorityOutcome::Existing(head)));
        }
        let MemoryCreatePlan::Publish(envelope) = plan else { unreachable!() };
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        if let Some(state) = authorities.get(&authority) {
            accounting.charge(authority_read_work())?;
            return Ok(Some(CreateAuthorityOutcome::Existing(state.head())));
        }
        if authorities.len() == authorities.capacity() { return Ok(None); }
        accounting.charge(authority_write_work())?;
        authorities.insert(authority, AuthorityState {
            epoch, _entry_envelope: envelope, ..AuthorityState::default()
        });
        Ok(Some(CreateAuthorityOutcome::Created(Head::genesis(epoch))))
    }

    fn first_record_matches(
        first: &DurableCommit, commit: &ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<bool, (AuthorityStoreError, WorkCounters)> {
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
    ) -> Result<MemoryFirstRecordPlan, (AuthorityStoreError, WorkCounters)> {
        let payload_bytes = self.validate_payload(commit, accounting)?;
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        if let Some(state) = authorities.get(&authority) {
            if let Some(first) = state.commits.first() {
                return Self::first_record_matches(first, commit, accounting)
                    .map(MemoryFirstRecordPlan::Resolved);
            }
            if state.publication_reservation.is_some() {
                return Ok(MemoryFirstRecordPlan::Resolved(false));
            }
        }
        let expected = match authorities.get(&authority) {
            Some(state) => state.head(),
            None => Head::genesis(Epoch::GENESIS),
        };
        accounting.admit(append_write_work(payload_bytes))?;
        let new_authority = !authorities.contains_key(&authority);
        let envelope = if new_authority {
            accounting.claim(size_of::<(AuthorityId, AuthorityState)>() as u64, 0)?
        } else { KernelOwnedAllocation::Standalone };
        let operation = accounting.claim(size_of::<(OperationId, (usize, KernelOwnedAllocation))>() as u64, 0)?;
        let mut commits = accounting.allocate_commits(1)?;
        let mut operations = HashMap::new();
        accounting.reserve_map_entry(&mut operations)?;
        if new_authority { accounting.reserve_map_entry(&mut authorities)?; }
        let durable = self.durable_accounted(authority, expected, commit.clone(), accounting)?;
        operations.insert(durable.operation_id, (0, operation));
        accounting.charge(metadata_copy_work(1, size_of::<DurableCommit>(), accounting)?)?;
        commits.0.push(durable);
        Ok(MemoryFirstRecordPlan::Publish(AuthorityState {
            epoch: expected.epoch, commits: commits.0, operations,
            commits_custody: commits.1,
            _entry_envelope: envelope,
            ..AuthorityState::default()
        }))
    }

    fn publish_first_record(
        &self, authority: AuthorityId, commit: &ProposedCommit, plan: MemoryFirstRecordPlan,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<bool>, (AuthorityStoreError, WorkCounters)> {
        accounting.check()?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        if let Some(existing) = authorities.get(&authority) {
            if let Some(first) = existing.commits.first() {
                return Self::first_record_matches(first, commit, accounting).map(Some);
            }
            if existing.publication_reservation.is_some() { return Ok(Some(false)); }
        }
        let MemoryFirstRecordPlan::Publish(mut state) = plan else { return Ok(Some(false)); };
        let actual = match authorities.get(&authority) {
            Some(existing) => existing.head(),
            None => Head::genesis(Epoch::GENESIS),
        };
        if actual != Head::genesis(state.epoch) { return Ok(Some(false)); }
        let write = append_write_work(commit.payload.len() as u64);
        accounting.admit(write)?;
        if !authorities.contains_key(&authority) && authorities.len() >= authorities.capacity() {
            return Ok(None);
        }
        // Creation and its exact initial append are one locked publication.
        if let Some(existing) = authorities.get_mut(&authority) {
            state._entry_envelope = std::mem::replace(&mut existing._entry_envelope, KernelOwnedAllocation::Standalone);
            state.publication_gate_tail = existing.publication_gate_tail;
            state.publication_proofs = std::mem::take(&mut existing.publication_proofs);
            *existing = state;
        } else {
            authorities.insert(authority, state);
        }
        accounting.record(write)?;
        Ok(Some(true))
    }

    fn first_record_accounted(
        &self, authority: AuthorityId, commit: ProposedCommit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<bool, (AuthorityStoreError, WorkCounters)> {
        loop {
            let plan = self.plan_first_record(authority, &commit, accounting)?;
            if let Some(value) = self.publish_first_record(authority, &commit, plan, accounting)? {
                return Ok(value);
            }
        }
    }

    fn plan_append(
        &self, request: &crate::storage::GuardedAppend, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<MemoryAppendPlan, (AuthorityStoreError, WorkCounters)> {
        self.validate_payload(&request.commit, accounting)?;
        Self::validate_permit(request.permit, accounting)?;
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get_mut(&request.authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head();
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? {
            return Ok(MemoryAppendPlan::Resolved(outcome));
        }
        if request.epoch != actual.epoch {
            return Ok(MemoryAppendPlan::Resolved(AppendOutcome::Fenced { actual_epoch: actual.epoch }));
        }
        if request.expected != actual {
            return Ok(MemoryAppendPlan::Resolved(AppendOutcome::Conflict { actual }));
        }
        if !Self::publication_permit_matches(state, request.permit) {
            return Ok(MemoryAppendPlan::Resolved(AppendOutcome::Conflict { actual }));
        }
        accounting.admit(append_write_work(request.commit.payload.len() as u64))?;
        let commits = accounting.replacement(&state.commits)?;
        let operation = accounting.claim(size_of::<(OperationId, (usize, KernelOwnedAllocation))>() as u64, 0)?;
        accounting.reserve_map_entry(&mut state.operations)?;
        let copied_records = if commits.is_some() {
            state.commits.len().checked_add(1).ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?
        } else { 1 };
        let copied = metadata_copy_work(copied_records, size_of::<DurableCommit>(), accounting)?;
        accounting.admit(copied)?;
        let durable = self.durable_accounted(request.authority_id, actual, request.commit.clone(), accounting)?;
        Ok(MemoryAppendPlan::Publish { durable, commits, operation })
    }

    fn publish_append(
        &self, request: &crate::storage::GuardedAppend, plan: MemoryAppendPlan,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<AppendOutcome, (AuthorityStoreError, WorkCounters)> {
        accounting.check()?;
        if let MemoryAppendPlan::Resolved(outcome) = plan { return Ok(outcome); }
        let MemoryAppendPlan::Publish { durable, commits, operation } = plan else { unreachable!() };
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        Self::validate_permit(request.permit, accounting)?;
        let state = authorities.get_mut(&request.authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head();
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? { return Ok(outcome); }
        if request.epoch != actual.epoch { return Ok(AppendOutcome::Fenced { actual_epoch: actual.epoch }); }
        if request.expected != actual { return Ok(AppendOutcome::Conflict { actual }); }
        if !Self::publication_permit_matches(state, request.permit) {
            return Ok(AppendOutcome::Conflict { actual });
        }
        let copied_records = if commits.is_some() {
            state.commits.len().checked_add(1).ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?
        } else { 1 };
        let copied = metadata_copy_work(copied_records, size_of::<DurableCommit>(), accounting)?;
        let write = append_write_work(durable.payload.len() as u64);
        accounting.admit(write.checked_add(copied).map_err(|error| accounting.work_failure(error))?)?;
        accounting.record(write)?;
        if let Some(mut buffer) = commits {
            buffer.0.append(&mut state.commits);
            drop(std::mem::replace(&mut state.commits, buffer.0));
            state.commits_custody = buffer.1;
        }
        let index = state.commits.len();
        state.operations.insert(durable.operation_id, (index, operation));
        state.commits.push(durable.clone());
        accounting.record(copied)?;
        Ok(AppendOutcome::Committed(durable))
    }

    fn head_accounted(
        &self, authority_id: AuthorityId, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Head, (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        Ok(authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?
            .head())
    }

    fn replay_accounted(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<(Vec<DurableCommit>, KernelOwnedAllocation), (AuthorityStoreError, WorkCounters)> {
        accounting.check()?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(accounting.failure(AuthorityStoreError::InvalidReplayLimit));
        }
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let head = state.head();
        if after > head.sequence {
            return Err(accounting.failure(AuthorityStoreError::ReplayCursorAhead {
                after, head: head.sequence,
            }));
        }
        let start = usize::try_from(after.get()).map_err(|_| accounting.failure(
            AuthorityStoreError::ReplayCursorProcessOverflow { after },
        ))?;
        let maximum_records = usize::try_from(limit.records).unwrap_or(usize::MAX);
        let mut payload_bytes = 0_u64;
        let mut record_count = 0_usize;
        for commit in state.commits.iter().skip(start).take(maximum_records) {
            accounting.examine()?;
            let record_bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
            let next = payload_bytes.checked_add(record_bytes).ok_or_else(|| accounting.failure(
                AuthorityStoreError::Work(crate::WorkError::Overflow),
            ))?;
            if next > limit.payload_bytes {
                if record_count == 0 {
                    return Err(accounting.failure(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: record_bytes, maximum: limit.payload_bytes,
                    }));
                }
                break;
            }
            payload_bytes = next;
            record_count = record_count.saturating_add(1);
        }
        let end = start.checked_add(record_count).ok_or_else(|| accounting.failure(
            AuthorityStoreError::Work(crate::WorkError::Overflow),
        ))?;
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
    ) -> Result<crate::storage::FenceOutcome, (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get_mut(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head();
        if actual != expected || state.publication_reservation.is_some() {
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
        state.epoch = epoch;
        Ok(crate::storage::FenceOutcome::Advanced(
            state.head(),
        ))
    }

    fn find_operation_accounted(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<DurableCommit>, (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock()
            .map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get(&authority_id)
            .ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let value = state.operations.get(&operation_id).map(|&(index, _)| &state.commits[index]);
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

    fn publication_reservation_accounted(
        &self, authority: AuthorityId, operation: OperationId, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<Option<crate::PublicationReservation>, (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let authorities = self.authorities.lock().map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get(&authority).ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let proof = state.publication_reservation.as_ref().filter(|proof| proof.operation_id == operation);
        if proof.is_some() {
            let copied = metadata_copy_work(1, size_of::<crate::PublicationReservation>(), accounting)?;
            accounting.charge(copied)?;
        }
        Ok(proof.copied())
    }

    fn reserve_publication_accounted(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<crate::ReservationOutcome, (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock().map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get_mut(&authority).ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let actual = state.head();
        if actual != expected { return Ok(crate::ReservationOutcome::Conflict { actual }); }
        if let Some(proof) = state.publication_reservation.as_ref() {
            if proof.operation_id != operation || proof.expected != expected {
                return Ok(crate::ReservationOutcome::Conflict { actual });
            }
            accounting.charge(metadata_copy_work(1, size_of::<crate::PublicationReservation>(), accounting)?)?;
            return Ok(crate::ReservationOutcome::AlreadyReserved(*proof));
        }
        let gate_tail = state.publication_gate_tail.max(1).checked_add(1)
            .ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?;
        let write = authority_write_work();
        let copied = metadata_copy_work(2, size_of::<crate::PublicationReservation>(), accounting)?;
        accounting.admit(write.checked_add(copied).map_err(|error| accounting.work_failure(error))?)?;
        let envelope = accounting.claim(size_of::<(u64, (crate::PublicationReservation, KernelOwnedAllocation))>() as u64, 0)?;
        accounting.reserve_map_entry(&mut state.publication_proofs)?;
        accounting.check()?;
        accounting.record(write)?;
        accounting.record(copied)?;
        let reservation = crate::PublicationReservation { authority_id: authority, operation_id: operation, expected, gate_tail };
        state.publication_proofs.insert(gate_tail, (reservation, envelope));
        state.publication_reservation = Some(reservation);
        state.publication_gate_tail = gate_tail;
        Ok(crate::ReservationOutcome::Reserved(reservation))
    }

    fn release_publication_accounted(
        &self, reservation: crate::PublicationReservation, accounting: &mut AuthorityAccounting<'_>,
    ) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        accounting.charge(authority_read_work())?;
        let mut authorities = self.authorities.lock().map_err(|_| accounting.failure(AuthorityStoreError::MemoryAuthorityPoisoned))?;
        accounting.examine()?;
        let state = authorities.get_mut(&reservation.authority_id).ok_or_else(|| accounting.failure(AuthorityStoreError::Missing))?;
        let proof = state.publication_proofs.get(&reservation.gate_tail).ok_or_else(|| accounting.failure(
            accounting.static_rejected("publication reservation is no longer owned by this operation")))?;
        if proof.0 != reservation {
            return Err(accounting.failure(accounting.static_rejected("publication reservation does not match its original proof")));
        }
        if state.publication_gate_tail > reservation.gate_tail { return Ok(()); }
        if state.publication_reservation != Some(reservation) {
            return Err(accounting.failure(accounting.static_rejected("publication reservation is no longer active")));
        }
        let next = reservation.gate_tail.checked_add(1).ok_or_else(|| accounting.work_failure(crate::WorkError::Overflow))?;
        let write = authority_write_work();
        accounting.admit(write)?;
        accounting.check()?;
        accounting.record(write)?;
        state.publication_reservation = None;
        state.publication_gate_tail = next;
        Ok(())
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

impl crate::async_storage::AsyncAuthorityStore for MemoryAuthorityStore {
    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let value = self.create_accounted(authority_id, genesis_epoch, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn head(&self, authority_id: AuthorityId, budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<Head> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let value = self.head_accounted(authority_id, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<AppendOutcome> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let request = crate::storage::GuardedAppend {
            authority_id, epoch, expected, commit,
            permit: crate::storage::PublicationPermit::Unrestricted,
        };
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let plan = self.plan_append(&request, &mut accounting)?;
        let value = self.publish_append(&request, plan, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn replay(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit, budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let (value, _allocation) = self.replay_accounted(authority_id, after, limit, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn fence(
        &self, authority_id: AuthorityId, expected: Head, budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<crate::storage::FenceOutcome> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let value = self.fence_accounted(authority_id, expected, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn find_operation(
        &self, authority_id: AuthorityId, operation_id: OperationId, budget: WorkBudget, cancellation: &crate::CancellationToken) -> AuthorityResult<Option<DurableCommit>> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, None);
        let value = self.find_operation_accounted(authority_id, operation_id, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn publication_reservation(
        &self, authority: AuthorityId, operation: OperationId, budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> AuthorityResult<Option<crate::PublicationReservation>> {
        let mut accounting = AuthorityAccounting::ordinary(budget, Some(cancellation));
        let value = self.publication_reservation_accounted(authority, operation, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn reserve_publication(
        &self, authority: AuthorityId, expected: Head, operation: OperationId, budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> AuthorityResult<crate::ReservationOutcome> {
        let mut accounting = AuthorityAccounting::ordinary(budget, Some(cancellation));
        let value = self.reserve_publication_accounted(authority, expected, operation, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn release_publication(
        &self, reservation: crate::PublicationReservation, budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> AuthorityResult<()> {
        let mut accounting = AuthorityAccounting::ordinary(budget, Some(cancellation));
        let value = self.release_publication_accounted(reservation, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn publication_reservation_with_native_work(
        &self, authority: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::PublicationReservation>, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        self.publication_reservation_accounted(authority, operation, &mut accounting)
            .map_err(|(error, _)| error)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn reserve_publication_with_native_work(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<crate::PublicationReservation, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        match self.reserve_publication_accounted(authority, expected, operation, &mut accounting)
            .map_err(|(error, _)| error)? {
            crate::ReservationOutcome::Reserved(proof) | crate::ReservationOutcome::AlreadyReserved(proof) => Ok(proof),
            crate::ReservationOutcome::Conflict { .. } =>
                Err(AuthorityStoreError::RejectedStatic("publication reservation head or gate changed")),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn release_publication_with_native_work(
        &self, reservation: crate::PublicationReservation,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(), AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        self.release_publication_accounted(reservation, &mut accounting).map_err(|(error, _)| error)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: crate::WorkspaceForkCommit, permit: crate::PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> Result<crate::WorkspaceForkOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        crate::async_storage::commit_workspace_fork_in_steps_guarded_with_native_work(
            self, fork, permit, original, cancellation, guard,
        ).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> Result<CreateAuthorityOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let mut accounting = AuthorityAccounting::native(original, cancellation).map_err(crate::FsError::from)?;
        loop {
            let plan = self.plan_create(authority, &mut accounting).map_err(|(error, _)| crate::FsError::from(error))?;
            accounting.check().map_err(|(error, _)| crate::FsError::from(error))?;
            guard(original).await?;
            accounting.check().map_err(|(error, _)| crate::FsError::from(error))?;
            if let Some(value) = self.publish_create(authority, epoch, plan, &mut accounting)
                .map_err(|(error, _)| crate::FsError::from(error))? {
                return Ok(value);
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> Result<bool, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let mut accounting = AuthorityAccounting::native(original, cancellation).map_err(crate::FsError::from)?;
        loop {
            let plan = self.plan_first_record(authority, &commit, &mut accounting)
                .map_err(|(error, _)| crate::FsError::from(error))?;
            accounting.check().map_err(|(error, _)| crate::FsError::from(error))?;
            guard(original).await?;
            accounting.check().map_err(|(error, _)| crate::FsError::from(error))?;
            if let Some(value) = self.publish_first_record(authority, &commit, plan, &mut accounting)
                .map_err(|(error, _)| crate::FsError::from(error))? {
                return Ok(value);
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<CreateAuthorityOutcome, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        self.create_accounted(authority, epoch, &mut accounting).map_err(|(error, _)| error)
    }
    async fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> AuthorityResult<bool> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let mut accounting = AuthorityAccounting::ordinary(budget, Some(cancellation));
        let value = self.first_record_accounted(authority, commit, &mut accounting)?;
        Ok(crate::OperationReceipt { value, work: accounting.spent() })
    }
    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> crate::facade::FsResult<bool>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        let mut accounting = AuthorityAccounting::ordinary(budget, Some(cancellation));
        loop {
            let plan = self.plan_first_record(authority, &commit, &mut accounting)
                .map_err(authority_fs_failure)?;
            if !matches!(&plan, MemoryFirstRecordPlan::Resolved(false)) {
                let live = accounting.ordinary_parts().0.live_bytes();
                let mut remaining = accounting.spent().remaining(budget)
                    .map_err(|error| authority_fs_failure(accounting.work_failure(error)))?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(live)
                    .ok_or_else(|| authority_fs_failure(accounting.failure(crate::WorkError::Overflow.into())))?;
                let receipt = guard(remaining).await.map_err(|mut failure| {
                    failure.work.peak_allocation_bytes = live
                        .checked_add(failure.work.peak_allocation_bytes).unwrap_or(u64::MAX);
                    failure.map_with_prior_work(accounting.spent(), std::convert::identity)
                })?;
                let mut spent = receipt.work;
                spent.peak_allocation_bytes = live.checked_add(spent.peak_allocation_bytes)
                    .ok_or_else(|| authority_fs_failure(accounting.failure(crate::WorkError::Overflow.into())))?;
                accounting.record(spent).map_err(authority_fs_failure)?;
                accounting.spent().verify(budget)
                    .map_err(|error| authority_fs_failure(accounting.failure(error.into())))?;
            }
            if let Some(value) = self.publish_first_record(authority, &commit, plan, &mut accounting)
                .map_err(authority_fs_failure)? {
                return Ok(crate::facade::FsReceipt { value, work: accounting.spent() });
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_with_native_work(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<bool, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        self.first_record_accounted(authority, commit, &mut accounting).map_err(|(error, _)| error)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_work(
        &self, request: crate::storage::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(original, cancellation)?;
        let plan = self.plan_append(&request, &mut accounting).map_err(|(error, _)| error)?;
        let value = self.publish_append(&request, plan, &mut accounting).map_err(|(error, _)| error)?;
        let allocation = original.claim_allocation(0, 0, WorkCounters::UNCHARGED)?;
        Ok((value, allocation))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: crate::storage::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, guard: &mut F,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<AuthorityStoreError> + Send + 'static,
    {
        let mut accounting = AuthorityAccounting::native(original, cancellation).map_err(E::from)?;
        let plan = self.plan_append(&request, &mut accounting).map_err(|(error, _)| E::from(error))?;
        guard(original).await?;
        let value = self.publish_append(&request, plan, &mut accounting)
            .map_err(|(error, _)| E::from(error))?;
        let allocation = original.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED)
            .map_err(|error| E::from(AuthorityStoreError::from(error)))?;
        Ok((value, allocation))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn head_with_native_work(
        &self, authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Head, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(work, cancellation)?;
        let head = self.head_accounted(authority_id, &mut accounting).map_err(|(error, _)| error)?;
        drop(work.claim_allocation(0, 0, WorkCounters::UNCHARGED)?);
        Ok(head)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn replay_with_native_work(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(work, cancellation)?;
        let (page, allocation) = self.replay_accounted(authority_id, after, limit, &mut accounting)
            .map_err(|(error, _)| error)?;
        match allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => Ok((page, allocation)),
            KernelOwnedAllocation::Standalone => unreachable!("native replay allocation"),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn fence_with_native_work(
        &self, authority_id: AuthorityId, expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::storage::FenceOutcome, AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(work, cancellation)?;
        let outcome = self.fence_accounted(authority_id, expected, &mut accounting).map_err(|(error, _)| error)?;
        drop(work.claim_allocation(0, 0, WorkCounters::UNCHARGED)?);
        Ok(outcome)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn find_operation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Option<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        let mut accounting = AuthorityAccounting::native(work, cancellation)?;
        let value = self.find_operation_accounted(authority_id, operation_id, &mut accounting)
            .map_err(|(error, _)| error)?;
        let allocation = work.claim_allocation(0, 0, WorkCounters::UNCHARGED)?;
        Ok((value, allocation))
    }
}



impl From<(AuthorityStoreError, WorkCounters)> for AuthorityFailure {
    fn from((error, work): (AuthorityStoreError, WorkCounters)) -> Self {
        Self::new(error, work)
    }
}

fn authority_fs_failure((error, work): (AuthorityStoreError, WorkCounters)) -> crate::performance::OperationFailure<crate::FsError> {
    crate::performance::OperationFailure::new(error.into(), work)
}

/// Funding for the common authority producer.
enum AuthorityAccounting<'a> {
    Ordinary {
        work: WorkCounters, budget: WorkBudget,
        allocations: crate::kernel::allocation::AllocationLedger,
        cancellation: Option<&'a crate::CancellationToken>,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

impl<'a> AuthorityAccounting<'a> {
    fn ordinary(budget: WorkBudget, cancellation: Option<&'a crate::CancellationToken>) -> Self {
        Self::Ordinary {
            work: WorkCounters::default(), budget, allocations: Default::default(), cancellation,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(original: &'a crate::materializer::NativeMaterializationWork, cancellation: &'a crate::CancellationToken)
    -> Result<Self, AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(Self::Native { original, cancellation })
    }

    fn spent(&self) -> WorkCounters {
        match self {
            Self::Ordinary { work, .. } => *work,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.receipt(),
        }
    }

    fn ordinary_parts(&mut self) -> (&mut crate::kernel::allocation::AllocationLedger, &mut WorkCounters, WorkBudget) {
        match self {
            Self::Ordinary { allocations, work, budget, .. } => (allocations, work, *budget),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => unreachable!("ordinary funding"),
        }
    }

    fn failure(&self, error: AuthorityStoreError) -> (AuthorityStoreError, WorkCounters) { (error, self.spent()) }

    fn static_rejected(&self, message: &'static str) -> AuthorityStoreError {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Native { .. }) { return AuthorityStoreError::RejectedStatic(message); }
        AuthorityStoreError::Rejected(message.to_owned())
    }

    fn work_failure(&self, error: crate::WorkError) -> (AuthorityStoreError, WorkCounters) {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Native { .. }) {
            return self.failure(AuthorityStoreError::NativeOriginal(crate::materializer::NativeTreeMaterializationError::Work(error)));
        }
        (error.into(), WorkCounters::default())
    }

    fn check(&self) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        let cancellation = match self {
            Self::Ordinary { cancellation, .. } => *cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { cancellation, .. } => Some(*cancellation),
        };
        if let Some(cancellation) = cancellation {
            cancellation.check().map_err(|_| self.failure(AuthorityStoreError::Cancelled))?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { original, .. } = self {
            original.admit(WorkCounters::default()).map_err(|error| self.failure(error.into()))?;
        }
        Ok(())
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        self.check()?;
        match self {
            Self::Ordinary { work, budget, .. } => work.checked_add(delta)
                .and_then(|work| work.verify(*budget)).map_err(|error| self.work_failure(error)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.admit(delta).map_err(|error| self.failure(error.into())),
        }
    }

    fn record(&mut self, delta: WorkCounters) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        let next = self.spent().checked_add(delta).map_err(|error| self.work_failure(error))?;
        match self {
            Self::Ordinary { work, .. } => *work = next,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.record_admitted(delta)
                .map_err(|error| (error.into(), original.receipt()))?,
        }
        Ok(())
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        self.admit(delta)?;
        self.record(delta)
    }

    fn examine(&mut self) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Native { .. }) {
            return self.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() });
        }
        self.check()
    }

    fn claim(&mut self, bytes: u64, operations: u64) -> Result<KernelOwnedAllocation, (AuthorityStoreError, WorkCounters)> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { original, .. } = self {
            return original.claim_allocation(bytes, operations, crate::WorkCounters::UNCHARGED)
                .map(KernelOwnedAllocation::NativeOriginal).map_err(|error| (error.into(), original.receipt()));
        }
        let (allocations, work, budget) = self.ordinary_parts();
        allocations.claim_bytes(bytes, operations, work, budget)
            .map_err(|error| (error.into_work_or(crate::WorkError::Overflow).into(), WorkCounters::default()))?;
        Ok(KernelOwnedAllocation::Standalone)
    }

    fn reserve_map_entry<K: Eq + std::hash::Hash, V>(
        &mut self, map: &mut HashMap<K, V>,
    ) -> Result<(), (AuthorityStoreError, WorkCounters)> {
        if map.len() < map.capacity() { return Ok(()); }
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        map.try_reserve(1)
            .map_err(|error| self.failure(AuthorityStoreError::MemoryReplayAllocationFailed(error)))
    }

    fn allocate_commits(&mut self, capacity: usize) -> Result<(Vec<DurableCommit>, KernelOwnedAllocation), (AuthorityStoreError, WorkCounters)> {
        self.check()?;
        let requested = capacity.checked_mul(size_of::<DurableCommit>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        // The vector dies before its guard on every allocator failure.
        let mut custody = self.claim(requested, u64::from(requested != 0))?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity)
            .map_err(|error| self.failure(AuthorityStoreError::MemoryReplayAllocationFailed(error)))?;
        let actual = values.capacity().checked_mul(size_of::<DurableCommit>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        match &mut custody {
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(guard) => {
                guard.reconcile_capacity(actual).map_err(|error| self.failure(error.into()))?;
            }
            KernelOwnedAllocation::Standalone => if actual > requested {
                let (allocations, work, budget) = self.ordinary_parts();
                allocations.record_owned_bytes(actual - requested, work)
                    .map_err(|error| (error.into_work_or(crate::WorkError::Overflow).into(), WorkCounters::default()))?;
                work.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
            },
        }
        Ok((values, custody))
    }

    fn replacement(&mut self, values: &Vec<DurableCommit>) -> Result<Option<(Vec<DurableCommit>, KernelOwnedAllocation)>, (AuthorityStoreError, WorkCounters)> {
        if values.len() < values.capacity() { return Ok(None); }
        let capacity = values.len().checked_add(1)
            .and_then(|needed| values.capacity().checked_mul(2).map(|double| needed.max(double)))
            .ok_or_else(|| self.work_failure(crate::WorkError::Overflow))?;
        self.allocate_commits(capacity).map(Some)
    }

    fn clone_page(&mut self, commits: &[DurableCommit]) -> Result<(Vec<DurableCommit>, KernelOwnedAllocation), (AuthorityStoreError, WorkCounters)> {
        let mut page = self.allocate_commits(commits.len())?;
        for commit in commits {
            self.examine()?;
            #[cfg(not(target_arch = "wasm32"))]
            if matches!(self, Self::Native { .. }) {
                self.charge(WorkCounters { bytes_copied: size_of::<DurableCommit>() as u64, ..WorkCounters::default() })?;
            }
            page.0.push(commit.clone());
        }
        Ok(page)
    }
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

/// Deterministic immutable-object store.
pub struct MemoryObjectStore {
    maximum_object_bytes: u64,
    objects: RwLock<MemoryObjectState>,
}

// The backing vector is destroyed before its original capacity guard.
struct MemoryObjectState {
    objects: Vec<(ObjectId, OwnedBytes)>,
    table_custody: KernelOwnedAllocation,
}

impl Default for MemoryObjectState {
    fn default() -> Self {
        Self { objects: Vec::new(), table_custody: KernelOwnedAllocation::Standalone }
    }
}

impl MemoryObjectState {
    fn get(&self, id: &ObjectId) -> Option<&OwnedBytes> {
        self.objects.binary_search_by_key(id, |(id, _)| *id)
            .ok().map(|index| &self.objects[index].1)
    }
}

enum MemoryObjectAccounting<'a> {
    Portable {
        work: WorkCounters, budget: &'a WorkBudget,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        work: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

impl<'a> MemoryObjectAccounting<'a> {
    fn allocate<T>(&mut self, count: usize) -> Result<(Vec<T>, KernelOwnedAllocation), ObjectStoreError> {
        let requested = count.checked_mul(size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        let allocation = WorkCounters {
            allocation_operations: 1, peak_allocation_bytes: requested,
            ..WorkCounters::default()
        };
        self.admit(allocation)?;
        let mut custody = match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => KernelOwnedAllocation::NativeOriginal(work.claim(requested)?),
            Self::Portable { .. } => {
                self.record(allocation)?;
                KernelOwnedAllocation::Standalone
            }
        };
        // Declare after the guard so errors free the backing first.
        let mut values = Vec::<T>::new();
        values.try_reserve_exact(count).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual = values.capacity().checked_mul(size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        match &mut custody {
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(custody) => custody.resize_retained(actual)?,
            KernelOwnedAllocation::Standalone => if actual > requested {
                self.charge(WorkCounters { peak_allocation_bytes: actual, ..WorkCounters::default() })?;
            },
        }
        Ok((values, custody))
    }

    fn portable(budget: &'a WorkBudget) -> Self {
        Self::Portable {
            work: WorkCounters::default(), budget,
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
            Self::Portable { work, budget, .. } => work.checked_add(delta)?.verify(**budget).map_err(Into::into),
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
        self.check()?;
        match self {
            Self::Portable { work, budget } => work.charge(&delta, budget).map_err(Into::into),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.charge(delta).map_err(Into::into),
        }
    }

    fn portable_work(&self) -> WorkCounters {
        match self {
            Self::Portable { work, .. } => *work,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => WorkCounters::default(),
        }
    }
}

impl MemoryObjectStore {
    /// Creates a bounded memory object store.
    pub fn new(maximum_object_bytes: u64) -> Result<Self, ObjectStoreError> {
        if maximum_object_bytes == 0 {
            return Err(ObjectStoreError::TooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            maximum_object_bytes,
            objects: RwLock::new(MemoryObjectState::default()),
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
        let mut replacement = None;
        if needed > state.objects.capacity() {
            let capacity = needed.max(state.objects.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?);
            replacement = Some(accounting.allocate(capacity)?);
        }
        let write = WorkCounters {
            backend_write_operations: 1, object_bytes_written: written,
            ..WorkCounters::default()
        };
        let copy = WorkCounters {
            bytes_copied: if replacement.is_some() {
                state.objects.len().checked_mul(size_of::<(ObjectId, OwnedBytes)>())
                    .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?
            } else { 0 },
            ..WorkCounters::default()
        };
        accounting.admit(write.checked_add(copy)?)?;
        accounting.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
        if let Some((mut table, custody)) = replacement {
            table.append(&mut state.objects);
            let old = std::mem::replace(&mut state.objects, table);
            drop(old);
            state.table_custody = custody;
        }
        accounting.record(copy)?;
        for entry in writes {
            if let Err(index) = state.objects.binary_search_by_key(&entry.object_id, |(id, _)| *id) {
                let shift = state.objects.len().checked_sub(index)
                    .and_then(|count| count.checked_mul(size_of::<(ObjectId, OwnedBytes)>()))
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
        accounting: &mut MemoryObjectAccounting<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        accounting.charge(WorkCounters {
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
        accounting.charge(WorkCounters {
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
    ) -> Result<(Vec<ObjectRead>, KernelOwnedAllocation), ObjectStoreError> {
        if requests.is_empty() { return Err(ObjectStoreError::EmptyReadBatch); }
        accounting.check()?;
        let mut output = accounting.allocate::<ObjectRead>(requests.len())?;
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
            output.0.push(ObjectRead { bytes: bytes.clone(), retention: ObjectReadRetention::Shared });
        }
        accounting.charge(WorkCounters {
            items_returned: u64::try_from(output.0.len()).map_err(|_| crate::WorkError::Overflow)?,
            ..WorkCounters::default()
        })?;
        accounting.check()?;
        Ok(output)
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self {
            maximum_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            objects: RwLock::new(MemoryObjectState::default()),
        }
    }
}

impl crate::async_storage::AsyncObjectStore for MemoryObjectStore {
    async fn put(&self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &crate::CancellationToken) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut accounting = MemoryObjectAccounting::portable(&budget);
        let result = self.put_admitted(&[ObjectWrite { object_id, bytes }], false, &mut accounting);
        result.map(|value| crate::OperationReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }
    async fn put_many(&self, writes: &[ObjectWrite], budget: WorkBudget, cancellation: &crate::CancellationToken) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut accounting = MemoryObjectAccounting::portable(&budget);
        let result = self.put_admitted(writes, false, &mut accounting);
        result.map(|value| crate::OperationReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget, cancellation: &crate::CancellationToken) -> ObjectResult<ObjectRead> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut accounting = MemoryObjectAccounting::portable(&budget);
        let result = self.read_admitted(object_id, maximum_bytes, &mut accounting);
        result.map(|value| crate::OperationReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::storage::ObjectRead, ObjectStoreError> {
        self.read_admitted(object_id, maximum_bytes,
            &mut MemoryObjectAccounting::Native { work, cancellation })
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget, cancellation: &crate::CancellationToken) -> ObjectResult<Vec<ObjectRead>> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut accounting = MemoryObjectAccounting::portable(&budget);
        let result = self.read_many_admitted(requests, &mut accounting);
        result.map(|value| crate::OperationReceipt { value: value.0, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self, requests: &[crate::storage::ObjectReadRequest],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<crate::storage::ObjectRead>, crate::materializer::NativeAllocation), ObjectStoreError> {
        let result = self.read_many_admitted(
            requests, &mut MemoryObjectAccounting::Native { work, cancellation },
        )?;
        let (reads, allocation) = result;
        let KernelOwnedAllocation::NativeOriginal(custody) = allocation else {
            drop(reads);
            return Err(ObjectStoreError::Corrupt);
        };
        crate::async_storage::native_read_batch(reads, custody)
    }
    async fn contains(&self, object_id: ObjectId, budget: WorkBudget, cancellation: &crate::CancellationToken) -> ObjectResult<bool> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut accounting = MemoryObjectAccounting::portable(&budget);
        let result = match self.read_admitted(object_id, u64::MAX, &mut accounting) {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        };
        result.map(|value| crate::OperationReceipt { value, work: accounting.portable_work() })
            .map_err(|error| ObjectFailure::new(error, accounting.portable_work()))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_admitted(&[ObjectWrite { object_id, bytes }], false,
            &mut MemoryObjectAccounting::Native { work, cancellation })
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        self.put_admitted(&[ObjectWrite { object_id, bytes }], true,
            &mut MemoryObjectAccounting::Native { work, cancellation })
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_admitted(writes, false, &mut MemoryObjectAccounting::Native { work, cancellation })
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, _scope: crate::async_storage::PublicationScope<'_>,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        let accounting = MemoryObjectAccounting::Native { work, cancellation };
        accounting.check()?;
        Ok(crate::PublicationHold::none())
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
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
