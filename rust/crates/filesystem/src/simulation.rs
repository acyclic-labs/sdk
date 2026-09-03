//! Deterministic bounded fault simulator for authority and immutable-object backends.
//!
//! The simulator is qualification infrastructure over the two canonical storage
//! primitives. It never becomes filesystem truth: healing or dropping it leaves
//! only the wrapped memory authority log and immutable objects.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityResult, AuthorityStoreError, CreateAuthorityOutcome,
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError, ReplayLimit, object_digest,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

/// One exact simulator interception point.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SimulationOperation {
    /// Authority creation.
    AuthorityCreate,
    /// Linearizable authority-head read.
    AuthorityHead,
    /// Authority compare-and-append.
    AuthorityAppend,
    /// Bounded authority replay.
    AuthorityReplay,
    /// Authority epoch fence.
    AuthorityFence,
    /// Idempotent operation lookup.
    AuthorityFindOperation,
    /// Immutable-object admission.
    ObjectPut,
    /// One immutable-object read.
    ObjectRead,
    /// Ordered immutable-object batch read.
    ObjectReadMany,
    /// Immutable-object presence probe.
    ObjectContains,
}

/// One exact injected backend behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimulationFault {
    /// Reject before touching the wrapped primitive.
    RejectBefore,
    /// Simulate an unavailable partition before touching the wrapped primitive.
    PartitionBefore,
    /// Acknowledge a valid object but delay its visibility until an explicit flush.
    DelayObjectVisibility,
    /// Return one digest-invalid byte from an otherwise successful object read.
    CorruptObjectRead,
    /// Return a strict prefix from an otherwise successful object read.
    PartialObjectRead,
    /// Return a caller-selected stale authority head.
    StaleAuthorityHead(Head),
    /// Duplicate the final record of an otherwise valid replay page.
    DuplicateAuthorityReplay,
    /// Durably append, then lose the acknowledgement as an indeterminate result.
    AmbiguousAuthorityAppend,
    /// Advance the epoch immediately before compare-and-append.
    FenceBeforeAuthorityAppend,
}

impl SimulationFault {
    const fn compatible(self, operation: SimulationOperation) -> bool {
        match self {
            Self::RejectBefore | Self::PartitionBefore => true,
            Self::DelayObjectVisibility => matches!(operation, SimulationOperation::ObjectPut),
            Self::CorruptObjectRead | Self::PartialObjectRead => matches!(
                operation,
                SimulationOperation::ObjectRead | SimulationOperation::ObjectReadMany
            ),
            Self::StaleAuthorityHead(_) => {
                matches!(operation, SimulationOperation::AuthorityHead)
            }
            Self::DuplicateAuthorityReplay => {
                matches!(operation, SimulationOperation::AuthorityReplay)
            }
            Self::AmbiguousAuthorityAppend | Self::FenceBeforeAuthorityAppend => {
                matches!(operation, SimulationOperation::AuthorityAppend)
            }
        }
    }
}

/// One exact operation occurrence and its injected behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledSimulationFault {
    /// Intercepted operation family.
    pub operation: SimulationOperation,
    /// One-based occurrence within that operation family.
    pub occurrence: u64,
    /// Behavior injected at the exact occurrence.
    pub fault: SimulationFault,
}

/// Hard simulator resource bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimulationOptions {
    /// Maximum scheduled fault points retained at once.
    pub maximum_scheduled_faults: usize,
    /// Maximum operation trace entries retained before fail-closed saturation.
    pub maximum_trace_entries: usize,
    /// Maximum delayed immutable objects retained before visibility flush.
    pub maximum_pending_objects: usize,
    /// Maximum cumulative delayed canonical object bytes.
    pub maximum_pending_object_bytes: u64,
}

impl Default for SimulationOptions {
    fn default() -> Self {
        Self {
            maximum_scheduled_faults: 4_096,
            maximum_trace_entries: 65_536,
            maximum_pending_objects: 4_096,
            maximum_pending_object_bytes: 256 * 1024 * 1024,
        }
    }
}

/// One retained deterministic interception trace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimulationTrace {
    /// Intercepted operation family.
    pub operation: SimulationOperation,
    /// One-based occurrence within the operation family.
    pub occurrence: u64,
    /// Injected behavior, when this operation matched a schedule entry.
    pub fault: Option<SimulationFault>,
}

/// Simulator configuration or state failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SimulationError {
    /// Every configured hard bound must be positive.
    #[error("simulation bounds must be positive")]
    ZeroBound,
    /// Fault occurrence identities are one-based.
    #[error("simulation fault occurrence must be positive")]
    ZeroOccurrence,
    /// The selected fault cannot apply to the selected operation family.
    #[error("simulation fault is incompatible with its operation family")]
    IncompatibleFault,
    /// The exact operation occurrence already has a scheduled fault.
    #[error("simulation operation occurrence already has a fault")]
    DuplicateFault,
    /// The selected operation occurrence has already completed.
    #[error("simulation operation occurrence has already completed")]
    PastOccurrence,
    /// The configured fault schedule bound is exhausted.
    #[error("simulation fault schedule is full")]
    ScheduleFull,
    /// A simulator lock was poisoned by a failed host thread.
    #[error("simulation state is poisoned")]
    Poisoned,
}

struct State {
    options: SimulationOptions,
    occurrences: BTreeMap<SimulationOperation, u64>,
    faults: BTreeMap<(SimulationOperation, u64), SimulationFault>,
    trace: Vec<SimulationTrace>,
    pending_objects: BTreeMap<ObjectId, Bytes>,
    pending_object_bytes: u64,
}

impl State {
    fn intercept(
        &mut self,
        operation: SimulationOperation,
    ) -> Result<(u64, Option<SimulationFault>), &'static str> {
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err("simulation trace bound exhausted");
        }
        let occurrence = self
            .occurrences
            .get(&operation)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or("simulation operation occurrence overflow")?;
        self.occurrences.insert(operation, occurrence);
        let fault = self.faults.remove(&(operation, occurrence));
        self.trace
            .try_reserve(1)
            .map_err(|_| "simulation trace allocation failed")?;
        self.trace.push(SimulationTrace {
            operation,
            occurrence,
            fault,
        });
        Ok((occurrence, fault))
    }
}

/// Shared deterministic simulator and its two backend handles.
#[derive(Clone)]
pub struct Simulation<A = MemoryAuthorityStore, O = MemoryObjectStore> {
    state: Arc<Mutex<State>>,
    authority: Arc<A>,
    objects: Arc<O>,
}

impl Simulation<MemoryAuthorityStore, MemoryObjectStore> {
    /// Creates a bounded simulator over fresh deterministic memory primitives.
    ///
    /// # Errors
    ///
    /// Rejects any zero hard bound before allocating simulator state.
    pub fn new(options: SimulationOptions) -> Result<Self, SimulationError> {
        Self::wrap(
            MemoryAuthorityStore::default(),
            MemoryObjectStore::default(),
            options,
        )
    }
}

impl<A, O> Simulation<A, O> {
    /// Wraps explicit backends with one bounded deterministic schedule.
    ///
    /// This is the black-box qualification seam for future infrastructure
    /// adapters. The simulator does not inspect or replace backend state.
    ///
    /// # Errors
    ///
    /// Rejects any zero hard bound before retaining the supplied backends.
    pub fn wrap(
        authority: A,
        objects: O,
        options: SimulationOptions,
    ) -> Result<Self, SimulationError> {
        if options.maximum_scheduled_faults == 0
            || options.maximum_trace_entries == 0
            || options.maximum_pending_objects == 0
            || options.maximum_pending_object_bytes == 0
        {
            return Err(SimulationError::ZeroBound);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                options,
                occurrences: BTreeMap::new(),
                faults: BTreeMap::new(),
                trace: Vec::new(),
                pending_objects: BTreeMap::new(),
                pending_object_bytes: 0,
            })),
            authority: Arc::new(authority),
            objects: Arc::new(objects),
        })
    }

    /// Returns independently cloneable authority and object handles sharing one schedule.
    #[must_use]
    pub fn stores(&self) -> (SimulatedAuthorityStore<A>, SimulatedObjectStore<O>) {
        (
            SimulatedAuthorityStore {
                state: self.state.clone(),
                inner: self.authority.clone(),
            },
            SimulatedObjectStore {
                state: self.state.clone(),
                inner: self.objects.clone(),
            },
        )
    }

    /// Schedules one exact one-based interception point.
    ///
    /// # Errors
    ///
    /// Rejects zero occurrences, incompatible behavior, duplicate identities,
    /// poisoned state, or a full bounded schedule.
    pub fn schedule(&self, scheduled: ScheduledSimulationFault) -> Result<(), SimulationError> {
        if scheduled.occurrence == 0 {
            return Err(SimulationError::ZeroOccurrence);
        }
        if !scheduled.fault.compatible(scheduled.operation) {
            return Err(SimulationError::IncompatibleFault);
        }
        let mut state = self.lock_state()?;
        schedule_locked(&mut state, scheduled)
    }

    /// Atomically schedules the next occurrence of one operation family.
    ///
    /// This is useful for deterministic black-box runners after setup traffic.
    /// Concurrent runners should retain the returned absolute occurrence in
    /// their evidence rather than assume which request consumes it.
    ///
    /// # Errors
    ///
    /// Rejects incompatible behavior, occurrence exhaustion, poisoned state,
    /// or a full bounded schedule.
    pub fn schedule_next(
        &self,
        operation: SimulationOperation,
        fault: SimulationFault,
    ) -> Result<ScheduledSimulationFault, SimulationError> {
        if !fault.compatible(operation) {
            return Err(SimulationError::IncompatibleFault);
        }
        let mut state = self.lock_state()?;
        let occurrence = state
            .occurrences
            .get(&operation)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(SimulationError::PastOccurrence)?;
        let scheduled = ScheduledSimulationFault {
            operation,
            occurrence,
            fault,
        };
        schedule_locked(&mut state, scheduled)?;
        Ok(scheduled)
    }

    /// Returns the complete bounded trace collected so far.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-state failure instead of silently discarding evidence.
    pub fn trace(&self) -> Result<Vec<SimulationTrace>, SimulationError> {
        Ok(self.lock_state()?.trace.clone())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, State>, SimulationError> {
        self.state.lock().map_err(|_| SimulationError::Poisoned)
    }
}

fn schedule_locked(
    state: &mut State,
    scheduled: ScheduledSimulationFault,
) -> Result<(), SimulationError> {
    if state
        .faults
        .contains_key(&(scheduled.operation, scheduled.occurrence))
    {
        return Err(SimulationError::DuplicateFault);
    }
    if state
        .occurrences
        .get(&scheduled.operation)
        .is_some_and(|completed| *completed >= scheduled.occurrence)
    {
        return Err(SimulationError::PastOccurrence);
    }
    if state.faults.len() == state.options.maximum_scheduled_faults {
        return Err(SimulationError::ScheduleFull);
    }
    state
        .faults
        .insert((scheduled.operation, scheduled.occurrence), scheduled.fault);
    Ok(())
}

impl<A, O: AsyncObjectStore> Simulation<A, O> {
    /// Publishes delayed immutable objects in canonical identity order.
    ///
    /// Only one pending entry is retained outside the simulator lock at a time,
    /// so flushing does not allocate or copy the delayed object set. `Bytes`
    /// cloning retains the same immutable allocation until backend admission.
    ///
    /// # Errors
    ///
    /// Returns exact immutable-store, cancellation, budget, or poisoned-state
    /// failures. Successfully published entries are removed; the first failed
    /// entry and every later entry remain pending for an idempotent retry.
    pub async fn flush_object_visibility(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<u64> {
        let mut work = WorkCounters::default();
        let mut published = 0_u64;
        loop {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let next = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
                state
                    .pending_objects
                    .first_key_value()
                    .map(|(object_id, bytes)| (*object_id, bytes.clone()))
            };
            let Some((object_id, bytes)) = next else {
                return Ok(ObjectReceipt {
                    value: published,
                    work,
                });
            };
            let remaining = work
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            let receipt = self
                .objects
                .put(object_id, bytes, remaining, cancellation)
                .await
                .map_err(|failure| {
                    let nested = *failure.work;
                    match work.checked_add(nested) {
                        Ok(combined) => ObjectFailure::new(failure.error, combined),
                        Err(error) => ObjectFailure::new(error.into(), work),
                    }
                })?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
            if let Some(removed) = state.pending_objects.remove(&object_id) {
                state.pending_object_bytes = state
                    .pending_object_bytes
                    .checked_sub(u64::try_from(removed.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
            }
            published = published.checked_add(1).ok_or_else(|| {
                ObjectFailure::new(ObjectStoreError::Work(crate::WorkError::Overflow), work)
            })?;
        }
    }
}

impl Default for Simulation {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                options: SimulationOptions::default(),
                occurrences: BTreeMap::new(),
                faults: BTreeMap::new(),
                trace: Vec::new(),
                pending_objects: BTreeMap::new(),
                pending_object_bytes: 0,
            })),
            authority: Arc::new(MemoryAuthorityStore::default()),
            objects: Arc::new(MemoryObjectStore::default()),
        }
    }
}

/// Authority backend handle controlled by one [`Simulation`].
#[derive(Clone)]
pub struct SimulatedAuthorityStore<A = MemoryAuthorityStore> {
    state: Arc<Mutex<State>>,
    inner: Arc<A>,
}

/// Immutable-object backend handle controlled by one [`Simulation`].
#[derive(Clone)]
pub struct SimulatedObjectStore<O = MemoryObjectStore> {
    state: Arc<Mutex<State>>,
    inner: Arc<O>,
}

fn authority_fault(error: impl Into<String>) -> AuthorityFailure {
    AuthorityFailure::before_work(AuthorityStoreError::Rejected(error.into()))
}

fn object_fault(error: impl Into<String>) -> ObjectFailure {
    ObjectFailure::before_work(ObjectStoreError::Rejected(error.into()))
}

fn intercept_authority(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, AuthorityFailure> {
    let mut state = state.lock().map_err(|_| {
        AuthorityFailure::before_work(AuthorityStoreError::Corrupt(
            "simulation state is poisoned".to_owned(),
        ))
    })?;
    state
        .intercept(operation)
        .map(|(_, fault)| fault)
        .map_err(authority_fault)
}

fn intercept_object(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, ObjectFailure> {
    let mut state = state
        .lock()
        .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Corrupt))?;
    state
        .intercept(operation)
        .map(|(_, fault)| fault)
        .map_err(object_fault)
}

fn reject_authority(fault: Option<SimulationFault>) -> Result<(), AuthorityFailure> {
    match fault {
        Some(SimulationFault::RejectBefore) => Err(authority_fault("scheduled rejection")),
        Some(SimulationFault::PartitionBefore) => Err(authority_fault("scheduled partition")),
        _ => Ok(()),
    }
}

fn reject_object(fault: Option<SimulationFault>) -> Result<(), ObjectFailure> {
    match fault {
        Some(SimulationFault::RejectBefore) => Err(object_fault("scheduled rejection")),
        Some(SimulationFault::PartitionBefore) => Err(object_fault("scheduled partition")),
        _ => Ok(()),
    }
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for SimulatedAuthorityStore<A> {
    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityCreate)?;
        reject_authority(fault)?;
        self.inner
            .create_authority(authority_id, genesis_epoch, budget, cancellation)
            .await
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityHead)?;
        reject_authority(fault)?;
        if let Some(SimulationFault::StaleAuthorityHead(head)) = fault {
            let work = WorkCounters {
                authority_records_read: 1,
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| AuthorityFailure::before_work(error.into()))?;
            return Ok(crate::AuthorityReceipt { value: head, work });
        }
        self.inner.head(authority_id, budget, cancellation).await
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityAppend)?;
        reject_authority(fault)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            let fenced = self
                .inner
                .fence(authority_id, expected, budget, cancellation)
                .await?;
            let remaining = fenced
                .work
                .remaining(budget)
                .map_err(|error| AuthorityFailure::new(error.into(), fenced.work))?;
            let appended = self
                .inner
                .compare_and_append(
                    authority_id,
                    epoch,
                    expected,
                    commit,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    let nested = *failure.work;
                    match fenced.work.checked_add(nested) {
                        Ok(combined) => AuthorityFailure::new(failure.error, combined),
                        Err(error) => AuthorityFailure::new(error.into(), fenced.work),
                    }
                })?;
            let work = fenced
                .work
                .checked_add(appended.work)
                .map_err(|error| AuthorityFailure::new(error.into(), fenced.work))?;
            return Ok(crate::AuthorityReceipt {
                value: appended.value,
                work,
            });
        }
        let receipt = self
            .inner
            .compare_and_append(authority_id, epoch, expected, commit, budget, cancellation)
            .await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Indeterminate {
                    operation: "compare-and-append",
                    source: std::io::Error::other("scheduled acknowledgement loss"),
                },
                receipt.work,
            ));
        }
        Ok(receipt)
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityReplay)?;
        reject_authority(fault)?;
        let mut receipt = self
            .inner
            .replay(authority_id, after, limit, budget, cancellation)
            .await?;
        if matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay))
            && let Some(last) = receipt.value.last().cloned()
        {
            let copied = u64::try_from(last.payload.len()).unwrap_or(u64::MAX);
            let extra = WorkCounters {
                allocation_operations: 1,
                bytes_copied: copied,
                peak_allocation_bytes: copied,
                items_returned: 1,
                ..WorkCounters::default()
            };
            let work = receipt
                .work
                .checked_add(extra)
                .map_err(|error| AuthorityFailure::new(error.into(), receipt.work))?;
            work.verify(budget)
                .map_err(|error| AuthorityFailure::new(error.into(), receipt.work))?;
            receipt.value.push(last);
            receipt.work = work;
        }
        Ok(receipt)
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<crate::storage::FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityFence)?;
        reject_authority(fault)?;
        self.inner
            .fence(authority_id, expected, budget, cancellation)
            .await
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityFindOperation)?;
        reject_authority(fault)?;
        self.inner
            .find_operation(authority_id, operation_id, budget, cancellation)
            .await
    }
}

fn altered_read(
    mut receipt: ObjectReceipt<ObjectRead>,
    fault: Option<SimulationFault>,
    budget: WorkBudget,
) -> ObjectResult<ObjectRead> {
    if !matches!(
        fault,
        Some(SimulationFault::CorruptObjectRead | SimulationFault::PartialObjectRead)
    ) {
        return Ok(receipt);
    }
    if receipt.value.bytes.is_empty() {
        return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
    }
    let original = u64::try_from(receipt.value.bytes.len()).unwrap_or(u64::MAX);
    let retained = match fault {
        Some(SimulationFault::PartialObjectRead) => original.saturating_sub(1),
        _ => original,
    };
    let extra = WorkCounters {
        allocation_operations: 1,
        bytes_copied: retained,
        peak_allocation_bytes: retained,
        ..WorkCounters::default()
    };
    let work = receipt
        .work
        .checked_add(extra)
        .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
    work.verify(budget)
        .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
    let mut bytes = receipt.value.bytes.to_vec();
    if matches!(fault, Some(SimulationFault::PartialObjectRead)) {
        bytes.pop();
    } else if let Some(first) = bytes.first_mut() {
        *first ^= 0x80;
    }
    receipt.value = ObjectRead {
        bytes: Bytes::from(bytes),
        retention: ObjectReadRetention::Owned {
            logical_bytes: retained,
        },
    };
    receipt.work = work;
    Ok(receipt)
}

impl<O: AsyncObjectStore> AsyncObjectStore for SimulatedObjectStore<O> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = intercept_object(&self.state, SimulationOperation::ObjectPut)?;
        reject_object(fault)?;
        if matches!(fault, Some(SimulationFault::DelayObjectVisibility)) {
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let work = WorkCounters {
                object_probes: 1,
                backend_write_operations: 1,
                object_bytes_written: length,
                bytes_hashed: length.saturating_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            if object_digest(object_id.kind, &bytes) != object_id.digest {
                return Err(ObjectFailure::new(ObjectStoreError::DigestMismatch, work));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
            if let Some(existing) = state.pending_objects.get(&object_id) {
                if existing != &bytes {
                    return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
                }
                return Ok(ObjectReceipt { value: (), work });
            }
            let next_count = state.pending_objects.len().saturating_add(1);
            let next_bytes = state
                .pending_object_bytes
                .checked_add(length)
                .ok_or_else(|| {
                    ObjectFailure::new(ObjectStoreError::Work(crate::WorkError::Overflow), work)
                })?;
            if next_count > state.options.maximum_pending_objects
                || next_bytes > state.options.maximum_pending_object_bytes
            {
                return Err(ObjectFailure::new(
                    ObjectStoreError::Rejected(
                        "simulation delayed-object bound exhausted".to_owned(),
                    ),
                    work,
                ));
            }
            state.pending_objects.insert(object_id, bytes);
            state.pending_object_bytes = next_bytes;
            return Ok(ObjectReceipt { value: (), work });
        }
        self.inner.put(object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = intercept_object(&self.state, SimulationOperation::ObjectRead)?;
        reject_object(fault)?;
        altered_read(
            self.inner
                .read(object_id, maximum_bytes, budget, cancellation)
                .await?,
            fault,
            budget,
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = intercept_object(&self.state, SimulationOperation::ObjectReadMany)?;
        reject_object(fault)?;
        let mut receipt = self.inner.read_many(requests, budget, cancellation).await?;
        if matches!(
            fault,
            Some(SimulationFault::CorruptObjectRead | SimulationFault::PartialObjectRead)
        ) && let Some(first) = receipt.value.first_mut()
        {
            let altered = altered_read(
                ObjectReceipt {
                    value: first.clone(),
                    work: WorkCounters::default(),
                },
                fault,
                receipt
                    .work
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?,
            )?;
            receipt.work = receipt
                .work
                .checked_add(altered.work)
                .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
            *first = altered.value;
        }
        Ok(receipt)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = intercept_object(&self.state, SimulationOperation::ObjectContains)?;
        reject_object(fault)?;
        self.inner.contains(object_id, budget, cancellation).await
    }
}

#[cfg(test)]
#[path = "tests/simulation.rs"]
mod tests;
