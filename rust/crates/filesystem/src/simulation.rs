//! Deterministic bounded fault simulator for authority and immutable-object backends.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore, GenerationForkSource};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityStoreError, CreateAuthorityOutcome, ObjectFailure, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult, ObjectStoreError,
    PublicationReservation, ReplayLimit, ReservationOutcome, object_digest,
};
use acyclic_stream::OwnedBytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

type ReferenceAuthority = crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>;
type ReferenceObjects = crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

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
    /// Existing-only original publication reservation lookup.
    AuthorityReservation,
    /// Atomic publication reservation acquisition.
    AuthorityReserve,
    /// Exact publication reservation release.
    AuthorityRelease,
    /// Immutable-object admission.
    ObjectPut,
    /// One immutable-object read.
    ObjectRead,
    /// Ordered immutable-object batch read.
    ObjectReadMany,
    /// Immutable-object presence probe.
    ObjectContains,
}

const SIMULATION_OPERATION_COUNT: usize = SimulationOperation::ObjectContains as usize + 1;

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
    /// Accept a first record or append, then lose the provider acknowledgement.
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
            Self::AmbiguousAuthorityAppend => matches!(
                operation,
                SimulationOperation::AuthorityCreate
                    | SimulationOperation::AuthorityAppend
                    | SimulationOperation::AuthorityReserve
                    | SimulationOperation::AuthorityRelease
            ),
            Self::FenceBeforeAuthorityAppend => {
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
    occurrences: [u64; SIMULATION_OPERATION_COUNT],
    faults: BTreeMap<(SimulationOperation, u64), SimulationFault>,
    trace: Vec<SimulationTrace>,
    trace_allocation: KernelOwnedAllocation,
    pending_objects: BTreeMap<ObjectId, OwnedBytes>,
    pending_object_bytes: u64,
}

impl State {
    fn prepare_trace(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err(ObjectStoreError::Simulation("simulation trace bound exhausted").into());
        }
        if self.trace.len() < self.trace.capacity() {
            return Ok(());
        }
        let capacity = self
            .trace
            .capacity()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?
            .max(1)
            .min(self.options.maximum_trace_entries);
        let bytes = u64::try_from(
            capacity
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let copied = u64::try_from(
            self.trace
                .len()
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let delta = WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        let allocation = accounting.claim_owned(bytes, 1)?;
        let mut replacement = (Vec::new(), allocation);
        replacement
            .0
            .try_reserve_exact(capacity)
            .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual = u64::try_from(
            replacement
                .0
                .capacity()
                .checked_mul(std::mem::size_of::<SimulationTrace>())
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        accounting.reconcile_owned(&mut replacement.1, bytes, actual)?;
        accounting.admit(delta)?;
        replacement.0.extend_from_slice(&self.trace);
        accounting.record_completed(delta)?;
        let (trace, allocation) = replacement;
        self.trace = trace;
        self.trace_allocation = allocation;
        Ok(())
    }

    fn intercept(
        &mut self,
        operation: SimulationOperation,
    ) -> Result<Option<SimulationFault>, &'static str> {
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err("simulation trace bound exhausted");
        }
        let occurrence = self.occurrences[operation as usize]
            .checked_add(1)
            .ok_or("simulation operation occurrence overflow")?;
        self.occurrences[operation as usize] = occurrence;
        let fault = self.faults.remove(&(operation, occurrence));
        let replaces_trace = self.trace.len() == self.trace.capacity();
        self.trace
            .try_reserve(1)
            .map_err(|_| "simulation trace allocation failed")?;
        if replaces_trace {
            self.trace_allocation = KernelOwnedAllocation::Standalone;
        }
        self.trace.push(SimulationTrace {
            operation,
            occurrence,
            fault,
        });
        Ok(fault)
    }
}

/// Shared deterministic simulator and its two backend handles.
#[derive(Clone)]
pub struct Simulation<
    A = crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    O = crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
> {
    state: Arc<Mutex<State>>,
    authority: Arc<A>,
    objects: Arc<O>,
}

impl Simulation<ReferenceAuthority, ReferenceObjects> {
    /// Creates a bounded simulator over fresh deterministic memory primitives.
    pub fn new(options: SimulationOptions) -> Result<Self, SimulationError> {
        let (authority, objects) = reference_backends();
        Self::wrap(authority, objects, options)
    }
}

impl<A, O> Simulation<A, O> {
    /// Wraps explicit backends with one bounded deterministic schedule.
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
                occurrences: [0; SIMULATION_OPERATION_COUNT],
                faults: BTreeMap::new(),
                trace: Vec::new(),
                trace_allocation: KernelOwnedAllocation::Standalone,
                pending_objects: BTreeMap::new(),
                pending_object_bytes: 0,
            })),
            authority: Arc::new(authority),
            objects: Arc::new(objects),
        })
    }

    /// Returns independently cloneable authority and object handles sharing one schedule.
    #[must_use]
    pub fn stores(&self) -> (SimulatedStore<A>, SimulatedStore<O>) {
        (
            SimulatedStore {
                state: self.state.clone(),
                inner: self.authority.clone(),
            },
            SimulatedStore {
                state: self.state.clone(),
                inner: self.objects.clone(),
            },
        )
    }

    /// Schedules one exact one-based interception point.
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
    pub fn schedule_next(
        &self,
        operation: SimulationOperation,
        fault: SimulationFault,
    ) -> Result<ScheduledSimulationFault, SimulationError> {
        if !fault.compatible(operation) {
            return Err(SimulationError::IncompatibleFault);
        }
        let mut state = self.lock_state()?;
        let occurrence = state.occurrences[operation as usize]
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
    if state.occurrences[scheduled.operation as usize] >= scheduled.occurrence {
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
    pub async fn flush_object_visibility(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<u64> {
        self.flush_object_visibility_engine(SimulationWriteWork::Ordinary(&budget), cancellation)
            .await
            .map(|(value, work)| crate::OperationReceipt { value, work })
            .map_err(|(error, work)| ObjectFailure::new(error, work))
    }

    /// Publishes delayed aliases under the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn flush_object_visibility_with_native_work(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<u64, ObjectStoreError> {
        self.flush_object_visibility_engine(SimulationWriteWork::Native(work), cancellation)
            .await
            .map(|(value, _)| value)
            .map_err(|(error, _)| error)
    }

    async fn flush_object_visibility_engine(
        &self,
        accounting: SimulationWriteWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(u64, WorkCounters), (ObjectStoreError, WorkCounters)> {
        let mut work = WorkCounters::default();
        let mut published = 0_u64;
        #[cfg(not(target_arch = "wasm32"))]
        if let SimulationWriteWork::Native(native) = accounting {
            native
                .remaining()
                .map_err(|error| (error.into(), WorkCounters::UNCHARGED))?;
        }
        loop {
            cancellation
                .check()
                .map_err(|_| (ObjectStoreError::Cancelled, work))?;
            let next = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| (ObjectStoreError::Corrupt, work))?;
                state
                    .pending_objects
                    .first_key_value()
                    .map(|(object_id, bytes)| (*object_id, bytes.clone()))
            };
            let Some((object_id, bytes)) = next else {
                return Ok((published, work));
            };
            let completed = match accounting {
                SimulationWriteWork::Ordinary(budget) => {
                    let remaining = work
                        .remaining(*budget)
                        .map_err(|error| (error.into(), work))?;
                    ({ let __object_args = (object_id, bytes, remaining, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = self.objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
                        .map(|receipt| receipt.work)
                        .map_err(|failure| (failure.error, *failure.work))
                }
                #[cfg(not(target_arch = "wasm32"))]
                SimulationWriteWork::Native(native) => ({ let __object_args = (object_id, bytes, native, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = self
                    .objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
                    .map(|()| WorkCounters::UNCHARGED)
                    .map_err(|error| (error, WorkCounters::UNCHARGED)),
            }
            .map_err(|(error, nested)| match work.checked_add(nested) {
                Ok(combined) => (error, combined),
                Err(error) => (error.into(), work),
            })?;
            work = work
                .checked_add(completed)
                .map_err(|error| (error.into(), work))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| (ObjectStoreError::Corrupt, work))?;
            if let Some(removed) = state.pending_objects.remove(&object_id) {
                state.pending_object_bytes = state
                    .pending_object_bytes
                    .checked_sub(u64::try_from(removed.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| (ObjectStoreError::Corrupt, work))?;
            }
            published = published
                .checked_add(1)
                .ok_or_else(|| (ObjectStoreError::Work(crate::WorkError::Overflow), work))?;
        }
    }
}

impl Default for Simulation {
    fn default() -> Self {
        Self::new(SimulationOptions::default()).expect("positive default simulation bounds")
    }
}

/// Backend handle controlled by one [`Simulation`].
#[derive(Clone)]
pub struct SimulatedStore<T> {
    state: Arc<Mutex<State>>,
    inner: Arc<T>,
}

#[derive(Clone, Copy)]
enum SimulationWriteWork<'a> {
    Ordinary(&'a WorkBudget),
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a crate::materializer::NativeMaterializationWork),
}

impl SimulationWriteWork<'_> {
    fn simulation_error(self, cause: &'static str) -> ObjectStoreError {
        match self {
            Self::Ordinary(_) => ObjectStoreError::Rejected(cause.to_owned()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => ObjectStoreError::Simulation(cause),
        }
    }

    fn charge(self, delta: WorkCounters) -> Result<WorkCounters, (ObjectStoreError, WorkCounters)> {
        match self {
            Self::Ordinary(budget) => {
                delta
                    .verify(*budget)
                    .map_err(|error| (error.into(), WorkCounters::UNCHARGED))?;
                Ok(delta)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(work) => {
                work.charge(delta)
                    .map_err(|error| (error.into(), WorkCounters::UNCHARGED))?;
                Ok(WorkCounters::default())
            }
        }
    }
}

enum SimulationWrite {
    Plain(ObjectId, OwnedBytes),
    Hashed(crate::storage::HashedObject),
}

impl SimulationWrite {
    fn parts(&self) -> (ObjectId, &OwnedBytes, bool) {
        match self {
            Self::Plain(id, bytes) => (*id, bytes, false),
            Self::Hashed(object) => (object.object_id(), object.bytes(), true),
        }
    }

    fn into_parts(self) -> (ObjectId, OwnedBytes) {
        match self {
            Self::Plain(id, bytes) => (id, bytes),
            Self::Hashed(object) => object.into_parts(),
        }
    }
}

impl<O: AsyncObjectStore> SimulatedStore<O> {
    async fn put_engine(
        &self,
        object: SimulationWrite,
        accounting: SimulationWriteWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, (ObjectStoreError, WorkCounters)> {
        cancellation
            .check()
            .map_err(|_| (ObjectStoreError::Cancelled, WorkCounters::UNCHARGED))?;
        let fault = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| (ObjectStoreError::Corrupt, WorkCounters::UNCHARGED))?;
            #[cfg(not(target_arch = "wasm32"))]
            if let SimulationWriteWork::Native(work) = accounting {
                state
                    .prepare_trace(&mut KernelAccounting::native(work, work.cancellation()))
                    .map_err(|error| {
                        (
                            crate::async_storage::object_error(error),
                            WorkCounters::UNCHARGED,
                        )
                    })?;
            }
            state
                .intercept(SimulationOperation::ObjectPut)
                .map_err(|cause| (accounting.simulation_error(cause), WorkCounters::UNCHARGED))?
        };
        match fault {
            Some(SimulationFault::RejectBefore) => {
                return Err((
                    accounting.simulation_error("scheduled rejection"),
                    WorkCounters::UNCHARGED,
                ));
            }
            Some(SimulationFault::PartitionBefore) => {
                return Err((
                    accounting.simulation_error("scheduled partition"),
                    WorkCounters::UNCHARGED,
                ));
            }
            _ => {}
        }
        if matches!(fault, Some(SimulationFault::DelayObjectVisibility)) {
            let (object_id, bytes, hashed) = object.parts();
            let length = u64::try_from(bytes.len())
                .map_err(|_| (crate::WorkError::Overflow.into(), WorkCounters::UNCHARGED))?;
            let hash_bytes = if hashed {
                0
            } else {
                length
                    .checked_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES)
                    .ok_or_else(|| (crate::WorkError::Overflow.into(), WorkCounters::UNCHARGED))?
            };
            let work = accounting.charge(WorkCounters {
                bytes_hashed: hash_bytes,
                ..WorkCounters::default()
            })?;
            if !hashed && object_digest(object_id.kind, bytes) != object_id.digest {
                return Err((ObjectStoreError::DigestMismatch, work));
            }
            cancellation
                .check()
                .map_err(|_| (ObjectStoreError::Cancelled, work))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| (ObjectStoreError::Corrupt, work))?;
            if let Some(existing) = state.pending_objects.get(&object_id) {
                if existing != bytes {
                    return Err((ObjectStoreError::Corrupt, work));
                }
                return Ok(work);
            }
            let next_count = state
                .pending_objects
                .len()
                .checked_add(1)
                .ok_or_else(|| (crate::WorkError::Overflow.into(), work))?;
            let next_bytes = state
                .pending_object_bytes
                .checked_add(length)
                .ok_or_else(|| (crate::WorkError::Overflow.into(), work))?;
            if next_count > state.options.maximum_pending_objects
                || next_bytes > state.options.maximum_pending_object_bytes
            {
                return Err((
                    accounting.simulation_error("simulation delayed-object bound exhausted"),
                    work,
                ));
            }
            let (_, bytes) = object.into_parts();
            state.pending_objects.insert(object_id, bytes);
            state.pending_object_bytes = next_bytes;
            return Ok(work);
        }
        match accounting {
            SimulationWriteWork::Ordinary(budget) => match object {
                SimulationWrite::Plain(id, bytes) => {
                    ({ let __object_args = (id, bytes, *budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = self.inner.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
                }
                SimulationWrite::Hashed(object) => {
                    ({ let __object_args = (object, *budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.put_hashed(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
                }
            }
            .map(|receipt| receipt.work)
            .map_err(|failure| (failure.error, *failure.work)),
            #[cfg(not(target_arch = "wasm32"))]
            SimulationWriteWork::Native(work) => {
                let result = match object {
                    SimulationWrite::Plain(id, bytes) => {
                        ({ let __object_args = (id, bytes, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = self.inner.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
                    }
                    SimulationWrite::Hashed(object) => {
                        ({ let __object_args = (object, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self.inner.put_hashed(__object_args.0, &mut __object_work).await; __object_result })
                    }
                };
                result
                    .map(|()| WorkCounters::UNCHARGED)
                    .map_err(|error| (error, WorkCounters::UNCHARGED))
            }
        }
    }

    fn read_fault(
        &self,
        operation: SimulationOperation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<SimulationFault>, crate::FsError> {
        accounting.check()?;
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        state.prepare_trace(accounting)?;
        let fault = state
            .intercept(operation)
            .map_err(ObjectStoreError::Simulation)?;
        match fault {
            Some(SimulationFault::RejectBefore) => {
                Err(ObjectStoreError::Simulation("scheduled rejection").into())
            }
            Some(SimulationFault::PartitionBefore) => {
                Err(ObjectStoreError::Simulation("scheduled partition").into())
            }
            _ => Ok(fault),
        }
    }

    async fn read_engine(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectRead, crate::FsError> {
        let fault = self.read_fault(SimulationOperation::ObjectRead, accounting)?;
        let mut value = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                ({ let __object_args = (object_id, maximum_bytes, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = self.inner.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })?
            }
            _ => match ({ let __object_args = (object_id, maximum_bytes, accounting.remaining()?, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = self
                .inner.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
            {
                Ok(receipt) => {
                    crate::async_storage::merge_completed_object_read(
                        accounting,
                        receipt.work,
                        object_read_retained_bytes(&receipt.value),
                    )?;
                    receipt.value
                }
                Err(failure) => {
                    crate::async_storage::merge_completed_object_read(
                        accounting,
                        *failure.work,
                        0,
                    )?;
                    return Err(failure.error.into());
                }
            },
        };
        if let Some(altered) = altered_read_data(&value, fault, accounting)? {
            value = altered;
        }
        Ok(value)
    }

    async fn read_many_engine(
        &self,
        requests: &[ObjectReadRequest],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<ObjectRead>, KernelOwnedAllocation), crate::FsError> {
        let fault = self.read_fault(SimulationOperation::ObjectReadMany, accounting)?;
        // Keep the vector before its guard throughout fallible fault injection.
        let mut values = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let (reads, allocation) = ({ let __object_args = (requests, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self
                    .inner.read_many(__object_args.0, &mut __object_work).await.map(|(values, allocation)| (values, allocation.expect("native batch allocation"))); __object_result })?;
                (reads, KernelOwnedAllocation::NativeOriginal(allocation))
            }
            _ => match ({ let __object_args = (requests, accounting.remaining()?, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self
                .inner.read_many(__object_args.0, &mut __object_work).await.map(|(values, _allocation)| values); __object_work.receipt(__object_result) })
            {
                Ok(receipt) => {
                    let retained = receipt.value.iter().try_fold(
                        u64::try_from(
                            receipt
                                .value
                                .capacity()
                                .checked_mul(std::mem::size_of::<ObjectRead>())
                                .ok_or(crate::WorkError::Overflow)?,
                        )
                        .map_err(|_| crate::WorkError::Overflow)?,
                        |bytes, read| {
                            bytes
                                .checked_add(object_read_retained_bytes(read))
                                .ok_or(crate::WorkError::Overflow)
                        },
                    )?;
                    crate::async_storage::merge_completed_object_read(
                        accounting,
                        receipt.work,
                        retained,
                    )?;
                    (receipt.value, KernelOwnedAllocation::Standalone)
                }
                Err(failure) => {
                    crate::async_storage::merge_completed_object_read(
                        accounting,
                        *failure.work,
                        0,
                    )?;
                    return Err(failure.error.into());
                }
            },
        };
        if let Some(first) = values.0.first_mut()
            && let Some(altered) = altered_read_data(first, fault, accounting)?
        {
            *first = altered;
        }
        Ok(values)
    }
}

fn reference_backends() -> (ReferenceAuthority, ReferenceObjects) {
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    (
        crate::distributed::StreamAuthorityStore::new(streams),
        crate::LogicalObjectStore::new(Arc::new(objects), bucket),
    )
}

fn object_fault(error: &'static str) -> ObjectFailure {
    ObjectFailure::before_work(ObjectStoreError::Rejected(error.into()))
}

fn select_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state
        .lock()
        .map_err(|_| AuthorityStoreError::CorruptStatic("simulation state is poisoned"))?;
    state
        .intercept(operation)
        .map_err(AuthorityStoreError::RejectedStatic)
}

#[cfg(not(target_arch = "wasm32"))]
fn select_native_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
    original: &crate::NativeMaterializationWork,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state
        .lock()
        .map_err(|_| AuthorityStoreError::CorruptStatic("simulation state is poisoned"))?;
    let mut accounting = KernelAccounting::native(original, original.cancellation());
    state
        .prepare_trace(&mut accounting)
        .map_err(|error| match error {
            crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
            crate::FsError::Work(error) => AuthorityStoreError::Work(error),
            crate::FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
            crate::FsError::Object(ObjectStoreError::MemoryTableAllocationFailed(error)) => {
                AuthorityStoreError::NativeOriginal(
                    crate::NativeTreeMaterializationError::AllocationFailed(error),
                )
            }
            crate::FsError::Object(ObjectStoreError::Simulation(reason)) => {
                AuthorityStoreError::RejectedStatic(reason)
            }
            _ => AuthorityStoreError::CorruptStatic("simulation trace ownership is invalid"),
        })?;
    state
        .intercept(operation)
        .map_err(AuthorityStoreError::RejectedStatic)
}

fn lost_authority_acknowledgement(operation: &'static str) -> AuthorityStoreError {
    AuthorityStoreError::Indeterminate {
        operation,
        source: std::io::ErrorKind::ConnectionReset.into(),
    }
}

fn check_authority_fault(fault: Option<SimulationFault>) -> Result<(), AuthorityStoreError> {
    match fault {
        Some(SimulationFault::RejectBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled rejection"))
        }
        Some(SimulationFault::PartitionBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled partition"))
        }
        _ => Ok(()),
    }
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for SimulatedStore<A> {
    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() && guard.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        let fault = if let Some(original) = work.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
            select_native_authority_fault(
                &self.state,
                SimulationOperation::AuthorityCreate,
                original,
            )?
        } else {
            select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?
        };
        #[cfg(target_arch = "wasm32")]
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .create_first_record(authority, commit, work, guard.as_deref_mut())
            .await?;
        if value && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(
                lost_authority_acknowledgement("create-authority-with-first-record").into(),
            );
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        let fault = if let Some(original) = work.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
            select_native_authority_fault(
                &self.state,
                SimulationOperation::AuthorityAppend,
                original,
            )?
        } else {
            select_authority_fault(&self.state, SimulationOperation::AuthorityAppend)?
        };
        #[cfg(target_arch = "wasm32")]
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityAppend)?;
        check_authority_fault(fault)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            self.inner
                .fence(request.authority_id, request.expected, work)
                .await?;
        }
        let value = self
            .inner
            .compare_and_append(request, work, guard.as_deref_mut())
            .await?;
        if matches!(
            &value.0,
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(lost_authority_acknowledgement("compare-and-append").into());
        }
        Ok(value)
    }
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn fork_authority<G: crate::AuthorityGuard>(
        &self,
        source: GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .fork_authority(source, destination, operation, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReservation)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .publication_reservation(authority, operation, work)
            .await?;
        Ok(value)
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReserve)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .reserve_publication(authority, expected, operation, work)
            .await?;
        if matches!(
            value,
            ReservationOutcome::Reserved(_) | ReservationOutcome::AlreadyReserved(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(AuthorityStoreError::Indeterminate {
                operation: "reserve-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            });
        }
        Ok(value)
    }
    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityRelease)?;
        check_authority_fault(fault)?;
        let value = self.inner.release_publication(reservation, work).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(AuthorityStoreError::Indeterminate {
                operation: "release-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            });
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityHead)?;
        check_authority_fault(fault)?;
        if let Some(SimulationFault::StaleAuthorityHead(head)) = fault {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                return Ok(head);
            }
            let delta = WorkCounters {
                authority_records_read: 1,
                ..WorkCounters::UNCHARGED
            };
            delta
                .verify(work.remaining()?)
                .map_err(AuthorityStoreError::from)?;
            work.record(delta)?;
            return Ok(head);
        }
        self.inner.head(authority, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityFence)?;
        check_authority_fault(fault)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityFindOperation)?;
        check_authority_fault(fault)?;
        self.inner.find_operation(authority, operation, work).await
    }
    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;

            use crate::materializer::NativeTreeMaterializationError;
            cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            work.admit(WorkCounters::UNCHARGED)?;
            let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReplay)?;
            check_authority_fault(fault)?;
            let (mut records, custody) = self
                .inner
                .replay(authority_id, after, limit, context)
                .await?;
            let mut custody = custody.expect("native replay allocation");
            if !matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay))
                || records.is_empty()
            {
                return Ok((records, Some(custody)));
            }

            let needs_growth = records.len() == records.capacity();
            let copied_records = if needs_growth {
                records.len().checked_add(1)
            } else {
                Some(1)
            }
            .ok_or(NativeTreeMaterializationError::Work(
                crate::WorkError::Overflow,
            ))?;
            let extra = WorkCounters {
                bytes_copied: copied_records
                    .checked_mul(std::mem::size_of::<DurableCommit>())
                    .ok_or(NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ))? as u64,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(extra)?;
            if needs_growth {
                let count =
                    records
                        .len()
                        .checked_add(1)
                        .ok_or(NativeTreeMaterializationError::Work(
                            crate::WorkError::Overflow,
                        ))?;
                let old_bytes = records
                    .capacity()
                    .checked_mul(std::mem::size_of::<DurableCommit>())
                    .ok_or(NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ))? as u64;
                let (mut replacement, mut replacement_custody) =
                    work.allocations(|ledger, counters, budget, cancellation| {
                        cancellation
                            .check()
                            .map_err(|_| AuthorityStoreError::Cancelled)?;
                        counters
                            .checked_add(extra)
                            .and_then(|projected| projected.verify(budget))
                            .map_err(NativeTreeMaterializationError::Work)?;
                        ledger
                            .claim_elements::<DurableCommit>(count, counters, budget)
                            .map_err(|error| {
                                error.into_work_or(NativeTreeMaterializationError::Work(
                                    crate::WorkError::Overflow,
                                ))
                            })?;
                        let mut replacement = Vec::new();
                        replacement
                            .try_reserve_exact(count)
                            .map_err(NativeTreeMaterializationError::AllocationFailed)?;
                        Ok::<_, AuthorityStoreError>(replacement)
                    })?;
                replacement.append(&mut records);
                drop(records);
                let moved = WorkCounters {
                    bytes_copied: extra.bytes_copied - std::mem::size_of::<DurableCommit>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                let transferred = work
                    .record_admitted(moved)
                    .and_then(|()| custody.release(old_bytes))
                    .and_then(|()| custody.absorb(&mut replacement_custody));
                if let Err(error) = transferred {
                    // The buffer must retire before either unchanged failure guard.
                    drop(replacement);
                    return Err(error.into());
                }
                records = replacement;
            }
            let last = records.last().expect("nonempty replay page").clone();
            records.push(last);
            work.record_admitted(WorkCounters {
                bytes_copied: std::mem::size_of::<DurableCommit>() as u64,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
            return Ok((records, Some(custody)));
        }
        context
            .cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReplay)?;
        check_authority_fault(fault)?;
        let (mut records, allocation) = self
            .inner
            .replay(authority_id, after, limit, context)
            .await?;
        if matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay))
            && let Some(last) = records.last().cloned()
        {
            let copied = u64::try_from(last.payload.len()).unwrap_or(u64::MAX);
            let extra = WorkCounters {
                allocation_operations: 1,
                bytes_copied: copied,
                peak_allocation_bytes: copied,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            };
            extra
                .verify(context.remaining()?)
                .map_err(AuthorityStoreError::from)?;
            records.push(last);
            context.record(extra)?;
        }
        Ok((records, allocation))
    }
}

fn object_read_retained_bytes(read: &ObjectRead) -> u64 {
    match read.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    }
}

fn altered_read_data(
    read: &ObjectRead,
    fault: Option<SimulationFault>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<ObjectRead>, crate::FsError> {
    if !matches!(
        fault,
        Some(SimulationFault::CorruptObjectRead | SimulationFault::PartialObjectRead,)
    ) {
        return Ok(None);
    }
    if read.bytes.is_empty() {
        return Err(ObjectStoreError::Corrupt.into());
    }
    accounting.check()?;
    let retained = if matches!(fault, Some(SimulationFault::PartialObjectRead)) {
        read.bytes.len() - 1
    } else {
        read.bytes.len()
    };
    let requested = u64::try_from(retained).map_err(|_| crate::WorkError::Overflow)?;
    accounting.admit(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    let allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let mut bytes = (Vec::new(), allocation);
    bytes
        .0
        .try_reserve_exact(retained)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual = u64::try_from(bytes.0.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut bytes.1, requested, actual)?;
    accounting.admit(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    bytes.0.extend_from_slice(&read.bytes[..retained]);
    accounting.record_completed(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    if matches!(fault, Some(SimulationFault::CorruptObjectRead))
        && let Some(first) = bytes.0.first_mut()
    {
        *first ^= 0x80;
    }
    let logical_bytes =
        u64::try_from(bytes.0.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    let (bytes, allocation) = bytes;
    let bytes = match allocation {
        KernelOwnedAllocation::Standalone => OwnedBytes::from(bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            let original = accounting.native_work().ok_or(ObjectStoreError::Corrupt)?;
            OwnedBytes::from_vec_with_guard(bytes, allocation, original.runtime_meter()).map_err(
                |failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        crate::FsError::from(original.runtime_error(error))
                    }
                    _ => ObjectStoreError::Corrupt.into(),
                },
            )?
        }
    };
    Ok(Some(ObjectRead {
        bytes,
        retention: ObjectReadRetention::Owned { logical_bytes },
    }))
}

impl<O: AsyncObjectStore> AsyncObjectStore for SimulatedStore<O> {
    fn collection(&self) -> Option<&std::sync::Arc<crate::Collection>> {
        self.inner.collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner.collection_sweeps()
    }

    async fn historical_generation_evidence(&self, authority_id: crate::AuthorityId, record: &crate::DurableCommit, maximum_bytes: u64, maximum_objects: u64, context: &mut crate::AuthorityWork<'_>) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        ({ let __object_args = (authority_id, record, maximum_bytes, maximum_objects, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.4, __object_args.5); let __object_result = self.inner.historical_generation_evidence(__object_args.0, __object_args.1, __object_args.2, __object_args.3, &mut __object_work).await; __object_work.receipt(__object_result) })
     }.await;
        context.accept(result)
    }

    async fn put(&self, object_id: ObjectId, bytes: OwnedBytes, context: &mut crate::AuthorityWork<'_>) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        self.put_engine(
            SimulationWrite::Plain(object_id, bytes),
            SimulationWriteWork::Native(work),
            cancellation,
        )
        .await
        .map(|_| ())
        .map_err(|(error, _)| error)
     }.await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        self.put_engine(
            SimulationWrite::Plain(object_id, bytes),
            SimulationWriteWork::Ordinary(&budget),
            cancellation,
        )
        .await
        .map(|work| crate::OperationReceipt { value: (), work })
        .map_err(|(error, work)| ObjectFailure::new(error, work))
     }.await;
        context.accept(result)
    }

    async fn put_many(&self, writes: &[crate::storage::ObjectWrite], context: &mut crate::AuthorityWork<'_>) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        work.remaining()?;
        ({ let __object_args = (writes, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self.inner.put_many(__object_args.0, &mut __object_work).await; __object_result })
     }.await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ({ let __object_args = (writes, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.put_many(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
     }.await;
        context.accept(result)
    }

    async fn put_hashed(&self, object: crate::storage::HashedObject, context: &mut crate::AuthorityWork<'_>) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        self.put_engine(
            SimulationWrite::Hashed(object),
            SimulationWriteWork::Native(work),
            cancellation,
        )
        .await
        .map(|_| ())
        .map_err(|(error, _)| error)
     }.await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        self.put_engine(
            SimulationWrite::Hashed(object),
            SimulationWriteWork::Ordinary(&budget),
            cancellation,
        )
        .await
        .map(|work| crate::OperationReceipt { value: (), work })
        .map_err(|(error, work)| ObjectFailure::new(error, work))
     }.await;
        context.accept(result)
    }







    async fn flush_before_publish(&self, scope: crate::async_storage::PublicationScope<'_>, context: &mut crate::AuthorityWork<'_>) -> Result<crate::PublicationHold, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        work.remaining()?;
        ({ let __object_args = (scope, work, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self.inner.flush_before_publish(__object_args.0, &mut __object_work).await; __object_result })
     }.await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        ({ let __object_args = (scope, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.flush_before_publish(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
     }.await;
        context.accept(result)
    }







    async fn read(&self, object_id: ObjectId, maximum_bytes: u64, context: &mut crate::AuthorityWork<'_>) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let read = self
            .read_engine(object_id, maximum_bytes, &mut accounting, cancellation)
            .await
            .map_err(crate::async_storage::object_error)?;
        match read.retention {
            ObjectReadRetention::Shared => Ok(read),
            ObjectReadRetention::Owned { .. } => crate::async_storage::owned_read(read, work),
        }
     }.await;
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .read_engine(object_id, maximum_bytes, &mut accounting, cancellation)
            .await;
        let work = accounting.spent();
        match result {
            Ok(value) => Ok(crate::OperationReceipt { value, work }),
            Err(error) => Err(ObjectFailure::new(
                crate::async_storage::object_error(error),
                work,
            )),
        }
     }.await;
        context.accept(result)
    }



    async fn read_many(&self, requests: &[ObjectReadRequest], context: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (reads, allocation) = self
            .read_many_engine(requests, &mut accounting, cancellation)
            .await
            .map_err(crate::async_storage::object_error)?;
        match allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                crate::async_storage::native_read_batch(reads, allocation)
            }
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
     }.await.map(|(values, allocation)| (values, Some(allocation)));
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .read_many_engine(requests, &mut accounting, cancellation)
            .await;
        let work = accounting.spent();
        match result {
            Ok((value, _)) => Ok(crate::OperationReceipt { value, work }),
            Err(error) => Err(ObjectFailure::new(
                crate::async_storage::object_error(error),
                work,
            )),
        }
     }.await;
        context.accept(result).map(|values| (values, None))
    }



    async fn contains(&self, object_id: ObjectId, context: &mut crate::AuthorityWork<'_>) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Corrupt))?;
            state
                .intercept(SimulationOperation::ObjectContains)
                .map_err(object_fault)?
        };
        match fault {
            Some(SimulationFault::RejectBefore) => return Err(object_fault("scheduled rejection")),
            Some(SimulationFault::PartitionBefore) => {
                return Err(object_fault("scheduled partition"));
            }
            _ => {}
        }
        ({ let __object_args = (object_id, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.contains(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
     }.await;
        context.accept(result)
    }
}

#[cfg(test)]
#[path = "tests/simulation/mod.rs"]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_write_controls {
    use super::*;
    use crate::materializer::NativeMaterializationWork;
    use crate::storage::{HashedObject, ObjectKind};

    fn budget() -> WorkBudget {
        let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget encoding");
        for field in value.as_object_mut().expect("budget fields").values_mut() {
            *field = serde_json::Value::from(1_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    #[tokio::test]
    async fn native_read_trace_denial_and_cancellation_precede_interceptor_and_backend() {
        let memory = crate::memory::MemoryObjectStore::default();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        ({ let __object_args = (object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = crate::async_storage::AsyncObjectStore::put(&memory, __object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
        .expect("seed memory");
        let simulation =
            Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
                .expect("bounded simulation");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 1,
                ..budget()
            },
            &cancellation,
        )
        .expect("only actual accounting constructor");
        let before = work.receipt();
        assert!(
            ({ let __object_args = (object.object_id(), 4, &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert_eq!(work.receipt(), before);
        assert!(simulation.trace().expect("trace").is_empty());
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            ({ let __object_args = (object.object_id(), 4, &work, &cancelled,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_result }),
            Err(ObjectStoreError::Cancelled)
        ));
        assert_eq!(work.receipt(), before);
    }

    #[tokio::test]
    async fn native_read_partial_copy_keeps_original_capacity_until_last_byte_alias() {
        let memory = crate::memory::MemoryObjectStore::default();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        ({ let __object_args = (object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = crate::async_storage::AsyncObjectStore::put(&memory, __object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
        .expect("seed memory");
        let simulation =
            Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
                .expect("bounded simulation");
        simulation
            .schedule_next(
                SimulationOperation::ObjectRead,
                SimulationFault::PartialObjectRead,
            )
            .expect("partial fault");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
        let baseline = work.live_allocation_bytes();
        let read = ({ let __object_args = (object.object_id(), 4, &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
            .expect("actual partial SDK copy");
        assert_eq!(read.bytes.as_ref(), b"bod");
        let alias = read.bytes.clone();
        let held = work.live_allocation_bytes();
        drop(read);
        assert_eq!(work.live_allocation_bytes(), held);
        drop(alias);
        assert!(work.live_allocation_bytes() + 3 <= held);
        drop(store);
        drop(simulation);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[tokio::test]
    async fn native_read_batch_preserves_one_real_memory_frontier_with_fault_custody() {
        let memory = crate::memory::MemoryObjectStore::default();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        ({ let __object_args = (object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = crate::async_storage::AsyncObjectStore::put(&memory, __object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
        .expect("seed memory");
        let simulation =
            Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
                .expect("bounded simulation");
        simulation
            .schedule_next(
                SimulationOperation::ObjectReadMany,
                SimulationFault::CorruptObjectRead,
            )
            .expect("batch fault");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
        let baseline = work.live_allocation_bytes();
        let requests = [ObjectReadRequest {
            object_id: object.object_id(),
            maximum_bytes: 4,
        }; 2];
        let batch = ({ let __object_args = (&requests, &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.read_many(__object_args.0, &mut __object_work).await.map(|(values, allocation)| (values, allocation.expect("native batch allocation"))); __object_result })
            .expect("one genuine batch");
        assert_eq!(batch.0.len(), 2);
        assert_eq!(batch.0[0].bytes[0], b'b' ^ 0x80);
        assert_eq!(batch.0[1].bytes.as_ref(), b"body");
        assert_eq!(work.receipt().object_probes, 2);
        drop(batch);
        drop(store);
        drop(simulation);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[tokio::test]
    async fn native_delayed_visibility_preserves_intrinsic_alias_custody() {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut custody = work
            .claim(4, 1, crate::WorkCounters::UNCHARGED)
            .expect("payload capacity");
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(4).expect("payload buffer");
        bytes.extend_from_slice(b"body");
        let object = HashedObject::from_native_vec(
            ObjectKind::TreePage,
            &mut bytes,
            &mut custody,
            &work,
            &cancellation,
        )
        .expect("intrinsic byte owner");
        let alias = object.bytes().clone();
        let payload_live = work.live_allocation_bytes();
        let hashed = work.receipt().bytes_hashed;
        let simulation = Simulation::default();
        simulation
            .schedule_next(
                SimulationOperation::ObjectPut,
                SimulationFault::DelayObjectVisibility,
            )
            .expect("delay schedule");
        let (_, store) = simulation.stores();
        ({ let __object_args = (object, &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await; __object_result })
            .expect("delayed admission");
        assert_eq!(work.receipt().bytes_hashed, hashed);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(
            simulation.state.lock().expect("state").pending_object_bytes,
            4
        );
        assert!(work.live_allocation_bytes() >= payload_live);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let before = work.receipt();
        assert!(matches!(
            simulation
                .flush_object_visibility_with_native_work(&work, &cancelled)
                .await,
            Err(ObjectStoreError::Cancelled)
        ));
        assert_eq!(work.receipt(), before);
        assert_eq!(
            simulation.state.lock().expect("state").pending_object_bytes,
            4
        );
        drop(store);
        drop(simulation);
        assert_eq!(work.live_allocation_bytes(), payload_live);
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[tokio::test]
    async fn native_put_trace_denial_precedes_interceptor_and_provider_effects() {
        let cancellation = CancellationToken::new();
        let mut denied = budget();
        denied.allocation_operations = 1;
        let work = NativeMaterializationWork::new(denied, &cancellation).expect("ledger only");
        let simulation = Simulation::default();
        simulation
            .schedule_next(
                SimulationOperation::ObjectPut,
                SimulationFault::DelayObjectVisibility,
            )
            .expect("delay schedule");
        let (_, store) = simulation.stores();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        let before = work.receipt();
        assert!(
            ({ let __object_args = (object.object_id(), object.bytes().clone(), &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert_eq!(work.receipt(), before);
        let state = simulation.state.lock().expect("state");
        assert!(state.trace.is_empty());
        assert!(state.occurrences.iter().all(|occurrence| *occurrence == 0));
        assert!(state.pending_objects.is_empty());
        assert_eq!(state.faults.len(), 1);
        drop(state);
        assert!(
            ({ let __object_args = (object, &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert_eq!(work.receipt(), before);
    }

    #[tokio::test]
    async fn native_scheduled_rejection_does_not_hash_or_write_payload() {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let simulation = Simulation::default();
        simulation
            .schedule_next(
                SimulationOperation::ObjectPut,
                SimulationFault::RejectBefore,
            )
            .expect("rejection schedule");
        let (_, store) = simulation.stores();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        assert!(matches!(
            ({ let __object_args = (object.object_id(), object.bytes().clone(), &work, &cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_result }),
            Err(ObjectStoreError::Simulation("scheduled rejection"))
        ));
        assert_eq!(work.receipt().bytes_hashed, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(
            simulation
                .state
                .lock()
                .expect("state")
                .pending_objects
                .is_empty()
        );
    }
    #[tokio::test]
    async fn native_write_hooks_preserve_original_cancellation_before_effects() {
        let original = CancellationToken::new();
        let argument = CancellationToken::new();
        let budget = budget();
        let work = crate::materializer::NativeMaterializationWork::new(budget, &original)
            .expect("original ledger");
        original.cancel();
        let before = work.receipt();
        let simulation = Simulation::default();
        let (_, store) = simulation.stores();
        let object = crate::storage::HashedObject::new(
            crate::storage::ObjectKind::TreePage,
            OwnedBytes::from_static(b"body"),
        );
        assert!(
            ({ let __object_args = (object.object_id(), object.bytes().clone(), &work, &argument,); let mut __object_work = crate::AuthorityWork::native(__object_args.2, __object_args.3); let __object_result = store.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert!(
            ({ let __object_args = (object.clone(), &work, &argument,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await; __object_result })
                .is_err()
        );
        let writes = [crate::storage::ObjectWrite {
            object_id: object.object_id(),
            bytes: object.bytes().clone(),
        }];
        assert!(
            ({ let __object_args = (&writes, &work, &argument,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.put_many(__object_args.0, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert!(
            ({ let __object_args = (crate::async_storage::PublicationScope::Everything, &work, &argument,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = store.flush_before_publish(__object_args.0, &mut __object_work).await; __object_result })
                .is_err()
        );
        assert!(
            simulation
                .flush_object_visibility_with_native_work(&work, &argument)
                .await
                .is_err()
        );
        assert_eq!(work.receipt(), before);
        let state = simulation.state.lock().expect("state");
        assert!(state.trace.is_empty());
        assert!(state.pending_objects.is_empty());
    }
}
