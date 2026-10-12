//! Deterministic bounded fault simulator for authority and immutable-object backends.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore, GenerationForkSource, StorageFuture};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityResult, AuthorityStoreError, CreateAuthorityOutcome,
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError, PublicationReservation, ReplayLimit, ReservationOutcome, object_digest,
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
                operation, SimulationOperation::AuthorityCreate | SimulationOperation::AuthorityAppend
                    | SimulationOperation::AuthorityReserve | SimulationOperation::AuthorityRelease
            ),
            Self::FenceBeforeAuthorityAppend => matches!(operation, SimulationOperation::AuthorityAppend),
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
    #[cfg(not(target_arch = "wasm32"))]
    trace_native: Option<crate::materializer::NativeAllocation>,
    pending_objects: BTreeMap<ObjectId, OwnedBytes>,
    pending_object_bytes: u64,
}

impl State {
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_native_trace(
        &mut self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), ObjectStoreError> {
        let mut accounting = KernelAccounting::native(
            work, work.cancellation(),
        );
        self.prepare_trace(&mut accounting).map_err(crate::async_storage::sequential_read_error)
    }

    fn prepare_trace(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err(ObjectStoreError::Simulation("simulation trace bound exhausted").into());
        }
        if self.trace.len() < self.trace.capacity() { return Ok(()); }
        let capacity = self.trace.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let bytes = u64::try_from(capacity.checked_mul(std::mem::size_of::<SimulationTrace>())
            .ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?;
        let copied = u64::try_from(self.trace.len().checked_mul(std::mem::size_of::<SimulationTrace>())
            .ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?;
        let delta = WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        let allocation = accounting.claim_owned(bytes, 1)?;
        let mut replacement = SimulationTraceReplacement { trace: Vec::new(), allocation };
        replacement.trace.try_reserve_exact(capacity)
            .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual = u64::try_from(replacement.trace.capacity().checked_mul(
            std::mem::size_of::<SimulationTrace>(),
        ).ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?;
        accounting.reconcile_owned(&mut replacement.allocation, bytes, actual)?;
        accounting.admit(delta)?;
        replacement.trace.extend_from_slice(&self.trace);
        accounting.record_completed(delta)?;
        let SimulationTraceReplacement { trace, allocation } = replacement;
        self.trace = trace;
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.trace_native = match allocation {
                KernelOwnedAllocation::Standalone => None,
                KernelOwnedAllocation::NativeOriginal(guard) => Some(guard),
            };
        }
        #[cfg(target_arch = "wasm32")]
        let _ = allocation;
        Ok(())
    }

    fn intercept(
        &mut self,
        operation: SimulationOperation,
    ) -> Result<(u64, Option<SimulationFault>), &'static str> {
        if self.trace.len() == self.options.maximum_trace_entries {
            return Err("simulation trace bound exhausted");
        }
        let occurrence = self.occurrences[operation as usize]
            .checked_add(1)
            .ok_or("simulation operation occurrence overflow")?;
        self.occurrences[operation as usize] = occurrence;
        let fault = self.faults.remove(&(operation, occurrence));
        #[cfg(not(target_arch = "wasm32"))]
        let replaces_trace = self.trace.len() == self.trace.capacity();
        self.trace
            .try_reserve(1)
            .map_err(|_| "simulation trace allocation failed")?;
        #[cfg(not(target_arch = "wasm32"))]
        if replaces_trace { self.trace_native = None; }
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
                #[cfg(not(target_arch = "wasm32"))]
                trace_native: None,
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
        self.flush_object_visibility_engine(SimulationWriteWork::Ordinary(budget), cancellation).await
    }

    /// Publishes delayed aliases under the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn flush_object_visibility_with_native_work(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<u64, ObjectStoreError> {
        self.flush_object_visibility_engine(SimulationWriteWork::Native(work), cancellation).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn flush_object_visibility_engine(
        &self,
        accounting: SimulationWriteWork<'_>,
        cancellation: &CancellationToken,
    ) -> ObjectResult<u64> {
        let mut work = WorkCounters::default();
        let mut published = 0_u64;
        #[cfg(not(target_arch = "wasm32"))]
        if let SimulationWriteWork::Native(native) = accounting {
            native.remaining().map_err(|error| ObjectFailure::before_work(error.into()))?;
        }
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
            let receipt = match accounting {
                SimulationWriteWork::Ordinary(budget) => {
                    let remaining = work.remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    self.objects.put(object_id, bytes, remaining, cancellation).await
                }
                #[cfg(not(target_arch = "wasm32"))]
                SimulationWriteWork::Native(native) => {
                    self.objects.put_with_native_work(object_id, bytes, native, cancellation).await
                        .map(|()| ObjectReceipt { value: (), work: WorkCounters::default() })
                        .map_err(ObjectFailure::before_work)
                }
                #[cfg(target_arch = "wasm32")]
                SimulationWriteWork::Lifetime(_) => unreachable!(),
            }.map_err(|failure| {
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
        let (authority, objects) = reference_backends();
        Self {
            state: Arc::new(Mutex::new(State {
                options: SimulationOptions::default(),
                occurrences: [0; SIMULATION_OPERATION_COUNT],
                faults: BTreeMap::new(),
                trace: Vec::new(),
                #[cfg(not(target_arch = "wasm32"))]
                trace_native: None,
                pending_objects: BTreeMap::new(),
                pending_object_bytes: 0,
            })),
            authority: Arc::new(authority),
            objects: Arc::new(objects),
        }
    }
}

/// Authority backend handle controlled by one [`Simulation`].
#[derive(Clone)]
pub struct SimulatedAuthorityStore<
    A = crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
> {
    state: Arc<Mutex<State>>,
    inner: Arc<A>,
}

/// Immutable-object backend handle controlled by one [`Simulation`].
#[derive(Clone)]
pub struct SimulatedObjectStore<O = crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>> {
    state: Arc<Mutex<State>>,
    inner: Arc<O>,
}

#[derive(Clone, Copy)]
enum SimulationWriteWork<'a> {
    Ordinary(WorkBudget),
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a crate::materializer::NativeMaterializationWork),
    #[cfg(target_arch = "wasm32")]
    #[allow(dead_code)]
    Lifetime(std::marker::PhantomData<&'a ()>),
}

impl SimulationWriteWork<'_> {
    fn simulation_error(self, cause: &'static str) -> ObjectStoreError {
        match self {
            Self::Ordinary(_) => ObjectStoreError::Rejected(cause.to_owned()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => ObjectStoreError::Simulation(cause),
            #[cfg(target_arch = "wasm32")]
            Self::Lifetime(_) => unreachable!(),
        }
    }

    fn charge(self, delta: WorkCounters) -> ObjectResult<()> {
        match self {
            Self::Ordinary(budget) => {
                delta.verify(budget)
                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                Ok(ObjectReceipt { value: (), work: delta })
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(work) => {
                work.charge(delta).map_err(|error| ObjectFailure::before_work(error.into()))?;
                Ok(ObjectReceipt { value: (), work: WorkCounters::default() })
            }
            #[cfg(target_arch = "wasm32")]
            Self::Lifetime(_) => unreachable!(),
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

impl<O: AsyncObjectStore> SimulatedObjectStore<O> {
    async fn put_engine(
        &self,
        object: SimulationWrite,
        accounting: SimulationWriteWork<'_>,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let fault = {
            let mut state = self.state.lock()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Corrupt))?;
            #[cfg(not(target_arch = "wasm32"))]
            if let SimulationWriteWork::Native(work) = accounting {
                state.prepare_native_trace(work).map_err(ObjectFailure::before_work)?;
            }
            state.intercept(SimulationOperation::ObjectPut)
                .map(|(_, fault)| fault)
                .map_err(|cause| ObjectFailure::before_work(accounting.simulation_error(cause)))?
        };
        match fault {
            Some(SimulationFault::RejectBefore) => {
                return Err(ObjectFailure::before_work(accounting.simulation_error("scheduled rejection")));
            }
            Some(SimulationFault::PartitionBefore) => {
                return Err(ObjectFailure::before_work(accounting.simulation_error("scheduled partition")));
            }
            _ => {}
        }
        if matches!(fault, Some(SimulationFault::DelayObjectVisibility)) {
            let (object_id, bytes, hashed) = object.parts();
            let length = u64::try_from(bytes.len())
                .map_err(|_| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
            let hash_bytes = if hashed { 0 } else {
                length.checked_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES)
                    .ok_or_else(|| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?
            };
            let work = accounting.charge(WorkCounters {
                bytes_hashed: hash_bytes,
                ..WorkCounters::default()
            })?.work;
            if !hashed && object_digest(object_id.kind, bytes) != object_id.digest {
                return Err(ObjectFailure::new(ObjectStoreError::DigestMismatch, work));
            }
            cancellation.check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let mut state = self.state.lock()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
            if let Some(existing) = state.pending_objects.get(&object_id) {
                if existing != bytes {
                    return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
                }
                return Ok(ObjectReceipt { value: (), work });
            }
            let next_count = state.pending_objects.len().checked_add(1)
                .ok_or_else(|| ObjectFailure::new(crate::WorkError::Overflow.into(), work))?;
            let next_bytes = state.pending_object_bytes.checked_add(length)
                .ok_or_else(|| ObjectFailure::new(crate::WorkError::Overflow.into(), work))?;
            if next_count > state.options.maximum_pending_objects
                || next_bytes > state.options.maximum_pending_object_bytes
            {
                return Err(ObjectFailure::new(accounting.simulation_error(
                    "simulation delayed-object bound exhausted"), work));
            }
            let (_, bytes) = object.into_parts();
            state.pending_objects.insert(object_id, bytes);
            state.pending_object_bytes = next_bytes;
            return Ok(ObjectReceipt { value: (), work });
        }
        match accounting {
            SimulationWriteWork::Ordinary(budget) => match object {
                SimulationWrite::Plain(id, bytes) => self.inner.put(id, bytes, budget, cancellation).await,
                SimulationWrite::Hashed(object) => self.inner.put_hashed(object, budget, cancellation).await,
            },
            #[cfg(not(target_arch = "wasm32"))]
            SimulationWriteWork::Native(work) => {
                let result = match object {
                    SimulationWrite::Plain(id, bytes) => self.inner.put_with_native_work(id, bytes, work, cancellation).await,
                    SimulationWrite::Hashed(object) => self.inner.put_hashed_with_native_work(object, work, cancellation).await,
                };
                result.map(|()| ObjectReceipt { value: (), work: WorkCounters::default() })
                    .map_err(ObjectFailure::before_work)
            }
            #[cfg(target_arch = "wasm32")]
            SimulationWriteWork::Lifetime(_) => unreachable!(),
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
        let fault = state.intercept(operation).map_err(ObjectStoreError::Simulation)?.1;
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
        &self, object_id: ObjectId, maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<ObjectRead, crate::FsError> {
        let fault = self.read_fault(SimulationOperation::ObjectRead, accounting)?;
        let mut value = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => self.inner
                .read_with_native_work(object_id, maximum_bytes, work, cancellation).await?.into_read(),
            _ => match self.inner.read(object_id, maximum_bytes, accounting.remaining()?, cancellation).await {
                Ok(receipt) => {
                    crate::async_storage::merge_completed_object_read(
                        accounting, receipt.work, object_read_retained_bytes(&receipt.value),
                    )?;
                    receipt.value
                }
                Err(failure) => {
                    crate::async_storage::merge_completed_object_read(accounting, *failure.work, 0)?;
                    return Err(failure.error.into());
                }
            },
        };
        if let Some(altered) = altered_read_data(&value, fault, accounting)? { value = altered; }
        Ok(value)
    }

    async fn read_many_engine(
        &self, requests: &[ObjectReadRequest], accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<ObjectRead>, KernelOwnedAllocation), crate::FsError> {
        let fault = self.read_fault(SimulationOperation::ObjectReadMany, accounting)?;
        // Keep the vector before its guard throughout fallible fault injection.
        let mut values = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let (reads, allocation) = self.inner.read_many_with_native_work(requests, work, cancellation).await?.into_parts();
                (reads, KernelOwnedAllocation::NativeOriginal(allocation))
            }
            _ => match self.inner.read_many(requests, accounting.remaining()?, cancellation).await {
                Ok(receipt) => {
                    let retained = receipt.value.iter().try_fold(
                        u64::try_from(receipt.value.capacity().checked_mul(std::mem::size_of::<ObjectRead>())
                            .ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?,
                        |bytes, read| bytes.checked_add(object_read_retained_bytes(read)).ok_or(crate::WorkError::Overflow),
                    )?;
                    crate::async_storage::merge_completed_object_read(accounting, receipt.work, retained)?;
                    (receipt.value, KernelOwnedAllocation::Standalone)
                }
                Err(failure) => {
                    crate::async_storage::merge_completed_object_read(accounting, *failure.work, 0)?;
                    return Err(failure.error.into());
                }
            },
        };
        if let Some(first) = values.0.first_mut()
            && let Some(altered) = altered_read_data(first, fault, accounting)? { *first = altered; }
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

fn object_fault(error: impl Into<String>) -> ObjectFailure {
    ObjectFailure::before_work(ObjectStoreError::Rejected(error.into()))
}

fn select_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state.lock().map_err(|_| {
        AuthorityStoreError::CorruptStatic("simulation state is poisoned")
    })?;
    state
        .intercept(operation)
        .map(|(_, fault)| fault)
        .map_err(AuthorityStoreError::RejectedStatic)
}

#[cfg(not(target_arch = "wasm32"))]
fn select_native_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
    original: &crate::NativeMaterializationWork,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state.lock().map_err(|_| {
        AuthorityStoreError::CorruptStatic("simulation state is poisoned")
    })?;
    let mut accounting = KernelAccounting::native(
        original, original.cancellation(),
    );
    state.prepare_trace(&mut accounting).map_err(|error| match error {
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
        crate::FsError::Work(error) => AuthorityStoreError::Work(error),
        crate::FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
        crate::FsError::Object(ObjectStoreError::MemoryTableAllocationFailed(error)) =>
            AuthorityStoreError::NativeOriginal(
                crate::NativeTreeMaterializationError::AllocationFailed(error),
            ),
        crate::FsError::Object(ObjectStoreError::Simulation(reason)) =>
            AuthorityStoreError::RejectedStatic(reason),
        _ => AuthorityStoreError::CorruptStatic("simulation trace ownership is invalid"),
    })?;
    state.intercept(operation).map(|(_, fault)| fault)
        .map_err(AuthorityStoreError::RejectedStatic)
}

fn lost_authority_acknowledgement(operation: &'static str) -> AuthorityStoreError {
    AuthorityStoreError::Indeterminate {
        operation,
        source: std::io::ErrorKind::ConnectionReset.into(),
    }
}

fn intercept_authority(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, AuthorityFailure> {
    select_authority_fault(state, operation).map_err(AuthorityFailure::before_work)
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

fn reject_authority(fault: Option<SimulationFault>) -> Result<(), AuthorityFailure> {
    check_authority_fault(fault).map_err(AuthorityFailure::before_work)
}

fn reject_object(fault: Option<SimulationFault>) -> Result<(), ObjectFailure> {
    match fault {
        Some(SimulationFault::RejectBefore) => Err(object_fault("scheduled rejection")),
        Some(SimulationFault::PartitionBefore) => Err(object_fault("scheduled partition")),
        _ => Ok(()),
    }
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for SimulatedAuthorityStore<A> {
    async fn publication_reservation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<PublicationReservation>> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityReservation)?;
        reject_authority(fault)?;
        self.inner.publication_reservation(authority_id, operation_id, budget, cancellation).await
    }

    async fn reserve_publication(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityReserve)?;
        reject_authority(fault)?;
        let receipt = self.inner.reserve_publication(authority_id, expected, operation_id, budget, cancellation).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
            && matches!(receipt.value, ReservationOutcome::Reserved(_) | ReservationOutcome::AlreadyReserved(_)) {
            return Err(AuthorityFailure::new(AuthorityStoreError::Indeterminate {
                operation: "reserve-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            }, receipt.work));
        }
        Ok(receipt)
    }

    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        cancellation.check().map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityRelease)?;
        reject_authority(fault)?;
        let receipt = self.inner.release_publication(reservation, budget, cancellation).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(AuthorityFailure::new(AuthorityStoreError::Indeterminate {
                operation: "release-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            }, receipt.work));
        }
        Ok(receipt)
    }

    async fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityCreate)?;
        reject_authority(fault)?;
        self.inner
            .fork_generation_authority(
                source,
                destination_authority,
                operation_id,
                budget,
                cancellation,
            )
            .await
    }

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

    async fn create_authority_with_first_record(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        cancellation.check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityCreate)?;
        reject_authority(fault)?;
        let receipt = self.inner.create_authority_with_first_record(
            authority, commit, budget, cancellation,
        ).await?;
        if receipt.value && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(AuthorityFailure::new(
                lost_authority_acknowledgement("create-authority-with-first-record"), receipt.work,
            ));
        }
        Ok(receipt)
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> crate::FsResult<bool>
    where
        F: FnMut(WorkBudget) -> Fut + StorageFuture,
        Fut: Future<Output = crate::FsResult<()>> + StorageFuture,
    {
        cancellation.check().map_err(|error| crate::OperationFailure::before_work(error.into()))?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)
            .map_err(|error| crate::OperationFailure::before_work(error.into()))?;
        check_authority_fault(fault)
            .map_err(|error| crate::OperationFailure::before_work(error.into()))?;
        let receipt = self.inner.create_authority_with_first_record_guarded(
            authority, commit, budget, cancellation, guard,
        ).await?;
        if receipt.value && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(crate::OperationFailure::new(
                lost_authority_acknowledgement("create-authority-with-first-record").into(),
                receipt.work,
            ));
        }
        Ok(receipt)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_with_native_work(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<bool, AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        original.admit(WorkCounters::UNCHARGED)?;
        let fault = select_native_authority_fault(
            &self.state, SimulationOperation::AuthorityCreate, original,
        )?;
        check_authority_fault(fault)?;
        let created = self.inner.create_authority_with_first_record_with_native_work(
            authority, commit, original, cancellation,
        ).await?;
        if created && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(lost_authority_acknowledgement("create-authority-with-first-record"));
        }
        Ok(created)
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
        self.compare_and_append_guarded(crate::GuardedAppend {
            authority_id, epoch, expected, commit,
            permit: crate::PublicationPermit::Unrestricted,
        }, budget, cancellation).await
    }

    async fn compare_and_append_guarded(
        &self,
        request: crate::GuardedAppend,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation.check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let fault = intercept_authority(&self.state, SimulationOperation::AuthorityAppend)?;
        reject_authority(fault)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            let fenced = self.inner.fence(
                request.authority_id, request.expected, budget, cancellation,
            ).await?;
            let remaining = fenced.work.remaining(budget)
                .map_err(|error| AuthorityFailure::new(error.into(), fenced.work))?;
            let appended = self.inner.compare_and_append_guarded(
                request, remaining, cancellation,
            ).await.map_err(|failure| {
                let nested = *failure.work;
                match fenced.work.checked_add(nested) {
                    Ok(combined) => AuthorityFailure::new(failure.error, combined),
                    Err(error) => AuthorityFailure::new(error.into(), fenced.work),
                }
            })?;
            let work = fenced.work.checked_add(appended.work)
                .map_err(|error| AuthorityFailure::new(error.into(), fenced.work))?;
            return Ok(crate::AuthorityReceipt { value: appended.value, work });
        }
        let receipt = self.inner.compare_and_append_guarded(request, budget, cancellation).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
            && matches!(receipt.value, AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_))
        {
            return Err(AuthorityFailure::new(
                lost_authority_acknowledgement("compare-and-append"), receipt.work,
            ));
        }
        Ok(receipt)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_work(
        &self,
        request: crate::GuardedAppend,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        original.admit(WorkCounters::UNCHARGED)?;
        let fault = select_native_authority_fault(
            &self.state, SimulationOperation::AuthorityAppend, original,
        )?;
        check_authority_fault(fault)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            self.inner.fence_with_native_work(
                request.authority_id, request.expected, original, cancellation,
            ).await?;
        }
        let result = self.inner.compare_and_append_guarded_with_native_work(
            request, original, cancellation,
        ).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
            && matches!(result.0, AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_))
        {
            return Err(lost_authority_acknowledgement("compare-and-append"));
        }
        Ok(result)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self,
        request: crate::GuardedAppend,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<AuthorityStoreError> + Send + 'static,
    {
        cancellation.check().map_err(|_| E::from(AuthorityStoreError::Cancelled))?;
        original.admit(WorkCounters::UNCHARGED)
            .map_err(AuthorityStoreError::from).map_err(E::from)?;
        let fault = select_native_authority_fault(
            &self.state, SimulationOperation::AuthorityAppend, original,
        ).map_err(E::from)?;
        check_authority_fault(fault).map_err(E::from)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            self.inner.fence_with_native_work(
                request.authority_id, request.expected, original, cancellation,
            ).await.map_err(E::from)?;
        }
        let result = self.inner.compare_and_append_guarded_with_native_owner(
            request, original, cancellation, guard,
        ).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
            && matches!(result.0, AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_))
        {
            return Err(lost_authority_acknowledgement("compare-and-append").into());
        }
        Ok(result)
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn head_with_native_work(
        &self,
        authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Head, AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityHead)?;
        check_authority_fault(fault)?;
        if let Some(SimulationFault::StaleAuthorityHead(head)) = fault {
            // This is synthetic metadata, not a read of the wrapped authority.
            return Ok(head);
        }
        self.inner.head_with_native_work(authority_id, work, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn replay_with_native_work(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        use crate::materializer::NativeTreeMaterializationError;
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReplay)?;
        check_authority_fault(fault)?;
        let (mut records, mut custody) = self.inner
            .replay_with_native_work(authority_id, after, limit, work, cancellation).await?;
        if !matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay)) || records.is_empty() {
            return Ok((records, custody));
        }

        let needs_growth = records.len() == records.capacity();
        let copied_records = if needs_growth {
            records.len().checked_add(1)
        } else {
            Some(1)
        }.ok_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        let extra = WorkCounters {
            bytes_copied: copied_records.checked_mul(std::mem::size_of::<DurableCommit>())
                .ok_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))? as u64,
            items_returned: 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(extra)?;
        if needs_growth {
            let count = records.len().checked_add(1)
                .ok_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
            let old_bytes = records.capacity().checked_mul(std::mem::size_of::<DurableCommit>())
                .ok_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))? as u64;
            let (mut replacement, mut replacement_custody) = work.with_owned_allocations(
                |ledger, counters, budget, cancellation| {
                    cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
                    counters.checked_add(extra)
                        .and_then(|projected| projected.verify(budget))
                        .map_err(NativeTreeMaterializationError::Work)?;
                    ledger.claim_elements::<DurableCommit>(count, counters, budget)
                        .map_err(|error| error.into_work_or(
                            NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
                    let mut replacement = Vec::new();
                    replacement.try_reserve_exact(count)
                        .map_err(NativeTreeMaterializationError::AllocationFailed)?;
                    Ok::<_, AuthorityStoreError>(replacement)
                },
            )?;
            replacement.append(&mut records);
            drop(records);
            let moved = WorkCounters {
                bytes_copied: extra.bytes_copied - std::mem::size_of::<DurableCommit>() as u64,
                ..WorkCounters::UNCHARGED
            };
            let transferred = work.record_admitted(moved)
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
        Ok((records, custody))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn fence_with_native_work(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::storage::FenceOutcome, AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityFence)?;
        check_authority_fault(fault)?;
        self.inner.fence_with_native_work(authority_id, expected, work, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn find_operation_with_native_work(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Option<DurableCommit>, crate::materializer::NativeAllocation), AuthorityStoreError> {
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityFindOperation)?;
        check_authority_fault(fault)?;
        self.inner.find_operation_with_native_work(authority_id, operation_id, work, cancellation).await
    }
}

struct SimulationTraceReplacement {
    trace: Vec<SimulationTrace>,
    allocation: KernelOwnedAllocation,
}

struct SimulationAlteredBytes {
    value: Vec<u8>,
    allocation: KernelOwnedAllocation,
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
    if !matches!(fault, Some(
        SimulationFault::CorruptObjectRead | SimulationFault::PartialObjectRead,
    )) {
        return Ok(None);
    }
    if read.bytes.is_empty() { return Err(ObjectStoreError::Corrupt.into()); }
    accounting.check()?;
    let retained = if matches!(fault, Some(SimulationFault::PartialObjectRead)) {
        read.bytes.len() - 1
    } else {
        read.bytes.len()
    };
    let requested = u64::try_from(retained).map_err(|_| crate::WorkError::Overflow)?;
    accounting.admit(WorkCounters { bytes_copied: requested, ..WorkCounters::UNCHARGED })?;
    let allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let mut bytes = SimulationAlteredBytes { value: Vec::new(), allocation };
    bytes.value.try_reserve_exact(retained)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual = u64::try_from(bytes.value.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut bytes.allocation, requested, actual)?;
    accounting.admit(WorkCounters { bytes_copied: requested, ..WorkCounters::UNCHARGED })?;
    bytes.value.extend_from_slice(&read.bytes[..retained]);
    accounting.record_completed(WorkCounters { bytes_copied: requested, ..WorkCounters::UNCHARGED })?;
    if matches!(fault, Some(SimulationFault::CorruptObjectRead))
        && let Some(first) = bytes.value.first_mut() { *first ^= 0x80; }
    let logical_bytes = u64::try_from(bytes.value.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    let SimulationAlteredBytes { value: bytes, allocation } = bytes;
    let bytes = match allocation {
        KernelOwnedAllocation::Standalone => OwnedBytes::from(bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            let original = accounting.native_work().ok_or(ObjectStoreError::Corrupt)?;
            OwnedBytes::from_vec_with_guard(bytes, allocation, original.runtime_meter_owner())
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) =>
                        crate::FsError::from(original.runtime_error(error)),
                    _ => ObjectStoreError::Corrupt.into(),
                })?
        }
    };
    Ok(Some(ObjectRead { bytes, retention: ObjectReadRetention::Owned { logical_bytes } }))
}

impl<O: AsyncObjectStore> AsyncObjectStore for SimulatedObjectStore<O> {

    fn collection(&self) -> Option<&std::sync::Arc<crate::Collection>> {
        self.inner.collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner.collection_sweeps()
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>> {
        self.inner.historical_generation_evidence(
            authority_id, record, maximum_bytes, maximum_objects, budget, cancellation,
        ).await
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_engine(SimulationWrite::Plain(object_id, bytes),
            SimulationWriteWork::Ordinary(budget), cancellation).await
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        self.inner.put_many(writes, budget, cancellation).await
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_engine(SimulationWrite::Hashed(object),
            SimulationWriteWork::Ordinary(budget), cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_engine(SimulationWrite::Plain(object_id, bytes),
            SimulationWriteWork::Native(work), cancellation).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_engine(SimulationWrite::Hashed(object),
            SimulationWriteWork::Native(work), cancellation).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[crate::storage::ObjectWrite],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.remaining()?;
        self.inner.put_many_with_native_work(writes, work, cancellation).await
    }

    async fn flush_before_publish(
        &self, scope: crate::async_storage::PublicationScope<'_>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> ObjectResult<crate::PublicationHold> {
        self.inner.flush_before_publish(scope, budget, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, scope: crate::async_storage::PublicationScope<'_>,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.remaining()?;
        self.inner.flush_before_publish_with_native_work(scope, work, cancellation).await
    }

    async fn prepare_generation_publication(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.prepare_generation_publication(
            request, proof, append, scope, budget, cancellation,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.inner.prepare_generation_publication_with_native_work(
            request, proof, append, scope, original, cancellation,
        ).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut accounting = KernelAccounting::standalone(
            budget, cancellation,
        );
        let result = self.read_engine(
            object_id, maximum_bytes, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        match result {
            Ok(value) => Ok(ObjectReceipt { value, work }),
            Err(error) => Err(ObjectFailure::new(crate::async_storage::sequential_read_error(error), work)),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self, object_id: ObjectId, maximum_bytes: u64,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let read = self.read_engine(object_id, maximum_bytes, &mut accounting, cancellation).await
            .map_err(crate::async_storage::sequential_read_error)?;
        match read.retention {
            ObjectReadRetention::Shared => crate::async_storage::NativeObjectRead::shared(read),
            ObjectReadRetention::Owned { .. } => crate::async_storage::NativeObjectRead::intrinsic(read, work),
        }
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut accounting = KernelAccounting::standalone(
            budget, cancellation,
        );
        let result = self.read_many_engine(requests, &mut accounting, cancellation).await;
        let work = accounting.spent();
        match result {
            Ok((value, _)) => Ok(ObjectReceipt { value, work }),
            Err(error) => Err(ObjectFailure::new(crate::async_storage::sequential_read_error(error), work)),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self, requests: &[ObjectReadRequest],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectReadBatch, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (reads, allocation) = self.read_many_engine(requests, &mut accounting, cancellation).await
            .map_err(crate::async_storage::sequential_read_error)?;
        match allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => crate::async_storage::NativeObjectReadBatch::new(reads, allocation),
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
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
        crate::storage::ObjectStore::put(
            &memory, object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED,
        ).expect("seed memory");
        let simulation = Simulation::wrap(
            reference_backends().0, memory, SimulationOptions::default(),
        ).expect("bounded simulation");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            WorkBudget { allocation_operations: 1, ..budget() }, &cancellation,
        ).expect("only actual accounting constructor");
        let before = work.receipt();
        assert!(store.read_with_native_work(object.object_id(), 4, &work, &cancellation).await.is_err());
        assert_eq!(work.receipt(), before);
        assert!(simulation.trace().expect("trace").is_empty());
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(store.read_with_native_work(
            object.object_id(), 4, &work, &cancelled,
        ).await, Err(ObjectStoreError::Cancelled)));
        assert_eq!(work.receipt(), before);
    }

    #[tokio::test]
    async fn native_read_partial_copy_keeps_original_capacity_until_last_byte_alias() {
        let memory = crate::memory::MemoryObjectStore::default();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        crate::storage::ObjectStore::put(
            &memory, object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED,
        ).expect("seed memory");
        let simulation = Simulation::wrap(
            reference_backends().0, memory, SimulationOptions::default(),
        ).expect("bounded simulation");
        simulation.schedule_next(SimulationOperation::ObjectRead,
            SimulationFault::PartialObjectRead).expect("partial fault");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
        let baseline = work.live_allocation_bytes();
        let read = store.read_with_native_work(
            object.object_id(), 4, &work, &cancellation,
        ).await.expect("actual partial SDK copy");
        assert_eq!(read.read().bytes.as_ref(), b"bod");
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().bytes_copied, 3);
        let alias = read.read().bytes.clone();
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
        crate::storage::ObjectStore::put(
            &memory, object.object_id(), object.bytes().clone(), WorkBudget::UNBOUNDED,
        ).expect("seed memory");
        let simulation = Simulation::wrap(
            reference_backends().0, memory, SimulationOptions::default(),
        ).expect("bounded simulation");
        simulation.schedule_next(SimulationOperation::ObjectReadMany,
            SimulationFault::CorruptObjectRead).expect("batch fault");
        let (_, store) = simulation.stores();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
        let baseline = work.live_allocation_bytes();
        let requests = [ObjectReadRequest { object_id: object.object_id(), maximum_bytes: 4 }; 2];
        let batch = store.read_many_with_native_work(&requests, &work, &cancellation)
            .await.expect("one genuine batch");
        assert_eq!(batch.reads().len(), 2);
        assert_eq!(batch.reads()[0].bytes[0], b'b' ^ 0x80);
        assert_eq!(batch.reads()[1].bytes.as_ref(), b"body");
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_probes, 2);
        assert_eq!(work.receipt().bytes_copied, 4);
        drop(batch);
        drop(store);
        drop(simulation);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[tokio::test]
    async fn native_delayed_visibility_preserves_intrinsic_alias_custody() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut custody = work.claim(4).expect("payload capacity");
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(4).expect("payload buffer");
        bytes.extend_from_slice(b"body");
        let object = HashedObject::from_native_vec(ObjectKind::TreePage,
            &mut bytes, &mut custody, &work, &cancellation).expect("intrinsic byte owner");
        let alias = object.bytes().clone();
        let payload_live = work.live_allocation_bytes();
        let hashed = work.receipt().bytes_hashed;
        let simulation = Simulation::default();
        simulation.schedule_next(SimulationOperation::ObjectPut,
            SimulationFault::DelayObjectVisibility).expect("delay schedule");
        let (_, store) = simulation.stores();
        store.put_hashed_with_native_work(object, &work, &cancellation).await.expect("delayed admission");
        assert_eq!(work.receipt().bytes_hashed, hashed);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(simulation.state.lock().expect("state").pending_object_bytes, 4);
        assert!(work.live_allocation_bytes() >= payload_live);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let before = work.receipt();
        assert!(matches!(simulation.flush_object_visibility_with_native_work(&work, &cancelled).await,
            Err(ObjectStoreError::Cancelled)));
        assert_eq!(work.receipt(), before);
        assert_eq!(simulation.state.lock().expect("state").pending_object_bytes, 4);
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
        simulation.schedule_next(SimulationOperation::ObjectPut,
            SimulationFault::DelayObjectVisibility).expect("delay schedule");
        let (_, store) = simulation.stores();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        let before = work.receipt();
        assert!(store.put_with_native_work(object.object_id(), object.bytes().clone(),
            &work, &cancellation).await.is_err());
        assert_eq!(work.receipt(), before);
        let state = simulation.state.lock().expect("state");
        assert!(state.trace.is_empty());
        assert!(state.occurrences.iter().all(|occurrence| *occurrence == 0));
        assert!(state.pending_objects.is_empty());
        assert_eq!(state.faults.len(), 1);
        drop(state);
        assert!(store.put_hashed_with_native_work(object, &work, &cancellation).await.is_err());
        assert_eq!(work.receipt(), before);
    }

    #[tokio::test]
    async fn native_scheduled_rejection_does_not_hash_or_write_payload() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let simulation = Simulation::default();
        simulation.schedule_next(SimulationOperation::ObjectPut,
            SimulationFault::RejectBefore).expect("rejection schedule");
        let (_, store) = simulation.stores();
        let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        assert!(matches!(store.put_with_native_work(object.object_id(), object.bytes().clone(),
            &work, &cancellation).await, Err(ObjectStoreError::Simulation("scheduled rejection"))));
        assert_eq!(work.receipt().bytes_hashed, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(simulation.state.lock().expect("state").pending_objects.is_empty());
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_write_cancellation_controls {
    use super::*;

    #[tokio::test]
    async fn native_write_hooks_preserve_original_cancellation_before_effects() {
        let original = CancellationToken::new();
        let argument = CancellationToken::new();
        let mut encoded = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget encoding");
        for field in encoded.as_object_mut().expect("budget fields").values_mut() {
            *field = serde_json::Value::from(1_000_000_u64);
        }
        let budget = serde_json::from_value(encoded).expect("finite budget");
        let work = crate::materializer::NativeMaterializationWork::new(budget, &original)
            .expect("original ledger");
        original.cancel();
        let before = work.receipt();
        let simulation = Simulation::default();
        let (_, store) = simulation.stores();
        let object = crate::storage::HashedObject::new(
            crate::storage::ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
        assert!(store.put_with_native_work(object.object_id(), object.bytes().clone(),
            &work, &argument).await.is_err());
        assert!(store.put_hashed_with_native_work(object.clone(), &work, &argument).await.is_err());
        let writes = [crate::storage::ObjectWrite {
            object_id: object.object_id(), bytes: object.bytes().clone(),
        }];
        assert!(store.put_many_with_native_work(&writes, &work, &argument).await.is_err());
        assert!(store.flush_before_publish_with_native_work(
            crate::async_storage::PublicationScope::Everything, &work, &argument).await.is_err());
        assert!(simulation.flush_object_visibility_with_native_work(&work, &argument).await.is_err());
        assert_eq!(work.receipt(), before);
        let state = simulation.state.lock().expect("state");
        assert!(state.trace.is_empty());
        assert!(state.pending_objects.is_empty());
    }
}
