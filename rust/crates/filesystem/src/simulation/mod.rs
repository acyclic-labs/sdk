//! Deterministic bounded fault simulator for storage backends.
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore, GenerationForkSource};
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::performance::WorkCounters;
use crate::storage::{
    AppendOutcome, AuthorityStoreError, CreateAuthorityOutcome, ObjectId, ObjectRead,
    ObjectReadRequest, ObjectReadRetention, ObjectStoreError, PublicationReservation, ReplayLimit,
    ReservationOutcome, object_digest,
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
    /// Existing publication reservation lookup.
    AuthorityReservation,
    /// Publication reservation acquisition.
    AuthorityReserve,
    /// Publication reservation release.
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

const SIMULATION_OPERATION_COUNT: usize = SimulationOperation::ObjectContains as usize + 1;

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
        let occurrence = state
            .occurrences
            .get(operation as usize)
            .ok_or(SimulationError::PastOccurrence)?
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
    if *state
        .occurrences
        .get(scheduled.operation as usize)
        .ok_or(SimulationError::PastOccurrence)?
        >= scheduled.occurrence
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

fn reference_backends() -> (ReferenceAuthority, ReferenceObjects) {
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    (
        crate::distributed::StreamAuthorityStore::new(streams),
        crate::LogicalObjectStore::new(Arc::new(objects), bucket),
    )
}

mod state;
use state::State;
mod authority;
mod objects;

#[cfg(test)]
#[path = "../tests/simulation/mod.rs"]
mod tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod native_write_controls;
