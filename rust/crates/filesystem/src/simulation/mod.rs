//! Storage backends as fault sites of the shared deterministic simulation.
//!
//! Each authority and object operation is a logical site of
//! [`acyclic_native_runtime::testing::Schedule`]. A driver plans a
//! [`SimulationFault`] with `Schedule::inject` at an exact call of a [`sites`]
//! name; the schedule counts calls, traces them and holds paused calls at
//! its boundaries. This module only defines what each fault does to a call.
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore, GenerationForkSource};
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::kernel::allocation::KernelOwnedAllocation;
use crate::performance::WorkCounters;
use crate::storage::{
    AppendOutcome, AuthorityStoreError, CreateAuthorityOutcome, ObjectId, ObjectRead,
    ObjectReadRequest, ObjectReadRetention, ObjectStoreError, PublicationReservation, ReplayLimit,
    ReservationOutcome, object_digest,
};
use acyclic_native_runtime::testing::Schedule;
use acyclic_stream::OwnedBytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type ReferenceAuthority = crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>;
type ReferenceObjects = crate::LogicalObjectStore<acyclic_objects::MemoryObjects>;

/// Site names of the intercepted storage operations.
pub mod sites {
    /// Authority creation, including a fork's and a first record's.
    pub const AUTHORITY_CREATE: &str = "authority.create";
    /// Linearizable authority-head read.
    pub const AUTHORITY_HEAD: &str = "authority.head";
    /// Authority compare-and-append, single or grouped.
    pub const AUTHORITY_APPEND: &str = "authority.append";
    /// Bounded authority replay.
    pub const AUTHORITY_REPLAY: &str = "authority.replay";
    /// Authority epoch fence.
    pub const AUTHORITY_FENCE: &str = "authority.fence";
    /// Idempotent operation lookup.
    pub const AUTHORITY_FIND_OPERATION: &str = "authority.find-operation";
    /// Existing publication reservation lookup.
    pub const AUTHORITY_RESERVATION: &str = "authority.reservation";
    /// Publication reservation acquisition.
    pub const AUTHORITY_RESERVE: &str = "authority.reserve";
    /// Publication reservation release.
    pub const AUTHORITY_RELEASE: &str = "authority.release";
    /// Immutable-object admission.
    pub const OBJECT_PUT: &str = "object.put";
    /// One immutable-object read.
    pub const OBJECT_READ: &str = "object.read";
    /// Ordered immutable-object batch read.
    pub const OBJECT_READ_MANY: &str = "object.read-many";
    /// Publication durability barrier over the authenticated immutable closure.
    pub const OBJECT_FLUSH: &str = "object.flush";
    /// Immutable-object presence probe.
    pub const OBJECT_CONTAINS: &str = "object.contains";
}

/// What a planned fault does to one storage call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimulationFault {
    /// Hold the object call at a schedule boundary; dropping it crashes the call.
    PauseBefore,
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
    fn applies_at(self, site: &str) -> bool {
        use sites::*;
        match self {
            Self::PauseBefore => matches!(
                site,
                OBJECT_PUT | OBJECT_READ | OBJECT_READ_MANY | OBJECT_CONTAINS | OBJECT_FLUSH
            ),
            Self::RejectBefore | Self::PartitionBefore => true,
            Self::DelayObjectVisibility => site == OBJECT_PUT,
            Self::CorruptObjectRead | Self::PartialObjectRead => {
                matches!(site, OBJECT_READ | OBJECT_READ_MANY)
            }
            Self::StaleAuthorityHead(_) => site == AUTHORITY_HEAD,
            Self::DuplicateAuthorityReplay => site == AUTHORITY_REPLAY,
            Self::AmbiguousAuthorityAppend => matches!(
                site,
                AUTHORITY_CREATE | AUTHORITY_APPEND | AUTHORITY_RESERVE | AUTHORITY_RELEASE
            ),
            Self::FenceBeforeAuthorityAppend => site == AUTHORITY_APPEND,
        }
    }
}

/// Counts one call at `site` and returns its planned fault.
///
/// # Panics
///
/// Panics when the planned fault cannot apply at `site`, so a plan never
/// silently injects nothing.
fn intercept(schedule: &Schedule, site: &'static str) -> Option<SimulationFault> {
    let fault = schedule.intercept::<SimulationFault>(site);
    if let Some(fault) = fault {
        assert!(fault.applies_at(site), "{fault:?} cannot apply at {site}");
    }
    fault
}

/// Objects acknowledged under [`SimulationFault::DelayObjectVisibility`].
struct Delayed {
    pending_objects: BTreeMap<ObjectId, OwnedBytes>,
    pending_object_bytes: u64,
}

/// Two backends whose operations are fault sites of one schedule.
#[derive(Clone)]
pub struct Simulation<
    A = crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    O = crate::LogicalObjectStore<acyclic_objects::MemoryObjects>,
> {
    schedule: Schedule,
    state: Arc<Mutex<Delayed>>,
    authority: Arc<A>,
    objects: Arc<O>,
}

impl Simulation<ReferenceAuthority, ReferenceObjects> {
    /// Fresh deterministic memory primitives under `schedule`.
    #[must_use]
    pub fn new(schedule: &Schedule) -> Self {
        let (authority, objects) = reference_backends();
        Self::wrap(schedule, authority, objects)
    }
}

impl<A, O> Simulation<A, O> {
    /// Explicit backends under `schedule`.
    #[must_use]
    pub fn wrap(schedule: &Schedule, authority: A, objects: O) -> Self {
        Self {
            schedule: schedule.observer(),
            state: Arc::new(Mutex::new(Delayed {
                pending_objects: BTreeMap::new(),
                pending_object_bytes: 0,
            })),
            authority: Arc::new(authority),
            objects: Arc::new(objects),
        }
    }

    /// Independently cloneable authority and object handles.
    #[must_use]
    pub fn stores(&self) -> (SimulatedStore<A>, SimulatedStore<O>) {
        (
            SimulatedStore {
                schedule: self.schedule.observer(),
                state: self.state.clone(),
                inner: self.authority.clone(),
            },
            SimulatedStore {
                schedule: self.schedule.observer(),
                state: self.state.clone(),
                inner: self.objects.clone(),
            },
        )
    }
}

/// Backend handle whose operations are sites of one [`Simulation`].
pub struct SimulatedStore<T> {
    schedule: Schedule,
    state: Arc<Mutex<Delayed>>,
    inner: Arc<T>,
}

impl<T> Clone for SimulatedStore<T> {
    fn clone(&self) -> Self {
        Self {
            schedule: self.schedule.observer(),
            state: self.state.clone(),
            inner: self.inner.clone(),
        }
    }
}

fn reference_backends() -> (ReferenceAuthority, ReferenceObjects) {
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
    (
        crate::distributed::StreamAuthorityStore::new(streams),
        crate::LogicalObjectStore::new(Arc::new(objects)),
    )
}

mod authority;
mod objects;

#[cfg(test)]
#[path = "../tests/simulation/mod.rs"]
mod tests;

#[cfg(test)]
mod native_write_controls;
