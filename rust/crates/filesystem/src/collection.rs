//! Coordination between an object collection and the live engine that keeps
//! writing while it runs.
//!
//! A collection marks everything its roots reach and sweeps the rest. Three
//! things keep that sound while the store stays open:
//!
//! - Every open checkout registers the generation it is based on and the
//!   root of its working tree, and every detached file its record, and the
//!   mark treats them as roots, so neither a checkout's base, nor the pages
//!   it staged and drained, nor an unlinked file still open is swept.
//! - A proof records the store's sweep count before it starts, and the
//!   publication admits its closure under the collection gate before its
//!   record names it: a closure with an object swept after the proof began is
//!   refused, so the publication fails rather than name a missing object,
//!   and a closure admitted while a collection runs is kept by it. The gate
//!   stays shared until the record is written; each sweep batch takes it
//!   exclusively.
//! - Objects written after a collection listed its candidates are not
//!   candidates, and a later write of a swept object stores it again.

#![cfg_attr(
    not(all(feature = "local", not(target_arch = "wasm32"))),
    allow(dead_code, reason = "only a local store collects")
)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Mutex, PoisonError};

use crate::kernel::GenerationRoot;
use crate::storage::{ObjectId, ObjectStoreError};
use crate::kernel::allocation::{AllocationError, AllocationLedger};
use crate::performance::{WorkBudget, WorkCounters};

#[derive(Default)]
struct Holds {
    slots: Vec<Slot>,
    free: Option<usize>,
}

struct Slot {
    held: Option<Held>,
    next_free: Option<usize>,
}

/// Collections whose swept objects are remembered; a closure proven before
/// all of them is refused.
const REMEMBERED_COLLECTIONS: usize = 4;

/// One store's collection state, shared by the store and every checkout.
#[derive(Default)]
pub struct Collection {
    gate: Arc<tokio::sync::RwLock<()>>,
    /// Held by the one collection that runs at a time.
    running: Arc<tokio::sync::Mutex<()>>,
    state: Mutex<State>,
    holds: Mutex<Holds>,
}

#[derive(Default)]
struct State {
    /// Objects swept so far, ever; each swept object is numbered by it.
    sweeps: u64,
    /// Each remembered swept object, by its number.
    swept: HashMap<ObjectId, u64>,
    /// The sweep count when each remembered collection began, oldest first.
    collections: VecDeque<u64>,
    /// Sweeps numbered at most this are forgotten.
    forgotten: u64,
    /// While a collection runs, every closure admitted since it began.
    admitted: Option<HashSet<ObjectId>>,
}

/// What one open handle holds live.
#[derive(Clone, Debug)]
pub(crate) enum Held {
    /// An open checkout's roots.
    Checkout(HeldRoots),
    /// A detached file's record.
    Record {
        record: crate::kernel::FileRecord,
        config: crate::model::VolumeConfig,
    },
}

/// The roots one open checkout holds live.
#[derive(Clone, Debug)]
pub(crate) struct HeldRoots {
    /// The generation the checkout is based on.
    pub(crate) base: ObjectId,
    /// The root of the checkout's working tree.
    pub(crate) working: GenerationRoot,
    /// The checkout's volume configuration, which bounds its pages.
    pub(crate) config: crate::model::VolumeConfig,
    // Last: the real held Vec is destroyed before its owner-local tag.
    control: crate::kernel::ProofDataClaim,
}

/// Keeps a publication's admitted closure from being swept until its record
/// is durable; the publication drops it after writing the record.
#[must_use = "a publication holds this until its record is written"]
pub struct PublicationHold {
    _gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl PublicationHold {
    /// Empty local hold for a provider that owns its durability and collection
    /// gate elsewhere. This grants no remote root claim, historical custody,
    /// acceptance, collection permission, or release permission.
    pub const fn none() -> Self {
        Self { _gate: None }
    }
}

/// Proof that no sweep batch runs: the collection gate, held shared.
/// Writing to the durable store while a collection runs requires one, so a
/// write lands either before a batch chooses what to sweep or after it.
pub(crate) struct Unswept(tokio::sync::OwnedRwLockReadGuard<()>);

impl Unswept {
    /// Holds sweeping off until the publication's record is written.
    pub(crate) fn into_hold(self) -> PublicationHold {
        PublicationHold {
            _gate: Some(self.0),
        }
    }
}

/// A collection in progress; see [`Collection::begin`].
pub(crate) struct Collecting {
    collection: Arc<Collection>,
    _running: tokio::sync::OwnedMutexGuard<()>,
}

impl Collection {
    /// The sweep count a proof records before it starts.
    pub fn sweeps(&self) -> u64 {
        self.state().sweeps
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Admits a closure proven when the sweep count was `proven_at`, for a
    /// record about to name it, and keeps sweeping off until the returned
    /// hold drops. Objects `staged` answers for are stored again before the
    /// record is written, so only the others must have survived.
    ///
    /// # Errors
    ///
    /// Refuses a closure with an unstaged object swept after `proven_at`, or
    /// one proven before every remembered collection.
    pub(crate) async fn admit(
        &self,
        closure: &[ObjectId],
        staged: impl Fn(&ObjectId) -> bool,
        proven_at: u64,
    ) -> Result<Unswept, ObjectStoreError> {
        let hold = self.unswept().await;
        let mut state = self.state();
        let refused = proven_at < state.sweeps
            && (proven_at < state.forgotten
                || closure.iter().any(|object| {
                    !staged(object)
                        && state
                            .swept
                            .get(object)
                            .is_some_and(|swept| *swept > proven_at)
                }));
        if refused {
            return Err(ObjectStoreError::Rejected(
                "a collection removed part of the closure after it was proven".to_owned(),
            ));
        }
        if let Some(admitted) = &mut state.admitted {
            admitted.extend(closure.iter().copied());
        }
        Ok(hold)
    }

    /// Starts a collection once every publication admitted before it has
    /// written its record: every closure admitted while it runs is kept.
    pub(crate) async fn begin(self: &Arc<Self>) -> Collecting {
        // One at a time: a collection owns the admitted set until it ends.
        let running = Arc::clone(&self.running).lock_owned().await;
        let _admitted_before = self.gate.write().await;
        let mut state = self.state();
        let began = state.sweeps;
        state.collections.push_back(began);
        if state.collections.len() > REMEMBERED_COLLECTIONS {
            state.collections.pop_front();
            let forgotten = state.collections.front().copied().unwrap_or(began);
            state.swept.retain(|_, swept| *swept > forgotten);
            state.forgotten = forgotten;
        }
        state.admitted = Some(HashSet::new());
        Collecting {
            collection: Arc::clone(self),
            _running: running,
        }
    }

    /// Holds sweeping off until the returned proof drops.
    pub(crate) async fn unswept(&self) -> Unswept {
        Unswept(Arc::clone(&self.gate).read_owned().await)
    }

    /// Records `objects`, about to be written to the durable store while
    /// `unswept` holds sweeping off, so a running collection keeps them: the
    /// write lands either before a sweep batch chooses (which then keeps
    /// them) or after its deletions (which the write undoes by storing them).
    pub(crate) fn writing(&self, _unswept: &Unswept, objects: impl IntoIterator<Item = ObjectId>) {
        if let Some(admitted) = &mut self.state().admitted {
            admitted.extend(objects);
        }
    }

    /// What every open handle holds.
    pub(crate) fn held(&self) -> Vec<Held> {
        self.holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .iter()
            .filter_map(|slot| slot.held.clone())
            .collect()
    }

    fn register(self: &Arc<Self>, held: Held) -> Hold {
        let mut holds = self.holds.lock().unwrap_or_else(PoisonError::into_inner);
        let id = if let Some(id) = holds.free {
            let slot = holds.slots.get_mut(id).expect("free hold slot exists");
            let next = slot.next_free.take();
            slot.held = Some(held);
            holds.free = next;
            id
        } else {
            let id = holds.slots.len();
            holds.slots.push(Slot { held: Some(held), next_free: None });
            id
        };
        Hold { collection: Arc::clone(self), id }
    }

    fn register_measured(
        self: &Arc<Self>,
        held: Held,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Hold, crate::FsError> {
        cancellation.check()?;
        let mut holds = self.holds.lock().unwrap_or_else(PoisonError::into_inner);
        if holds.free.is_none() && holds.slots.len() == holds.slots.capacity() {
            let old_capacity = holds.slots.capacity();
            let next_capacity = old_capacity.checked_mul(2)
                .map(|capacity| capacity.max(1))
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            let old_bytes = crate::foundation::usize_to_u64(
                old_capacity.checked_mul(std::mem::size_of::<Slot>())
                    .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?,
            );
            allocations.claim_bytes(old_bytes, 0, work, budget).map_err(handle_allocation_error)?;
            allocations.claim_elements::<Slot>(next_capacity, work, budget).map_err(handle_allocation_error)?;
            let mut slots = Vec::new();
            slots.try_reserve_exact(next_capacity).map_err(|_| crate::FsError::HandleAllocationFailed)?;
            let actual_capacity = slots.capacity();
            let excess = actual_capacity.checked_sub(next_capacity)
                .ok_or(crate::FsError::HandleAllocationFailed)?;
            if excess != 0 {
                let excess_bytes = excess.checked_mul(std::mem::size_of::<Slot>())
                    .map(crate::foundation::usize_to_u64)
                    .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
                allocations.claim_bytes(excess_bytes, 0, work, WorkBudget::UNBOUNDED).map_err(handle_allocation_error)?;
                work.verify(budget)?;
            }
            charge_handle_copy(old_bytes, work, budget)?;
            slots.append(&mut holds.slots);
            holds.slots = slots;
            let new_bytes = actual_capacity.checked_mul(std::mem::size_of::<Slot>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            allocations.release(old_bytes).map_err(handle_allocation_error)?;
            allocations.release(new_bytes).map_err(handle_allocation_error)?;
        }
        // The handle retains its occupied slot even when an existing free slot
        // required no backing allocation. Free backing capacity stays global.
        allocations.claim_bytes(crate::foundation::usize_to_u64(std::mem::size_of::<Slot>()), 0, work, budget)
            .map_err(handle_allocation_error)?;
        let id = if let Some(id) = holds.free {
            let slot = holds.slots.get_mut(id).ok_or(crate::FsError::HandleAllocationFailed)?;
            let next = slot.next_free.take();
            slot.held = Some(held);
            holds.free = next;
            id
        } else {
            let id = holds.slots.len();
            holds.slots.push(Slot { held: Some(held), next_free: None });
            id
        };
        Ok(Hold { collection: Arc::clone(self), id })
    }

    pub(crate) fn hold_record_measured(
        collection: Option<&Arc<Self>>,
        record: crate::kernel::FileRecord,
        config: crate::model::VolumeConfig,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<Hold>, crate::FsError> {
        collection.map(|collection| collection.register_measured(
            Held::Record { record, config }, allocations, work, budget, cancellation,
        )).transpose()
    }

    /// Keeps what a detached file's `record` reaches for as long as the
    /// returned hold lives.
    pub(crate) fn hold_record(
        collection: Option<&Arc<Self>>,
        record: crate::kernel::FileRecord,
        config: crate::model::VolumeConfig,
    ) -> Option<Hold> {
        collection.map(|collection| collection.register(Held::Record { record, config }))
    }
}

impl Collecting {
    /// Takes the gate for one sweep batch, so no publication admits a
    /// closure until the returned guard drops, and returns the candidates in
    /// `batch` that nothing admitted since the collection began.
    pub(crate) async fn sweepable(
        &self,
        batch: Vec<ObjectId>,
    ) -> (tokio::sync::OwnedRwLockWriteGuard<()>, Vec<ObjectId>) {
        let guard = Arc::clone(&self.collection.gate).write_owned().await;
        let state = self.collection.state();
        let sweepable = batch
            .into_iter()
            .filter(|object| {
                !state
                    .admitted
                    .as_ref()
                    .is_some_and(|admitted| admitted.contains(object))
            })
            .collect();
        (guard, sweepable)
    }

    /// Numbers `object`, about to be swept under the gate of
    /// [`Self::sweepable`], so a closure proven before now that names it is
    /// refused.
    pub(crate) fn sweeping(&self, object: ObjectId) {
        let mut state = self.collection.state();
        state.sweeps += 1;
        let sweep = state.sweeps;
        state.swept.insert(object, sweep);
    }
}

impl Drop for Collecting {
    fn drop(&mut self) {
        self.collection.state().admitted = None;
    }
}

/// An open handle's registration; dropping it releases what it holds.
pub(crate) struct Hold {
    collection: Arc<Collection>,
    id: usize,
}

impl Hold {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let holds = self.collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
        let held = holds.slots.get(self.id).and_then(|slot| slot.held.as_ref())
            .expect("live hold remains registered until drop");
        held_owned_bytes(held)
    }

    fn update(&self, held: Held) {
        let mut holds = self.collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = holds.slots.get_mut(self.id) {
            slot.held = Some(held);
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut holds = self.collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
        let next = holds.free;
        if let Some(slot) = holds.slots.get_mut(self.id) {
            slot.held = None;
            slot.next_free = next;
            holds.free = Some(self.id);
        }
    }
}

/// A checkout's base and working roots, which a collection treats as live.
/// They change only through [`Self::set`] and [`Self::set_working`], which
/// keep the registration exact.
pub(crate) struct CheckoutRoots {
    base: ObjectId,
    working: GenerationRoot,
    config: crate::model::VolumeConfig,
    hold: Option<Hold>,
    // Last: both working and collection-held root buffers retire first.
    working_control: crate::kernel::ProofDataClaim,
}

impl CheckoutRoots {
    pub(crate) fn new(
        collection: Option<&Arc<Collection>>,
        config: crate::model::VolumeConfig,
        base: ObjectId,
        working: GenerationRoot,
    ) -> Self {
        let hold = collection.map(|collection| {
            collection.register(Held::Checkout(HeldRoots {
                base,
                working: working.clone(),
                config,
                control: crate::kernel::ProofDataClaim::default(),
            }))
        });
        Self {
            base,
            working,
            config,
            hold,
            working_control: crate::kernel::ProofDataClaim::default(),
        }
    }

    pub(crate) fn new_measured(
        collection: Option<&Arc<Collection>>,
        config: crate::model::VolumeConfig,
        base: ObjectId,
        working: GenerationRoot,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, crate::FsError> {
        Self::new_measured_with_control(
            collection, config, base, (working, crate::kernel::ProofDataClaim::default()),
            allocations, work, budget, cancellation, None,
        )
    }

    /// Moves the decoder's same already-admitted working buffer and tag while
    /// attaching exact held copies before any registration can fail.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new_native_measured(
        collection: Option<&Arc<Collection>>,
        config: crate::model::VolumeConfig,
        base: ObjectId,
        working: (GenerationRoot, crate::kernel::ProofDataClaim),
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
        owner: &Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<Self, crate::FsError> {
        Self::new_measured_with_control(
            collection, config, base, working, allocations, work, budget, cancellation, Some(owner),
        )
    }

    fn new_measured_with_control(
        collection: Option<&Arc<Collection>>,
        config: crate::model::VolumeConfig,
        base: ObjectId,
        working: (GenerationRoot, crate::kernel::ProofDataClaim),
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
        native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<Self, crate::FsError> {
        let hold = match collection {
            Some(collection) => {
                let held_working = clone_generation_root_with_control(
                    &working.0, allocations, work, budget, native_owner,
                )?;
                Some(collection.register_measured(
                    Held::Checkout(HeldRoots {
                        base, working: held_working.0, config, control: held_working.1,
                    }),
                    allocations, work, budget, cancellation,
                )?)
            }
            None => None,
        };
        Ok(Self {
            base, working: working.0, config, hold, working_control: working.1,
        })
    }

    pub(crate) fn replicate_measured(
        &self,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, crate::FsError> {
        self.replicate_with_control(allocations, work, budget, cancellation, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn replicate_native_measured(
        &self,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
        owner: &Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<Self, crate::FsError> {
        self.replicate_with_control(allocations, work, budget, cancellation, Some(owner))
    }

    fn replicate_with_control(
        &self,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
        native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<Self, crate::FsError> {
        let working = clone_generation_root_with_control(
            &self.working, allocations, work, budget, native_owner,
        )?;
        Self::new_measured_with_control(
            self.hold.as_ref().map(|hold| &hold.collection), self.config,
            self.base, working, allocations, work, budget, cancellation, native_owner,
        )
    }

    /// Verifies the exact working/held vectors that move into an original
    /// pinned reader. Cached slot storage and token lineage prove no payment.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn same_original_scope(
        &self, original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        if !self.working_control.is_native_original(original, root_parent_bytes(&self.working)) {
            return false;
        }
        let Some(hold) = &self.hold else { return true; };
        let holds = hold.collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
        match holds.slots.get(hold.id).and_then(|slot| slot.held.as_ref()) {
            Some(Held::Checkout(roots)) => roots.control.is_native_original(
                original, root_parent_bytes(&roots.working),
            ),
            _ => false,
        }
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        crate::foundation::usize_to_u64(self.working.parents.capacity() * std::mem::size_of::<crate::GenerationId>())
            + self.hold.as_ref().map_or(0, Hold::logical_owned_bytes)
    }

    #[cfg(test)]
    pub(crate) const fn has_hold(&self) -> bool {
        self.hold.is_some()
    }

    /// Only freshly cloned, measured root buffers receive these exact tags.
    /// Collection slot capacity is cached shared storage, not handle custody.
    pub(crate) fn attach_control_custody(&mut self, owner: &Arc<crate::kernel::ProofDataCustody>) {
        self.working_control = crate::kernel::ProofDataClaim::control(owner, root_parent_bytes(&self.working));
        if let Some(hold) = &self.hold {
            let mut holds = hold.collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
            let held = holds.slots.get_mut(hold.id).and_then(|slot| slot.held.as_mut())
                .expect("a freshly admitted hold remains registered");
            if let Held::Checkout(roots) = held {
                roots.control = crate::kernel::ProofDataClaim::control(owner, root_parent_bytes(&roots.working));
            }
        }
    }


    pub(crate) fn prepare_held_root(
        &self,
        working: &GenerationRoot,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<Option<GenerationRoot>, crate::FsError> {
        self.hold.as_ref().map(|_| clone_generation_root(working, allocations, work, budget)).transpose()
    }

    /// Copies only the publication-held root, charging its actual primitive
    /// buffer to the original checkout control owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_held_root_native(
        &self,
        working: &GenerationRoot,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        owner: &Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<Option<(GenerationRoot, crate::kernel::ProofDataClaim)>, crate::FsError> {
        self.hold.as_ref().map(|_| {
            clone_generation_root_native(working, allocations, work, budget, owner)
        }).transpose()
    }

    /// Moves already admitted root copies after the publication seal.
    /// Replacements destroy each old vector before retiring its tag.
    pub(crate) fn set_prepared_root(
        &mut self,
        base: ObjectId,
        working: GenerationRoot,
        control: crate::kernel::ProofDataClaim,
        held: Option<(GenerationRoot, crate::kernel::ProofDataClaim)>,
    ) {
        self.base = base;
        self.set_prepared_working(working, control, held);
    }

    /// Replaces only the working tree with admitted buffers; base lineage and
    /// the checkout's base-root copy remain unchanged.
    pub(crate) fn set_prepared_working(
        &mut self,
        working: GenerationRoot,
        control: crate::kernel::ProofDataClaim,
        held: Option<(GenerationRoot, crate::kernel::ProofDataClaim)>,
    ) {
        if let Some(hold) = &self.hold {
            let (working, control) = held.expect("the existing registration has a prepared root");
            hold.update(Held::Checkout(HeldRoots { base: self.base, working, config: self.config, control }));
        } else {
            debug_assert!(held.is_none());
        }
        self.working = working;
        self.working_control = control;
    }

    /// The generation the checkout is based on.
    pub(crate) const fn base(&self) -> ObjectId {
        self.base
    }

    /// The same roots, registered again for a replica of the checkout.
    pub(crate) fn replicate(&self) -> Self {
        Self::new(
            self.hold.as_ref().map(|hold| &hold.collection),
            self.config,
            self.base,
            self.working.clone(),
        )
    }

    /// Rebases the checkout onto `base` with the working tree `working`.
    pub(crate) fn set(&mut self, base: ObjectId, working: GenerationRoot) {
        self.base = base;
        self.set_working(working);
    }

    /// Replaces the working tree.
    pub(crate) fn set_working(&mut self, working: GenerationRoot) {
        if let Some(hold) = &self.hold {
            hold.update(Held::Checkout(HeldRoots {
                base: self.base,
                working: working.clone(),
                config: self.config,
                control: crate::kernel::ProofDataClaim::default(),
            }));
        }
        self.working = working;
        self.working_control = crate::kernel::ProofDataClaim::default();
    }
}

impl PartialEq for CheckoutRoots {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.working == other.working
    }
}

impl Deref for CheckoutRoots {
    type Target = GenerationRoot;

    fn deref(&self) -> &GenerationRoot {
        &self.working
    }
}

fn root_parent_bytes(root: &GenerationRoot) -> u64 {
    crate::foundation::usize_to_u64(root.parents.capacity() * std::mem::size_of::<crate::GenerationId>())
}

fn held_owned_bytes(held: &Held) -> u64 {
    let parents = match held {
        Held::Checkout(roots) => roots.working.parents.capacity() * std::mem::size_of::<crate::GenerationId>(),
        Held::Record { .. } => 0,
    };
    crate::foundation::usize_to_u64(std::mem::size_of::<Slot>() + parents)
}

pub(crate) fn handle_allocation_error(error: AllocationError) -> crate::FsError {
    error.into_work_or(crate::FsError::HandleAllocationFailed)
}

pub(crate) fn charge_handle_copy(
    bytes: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), crate::FsError> {
    let charged = work.checked_add(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })?;
    charged.verify(budget)?;
    *work = charged;
    Ok(())
}

pub(crate) fn clone_generation_root(
    root: &GenerationRoot,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<GenerationRoot, crate::FsError> {
    clone_generation_root_with_control(root, allocations, work, budget, None)
        .map(|(root, _)| root)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn clone_generation_root_native(
    root: &GenerationRoot,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    owner: &Arc<crate::kernel::ProofDataCustody>,
) -> Result<(GenerationRoot, crate::kernel::ProofDataClaim), crate::FsError> {
    clone_generation_root_with_control(root, allocations, work, budget, Some(owner))
}

fn clone_generation_root_with_control(
    root: &GenerationRoot,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
) -> Result<(GenerationRoot, crate::kernel::ProofDataClaim), crate::FsError> {
    let requested = crate::foundation::usize_to_u64(
        root.parents.len() * std::mem::size_of::<crate::GenerationId>(),
    );
    allocations.claim_elements::<crate::GenerationId>(root.parents.len(), work, budget)
        .map_err(handle_allocation_error)?;
    let mut parents = Vec::new();
    if parents.try_reserve_exact(root.parents.len()).is_err() {
        drop(parents);
        allocations.release(requested).map_err(handle_allocation_error)?;
        return Err(crate::FsError::HandleAllocationFailed);
    }
    let actual = crate::foundation::usize_to_u64(
        parents.capacity() * std::mem::size_of::<crate::GenerationId>(),
    );
    let excess = actual.saturating_sub(requested);
    if excess != 0 {
        if let Err(error) = allocations.claim_bytes(excess, 0, work, WorkBudget::UNBOUNDED) {
            drop(parents);
            allocations.release(requested).map_err(handle_allocation_error)?;
            return Err(handle_allocation_error(error));
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    let control = native_owner.map(|owner| crate::kernel::ProofDataClaim::native_control(owner, actual))
        .unwrap_or_default();
    #[cfg(target_arch = "wasm32")]
    let control = {
        debug_assert!(native_owner.is_none());
        crate::kernel::ProofDataClaim::default()
    };
    // The actual buffer precedes its tag through every fallible copy/registration
    // boundary, including a capacity correction that exceeds the original budget.
    let mut parents = (parents, control);
    if excess != 0 { work.verify(budget)?; }
    charge_handle_copy(requested, work, budget)?;
    parents.0.extend_from_slice(&root.parents);
    Ok((GenerationRoot {
        volume_id: root.volume_id, root_file_id: root.root_file_id, file_table: root.file_table,
        parents: parents.0, required_features: root.required_features,
    }, parents.1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ObjectId, ObjectKind};

    fn object(byte: u8) -> ObjectId {
        ObjectId {
            kind: ObjectKind::Blob,
            digest: crate::foundation::Digest::from_bytes([byte; 32]),
        }
    }

    fn checkout_hold() -> Held {
        Held::Checkout(HeldRoots {
            base: object(1),
            working: GenerationRoot {
                volume_id: crate::VolumeId::new(),
                root_file_id: crate::FileId::new(),
                file_table: ObjectId { kind: ObjectKind::FileTablePage, ..object(2) },
                parents: Vec::new(),
                required_features: 0,
            },
            config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
            control: crate::kernel::ProofDataClaim::default(),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_native_root_copy_and_registration_failures_retire_actual_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        let custody = meter.with_original_leaf(|original| -> Result<_, Box<dyn std::error::Error>> {
            let (mut sink, allocations, work, budget, _) = original.native_checkout_control_parts(&meter);
            allocations.claim_bytes(std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64, 1, work, budget)?;
            let custody = Arc::new(crate::kernel::ProofDataCustody::default());
            sink.register(&custody, allocations, work, budget)?;
            Ok(custody)
        })?;
        let baseline = meter.with_original_leaf(|original| original.allocation_parts().0.live_bytes());
        let Held::Checkout(mut roots) = checkout_hold() else { unreachable!() };
        roots.working.parents = vec![crate::GenerationId::new(object(7).digest)];
        let denied = meter.with_original_leaf(|original| {
            let (allocations, work, _, _) = original.allocation_parts();
            clone_generation_root_native(
                &roots.working, allocations, work,
                WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &custody,
            )
        }).err().ok_or("zero copy budget copied a native parent buffer")?;
        assert!(matches!(denied, crate::FsError::Work(_)));
        let (born, retired) = custody.native_control_events();
        assert!(born >= std::mem::size_of::<crate::GenerationId>() as u128);
        assert_eq!(born, retired, "a copy-denied actual vector drops its tag");
        assert_eq!(custody.physical_retained_bytes(), 0);
        meter.with_original_leaf(|original| original.reconcile_native_controls())?;
        assert_eq!(meter.with_original_leaf(|original| original.allocation_parts().0.live_bytes()), baseline);
        let collection = Arc::new(Collection::default());
        let source = CheckoutRoots::new(Some(&collection), roots.config, roots.base, roots.working);
        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        let failed = meter.with_original_leaf(|original| {
            let (allocations, work, budget, _) = original.allocation_parts();
            source.replicate_native_measured(allocations, work, budget, &cancelled, &custody)
        }).err().ok_or("cancelled held registration installed a native replica")?;
        assert!(matches!(failed, crate::FsError::Cancelled(_)));
        let (next_born, next_retired) = custody.native_control_events();
        assert!(next_born > born, "both actual working and held copies reached allocation");
        assert_eq!(next_born, next_retired);
        assert_eq!(custody.physical_retained_bytes(), 0);
        meter.with_original_leaf(|original| original.reconcile_native_controls())?;
        assert_eq!(meter.with_original_leaf(|original| original.allocation_parts().0.live_bytes()), baseline);
        let mut paid = meter.with_original_leaf(|original| {
            let (allocations, work, budget, _) = original.allocation_parts();
            source.replicate_native_measured(allocations, work, budget, &cancellation, &custody)
        })?;
        assert!(paid.same_original_scope(&meter));
        let foreign = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        assert!(!paid.same_original_scope(&foreign));
        let held_control = {
            let mut holds = collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
            let held = holds.slots.get_mut(paid.hold.as_ref().expect("paid registration").id)
                .and_then(|slot| slot.held.as_mut()).expect("actual held vector");
            let Held::Checkout(roots) = held else { unreachable!() };
            std::mem::take(&mut roots.control)
        };
        assert!(!paid.same_original_scope(&meter), "a paid working vector does not certify an untagged held copy");
        {
            let mut holds = collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
            let held = holds.slots.get_mut(paid.hold.as_ref().expect("paid registration").id)
                .and_then(|slot| slot.held.as_mut()).expect("actual held vector");
            let Held::Checkout(roots) = held else { unreachable!() };
            roots.control = held_control;
        }
        assert!(paid.same_original_scope(&meter));
        paid.set_working(source.working.clone());
        assert!(custody.is_native_original(&meter), "token lineage is unchanged by an ordinary replacement");
        assert!(!paid.same_original_scope(&meter), "token lineage cannot certify the new unadmitted vectors");
        let Held::Checkout(empty) = checkout_hold() else { unreachable!() };
        let empty = CheckoutRoots::new(None, empty.config, empty.base, empty.working);
        assert!(empty.same_original_scope(&meter), "actual zero-capacity buffers require no allocation tag");
        Ok(())
    }

    #[test]
    fn private_root_vector_tags_retire_actual_buffers_without_adopting_cached_slots()
    -> Result<(), Box<dyn std::error::Error>> {
        let collection = Arc::new(Collection::default());
        let cancellation = crate::CancellationToken::new();
        let Held::Checkout(mut roots) = checkout_hold() else { unreachable!() };
        roots.working.parents = vec![crate::GenerationId::new(object(7).digest)];
        let original = CheckoutRoots::new(
            Some(&collection), roots.config, roots.base, roots.working,
        );
        let custody = Arc::new(crate::kernel::ProofDataCustody::default());
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let mut measured = original.replicate_measured(
            &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        measured.attach_control_custody(&custody);
        let (copied_held, held_bytes) = {
            let holds = collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
            let held = holds.slots.get(measured.hold.as_ref().expect("registered clone").id)
                .and_then(|slot| slot.held.as_ref()).expect("live cloned roots");
            let Held::Checkout(roots) = held else { unreachable!() };
            (held.clone(), root_parent_bytes(&roots.working))
        };
        let bytes = root_parent_bytes(&measured.working) + held_bytes;
        assert_eq!(custody.physical_retained_bytes(), bytes);
        assert_eq!(custody.retained_bytes(), 0);
        assert!(work.peak_allocation_bytes >= bytes);
        drop(measured);
        assert_eq!(custody.physical_retained_bytes(), 0,
            "snapshot clones never inherit original vector tags");
        assert!(collection.holds.lock().unwrap_or_else(PoisonError::into_inner).slots.capacity() > 0,
            "freed per-handle buffers do not destroy or adopt cached slot backing");
        drop(copied_held);
        let mut measured = original.replicate_measured(
            &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        measured.attach_control_custody(&custody);
        assert!(custody.physical_retained_bytes() > 0);
        measured.set_working(original.working.clone());
        assert_eq!(custody.physical_retained_bytes(), 0,
            "unmeasured replacements retire old buffers without phantom adoption");
        drop(measured);
        drop(original);
        Ok(())
    }

    #[test]
    fn bounded_registration_preserves_roots_and_reuses_vacant_slots()
    -> Result<(), Box<dyn std::error::Error>> {
        let collection = Arc::new(Collection::default());
        let cancellation = crate::CancellationToken::new();
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        assert!(collection.register_measured(
            checkout_hold(), &mut allocations, &mut work, WorkBudget::default(), &cancellation,
        ).is_err());
        assert!(collection.held().is_empty());
        assert_eq!(work, WorkCounters::default());

        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        let held = collection.register_measured(
            checkout_hold(), &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        assert_eq!(collection.held().len(), 1);
        assert_eq!(work.allocation_operations, 1);
        assert_eq!(held.logical_owned_bytes(), crate::foundation::usize_to_u64(std::mem::size_of::<Slot>()));

        let mut extra = Vec::new();
        loop {
            let full = {
                let holds = collection.holds.lock().unwrap_or_else(PoisonError::into_inner);
                holds.slots.len() == holds.slots.capacity()
            };
            if full {
                break;
            }
            extra.push(collection.register(checkout_hold()));
        }
        let retained_roots = collection.held().len();

        let mut limited = WorkBudget::UNBOUNDED;
        limited.bytes_copied = 0;
        let mut failed_work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        assert!(collection.register_measured(
            checkout_hold(), &mut allocations, &mut failed_work, limited, &cancellation,
        ).is_err());
        assert_eq!(collection.held().len(), retained_roots, "failed growth does not unregister old roots");
        assert_eq!(failed_work.allocation_operations, 1, "the attempted backing allocation remains measured");
        drop(held);
        drop(extra);
        assert!(collection.held().is_empty());

        let mut reused_work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        let mut reuse_budget = WorkBudget::UNBOUNDED;
        reuse_budget.allocation_operations = 0;
        let reused = collection.register_measured(
            checkout_hold(), &mut allocations, &mut reused_work, reuse_budget, &cancellation,
        )?;
        assert_eq!(reused_work.allocation_operations, 0);
        assert_eq!(collection.held().len(), 1);
        drop(reused);
        assert!(collection.held().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_closure_proven_before_a_sweep_of_its_object_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let collection = Arc::new(Collection::default());
        let proven_at = collection.sweeps();
        let collecting = collection.begin().await;
        let (gate, sweepable) = collecting.sweepable(vec![object(1), object(2)]).await;
        assert_eq!(sweepable, vec![object(1), object(2)]);
        for swept in sweepable {
            collecting.sweeping(swept);
        }
        drop(gate);
        assert!(
            collection
                .admit(&[object(1)], |_| false, proven_at)
                .await
                .is_err()
        );
        // A staged copy is stored again by the publication's drain.
        drop(collection.admit(&[object(1)], |_| true, proven_at).await?);
        // A proof made after the sweep read the store as it is.
        drop(
            collection
                .admit(&[object(1)], |_| false, collection.sweeps())
                .await?,
        );
        drop(collection.admit(&[object(3)], |_| false, proven_at).await?);
        Ok(())
    }

    #[tokio::test]
    async fn a_closure_admitted_while_collecting_is_kept() -> Result<(), Box<dyn std::error::Error>>
    {
        let collection = Arc::new(Collection::default());
        let collecting = collection.begin().await;
        drop(
            collection
                .admit(&[object(1)], |_| false, collection.sweeps())
                .await?,
        );
        let (_gate, sweepable) = collecting.sweepable(vec![object(1), object(2)]).await;
        assert_eq!(sweepable, vec![object(2)]);
        Ok(())
    }

    #[tokio::test]
    async fn a_collection_begins_after_every_admitted_record_is_written()
    -> Result<(), Box<dyn std::error::Error>> {
        let collection = Arc::new(Collection::default());
        let hold = collection.admit(&[object(1)], |_| false, 0).await?;
        let begun = tokio::spawn({
            let collection = Arc::clone(&collection);
            async move { drop(collection.begin().await) }
        });
        tokio::task::yield_now().await;
        assert!(!begun.is_finished(), "the collection waits for the record");
        drop(hold);
        begun.await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_proof_older_than_every_remembered_collection_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let collection = Arc::new(Collection::default());
        let proven_at = collection.sweeps();
        for round in 0..=REMEMBERED_COLLECTIONS {
            let collecting = collection.begin().await;
            let (_gate, _) = collecting.sweepable(Vec::new()).await;
            collecting.sweeping(object(u8::try_from(round)? + 10));
        }
        assert!(
            collection
                .admit(&[object(1)], |_| false, proven_at)
                .await
                .is_err()
        );
        Ok(())
    }
}
