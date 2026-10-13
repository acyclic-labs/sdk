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
use crate::kernel::allocation::{
    AllocationError, KernelAccounting, KernelOwnedAllocation,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{ObjectId, ObjectStoreError};

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

mod buffers;
mod holds;
mod roots;
pub(crate) use buffers::{
    charge_handle_copy, clone_generation_root, clone_generation_root_with_accounting,
    handle_allocation_error,
};
use buffers::{held_owned_bytes, root_parent_bytes};
pub(crate) use holds::Hold;
use holds::{Holds, Slot};
pub(crate) use roots::CheckoutRoots;
#[cfg(test)]
mod tests;
