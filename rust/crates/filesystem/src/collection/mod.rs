//! Collection roots and admission while a live store keeps writing.

#![cfg_attr(
    not(all(feature = "local", not(target_arch = "wasm32"))),
    allow(dead_code, reason = "only a local store collects")
)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Mutex, PoisonError};

use crate::kernel::GenerationRoot;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::performance::WorkCounters;
use crate::storage::{ObjectId, ObjectStoreError};

const REMEMBERED_COLLECTIONS: usize = 4;

#[derive(Default)]
/// Shared collection state for a store and its checkouts.
pub struct Collection {
    /// The current generation's gate. Each admission holds it until its
    /// publication is durable; a collection retires it for a fresh one and
    /// waits only on the retired one, so no admission ever waits on a
    /// collection's drain.
    gate: Mutex<Arc<tokio::sync::RwLock<()>>>,
    running: Arc<tokio::sync::Mutex<()>>,
    epochs: Epochs,
    state: Mutex<State>,
    holds: Mutex<Holds>,
    /// Signalled whenever an admission waits on a gate.
    #[cfg(test)]
    pub(crate) gate_waits: tokio::sync::Notify,
    /// Signalled when a collection starts waiting on admitted publications.
    #[cfg(test)]
    pub(crate) draining: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    sweeps: u64,
    // Keyed by blob digest: objects of different kinds with equal bytes share one blob.
    swept: HashMap<crate::Digest, u64>,
    collections: VecDeque<u64>,
    forgotten: u64,
    admitted: Option<HashSet<crate::Digest>>,
    /// Retired gates not yet drained. A collection dropped mid-drain leaves
    /// them here, and the next one drains them before it marks.
    retiring: Vec<Arc<tokio::sync::RwLock<()>>>,
}

#[derive(Clone, Debug)]
pub(crate) enum Held {
    Checkout {
        base: ObjectId,
        working: GenerationRoot,
        config: crate::model::VolumeConfig,
        // Last: the held buffer retires before its owner-local tag.
        control: crate::kernel::ProofDataClaim,
    },
    Record {
        record: crate::kernel::FileRecord,
        config: crate::model::VolumeConfig,
    },
    Generation {
        root: ObjectId,
        config: crate::model::VolumeConfig,
    },
}

#[must_use = "a publication holds this until its record is written"]
/// Keeps an admitted closure live until its publication is durable.
pub struct PublicationHold {
    _gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    _inner: Option<Box<PublicationHold>>,
    proof: Option<crate::kernel::closure::PendingProof>,
}

impl PublicationHold {
    /// An empty hold for stores with their own collection gate.
    pub const fn none() -> Self {
        Self {
            _gate: None,
            _inner: None,
            proof: None,
        }
    }

    /// Also holds the hold of the store this one composes over.
    pub(crate) fn with(mut self, inner: Self) -> Self {
        if inner._gate.is_some() || inner._inner.is_some() || inner.proof.is_some() {
            self._inner = Some(Box::new(inner));
        }
        self
    }

    /// Keeps the original proof unpublished until this hold's creation commits.
    pub(crate) fn with_proof(
        mut self,
        proof: Option<crate::kernel::closure::PendingProof>,
    ) -> Self {
        self.proof = proof;
        self
    }

    /// The authoritative creation outcome, before any fallible cache refresh.
    pub(crate) fn created(&mut self) {
        let mut hold = self;
        loop {
            if let Some(proof) = hold.proof.take() {
                proof.created();
            }
            let Some(inner) = hold._inner.as_deref_mut() else {
                return;
            };
            hold = inner;
        }
    }
}

pub(crate) struct Unswept(tokio::sync::OwnedRwLockReadGuard<()>);

impl Unswept {
    pub(crate) fn into_hold(self) -> PublicationHold {
        PublicationHold {
            _gate: Some(self.0),
            _inner: None,
            proof: None,
        }
    }
}

/// The one collection allowed to run, from before it reads its epoch.
pub(crate) struct Running {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct Collecting {
    collection: Arc<Collection>,
    _running: Running,
}

impl Collection {
    /// Returns the cumulative sweep count.
    pub fn sweeps(&self) -> u64 {
        self.state().sweeps
    }

    /// Collections begun, for cached proofs that must not outlive one.
    pub(crate) fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn holds(&self) -> std::sync::MutexGuard<'_, Holds> {
        self.holds.lock().unwrap_or_else(PoisonError::into_inner)
    }

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
                            .get(&object.digest)
                            .is_some_and(|swept| *swept > proven_at)
                }));
        if refused {
            return Err(ObjectStoreError::Rejected(
                "a collection removed part of the closure after it was proven".to_owned(),
            ));
        }
        if let Some(admitted) = &mut state.admitted {
            admitted.extend(closure.iter().map(|object| object.digest));
        }
        Ok(hold)
    }

    pub(crate) async fn begin(self: &Arc<Self>) -> Collecting {
        let running = self.run().await;
        self.begin_then(running, || {}).await
    }

    /// Waits until no other collection runs. A collection owns the admitted
    /// set and the epoch until it ends.
    pub(crate) async fn run(&self) -> Running {
        Running {
            _guard: Arc::clone(&self.running).lock_owned().await,
        }
    }

    /// Begins a collection. `opened` runs as the current generation retires:
    /// every later admission joins the next generation, and the collection
    /// returns once every admission of every retired one has finished.
    pub(crate) async fn begin_then(
        self: &Arc<Self>,
        running: Running,
        opened: impl FnOnce(),
    ) -> Collecting {
        // Admissions of the next generation are recorded from its start.
        self.state().admitted = Some(HashSet::new());
        let retired = {
            let mut gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
            opened();
            std::mem::take(&mut *gate)
        };
        let retiring = {
            let mut state = self.state();
            state.retiring.push(retired);
            state.retiring.clone()
        };
        for retired in retiring {
            // A retired generation with no publication in flight drains at once.
            if retired.try_write().is_err() {
                #[cfg(test)]
                self.draining.notify_one();
                drop(retired.write().await);
            }
        }
        self.state().retiring.clear();
        // Before marking: a proof cached under an earlier epoch is never reused.
        self.epochs.begin();
        let mut state = self.state();
        let began = state.sweeps;
        state.collections.push_back(began);
        if state.collections.len() > REMEMBERED_COLLECTIONS {
            state.collections.pop_front();
            let forgotten = state.collections.front().copied().unwrap_or(began);
            state.swept.retain(|_, swept| *swept > forgotten);
            state.forgotten = forgotten;
        }
        Collecting {
            collection: Arc::clone(self),
            _running: running,
        }
    }

    fn gate(&self) -> Arc<tokio::sync::RwLock<()>> {
        Arc::clone(&self.gate.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Admits into the current generation. A retired generation is skipped,
    /// so this retries once per collection that begins meanwhile, and waits
    /// only while a sweep batch holds the current one.
    pub(crate) async fn unswept(&self) -> Unswept {
        loop {
            let gate = self.gate();
            let guard = match Arc::clone(&gate).try_read_owned() {
                Ok(guard) => guard,
                Err(_) if Arc::ptr_eq(&gate, &self.gate()) => {
                    #[cfg(test)]
                    self.gate_waits.notify_one();
                    Arc::clone(&gate).read_owned().await
                }
                Err(_) => continue,
            };
            if Arc::ptr_eq(&gate, &self.gate()) {
                return Unswept(guard);
            }
        }
    }

    pub(crate) fn writing(&self, _unswept: &Unswept, objects: impl IntoIterator<Item = ObjectId>) {
        if let Some(admitted) = &mut self.state().admitted {
            admitted.extend(objects.into_iter().map(|object| object.digest));
        }
    }

    pub(crate) fn held(&self) -> Vec<Held> {
        self.holds()
            .slots
            .iter()
            .filter_map(|slot| match slot {
                Slot::Occupied(held) => Some(held.clone()),
                Slot::Free(_) => None,
            })
            .collect()
    }
}

impl Collecting {
    pub(crate) async fn sweepable(
        &self,
        batch: Vec<crate::Digest>,
    ) -> (tokio::sync::OwnedRwLockWriteGuard<()>, Vec<crate::Digest>) {
        let guard = self.collection.gate().write_owned().await;
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

    pub(crate) fn sweeping(&self, object: crate::Digest) {
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
mod epoch;
mod holds;
mod roots;
mod sweep;
pub(crate) use buffers::{clone_generation_root, clone_generation_root_with_accounting};
use buffers::{held_owned_bytes, root_parent_bytes};
pub(crate) use epoch::{CollectionEpoch, Epochs};
pub(crate) use holds::Hold;
use holds::{Holds, Slot};
pub(crate) use roots::CheckoutRoots;
#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
pub(crate) use sweep::Worklist;
#[cfg(test)]
mod tests;
