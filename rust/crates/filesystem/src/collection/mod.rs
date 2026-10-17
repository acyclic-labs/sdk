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
    gate: Arc<tokio::sync::RwLock<()>>,
    running: Arc<tokio::sync::Mutex<()>>,
    epochs: Epochs,
    state: Mutex<State>,
    holds: Mutex<Holds>,
}

#[derive(Default)]
struct State {
    sweeps: u64,
    swept: HashMap<ObjectId, u64>,
    collections: VecDeque<u64>,
    forgotten: u64,
    admitted: Option<HashSet<ObjectId>>,
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
}

#[must_use = "a publication holds this until its record is written"]
/// Keeps an admitted closure live until its publication is durable.
pub struct PublicationHold {
    _gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl PublicationHold {
    /// An empty hold for stores with their own collection gate.
    pub const fn none() -> Self {
        Self { _gate: None }
    }
}

pub(crate) struct Unswept(tokio::sync::OwnedRwLockReadGuard<()>);

impl Unswept {
    pub(crate) fn into_hold(self) -> PublicationHold {
        PublicationHold {
            _gate: Some(self.0),
        }
    }
}

pub(crate) struct Collecting {
    collection: Arc<Collection>,
    _running: tokio::sync::OwnedMutexGuard<()>,
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

    pub(crate) async fn begin(self: &Arc<Self>) -> Collecting {
        // One at a time: a collection owns the admitted set until it ends.
        let running = Arc::clone(&self.running).lock_owned().await;
        let _admitted_before = self.gate.write().await;
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
        state.admitted = Some(HashSet::new());
        Collecting {
            collection: Arc::clone(self),
            _running: running,
        }
    }

    pub(crate) async fn unswept(&self) -> Unswept {
        Unswept(Arc::clone(&self.gate).read_owned().await)
    }

    pub(crate) fn writing(&self, _unswept: &Unswept, objects: impl IntoIterator<Item = ObjectId>) {
        if let Some(admitted) = &mut self.state().admitted {
            admitted.extend(objects);
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
mod epoch;
mod holds;
mod roots;
pub(crate) use buffers::{clone_generation_root, clone_generation_root_with_accounting};
use buffers::{held_owned_bytes, root_parent_bytes};
pub(crate) use epoch::{CollectionEpoch, Epochs};
pub(crate) use holds::Hold;
use holds::{Holds, Slot};
pub(crate) use roots::CheckoutRoots;
#[cfg(test)]
mod tests;
