//! Collection roots and admission while a live store keeps writing.

#![cfg_attr(
    not(all(feature = "local", not(target_arch = "wasm32"))),
    allow(dead_code, reason = "only a local store collects")
)]

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex, PoisonError};

use crate::kernel::GenerationRoot;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::performance::WorkCounters;
use crate::storage::ObjectId;

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
    admitted: Option<HashSet<crate::Digest>>,
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

    pub(crate) async fn admit<S: crate::AsyncObjectStore>(
        &self,
        objects: &S,
        scope: crate::PublicationScope<'_>,
        staged: impl Fn(&ObjectId) -> bool + acyclic_stream::ProviderPlatform,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Unswept, crate::FsError> {
        accounting.check()?;
        let hold = self.unswept().await;
        self.admit_held(hold, objects, scope, staged, accounting)
            .await
    }

    /// Admits while retaining the caller's barrier, before any private spill
    /// lock. Pins, publication and pressure relief share this lock order.
    pub(crate) async fn admit_held<S: crate::AsyncObjectStore>(
        &self,
        hold: Unswept,
        objects: &S,
        scope: crate::PublicationScope<'_>,
        staged: impl Fn(&ObjectId) -> bool + acyclic_stream::ProviderPlatform,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Unswept, crate::FsError> {
        accounting.check()?;
        let crate::PublicationScope::Closure {
            objects: closure,
            proven_at,
        } = scope
        else {
            return Ok(hold);
        };
        let stale = proven_at < self.state().sweeps;
        if stale {
            // Revalidate authoritative immutable bytes under the same barrier
            // used by the drain and root-pin handoff. No collection history or
            // age cutoff is needed; only one object is resident at a time.
            let cancellation = accounting.cancellation();
            for object in closure {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if staged(object) {
                    continue;
                }
                let (read, retained) = crate::kernel::read_object_with_accounting(
                    objects,
                    *object,
                    u64::MAX,
                    accounting,
                    cancellation,
                )
                .await?;
                drop(read);
                crate::kernel::release_standalone_read_bytes(retained, accounting)?;
            }
        }
        accounting.check()?;
        if let Some(admitted) = &mut self.state().admitted {
            admitted.extend(closure.iter().map(|object| object.digest));
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

    pub(crate) fn sweeping(&self) {
        self.collection.state().sweeps += 1;
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
