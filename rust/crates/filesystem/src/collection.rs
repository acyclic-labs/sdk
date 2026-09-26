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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::kernel::GenerationRoot;
use crate::storage::{ObjectId, ObjectStoreError};

/// Collections whose swept objects are remembered; a closure proven before
/// all of them is refused.
const REMEMBERED_COLLECTIONS: usize = 4;

/// One store's collection state, shared by the store and every checkout.
#[derive(Default)]
pub struct Collection {
    gate: Arc<tokio::sync::RwLock<()>>,
    state: Mutex<State>,
    holds: Mutex<HashMap<u64, Held>>,
    next_hold: AtomicU64,
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
}

/// Keeps a publication's admitted closure from being swept until its record
/// is durable; the publication drops it after writing the record.
#[must_use = "a publication holds this until its record is written"]
pub struct PublicationHold {
    _gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl PublicationHold {
    /// A hold for a store that never collects.
    pub(crate) const fn none() -> Self {
        Self { _gate: None }
    }
}

/// A collection in progress; see [`Collection::begin`].
pub(crate) struct Collecting {
    collection: Arc<Collection>,
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
    ) -> Result<PublicationHold, ObjectStoreError> {
        let hold = Arc::clone(&self.gate).read_owned().await;
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
        Ok(PublicationHold { _gate: Some(hold) })
    }

    /// Starts a collection once every publication admitted before it has
    /// written its record: every closure admitted while it runs is kept.
    pub(crate) async fn begin(self: &Arc<Self>) -> Collecting {
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
        }
    }

    /// What every open handle holds.
    pub(crate) fn held(&self) -> Vec<Held> {
        self.holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn register(self: &Arc<Self>, held: Held) -> Hold {
        let id = self.next_hold.fetch_add(1, Ordering::Relaxed);
        self.holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, held);
        Hold {
            collection: Arc::clone(self),
            id,
        }
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
    id: u64,
}

impl Hold {
    fn update(&self, held: Held) {
        self.collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self.id, held);
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.id);
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
            }))
        });
        Self {
            base,
            working,
            config,
            hold,
        }
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
            }));
        }
        self.working = working;
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
