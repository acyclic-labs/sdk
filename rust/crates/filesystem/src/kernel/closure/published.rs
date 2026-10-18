//! The last published proof of one volume, reusable only inside the epoch it was proven in.
//!
//! A proof becomes reusable by consuming its [`Unpublished`] ticket with the
//! durable append outcome; a failed append drops the ticket instead. A
//! reused proof must also have begun after the latest collection began, so
//! each object it vouches for was admitted to that collection or never at
//! risk: [`ProofSlot::current`] reads the store's epoch itself.

use crate::collection::{CollectionEpoch, Epochs};
use crate::foundation::Sequence;
use crate::storage::AppendOutcome;
use crate::sync::Mutex;
use std::sync::{Arc, PoisonError};

/// One volume's last published proof and the epoch its proof began in.
pub(crate) struct ProofSlot<T> {
    // Lock order: slot only; replaced proof custody retires after unlocking.
    published: Mutex<Option<(CollectionEpoch, Sequence, Arc<T>)>>,
}

impl<T> Default for ProofSlot<T> {
    fn default() -> Self {
        Self {
            published: Mutex::new(None),
        }
    }
}

impl<T> ProofSlot<T> {
    /// The published proof, if no collection has begun since its proof began.
    pub(crate) fn current(&self, epochs: Option<&Epochs>) -> Option<Arc<T>> {
        let now = CollectionEpoch::read(epochs);
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|(proven, _, _)| *proven == now)
            .map(|(_, _, proof)| Arc::clone(proof))
    }

    #[cfg(test)]
    pub(super) fn current_in_any_epoch(&self) -> Option<Arc<T>> {
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(_, _, proof)| Arc::clone(proof))
    }
}

/// A proof of a generation that is not yet durable. Only its own append outcome publishes it.
#[must_use = "a proof is reusable only after its generation is published"]
pub(crate) struct Unpublished<T> {
    slot: Arc<ProofSlot<T>>,
    began: CollectionEpoch,
    proof: Arc<T>,
}

impl<T> Unpublished<T> {
    /// A proof that began in `began`, read before its first object read.
    pub(crate) fn new(slot: Arc<ProofSlot<T>>, began: CollectionEpoch, proof: T) -> Self {
        Self {
            slot,
            began,
            proof: Arc::new(proof),
        }
    }

    /// Makes the proof reusable when its generation is durable; otherwise discards it.
    pub(crate) fn publish(self, outcome: &AppendOutcome) {
        match outcome {
            AppendOutcome::Committed(commit) | AppendOutcome::AlreadyCommitted(commit) => {
                self.install::<true, true>(commit.sequence);
            }
            _ => {}
        }
    }

    fn install<const ORDERED: bool, const REFRESH: bool>(self, sequence: Sequence) {
        let mut published = self
            .slot
            .published
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Durable history wins first; only a newer collection proof may refresh
        // the same generation. Exact ties keep their existing custody.
        let replaced = if !ORDERED
            || published.as_ref().is_none_or(|(epoch, current, _)| {
                sequence > *current || (REFRESH && sequence == *current && self.began > *epoch)
            }) {
            published.replace((self.began, sequence, self.proof))
        } else {
            None
        };
        drop(published);
        drop(replaced);
    }

    #[cfg(all(test, acyclic_fs_loom))]
    pub(super) fn install_unpublished(self) {
        self.install::<true, true>(Sequence::new(1));
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod loom_tests;

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;

#[cfg(test)]
mod test_support;
