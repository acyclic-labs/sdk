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
use std::ops::Deref;
use std::sync::{Arc, PoisonError};

/// One volume's last published proof and the epoch its proof began in.
pub(crate) struct ProofSlot<P> {
    // Lock order: slot only; replaced proof custody retires after unlocking.
    published: Mutex<Option<(CollectionEpoch, Sequence, P)>>,
}

impl<P> Default for ProofSlot<P> {
    fn default() -> Self {
        Self {
            published: Mutex::new(None),
        }
    }
}

impl<P: Clone> ProofSlot<P> {
    /// The published proof, if no collection has begun since its proof began.
    pub(crate) fn current(&self, epochs: Option<&Epochs>) -> Option<P> {
        let now = CollectionEpoch::read(epochs);
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|(proven, _, _)| *proven == now)
            .map(|(_, _, proof)| proof.clone())
    }

    #[cfg(test)]
    pub(super) fn current_in_any_epoch(&self) -> Option<P> {
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(_, _, proof)| proof.clone())
    }
}

/// A proof of a generation that is not yet durable. Only its own append outcome publishes it.
#[must_use = "a proof is reusable only after its generation is published"]
pub(crate) struct Unpublished<P, S = Arc<ProofSlot<P>>> {
    slot: S,
    began: CollectionEpoch,
    proof: P,
}

/// The original proof of one durable generation, independent of the latest slot.
#[derive(Clone)]
pub(crate) struct Published<P> {
    began: CollectionEpoch,
    proof: P,
}

impl<P: Clone> Published<P> {
    pub(crate) fn current(&self, epochs: Option<&Epochs>) -> Option<P> {
        (self.began == CollectionEpoch::read(epochs)).then(|| self.proof.clone())
    }
}

impl<P, S: Deref<Target = ProofSlot<P>>> Unpublished<P, S> {
    /// A proof that began in `began`, read before its first object read.
    pub(crate) fn new(slot: S, began: CollectionEpoch, proof: P) -> Self {
        Self { slot, began, proof }
    }

    /// Makes the proof reusable when its generation is durable; otherwise discards it.
    pub(crate) fn publish(self, outcome: &AppendOutcome) -> Option<Published<P>>
    where
        P: Clone,
    {
        match outcome {
            AppendOutcome::Committed(commit) | AppendOutcome::AlreadyCommitted(commit) => {
                let published = Published {
                    began: self.began,
                    proof: self.proof.clone(),
                };
                self.install::<true, true>(commit.sequence);
                Some(published)
            }
            _ => None,
        }
    }

    /// Makes a new volume's proof of its creating generation reusable.
    pub(crate) fn created(self) {
        self.install::<true, true>(Sequence::new(1));
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

    #[cfg(test)]
    #[cfg(loom)]
    pub(super) fn install_unpublished(self) {
        self.install::<true, true>(Sequence::new(1));
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests;

#[cfg(test)]
#[cfg(not(loom))]
mod tests;

#[cfg(test)]
mod test_support;
