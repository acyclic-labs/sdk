//! Collections begun on one store: the validity epoch of a cached closure proof.

use crate::sync::atomic::{AtomicU64, Ordering};

/// Counts collections begun, bumped before a collection marks or sweeps anything.
pub(crate) struct Epochs(AtomicU64);

impl Default for Epochs {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Epochs {
    /// Starts one collection's epoch. Every proof begun earlier is now stale.
    pub(crate) fn begin(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// The collections begun when it was read; only [`CollectionEpoch::read`] makes one.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CollectionEpoch(u64);

impl CollectionEpoch {
    /// Reads the current epoch. A store that never collects has one epoch forever.
    pub(crate) fn read(epochs: Option<&Epochs>) -> Self {
        Self(epochs.map_or(0, |epochs| epochs.0.load(Ordering::SeqCst)))
    }
}
