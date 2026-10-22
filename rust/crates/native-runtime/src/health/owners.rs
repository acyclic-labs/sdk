//! The live-owner count that decides which `HealthOwner` drop unregisters.
use crate::sync::atomic::{AtomicUsize, Ordering};

/// Counts live owners apart from the `Arc`, whose strong count also holds
/// transient fence upgrades and is read before a dropping owner's own `Arc`
/// goes: two final owners dropping together could each see the other and
/// both skip cleanup. Here one read-modify-write decides.
pub(crate) struct OwnerCount(AtomicUsize);

impl Default for OwnerCount {
    fn default() -> Self {
        Self(AtomicUsize::new(0))
    }
}

impl OwnerCount {
    pub(crate) fn acquire(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether this release took the count to zero; exactly one does.
    pub(crate) fn release(&self) -> bool {
        self.0.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Whether no owner is live. Read under the registry lock, which every
    /// owner made from a row is acquired under.
    pub(crate) fn idle(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }
}

#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod model;
