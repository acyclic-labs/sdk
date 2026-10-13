use super::*;

/// Custody of a backing of the existing registry. Only active waiter/future
/// nodes own this lease; the token owns only their existing Weak slots.
#[cfg(not(target_arch = "wasm32"))]
pub(super) struct NativeRegistryLease {
    pub(super) state: Weak<CancellationState>,
    pub(super) generation: u64,
    pub(super) registry_bytes: AtomicU64,
    pub(super) registered: AtomicU64,
    // Logical highwater admitted before allocating this backing. Allocator
    // capacity rounding must never expose unfunded slots.
    pub(super) funded_slots: usize,
    pub(super) original: crate::NativeMaterializationWork,
    pub(super) cancellation_visits:
        parking_lot::Mutex<Option<crate::materializer::NativeWorkReservation>>,
    pub(super) allocation: parking_lot::Mutex<crate::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeRegistryLease {
    pub(super) fn retire_backing(&self) -> Result<(), crate::NativeTreeMaterializationError> {
        let visits = self.cancellation_visits.lock().take();
        drop(visits);
        let bytes = self.registry_bytes.swap(0, Ordering::AcqRel);
        self.allocation.lock().release(bytes)
    }

    pub(super) fn retire_backing_deferred(&self) {
        let visits = self.cancellation_visits.lock().take();
        drop(visits);
        let bytes = self.registry_bytes.swap(0, Ordering::AcqRel);
        self.allocation.lock().release_deferred(bytes);
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn registry_lease<E>(
    waiters: &[Option<Weak<CancellationWaiter>>],
    mut visit: impl FnMut() -> Result<(), E>,
) -> Result<Option<Arc<NativeRegistryLease>>, E> {
    for slot in waiters {
        visit()?;
        let Some(slot) = slot else {
            continue;
        };
        if let Some(waiter) = slot.upgrade() {
            let lease = waiter
                .registry_lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if lease.is_some() {
                return Ok(lease);
            }
        }
    }
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeRegistryLease {
    fn drop(&mut self) {
        let detached = self.state.upgrade().and_then(|state| {
            if state.registry_generation.load(Ordering::Acquire) != self.generation {
                return None;
            }
            let mut waiters = state
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.registry_generation.load(Ordering::Acquire) != self.generation {
                return None;
            }
            debug_assert!(
                waiters
                    .iter()
                    .flatten()
                    .all(|slot| slot.strong_count() == 0)
            );
            let detached = std::mem::take(&mut *waiters);
            state.registry_generation.fetch_add(1, Ordering::AcqRel);
            drop(waiters);
            Some(detached)
        });
        drop(detached);
        self.retire_backing_deferred();
    }
}
