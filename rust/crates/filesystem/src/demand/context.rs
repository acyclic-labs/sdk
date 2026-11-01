use super::*;
use crate::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(any(test, feature = "native-watch"))]
use std::sync::Arc;

// Lock order: observer registry (35), source cursors (40), original leaf (50),
// gate counters (60). Resource allowances follow the leaf, ancestors first.
/// The original ledger, with its cancellation readable without the leaf lock. Every
/// handle on the ledger shares this one token instead of carrying its own copy.
pub(crate) struct NativeDemandReadMeter(
    pub(crate) Mutex<DemandWork>,
    CancellationToken,
    // A standalone handoff retains its original shared-ledger allocation.
    pub(crate) Option<crate::kernel::allocation::retained::StandaloneLease>,
);
impl std::fmt::Debug for NativeDemandReadMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDemandReadMeter")
            .field("owns_handoff", &self.2.is_some())
            .finish_non_exhaustive()
    }
}
impl NativeDemandReadMeter {
    pub(crate) fn new(work: DemandWork) -> Self {
        let cancellation = work.cancellation.clone();
        Self(Mutex::new(work), cancellation, None)
    }
    pub(crate) fn with_backing(
        work: DemandWork,
        backing: crate::kernel::allocation::retained::StandaloneLease,
    ) -> Self {
        let cancellation = work.cancellation.clone();
        Self(Mutex::new(work), cancellation, Some(backing))
    }
    /// The original ledger's cancellation.
    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.1
    }
    #[track_caller]
    pub(crate) fn lock(&self) -> crate::lock_order::MutexGuard<'_, DemandWork> {
        #[cfg(all(test, not(loom)))]
        lock_probe::visit(self);
        crate::lock_order::lock::<_, { crate::lock_order::LEAF }>(&self.0).unwrap_or_else(
            |poisoned| {
                let mut state = poisoned.into_inner();
                state.poison_native_original_work();
                state
            },
        )
    }
    #[cfg(test)]
    #[cfg(not(loom))]
    pub(crate) fn count_locks<T>(&self, operation: impl FnOnce() -> T) -> (T, usize) {
        lock_probe::count(self, operation)
    }
    #[track_caller]
    pub(crate) fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(&mut self.lock())
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod lock_probe;

#[cfg(test)]
#[cfg(loom)]
mod model_checking;

pub(crate) struct CaptureTransientCustody {
    released: acyclic_native_runtime::OwnershipReleaseSignal<CaptureRetirement>,
}

struct CaptureRetirement {
    retained: AtomicU64,
    // The final packet may drop while the original leaf is locked. Keep both
    // control guards with the observer, which retires them outside that lock.
    #[cfg(any(test, feature = "native-watch"))]
    controls: Mutex<Option<[crate::NativeAllocation; 2]>>,
}
impl CaptureTransientCustody {
    #[cfg(test)]
    pub(crate) const fn allocation_bytes() -> u64 {
        (acyclic_native_runtime::arc_allocation_bytes::<Self>()
            + acyclic_native_runtime::OwnershipReleaseSignal::<CaptureRetirement>::allocation_bytes(
            )) as u64
    }

    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn new_native(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, crate::NativeTreeMaterializationError> {
        let header = work.claim(
            acyclic_native_runtime::arc_allocation_bytes::<Self>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let allocation = work.claim(
            acyclic_native_runtime::OwnershipReleaseSignal::<CaptureRetirement>::allocation_bytes()
                as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let released = acyclic_native_runtime::OwnershipReleaseSignal::new(CaptureRetirement {
            retained: AtomicU64::new(0),
            controls: Mutex::new(Some([header, allocation])),
        });
        Ok(Arc::new(Self { released }))
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
        self.released
            .state()
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|controls| controls[0].work().shares_allocation_context(work))
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn take_retained_bytes(&self) -> u64 {
        self.released.state().retained.swap(0, Ordering::AcqRel)
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn retire(self: Arc<Self>) -> (u64, [crate::NativeAllocation; 2]) {
        let barrier = self.released.release_barrier();
        drop(self);
        barrier.wait();
        // Notification can race the signal's final Arc decrement. Move the
        // guards to this observer, so neither can ever drop on the packet
        // thread while that thread holds the original leaf lock.
        let controls = barrier
            .state()
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("one capture retirement observer");
        (barrier.state().retained.swap(0, Ordering::AcqRel), controls)
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn transfer_retained_bytes(&self, bytes: u64) -> Result<(), crate::WorkError> {
        self.released
            .state()
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|_| crate::WorkError::Overflow)
    }
    pub(super) fn claim(&self, bytes: u64) {
        self.released
            .state()
            .retained
            .fetch_add(bytes, Ordering::AcqRel);
    }
    pub(super) fn release(&self, bytes: u64) {
        #[cfg(all(test, loom))]
        if model_checking::broken_release() {
            let current = self.released.state().retained.load(Ordering::Acquire);
            self.released
                .state()
                .retained
                .store(current.saturating_sub(bytes), Ordering::Release);
            return;
        }
        let _ = self.released.state().retained.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(bytes)),
        );
    }
}

#[cfg(all(test, not(loom)))]
mod retirement_tests;

pub(crate) fn native_demand_error(error: std::io::Error) -> DemandError {
    error
        .downcast::<DemandError>()
        .unwrap_or_else(DemandError::Io)
}

#[cfg(target_os = "macos")]
pub(crate) fn native_demand_read_error(error: DemandError) -> std::io::Error {
    let kind = if matches!(error, DemandError::Cancelled) {
        std::io::ErrorKind::Interrupted
    } else {
        std::io::ErrorKind::Other
    };
    std::io::Error::new(kind, error)
}
