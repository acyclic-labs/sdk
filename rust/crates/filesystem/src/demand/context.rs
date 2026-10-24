use super::*;
use crate::sync::Arc;
use crate::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

// Lock order: observer registry (35), source cursors (40), original leaf (50),
// gate counters (60). Resource allowances follow the leaf, ancestors first.
/// The original ledger, with its cancellation readable without the leaf lock. Every
/// handle on the ledger shares this one token instead of carrying its own copy.
pub(crate) struct NativeDemandReadMeter(pub(crate) Mutex<DemandWork>, CancellationToken);
impl std::fmt::Debug for NativeDemandReadMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDemandReadMeter")
            .finish_non_exhaustive()
    }
}
impl NativeDemandReadMeter {
    pub(crate) fn new(work: DemandWork) -> Self {
        let cancellation = work.cancellation.clone();
        Self(Mutex::new(work), cancellation)
    }
    /// The original ledger's cancellation.
    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.1
    }
    #[track_caller]
    pub(crate) fn lock(&self) -> crate::lock_order::MutexGuard<'_, DemandWork> {
        #[cfg(all(test, not(acyclic_fs_loom)))]
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
    #[cfg(not(acyclic_fs_loom))]
    pub(crate) fn count_locks<T>(&self, operation: impl FnOnce() -> T) -> (T, usize) {
        lock_probe::count(self, operation)
    }
    #[track_caller]
    pub(crate) fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(&mut self.lock())
    }
}

#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
mod lock_probe;

#[cfg(test)]
#[cfg(acyclic_fs_loom)]
mod model_checking;

#[cfg(all(test, not(acyclic_fs_loom), feature = "native-watch"))]
mod capture_tests;

#[derive(Clone)]
#[repr(transparent)]
pub(crate) struct CaptureCustody(Arc<CaptureTransientCustody>);

impl std::ops::Deref for CaptureCustody {
    type Target = CaptureTransientCustody;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CaptureCustody {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const CaptureTransientCustody {
        Arc::as_ptr(&self.0)
    }
    #[inline]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub(crate) struct CaptureTransientCustody {
    retained: AtomicU64,
    #[cfg(any(test, feature = "native-watch"))]
    workers: AtomicU64,
    closed: AtomicBool,
    #[cfg(any(test, feature = "native-watch"))]
    header: crate::NativeAllocation,
}
impl CaptureTransientCustody {
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) const ALLOCATION_BYTES: u64 =
        match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>()) {
            Ok(bytes) => bytes as u64,
            Err(_) => panic!("capture custody Arc layout overflow"),
        };

    pub(crate) fn is_open(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn new_native(
        work: &crate::NativeMaterializationWork,
    ) -> Result<CaptureCustody, crate::NativeTreeMaterializationError> {
        #[cfg(all(test, not(acyclic_fs_loom), feature = "native-watch"))]
        let bytes = capture_tests::claimed_bytes();
        #[cfg(not(all(test, not(acyclic_fs_loom), feature = "native-watch")))]
        let bytes = Self::ALLOCATION_BYTES;
        Ok(CaptureCustody(Arc::new(Self {
            retained: AtomicU64::new(0),
            workers: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            header: work.claim(bytes, 1, WorkCounters::UNCHARGED)?,
        })))
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
        self.header.work().shares_allocation_context(work)
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn take_retained_bytes(&self) -> u64 {
        self.retained.swap(0, Ordering::AcqRel)
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn transfer_retained_bytes(&self, bytes: u64) -> Result<(), crate::WorkError> {
        self.retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|_| crate::WorkError::Overflow)
    }
    pub(super) fn claim(&self, bytes: u64) {
        self.retained.fetch_add(bytes, Ordering::AcqRel);
    }
    pub(super) fn release(&self, bytes: u64) {
        #[cfg(all(test, acyclic_fs_loom))]
        if model_checking::broken_release() {
            let current = self.retained.load(Ordering::Acquire);
            self.retained
                .store(current.saturating_sub(bytes), Ordering::Release);
            return;
        }
        let _ = self
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }
}

#[cfg(any(test, feature = "native-watch"))]
#[path = "context/capture_lifetime.rs"]
mod capture_lifetime;

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
