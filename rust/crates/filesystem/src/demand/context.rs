use super::*;
use crate::sync::{Mutex, MutexGuard};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

// Lock order: observer registry (35), source cursors (40), original leaf (50),
// gate counters (60). Resource allowances follow the leaf, ancestors first.
pub(crate) struct NativeDemandReadMeter(pub(crate) Mutex<DemandWork>);
impl NativeDemandReadMeter {
    pub(crate) fn new(work: DemandWork) -> Self {
        Self(Mutex::new(work))
    }
    pub(crate) fn lock(&self) -> MutexGuard<'_, DemandWork> {
        self.0.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.poison_native_original_work();
            state
        })
    }
    pub(crate) fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(&mut self.lock())
    }
    pub(crate) fn take_native_io_failure(&self) -> Option<DemandError> {
        self.lock().take_native_io_failure()
    }
}

pub(crate) struct CaptureTransientCustody {
    retained: AtomicU64,
    header: crate::NativeAllocation,
}
impl CaptureTransientCustody {
    pub(crate) fn new_native(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, crate::NativeTreeMaterializationError> {
        Ok(Arc::new(Self {
            retained: AtomicU64::new(0),
            header: work.claim(
                std::mem::size_of::<Self>() as u64,
                1,
                WorkCounters::UNCHARGED,
            )?,
        }))
    }
    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
        self.header.work().shares_allocation_context(work)
    }
    pub(crate) fn take_retained_bytes(&self) -> u64 {
        self.retained.swap(0, Ordering::AcqRel)
    }
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
        let _ = self
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }
}
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
