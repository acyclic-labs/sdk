use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

pub(crate) struct NativeDemandReadMeter(pub(crate) Mutex<DemandWork>);
impl NativeDemandReadMeter {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, DemandWork> {
        self.0.lock().unwrap_or_else(|poisoned| {
            let mut work = poisoned.into_inner();
            work.poison_native_original_work();
            work
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
        let header = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            retained: AtomicU64::new(0),
            header,
        }))
    }
    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
        self.header.matches_work(work)
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

/// Retained backing of one native watch registration.
pub struct SourceWatchAllocationCustody {
    retained: AtomicU64,
    pub(super) ownership: SourcePayloadOwnership,
}
impl SourceWatchAllocationCustody {
    /// Returns the registration's live buffer bytes.
    pub fn retained_bytes(&self) -> u64 {
        self.retained.load(Ordering::Acquire)
    }
    pub(super) fn new(ownership: SourcePayloadOwnership) -> Self {
        Self {
            retained: AtomicU64::new(0),
            ownership,
        }
    }
    pub(super) fn publish(&self, bytes: u64) {
        self.retained.store(bytes, Ordering::Release);
    }
    pub(super) fn retired(&self) {
        self.retained.store(0, Ordering::Release);
    }
}

pub(crate) fn native_demand_error(error: std::io::Error) -> DemandError {
    if error
        .get_ref()
        .is_some_and(|error| error.is::<DemandError>())
    {
        *error
            .into_inner()
            .expect("typed native error")
            .downcast::<DemandError>()
            .expect("typed demand error")
    } else {
        DemandError::Io(error)
    }
}
#[cfg(target_os = "macos")]
pub(crate) fn native_demand_read_error(error: DemandError) -> std::io::Error {
    if matches!(error, DemandError::Cancelled) {
        std::io::Error::new(std::io::ErrorKind::Interrupted, error)
    } else {
        std::io::Error::other(error)
    }
}
