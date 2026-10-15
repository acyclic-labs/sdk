use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

pub(crate) struct NativeDemandReadMeter(pub(crate) Mutex<DemandWork>);
impl NativeDemandReadMeter {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, DemandWork> {
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

#[cfg(all(test, feature = "model-checking", not(target_arch = "wasm32")))]
mod model_checking;
mod retained;

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
        retained::take(&self.retained)
    }
    pub(crate) fn transfer_retained_bytes(&self, bytes: u64) -> Result<(), crate::WorkError> {
        retained::transfer(&self.retained, bytes)
    }
    pub(super) fn claim(&self, bytes: u64) {
        retained::claim(&self.retained, bytes);
    }
    pub(super) fn release(&self, bytes: u64) {
        retained::release(&self.retained, bytes);
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
