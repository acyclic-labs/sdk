use crate::WorkCounters;
use crate::demand::{DemandError, DemandWork, NativeSourceWork, SourceDemandReadWork};
use crate::native_host::NativeHostWork;

pub(crate) enum CallbackWork<'a> {
    Ordinary(&'a mut DemandWork),
    Original(NativeSourceWork<'a>),
}

macro_rules! delegate {
    ($self:ident, $method:ident($($arg:expr),* $(,)?)) => {
        match $self {
            CallbackWork::Ordinary(work) => work.$method($($arg),*),
            CallbackWork::Original(work) => work.$method($($arg),*),
        }
    };
}

impl NativeHostWork for CallbackWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
        <DemandWork as NativeHostWork>::io_error_kind(error)
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> DemandError {
        DemandError::Allocation(error)
    }
    fn invalid_request() -> DemandError {
        DemandError::InvalidRequest
    }
    fn check(&self) -> Result<(), DemandError> {
        delegate!(self, check())
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        delegate!(self, admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        delegate!(self, record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        delegate!(self, started_io(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        delegate!(self, claim(bytes))
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> {
        delegate!(self, reserve_opaque_decode(maximum))
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut u64,
        actual: u64,
    ) -> Result<(), DemandError> {
        delegate!(self, reconcile_allocation(allocation, actual))
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        delegate!(self, release(bytes))
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        delegate!(self, reserve_cleanup_write())
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        delegate!(self, cancel_cleanup_write(reservation));
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        delegate!(self, attempt_cleanup_write(reservation, operation))
    }
}

impl SourceDemandReadWork for CallbackWork<'_> {
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        delegate!(self, with_native_read_meter(operation))
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        match self {
            Self::Ordinary(work) => operation(work),
            Self::Original(work) => work.with_original_leaf(operation),
        }
    }
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        match self {
            Self::Ordinary(work) => Some(operation(work)),
            Self::Original(_) => None,
        }
    }
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        match self {
            Self::Ordinary(_) => None,
            Self::Original(work) => work.native_source_work(),
        }
    }
    fn original_meter(&self) -> Option<&std::sync::Arc<crate::demand::NativeDemandReadMeter>> {
        match self {
            Self::Ordinary(_) => None,
            Self::Original(work) => work.original_meter(),
        }
    }
}
