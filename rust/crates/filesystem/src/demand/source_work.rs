use super::*;
use crate::native_host::NativeHostWork;
use std::sync::Arc;

/// Borrow of an existing native accounting context.
#[derive(Clone, Copy)]
pub struct NativeSourceWork<'a> {
    original: &'a Arc<NativeDemandReadMeter>,
    context: Option<&'a crate::NativeMaterializationWork>,
}

impl<'a> NativeSourceWork<'a> {
    pub(crate) fn new(original: &'a Arc<NativeDemandReadMeter>) -> Self {
        Self {
            original,
            context: None,
        }
    }
    /// Borrows the invocation's existing accounting state.
    pub fn from_native_materialization_work(work: &'a crate::NativeMaterializationWork) -> Self {
        Self {
            original: &work.state,
            context: Some(work),
        }
    }
    pub(crate) fn owned(&self) -> crate::NativeMaterializationWork {
        self.context
            .cloned()
            .unwrap_or_else(|| crate::NativeMaterializationWork::from_meter(self.original.clone()))
    }
    pub(crate) fn materialization_work(
        &self,
    ) -> Result<crate::NativeMaterializationWork, DemandError> {
        self.check()?;
        Ok(self.owned())
    }
}

pub(crate) trait SourceDemandReadWork:
    NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T;
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        None
    }
    fn with_ordinary_work<T>(&mut self, _: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        None
    }
    fn original_meter(&self) -> Option<&Arc<NativeDemandReadMeter>> {
        self.native_source_work().map(|work| work.original)
    }
    fn is_native_original(&self) -> bool {
        self.native_source_work().is_some()
    }
    fn reserve_source_copy(
        &mut self,
        bytes: u64,
    ) -> Result<Option<crate::materializer::NativeWorkReservation>, DemandError> {
        let delta = WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::UNCHARGED
        };
        self.check()?;
        if let Some(native) = self.native_source_work() {
            return native
                .owned()
                .reserve_work(delta)
                .map(Some)
                .map_err(crate::native_name::original_name_error);
        }
        self.admit(delta)?;
        Ok(None)
    }
    fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim_allocation(&mut self, bytes: u64, operations: u64) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| {
            work.claim_native_owned(
                bytes,
                operations,
                WorkCounters::UNCHARGED,
                &work.capture_custody(),
            )
        })
    }
    fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.claim_allocation(bytes, u64::from(bytes != 0))
    }
    fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| {
            work.claim_native_owned(bytes, u64::from(bytes != 0), delta, &None)
        })
    }
    fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|work| work.remaining())
    }
    fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.claim_allocation(bytes, 0)
    }
    fn merge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.merge(delta))
    }
}

impl SourceDemandReadWork for DemandWork {
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(self)
    }
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        Some(operation(self))
    }
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let meter = NativeDemandReadMeter(std::sync::Mutex::new(self.take_for_native_queue()));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&meter)));
        *self = meter.0.into_inner().unwrap_or_else(|poisoned| {
            let mut work = poisoned.into_inner();
            work.poison_native_original_work();
            work
        });
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

impl SourceDemandReadWork for NativeSourceWork<'_> {
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        Some(*self)
    }
    fn original_meter(&self) -> Option<&Arc<NativeDemandReadMeter>> {
        Some(self.original)
    }
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        operation(self.owned().runtime_meter().as_ref())
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        self.owned().with_original_leaf(operation)
    }
}

impl NativeHostWork for NativeSourceWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
        <DemandWork as NativeHostWork>::io_error_kind(error)
    }
    fn invalid_request() -> DemandError {
        DemandError::InvalidRequest
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> DemandError {
        DemandError::Allocation(error)
    }
    fn check(&self) -> Result<(), DemandError> {
        self.owned().with_original_leaf(|work| work.check())
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.charge(delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        self.allocate(bytes)?;
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> {
        self.adopt(maximum)?;
        Ok(maximum)
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut u64,
        actual: u64,
    ) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.reconcile_native_allocation(allocation, actual))
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.release(bytes))
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        self.with_original_leaf(NativeHostWork::reserve_cleanup_write)
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.with_original_leaf(|work| work.cancel_cleanup_write(reservation));
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        let recorded =
            self.with_original_leaf(|work| work.attempt_cleanup_write(reservation, || ()).1);
        (operation(), recorded)
    }
}
