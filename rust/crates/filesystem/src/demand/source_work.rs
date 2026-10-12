use super::*;
use crate::NativeMaterializationWork;
use crate::native_host::NativeHostWork;

pub(crate) trait SourceDemandReadWork:
    NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T;
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn native_source_work(&self) -> Option<NativeMaterializationWork> {
        None
    }
    fn with_ordinary_work<T>(&mut self, _: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        None
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
                .reserve_work(delta)
                .map(Some)
                .map_err(crate::native_name::original_name_error);
        }
        self.admit(delta)?;
        Ok(None)
    }
    fn claim_allocation(
        &mut self,
        bytes: u64,
        operations: u64,
        delta: WorkCounters,
    ) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.claim_allocation(bytes, operations, delta))
    }
    fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|work| work.remaining())
    }
    fn merge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.merge(delta, work.live_allocation_bytes()))
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

impl SourceDemandReadWork for NativeMaterializationWork {
    fn native_source_work(&self) -> Option<NativeMaterializationWork> {
        Some(self.clone())
    }
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        operation(self.runtime_meter().as_ref())
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        NativeMaterializationWork::with_original_leaf(self, operation)
    }
}

impl NativeHostWork for NativeMaterializationWork {
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
        NativeMaterializationWork::with_original_leaf(self, |work| work.check())
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        self.claim_allocation(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?;
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> {
        self.claim_allocation(maximum, 0, WorkCounters::UNCHARGED)?;
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
