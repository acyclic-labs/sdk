//! `native_host` accounting charged through an original work reservation.
use super::NativeTreeMaterializationError;
use crate::WorkCounters;
use crate::ledger::NativeWorkReservation;
#[cfg(any(windows, target_os = "linux", test))]
use acyclic_native_runtime::NativeOperationKind;
use acyclic_native_runtime::NativeReadMeter;
use std::{io, sync::Arc};

impl crate::native_host::NativeHostWork for &Arc<NativeWorkReservation> {
    type Error = NativeTreeMaterializationError;
    type Allocation = u64;
    type CleanupWrite =
        acyclic_native_runtime::NativeOperationReservation<Arc<dyn NativeReadMeter>>;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        if let Self::Error::Io(error) = error {
            Some(error.kind())
        } else {
            None
        }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        Self::Error::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error {
        Self::Error::InvalidLayout
    }
    fn check(&self) -> Result<(), Self::Error> {
        NativeReadMeter::check(self.as_ref()).map_err(|error| self.work().runtime_error(error))
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.admit_host_work(delta)
            .map_err(|error| self.work().runtime_error(error))
    }
    fn record(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        self.record_host_work(actual)
            .map_err(|error| self.work().runtime_error(error))
    }
    fn with_admitted_work<T>(
        &mut self,
        maximum: WorkCounters,
        operation: impl FnOnce() -> (T, WorkCounters),
    ) -> Result<T, Self::Error> {
        self.as_ref().with_admitted_work(maximum, None, operation)
    }
    fn started_io(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        if actual.allocation_operations != 0 || actual.peak_allocation_bytes != 0 {
            return Err(Self::Error::InvalidLayout);
        }
        self.record_started_host_io(actual)
            .map_err(|error| self.work().runtime_error(error))
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        if bytes != 0 {
            return Err(Self::Error::InvalidLayout);
        }
        Ok(0)
    }
    #[cfg(windows)]
    fn reserve_opaque_decode(&mut self, _: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn reconcile_allocation(
        &mut self,
        _: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error> {
        if actual == 0 {
            Ok(())
        } else {
            Err(Self::Error::InvalidLayout)
        }
    }
    fn release(&mut self, _: Self::Allocation) -> Result<(), Self::Error> {
        Ok(())
    }
    #[cfg(any(windows, target_os = "linux", test))]
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        let meter: Arc<dyn NativeReadMeter> = (*self).clone();
        acyclic_native_runtime::NativeOperationReservation::new(
            meter,
            NativeOperationKind::Write,
            1,
        )
        .map_err(|error| self.work().runtime_error(error))
    }
    #[cfg(any(target_os = "linux", all(test, unix)))]
    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        drop(reservation);
    }
    #[cfg(any(windows, target_os = "linux", test))]
    fn attempt_cleanup_write<T>(
        &mut self,
        mut reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        let (value, recorded) = reservation.attempt(operation);
        (
            value,
            recorded.map_err(|error| self.work().runtime_error(error)),
        )
    }
}
