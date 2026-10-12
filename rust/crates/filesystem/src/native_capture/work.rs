use super::*;
use crate::kernel::{KernelAccounting, KernelOwnedAllocation};
use crate::native_host::NativeHostWork;

impl NativeHostWork for KernelAccounting<'_> {
    type Error = CaptureError;
    type Allocation = (KernelOwnedAllocation, u64);
    type CleanupWrite = Option<crate::materializer::NativeCleanupWrite>;
    fn io_error_kind(error: &CaptureError) -> Option<std::io::ErrorKind> {
        match error {
            CaptureError::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> CaptureError {
        CaptureError::Kernel(crate::FsError::EncodedAllocationFailed(error))
    }
    fn invalid_request() -> CaptureError {
        CaptureError::InvalidOptions
    }
    fn check(&self) -> Result<(), CaptureError> {
        KernelAccounting::check(self).map_err(Into::into)
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), CaptureError> {
        KernelAccounting::admit(self, delta).map_err(Into::into)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), CaptureError> {
        self.record_completed(delta).map_err(Into::into)
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), CaptureError> {
        self.charge(delta).map_err(Into::into)
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, CaptureError> {
        Ok((self.claim_owned(bytes, u64::from(bytes != 0))?, bytes))
    }
    fn reserve_opaque_decode(&mut self, bytes: u64) -> Result<Self::Allocation, CaptureError> {
        Ok((self.claim_owned(bytes, 0)?, bytes))
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), CaptureError> {
        let result = self.reconcile_owned(&mut allocation.0, allocation.1, actual);
        allocation.1 = actual;
        result.map_err(Into::into)
    }
    fn release(&mut self, mut allocation: Self::Allocation) -> Result<(), CaptureError> {
        self.release_owned(&mut allocation.0, allocation.1)
            .map_err(Into::into)
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, CaptureError> {
        if let Some(original) = self.native_work() {
            return original
                .reserve_cleanup_write()
                .map(Some)
                .map_err(|error| CaptureError::Kernel(error.into()));
        }
        KernelAccounting::admit(
            self,
            WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        match self {
            Self::Standalone { budget, .. } | Self::BorrowedStandalone { budget, .. } => {
                budget.backend_write_operations -= 1
            }
            Self::NativeOriginal { .. } => unreachable!(),
        }
        Ok(None)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        if reservation.is_none() {
            match self {
                Self::Standalone { budget, .. } | Self::BorrowedStandalone { budget, .. } => {
                    budget.backend_write_operations += 1
                }
                Self::NativeOriginal { .. } => unreachable!(),
            }
        }
        drop(reservation);
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), CaptureError>) {
        if let Some(reservation) = reservation {
            let (value, recorded) = reservation.attempt(operation);
            return (
                value,
                recorded.map_err(|error| CaptureError::Kernel(error.into())),
            );
        }
        self.cancel_cleanup_write(None);
        let value = operation();
        (
            value,
            self.record_completed(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(Into::into),
        )
    }
}
