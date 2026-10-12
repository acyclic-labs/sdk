use super::*;
use crate::{DemandError, WorkCounters};
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use std::{io, sync::Arc};

macro_rules! reservation_counters {
    ($($admit:ident, $record:ident($($arg:ident: $kind:ty),*) { $($field:ident: $value:expr),* };)*) => {$(
        fn $admit(&self, $($arg:$kind),*) -> io::Result<()> {
            self.reserve(WorkCounters { $($field: $value),*, ..WorkCounters::UNCHARGED })
        }
        fn $record(&self, $($arg:$kind),*) -> io::Result<()> {
            let actual = WorkCounters { $($field: $value),*, ..WorkCounters::UNCHARGED };
            self.complete(Some(actual), actual)
        }
    )*};
}
impl NativeReadMeter for NativeWorkReservation {
    reservation_counters! {
        admit_codec, record_codec(encoded: u64, copied: u64, hashed: u64) { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed };
        admit_examination, record_examination(items: u64, copied: u64) { items_examined: items, bytes_copied: copied };
    }
    fn check(&self) -> io::Result<()> {
        self.edit(|state| state.available().map(|_| ()))
    }
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        let owner = self.work().runtime_meter();
        Some(owner.allocation_owner().unwrap_or(owner))
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
        self.reserve(crate::demand::DemandWork::operation_credit(kind).delta(count))
    }
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
        self.cancel(crate::demand::DemandWork::operation_credit(kind).delta(count));
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
        let actual = crate::demand::DemandWork::operation_credit(kind).delta(1);
        let mut state = self.state.lock();
        state.pending = actual
            .remaining(state.pending)
            .map_err(|error| self.failure(error.into()))?;
        if let Err(error) = self.begin(&mut state, actual) {
            let error = self.failure(error);
            self.begin_drop(&mut state, actual);
            return Err(error);
        }
        Ok(())
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, bytes: u64) -> io::Result<()> {
        self.reserve(crate::demand::DemandWork::byte_credit(kind)?.delta(bytes))
    }
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, bytes: u64) {
        match crate::demand::DemandWork::byte_credit(kind) {
            Ok(credit) => self.cancel(credit.delta(bytes)),
            Err(error) => {
                self.failure(error.into());
            }
        }
    }
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()> {
        let credit = crate::demand::DemandWork::byte_credit(kind)?;
        self.complete(Some(credit.delta(maximum)), credit.delta(actual))
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        if bytes == 0 {
            self.check()
        } else {
            Err(self.failure(DemandError::InvalidRequest))
        }
    }
    fn reserve_allocation(&self, _: u64) -> io::Result<()> {
        Err(self.failure(DemandError::InvalidRequest))
    }
    fn cancel_reserved_allocation(&self, _: u64) {
        self.work()
            .poison(NativeTreeMaterializationError::InvalidLayout);
    }
    fn record_reserved_allocation(&self, _: u64) -> io::Result<()> {
        Err(self.failure(DemandError::InvalidRequest))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        self.failure(DemandError::Allocation(error))
    }
    fn allocate_refcount_header(&self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        if bytes == 0 {
            self.check()
        } else {
            Err(io::ErrorKind::Unsupported.into())
        }
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        self.work().with_original_leaf(|state| {
            let (bytes, recorded) = state.record_native_owned_completed(bytes, &None);
            (
                bytes,
                recorded.map_err(|error| state.record_native_io_failure(error)),
            )
        })
    }
    fn release(&self, bytes: u64) {
        self.allocation_owner()
            .expect("original owner")
            .release(bytes);
    }
    fn admit_bytes(&self, bytes: usize) -> io::Result<()> {
        self.preflight(NativeResourceCredit::ReadBytes.delta(bytes as u64))
    }
    fn admit_write_bytes(&self, bytes: usize) -> io::Result<()> {
        self.preflight(NativeResourceCredit::WriteBytes.delta(bytes as u64))
    }
    fn record_read(&self, bytes: usize) -> io::Result<()> {
        self.complete(None, NativeResourceCredit::ReadBytes.delta(bytes as u64))
    }
    fn record_write(&self, bytes: usize) -> io::Result<()> {
        self.complete(None, NativeResourceCredit::WriteBytes.delta(bytes as u64))
    }
    fn examine(&self, items: u64, copied: u64) -> io::Result<()> {
        self.admit_examination(items, copied)?;
        self.record_examination(items, copied)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::ReadOperations,
            NativeResourceCredit::ReadBytes,
            maximum as u64,
        )
    }
    fn admit_write(&self, maximum: usize) -> io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::WriteOperations,
            NativeResourceCredit::WriteBytes,
            maximum as u64,
        )
    }
    fn admit_durability(&self) -> io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::DurabilityOperations,
            NativeResourceCredit::Copy,
            0,
        )
    }
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        self.reserve(NativeResourceCredit::WriteOperations.delta(1))?;
        Ok(1)
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        self.cancel(NativeResourceCredit::WriteOperations.delta(reservation));
    }
    fn record_cleanup_write(&self, reservation: u64) -> io::Result<()> {
        if reservation != 1 {
            return Err(self.failure(DemandError::InvalidRequest));
        }
        self.record_reserved_operation(NativeOperationKind::Write)
    }
}
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
        self.edit(|state| {
            delta.remaining(state.available()?)?;
            state.host_pending = state.host_pending.checked_add(delta)?;
            Ok(())
        })
        .map_err(|error| self.work().runtime_error(error))
    }
    fn record(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        self.edit(|state| {
            state.host_pending = actual.remaining(state.host_pending)?;
            let started = io_work(actual);
            self.begin(state, started)?;
            let completed = started.remaining(actual)?;
            self.finish(state, completed, completed)
        })
        .map_err(|error| self.work().runtime_error(error))
    }
    fn started_io(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        self.edit(|state| {
            match actual.remaining(state.host_pending) {
                Ok(pending) => state.host_pending = pending,
                Err(_) => {
                    actual.remaining(state.available()?)?;
                }
            }
            self.begin(state, actual)
        })
        .map_err(|error| self.work().runtime_error(error))
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        if bytes != 0 {
            return Err(Self::Error::InvalidLayout);
        }
        Ok(0)
    }
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
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        acyclic_native_runtime::NativeOperationReservation::new(
            self.runtime_meter(),
            NativeOperationKind::Write,
            1,
        )
        .map_err(|error| self.work().runtime_error(error))
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        drop(reservation);
    }
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
