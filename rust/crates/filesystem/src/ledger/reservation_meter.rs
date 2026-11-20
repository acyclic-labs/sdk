use super::reservation::Reservation;
use super::{NativeResourceCredit, NativeWorkReservation};
use crate::{DemandError, NativeTreeMaterializationError, WorkCounters};
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use std::{io, sync::Arc};

impl NativeWorkReservation {
    fn original_allocation_owner(&self) -> Arc<dyn NativeReadMeter> {
        let owner = self.work().runtime_meter();
        owner.allocation_owner().unwrap_or(owner)
    }
    /// Withholds host work from this reservation until it is recorded.
    pub(crate) fn admit_host_work(&self, delta: WorkCounters) -> io::Result<()> {
        self.edit(|state| {
            delta.remaining(state.available()?)?;
            state.host_pending = state.host_pending.checked_add(delta)?;
            Ok(())
        })
    }
    /// Records admitted host work: its I/O starts, the rest completes.
    pub(crate) fn record_host_work(&self, actual: WorkCounters) -> io::Result<()> {
        self.edit(|state| {
            state.host_pending = actual.remaining(state.host_pending)?;
            self.settle_host_work(state, actual)
        })
    }
    /// Records started host I/O, admitted earlier or within the unspent reservation.
    pub(crate) fn record_started_host_io(&self, actual: WorkCounters) -> io::Result<()> {
        self.edit(|state| {
            match actual.remaining(state.host_pending) {
                Ok(pending) => state.host_pending = pending,
                Err(_) => {
                    actual.remaining(state.available()?)?;
                }
            }
            self.settle_host_work(state, actual)
        })
    }
    fn settle_host_work(
        &self,
        state: &mut Reservation,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        let started = actual.native_io_operations();
        self.begin(state, started)?;
        let completed = started.remaining(actual)?;
        self.finish(state, completed, completed)
    }
}

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
    fn reserve_codec(&self, maximum: acyclic_native_runtime::NativeCodecWork) -> io::Result<()> {
        self.reserve(super::runtime_meter::codec_counters(maximum))
    }
    fn cancel_reserved_codec(&self, unused: acyclic_native_runtime::NativeCodecWork) {
        self.cancel(super::runtime_meter::codec_counters(unused));
    }
    fn record_reserved_codec(
        &self,
        actual: acyclic_native_runtime::NativeCodecWork,
    ) -> io::Result<()> {
        let actual = super::runtime_meter::codec_counters(actual);
        self.complete(Some(actual), actual)
    }
    reservation_counters! {
        admit_codec, record_codec(encoded: u64, copied: u64, hashed: u64) { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed };
        admit_examination, record_examination(items: u64, copied: u64) { items_examined: items, bytes_copied: copied };
    }
    fn check(&self) -> io::Result<()> {
        self.edit(|state| state.available().map(|_| ()))
    }
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        Some(self.original_allocation_owner())
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
        self.reserve(crate::ledger::DemandWork::operation_credit(kind).delta(count))
    }
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
        self.cancel(crate::ledger::DemandWork::operation_credit(kind).delta(count));
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
        let actual = crate::ledger::DemandWork::operation_credit(kind).delta(1);
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
        self.reserve(crate::ledger::DemandWork::byte_credit(kind)?.delta(bytes))
    }
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, bytes: u64) {
        match crate::ledger::DemandWork::byte_credit(kind) {
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
        let credit = crate::ledger::DemandWork::byte_credit(kind)?;
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
        self.original_allocation_owner()
            .record_completed_adoption(bytes)
    }
    fn release(&self, bytes: u64) {
        self.original_allocation_owner().release(bytes);
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
    fn reserve_examination(&self, items: u64) -> io::Result<()> {
        self.reserve(NativeResourceCredit::Items.delta(items))
    }
    fn cancel_reserved_examination(&self, items: u64) {
        self.cancel(NativeResourceCredit::Items.delta(items));
    }
    fn record_reserved_examination(&self, items: u64) -> io::Result<()> {
        let probe = NativeResourceCredit::Items.delta(items);
        self.complete(Some(probe), probe)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.start_transfer(NativeOperationKind::Read, maximum as u64)
    }
    fn admit_write(&self, maximum: usize) -> io::Result<()> {
        self.start_transfer(NativeOperationKind::Write, maximum as u64)
    }
    fn admit_durability(&self) -> io::Result<()> {
        self.start_transfer(NativeOperationKind::Durability, 0)
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        self.reserve(NativeResourceCredit::WriteOperations.delta(1))?;
        Ok(1)
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
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
