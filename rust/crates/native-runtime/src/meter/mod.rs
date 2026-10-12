use crate::*;

mod allocation;
mod reservation;
pub use allocation::{ReadAllocation, ReadAllocationReservation};
pub use reservation::{NativeByteReservation, NativeOperationReservation};

/// Filesystem-operation allowance owned by one accepted native request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeOperationKind {
    /// A native read, metadata query, or its completion retrieval.
    Read,
    /// A native write, mutating control, or its completion retrieval.
    Write,
    /// An actual durability attempt, reserved before its accepted commit point.
    Durability,
}

/// Invocation-local accounting shared by bounded native I/O.
pub trait NativeReadMeter: Send + Sync {
    /// Checks the original request's cancellation and ownership conditions.
    fn check(&self) -> io::Result<()>;
    /// Returns a pre-existing owner of the same ledger, allowance, cancellation and limits, without request retirement custody.
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        None
    }
    /// Withholds a concrete request's possible operations without inventing spent work; allowance remains unavailable to reentrant callbacks.
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()>;
    /// Refunds only a request token's unused withheld allowance.
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64);
    /// Consumes one request-owned allowance and records its actual attempt, without cancellation checks that could abandon accepted completion.
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()>;
    /// Preflights write bytes without reporting either bytes or an operation.
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()>;
    /// Withholds possible physical bytes without reporting a completed transfer.
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()>;
    /// Refunds only this request's unconsumed physical-byte allowance.
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64);
    /// Restores the request's maximum, then records its exact completed bytes, without cancellation or proof reconciliation before preserving work.
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()>;
    /// Admits and records a real allocation with this logical payload.
    fn allocate(&self, bytes: u64) -> io::Result<()>;
    /// Withholds one future allocation attempt and its known logical payload before accepting IO that may produce that allocation.
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Refunds only an unconsumed allocation reservation, even after cancellation.
    fn cancel_reserved_allocation(&self, bytes: u64);
    /// Converts the caller's withheld credit into one actual allocation attempt and retained logical payload immediately before the allocator boundary.
    fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Retains the real allocator failure in the original meter's existing inline fatal-cause slot before returning a nonallocating IO relay.
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error;
    /// Admits one real reference-count/header allocation with no logical payload.
    fn allocate_refcount_header(&self) -> io::Result<()>;
    /// Retains an already allocated owned payload without another allocation.
    fn adopt(&self, bytes: u64) -> io::Result<()>;
    /// Records known SDK-owned capacity that already exists, before any late cancellation/budget check.
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>);
    /// Preflights the real codec byte counters.
    fn admit_codec(&self, _encoded: u64, _copied: u64, _hashed: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Records actual completed codec bytes, including failure prefixes.
    fn record_codec(&self, _encoded: u64, _copied: u64, _hashed: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Admits and records examined items and actual owned-buffer copies.
    fn examine(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Preflights examination without reporting work not yet performed.
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Records examination already admitted by the preceding preflight.
    fn record_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Retires owned capacity after its actual buffer is freed, even if cancelled.
    fn release(&self, bytes: u64);
    /// Preflights the requested read-byte allowance without reporting completion.
    fn admit_bytes(&self, maximum: usize) -> io::Result<()>;
    /// Admits one read attempt and its maximum byte transfer before dispatch.
    fn admit_read(&self, maximum: usize) -> io::Result<()>;
    /// Records bytes actually returned by the native read.
    fn record_read(&self, bytes: usize) -> io::Result<()>;
    /// Checks the completed logical read before constructing an owned chunk.
    fn finish_read(&self, _requested: usize, _completed: usize) -> io::Result<()> {
        self.check()
    }
    /// Admits one write attempt and its maximum byte transfer before dispatch.
    fn admit_write(&self, maximum: usize) -> io::Result<()>;
    /// Records bytes actually accepted by the native write.
    fn record_write(&self, bytes: usize) -> io::Result<()>;
    /// Admits and records one actual durability attempt before dispatch.
    fn admit_durability(&self) -> io::Result<()>;
    /// Reserves one mandatory native cleanup attempt before acquiring custody.
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Cancels an unused reservation; unsupported meters cannot create one.
    fn cancel_cleanup_write(&self, _reservation: u64) {
        unreachable!("this meter cannot create native cleanup reservations");
    }
    /// Consumes a reservation without checking cancellation.
    fn record_cleanup_write(&self, _reservation: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}
