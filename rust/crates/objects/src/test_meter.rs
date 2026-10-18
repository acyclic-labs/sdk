//! Meter used by the decoder fuzz target and allocation property tests.
#![allow(
    missing_docs,
    reason = "Test-support counters are inspected directly by fuzz properties."
)]
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use std::sync::Mutex;

/// Counts the production path's physical reads. Buffer allocations are exact
/// in the work trace; the meter also sees runtime worker bookkeeping.
#[derive(Default)]
pub struct CountingMeter {
    counts: Mutex<Counts>,
    retained: std::sync::atomic::AtomicU64,
    pub written: std::sync::atomic::AtomicU64,
    pub codec_copied: std::sync::atomic::AtomicU64,
    pub allocations: std::sync::atomic::AtomicU64,
    pub allocation_limit: std::sync::atomic::AtomicU64,
    pub byte_limit: std::sync::atomic::AtomicU64,
    pub cancelled: std::sync::atomic::AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Counts {
    pub(crate) read_operations: u64,
    pub(crate) read_bytes: u64,
}

impl CountingMeter {
    #[cfg(test)]
    pub(crate) fn counts(&self) -> Counts {
        *self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn update(&self, change: impl FnOnce(&mut Counts)) {
        change(
            &mut self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
    pub fn retained(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl NativeReadMeter for CountingMeter {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            Err(std::io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }
    fn reserve_operations(&self, _: NativeOperationKind, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, _: u64) {}
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.update(|counts| counts.read_operations += 1);
        }
        Ok(())
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_bytes(&self, _: NativeOperationKind, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, _: u64) {}
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        _: u64,
        actual: u64,
    ) -> std::io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.update(|counts| counts.read_bytes += actual);
        }
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        self.record_reserved_allocation(bytes)
    }
    fn reserve_allocation(&self, _: u64) -> std::io::Result<()> {
        self.check()
    }
    fn cancel_reserved_allocation(&self, _: u64) {}
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering;
        let limit = self.allocation_limit.load(Ordering::SeqCst);
        if limit != 0 && self.allocations.load(Ordering::SeqCst) >= limit {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        let byte_limit = self.byte_limit.load(Ordering::SeqCst);
        if byte_limit != 0 && self.retained() + bytes > byte_limit {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        self.allocations.fetch_add(1, Ordering::SeqCst);
        self.retained
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        std::io::Error::other(error)
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.allocate(0)
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.retained
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        (bytes, self.adopt(bytes))
    }
    fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> {
        self.check()
    }
    fn reserve_codec(&self, _: u64, _: u64) -> std::io::Result<()> {
        self.check()
    }
    fn record_reserved_codec(&self, _: u64, copied: u64) -> std::io::Result<()> {
        self.codec_copied
            .fetch_add(copied, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn record_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn examine(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn record_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn release(&self, bytes: u64) {
        let previous = self
            .retained
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
        assert!(previous >= bytes, "released unowned capacity");
    }
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn record_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.written
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        Ok(())
    }
}
