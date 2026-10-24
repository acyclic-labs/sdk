use super::*;
use acyclic_native_runtime::NativeOperationKind;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct Meter {
    pub(super) held: AtomicU64,
    pub(super) peak: AtomicU64,
}

macro_rules! accept {
    ($($name:ident($($arg:ident: $ty:ty),*));* $(;)?) => {
        $(fn $name(&self, $(_: $ty),*) -> io::Result<()> { Ok(()) })*
    };
}
macro_rules! refund {
    ($($name:ident($($arg:ident: $ty:ty),*));* $(;)?) => {
        $(fn $name(&self, $(_: $ty),*) {})*
    };
}

impl NativeReadMeter for Meter {
    accept! {
        check();
        reserve_operations(kind: NativeOperationKind, count: u64);
        record_reserved_operation(kind: NativeOperationKind);
        admit_write_bytes(count: usize);
        reserve_bytes(kind: NativeOperationKind, count: u64);
        record_reserved_bytes(kind: NativeOperationKind, maximum: u64, actual: u64);
        reserve_allocation(bytes: u64);
        allocate_refcount_header();
        admit_codec(encoded: u64, copied: u64, hashed: u64);
        record_codec(encoded: u64, copied: u64, hashed: u64);
        examine(items: u64, copied: u64);
        admit_examination(items: u64, copied: u64);
        record_examination(items: u64, copied: u64);
        admit_bytes(bytes: usize);
        admit_read(bytes: usize);
        record_read(bytes: usize);
        admit_write(bytes: usize);
        record_write(bytes: usize);
        admit_durability();
    }
    refund! {
        cancel_reserved_operations(kind: NativeOperationKind, count: u64);
        cancel_reserved_bytes(kind: NativeOperationKind, count: u64);
        cancel_reserved_allocation(bytes: u64);
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        let held = self.held.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak.fetch_max(held, Ordering::SeqCst);
        Ok(())
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.allocate(bytes)
    }
    fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()> {
        self.allocate(bytes)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        (bytes, self.adopt(bytes))
    }
    fn release(&self, bytes: u64) {
        assert!(self.held.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        io::Error::other(error)
    }
}
