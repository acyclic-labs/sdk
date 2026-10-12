use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}};
use acyclic_native_runtime::{NativeReadMeter, NativeOperationKind};
use crate::memory::TestClock;

#[derive(Debug)]
pub(crate) struct NativeDenial(pub(crate) &'static str);
impl std::fmt::Display for NativeDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.0) }
}
impl std::error::Error for NativeDenial {}

pub(crate) struct Meter {
    pub(crate) check_on_record: bool,
    pub(crate) deny_allocation: AtomicBool,
    pub(crate) deny_durability: AtomicBool,
    pub(crate) cancel_on_write: AtomicBool,
    pub(crate) cancel_on_copy: AtomicBool,
    pub(crate) cancel_on_durability: AtomicBool,
    pub(crate) expire_after_hash: Mutex<Option<(Arc<TestClock>, u64)>>,
    pub(crate) read_operations: AtomicU64,
    pub(crate) write_operations: AtomicU64,
    pub(crate) durability_operations: AtomicU64,
    pub(crate) write_bytes: AtomicU64,
    pub(crate) hashed: AtomicU64,
    pub(crate) held_cleanup: AtomicU64,
    pub(crate) cleanup_tokens: AtomicU64,
    pub(crate) held: AtomicU64,
    pub(crate) limit: AtomicU64,
    pub(crate) allocations: AtomicU64,
    pub(crate) read: AtomicU64,
    pub(crate) read_limit: u64,
    pub(crate) pending_read: AtomicU64,
    pub(crate) pending_write: AtomicU64,
    pub(crate) pending_read_operations: AtomicU64,
    pub(crate) pending_write_operations: AtomicU64,
    pub(crate) pending_durability: AtomicU64,
    pub(crate) copied: AtomicU64,
    pub(crate) cancel_after_read: u64,
    pub(crate) late_write_error: bool,
    pub(crate) cancelled: AtomicBool,
}

impl Default for Meter {
    fn default() -> Self {
        Self {
            check_on_record: false,
            deny_allocation: AtomicBool::new(false), deny_durability: AtomicBool::new(false),
            cancel_on_write: AtomicBool::new(false), cancel_on_copy: AtomicBool::new(false),
            cancel_on_durability: AtomicBool::new(false), expire_after_hash: Mutex::new(None),
            read_operations: AtomicU64::new(0), write_operations: AtomicU64::new(0),
            durability_operations: AtomicU64::new(0), write_bytes: AtomicU64::new(0),
            hashed: AtomicU64::new(0), held_cleanup: AtomicU64::new(0), cleanup_tokens: AtomicU64::new(0),
            held: AtomicU64::new(0), limit: AtomicU64::new(u64::MAX), allocations: AtomicU64::new(0),
            read: AtomicU64::new(0), read_limit: u64::MAX,
            pending_read: AtomicU64::new(0), pending_write: AtomicU64::new(0),

            pending_read_operations: AtomicU64::new(0), pending_write_operations: AtomicU64::new(0),
            pending_durability: AtomicU64::new(0), copied: AtomicU64::new(0),

            cancel_after_read: u64::MAX, late_write_error: false,
            cancelled: AtomicBool::new(false),
        }
    }
}

impl Meter {
    #[cfg(feature = "local")]
    pub(crate) fn local() -> Self { Self { check_on_record: true, ..Self::default() } }
    fn recorded(&self) -> std::io::Result<()> {
        if self.check_on_record { self.check() } else { Ok(()) }
    }
    fn denial(reason: &'static str) -> std::io::Error { std::io::Error::other(NativeDenial(reason)) }
    pub(crate) fn assert_refunded(&self) {
        for held in [&self.pending_read, &self.pending_write, &self.pending_read_operations,
            &self.pending_write_operations, &self.pending_durability, &self.held_cleanup] {
            assert_eq!(held.load(Ordering::SeqCst), 0);
        }
    }
    fn admit_allocation(&self, operation: bool) -> std::io::Result<()> {
        self.allocations.load(Ordering::SeqCst).checked_add(u64::from(operation))
            .filter(|count| *count <= self.limit.load(Ordering::SeqCst))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory)).map(|_| ())
    }
    pub(crate) fn original(self: &Arc<Self>) -> Arc<dyn NativeReadMeter> { self.clone() }
    fn pending_operations(&self, kind: NativeOperationKind) -> &AtomicU64 {
        match kind {
            NativeOperationKind::Read => &self.pending_read_operations,
            NativeOperationKind::Write => &self.pending_write_operations,
            NativeOperationKind::Durability => &self.pending_durability,
        }
    }
    fn pending_bytes(&self, kind: NativeOperationKind) -> &AtomicU64 {
        match kind {
            NativeOperationKind::Read => &self.pending_read,
            NativeOperationKind::Write => &self.pending_write,
            NativeOperationKind::Durability => unreachable!("durability carries no bytes"),
        }
    }
}

impl NativeReadMeter for Meter {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(Ordering::SeqCst) {
            Err(std::io::Error::new(std::io::ErrorKind::Interrupted, NativeDenial("cancelled")))
        } else { Ok(()) }
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.check()?;
        if kind == NativeOperationKind::Durability && self.deny_durability.load(Ordering::SeqCst) {
            return Err(Self::denial("durability"));
        }
        self.pending_operations(kind).fetch_add(count, Ordering::SeqCst);
        Ok(())
    }
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
        assert!(self.pending_operations(kind).fetch_sub(count, Ordering::SeqCst) >= count);
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
        assert!(self.pending_operations(kind).fetch_sub(1, Ordering::SeqCst) >= 1);
        match kind {
            NativeOperationKind::Read => { self.read_operations.fetch_add(1, Ordering::SeqCst); }
            NativeOperationKind::Write => { self.write_operations.fetch_add(1, Ordering::SeqCst); }
            NativeOperationKind::Durability => {
                self.durability_operations.fetch_add(1, Ordering::SeqCst);
                if self.cancel_on_durability.swap(false, Ordering::SeqCst) { self.cancelled.store(true, Ordering::SeqCst); }
            }
        }
        self.recorded()
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> { self.check() }
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.check()?;
        let pending = self.pending_bytes(kind);
        pending.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
            let next = held.checked_add(maximum)?;
            if kind == NativeOperationKind::Read
                && self.read.load(Ordering::SeqCst).checked_add(next)? > self.read_limit
            { None } else { Some(next) }
        }).map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        Ok(())
    }
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
        assert!(self.pending_bytes(kind).fetch_sub(maximum, Ordering::SeqCst) >= maximum);
    }
    fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.cancel_reserved_bytes(kind, maximum);
        assert!(actual <= maximum);
        match kind {
            NativeOperationKind::Read => self.record_read(actual as usize),
            NativeOperationKind::Write => self.record_write(actual as usize),
            NativeOperationKind::Durability => unreachable!("durability carries no bytes"),
        }
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        if self.deny_allocation.load(Ordering::SeqCst) { return Err(Self::denial("allocation")); }
        self.admit_allocation(true)?;
        self.allocations.fetch_add(1, Ordering::SeqCst);
        self.held.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
    fn reserve_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn cancel_reserved_allocation(&self, _: u64) {}
    fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
        std::io::ErrorKind::OutOfMemory.into()
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> { self.allocate(0) }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        self.admit_allocation(false)?;
        self.held.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.held.fetch_add(bytes, Ordering::SeqCst);
        (bytes, self.admit_allocation(false).and_then(|()| self.check()))
    }
    fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> { self.check() }
    fn record_codec(&self, _: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.copied.fetch_add(copied, Ordering::SeqCst);
        self.hashed.fetch_add(hashed, Ordering::SeqCst);
        if copied != 0 && self.cancel_on_copy.swap(false, Ordering::SeqCst) { self.cancelled.store(true, Ordering::SeqCst); }
        if hashed != 0 && let Some((clock, now)) = self.expire_after_hash.lock().expect("test clock is not poisoned").take() {
            clock.0.store(now, Ordering::SeqCst);
        }
        self.recorded()
    }
    fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
        self.admit_examination(items, copied)?;
        self.record_examination(items, copied)
    }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> { self.check() }
    fn record_examination(&self, _: u64, copied: u64) -> std::io::Result<()> {
        self.copied.fetch_add(copied, Ordering::SeqCst);
        self.recorded()
    }
    fn release(&self, bytes: u64) {
        assert!(self.held.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
    }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.check()?;
        if self.read.load(Ordering::SeqCst).saturating_add(maximum as u64) > self.read_limit {
            Err(std::io::ErrorKind::OutOfMemory.into())
        } else { Ok(()) }
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.admit_bytes(maximum)?;

        Ok(())
    }
    fn admit_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
    fn record_operations(&self, count: u64) -> std::io::Result<()> {
        self.read_operations.fetch_add(count, Ordering::SeqCst);
        self.recorded()
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        let completed = self.read.fetch_add(bytes as u64, Ordering::SeqCst) + bytes as u64;
        if completed >= self.cancel_after_read { self.cancelled.store(true, Ordering::SeqCst); }
        self.recorded()
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> { self.check() }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.write_bytes.fetch_add(bytes as u64, Ordering::SeqCst);
        if self.late_write_error || bytes != 0 && self.cancel_on_write.swap(false, Ordering::SeqCst) {
            self.cancelled.store(true, Ordering::SeqCst);
            return self.check();
        }
        self.recorded()
    }
    fn admit_write_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
    fn record_write_operations(&self, count: u64) -> std::io::Result<()> {
        self.write_operations.fetch_add(count, Ordering::SeqCst);
        self.recorded()
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        self.check()?;
        self.durability_operations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.check()?;
        self.held_cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(self.cleanup_tokens.fetch_add(1, Ordering::SeqCst) + 1)
    }
    fn cancel_cleanup_write(&self, _: u64) { assert!(self.held_cleanup.fetch_sub(1, Ordering::SeqCst) >= 1); }
    fn record_cleanup_write(&self, token: u64) -> std::io::Result<()> {
        self.cancel_cleanup_write(token);
        self.record_write_operations(1)
    }
}
