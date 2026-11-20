//! Test fixtures shared by the crate's own tests and by sibling crates' tests.
//!
//! These are compiled with `test-support`, which every Bazel target enables, so
//! a crate that moved a module out can still drive the same meter and exclusive
//! process helpers its tests used before the move.

use crate::meter::{NativeCodecWork, NativeOperationKind, NativeReadMeter};
use crate::sync::Mutex;
use crate::OwnershipAnchor;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};

/// The credit a meter returns when a reservation is cancelled or retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Credit {
    /// A reserved operation.
    Operations,
    /// Reserved bytes.
    Bytes,
    /// Reserved codec work.
    Codec,
    /// A reserved allocation.
    AllocationReservation,
    /// A recorded allocation.
    Allocation,
}

#[derive(Default)]
pub struct ReadBudgetMeter {
    #[cfg(windows)]
    pub shared_owner: std::sync::Weak<Self>,
    #[cfg(windows)]
    pub bytes_recorded: Mutex<Option<mpsc::Sender<(u64, u64)>>>,
    pub used_operations: AtomicU64,
    pub read_attempts: AtomicU64,
    pub write_attempts: AtomicU64,
    pub used_bytes: AtomicU64,
    pub used_allocations: AtomicU64,
    pub examined_items: AtomicU64,
    pub encoded: AtomicU64,
    pub copied: AtomicU64,
    pub hashed: AtomicU64,
    pub item_limit: Option<u64>,
    pub read: AtomicU64,
    pub memory: AtomicU64,
    /// Terminal handoff for tests that need the last physical owner's release.
    pub memory_retired: Mutex<Option<mpsc::Sender<()>>>,
    pub operation_limit: u64,
    pub byte_limit: u64,
    pub allocation_limit: u64,
    pub deny_allocations: AtomicBool,
    pub operation_denial: Option<NativeOperationKind>,
    pub cancel_after_operation: bool,
    pub fail_recorded_operation: Option<u64>,
    /// Observes an actual operation for event-driven native progress controls.
    pub operation_recorded: Option<Box<dyn Fn() -> io::Result<()> + Send + Sync>>,
    /// Observes the actual refund of unused native cleanup credit.
    pub cleanup_cancelled: Option<Box<dyn Fn() + Send + Sync>>,
    /// Observes a completed original-ledger refund, including unwind cuts.
    pub credit_returned: Option<Box<dyn Fn(Credit) + Send + Sync>>,
    /// Observes the original completed examination, including unwind cuts.
    pub examination_recorded: Option<Box<dyn Fn() -> io::Result<()> + Send + Sync>>,
    /// Observes original ledger-owner selection before allocation admission.
    pub owner_selected: Option<Box<dyn Fn() + Send + Sync>>,
    pub memory_limit: u64,
    pub cancelled: AtomicBool,
    pub cancel_after_read: bool,
    #[cfg(test)]
    pub panic_after_read: bool,
    /// Runs when a write moved nothing because its pipe was full.
    pub blocked_write: Option<Box<dyn Fn() + Send + Sync>>,
    // Last: the shared ownership primitive observes the final meter cleanup.
    pub _retirement: Option<OwnershipAnchor>,
}

impl ReadBudgetMeter {
    #[cfg(unix)]
    pub fn on_allocation_returned(&mut self, returned: impl Fn() + Send + Sync + 'static) {
        self.credit_returned = Some(Box::new(move |credit| {
            if credit == Credit::Allocation {
                returned();
            }
        }));
    }

    /// After the original operation ends, await its final allocation release.
    /// The release callback and this registration share one mutex, including
    /// the already-retired case, so neither ordering loses the terminal event.
    pub fn wait_for_memory_retirement(&self) -> io::Result<()> {
        let receiver = {
            let mut retired = self
                .memory_retired
                .lock()
                .map_err(|_| io::Error::other("allocation retirement registration poisoned"))?;
            if self.memory.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            if retired.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "allocation retirement already observed",
                ));
            }
            let (sender, receiver) = mpsc::channel();
            *retired = Some(sender);
            receiver
        };
        receiver.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "allocation release producer lost",
            )
        })?;
        assert_eq!(self.memory.load(Ordering::Acquire), 0);
        Ok(())
    }
    #[cfg(windows)]
    pub fn shared(operation_limit: u64, byte_limit: u64) -> Arc<Self> {
        Arc::new_cyclic(|owner| Self {
            shared_owner: owner.clone(),
            ..Self::new(operation_limit, byte_limit)
        })
    }
    pub fn new(operation_limit: u64, byte_limit: u64) -> Self {
        Self {
            operation_limit,
            byte_limit,
            allocation_limit: u64::MAX,
            memory_limit: 1024 * 1024,
            ..Self::default()
        }
    }
    fn charge(counter: &AtomicU64, amount: u64, limit: u64) -> io::Result<u64> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|total| *total <= limit)
            })
            .map_err(|_| Self::refuse())
    }
    fn preflight(&self, counter: &AtomicU64, amount: u64, limit: u64) -> io::Result<()> {
        self.check()?;
        counter
            .load(Ordering::Acquire)
            .checked_add(amount)
            .filter(|total| *total <= limit)
            .map(|_| ())
            .ok_or_else(Self::refuse)
    }
    fn refuse() -> io::Error {
        io::ErrorKind::Other.into()
    }
}

impl NativeReadMeter for ReadBudgetMeter {
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        if let Some(selected) = &self.owner_selected {
            selected();
        }
        #[cfg(windows)]
        {
            self.shared_owner
                .upgrade()
                .map(|owner| owner as Arc<dyn NativeReadMeter>)
        }
        #[cfg(not(windows))]
        {
            None
        }
    }
    fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
        self.check()?;
        if self.operation_denial == Some(kind) {
            return Err(Self::refuse());
        }
        Self::charge(&self.used_operations, count, self.operation_limit).map(|_| ())
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, count: u64) {
        assert!(self.used_operations.fetch_sub(count, Ordering::AcqRel) >= count);
        if let Some(returned) = &self.credit_returned {
            returned(Credit::Operations);
        }
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.read_attempts.fetch_add(1, Ordering::AcqRel);
        }
        if kind == NativeOperationKind::Write {
            self.write_attempts.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(recorded) = &self.operation_recorded {
            recorded()?;
        }
        if self.cancel_after_operation {
            self.cancelled.store(true, Ordering::Release);
        }
        if self.fail_recorded_operation == Some(self.used_operations.load(Ordering::Acquire)) {
            return Err(Self::refuse());
        }
        Ok(())
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
        assert_ne!(kind, NativeOperationKind::Durability);
        self.check()?;
        Self::charge(&self.used_bytes, maximum, self.byte_limit).map(|_| ())
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, maximum: u64) {
        assert!(self.used_bytes.fetch_sub(maximum, Ordering::AcqRel) >= maximum);
        if let Some(returned) = &self.credit_returned {
            returned(Credit::Bytes);
        }
    }
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()> {
        assert!(actual <= maximum);
        if kind == NativeOperationKind::Write
            && actual == 0
            && let Some(blocked) = &self.blocked_write
        {
            blocked();
        }
        assert!(
            self.used_bytes
                .fetch_sub(maximum - actual, Ordering::AcqRel)
                >= maximum
        );
        self.read.fetch_add(actual, Ordering::AcqRel);
        #[cfg(windows)]
        if kind == NativeOperationKind::Write {
            if let Some(recorded) = self.bytes_recorded.lock().unwrap().take() {
                let _ = recorded.send((maximum, actual));
            }
        }
        if self.cancel_after_read && actual != 0 {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        self.reserve_allocation(bytes)?;
        self.record_reserved_allocation(bytes)
    }
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
        self.check()?;
        if self.deny_allocations.load(Ordering::Acquire) {
            return Err(Self::refuse());
        }
        Self::charge(&self.used_allocations, 1, self.allocation_limit)?;
        if let Err(error) = self.adopt(bytes) {
            self.used_allocations.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        assert!(self.used_allocations.fetch_sub(1, Ordering::AcqRel) != 0);
        self.release(bytes);
        if let Some(returned) = &self.credit_returned {
            returned(Credit::AllocationReservation);
        }
    }
    fn record_reserved_allocation(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        io::Error::new(io::ErrorKind::OutOfMemory, error)
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        self.check()?;
        for (counter, bytes) in [
            (&self.encoded, encoded),
            (&self.copied, copied),
            (&self.hashed, hashed),
        ] {
            self.preflight(counter, bytes, u64::MAX)?;
        }
        Ok(())
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        for (counter, bytes) in [
            (&self.encoded, encoded),
            (&self.copied, copied),
            (&self.hashed, hashed),
        ] {
            Self::charge(counter, bytes, u64::MAX)?;
        }
        Ok(())
    }
    fn reserve_codec(&self, maximum: NativeCodecWork) -> io::Result<()> {
        self.admit_codec(maximum.encoded, maximum.copied, maximum.hashed)?;
        self.record_codec(maximum.encoded, maximum.copied, maximum.hashed)
    }
    fn cancel_reserved_codec(&self, unused: NativeCodecWork) {
        for (counter, bytes) in [
            (&self.encoded, unused.encoded),
            (&self.copied, unused.copied),
            (&self.hashed, unused.hashed),
        ] {
            assert!(counter.fetch_sub(bytes, Ordering::AcqRel) >= bytes);
        }
        if let Some(returned) = &self.credit_returned {
            returned(Credit::Codec);
        }
    }
    fn record_reserved_codec(&self, _: NativeCodecWork) -> io::Result<()> {
        Ok(())
    }
    fn allocate_refcount_header(&self) -> io::Result<()> {
        self.allocate(0)
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.check()?;
        Self::charge(&self.memory, bytes, self.memory_limit).map(|_| ())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        match Self::charge(&self.memory, bytes, u64::MAX) {
            Err(error) => (0, Err(error)),
            Ok(previous) => (
                bytes,
                if previous + bytes > self.memory_limit {
                    Err(Self::refuse())
                } else {
                    self.check()
                },
            ),
        }
    }
    fn release(&self, bytes: u64) {
        let previous = self.memory.fetch_sub(bytes, Ordering::AcqRel);
        assert!(previous >= bytes);
        if previous == bytes
            && let Some(retired) = self
                .memory_retired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        {
            let _ = retired.send(());
        }
        if let Some(returned) = &self.credit_returned {
            returned(Credit::Allocation);
        }
    }
    fn examine(&self, items: u64, _: u64) -> io::Result<()> {
        self.check()?;
        self.record_examination(items, 0)
    }
    fn admit_examination(&self, items: u64, _: u64) -> io::Result<()> {
        self.check()?;
        match self.item_limit {
            Some(limit) => self.preflight(&self.examined_items, items, limit),
            None => Ok(()),
        }
    }
    fn record_examination(&self, items: u64, _: u64) -> io::Result<()> {
        if let Some(recorded) = &self.examination_recorded {
            recorded()?;
        }
        match self.item_limit {
            Some(limit) => Self::charge(&self.examined_items, items, limit).map(|_| ()),
            None => Ok(()),
        }
    }
    fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
        self.preflight(&self.used_bytes, maximum as u64, self.byte_limit)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.admit_bytes(maximum)?;
        self.reserve_operations(NativeOperationKind::Read, 1)
    }
    fn record_read(&self, bytes: usize) -> io::Result<()> {
        self.used_bytes.fetch_add(bytes as u64, Ordering::AcqRel);
        self.read.fetch_add(bytes as u64, Ordering::AcqRel);
        if self.cancel_after_read && bytes != 0 {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn finish_read(&self, _: usize, _completed: usize) -> io::Result<()> {
        #[cfg(test)]
        if self.panic_after_read && _completed != 0 {
            std::panic::panic_any(0x7072_6570_6172_6564_u64);
        }
        self.check()
    }
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> {
        self.admit_bytes(maximum)
    }
    fn admit_write(&self, maximum: usize) -> io::Result<()> {
        self.admit_read(maximum)
    }
    fn record_write(&self, bytes: usize) -> io::Result<()> {
        self.record_read(bytes)
    }
    fn admit_durability(&self) -> io::Result<()> {
        self.admit_write(0)
    }
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        self.reserve_operations(NativeOperationKind::Write, 1)?;
        Ok(1)
    }
    fn cancel_cleanup_write(&self, token: u64) {
        self.cancel_reserved_operations(NativeOperationKind::Write, token);
        if let Some(cancelled) = &self.cleanup_cancelled {
            cancelled();
        }
    }
    fn record_cleanup_write(&self, _: u64) -> io::Result<()> {
        self.record_reserved_operation(NativeOperationKind::Write)
    }
}

