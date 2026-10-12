#[cfg(not(target_arch = "wasm32"))]
pub(in crate::v1) struct NativeTransportMeter {
    pub memory: std::sync::atomic::AtomicU64,
    pub limit: std::sync::atomic::AtomicU64,
    pub cancelled: std::sync::atomic::AtomicBool,
    pub cancel_on_copy: std::sync::atomic::AtomicU64,
    pub quota_on_copy: std::sync::atomic::AtomicU64,
    pending_allocations: parking_lot::Mutex<(u64, u64)>,
    pub reads: std::sync::atomic::AtomicU64,
    pub writes: std::sync::atomic::AtomicU64,
    pub(super) reserved: std::sync::atomic::AtomicU64,
}
#[cfg(not(target_arch = "wasm32"))]
impl NativeTransportMeter {
    pub(in crate::v1) fn new() -> Self {
        Self {
            memory: 0.into(),
            limit: u64::MAX.into(),
            cancelled: false.into(),
            cancel_on_copy: u64::MAX.into(),
            quota_on_copy: u64::MAX.into(),
            pending_allocations: parking_lot::Mutex::new((0, 0)),
            reads: 0.into(),
            writes: 0.into(),
            reserved: 0.into(),
        }
    }
    fn claim(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        acyclic_native_runtime::NativeReadMeter::check(self)?;
        let pending = self.pending_allocations.lock();
        let retained = self
            .memory
            .load(SeqCst)
            .checked_add(bytes)
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        if retained
            .checked_add(pending.0)
            .is_none_or(|value| value > self.limit.load(SeqCst))
        {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        self.memory.fetch_add(bytes, SeqCst);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for NativeTransportMeter {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            Err(std::io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.claim(bytes)
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        self.check()?;
        let mut pending = self.pending_allocations.lock();
        let retained = pending
            .0
            .checked_add(bytes)
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        let attempts = pending
            .1
            .checked_add(1)
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        if self
            .memory
            .load(SeqCst)
            .checked_add(retained)
            .is_none_or(|value| value > self.limit.load(SeqCst))
        {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        *pending = (retained, attempts);
        Ok(())
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        let mut pending = self.pending_allocations.lock();
        assert!(
            pending.0 >= bytes && pending.1 > 0,
            "allocation credit cannot retire twice"
        );
        pending.0 -= bytes;
        pending.1 -= 1;
    }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut pending = self.pending_allocations.lock();
        assert!(
            pending.0 >= bytes && pending.1 > 0,
            "owner attempt requires its original allocation credit"
        );
        pending.0 -= bytes;
        pending.1 -= 1;
        self.memory.fetch_add(bytes, SeqCst);
        Ok(())
    }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
        std::io::ErrorKind::OutOfMemory.into()
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.claim(0)
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.claim(bytes)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.memory
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        (bytes, self.check())
    }
    fn release(&self, bytes: u64) {
        let previous = self
            .memory
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
        assert!(
            previous >= bytes,
            "original allocation claim cannot retire twice"
        );
    }
    fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> {
        self.check()
    }
    fn record_codec(&self, encoded: u64, copied: u64, _: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        if encoded == 0 && copied >= self.cancel_on_copy.load(SeqCst) {
            self.cancelled.store(true, SeqCst);
        }
        if encoded == 0 && copied >= self.quota_on_copy.load(SeqCst) {
            self.limit.store(0, SeqCst);
        }
        Ok(())
    }
    fn examine(&self, _: u64, copied: u64) -> std::io::Result<()> {
        self.record_codec(0, copied, 0)
    }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        self.check()
    }
    fn record_examination(&self, _: u64, copied: u64) -> std::io::Result<()> {
        self.record_codec(0, copied, 0)
    }
    fn reserve_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) -> std::io::Result<()> {
        self.check()?;
        self.reserved
            .fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn cancel_reserved_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) {
        let previous = self
            .reserved
            .fetch_sub(count, std::sync::atomic::Ordering::SeqCst);
        assert!(previous >= count, "operation credit released twice");
    }
    fn record_reserved_operation(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        assert!(
            self.reserved.fetch_sub(1, SeqCst) > 0,
            "RPC attempt lacks its original reservation"
        );
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => {
                self.reads.fetch_add(1, SeqCst);
            }
            acyclic_native_runtime::NativeOperationKind::Write => {
                self.writes.fetch_add(1, SeqCst);
            }
            _ => panic!("transport cannot invent a non-RPC backend operation"),
        }
        self.check()
    }
    fn reserve_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
    ) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn cancel_reserved_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64) {
        panic!("transport cannot reserve invented physical bytes")
    }
    fn record_reserved_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
        _: u64,
    ) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn admit_read(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn record_read(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn record_write(&self, _: usize) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}
