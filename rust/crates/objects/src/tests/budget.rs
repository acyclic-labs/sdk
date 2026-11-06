use std::sync::atomic::Ordering;

/// A meter that admits all but live allocation past `limit`, recording the
/// peak it held, for driving the native entry points.
pub(crate) struct Budget {
    pub(super) limit: u64,
    pub(super) held: std::sync::Mutex<(u64, u64)>,
    pub(super) cancelled: std::sync::atomic::AtomicBool,
    /// Physical reads left, unbounded when `None`.
    pub(super) reads: std::sync::Mutex<Option<u64>>,
    /// Writes left, unbounded when `None`.
    pub(super) writes: std::sync::Mutex<Option<u64>>,
    /// Whether a reservation withholds its writes; a hollow one charges
    /// each reserved write as it happens, as if nothing was reserved.
    pub(super) withholds: bool,
}

impl Budget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            held: std::sync::Mutex::new((0, 0)),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            reads: std::sync::Mutex::new(None),
            writes: std::sync::Mutex::new(None),
            withholds: true,
        }
    }

    #[cfg(feature = "s3")]
    pub(crate) fn live(&self) -> u64 {
        self.held.lock().unwrap().0
    }

    #[cfg(feature = "s3")]
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Spends original operation credit, refused past the budget.
    fn spend(counter: &std::sync::Mutex<Option<u64>>, count: u64) -> std::io::Result<()> {
        match &mut *counter.lock().unwrap() {
            Some(left) if *left < count => Err(std::io::ErrorKind::QuotaExceeded.into()),
            Some(left) => {
                *left -= count;
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn hold(&self, bytes: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::check(self)?;
        let mut held = self.held.lock().unwrap();
        if held.0 + bytes > self.limit {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        held.0 += bytes;
        held.1 = held.1.max(held.0);
        Ok(())
    }

    /// The peak live allocation since the last call.
    pub(super) fn peak(&self) -> u64 {
        let mut held = self.held.lock().unwrap();
        let live = held.0;
        std::mem::replace(&mut held.1, live)
    }
}

impl acyclic_native_runtime::NativeReadMeter for Budget {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
    fn reserve_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) -> std::io::Result<()> {
        if self.withholds {
            Self::spend(&self.writes, count)
        } else {
            Ok(())
        }
    }
    fn cancel_reserved_operations(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) {
        if self.withholds
            && let Some(left) = &mut *self.writes.lock().unwrap()
        {
            *left += count;
        }
    }
    fn record_reserved_operation(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<()> {
        if self.withholds {
            Ok(())
        } else {
            Self::spend(&self.writes, 1)
        }
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
    ) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64) {}
    fn record_reserved_bytes(
        &self,
        _: acyclic_native_runtime::NativeOperationKind,
        _: u64,
        _: u64,
    ) -> std::io::Result<()> {
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        self.release(bytes);
    }
    fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
        std::io::ErrorKind::OutOfMemory.into()
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.hold(bytes)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        let mut held = self.held.lock().unwrap();
        held.0 += bytes;
        held.1 = held.1.max(held.0);
        (bytes, Ok(()))
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
        self.held.lock().unwrap().0 -= bytes;
    }
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_read(&self, _: usize) -> std::io::Result<()> {
        self.check()?;
        Self::spend(&self.reads, 1)
    }
    fn record_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> {
        self.check()?;
        Self::spend(&self.writes, 1)
    }
    fn record_write(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        Ok(())
    }
}
