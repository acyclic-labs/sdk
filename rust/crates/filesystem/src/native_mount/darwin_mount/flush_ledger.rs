use super::*;

pub(super) struct FlushLedger {
    completed: AtomicU64,
    flushed: AtomicU64,
    flushing: Mutex<()>,
}

impl FlushLedger {
    pub(super) const fn new() -> Self {
        Self {
            completed: AtomicU64::new(1),
            flushed: AtomicU64::new(0),
            flushing: Mutex::new(()),
        }
    }

    pub(super) fn record(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(super) fn latest(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    pub(super) fn flush_through(
        &self,
        required: u64,
        source: &dyn MountFilesystem,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let _flushing = self.flushing.lock().map_err(|_| driver_errno(libc::EIO))?;
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let covered = self.latest();
        source.flush(work)?;
        self.flushed.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }
}
