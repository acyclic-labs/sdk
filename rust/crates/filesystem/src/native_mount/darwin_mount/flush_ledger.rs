use super::*;

pub(super) struct FlushLedger {
    pub(super) completed: AtomicU64,
    pub(super) flushed: AtomicU64,
    pub(super) flushing: Mutex<()>,
}

impl FlushLedger {
    pub(super) const fn new() -> Self {
        Self {
            completed: AtomicU64::new(1),
            flushed: AtomicU64::new(0),
            flushing: Mutex::new(()),
        }
    }

    pub(in crate::native_mount) fn record(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(in crate::native_mount) fn latest(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    pub(in crate::native_mount) fn flush_through(
        &self,
        required: u64,
        source: &dyn MountFilesystem,
        work: &crate::NativeMaterializationWork,
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
