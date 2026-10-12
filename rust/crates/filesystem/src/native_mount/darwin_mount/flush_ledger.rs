use super::*;

/// Orders completed native mutations against source flushes.
///
/// A mutating callback advances `completed` only after its source call
/// returns, so a flush that samples `completed` before it starts covers every
/// mutation up to that sample. The ledger starts dirty because the state the
/// mount opened over may be unpublished, and the first sync must cover it.
pub(super) struct FlushLedger {
    pub(super) completed: AtomicU64,
    pub(super) flushed: AtomicU64,
    /// Serializes flushes: a waiter that a finished flush already covered
    /// returns without flushing again, so concurrent syncs share one flush.
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

    /// Records one completed (or failed, possibly partial) mutation and
    /// returns its sequence.
    pub(in crate::native_mount) fn record(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(in crate::native_mount) fn latest(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Makes every mutation up to `required` durable, flushing the source
    /// only when no earlier successful flush already covers it.
    pub(in crate::native_mount) fn flush_through(&self, required: u64, source: &dyn MountFilesystem) -> Result<(), i32> {
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let _flushing = self.flushing.lock().map_err(|_| libc::EIO)?;
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let covered = self.latest();
        source.flush().map_err(errno)?;
        self.flushed.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }
}

