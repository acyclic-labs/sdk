//! Per-attempt storage on the single standalone accounting ledger.
use super::*;

pub(super) type StandaloneLedger = Arc<
    SyncMutex<(
        WorkCounters,
        std::result::Result<AllocationLedger, acyclic_fs::WorkError>,
    )>,
>;

#[doc(hidden)]
pub struct StandaloneCustody {
    work: StandaloneLedger,
    pub(super) bytes: AtomicU64,
}

impl StandaloneCustody {
    pub(super) fn new(work: StandaloneLedger) -> Self {
        Self {
            work,
            bytes: AtomicU64::new(0),
        }
    }

    // The shared ledger lock serializes every change, including failed claims.
    pub(super) fn record(
        &self,
        before: u64,
        after: u64,
    ) -> std::result::Result<(), acyclic_fs::WorkError> {
        if before == after {
            return Ok(());
        }
        let owned = self.bytes.load(Ordering::Relaxed);
        let next = if after > before {
            owned.checked_add(after - before)
        } else {
            owned.checked_sub(before - after)
        }
        .ok_or(acyclic_fs::WorkError::Overflow)?;
        self.bytes.store(next, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for StandaloneCustody {
    fn drop(&mut self) {
        let bytes = self.bytes.load(Ordering::Relaxed);
        if bytes == 0 {
            return;
        }
        let mut state = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_, allocations) = &mut *state;
        if let Ok(ledger) = allocations
            && ledger.release_owned(bytes).is_err()
        {
            *allocations = Err(acyclic_fs::WorkError::Overflow);
        }
    }
}
