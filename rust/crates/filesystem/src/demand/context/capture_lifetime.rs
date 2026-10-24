use super::*;

/// An accepted worker, including its completion payload, participates in
/// shutdown. Ordinary accounting handles retain funding without delaying it.
pub(crate) struct CaptureWorker {
    custody: CaptureCustody,
}

impl CaptureTransientCustody {
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn worker(
        custody: &CaptureCustody,
    ) -> Result<CaptureWorker, crate::NativeTreeMaterializationError> {
        // The low bit closes registration; each accepted payload owns two.
        // Admission and close modify the same atomic, so neither can miss the other.
        custody
            .workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                #[cfg(all(test, acyclic_fs_loom))]
                let closed = state & 1 != 0 && !model_checking::allow_closed_worker();
                #[cfg(not(all(test, acyclic_fs_loom)))]
                let closed = state & 1 != 0;
                (!closed).then(|| state.checked_add(2)).flatten()
            })
            .map_err(|state| {
                if state & 1 != 0 {
                    crate::NativeTreeMaterializationError::OriginalWorkPoisoned
                } else {
                    crate::WorkError::Overflow.into()
                }
            })?;
        let worker = CaptureWorker {
            custody: custody.clone(),
        };
        Ok(worker)
    }

    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn close(&self) -> Result<(), DemandError> {
        self.workers.fetch_or(1, Ordering::AcqRel);
        #[cfg(all(test, acyclic_fs_loom))]
        let wait = !model_checking::skip_worker_wait();
        #[cfg(not(all(test, acyclic_fs_loom)))]
        let wait = true;
        while wait && self.workers.load(Ordering::Acquire) != 1 {
            #[cfg(acyclic_fs_loom)]
            loom::thread::yield_now();
            #[cfg(not(acyclic_fs_loom))]
            std::thread::yield_now();
        }
        // Join in-flight transient accounting before draining retained bytes.
        // No worker wait or funding-owner destruction occurs under the leaf.
        #[cfg(all(test, acyclic_fs_loom))]
        if model_checking::close_without_leaf() {
            self.closed.store(true, Ordering::Release);
            let bytes = self.take_retained_bytes();
            return self.header.work().state(|work| work.release(bytes));
        }
        self.header.work().state(|work| {
            self.closed.store(true, Ordering::Release);
            work.release(self.take_retained_bytes())
        })
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        let previous = self.custody.workers.fetch_sub(2, Ordering::AcqRel);
        assert!(previous >= 2, "balanced capture worker retirement");
    }
}
