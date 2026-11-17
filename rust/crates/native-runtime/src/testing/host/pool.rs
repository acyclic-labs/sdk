//! Physical startup yields when every worker is parked or occupied outside DST.
use super::*;
use crate::sync::MutexGuard;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct Pool {
    threads: usize,
    started: AtomicUsize,
    unavailable: AtomicUsize,
    progress: Mutex<()>,
    changed: Condvar,
}

impl Pool {
    pub(crate) fn new(threads: usize) -> Arc<Self> {
        Arc::new(Self {
            threads,
            started: AtomicUsize::new(0),
            unavailable: AtomicUsize::new(0),
            progress: Mutex::new(()),
            changed: Condvar::new(),
        })
    }

    /// Records one worker this pool actually started. A seeded world starts
    /// only the workers its poller needs, so saturation is measured against
    /// the started width, never the host's maximum.
    pub(crate) fn started(&self) {
        let started = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        debug_assert!(started <= self.threads, "worker pool exceeded its width");
        self.changed();
    }

    pub(super) fn park(&self) -> bool {
        let unavailable = self.unavailable.fetch_add(1, Ordering::SeqCst) + 1;
        // Read the width after claiming this slot: another worker may have
        // started between the claim and the read, and the earlier width would
        // hide the saturated state from the driver.
        let started = self.started.load(Ordering::SeqCst);
        let saturated = started != 0 && unavailable == started;
        if saturated {
            self.changed();
        }
        saturated
    }

    pub(super) fn resume(&self) {
        self.unavailable.fetch_sub(1, Ordering::SeqCst);
    }

    // A controlled running closure remains atomic. An unscoped physical job
    // may await the root, so it cannot be mistaken for an available worker.
    pub(crate) fn unscoped(&self) -> Occupant<'_> {
        self.park();
        Occupant(self)
    }

    // Called without a host-state lock. This gate and predicate check prevent
    // a physical transition from racing the driver's condition-variable wait.
    pub(super) fn changed(&self) {
        let _progress = lock(&self.progress);
        self.changed.notify_all();
    }

    pub(super) fn start_or_saturation<'a>(&self, host: &'a Mutex<State>) -> MutexGuard<'a, State> {
        let mut progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            let state = host.lock().unwrap_or_else(PoisonError::into_inner);
            if state.phase != Phase::Starting
                || state.io_ready
                || self.unavailable.load(Ordering::SeqCst) == self.started.load(Ordering::SeqCst)
            {
                drop(progress);
                return state;
            }
            drop(state);
            progress = self
                .changed
                .wait(progress)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

pub(crate) struct Occupant<'a>(&'a Pool);

impl Drop for Occupant<'_> {
    fn drop(&mut self) {
        self.0.resume();
        self.0.changed();
    }
}
