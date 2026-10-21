//! Physical startup yields when every worker is parked or occupied outside DST.
use super::*;
#[cfg(acyclic_native_runtime_loom)]
use loom::sync::{
    MutexGuard,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(not(acyclic_native_runtime_loom))]
use std::sync::{
    MutexGuard,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) struct Pool {
    threads: usize,
    unavailable: AtomicUsize,
    progress: Mutex<()>,
    changed: Condvar,
}

impl Pool {
    pub(crate) fn new(threads: usize) -> Arc<Self> {
        Arc::new(Self {
            threads,
            unavailable: AtomicUsize::new(0),
            progress: Mutex::new(()),
            changed: Condvar::new(),
        })
    }

    pub(super) fn park(&self) -> bool {
        self.unavailable.fetch_add(1, Ordering::SeqCst) + 1 == self.threads
    }

    pub(super) fn resume(&self) {
        self.unavailable.fetch_sub(1, Ordering::SeqCst);
    }

    // A controlled running closure remains atomic. An unscoped physical job
    // may await the root, so it cannot be mistaken for an available worker.
    pub(crate) fn unscoped(&self) -> Occupant<'_> {
        if self.park() {
            self.changed();
        }
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
                || self.unavailable.load(Ordering::SeqCst) == self.threads
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
