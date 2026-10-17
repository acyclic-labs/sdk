//! Reusable readers for files under roots that are not local.
//!
//! A read under such a root can block for as long as its server does, so it
//! runs off the callback thread. Readers are kept and reused instead of a
//! thread per read. Each accepted read gets a reader that is not running
//! another read, so no read ever waits behind a blocked one; when every
//! permitted reader is busy, the read is refused. A reader is free again
//! before its answer is delivered, so a caller that has its answer can
//! always submit the next read.

#[cfg(all(test, acyclic_fs_native_loom))]
use loom::{
    sync::{Condvar, Mutex},
    thread,
};
use std::sync::{Arc, PoisonError};
#[cfg(not(all(test, acyclic_fs_native_loom)))]
use std::{
    sync::{Condvar, Mutex},
    thread,
};

/// One read; it yields the delivery of its answer to the waiting caller.
pub(super) trait RemoteRead: Send + 'static {
    type Delivery: RemoteDelivery;
    fn read(self) -> Self::Delivery;
}

/// An answer not yet handed to its caller.
pub(super) trait RemoteDelivery {
    fn deliver(self);
}

pub(super) struct RemoteReaders<J> {
    limit: usize,
    // This is the only pool lock. Never hold it while reading or delivering.
    state: Mutex<RemoteState<J>>,
    work: Condvar,
    /// The negative control: free the reader only after delivering.
    #[cfg(all(test, acyclic_fs_native_loom))]
    late_release: bool,
}

struct RemoteState<J> {
    /// Accepted reads, each with a free reader already claimed for it.
    queued: std::collections::VecDeque<J>,
    /// Free readers not yet claimed by an accepted read.
    idle: usize,
    /// Readers started, busy or free.
    readers: usize,
    #[cfg(test)]
    closed: bool,
}

impl<J: RemoteRead> RemoteReaders<J> {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(RemoteState {
                queued: std::collections::VecDeque::new(),
                idle: 0,
                readers: 0,
                #[cfg(test)]
                closed: false,
            }),
            work: Condvar::new(),
            #[cfg(all(test, acyclic_fs_native_loom))]
            late_release: false,
        })
    }

    /// Starts `job` on a free reader, or on a new one while fewer than the
    /// limit exist (then true); returns the job when every permitted reader is busy.
    pub(super) fn submit(self: &Arc<Self>, job: J) -> Result<bool, J> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        if state.closed {
            return Err(job);
        }
        if state.idle > 0 {
            state.idle -= 1;
            state.queued.push_back(job);
            drop(state);
            self.work.notify_one();
            return Ok(false);
        }
        if state.readers >= self.limit {
            return Err(job);
        }
        let readers = Arc::clone(self);
        let started = thread::Builder::new()
            .name("acyclic-remote-read".to_owned())
            .spawn(move || serve(&readers));
        if started.is_err() {
            return Err(job);
        }
        // The new reader takes its first job once this lock is released.
        state.readers += 1;
        state.queued.push_back(job);
        Ok(true)
    }

    fn releases_late(&self) -> bool {
        #[cfg(all(test, acyclic_fs_native_loom))]
        return self.late_release;
        #[cfg(not(all(test, acyclic_fs_native_loom)))]
        false
    }

    /// Readers started so far, busy or free.
    #[cfg(test)]
    pub(super) fn started(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .readers
    }

    /// Drains accepted reads and stops readers after their final delivery.
    #[cfg(test)]
    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        self.work.notify_all();
    }

    #[cfg(test)]
    pub(super) fn wait_closed(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.closed);
        while state.readers != 0 {
            state = self
                .work
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    #[cfg(all(test, acyclic_fs_native_loom))]
    pub(super) fn releasing_late(limit: usize) -> Arc<Self> {
        let mut readers = Self::new(limit);
        if let Some(readers) = Arc::get_mut(&mut readers) {
            readers.late_release = true;
        }
        readers
    }
}

/// Runs reads for the life of the process. A panicking read leaves the reader
/// serving and drops its job, so the caller sees its answer channel close.
fn serve<J: RemoteRead>(readers: &RemoteReaders<J>) {
    let mut state = readers.state.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        let Some(job) = state.queued.pop_front() else {
            #[cfg(test)]
            if state.closed {
                state.idle -= 1;
                state.readers -= 1;
                readers.work.notify_all();
                return;
            }
            state = readers
                .work
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        };
        drop(state);
        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.read())).ok();
        if readers.releases_late() {
            if let Some(answer) = answer {
                answer.deliver();
            }
            state = readers.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.idle += 1;
            continue;
        }
        // Free before delivering: the caller that has its answer finds this
        // reader. Delivery runs unlocked, since an abandoned answer drops there.
        readers
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .idle += 1;
        if let Some(answer) = answer {
            answer.deliver();
        }
        state = readers.state.lock().unwrap_or_else(PoisonError::into_inner);
    }
}

#[cfg(all(test, acyclic_fs_native_loom))]
#[path = "remote_loom_tests.rs"]
mod loom_tests;
