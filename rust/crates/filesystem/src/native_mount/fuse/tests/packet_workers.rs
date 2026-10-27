//! Real worker retirement must finish before returning a callback failure.

use super::super::packet_workers::{self, PacketWorkers};
use super::*;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Condvar, Mutex};

struct Workers {
    started: Barrier,
    stopped: (Mutex<bool>, Condvar),
    retired: AtomicUsize,
    panic: bool,
}

impl PacketWorkers for Workers {
    fn stop(&self) {
        *self.stopped.0.lock().unwrap() = true;
        self.stopped.1.notify_all();
    }

    fn run_worker(&self, index: u32) -> Result<(), NativeMountError> {
        self.started.wait();
        if index == 0 {
            if self.panic {
                panic!("packet callback panic for positive-join control");
            }
            return Err(libc::EIO.into());
        }
        let mut stopped = self.stopped.0.lock().unwrap();
        while !*stopped {
            stopped = self.stopped.1.wait(stopped).unwrap();
        }
        self.retired.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

#[test]
fn callback_failure_and_panic_join_the_other_physical_worker() {
    for panic in [false, true] {
        let cancellation = crate::CancellationToken::new();
        let work = cache_work(FINITE_CACHE_BUDGET, &cancellation);
        let baseline = work.live_allocation_bytes();
        let owner = Workers {
            started: Barrier::new(2),
            stopped: (Mutex::new(false), Condvar::new()),
            retired: AtomicUsize::new(0),
            panic,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            packet_workers::run(&owner, NonZeroU32::new(2).unwrap(), &work)
        }));
        if panic {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(owner.retired.load(Ordering::Acquire), 1);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
