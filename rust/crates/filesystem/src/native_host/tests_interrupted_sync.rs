//! A held flush makes exactly one native call per metered attempt, so every
//! interrupted call is charged and passes a cancellation check.
use super::*;
use crate::demand::DemandWork;
use crate::{CancellationToken, WorkCounters};
use std::cell::Cell;

thread_local! {
    static INTERRUPTIONS: Cell<u32> = const { Cell::new(0) };
    static CALLS: Cell<u32> = const { Cell::new(0) };
}

/// Interposes libc's `fsync` for this test binary. On the probing thread, the
/// next `INTERRUPTIONS` calls fail with `EINTR` and every call is counted;
/// every call otherwise reaches the kernel's fsync unchanged.
#[unsafe(no_mangle)]
extern "C" fn fsync(descriptor: libc::c_int) -> libc::c_int {
    let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
    let interrupted = INTERRUPTIONS
        .try_with(|left| {
            let interrupted = left.get() > 0;
            left.set(left.get().saturating_sub(u32::from(interrupted)));
            interrupted
        })
        .unwrap_or(false);
    if interrupted {
        // SAFETY: errno is this thread's own slot.
        unsafe { *libc::__errno_location() = libc::EINTR };
        return -1;
    }
    // SAFETY: the raw syscall acts only on the caller's descriptor.
    unsafe { libc::syscall(libc::SYS_fsync, descriptor) as libc::c_int }
}

/// Runs `sync` under two injected interruptions; returns the metered
/// attempts and the native calls it made.
fn interrupted_twice(sync: impl FnOnce(&mut DemandWork)) -> (u64, u32) {
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    INTERRUPTIONS.set(2);
    CALLS.set(0);
    sync(&mut work);
    let calls = CALLS.get();
    assert_eq!(INTERRUPTIONS.get(), 0, "both interruptions were delivered");
    (work.work.durability_operations, calls)
}

#[test]
fn every_interrupted_held_flush_is_its_own_metered_attempt() {
    let file = tempfile::tempfile().unwrap();
    let (attempts, calls) = interrupted_twice(|work| sync_file_held(&file, work).unwrap());
    assert_eq!((attempts, calls), (3, 3));
}

#[test]
fn negative_control_std_sync_all_hides_interruptions_inside_one_attempt() {
    let file = tempfile::tempfile().unwrap();
    let durability = WorkCounters {
        durability_operations: 1,
        ..WorkCounters::UNCHARGED
    };
    // The round-2 helper: std retries EINTR inside one metered attempt.
    let (attempts, calls) = interrupted_twice(|work| {
        open::retry_io(work, durability, || file.sync_all()).unwrap();
    });
    assert_eq!((attempts, calls), (1, 3));
}
