//! A held flush makes one native call and returns its original interruption.
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
    let result = unsafe { libc::syscall(libc::SYS_fsync, descriptor) };
    libc::c_int::try_from(result).unwrap_or(-1)
}

/// Runs `sync` under two injected interruptions, retaining the unused fault
/// count to distinguish refusal from a hidden second native call.
fn interrupted_twice(sync: impl FnOnce(&mut DemandWork)) -> (u64, u32, u32) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            INTERRUPTIONS.set(0);
        }
    }
    let _reset = Reset;
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    INTERRUPTIONS.set(2);
    CALLS.set(0);
    sync(&mut work);
    let calls = CALLS.get();
    (work.work.durability_operations, calls, INTERRUPTIONS.get())
}

#[test]
fn interrupted_held_flush_returns_after_the_original_native_call() {
    let file = tempfile::tempfile().unwrap();
    let (attempts, calls, remaining) = interrupted_twice(|work| {
        let error = sync_file_held(&file, work).unwrap_err();
        assert!(
            matches!(error, DemandError::Io(error) if error.kind() == io::ErrorKind::Interrupted)
        );
    });
    assert_eq!((attempts, calls, remaining), (1, 1, 1));
    sync_file_held(&file, &mut ordinary_work()).unwrap();
}

#[test]
fn negative_control_std_sync_all_hides_interruptions_inside_one_attempt() {
    let file = tempfile::tempfile().unwrap();
    let durability = WorkCounters {
        durability_operations: 1,
        ..WorkCounters::UNCHARGED
    };
    // The actual standard-library operation is the negative control: it
    // consumes both injected interruptions inside one admitted SDK operation.
    let (attempts, calls, remaining) = interrupted_twice(|work| {
        work.attempt_io(durability, || file.sync_all()).unwrap();
    });
    assert_eq!((attempts, calls, remaining), (1, 3, 0));
}
