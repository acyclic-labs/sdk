//! A held flush makes one native call and returns its original interruption.
use super::*;
use crate::demand::DemandWork;
use crate::{CancellationToken, WorkCounters};
use std::cell::Cell;

thread_local! {
    static INTERRUPTIONS: Cell<u32> = const { Cell::new(0) };
    static CALLS: Cell<u32> = const { Cell::new(0) };
    static DATA_CALLS: Cell<u32> = const { Cell::new(0) };
    static RESIZE_INTERRUPTION: Cell<bool> = const { Cell::new(false) };
    static RESIZE_CALLS: Cell<u32> = const { Cell::new(0) };
}

/// Interposes libc's `fsync` for this test binary. On the probing thread, the
/// next `INTERRUPTIONS` calls fail with `EINTR` and every call is counted;
/// every call otherwise reaches the kernel's fsync unchanged.
#[unsafe(no_mangle)]
extern "C" fn fsync(descriptor: libc::c_int) -> libc::c_int {
    if interrupt_flush() {
        return -1;
    }
    // SAFETY: the raw syscall acts only on the caller's descriptor.
    let result = unsafe { libc::syscall(libc::SYS_fsync, descriptor) };
    libc::c_int::try_from(result).unwrap_or(-1)
}

fn interrupt_flush() -> bool {
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
    }
    interrupted
}

#[unsafe(no_mangle)]
extern "C" fn fdatasync(descriptor: libc::c_int) -> libc::c_int {
    let _ = DATA_CALLS.try_with(|calls| calls.set(calls.get() + 1));
    if interrupt_flush() {
        return -1;
    }
    // SAFETY: the raw data-only flush acts on the caller's descriptor.
    let result = unsafe { libc::syscall(libc::SYS_fdatasync, descriptor) };
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
    DATA_CALLS.set(0);
    sync(&mut work);
    let calls = CALLS.get();
    (work.work.durability_operations, calls, INTERRUPTIONS.get())
}

#[test]
fn interrupted_held_flush_returns_after_the_original_native_call() {
    let file = tempfile::tempfile().unwrap();
    for flush in [0, 1, 2, 3, 4] {
        let operation = |work: &mut DemandWork| match flush {
            0 => sync_file_held(&file, work),
            _ => work.attempt_io(
                WorkCounters {
                    durability_operations: 1,
                    ..WorkCounters::UNCHARGED
                },
                || match flush {
                    1 => acyclic_native_runtime::sync_file(
                        &file,
                        acyclic_native_runtime::Durability::Full,
                        None,
                    ),
                    2 => acyclic_native_runtime::flush_file_all(&file),
                    3 => acyclic_native_runtime::flush_file_data(&file),
                    _ => acyclic_native_runtime::sync_data(
                        &file,
                        acyclic_native_runtime::Durability::Full,
                    ),
                },
            ),
        };
        let (attempts, calls, remaining) = interrupted_twice(|work| {
            let error = operation(work).unwrap_err();
            assert!(
                matches!(error, DemandError::Io(error) if error.kind() == io::ErrorKind::Interrupted)
            );
        });
        assert_eq!((attempts, calls, remaining), (1, 1, 1));
        assert_eq!(DATA_CALLS.get(), u32::from(flush >= 3));
        operation(&mut ordinary_work()).unwrap();
        assert_eq!(CALLS.get(), 2);
        assert_eq!(DATA_CALLS.get(), 2 * u32::from(flush >= 3));
    }
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

// Both libc names reach one real kernel resize; this also observes std's
// large-file entry point without duplicating the physical fault producer.
#[cfg(target_pointer_width = "64")]
fn resize(descriptor: libc::c_int, length: libc::off_t) -> libc::c_int {
    let _ = RESIZE_CALLS.try_with(|calls| calls.set(calls.get() + 1));
    if RESIZE_INTERRUPTION
        .try_with(|fault| fault.replace(false))
        .unwrap_or(false)
    {
        // SAFETY: errno is this thread's own slot.
        unsafe { *libc::__errno_location() = libc::EINTR };
        return -1;
    }
    // SAFETY: the raw syscall acts only on the caller's descriptor and length.
    let result = unsafe { libc::syscall(libc::SYS_ftruncate, descriptor, length) };
    libc::c_int::try_from(result).unwrap_or(-1)
}

#[cfg(target_pointer_width = "64")]
#[unsafe(no_mangle)]
extern "C" fn ftruncate(descriptor: libc::c_int, length: libc::off_t) -> libc::c_int {
    resize(descriptor, length)
}

#[cfg(target_pointer_width = "64")]
#[unsafe(no_mangle)]
extern "C" fn ftruncate64(descriptor: libc::c_int, length: libc::off64_t) -> libc::c_int {
    resize(descriptor, length)
}

#[cfg(target_pointer_width = "64")]
#[test]
fn interrupted_resize_keeps_physical_length_and_returns_after_one_charged_call() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESIZE_INTERRUPTION.set(false);
        }
    }
    let _reset = Reset;
    let file = tempfile::tempfile().unwrap();
    acyclic_native_runtime::write_all_at(&file, 0, b"retained", None).unwrap();
    let mut work = ordinary_work();
    RESIZE_CALLS.set(0);
    RESIZE_INTERRUPTION.set(true);
    let error = truncate_file(&file, 3, &mut work).unwrap_err();
    assert!(matches!(error, DemandError::Io(error) if error.kind() == io::ErrorKind::Interrupted));
    assert_eq!(RESIZE_CALLS.get(), 1);
    assert_eq!(work.work.backend_write_operations, 1);
    assert_eq!(file.metadata().unwrap().len(), 8);
    // A later explicit invocation has its own admission and physical result.
    truncate_file(&file, 3, &mut work).unwrap();
    assert_eq!(RESIZE_CALLS.get(), 2);
    assert_eq!(work.work.backend_write_operations, 2);
    assert_eq!(file.metadata().unwrap().len(), 3);
    let before = work.work;
    let error = truncate_file(&file, u64::MAX, &mut work).unwrap_err();
    assert!(matches!(error, DemandError::Work(_)));
    assert_eq!(work.work, before);
    assert_eq!(RESIZE_CALLS.get(), 2);
    // The actual former std operation consumes an interruption and mutates
    // the file under one admitted call, violating the same physical oracle.
    RESIZE_INTERRUPTION.set(true);
    work.attempt_io(WRITE, || file.set_len(1)).unwrap();
    assert_eq!(RESIZE_CALLS.get(), 4);
    assert_eq!(file.metadata().unwrap().len(), 1);
}
