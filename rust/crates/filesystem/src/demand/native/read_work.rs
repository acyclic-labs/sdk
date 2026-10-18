//! Test observation of demand read work, recorded on the calling thread for
//! the differential harness.
use std::cell::Cell;

thread_local! {
    static RESTORE_OLD_WORK: Cell<bool> = const { Cell::new(false) };
}

/// Runs `run` with the removed zero-fill restored: the negative control.
pub(crate) fn restoring_old_work<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESTORE_OLD_WORK.set(false);
        }
    }
    RESTORE_OLD_WORK.set(true);
    let _reset = Reset;
    run()
}

pub(crate) fn record(operation: &'static str, offset: u64, length: usize) {
    acyclic_test_support::differential::record((operation, offset, length as u64));
}

/// Records new storage and the bytes initialized before any read fills it.
pub(crate) fn storage(data: &mut Vec<u8>) {
    let initialized = if RESTORE_OLD_WORK.get() {
        data.resize(data.capacity(), 0);
        let initialized = data.len();
        data.clear();
        initialized
    } else {
        0
    };
    record("storage", initialized as u64, data.capacity());
}

thread_local! {
    static EXTRA: Cell<bool> = const { Cell::new(false) };
    static ERRNO: Cell<Option<i32>> = const { Cell::new(None) };
}

pub(crate) fn take_extra() -> bool {
    EXTRA.replace(false)
}
pub(crate) fn adding_allocation<T>(run: impl FnOnce() -> T) -> T {
    EXTRA.set(true);
    let value = run();
    EXTRA.set(false);
    value
}
pub(crate) fn with_errno<T>(errno: i32, run: impl FnOnce() -> T) -> T {
    ERRNO.set(Some(errno));
    let value = run();
    ERRNO.set(None);
    value
}
pub(crate) fn injected_error() -> Result<(), super::DemandError> {
    match ERRNO.get() {
        Some(errno) => Err(std::io::Error::from_raw_os_error(errno).into()),
        None => Ok(()),
    }
}
pub(crate) fn capture<T>(run: impl FnOnce() -> T) -> (T, Vec<(&'static str, u64, u64)>) {
    acyclic_test_support::differential::observe(|| (), |()| run())
}
pub(crate) fn replay(trace: Vec<(&'static str, u64, u64)>) {
    for event in trace {
        acyclic_test_support::differential::record(event);
    }
}
