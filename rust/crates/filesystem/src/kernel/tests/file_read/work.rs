//! Test observation of file-read output work, recorded on the calling thread
//! for the differential harness.
use std::cell::Cell;

/// Output event: name, output offset or count, and bytes.
pub(crate) type Work = (&'static str, u64, u64);

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

pub(crate) fn record(operation: &'static str, at: usize, bytes: usize) {
    acyclic_test_support::differential::record::<Work>((operation, at as u64, bytes as u64));
}

/// Records the output buffer and the bytes initialized before spans fill it.
pub(crate) fn storage(data: &mut Vec<u8>) {
    let initialized = if RESTORE_OLD_WORK.get() {
        data.resize(data.capacity(), 0);
        let initialized = data.len();
        data.clear();
        initialized
    } else {
        0
    };
    record("storage", initialized, data.capacity());
}
