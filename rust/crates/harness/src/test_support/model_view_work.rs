//! Counts the real identity decoder invocations and their admitted input bytes.
use std::cell::Cell;
thread_local! {
    static WORK: Cell<Option<(u64,u64)>> = const { Cell::new(None) };
}
/// Measures (requests parsed, input bytes) on the current synchronous thread.
/// # Panics
/// Panics when measurements nest on the same thread.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, (u64, u64)) {
    WORK.with(|counter| {
        assert!(counter.get().is_none(), "nested model-view measurement");
        struct Reset<'a>(&'a Cell<Option<(u64, u64)>>);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.set(None);
            }
        }
        counter.set(Some((0, 0)));
        let _reset = Reset(counter);
        let value = operation();
        (value, counter.get().unwrap_or_default())
    })
}
pub(crate) fn parsed(bytes: usize) {
    WORK.with(|counter| {
        if let Some((requests, prior)) = counter.get() {
            counter.set(Some((requests + 1, prior + bytes as u64)));
        }
    });
}
