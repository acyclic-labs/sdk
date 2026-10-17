//! Independent-state differential runs with unwind-safe operation traces.
use std::cell::RefCell;

/// All physical allocation fields, without a lossy sum or tolerance.
pub fn allocation_counts(info: crate::AllocationInfo) -> [i128; 6] {
    [
        i128::from(info.count_total),
        i128::from(info.count_current),
        i128::from(info.count_max),
        i128::from(info.bytes_total),
        i128::from(info.bytes_current),
        i128::from(info.bytes_max),
    ]
}

thread_local! {
    static TRACE: RefCell<Option<Vec<(&'static str, u64)>>> = const { RefCell::new(None) };
}

/// Record an actual production operation during a differential run.
pub fn record(operation: &'static str, amount: u64) {
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace.push((operation, amount));
        }
    });
}

/// Initialize disjoint state for the old and candidate paths, capturing each trace.
pub fn pair<S, R>(
    mut initialize: impl FnMut() -> S,
    mut run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<(&'static str, u64)>); 2] {
    [true, false].map(|old| {
        let state = initialize();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TRACE.with_borrow_mut(|trace| *trace = None);
            }
        }
        TRACE.with_borrow_mut(|trace| {
            assert!(trace.is_none());
            *trace = Some(Vec::new());
        });
        let _reset = Reset;
        let result = run(state, old);
        (result, TRACE.with_borrow_mut(|trace| trace.take().unwrap()))
    })
}
