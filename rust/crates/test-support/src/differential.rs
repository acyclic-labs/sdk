//! Independent current-thread runs with an ordered, caller-defined operation trace.
use std::{any::Any, cell::RefCell};
thread_local! {
    static TRACE: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

/// Record an event; inactive recording does nothing. A mismatched event type panics.
pub fn record<T: 'static>(event: T) {
    let mut pending = Some(event);
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace
                .downcast_mut::<Vec<T>>()
                .expect("trace event type")
                .push(pending.take().unwrap());
        }
    });
    // Inactive events may themselves record from Drop.
    drop(pending);
}

/// Initialize each side separately, then return its result and complete trace.
/// The callback receives `true` for the baseline and `false` for the candidate.
/// Captures this thread only; rejects overlapping capture and restores after panic.
pub fn pair<S, R, T: 'static>(
    mut initialize: impl FnMut() -> S,
    mut run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<T>); 2] {
    TRACE.with_borrow(|trace| assert!(trace.is_none(), "nested differential capture"));
    [true, false].map(|old| {
        let state = initialize();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                // Drop events after releasing the TLS borrow: their destructors
                // may themselves record operations.
                drop(TRACE.with_borrow_mut(Option::take));
            }
        }
        TRACE.with_borrow_mut(|trace| *trace = Some(Box::new(Vec::<T>::new())));
        let _reset = Reset;
        let result = run(state, old);
        (
            result,
            TRACE.with_borrow_mut(|trace| {
                *trace
                    .take()
                    .unwrap()
                    .downcast::<Vec<T>>()
                    .expect("trace event type")
            }),
        )
    })
}

#[cfg(test)]
mod tests;

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
