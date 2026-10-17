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

/// Initialize outside capture, then return one run's result and ordered events.
/// Captures this thread only; rejects overlapping capture and restores on panic.
pub fn observe<S, R, T: 'static>(
    initialize: impl FnOnce() -> S,
    run: impl FnOnce(S) -> R,
) -> (R, Vec<T>) {
    TRACE.with_borrow(|trace| assert!(trace.is_none(), "nested differential capture"));
    let state = initialize();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            // Event destructors may themselves record operations.
            drop(TRACE.with_borrow_mut(Option::take));
        }
    }
    TRACE.with_borrow_mut(|trace| *trace = Some(Box::new(Vec::<T>::new())));
    let _reset = Reset;
    let result = run(state);
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
}

/// Initialize each side separately; `run` receives true for the baseline.
pub fn pair<S, R, T: 'static>(
    mut initialize: impl FnMut() -> S,
    mut run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<T>); 2] {
    [true, false].map(|old| observe(&mut initialize, |state| run(state, old)))
}

#[cfg(test)]
mod tests;
