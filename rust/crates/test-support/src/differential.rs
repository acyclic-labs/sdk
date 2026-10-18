//! Independent current-thread runs with an ordered, caller-defined operation trace.
pub mod oracle;

use std::{any::Any, cell::RefCell};
thread_local! {
    static TRACE: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

/// Record an event into an active capture of the same event type. Inactive
/// recording, or an event of another type, does nothing: independent
/// observation points share code paths, and a capture subscribes to exactly
/// one of them by its type.
pub fn record<T: 'static>(event: T) {
    let mut pending = Some(event);
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace.as_mut().and_then(|trace| trace.downcast_mut::<Vec<T>>()) {
            trace.push(pending.take().unwrap());
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

/// A named operation event. Observation points that share this type are told
/// apart by name.
pub type Operation = (&'static str, u64);

/// `pair` over named operations, keeping only the named `kinds` in order: a
/// capture subscribes to the operations it asserts, so another observation
/// point on the same code path cannot enter its trace.
pub fn pair_of<S, R>(
    kinds: &[&str],
    initialize: impl FnMut() -> S,
    run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<Operation>); 2] {
    pair::<S, R, Operation>(initialize, run).map(|(result, trace)| {
        (
            result,
            trace
                .into_iter()
                .filter(|(kind, _)| kinds.contains(kind))
                .collect(),
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
