//! Independent current-thread runs with an ordered, caller-defined operation trace.
pub mod oracle;

use acyclic_differential as storage;

pub use storage::record;
use storage::{begin, capturing, end};

/// Initialize outside capture, then return one run's result and ordered events.
/// Captures this thread only; rejects overlapping capture and restores on panic.
pub fn observe<S, R, T: 'static>(
    initialize: impl FnOnce() -> S,
    run: impl FnOnce(S) -> R,
) -> (R, Vec<T>) {
    assert!(!capturing(), "nested differential capture");
    let state = initialize();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            // Event destructors may themselves record operations.
            drop(end());
        }
    }
    begin::<T>();
    let _reset = Reset;
    let result = run(state);
    (
        result,
        *end()
            .unwrap()
            .downcast::<Vec<T>>()
            .expect("trace event type"),
    )
}

/// Capture only polls of this future, using the original thread-local storage.
///
/// Other tasks polled while this future is pending do not enter its trace.
/// Work spawned onto other tasks or threads is not captured: those original
/// owners need their own captures and causal identities. A dropped or panicking
/// future leaves no active capture behind.
pub async fn observe_async<R, T: 'static>(
    future: impl std::future::Future<Output = R>,
) -> (R, Vec<T>) {
    let mut future = std::pin::pin!(future);
    let mut trace = Vec::new();
    let result = std::future::poll_fn(|cx| {
        let (result, events) = observe(|| (), |()| future.as_mut().poll(cx));
        trace.extend(events);
        result
    })
    .await;
    (result, trace)
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
