use loom::sync::{Arc, Mutex};
use std::collections::BTreeSet;

/// Enumerate external event orders without mocking production TypeScript state.
///
/// # Panics
/// Fails the model if an event producer panics or a schedule lock is poisoned.
pub fn traces(
    left: &'static [&'static str],
    right: &'static [&'static str],
) -> std::io::Result<BTreeSet<Vec<&'static str>>> {
    // This outer mutex only collects completed schedules, outside the explored
    // threads. It neither controls nor prunes their ordering.
    let output = std::sync::Arc::new(std::sync::Mutex::new(BTreeSet::new()));
    let completed = std::sync::Arc::clone(&output);
    loom::model(move || {
        // One model-only lock rank: the external transport-event delivery order.
        // No production JS state is mocked or guarded by this Rust mutex.
        let events = Arc::new(Mutex::new(Vec::new()));
        let one = Arc::clone(&events);
        let one = loom::thread::spawn(move || {
            for event in left {
                let delivered = one.lock().map(|mut events| events.push(*event));
                assert!(delivered.is_ok(), "deliver first event: {delivered:?}");
            }
        });
        let two = Arc::clone(&events);
        let two = loom::thread::spawn(move || {
            for event in right {
                let delivered = two.lock().map(|mut events| events.push(*event));
                assert!(delivered.is_ok(), "deliver second event: {delivered:?}");
            }
        });
        let joined = one.join();
        assert!(joined.is_ok(), "join first event producer: {joined:?}");
        let joined = two.join();
        assert!(joined.is_ok(), "join second event producer: {joined:?}");
        let collected = events
            .lock()
            .map(|events| events.clone())
            .map_err(|error| error.to_string())
            .and_then(|trace| {
                completed
                    .lock()
                    .map(|mut output| output.insert(trace))
                    .map_err(|error| error.to_string())
            });
        assert!(collected.is_ok(), "collect event order: {collected:?}");
    });
    std::sync::Arc::try_unwrap(output)
        .map_err(|_| std::io::Error::other("event-order collector still has an owner"))?
        .into_inner()
        .map_err(|error| std::io::Error::other(format!("event-order collector poisoned: {error}")))
}
