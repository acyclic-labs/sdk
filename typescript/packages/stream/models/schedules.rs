use loom::sync::{Arc, Mutex};
use std::collections::BTreeSet;

pub fn traces(
    left: &'static [&'static str],
    right: &'static [&'static str],
) -> BTreeSet<Vec<&'static str>> {
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
                one.lock().unwrap().push(*event);
            }
        });
        let two = Arc::clone(&events);
        let two = loom::thread::spawn(move || {
            for event in right {
                two.lock().unwrap().push(*event);
            }
        });
        one.join().unwrap();
        two.join().unwrap();
        let trace = events.lock().unwrap().clone();
        completed.lock().unwrap().insert(trace);
    });
    std::sync::Arc::try_unwrap(output)
        .unwrap()
        .into_inner()
        .unwrap()
}
