use super::*;
use crate::sync::owned::{Arc as GateArc, Mutex as GateMutex};
use acyclic_stream::verification::block_on;
use std::cell::Cell;

thread_local! {
    static OUTSIDE_THE_GATE: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn observes_outside_the_gate() -> bool {
    OUTSIDE_THE_GATE.with(Cell::get)
}

/// The durable facts routing reads: the operation's retained continuation and
/// its admission record (`Some(parent)`, where a root has no parent).
#[derive(Default)]
struct Durable {
    continuation: bool,
    admission: Option<Option<TaskId>>,
}

/// Two submissions of one operation beneath a settled parent race through
/// the production `route` and `routed`. Reads are separate, as in
/// `submit_to`, so only the shared gate keeps them consistent.
fn race(outside: bool) {
    let parent = TaskId::from_bytes([1; 16]);
    let durable = loom::sync::Arc::new(loom::sync::Mutex::new(Durable::default()));
    let gate = GateArc::new(GateMutex::new(()));
    let peers: Vec<_> = (0..2)
        .map(|_| {
            let durable = durable.clone();
            let gate = gate.clone();
            loom::thread::spawn(move || {
                OUTSIDE_THE_GATE.with(|flag| flag.set(outside));
                block_on(routed(
                    &gate,
                    async || {
                        let continuation = durable.lock().unwrap().continuation;
                        let existing = durable.lock().unwrap().admission;
                        route(parent, continuation, false, true, existing)
                    },
                    async |route| {
                        let mut durable = durable.lock().unwrap();
                        match route {
                            Route::Continue { .. } => {
                                durable.continuation = true;
                                match durable.admission {
                                    None | Some(None) => durable.admission = Some(None),
                                    Some(Some(_)) => {
                                        return Err(Error::Conflict("root mismatch".into()));
                                    }
                                }
                            }
                            Route::Child => match durable.admission {
                                None => durable.admission = Some(Some(parent)),
                                Some(retained) if retained == Some(parent) => {}
                                Some(_) => {
                                    return Err(Error::Conflict(
                                        "inheritance rejected the parent".into(),
                                    ));
                                }
                            },
                        }
                        Ok(route)
                    },
                ))
            })
        })
        .collect();
    for peer in peers {
        let routed = peer.join().unwrap();
        assert!(
            matches!(routed, Ok(Route::Continue { .. })),
            "a concurrent retry rejected the winning admission: {routed:?}"
        );
    }
    assert_eq!(durable.lock().unwrap().admission, Some(None));
}

#[test]
fn concurrent_retries_of_one_continuation_agree_on_its_admission() {
    loom::model(|| race(false));
}

#[test]
#[should_panic(expected = "a concurrent retry rejected the winning admission")]
fn negative_control_routes_outside_the_gate() {
    loom::model(|| race(true));
}

#[test]
fn routing_continues_settled_parents_and_parents_live_ones() {
    let parent = TaskId::from_bytes([1; 16]);
    let other = TaskId::from_bytes([2; 16]);
    assert_eq!(
        route(parent, false, false, true, None).unwrap(),
        Route::Continue { history: true }
    );
    assert_eq!(
        route(parent, false, true, true, None).unwrap(),
        Route::Continue { history: false }
    );
    assert_eq!(
        route(parent, true, false, false, Some(None)).unwrap(),
        Route::Continue { history: false }
    );
    assert_eq!(
        route(parent, false, false, false, None).unwrap(),
        Route::Child
    );
    assert_eq!(
        route(parent, false, false, true, Some(Some(parent))).unwrap(),
        Route::Child
    );
    assert!(route(parent, false, true, true, Some(Some(other))).is_err());
}
