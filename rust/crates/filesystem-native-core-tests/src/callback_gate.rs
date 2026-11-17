//! Shared callback-drain epochs against event-driven native concurrency cases.
#![cfg(not(loom))]

#[path = "../../filesystem/src/native_mount/shared/callback_gate.rs"]
mod callback_gate;

use callback_gate::CallbackGate;
use std::sync::Arc;

/// A drain waits for the callback admitted before it, and finishes
/// while a later callback remains active.
/// Negative control: with no earlier callback it returns at once,
/// without running `before_waiting`.
#[test]
fn a_drain_finishes_while_a_later_callback_remains_active() {
    let gate = Arc::new(CallbackGate::new());
    assert_eq!(gate.drain(|| Err::<(), _>("waited")), Ok(()));
    let earlier = gate.enter().expect("open");
    let (entered, waiting) = std::sync::mpsc::channel();
    let (sent, finished) = std::sync::mpsc::channel();
    let barrier = std::thread::spawn({
        let gate = Arc::clone(&gate);
        move || {
            let waited = gate.drain(|| entered.send(()).map_err(|_| "settle receiver disappeared"));
            let _ = sent.send(waited);
        }
    });
    waiting.recv().expect("barrier entered its new epoch");
    assert!(
        finished.try_recv().is_err(),
        "the earlier callback is still running"
    );
    // This pass is admitted after the barrier's explicit settle event.
    // Holding it through completion rejects a single-counter drain.
    let later = gate.enter().expect("open");
    drop(earlier);
    let waited = finished.recv();
    assert_eq!(waited, Ok(Ok(())), "a drain starved behind later callbacks");
    drop(later);
    barrier.join().expect("barrier");
}

/// A failed wait admission releases the drain's epoch.
#[test]
fn failed_wait_admission_does_not_hold_a_callback_epoch() {
    let gate = CallbackGate::new();
    let pass = gate.enter().expect("open");
    assert_eq!(gate.drain(|| Err("over budget")), Err("over budget"));
    drop(pass);
    assert_eq!(gate.drain(|| Err("over budget")), Ok(()));
}
