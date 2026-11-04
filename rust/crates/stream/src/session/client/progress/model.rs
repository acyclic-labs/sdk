use super::super::Arc;
use super::*;

/// Race transport handoff against cursor destruction on the production atomic.
/// Cancellation must retain evidence of an already handed-off Hello, and a
/// retry reset must never resurrect a withdrawn interest.
#[cfg(test)]
fn verify_opening_retirement(broken: bool) {
    loom::model(move || {
        let progress = Arc::new(Progress::new(0));
        let sending = Arc::clone(&progress);
        let sender = loom::thread::spawn(move || sending.admit_opening());
        if broken {
            progress.ownership.store(CANCELLED, Ordering::Release);
        } else {
            progress.cancel();
        }
        let admitted = sender.join().unwrap();
        assert!(
            progress.cancelled() && progress.opening_admitted() == admitted,
            "withdrawal must preserve cancellation and the original Hello handoff"
        );
        progress.reset_opening();
        assert!(progress.cancelled());
        assert!(!progress.admit_opening());
        assert!(!progress.opening_admitted());
    });
}

#[test]
fn opening_retirement_preserves_the_transport_handoff() {
    verify_opening_retirement(false);
}

#[test]
#[should_panic(expected = "withdrawal must preserve cancellation and the original Hello handoff")]
fn opening_retirement_negative_control_erases_the_transport_handoff() {
    verify_opening_retirement(true);
}

/// One application thread races one placement fence over the production lock.
/// No Tokio synchronization or transport state is substituted for this cut.
pub fn verify_latest_progress(broken: bool) {
    loom::model(move || {
        let progress = Arc::new(Progress::new(1));
        let applying = Arc::clone(&progress);
        let consumer = loom::thread::spawn(move || {
            if broken {
                // Mutant reads generation outside the lock that advances the cut.
                let old = applying.state.lock().unwrap().generation;
                loom::thread::yield_now();
                if old == 1 {
                    applying.state.lock().unwrap().position.next = 8;
                    true
                } else {
                    false
                }
            } else {
                applying.consume_latest(1, 7).unwrap()
            }
        });
        // The cursor captured at fencing must remain the resume cut if generation
        // 1 is rejected. If consumed first, the fence must see the complete cut.
        assert_eq!(progress.fence().unwrap(), 2);
        let resume = progress.next().unwrap();
        let applied = consumer.join().unwrap();
        let actual = progress.next().unwrap();
        assert_eq!(
            actual, resume,
            "a stale delivery cannot advance the fenced resume cut"
        );
        assert_eq!(actual, if applied { 8 } else { 1 });
    });
}
