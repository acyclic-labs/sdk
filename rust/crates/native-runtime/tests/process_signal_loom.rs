//! Exercises the production terminal signal with instrumented locks.
#![cfg(loom)]
#[path = "../src/process_tree/events/outcome.rs"]
mod outcome;
use outcome::ProcessWaitOutcome;
#[path = "../src/process_tree/events/signal.rs"]
mod signal;
#[path = "../src/wake.rs"]
mod wake;

use loom::sync::Arc;
use signal::Signal;
use std::task::{Context, Poll, Waker};

#[test]
fn settlement_cannot_pass_waker_registration() {
    loom::model(|| {
        let signal = Arc::new(Signal::default());
        let producer = Arc::clone(&signal);
        let thread = loom::thread::spawn(move || producer.settle(ProcessWaitOutcome::Ready));
        let wake = std::sync::Arc::new(wake::NativeWake::default());
        let waker = Waker::from(std::sync::Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        if signal.poll(&mut context).is_pending() {
            wake.wait(None);
        }
        assert!(matches!(
            signal.poll(&mut context),
            Poll::Ready(ProcessWaitOutcome::Ready)
        ));
        thread.join().unwrap();
        signal.settle(ProcessWaitOutcome::Cancelled);
        assert!(matches!(
            signal.poll(&mut context),
            Poll::Ready(ProcessWaitOutcome::Ready)
        ));
        let deadline = Signal::default();
        deadline.settle(ProcessWaitOutcome::Deadline);
        assert!(matches!(
            deadline.poll(&mut context),
            Poll::Ready(ProcessWaitOutcome::Deadline)
        ));
    });
}

#[test]
fn failure_and_cancellation_settle_once_at_every_interleaving() {
    loom::model(|| {
        let signal = Arc::new(Signal::default());
        let cancelled = Arc::clone(&signal);
        let failed = Arc::clone(&signal);
        let cancel = loom::thread::spawn(move || cancelled.settle(ProcessWaitOutcome::Cancelled));
        let fail = loom::thread::spawn(move || {
            failed.settle(ProcessWaitOutcome::ServiceFailed(std::sync::Arc::new(
                std::io::Error::other("producer dropped"),
            )))
        });
        let wake = std::sync::Arc::new(wake::NativeWake::default());
        let waker = Waker::from(std::sync::Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        if signal.poll(&mut context).is_pending() {
            wake.wait(None);
        }
        let Poll::Ready(first) = signal.poll(&mut context) else {
            panic!("lost terminal event")
        };
        cancel.join().unwrap();
        fail.join().unwrap();
        let Poll::Ready(second) = signal.poll(&mut context) else {
            panic!("lost retained event")
        };
        assert_eq!(
            std::mem::discriminant(&first),
            std::mem::discriminant(&second)
        );
        // Both failure variants map to an error at the public boundary.
        assert!(second.into_result().is_err());
    });
}

#[test]
#[should_panic(expected = "lost terminal wake")]
fn separated_pending_check_and_registration_fails_the_oracle() {
    loom::model(|| {
        use loom::sync::atomic::{AtomicBool, Ordering};
        let done = Arc::new(loom::sync::Mutex::new(false));
        let registered = Arc::new(AtomicBool::new(false));
        let woken = Arc::new(AtomicBool::new(false));
        let producer = (
            Arc::clone(&done),
            Arc::clone(&registered),
            Arc::clone(&woken),
        );
        let thread = loom::thread::spawn(move || {
            *producer.0.lock().unwrap() = true;
            if producer.1.load(Ordering::SeqCst) {
                producer.2.store(true, Ordering::SeqCst);
            }
        });
        let pending = !*done.lock().unwrap();
        registered.store(true, Ordering::SeqCst);
        thread.join().unwrap();
        assert!(
            !pending || woken.load(Ordering::SeqCst),
            "lost terminal wake"
        );
    });
}
