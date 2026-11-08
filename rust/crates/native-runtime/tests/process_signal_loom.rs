//! Exercises the production terminal signal with instrumented locks.
#![cfg(loom)]
#[path = "../src/process_tree/events/outcome.rs"]
mod outcome;
#[path = "../src/sync/locks.rs"]
pub mod sync;
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
    struct Notified(loom::sync::atomic::AtomicBool);
    impl std::task::Wake for Notified {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, loom::sync::atomic::Ordering::Release);
        }
    }
    loom::model(|| {
        let signal = Arc::new(Signal::default());
        let producer = Arc::clone(&signal);
        let thread = loom::thread::spawn(move || producer.settle(ProcessWaitOutcome::Ready));
        let wake = std::sync::Arc::new(Notified(loom::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(std::sync::Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let pending = signal.poll(&mut context).is_pending();
        thread.join().unwrap();
        assert!(
            !pending || wake.0.load(loom::sync::atomic::Ordering::Acquire),
            "original terminal passed observer registration without a wake"
        );
        assert!(
            matches!(
                signal.poll(&mut context),
                Poll::Ready(ProcessWaitOutcome::Ready)
            ),
            "first terminal event changed"
        );
        signal.settle(ProcessWaitOutcome::Cancelled);
        assert!(
            matches!(
                signal.poll(&mut context),
                Poll::Ready(ProcessWaitOutcome::Ready)
            ),
            "first terminal event changed"
        );
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
        let failure = std::sync::Arc::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "native readiness source lost",
        ));
        let cause = std::sync::Arc::clone(&failure);
        let cancel = loom::thread::spawn(move || cancelled.settle(ProcessWaitOutcome::Cancelled));
        let fail =
            loom::thread::spawn(move || failed.settle(ProcessWaitOutcome::ServiceFailed(cause)));
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
            std::mem::discriminant(&second),
            "first terminal event changed"
        );
        let returned = second.into_result().unwrap_err();
        if let ProcessWaitOutcome::ServiceFailed(first) = first {
            assert!(std::sync::Arc::ptr_eq(&first, &failure));
            assert_eq!(
                returned.kind(),
                failure.kind(),
                "service failure kind changed"
            );
            assert!(
                returned
                    .get_ref()
                    .and_then(|error| error.downcast_ref::<std::sync::Arc<std::io::Error>>())
                    .is_some_and(|cause| std::sync::Arc::ptr_eq(cause, &failure)),
                "service failure cause changed"
            );
        } else {
            assert_eq!(returned.kind(), std::io::ErrorKind::Interrupted);
        }
    });
}
