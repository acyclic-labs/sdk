//! Drives a consumer away from the real snapshot job it dispatched, under the
//! contract-core Loom executor.
use crate::snapshot::model::{Worker, capture, take_pending};

pub(crate) fn take() -> Worker {
    let mut workers = take_pending();
    assert_eq!(workers.len(), 1);
    workers.pop().unwrap()
}

/// How a consumer leaves the real job it dispatched.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Departure {
    /// The consumer receives the read; the queued job is handed the reservation.
    Keep,
    /// Dropping the consumer races the job's first poll under the writer.
    CancelRacesFirstPoll,
    /// After the job queued, dropping the consumer races releasing the writer
    /// and so the handoff of a reservation to the job.
    CancelRacesRelease,
}

struct Flag(loom::sync::atomic::AtomicBool);
impl std::task::Wake for Flag {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, loom::sync::atomic::Ordering::SeqCst);
    }
}
impl Flag {
    fn take(&self) -> bool {
        self.0.swap(false, loom::sync::atomic::Ordering::SeqCst)
    }
}

/// Drives one consumer of a provider stream while `held` excludes its read.
/// `available` observes whether a writer could take the lock right now.
pub(crate) fn depart<H>(
    mut follow: futures::stream::BoxStream<'static, Result<(), crate::StreamError>>,
    held: H,
    available: impl Fn() -> bool,
    departure: Departure,
) {
    use futures::StreamExt as _;
    use std::task::{Context, Waker};
    capture(false);
    assert!(
        follow
            .as_mut()
            .poll_next(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let mut worker = take();
    let woken = std::sync::Arc::new(Flag(loom::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    match departure {
        Departure::Keep => {
            assert!(worker.as_mut().poll(&mut cx).is_pending());
            drop(held);
            assert!(
                !available(),
                "the queued job holds the handed-off reservation"
            );
            assert!(worker.as_mut().poll(&mut cx).is_ready());
            woken.take();
            assert!(matches!(
                crate::snapshot::park(Box::pin(follow.next())),
                Some(Ok(()))
            ));
            drop(follow);
            assert!(!woken.take(), "a completed job was woken for cancellation");
            assert!(available());
        }
        Departure::CancelRacesFirstPoll => {
            let consumer = loom::thread::spawn(move || drop(follow));
            let first = worker.as_mut().poll(&mut cx);
            consumer.join().unwrap();
            if first.is_pending() {
                assert!(woken.take(), "cancellation did not wake the queued job");
                assert!(
                    worker.as_mut().poll(&mut cx).is_ready(),
                    "an abandoned job kept its queued reservation"
                );
            }
            drop(held);
            assert!(available(), "an abandoned job took the reservation");
        }
        Departure::CancelRacesRelease => {
            assert!(worker.as_mut().poll(&mut cx).is_pending());
            let consumer = loom::thread::spawn(move || drop(follow));
            drop(held);
            consumer.join().unwrap();
            assert!(
                woken.take(),
                "neither handoff nor cancellation woke the job"
            );
            assert!(
                worker.as_mut().poll(&mut cx).is_ready(),
                "an abandoned job kept its reservation"
            );
            assert!(available(), "the handed-off reservation was not returned");
        }
    }
    drop(worker);
}
