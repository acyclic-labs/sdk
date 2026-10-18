use std::{cell::RefCell, future::Future, pin::Pin};

type Worker = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Targeted production mutants, selected per model execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mutant {
    #[default]
    None,
    /// An abandoned job keeps its queued or granted lock reservation.
    LeakReservation,
    /// Dropping the consumer does not wake a queued job.
    NoWake,
    /// Completion leaves the job registered for a cancellation wake.
    ArmedAfterCompletion,
}

loom::thread_local! {
    static MUTANT: std::cell::Cell<Mutant> = std::cell::Cell::new(Mutant::None);
}

pub(crate) fn set_mutant(mutant: Mutant) {
    MUTANT.with(|cell| cell.set(mutant));
}

pub(super) fn mutant() -> Mutant {
    MUTANT.with(std::cell::Cell::get)
}

loom::thread_local! {
    // The model owns executor task storage, just as Tokio does. Dropping these
    // futures exercises the production Job's cancellation and ownership return.
    static EXECUTOR: RefCell<Option<(bool, Vec<Worker>)>> = RefCell::new(None);
}

pub(crate) fn capture(discard_on_drop: bool) {
    EXECUTOR.with(|executor| {
        assert!(
            executor
                .borrow_mut()
                .replace((discard_on_drop, Vec::new()))
                .is_none()
        );
    });
}

pub(super) fn discard_on_drop() -> bool {
    EXECUTOR.with(|executor| executor.borrow().as_ref().is_some_and(|entry| entry.0))
}

pub(crate) fn take() -> Worker {
    EXECUTOR.with(|executor| {
        let (_, mut workers) = executor.borrow_mut().take().unwrap();
        assert_eq!(workers.len(), 1);
        workers.pop().unwrap()
    })
}

pub(super) fn spawn(worker: impl Future<Output = ()> + Send + 'static) {
    let worker = EXECUTOR.with(|executor| {
        let mut executor = executor.borrow_mut();
        if let Some((_, workers)) = executor.as_mut() {
            workers.push(Box::pin(worker));
            None
        } else {
            Some(worker)
        }
    });
    if let Some(worker) = worker {
        loom::thread::spawn(move || super::park(worker));
    }
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
            assert_eq!(super::park(follow.next()), Some(Ok(())));
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
