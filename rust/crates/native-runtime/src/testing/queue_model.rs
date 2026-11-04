//! The real admission, owned job, terminal poll and observer-drop paths.
//! `tests/signal-control.py` plants capacity, terminal and funding defects in
//! those production operations; the assertions below stay unchanged.
use super::*;
use crate::{AdmissionState, NativeWorkers, TaskTerminal, run_blocking_io, wake_capacity_waiter};
use loom::{sync::atomic::AtomicUsize, thread};
use std::{pin::Pin, sync::atomic::Ordering, task::Wake};

struct Woken(AtomicUsize);

const PANIC: &str = "original native task panic control";
type TaskOutcome = Result<Poll<std::io::Result<i32>>, Box<dyn std::any::Any + Send>>;

#[derive(Clone, Copy, Debug)]
enum Disposition {
    Returned,
    Failed,
    Panicked,
}

#[derive(Debug)]
struct TaskFailure;

impl std::fmt::Display for TaskFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original worker refusal")
    }
}

impl std::error::Error for TaskFailure {}

fn task_result(disposition: Disposition) -> std::io::Result<i32> {
    match disposition {
        Disposition::Returned => Ok(17),
        Disposition::Failed => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            TaskFailure,
        )),
        Disposition::Panicked => std::panic::panic_any(PANIC),
    }
}

fn poll_task(
    task: &mut crate::NativeCompletion<i32>,
    cx: &mut std::task::Context<'_>,
) -> TaskOutcome {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Pin::new(task).poll(cx)))
}

fn assert_outcome(outcome: TaskOutcome, disposition: Disposition) {
    match disposition {
        Disposition::Returned => assert!(matches!(outcome, Ok(Poll::Ready(Ok(17))))),
        Disposition::Failed => match outcome {
            Ok(Poll::Ready(Err(error))) => {
                assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
                assert!(
                    error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<TaskFailure>()),
                    "original worker failure cause replaced"
                );
            }
            _ => panic!("original worker failure missing"),
        },
        Disposition::Panicked => {
            assert_eq!(outcome.unwrap_err().downcast_ref::<&str>(), Some(&PANIC));
        }
    }
}

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn capacity() {
    // Outside-model counters audit the explored schedule set; they never
    // participate in queue synchronization or determine a modeled outcome.
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let orders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    let observed = orders.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = NativeWorkers::single(sender, AdmissionState::new());
        let mut filler = run_blocking_io(|| (), None).unwrap();
        assert!(
            workers
                .sender
                .try_send(filler.pending.take().unwrap())
                .is_ok()
        );
        workers.admission.lock().unwrap().queued_jobs = 1;
        let released = workers.admission.clone();
        let receiving = receiver.clone();
        let worker = thread::spawn(move || {
            let filler = crate::workers::receive_job(&receiving, &released).unwrap();
            wake_capacity_waiter(filler.run());
        });

        let effects = Arc::new(AtomicUsize::new(0));
        let executed = effects.clone();
        let mut task = run_blocking_io(
            move || {
                executed.fetch_add(1, Ordering::SeqCst);
                17
            },
            None,
        )
        .unwrap();
        let flag = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(flag.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut completion = Box::pin(queued(task.pending.take().unwrap(), &workers));
        assert!(completion.as_mut().poll(&mut cx).is_pending());
        worker.join().unwrap();
        let waiting = {
            let admission = workers.admission.lock().unwrap();
            // The target is either in the queue or waiting for its empty slot.
            assert_eq!(
                admission.queued_jobs,
                usize::from(admission.waiter_count == 0),
                "queue accounting lost"
            );
            admission.waiter_count != 0
        };
        observed.fetch_or(1 << usize::from(waiting), Ordering::Relaxed);
        if waiting {
            assert_ne!(flag.0.load(Ordering::SeqCst), 0, "capacity wake lost");
        }
        assert!(completion.as_mut().poll(&mut cx).is_pending());
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(workers.admission.lock().unwrap().waiter_count, 0);
        let accepted = crate::workers::receive_job(&receiver, &workers.admission).unwrap();
        wake_capacity_waiter(accepted.run());
        assert_ne!(flag.0.load(Ordering::SeqCst), 0, "terminal wake lost");
        assert!(matches!(
            completion.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(workers.admission.lock().unwrap().queued_jobs, 0);
        assert_eq!(workers.admission.lock().unwrap().waiter_count, 0);
        drop(completion);
        assert!(matches!(
            Pin::new(&mut task).poll(&mut cx),
            Poll::Ready(Ok(17))
        ));
    });
    assert_eq!(
        orders.load(Ordering::Relaxed),
        3,
        "a capacity order was not explored"
    );
    eprintln!("capacity schedules: {}", schedules.load(Ordering::Relaxed));
}

fn terminal(drop_observer: bool, disposition: Disposition) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let orders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    let observed = orders.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let effects = Arc::new(AtomicUsize::new(0));
        let executed = effects.clone();
        let mut task = crate::blocking::task_completion(
            move || {
                executed.fetch_add(1, Ordering::SeqCst);
                task_result(disposition)
            },
            None,
            None,
            None,
            &crate::BLOCKING_WORKERS,
        )
        .unwrap();
        let state = task.state.clone().unwrap();
        let job = task.pending.take().unwrap();
        let worker = thread::spawn(move || wake_capacity_waiter(job.run()));
        let flag = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(flag.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let pending = state.test_poll_completion(&waker).is_pending();
        let mut observer = Some(task);
        if drop_observer {
            drop(observer.take());
        }
        observed.fetch_or(
            1 << usize::from(if drop_observer {
                state.lock().unwrap().task_pending
            } else {
                pending
            }),
            Ordering::Relaxed,
        );
        worker.join().unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(
            !state.lock().unwrap().task_pending,
            "terminal accounting lost"
        );
        assert!(state.test_poll_completion(&waker).is_ready());
        if pending && !drop_observer {
            assert_ne!(flag.0.load(Ordering::SeqCst), 0, "terminal wake lost");
        }
        let outcome = match observer.as_mut() {
            Some(task) => poll_task(task, &mut cx),
            None => {
                // The independently owned worker still publishes after the
                // sole caller drops; inspect its original result or panic.
                let mut state = state.lock().unwrap();
                if let Some(payload) = state.panic.take() {
                    assert!(state.result.is_none());
                    Err(payload)
                } else {
                    Ok(Poll::Ready(
                        state.result.take().expect("original result retained"),
                    ))
                }
            }
        };
        assert_outcome(outcome, disposition);
    });
    assert_eq!(
        orders.load(Ordering::Relaxed),
        3,
        "a completion order was not explored"
    );
    eprintln!(
        "terminal schedules (drop={drop_observer}, disposition={disposition:?}): {}",
        schedules.load(Ordering::Relaxed)
    );
}

#[test]
fn capacity_release_races_registration_on_the_real_queue() {
    capacity();
}

#[test]
// The source control --native-terminal stringifies task_completion's actual
// retained error. This same oracle must reject the lost typed cause; the model
// does not manufacture a replacement result or prove OS producer retirement.
fn physical_completion_races_observer_drop_on_the_real_terminal() {
    for disposition in [
        Disposition::Returned,
        Disposition::Failed,
        Disposition::Panicked,
    ] {
        terminal(false, disposition);
        terminal(true, disposition);
    }
}

/// Result observation and abandonment race the worker's final shared release.
/// Every remaining state owner must retain the original physical funding.
fn charge(drop_observer: bool, disposition: Disposition) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let early = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    let raced = early.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let meter = Arc::new(crate::tests::ReadBudgetMeter::new(1, 0));
        let observed: Arc<dyn crate::NativeReadMeter> = meter.clone();
        let mut task = crate::blocking::task_completion(
            move || task_result(disposition),
            Some(observed),
            None,
            None,
            &crate::BLOCKING_WORKERS,
        )
        .unwrap();
        let state = task.state.as_ref().unwrap().clone();
        let funded = {
            let state = state.lock().unwrap();
            state._allocation.as_ref().unwrap().bytes()
        };
        assert_ne!(meter.memory.load(Ordering::SeqCst), 0);
        let job = task.pending.take().unwrap();
        let worker = thread::spawn(move || wake_capacity_waiter(job.run()));
        if drop_observer {
            drop(task);
            worker.join().unwrap();
            assert_eq!(
                meter.memory.load(Ordering::SeqCst),
                funded,
                "abandoned shared state lost its funding"
            );
            drop(state);
            assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
            return;
        }
        let flag = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(flag);
        let mut cx = std::task::Context::from_waker(&waker);
        let mut worker = Some(worker);
        let outcome = match poll_task(&mut task, &mut cx) {
            Ok(Poll::Pending) => {
                worker.take().unwrap().join().unwrap();
                poll_task(&mut task, &mut cx)
            }
            ready => {
                raced.fetch_add(1, Ordering::Relaxed);
                ready
            }
        };
        drop(task);
        let live_funding = meter.memory.load(Ordering::SeqCst);
        if let Some(worker) = worker {
            worker.join().unwrap();
        }
        let joined_funding = meter.memory.load(Ordering::SeqCst);
        drop(state);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        // Capture the raced observation first, but retire the modeled worker
        // before panicking: its destructor must run inside this Loom model.
        assert!(live_funding >= funded, "live shared state lost its funding");
        assert_eq!(joined_funding, funded);
        assert_outcome(outcome, disposition);
    });
    if !drop_observer {
        assert_ne!(
            early.load(Ordering::Relaxed),
            0,
            "no early observation explored"
        );
    }
    eprintln!(
        "charge schedules (drop={drop_observer}, disposition={disposition:?}): {}",
        schedules.load(Ordering::Relaxed)
    );
}

#[test]
fn shared_state_funding_survives_observation_and_worker_release() {
    for disposition in [
        Disposition::Returned,
        Disposition::Failed,
        Disposition::Panicked,
    ] {
        charge(false, disposition);
        charge(true, disposition);
    }
}

/// A metered submission waits for capacity while the worker's release
/// notifier holds its waiter node. Admission and cancellation unlink the node
/// without refunding memory still owned by that notifier.
fn waiter_charge(cancel: bool) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = NativeWorkers::single(sender, AdmissionState::new());
        let mut filler = run_blocking_io(|| (), None).unwrap();
        assert!(
            workers
                .sender
                .try_send(filler.pending.take().unwrap())
                .is_ok()
        );
        workers.admission.lock().unwrap().queued_jobs = 1;

        let meter = Arc::new(crate::tests::ReadBudgetMeter::new(1, 0));
        let observed: Arc<dyn crate::NativeReadMeter> = meter.clone();
        let mut task = run_blocking_io(|| 17, Some((observed, false))).unwrap();
        let mut pending = task.pending.take();
        let mut waiter = None;
        let flag = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(flag);
        let cx = std::task::Context::from_waker(&waker);
        assert!(
            crate::workers::poll_submission_with(&workers, &mut pending, &mut waiter, &cx)
                .is_pending()
        );
        let retained = waiter.as_ref().unwrap().clone();
        let funded = retained
            .state
            .lock()
            .unwrap()
            ._allocation
            .as_ref()
            .unwrap()
            .bytes();
        let registered = meter.memory.load(Ordering::SeqCst);
        assert!(registered >= funded && funded > 0, "waiter uncharged");

        let released = workers.admission.clone();
        let receiving = receiver.clone();
        let notifier = thread::spawn(move || {
            let filler = crate::workers::receive_job(&receiving, &released).unwrap();
            wake_capacity_waiter(filler.run());
        });
        let mut notifier = Some(notifier);
        let submission = if cancel {
            crate::remove_capacity_waiter(&workers, waiter.take());
            None
        } else {
            let mut submission =
                crate::workers::poll_submission_with(&workers, &mut pending, &mut waiter, &cx);
            if submission.is_pending() {
                notifier.take().unwrap().join().unwrap();
                submission =
                    crate::workers::poll_submission_with(&workers, &mut pending, &mut waiter, &cx);
            }
            Some(submission)
        };
        // Ready admission and cancellation can precede the notifier's release.
        // Pending admission waits for that original producer's terminal, while
        // retained still owns the funded waiter in either order. Defer a failing
        // assertion until the producer has retired inside the model.
        let retained_funding = meter.memory.load(Ordering::SeqCst);
        if let Some(notifier) = notifier {
            notifier.join().unwrap();
        }
        if let Some(submission) = submission {
            assert!(matches!(submission, Poll::Ready(Ok(()))));
            assert!(pending.is_none() && waiter.is_none());
        }
        drop(retained);
        assert_eq!(meter.memory.load(Ordering::SeqCst), registered - funded);
        let admission = workers.admission.lock().unwrap();
        assert_eq!(admission.waiter_count, 0);
        assert_eq!(admission.queued_jobs, usize::from(!cancel));
        drop(admission);
        if !cancel {
            let accepted = crate::workers::receive_job(&receiver, &workers.admission).unwrap();
            wake_capacity_waiter(accepted.run());
        }
        drop(pending);
        drop(task);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        assert_eq!(
            retained_funding, registered,
            "live shared waiter lost its funding"
        );
    });
    eprintln!(
        "waiter charge schedules (cancel={cancel}): {}",
        schedules.load(Ordering::Relaxed)
    );
}

#[test]
fn shared_waiter_funding_survives_admission_and_cancellation() {
    waiter_charge(false);
    waiter_charge(true);
}

fn release_broadcast() {
    // Prove broadcast without cancellation's independent successor wake too.
    // Otherwise a negative control can accidentally borrow that second path.
    for cancel in [None, Some(0), Some(1), Some(2)] {
        loom::model(move || {
            let (sender, _receiver) = crate::sync::mpsc::sync_channel(1);
            let workers = NativeWorkers::single(sender, AdmissionState::new());
            let flags = std::array::from_fn::<_, 3, _>(|_| Arc::new(Woken(AtomicUsize::new(0))));
            let nodes = flags.each_ref().map(|flag| {
                workers
                    .admission
                    .lock()
                    .unwrap()
                    .register(&Waker::from(flag.clone()), None)
            });
            workers.admission.lock().unwrap().queued_jobs = 1;
            let admission = workers.admission.clone();
            let notifier = thread::spawn(move || crate::notify_capacity_released(&admission));
            if let Some(cancel) = cancel {
                crate::remove_capacity_waiter(&workers, Some(nodes[cancel].clone()));
            }
            notifier.join().unwrap();
            // Observe the entire broadcast before cleanup: unlinking an
            // earlier node wakes its successor and would mask a lost wake.
            for (index, flag) in flags.iter().enumerate() {
                if Some(index) != cancel {
                    assert_ne!(
                        flag.0.load(Ordering::SeqCst),
                        0,
                        "capacity broadcast lost a live waiter"
                    );
                }
            }
            for (index, node) in nodes.into_iter().enumerate() {
                if Some(index) != cancel {
                    crate::remove_capacity_waiter(&workers, Some(node));
                }
            }
            assert_eq!(workers.admission.lock().unwrap().waiter_count, 0);
        });
    }
}

#[test]
fn release_broadcast_reaches_every_waiter_across_concurrent_cancellation() {
    release_broadcast();
}
