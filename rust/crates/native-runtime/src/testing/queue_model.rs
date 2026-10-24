//! The real admission, owned job, terminal poll and observer-drop paths.
use super::*;
use crate::{AdmissionState, NativeWorkers, TaskTerminal, run_blocking_io, wake_capacity_waiter};
use loom::{sync::atomic::AtomicUsize, thread};
use std::{pin::Pin, sync::atomic::Ordering, task::Wake};

#[derive(Clone, Copy)]
enum Fault {
    None,
    Wake,
    Accounting,
}

struct Woken(AtomicUsize);
impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn capacity(fault: Fault) {
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
        let mut admission = AdmissionState::new();
        admission.broken_wake = matches!(fault, Fault::Wake);
        admission.broken_accounting = matches!(fault, Fault::Accounting);
        let workers = NativeWorkers::single(sender, admission);
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

fn terminal(drop_observer: bool, fault: Fault) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let orders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    let observed = orders.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
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
        let state = task.state.clone().unwrap();
        {
            let mut state = state.lock().unwrap();
            state.broken_wake = matches!(fault, Fault::Wake);
            state.broken_accounting = matches!(fault, Fault::Accounting);
        }
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
        match observer.as_mut() {
            Some(task) => {
                assert!(matches!(Pin::new(task).poll(&mut cx), Poll::Ready(Ok(17))));
            }
            None => {
                // The independently owned worker still publishes after the
                // sole caller drops; dropping a waker must not lose its result.
                assert!(matches!(state.lock().unwrap().result.take(), Some(Ok(17))));
            }
        }
    });
    assert_eq!(
        orders.load(Ordering::Relaxed),
        3,
        "a completion order was not explored"
    );
    eprintln!(
        "terminal schedules (drop={drop_observer}): {}",
        schedules.load(Ordering::Relaxed)
    );
}

#[test]
fn capacity_release_races_registration_on_the_real_queue() {
    capacity(Fault::None);
}

#[test]
fn physical_completion_races_observer_drop_on_the_real_terminal() {
    terminal(false, Fault::None);
    terminal(true, Fault::None);
}

#[test]
#[should_panic(expected = "capacity wake lost")]
fn broken_capacity_wake_is_detected() {
    capacity(Fault::Wake);
}

#[test]
#[should_panic(expected = "queue accounting lost")]
fn broken_capacity_accounting_is_detected() {
    capacity(Fault::Accounting);
}

#[test]
#[should_panic(expected = "terminal wake lost")]
fn broken_terminal_wake_is_detected() {
    terminal(false, Fault::Wake);
}

#[test]
#[should_panic(expected = "terminal accounting lost")]
fn broken_terminal_accounting_is_detected() {
    terminal(true, Fault::Accounting);
}

/// Result observation and abandonment race the worker's final shared release.
/// Every remaining state owner must retain the original physical funding.
fn charge(drop_observer: bool, broken: bool) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let early = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    let raced = early.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let meter = Arc::new(crate::tests::ReadBudgetMeter::new(1, 0));
        let observed: Arc<dyn crate::NativeReadMeter> = meter.clone();
        let mut task = run_blocking_io(|| 17, Some((observed, false))).unwrap();
        let state = task.state.as_ref().unwrap().clone();
        let funded = {
            let mut state = state.lock().unwrap();
            state.broken_charge = broken;
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
        match Pin::new(&mut task).poll(&mut cx) {
            Poll::Ready(result) => {
                raced.fetch_add(1, Ordering::Relaxed);
                assert!(matches!(result, Ok(17)));
            }
            Poll::Pending => {
                worker.take().unwrap().join().unwrap();
                assert!(matches!(
                    Pin::new(&mut task).poll(&mut cx),
                    Poll::Ready(Ok(17))
                ));
            }
        }
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
    });
    if !drop_observer {
        assert_ne!(
            early.load(Ordering::Relaxed),
            0,
            "no early observation explored"
        );
    }
    eprintln!(
        "charge schedules (drop={drop_observer}): {}",
        schedules.load(Ordering::Relaxed)
    );
}

#[test]
fn shared_state_funding_survives_observation_and_worker_release() {
    charge(false, false);
    charge(true, false);
}

#[test]
#[should_panic(expected = "live shared state lost its funding")]
fn premature_shared_state_refund_is_detected() {
    charge(false, true);
}

/// A metered submission waits for capacity while the worker's release
/// notifier holds its waiter node. Admission and cancellation unlink the node
/// without refunding memory still owned by that notifier.
fn waiter_charge(cancel: bool, broken: bool) {
    let schedules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let explored = schedules.clone();
    loom::model(move || {
        explored.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut admission = AdmissionState::new();
        admission.broken_charge = broken;
        let workers = NativeWorkers::single(sender, admission);
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
        if cancel {
            crate::remove_capacity_waiter(&workers, waiter.take());
        } else {
            while crate::workers::poll_submission_with(&workers, &mut pending, &mut waiter, &cx)
                .is_pending()
            {
                thread::yield_now();
            }
            assert!(pending.is_none() && waiter.is_none());
        }
        // Observe before joining: the notifier may still hold the node. Defer
        // a failing assertion until it has retired inside the model, so the
        // negative control cannot strand a modeled receiver during unwind.
        let retained_funding = meter.memory.load(Ordering::SeqCst);
        notifier.join().unwrap();
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
    waiter_charge(false, false);
    waiter_charge(true, false);
}

#[test]
#[should_panic(expected = "live shared waiter lost its funding")]
fn premature_admitted_waiter_refund_is_detected() {
    waiter_charge(false, true);
}

#[test]
#[should_panic(expected = "live shared waiter lost its funding")]
fn premature_cancelled_waiter_refund_is_detected() {
    waiter_charge(true, true);
}

fn release_broadcast(head_only: bool) {
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
            workers.admission.lock().unwrap().broken_broadcast = head_only;
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
    release_broadcast(false);
}

#[test]
#[should_panic(expected = "capacity broadcast lost a live waiter")]
fn head_only_capacity_wake_is_detected() {
    release_broadcast(true);
}
