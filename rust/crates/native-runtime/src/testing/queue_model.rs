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
        let workers = NativeWorkers {
            sender,
            admission: Arc::new(Mutex::new(admission)),
        };
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

/// The worker publishes and then drops its reference to the shared state; the
/// observer can take the result in between. The state's charge must already
/// be retired when the observer returns, so a caller that sees the result
/// sees its whole budget back.
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
        task.state.as_ref().unwrap().lock().unwrap().broken_charge = broken;
        assert_ne!(meter.memory.load(Ordering::SeqCst), 0);
        let job = task.pending.take().unwrap();
        let worker = thread::spawn(move || wake_capacity_waiter(job.run()));
        if drop_observer {
            // Abandoned: the charge stays with the state, and the worker's
            // last reference retires it.
            drop(task);
            worker.join().unwrap();
            assert_eq!(
                meter.memory.load(Ordering::SeqCst),
                0,
                "abandoned charge leaked"
            );
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
        assert_eq!(
            meter.memory.load(Ordering::SeqCst),
            0,
            "charge outlived its observer"
        );
        if let Some(worker) = worker {
            worker.join().unwrap();
        }
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
fn observer_retires_the_state_charge_before_the_worker_lets_go() {
    charge(false, false);
    charge(true, false);
}

#[test]
#[should_panic(expected = "charge outlived its observer")]
fn a_charge_left_on_the_shared_state_is_detected() {
    charge(false, true);
}
