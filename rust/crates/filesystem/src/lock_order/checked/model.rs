use super::*;
use loom::sync::{Arc, mpsc};
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Fault {
    None,
    MissingCheck,
    MissingRestore,
    UntrackedPredicate,
}

loom::thread_local! {
    static FAULT: Cell<Fault> = Cell::new(Fault::None);
}

pub(super) fn fault() -> Fault {
    FAULT.with(Cell::get)
}

fn set_fault(fault: Fault) {
    FAULT.with(|value| value.set(fault));
}

fn rejects_lower() -> bool {
    // An independent lock keeps the broken control from blocking on itself.
    let lower = Mutex::<_, 10>::new(());
    catch_unwind(AssertUnwindSafe(|| drop(lower.lock()))).is_err()
}

fn with_negative_control(fault: Fault, exercise: impl Fn(Fault) + std::panic::RefUnwindSafe) {
    exercise(Fault::None);
    assert!(catch_unwind(AssertUnwindSafe(|| exercise(fault))).is_err());
}

#[test]
fn ordered_acquisition_and_out_of_order_drop() {
    with_negative_control(Fault::MissingCheck, |fault| {
        loom::model(move || {
            let first = Arc::new(Mutex::<_, 10>::new(0));
            let second = Arc::new(Mutex::<_, 20>::new(0));
            let threads = (0..2)
                .map(|_| {
                    let first = Arc::clone(&first);
                    let second = Arc::clone(&second);
                    loom::thread::spawn(move || {
                        set_fault(fault);
                        let mut low = first.lock().unwrap();
                        let mut high = second.lock().unwrap();
                        *low += 1;
                        *high += 1;
                        drop(low);
                        let rejected = rejects_lower();
                        drop(high);
                        drop(first.lock().unwrap());
                        rejected
                    })
                })
                .collect::<Vec<_>>();
            let rejected = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(*first.lock().unwrap(), 2);
            assert_eq!(*second.lock().unwrap(), 2);
            assert!(rejected.into_iter().all(|rejected| rejected));
        });
    });
}

#[test]
fn condition_wait_removes_and_restores_rank_without_lost_wakeup() {
    with_negative_control(Fault::MissingRestore, |fault| {
        loom::model(move || {
            let shared = Arc::new((Mutex::<_, 20>::new(false), Condvar::new()));
            let other = Arc::clone(&shared);
            let (ready, started) = mpsc::channel();
            let waiter = loom::thread::spawn(move || {
                set_fault(fault);
                let mut guard = other.0.lock().unwrap();
                ready.send(()).unwrap();
                while !*guard {
                    guard = other.1.wait(guard).unwrap();
                }
                let rejected = rejects_lower();
                drop(guard);
                drop(other.0.lock().unwrap());
                rejected
            });
            started.recv().unwrap();
            let mut guard = shared.0.lock().unwrap();
            *guard = true;
            shared.1.notify_all();
            drop(guard);
            assert!(waiter.join().unwrap());
        });
    });
}

#[test]
fn both_while_predicates_keep_the_held_rank() {
    for timed in [false, true] {
        with_negative_control(Fault::UntrackedPredicate, |fault| {
            loom::model(move || {
                let shared = Arc::new((Mutex::<_, 20>::new(false), Condvar::new()));
                let other = Arc::clone(&shared);
                let (ready, started) = mpsc::channel();
                let waiter = loom::thread::spawn(move || {
                    set_fault(fault);
                    let guard = other.0.lock().unwrap();
                    ready.send(()).unwrap();
                    let mut rejected = true;
                    let predicate = |ready: &mut bool| {
                        rejected &= rejects_lower();
                        !*ready
                    };
                    let guard = if timed {
                        let (guard, timeout) = other
                            .1
                            .wait_timeout_while(guard, Duration::MAX, predicate)
                            .unwrap();
                        assert!(!timeout.timed_out());
                        guard
                    } else {
                        other.1.wait_while(guard, predicate).unwrap()
                    };
                    rejected &= rejects_lower();
                    drop(guard);
                    rejected
                });
                started.recv().unwrap();
                let mut guard = shared.0.lock().unwrap();
                *guard = true;
                shared.1.notify_all();
                drop(guard);
                let rejected = waiter.join().unwrap();
                // A timeout also evaluates its predicate with the mutex held.
                set_fault(fault);
                let guard = shared.0.lock().unwrap();
                let mut timeout_rejected = false;
                let (guard, timeout) = shared
                    .1
                    .wait_timeout_while(guard, Duration::ZERO, |_| {
                        timeout_rejected = rejects_lower();
                        true
                    })
                    .unwrap();
                assert!(timeout.timed_out());
                drop(guard);
                assert!(rejected && timeout_rejected);
            });
        });
    }
}
