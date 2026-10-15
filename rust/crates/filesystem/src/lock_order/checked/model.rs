use super::*;
use loom::sync::{Arc, Condvar, Mutex};
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static MODEL_HELD: RefCell<Vec<(u16, &'static Location<'static>)>> = RefCell::new(Vec::new());
}

fn with_model_held(edit: &mut dyn FnMut(&mut Vec<(u16, &'static Location<'static>)>)) {
    MODEL_HELD.with(|held| edit(&mut held.borrow_mut()));
}

fn healthy<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())))
}

#[test]
fn ordered_acquisition_and_out_of_order_drop() {
    loom::model(|| {
        let first = Arc::new(Mutex::new(0));
        let second = Arc::new(Mutex::new(0));
        let threads = (0..2)
            .map(|_| {
                let first = Arc::clone(&first);
                let second = Arc::clone(&second);
                loom::thread::spawn(move || {
                    let mut first_guard = healthy(acquire(10, || first.lock(), with_model_held));
                    let mut second_guard = healthy(acquire(20, || second.lock(), with_model_held));
                    *first_guard += 1;
                    *second_guard += 1;
                    drop(first_guard);
                    // The remaining high rank still forbids reacquiring the low lock.
                    let attempted = std::cell::Cell::new(false);
                    assert!(
                        catch_unwind(AssertUnwindSafe(|| acquire(
                            10,
                            || {
                                attempted.set(true);
                                first.lock()
                            },
                            with_model_held
                        )))
                        .is_err()
                    );
                    assert!(!attempted.get());
                    drop(second_guard);
                    drop(healthy(acquire(10, || first.lock(), with_model_held)));
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            assert!(thread.join().is_ok());
        }
        assert_eq!(*healthy(first.lock()), 2);
        assert_eq!(*healthy(second.lock()), 2);
    });
}

#[test]
fn condition_wait_removes_and_restores_rank_without_lost_wakeup() {
    loom::model(|| {
        let shared = Arc::new((Mutex::new(false), Condvar::new()));
        let other = Arc::clone(&shared);
        let waiter = loom::thread::spawn(move || {
            let mut guard = healthy(acquire(20, || other.0.lock(), with_model_held));
            while !*guard {
                guard = healthy(wait_plain(guard, |raw| other.1.wait(raw)));
            }
            // Rank must be restored before returning a woken guard to its caller.
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    acquire(10, || other.0.lock(), with_model_held)
                }))
                .is_err()
            );
            drop(guard);
            drop(healthy(acquire(10, || other.0.lock(), with_model_held)));
        });
        let mut guard = healthy(acquire(20, || shared.0.lock(), with_model_held));
        *guard = true;
        shared.1.notify_all();
        drop(guard);
        assert!(waiter.join().is_ok());
    });
}

#[test]
fn both_while_predicates_keep_the_held_rank() {
    for timed in [false, true] {
        loom::model(move || {
            let high = Arc::new((Mutex::new(false), Condvar::new()));
            let low = Arc::new(Mutex::new(()));
            let other_high = Arc::clone(&high);
            let other_low = Arc::clone(&low);
            let waiter = loom::thread::spawn(move || {
                let guard = healthy(acquire(20, || other_high.0.lock(), with_model_held));
                let predicate = |ready: &mut bool| {
                    let attempted = std::cell::Cell::new(false);
                    assert!(
                        catch_unwind(AssertUnwindSafe(|| acquire(
                            10,
                            || {
                                attempted.set(true);
                                other_low.lock()
                            },
                            with_model_held
                        )))
                        .is_err()
                    );
                    assert!(!attempted.get());
                    !*ready
                };
                let wait = |guard| wait_plain(guard, |raw| other_high.1.wait(raw));
                if timed {
                    let (_, timeout) =
                        healthy(while_timed(guard, Duration::MAX, predicate, |guard, _| {
                            wait(guard)
                        }));
                    assert!(!timeout.timed_out());
                } else {
                    drop(healthy(while_locked(guard, predicate, wait)));
                }
            });
            let _low = healthy(acquire(10, || low.lock(), with_model_held));
            let mut guard = healthy(acquire(20, || high.0.lock(), with_model_held));
            *guard = true;
            high.1.notify_all();
            drop(guard);
            assert!(waiter.join().is_ok());
        });
    }
}

#[test]
fn timed_predicate_and_timeout_return_keep_the_held_rank() {
    loom::model(|| {
        let high = Mutex::new(());
        let low = Mutex::new(());
        let guard = healthy(acquire(20, || high.lock(), with_model_held));
        let predicate = |_: &mut ()| {
            assert!(
                catch_unwind(AssertUnwindSafe(|| acquire(
                    10,
                    || low.lock(),
                    with_model_held
                )))
                .is_err()
            );
            true
        };
        let (guard, timeout) = healthy(while_timed(guard, Duration::ZERO, predicate, |_, _| {
            unreachable!("an elapsed timeout never releases the lock")
        }));
        assert!(timeout.timed_out());
        assert!(
            catch_unwind(AssertUnwindSafe(|| acquire(
                10,
                || low.lock(),
                with_model_held
            )))
            .is_err()
        );
        drop(guard);
        drop(healthy(acquire(10, || low.lock(), with_model_held)));
    });
}
