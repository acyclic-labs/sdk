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
