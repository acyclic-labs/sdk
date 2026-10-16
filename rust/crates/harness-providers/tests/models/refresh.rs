mod refresh {
    include!("../../src/refresh.rs");
    pub fn ticket<F>(state: &Refresh<F>) -> u64 {
        state.generation.load(Ordering::Acquire)
    }
    pub async fn joined<T, F: Copy>(
        state: &Refresh<F>,
        ticket: u64,
        operation: impl std::future::Future<Output = (T, Option<F>)>,
    ) -> Result<T, F> {
        execute(state, ticket, operation).await
    }
}
use crate::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};
fn complete<T>(operation: impl Future<Output = T>) -> T {
    let mut operation = std::pin::pin!(operation);
    match operation
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unexpected suspension outside the modeled mutex"),
    }
}

fn sharing_model(fail: bool, broken: bool) {
    loom::model(move || {
        let state = Arc::new(refresh::Refresh::new());
        let requests = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        // Both bearer futures are polled up to lock acquisition before I/O
        // completes. A start barrier fixes the attempted generation, not the outcome.
        let ticket = refresh::ticket(&state);
        let published = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let (state, requests, active, published) = (
                state.clone(),
                requests.clone(),
                active.clone(),
                published.clone(),
            );
            tasks.push(loom::thread::spawn(move || {
                // Snapshot before queuing, exactly as in production.
                let state = if broken {
                    Arc::new(refresh::Refresh::new())
                } else {
                    state
                };
                let operation = async {
                    if published.load(Ordering::SeqCst) == 1 {
                        return (Ok(()), None);
                    }
                    assert_eq!(
                        active.fetch_add(1, Ordering::SeqCst),
                        0,
                        "refresh ownership overlapped"
                    );
                    requests.fetch_add(1, Ordering::SeqCst);
                    loom::thread::yield_now();
                    assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    if fail {
                        (Err::<(), _>(503u16), Some(503u16))
                    } else {
                        published.store(1, Ordering::SeqCst);
                        (Ok(()), None)
                    }
                };
                let result = complete(refresh::joined(&state, ticket, operation));
                if fail {
                    assert!(matches!(result, Ok(Err(503)) | Err(503)));
                } else {
                    assert_eq!(result, Ok(Ok(())));
                }
            }));
        }
        for task in tasks {
            task.join().unwrap();
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "failed refresh was repeated by a waiter"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    });
}
#[test]
fn refresh_attempt_failure_retains_exclusive_ownership() {
    sharing_model(true, false);
}
#[test]
fn successful_refresh_is_shared_without_repeating_io() {
    sharing_model(false, false);
}
#[test]
#[should_panic(expected = "failed refresh was repeated by a waiter")]
fn negative_control_serialization_without_failure_sharing_repeats_io() {
    // Independent failed attempts can also run sequentially; force that
    // interleaving to isolate result sharing from mutual exclusion.
    loom::model(|| {
        let requests = AtomicUsize::new(0);
        for _ in 0..2 {
            let state = refresh::Refresh::<u16>::new();
            let _ = complete(refresh::serialized(&state, async {
                requests.fetch_add(1, Ordering::SeqCst);
                ((), Some(503))
            }));
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "failed refresh was repeated by a waiter"
        );
    });
}
#[test]
#[should_panic(expected = "refresh ownership overlapped")]
fn negative_control_independent_attempts_overlap() {
    sharing_model(true, true);
}

#[test]
fn cancelling_the_holder_releases_the_guard_and_wakes_the_waiter() {
    loom::model(|| {
        let state = Arc::new(refresh::Refresh::<u16>::new());
        let holder_state = state.clone();
        let holder = loom::thread::spawn(move || {
            let mut operation = Box::pin(refresh::serialized(
                &holder_state,
                std::future::pending::<((), Option<u16>)>(),
            ));
            assert_eq!(
                operation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            );
            loom::thread::yield_now();
            drop(operation);
        });
        let waiter_state = state.clone();
        let waiter = loom::thread::spawn(move || {
            assert_eq!(
                complete(refresh::serialized(&waiter_state, async { ((), None) })),
                Ok(())
            );
        });
        holder.join().unwrap();
        waiter.join().unwrap();
    });
}

fn binding_failure_model(broken: bool) {
    loom::model(move || {
        let state = Arc::new(refresh::Refresh::<u16>::new());
        let ticket = refresh::ticket(&state);
        let old = state.clone();
        // Both bindings are queued in the same generation. Constrain the old
        // binding to run first, reproducing the poisoned-result interleaving.
        loom::thread::spawn(move || {
            let result = complete(refresh::joined(&old, ticket, async {
                (Err::<(), _>(401u16), broken.then_some(401u16))
            }));
            assert_eq!(result, Ok(Err(401)));
        })
        .join()
        .unwrap();
        let valid = loom::thread::spawn(move || {
            complete(refresh::joined(&state, ticket, async {
                (Ok::<(), u16>(()), None)
            }))
        })
        .join()
        .unwrap();
        assert_eq!(
            valid,
            Ok(Ok(())),
            "model-local account error leaked to another binding"
        );
    });
}

#[test]
fn model_local_account_errors_do_not_poison_other_bindings() {
    binding_failure_model(false);
}

#[test]
#[should_panic(expected = "model-local account error leaked to another binding")]
fn negative_control_sharing_model_local_errors_rejects_valid_bindings() {
    binding_failure_model(true);
}
