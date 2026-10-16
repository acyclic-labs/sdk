use super::*;
use loom::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn checked_model(scenario: fn(bool), diagnostic: &str) {
    loom::model(move || scenario(false));
    let failure = std::panic::catch_unwind(|| loom::model(move || scenario(true)))
        .expect_err("negative control did not expose the broken boundary");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(diagnostic),
        "unexpected negative control: {message}"
    );
}

// The shim's admission waits are Loom condition-variable steps, so these
// finite futures finish in one poll. Avoid a thread-local executor, which
// cannot be nested across Loom's generator-backed threads.
fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("model operation did not finish"),
    }
}

// Both variants call the production serialization function and its cfg(acyclic_harness_loom)
// mutex shim. The negative control releases ownership before publication.
fn publication(broken: bool) {
    let state = StateArc::new(AsyncMutex::new((0, Some(0))));
    let commands: Vec<_> = (0..2)
        .map(|_| {
            let state = StateArc::clone(&state);
            thread::spawn(move || {
                complete(serialized(&state, async |mut cursor| {
                    let generation = cursor.0;
                    let consistent = cursor.1.take() == Some(generation);
                    if broken {
                        drop(cursor);
                        thread::yield_now();
                        let mut cursor = lock_owned(StateArc::clone(&state)).await;
                        cursor.0 = generation + 1;
                        cursor.1 = Some(cursor.0);
                    } else {
                        thread::yield_now();
                        cursor.0 += 1;
                        cursor.1 = Some(cursor.0);
                    }
                    consistent
                }))
            })
        })
        .collect();
    let results: Vec<_> = commands
        .into_iter()
        .map(|command| command.join().unwrap())
        .collect();
    let final_state = *complete(lock_owned(state));
    assert!(
        results.into_iter().all(|consistent| consistent) && final_state == (2, Some(2)),
        "cursor torn"
    );
}

#[test]
fn commands_publish_generation_and_cursor_without_interleaving() {
    checked_model(publication, "cursor torn");
}

fn cancellation(broken: bool) {
    let state = StateArc::new(AsyncMutex::new(Some(0)));
    let mut command = Box::pin(serialized(&state, async |mut cursor| {
        if !broken {
            cursor.take();
        }
        std::future::pending::<()>().await;
    }));
    assert_eq!(
        command
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    );
    drop(command);
    let invalidated = complete(lock_owned(state)).is_none();
    assert!(invalidated, "cancelled capture retained its cursor");
}

#[test]
fn cancellation_releases_the_guard_and_invalidates_the_cursor() {
    checked_model(cancellation, "cancelled capture retained its cursor");
}

fn offloaded_cleanup(broken: bool) {
    let state = StateArc::new(AsyncMutex::new(()));
    let cleaned = StateArc::new(AtomicBool::new(false));
    let completed = StateArc::clone(&cleaned);
    let mut backend = None;
    let mut command = Box::pin(serialized(&state, async |guard| {
        if broken {
            drop(guard);
            backend = Some(thread::spawn(move || {
                thread::yield_now();
                completed.store(true, Ordering::SeqCst);
            }));
        } else {
            backend = Some(thread::spawn(move || {
                guarded(guard, |_| {
                    thread::yield_now();
                    completed.store(true, Ordering::SeqCst);
                });
            }));
        }
        std::future::pending::<()>().await;
    }));
    assert_eq!(
        command
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    );
    drop(command); // The awaiting task is cancelled after submitting the job.
    let completed_before_next_command =
        complete(serialized(&state, async |_| cleaned.load(Ordering::SeqCst)));
    backend.unwrap().join().unwrap();
    drop(state);
    drop(cleaned);
    assert!(
        completed_before_next_command,
        "cleanup escaped checkout ownership"
    );
}

#[test]
fn cancelled_waiter_keeps_a_submitted_jobs_guard_until_cleanup() {
    checked_model(offloaded_cleanup, "cleanup escaped checkout ownership");
}

#[path = "tests/fencing.rs"]
mod fencing;
