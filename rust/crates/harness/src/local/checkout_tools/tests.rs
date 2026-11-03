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

fn complete<F: Future>(future: F) -> F::Output {
    crate::sync::owned::block_on(future)
}

// Both variants call the production serialization function and owned mutex.
// The negative control releases ownership before publication.
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
                crate::filesystem::native_process::capture::with_custody(guard, |_| {
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

fn offloaded_projection_cleanup(broken: bool) {
    let slot = StateArc::new(AsyncMutex::new(None));
    let users = StateArc::new(crate::sync::SyncMutex::new(0));
    let claim = complete(crate::local::workspace::projection::enter(
        &slot,
        &users,
        async || Ok(7),
    ))
    .unwrap();
    let observed = users.clone();
    let worker_slot = slot.clone();
    let mut caller_claim = None;
    let worker = if broken {
        caller_claim = Some(claim);
        thread::spawn(move || {
            thread::yield_now();
            assert!(
                *observed.lock().unwrap() > 0,
                "bash worker lost its projection before physical cleanup"
            );
            assert_eq!(
                *complete(crate::sync::owned::lock_owned(worker_slot)),
                Some(7),
                "bash worker lost its projection before physical cleanup"
            );
        })
    } else {
        thread::spawn(move || {
            let ((), claim) =
                crate::filesystem::native_process::capture::with_custody(claim, |_| {
                    thread::yield_now();
                    assert!(
                        *observed.lock().unwrap() > 0,
                        "bash worker lost its projection before physical cleanup"
                    );
                    assert_eq!(
                        *complete(crate::sync::owned::lock_owned(worker_slot)),
                        Some(7),
                        "bash worker lost its projection before physical cleanup"
                    );
                });
            // The dropped waiter cannot receive the completed custody. It is
            // released here, after the same physical cleanup boundary as Bash.
            drop(claim);
        })
    };
    drop(caller_claim);
    // A second command can enter and complete while the first command's
    // caller is gone. Its last-user check must still see physical custody.
    let next = complete(crate::local::workspace::projection::enter(
        &slot,
        &users,
        async || Ok(8),
    ))
    .unwrap();
    complete(crate::local::workspace::projection::leave(
        &slot,
        &users,
        next,
        async |slot, retire| {
            if retire {
                *slot = None;
            }
            Ok(())
        },
    ))
    .unwrap();
    worker
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(*users.lock().unwrap(), 0);
}

#[test]
fn cancelled_bash_retains_projection_until_physical_cleanup() {
    checked_model(
        offloaded_projection_cleanup,
        "bash worker lost its projection before physical cleanup",
    );
}

#[path = "tests/fencing.rs"]
mod fencing;
