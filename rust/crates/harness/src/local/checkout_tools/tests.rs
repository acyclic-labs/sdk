use super::*;
use loom::thread;
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

#[path = "tests/fencing.rs"]
mod fencing;
