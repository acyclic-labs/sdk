use super::*;
use loom::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("model operation did not finish"),
    }
}

fn schedule<const RETAIN: bool>() {
    let state = StateArc::new(AsyncMutex::new(()));
    let cleaned = StateArc::new(AtomicBool::new(false));
    let completed = StateArc::clone(&cleaned);
    let mut worker = None;
    let mut command = Box::pin(serialized(&state, async |guard| {
        with_workers::<_, _, _, _, RETAIN>(
            guard,
            || Ok(()),
            |check| {
                worker = Some(thread::spawn(move || {
                    thread::yield_now();
                    check().unwrap();
                    completed.store(true, Ordering::SeqCst);
                }));
                std::future::pending::<()>()
            },
        )
        .await
    }));
    assert!(matches!(
        command
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(command);
    let observed = complete(serialized(&state, async |_| cleaned.load(Ordering::SeqCst)));
    worker.unwrap().join().unwrap();
    assert!(observed, "hook cleanup escaped its workspace guard");
}

#[test]
fn cancellation_keeps_hook_workers_inside_the_selected_workspace() {
    loom::model(schedule::<true>);
    let failure = std::panic::catch_unwind(|| loom::model(schedule::<false>))
        .expect_err("missing guard retention escaped the cleanup detector");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("hook cleanup escaped its workspace guard"),
        "{text}"
    );
}

fn projection_cancellation<const RETAIN: bool>() {
    let slot = StateArc::new(AsyncMutex::new(None));
    let users = StateArc::new(crate::sync::SyncMutex::new(0));
    let claim = complete(crate::local::workspace::projection::enter(
        &slot,
        &users,
        async || Ok(7),
    ))
    .unwrap();
    let mut worker = None;
    let observed = users.clone();
    let worker_slot = slot.clone();
    let mut command = Box::pin(with_workers::<_, _, _, _, RETAIN>(
        claim,
        || Ok(()),
        |check| {
            worker = Some(thread::spawn(move || {
                thread::yield_now();
                check().unwrap();
                assert!(
                    *observed.lock().unwrap() > 0,
                    "physical worker lost its projection claim"
                );
                assert_eq!(
                    *complete(crate::sync::owned::lock_owned(worker_slot)),
                    Some(7),
                    "physical worker lost its projection claim"
                );
                drop(check);
            }));
            std::future::pending::<()>()
        },
    ));
    assert!(
        command
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(command);
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
        .unwrap()
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(*users.lock().unwrap(), 0);
}

#[test]
fn cancelled_hook_retains_projection_claim_until_physical_cleanup() {
    loom::model(projection_cancellation::<true>);
    let failure = std::panic::catch_unwind(|| loom::model(projection_cancellation::<false>))
        .expect_err("lost worker claim was accepted");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("physical worker lost its projection claim"),
        "{text}"
    );
}
