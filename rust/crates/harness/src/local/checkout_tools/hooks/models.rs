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
