use super::*;
use loom::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

// The owned-lock shim admits waiters through Loom condition variables, so
// each modeled operation finishes in one poll.
fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("model operation did not finish"),
    }
}

fn checked(scenario: fn(bool), diagnostic: &str) {
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

const TASK: TaskId = TaskId::from_bytes([1; 16]);

fn construct(live: &AtomicUsize) -> Result<u8> {
    assert_eq!(
        live.fetch_add(1, Ordering::SeqCst),
        0,
        "two live child views"
    );
    Ok(1)
}

// A settling child retires its view under the child's own gate, so a caller
// that arrives meanwhile constructs a replacement only after the old view is
// physically retired. The negative control lets retirement escape the gate.
fn retirement(broken: bool) {
    let views = StateArc::new(ChildViews::<u8>::default());
    let live = StateArc::new(AtomicUsize::new(0));
    drop(
        complete(ChildViews::acquire(
            &views,
            TASK,
            async || construct(&live),
            async |_| Ok(()),
        ))
        .unwrap(),
    );
    let escaped = StateArc::new(crate::sync::SyncMutex::new(None));
    let settling = {
        let (views, live, escaped) = (
            StateArc::clone(&views),
            StateArc::clone(&live),
            StateArc::clone(&escaped),
        );
        thread::spawn(move || {
            complete(ChildViews::settle(&views, TASK, async |_| {
                let live = StateArc::clone(&live);
                let retire = move || {
                    thread::yield_now();
                    live.fetch_sub(1, Ordering::SeqCst);
                };
                if broken {
                    *escaped.lock().unwrap() = Some(thread::spawn(retire));
                } else {
                    retire();
                }
                Ok(())
            }))
            .unwrap();
        })
    };
    let acquiring = {
        let (views, live) = (StateArc::clone(&views), StateArc::clone(&live));
        thread::spawn(move || {
            drop(
                complete(ChildViews::acquire(
                    &views,
                    TASK,
                    async || construct(&live),
                    async |_| Ok(()),
                ))
                .unwrap(),
            );
        })
    };
    settling.join().unwrap();
    acquiring.join().unwrap();
    let escaped = escaped.lock().unwrap().take();
    if let Some(escaped) = escaped {
        escaped.join().unwrap();
    }
}

#[test]
fn settled_child_view_retires_before_its_replacement_is_constructed() {
    checked(retirement, "two live child views");
}

// A child's view is constructed and leased while its parent's running command
// holds the parent's checkout guard. The negative control constructs under
// that guard, as the parent-owned index did, and deadlocks.
fn progress(broken: bool) {
    let parent = StateArc::new(AsyncMutex::new(()));
    let views = StateArc::new(ChildViews::<u8>::default());
    let command = complete(lock_owned(StateArc::clone(&parent)));
    let child = thread::spawn(move || {
        for _ in 0..2 {
            let view = complete(ChildViews::acquire(
                &views,
                TASK,
                async || {
                    if broken {
                        drop(lock_owned(StateArc::clone(&parent)).await);
                    }
                    Ok(1)
                },
                async |_| Ok(()),
            ))
            .unwrap();
            assert_eq!(*view, 1);
        }
    });
    child.join().unwrap();
    drop(command);
}

#[test]
fn a_child_view_never_waits_for_the_parents_running_command() {
    checked(progress, "deadlock");
}
