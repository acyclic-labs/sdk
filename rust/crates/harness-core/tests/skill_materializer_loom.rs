//! Schedule the production journal owner, including its pre-fix negative control.
//! Tokio is the real primitive; its internal atomics are not Loom-instrumented.
#![cfg(loom)]

#[path = "../src/context/skill_materializer.rs"]
mod skill_materializer;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::{
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };

    struct Unpark(loom::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    // Tokio may clone a waker while holding its uninstrumented wait-list lock.
    // Clone/drop only std::Arc here; the model event is the actual wake.
    let waker = Waker::from(Arc::new(Unpark(loom::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => loom::thread::park(),
        }
    }
}

fn schedule<const SERIALIZE: bool>() {
    use loom::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let owner = skill_materializer::SkillMaterializer::new();
    let head = Arc::new(AtomicUsize::new(0));
    let tasks = (0..2)
        .map(|_| {
            let owner = owner.clone();
            let head = head.clone();
            loom::thread::spawn(move || {
                let _guard = SERIALIZE.then(|| block_on(owner.enter()));
                let pinned = head.load(Ordering::SeqCst);
                loom::thread::yield_now();
                assert!(
                    head.compare_exchange(pinned, pinned + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok(),
                    "materialization conflict"
                );
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(head.load(Ordering::SeqCst), 2);
}

#[test]
fn journal_owner_serializes_first_publications() {
    loom::model(schedule::<true>);
}

#[test]
fn former_independent_publishers_can_conflict() {
    let failure = std::panic::catch_unwind(|| loom::model(schedule::<false>))
        .expect_err("negative control must conflict");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("materialization conflict"),
        "unexpected model failure: {text}"
    );
}

#[test]
fn cancelled_waiter_does_not_take_the_next_owners_wake() {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Wake, Waker},
    };

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    loom::model(|| {
        let owner = skill_materializer::SkillMaterializer::new();
        let held = block_on(owner.enter());
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        let mut abandoned = Box::pin(owner.enter());
        let mut remaining = Box::pin(owner.enter());
        assert!(abandoned.as_mut().poll(&mut context).is_pending());
        assert!(remaining.as_mut().poll(&mut context).is_pending());
        drop(abandoned);
        wakes.0.store(0, Ordering::Relaxed);
        drop(held);
        assert!(
            wakes.0.load(Ordering::Relaxed) > 0,
            "owner release must wake its queued successor"
        );
        assert!(remaining.as_mut().poll(&mut context).is_ready());
    });
}
