//! Run this target with --cfg acyclic_e2e_progress_loom; ordinary gates use real locks.
#![cfg(acyclic_e2e_progress_loom)]
#[path = "support/progress_registry.rs"]
mod registry;
use registry::Registry;
use std::{path::Path, sync::Arc};

#[test]
fn registration_reporting_and_cancel_or_panic_cleanup_are_atomic() {
    for panic in [false, true] {
        loom::model(move || {
            let registry = Arc::new(Registry::default());
            let worker = registry.clone();
            let task = loom::thread::spawn(move || {
                let outcome = std::panic::catch_unwind(|| {
                    let admitted = worker.register(Path::new("scenario"));
                    let waiting = admitted.waiting("status follow");
                    if panic {
                        panic!("scenario panic control");
                    }
                    // Cancellation drops this same guard without awaiting the follow.
                    drop(waiting);
                    assert!(
                        worker
                            .snapshot()
                            .1
                            .iter()
                            .all(|(path, label)| path != Path::new("scenario")
                                || *label == "admitted")
                    );
                    drop(admitted);
                });
                assert_eq!(outcome.is_err(), panic);
            });
            let second = registry.clone();
            let other = loom::thread::spawn(move || {
                let admitted = second.register(Path::new("other"));
                let waiting = admitted.waiting("status follow");
                drop(waiting);
                drop(admitted);
            });
            let (revision, rows) = registry.snapshot();
            assert!(rows.len() <= 2);
            assert!(rows.windows(2).all(|pair| pair[0].0 != pair[1].0));
            for (path, label) in rows {
                assert!(path == Path::new("scenario") || path == Path::new("other"));
                assert!(matches!(label, "admitted" | "status follow"));
                assert!(revision >= 1);
            }
            task.join().unwrap();
            other.join().unwrap();
            let (revision, rows) = registry.snapshot();
            assert_eq!(revision, 8);
            assert!(rows.is_empty(), "cleanup lost its registration");
        });
    }
}

#[test]
#[should_panic(expected = "cleanup lost its registration")]
fn forgotten_cleanup_is_detected_on_the_real_registration() {
    loom::model(|| {
        let registry = Arc::new(Registry::default());
        let admitted = registry.register(Path::new("scenario"));
        let _waiting = admitted.waiting("status follow");
        // Broken variant omits precisely the production cleanup path.
        std::mem::forget(admitted);
        assert!(
            registry.snapshot().1.is_empty(),
            "cleanup lost its registration"
        );
    });
}

fn wake_oracle(broken: bool) {
    loom::model(move || {
        let registry = Arc::new(Registry::default());
        let admitted = registry.register(Path::new("runnable"));
        let wake = if broken {
            std::task::Waker::noop().clone()
        } else {
            admitted.observed_waker(std::task::Waker::noop())
        };
        let polling = admitted.polling();
        assert!(registry.blocked_snapshot(registry.snapshot().0).is_none());
        let worker = loom::thread::spawn(move || wake.wake());
        // The real wake races with Pending publication and reporter snapshots.
        assert!(registry.blocked_snapshot(registry.snapshot().0).is_none());
        drop(polling);
        worker.join().unwrap();
        assert!(
            registry.blocked_snapshot(registry.snapshot().0).is_none(),
            "a runnable task was reported blocked"
        );
        let polling = admitted.polling();
        assert!(registry.blocked_snapshot(registry.snapshot().0).is_none());
        drop(polling);
        assert!(registry.blocked_snapshot(registry.snapshot().0).is_some());
        let late = admitted.observed_waker(std::task::Waker::noop());
        drop(admitted);
        late.wake();
        assert!(registry.snapshot().1.is_empty());
    });
}

#[test]
fn polling_wake_and_reporting_never_lose_runnability() {
    wake_oracle(false);
}

#[test]
#[should_panic(expected = "a runnable task was reported blocked")]
fn an_unobserved_wake_fails_the_same_runnability_oracle() {
    wake_oracle(true);
}
