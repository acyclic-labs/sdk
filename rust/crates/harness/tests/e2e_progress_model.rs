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
                    admitted.waiting("status follow");
                    if panic {
                        panic!("scenario panic control");
                    }
                    // Cancellation drops this same guard without awaiting the follow.
                    drop(admitted);
                });
                assert_eq!(outcome.is_err(), panic);
            });
            let second = registry.clone();
            let other = loom::thread::spawn(move || {
                let admitted = second.register(Path::new("other"));
                admitted.waiting("status follow");
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
            assert_eq!(revision, 6);
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
        admitted.waiting("status follow");
        // Broken variant omits precisely the production cleanup path.
        std::mem::forget(admitted);
        assert!(
            registry.snapshot().1.is_empty(),
            "cleanup lost its registration"
        );
    });
}
