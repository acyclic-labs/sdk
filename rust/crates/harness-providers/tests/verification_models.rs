//! Exhaustive model of the production usage calibration.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness_providers::Adapter;
#[cfg(loom)]
#[path = "../src/sync.rs"]
mod sync;
#[cfg(not(loom))]
#[path = "../src/sync/loom.rs"]
mod sync;
use sync::Arc;
mod accounting {
    include!("../src/accounting.rs");

    // This mutation needs a switch between load and store to lose an update.
    pub fn observe_racy(estimator: &TokenEstimator, ratio: f64) {
        let old = estimator.bytes_per_token.load(Ordering::Relaxed);
        let next = f64::from_bits(old).min(ratio);
        estimator
            .bytes_per_token
            .store(next.to_bits(), Ordering::Relaxed);
    }
}

#[test]
fn concurrent_calibration_preserves_the_dense_minimum_without_torn_reads() {
    calibration_model(false);
}

#[test]
#[should_panic(expected = "calibration lost the dense minimum")]
fn negative_control_detects_a_non_atomic_minimum_update() {
    calibration_model(true);
}

fn calibration_model(broken: bool) {
    loom::model(move || {
        let estimator = Arc::new(accounting::TokenEstimator::new(Adapter::Responses));
        let dense = estimator.clone();
        let dense = loom::thread::spawn(move || {
            if broken {
                accounting::observe_racy(&dense, 0.81);
            } else {
                dense.observe(144, &serde_json::json!({"usage":{"input_tokens":160}}));
            }
        });
        let sparse = estimator.clone();
        let sparse = loom::thread::spawn(move || {
            if broken {
                accounting::observe_racy(&sparse, 900.0);
            } else {
                sparse.observe(1000, &serde_json::json!({"usage":{"input_tokens":1}}));
            }
        });
        assert!((16..=40).contains(&estimator.tokens(32).unwrap()));
        dense.join().unwrap();
        sparse.join().unwrap();
        assert_eq!(
            estimator.tokens(32),
            Some(40),
            "calibration lost the dense minimum"
        );
    });
}

#[path = "models/refresh.rs"]
mod refresh;

#[path = "../src/usage_snapshot.rs"]
mod usage_snapshot;
#[test]
fn usage_snapshot_is_atomic_across_concurrent_counting_and_completion() {
    loom::model(|| {
        let cell = Arc::new(usage_snapshot::Snapshot::default());
        let first = usage_snapshot::Usage {
            digest: [1; 32],
            messages: 1,
            bytes: 3,
            tokens: 5,
        };
        let second = usage_snapshot::Usage {
            digest: [2; 32],
            messages: 2,
            bytes: 7,
            tokens: 11,
        };
        cell.set(first).unwrap();
        let writer = cell.clone();
        let task = loom::thread::spawn(move || writer.set(second).unwrap());
        let observed = cell.get().unwrap().unwrap();
        assert!(
            observed == first || observed == second,
            "torn usage snapshot"
        );
        task.join().unwrap();
        assert_eq!(cell.get().unwrap(), Some(second));
    });
}
#[test]
#[should_panic(expected = "torn usage snapshot")]
fn negative_control_independent_usage_fields_can_tear() {
    use sync::atomic::{AtomicU64, Ordering};
    loom::model(|| {
        let cell = Arc::new((AtomicU64::new(3), AtomicU64::new(5)));
        let writer = cell.clone();
        let task = loom::thread::spawn(move || {
            writer.0.store(7, Ordering::SeqCst);
            writer.1.store(11, Ordering::SeqCst);
        });
        let observed = (cell.0.load(Ordering::SeqCst), cell.1.load(Ordering::SeqCst));
        assert!(
            observed == (3, 5) || observed == (7, 11),
            "torn usage snapshot"
        );
        task.join().unwrap();
    });
}

#[path = "models/cancellation.rs"]
mod cancellation;
