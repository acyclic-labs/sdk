//! Exhaustive model of the production usage calibration.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness_providers::Adapter;
#[cfg(loom)]
#[path = "../src/sync.rs"]
mod sync;
#[cfg(not(loom))]
#[path = "../src/sync/loom.rs"]
pub mod sync;
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
