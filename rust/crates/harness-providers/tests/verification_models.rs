//! Exhaustive model of the production usage calibration.
#![cfg(not(target_arch = "wasm32"))]
use crate::{Adapter, sync};
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

// Exercise the production owned mutex; only its callers' scheduling is modeled.
#[cfg(loom)]
#[path = "models/refresh.rs"]
mod refresh;

#[path = "../src/usage_snapshot.rs"]
mod usage_snapshot;
#[test]
fn usage_snapshot_is_atomic_across_concurrent_counting_and_completion() {
    use acyclic_harness_core::model::{
        Model, ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
    };
    // Anchors belong to the prepared input owner, including in the model.
    // Prepare once; each schedule exercises only the original snapshot lock.
    let input = |messages: &[&str]| {
        PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: Model::new("openai".into(), "snapshot".into(), "1".into()).unwrap(),
                messages: messages
                    .iter()
                    .map(|text| ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text((*text).into()),
                    })
                    .collect(),
                tools: Vec::new(),
                sampling: Default::default(),
                max_output_tokens: None,
            },
            Default::default(),
            None,
        )
        .unwrap()
        .observe_input()
        .unwrap()
    };
    let first = usage_snapshot::Usage {
        input: input(&["one"]),
        tokens: 5,
    };
    let second = usage_snapshot::Usage {
        input: input(&["one", "two"]),
        tokens: 11,
    };
    assert_ne!(first.input, second.input);
    loom::model(move || {
        let cell = Arc::new(usage_snapshot::Snapshot::default());
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

#[path = "models/credential_snapshot.rs"]
mod credential_snapshot;
