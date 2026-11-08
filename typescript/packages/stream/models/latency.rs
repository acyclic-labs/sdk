use super::driver;
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

fn verify(state: &Value, warm: u8, samples: u8) {
    let records = u64::from(1 + warm + samples);
    assert_eq!(
        state["isolated"], true,
        "runtime latency workloads collided: {state}"
    );
    assert_eq!(state["accepted"], 2 * records);
    assert_eq!(state["tails"], json!([records, records]));
}

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 32,
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/latency-regressions.txt")))),
        ..Default::default()
    })]
    #[test]
    fn runtime_workloads_keep_independent_cursors_and_exact_append_counts(
        name in "[a-z]{1,8}", warm in 0u8..3, samples in 1u8..5, reverse in any::<bool>(),
    ) {
        // One test-harness lock, never nested: serialize commands to the JS
        // process. Production provider synchronization executes inside Bun.
        static DRIVER: driver::Shared = driver::Shared::new();
        let state = driver::Driver::shared(&DRIVER, &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."), "decode", "")?
            .run()?.call(&json!({ "action":"latency", "path":name,
            "work": { "warm":warm, "samples":samples, "reverse":reverse, "shared":false } }))?;
        verify(&state, warm, samples);
    }
}

fn shared_namespace() -> std::io::Result<()> {
    let mut real = driver::Driver::configured(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."),
        "decode",
        "",
    )?;
    let state = real.call(&json!({ "action":"latency", "path":"before",
        "work": { "warm":1, "samples":2, "reverse":false, "shared":true } }))?;
    assert!(
        state["rejection"]
            .as_str()
            .unwrap()
            .contains("initial latency append failed")
    );
    assert_eq!(state["accepted"], 4);
    verify(&state, 1, 2);
    Ok(())
}

#[test]
#[should_panic(expected = "runtime latency workloads collided")]
fn negative_control_reuses_one_runtime_namespace() {
    super::completed(shared_namespace());
}
