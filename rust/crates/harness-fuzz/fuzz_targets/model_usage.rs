#![no_main]

use acyclic_harness_core::{contract, executor::ModelUsageRecord};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    // The persisted completion reader uses this same typed decoder. Event and
    // request authentication remain the journal projection's responsibility.
    let Ok(record) = contract::json_from_slice::<ModelUsageRecord>(bytes) else {
        return;
    };
    let input: serde_json::Value = contract::json_from_slice(bytes).unwrap();
    let encoded = serde_json::to_vec(&record).unwrap();
    let restored: ModelUsageRecord = contract::json_from_slice(&encoded).unwrap();
    let output: serde_json::Value = contract::json_from_slice(&encoded).unwrap();
    for field in ["started_at", "completed_at", "latency_ns", "ttft_ns"] {
        assert_eq!(
            input.get(field).unwrap_or(&serde_json::Value::Null),
            &output[field],
            "usage timing observation changed: {field}"
        );
    }
    assert_eq!(serde_json::to_value(restored).unwrap(), output);
});
