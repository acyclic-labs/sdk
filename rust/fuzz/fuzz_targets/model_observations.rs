//! Exercise the shipped model-observation decoders with arbitrary bytes.
//! Decoding is not terminal admission or evidence of provider execution.
#![no_main]

use acyclic_harness_core::{
    contract::{canonical_json_bytes, json_from_slice},
    executor::{ModelRequestBody, ModelUsageRecord},
    model::ModelEvent,
};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fn body(metadata: &Value) {
    let Some(original) = ModelRequestBody::reported(metadata) else {
        return;
    };
    assert_eq!(original.version, ModelRequestBody::VERSION);
    let encoded = canonical_json_bytes(metadata).expect("accepted metadata encodes");
    let restored: Value = json_from_slice(&encoded).expect("canonical metadata decodes");
    assert_eq!(ModelRequestBody::reported(&restored), Some(original));

    let mut unsupported = restored.clone();
    unsupported["request_body"]["version"] = Value::from(ModelRequestBody::VERSION + 1);
    assert_eq!(ModelRequestBody::reported(&unsupported), None);
    unsupported["request_body"]["version"] = Value::from(ModelRequestBody::VERSION);
    unsupported["request_body"]["digest"] = Value::Array(Vec::new());
    assert_eq!(ModelRequestBody::reported(&unsupported), None);
}

fuzz_target!(|bytes: &[u8]| {
    if let Ok(metadata) = json_from_slice::<Value>(bytes) {
        body(&metadata);
    }
    if let Ok(event) = json_from_slice::<ModelEvent>(bytes) {
        let encoded = canonical_json_bytes(&event).expect("accepted event encodes");
        let restored: ModelEvent = json_from_slice(&encoded).expect("canonical event decodes");
        assert_eq!(restored, event);
        if let ModelEvent::Completed { metadata } = &event {
            body(metadata);
        }
    }
    if let Ok(usage) = json_from_slice::<ModelUsageRecord>(bytes) {
        let encoded = canonical_json_bytes(&usage).expect("accepted usage encodes");
        let restored: ModelUsageRecord =
            json_from_slice(&encoded).expect("canonical usage decodes");
        assert_eq!(canonical_json_bytes(&restored).ok(), Some(encoded));
        // Only the committed terminal and original admission can validate
        // usage. A syntactically valid unsupported version stays unsupported.
        assert_eq!(restored.version, usage.version);
        assert_eq!(restored.request_body, usage.request_body);
    }
});
