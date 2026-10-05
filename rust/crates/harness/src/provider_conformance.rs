//! Native provider request/response consumption cases shared with the WASM
//! facade. The fixture records canonical event bytes so a provider adapter
//! cannot accidentally serialize a different response shape per target.

#![cfg(test)]

use crate::{
    conversation::Limits,
    executor::ModelEventAdmission,
    model::{ModelEvent, ModelOptionPolicy, ModelRequest},
    model_input::PreparedModelInput,
    Result,
};
use serde::Deserialize;
use serde_json::Value;

const MODEL_VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-v3.json"
));
const REQUEST_MATRIX: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-rejection-v1.json"
));
const PROVIDER_VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/provider-consumption-v1.json"
));

#[derive(Debug, Deserialize)]
struct ModelVector {
    limits: Limits,
    policy: ModelOptionPolicy,
    root: ModelRoot,
}

#[derive(Debug, Deserialize)]
struct ModelRoot {
    request: ModelRequest,
    expected: ModelExpected,
}

#[derive(Debug, Deserialize)]
struct ModelExpected {
    request_json: String,
}

#[derive(Debug, Deserialize)]
struct RequestMatrix {
    version: u32,
}

#[derive(Debug, Deserialize)]
struct ProviderVector {
    version: u32,
    request_matrix: String,
    request_capture: RequestCapture,
    responses: Vec<ResponseCase>,
}

#[derive(Debug, Deserialize)]
struct RequestCapture {
    model_vector: String,
    case: String,
    boundary: String,
    provider_adapter: String,
    replay: String,
    forbidden_model_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ResponseCase {
    name: String,
    prefix: Vec<Value>,
    event: Value,
    canonical_json: String,
    #[serde(default)]
    model_events_per_step: Option<usize>,
    expected: String,
}

fn limits_for(base: Limits, case: &ResponseCase) -> Limits {
    match case.model_events_per_step {
        Some(model_events_per_step) => Limits {
            model_events_per_step,
            ..base
        },
        None => base,
    }
}

#[test]
fn native_provider_consumption_matches_shared_request_and_response_fixture() -> Result<()> {
    let model: ModelVector =
        serde_json::from_str(MODEL_VECTOR).expect("model-input-v3 fixture must decode");
    let request_matrix: RequestMatrix =
        serde_json::from_str(REQUEST_MATRIX).expect("model-input-rejection-v1 fixture must decode");
    let provider: ProviderVector =
        serde_json::from_str(PROVIDER_VECTOR).expect("provider-consumption-v1 fixture must decode");
    assert_eq!(request_matrix.version, 1);
    assert_eq!(provider.version, 1);
    assert_eq!(provider.request_matrix, "model-input-rejection-v1.json");
    assert_eq!(provider.request_capture.model_vector, "model-input-v3.json");
    assert_eq!(provider.request_capture.case, "root");
    assert_eq!(provider.request_capture.boundary, "prepareModelRequest");
    assert_eq!(provider.request_capture.provider_adapter, "not-exposed");
    assert_eq!(provider.request_capture.replay, "byte-equal");

    let request = model.root.request;
    let prepared = PreparedModelInput::prepare_with_policy(
        request.clone(),
        model.limits,
        Some(&model.policy),
    )?;
    assert_eq!(
        prepared.bytes(),
        model.root.expected.request_json.as_bytes()
    );
    let replayed =
        PreparedModelInput::prepare_with_policy(request, model.limits, Some(&model.policy))?;
    assert_eq!(replayed.bytes(), prepared.bytes());
    assert_eq!(
        replayed.manifest().request_digest,
        prepared.manifest().request_digest
    );
    let serialized: Value = serde_json::from_slice(prepared.bytes())?;
    for field in provider.request_capture.forbidden_model_fields {
        assert!(
            !contains_key(&serialized, &field),
            "transport field leaked into model request: {field}"
        );
    }

    for case in provider.responses {
        let limits = limits_for(model.limits, &case);
        let mut admission = ModelEventAdmission::default();
        for prefix in case.prefix {
            let event: ModelEvent =
                serde_json::from_value(prefix).expect("provider response prefix must decode");
            admission.observe(&event, limits)?;
        }
        let event: ModelEvent =
            serde_json::from_value(case.event).expect("provider response must decode");
        let canonical = crate::contract::canonical_json_bytes(&event)?;
        assert_eq!(canonical, case.canonical_json.as_bytes(), "{}", case.name);
        let admitted = admission.observe(&event, limits).is_ok();
        assert_eq!(admitted, case.expected == "accept", "{}", case.name);
    }
    Ok(())
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => object
            .iter()
            .any(|(name, child)| name == key || contains_key(child, key)),
        Value::Array(values) => values.iter().any(|child| contains_key(child, key)),
        _ => false,
    }
}
