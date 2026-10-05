//! Shared malformed model-input cases consumed by native Rust and the WASM
//! TypeScript facade. The request itself stays in model-input-v3; this vector
//! only describes deterministic JSON edits and the expected admission outcome.

use acyclic_harness::{
    conversation::Limits,
    model::{ModelOptionPolicy, ModelRequest},
    model_input::PreparedModelInput,
};
use serde::Deserialize;
use serde_json::Value;

const REQUEST_VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-v3.json"
));
const REJECTION_VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-rejection-v1.json"
));

#[derive(Debug, Deserialize)]
struct RequestVector {
    limits: Limits,
    policy: ModelOptionPolicy,
    root: Root,
}

#[derive(Debug, Deserialize)]
struct Root {
    request: ModelRequest,
}

#[derive(Debug, Deserialize)]
struct RejectionVector {
    version: u32,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    operation: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    context_messages: Option<usize>,
    expected: String,
}

fn edit_request(mut request: Value, case: &Case) -> Value {
    match case.operation.as_str() {
        "identity" => request,
        "replace" => {
            let path = case.path.as_deref().expect("replacement path");
            *request.pointer_mut(path).expect("replacement target") =
                case.value.clone().expect("replacement value");
            request
        }
        "remove" => {
            let path = case.path.as_deref().expect("removal path");
            let (parent_path, key) = path.rsplit_once('/').expect("removal parent");
            let parent = request
                .pointer_mut(if parent_path.is_empty() {
                    "/"
                } else {
                    parent_path
                })
                .expect("removal parent target");
            match parent {
                Value::Array(items) => {
                    items.remove(key.parse::<usize>().expect("array removal index"));
                }
                Value::Object(fields) => {
                    fields.remove(key);
                }
                _ => panic!("removal parent must be an object or array"),
            }
            request
        }
        operation => panic!("unsupported rejection operation {operation}"),
    }
}

fn limits_for(base: Limits, case: &Case) -> Limits {
    match case.context_messages {
        Some(context_messages) => Limits {
            context_messages,
            ..base
        },
        None => base,
    }
}

#[test]
fn native_admission_matches_shared_malformed_model_input_matrix() {
    let source: RequestVector =
        serde_json::from_str(REQUEST_VECTOR).expect("model-input-v3 fixture must decode");
    let matrix: RejectionVector = serde_json::from_str(REJECTION_VECTOR)
        .expect("model-input-rejection-v1 fixture must decode");
    assert_eq!(matrix.version, 1);

    let base = serde_json::to_value(source.root.request).expect("base request must encode");
    for case in matrix.cases {
        let request: Result<ModelRequest, _> =
            serde_json::from_value(edit_request(base.clone(), &case));
        let admitted = request
            .ok()
            .and_then(|request| {
                PreparedModelInput::prepare_with_policy(
                    request,
                    limits_for(source.limits, &case),
                    Some(&source.policy),
                )
                .ok()
            })
            .is_some();
        let expected = case.expected == "accept";
        assert_eq!(admitted, expected, "{}", case.name);
    }
}
