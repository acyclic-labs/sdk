//! Rust-owned Workers scenarios for the SDK examples bundle.
//!
//! This module deliberately stops at the Workers contract boundary. It
//! hashes and validates a publish request locally; it does not pretend that a
//! registry artifact or hosted execution endpoint is available.

use acyclic_workers::{validate_publish, wire};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Stable source-bound scenario identity.
pub const SCENARIO_ID: &str = "workers-publish-roundtrip";
/// Fully-qualified RPC identity from the Workers descriptor.
pub const OPERATION_ID: &str = "acyclic.workers.v1.WorkersService/PublishVersion";
/// Canonical HTTP route from `acyclic_workers::HTTP_ROUTES`.
pub const ROUTE: &str = "/v1/workers/versions/publish";
/// Rust source path recorded in generated receipts.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/workers_scenarios.rs";
/// Small immutable module used by the executable validation scenario.
pub const MODULE: &[u8] = br#"export default { fetch() { return new Response('ok') } }"#;

/// Stable request fixture for Workers publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishFixture {
    /// Encoded canonical protobuf request.
    pub request: Vec<u8>,
    /// Stable semantic expectation for the local receipt.
    pub expected: Value,
}

/// Receipt emitted after local wire roundtrip and validator execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReceipt {
    /// Stable scenario identity.
    pub scenario_id: &'static str,
    /// Local validation result.
    pub status: &'static str,
    /// SHA-256 of the exact module bytes.
    pub module_sha256: Vec<u8>,
    /// Scope of the evidence; no remote transport is implied.
    pub scope: &'static str,
}

/// Builds the canonical Workers publish request from typed Rust values.
#[must_use]
pub fn request() -> wire::PublishVersionRequest {
    wire::PublishVersionRequest {
        javascript_module: MODULE.to_vec(),
        expected_sha256: Sha256::digest(MODULE).to_vec(),
        idempotency_key: "publish-example-v1".to_owned(),
    }
}

/// Builds the fixture consumed by a transport qualification runner.
#[must_use]
pub fn fixture() -> PublishFixture {
    let request = request();
    PublishFixture {
        request: request.encode_to_vec(),
        expected: json!({
            "kind": "request-validation",
            "accepted": true,
            "message": "acyclic.workers.v1.PublishVersionRequest",
            "route": ROUTE,
            "module_sha256": format!("{:x}", Sha256::digest(MODULE)),
            "idempotency_key": request.idempotency_key,
        }),
    }
}

/// Executes the scenario against the real Workers validator.
pub fn execute() -> Result<PublishReceipt, Box<dyn std::error::Error + Send + Sync>> {
    let original = request();
    let decoded = wire::PublishVersionRequest::decode(original.encode_to_vec().as_slice())?;
    validate_publish(&decoded)?;
    Ok(PublishReceipt {
        scenario_id: SCENARIO_ID,
        status: "qualified",
        module_sha256: Sha256::digest(MODULE).to_vec(),
        scope: "rust-wire-validation",
    })
}

/// Rust snippet rendered into the source-bound examples bundle.
#[must_use]
pub fn rust_snippet() -> String {
    format!(
        "// capability: supported\nuse acyclic_workers::{{validate_publish, wire}};\nuse sha2::{{Digest, Sha256}};\n\nlet module = {module:?};\nlet request = wire::PublishVersionRequest {{ javascript_module: module.to_vec(), expected_sha256: Sha256::digest(module).to_vec(), idempotency_key: \"publish-example-v1\".into() }};\nvalidate_publish(&request)?;",
        module = String::from_utf8_lossy(MODULE),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_request_roundtrips_and_validates() {
        let receipt = execute().expect("Workers request should validate");
        assert_eq!(receipt.scenario_id, SCENARIO_ID);
        assert_eq!(receipt.status, "qualified");
        assert_eq!(receipt.scope, "rust-wire-validation");
    }

    #[test]
    fn fixture_is_canonical_and_has_no_placeholders() {
        let fixture = fixture();
        let decoded = wire::PublishVersionRequest::decode(fixture.request.as_slice())
            .expect("fixture must be canonical protobuf");
        validate_publish(&decoded).expect("fixture must satisfy Workers validation");
        assert_eq!(fixture.expected["accepted"], true);
        assert!(!rust_snippet().contains("TODO"));
        assert!(!rust_snippet().contains("{{receipt"));
    }
}
