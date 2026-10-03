//! Rust-owned Objects v2 scenarios for the SDK examples bundle.
//!
//! The scenario exercises the public in-memory provider and the canonical
//! request messages. It proves local provider behavior only; Objects v2 is an
//! unreleased source preview and this module makes no hosted-service claim.

use acyclic_objects::{MemoryObjects, ObjectsProvider, wire};
use bytes::Bytes;
use prost::Message;
use serde_json::{Value, json};

/// Stable source-bound scenario identity.
pub const SCENARIO_ID: &str = "objects-memory-put-get";
/// Fully-qualified RPC identity used for the object PUT fixture.
pub const OPERATION_ID: &str = "acyclic.objects.v2.ObjectsService/PutObject";
/// Canonical HTTP route represented by the fixture.
pub const ROUTE: &str = "/v2/objects/objects/put";
/// Rust source path recorded in generated receipts.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/objects_scenarios.rs";
const OBJECT_KEY: &str = "hello.txt";
const BODY: &[u8] = b"hello";

/// One encoded request in the Objects fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureRequest {
    /// Stable fixture filename stem.
    pub name: &'static str,
    /// Fully-qualified generated message identity.
    pub message: &'static str,
    /// Canonical protobuf bytes.
    pub bytes: Vec<u8>,
}

/// Stable request/result fixture for local provider qualification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectsFixture {
    /// Encoded requests consumed by a transport runner.
    pub requests: Vec<FixtureRequest>,
    /// Semantic values stable across providers.
    pub expected: Value,
}

/// Receipt emitted after a bounded in-memory put/get execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectsReceipt {
    /// Stable scenario identity.
    pub scenario_id: &'static str,
    /// Local execution result.
    pub status: &'static str,
    /// Scope of evidence; no remote transport is implied.
    pub scope: &'static str,
    /// Number of bytes returned by the bounded get.
    pub body_size: usize,
}

fn put_header(bucket: wire::BucketRef) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: Some(bucket),
        object_key: OBJECT_KEY.to_owned(),
        ..Default::default()
    }
}

fn get_request(bucket: wire::BucketRef) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: Some(bucket),
        object_key: OBJECT_KEY.to_owned(),
        ..Default::default()
    }
}

/// Returns the fixture's typed requests and stable semantic expectation.
#[must_use]
pub fn fixture() -> ObjectsFixture {
    let (_, bucket) = MemoryObjects::with_default_bucket();
    let put = put_header(bucket.clone());
    let get = get_request(bucket);
    ObjectsFixture {
        requests: vec![
            FixtureRequest {
                name: "put-header",
                message: "acyclic.objects.v2.PutObjectHeader",
                bytes: put.encode_to_vec(),
            },
            FixtureRequest {
                name: "get-request",
                message: "acyclic.objects.v2.GetObjectRequest",
                bytes: get.encode_to_vec(),
            },
        ],
        expected: json!({
            "kind": "local-provider-result",
            "accepted": true,
            "route": ROUTE,
            "object_key": OBJECT_KEY,
            "body": "hello",
            "body_size": BODY.len(),
            "maximum_bytes": 1024,
        }),
    }
}

/// Executes the scenario against the real bounded in-memory Objects provider.
pub async fn execute() -> Result<ObjectsReceipt, Box<dyn std::error::Error + Send + Sync>> {
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    provider
        .put(put_header(bucket.clone()), Bytes::from_static(BODY))
        .await?;
    let object = provider.get(get_request(bucket), 1024).await?;
    if object.body != BODY {
        return Err(format!("unexpected Objects body: {:?}", object.body).into());
    }
    Ok(ObjectsReceipt {
        scenario_id: SCENARIO_ID,
        status: "qualified",
        scope: "rust-memory-provider",
        body_size: object.body.len(),
    })
}

/// Rust snippet rendered into the source-bound examples bundle.
#[must_use]
pub fn rust_snippet() -> &'static str {
    r#"// capability: supported
use acyclic_objects::{wire, MemoryObjects, ObjectsProvider};
use bytes::Bytes;

let (provider, bucket) = MemoryObjects::with_default_bucket();
provider.put(wire::PutObjectHeader {
    bucket: Some(bucket.clone()),
    object_key: "hello.txt".into(),
    ..Default::default()
}, Bytes::from_static(b"hello")).await?;
let object = provider.get(wire::GetObjectRequest {
    bucket: Some(bucket),
    object_key: "hello.txt".into(),
    ..Default::default()
}, 1024).await?;
assert_eq!(object.body, Bytes::from_static(b"hello"));"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_provider_put_get_is_bounded_and_source_owned() {
        let receipt = execute().await.expect("Objects provider scenario");
        assert_eq!(receipt.scenario_id, SCENARIO_ID);
        assert_eq!(receipt.status, "qualified");
        assert_eq!(receipt.scope, "rust-memory-provider");
        assert_eq!(receipt.body_size, BODY.len());
    }

    #[test]
    fn fixture_is_canonical_and_has_no_placeholders() {
        let fixture = fixture();
        assert_eq!(fixture.requests.len(), 2);
        let put = wire::PutObjectHeader::decode(fixture.requests[0].bytes.as_slice())
            .expect("PUT header must be canonical protobuf");
        assert_eq!(put.object_key, OBJECT_KEY);
        let get = wire::GetObjectRequest::decode(fixture.requests[1].bytes.as_slice())
            .expect("GET request must be canonical protobuf");
        assert_eq!(get.object_key, OBJECT_KEY);
        assert_eq!(fixture.expected["accepted"], true);
        assert!(!rust_snippet().contains("TODO"));
        assert!(!rust_snippet().contains("{{receipt"));
    }
}
