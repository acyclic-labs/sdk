//! Frozen native model-input vectors used by the cross-target qualification lane.
//!
//! The vector contains the exact canonical request and manifest bytes captured
//! from the provider-neutral request.  This test deliberately exercises the
//! Rust builder directly; a WASM wrapper or a separately reconstructed digest
//! cannot stand in for native admission.

use acyclic_harness::{
    Result,
    conversation::Limits,
    model::{ModelOptionPolicy, ModelRequest},
    model_input::{FrozenModelPrefix, PreparedModelInput},
};
use serde::Deserialize;

const VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../conformance/vectors/harness/model-input-v3.json"
));

#[derive(Debug, Deserialize)]
struct Vector {
    version: u32,
    limits: Limits,
    policy: ModelOptionPolicy,
    root: Case,
    children: Vec<Case>,
    grandchild: Case,
}

#[derive(Debug, Deserialize)]
struct Case {
    #[serde(default)]
    name: Option<String>,
    prefix_message_count: usize,
    request: ModelRequest,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct Expected {
    request_json: String,
    manifest_json: String,
    request_digest: [u8; 32],
    binding_digest: [u8; 32],
    manifest_digest: [u8; 32],
    prefix_digest: [u8; 32],
}

fn parse_vector() -> Vector {
    serde_json::from_str(VECTOR).expect("model-input-v3 fixture must decode")
}

fn prepare(case: &Case, limits: Limits, policy: &ModelOptionPolicy) -> Result<PreparedModelInput> {
    let prepared =
        PreparedModelInput::prepare_with_policy(case.request.clone(), limits, Some(policy))?;
    assert_eq!(
        prepared.bytes(),
        case.expected.request_json.as_bytes(),
        "{} prepared bytes",
        case.name.as_deref().unwrap_or("root")
    );
    let manifest_bytes = prepared.manifest_bytes()?;
    assert_eq!(
        manifest_bytes,
        case.expected.manifest_json.as_bytes(),
        "{} manifest bytes",
        case.name.as_deref().unwrap_or("root")
    );
    assert_eq!(
        prepared.manifest().request_digest,
        case.expected.request_digest,
        "{} request digest",
        case.name.as_deref().unwrap_or("root")
    );
    assert_eq!(
        prepared.manifest().binding_digest,
        case.expected.binding_digest,
        "{} binding digest",
        case.name.as_deref().unwrap_or("root")
    );
    assert_eq!(
        *blake3::hash(&manifest_bytes).as_bytes(),
        case.expected.manifest_digest,
        "{} manifest digest",
        case.name.as_deref().unwrap_or("root")
    );
    acyclic_harness::model_input::validate_manifest(
        case.request.clone(),
        limits,
        Some(policy),
        &case.expected.manifest_json,
    )?;
    Ok(prepared)
}

#[test]
fn native_prepared_model_input_matches_frozen_request_and_manifest() -> Result<()> {
    let vector = parse_vector();
    assert_eq!(vector.version, 3);
    let root = prepare(&vector.root, vector.limits, &vector.policy)?;
    let root_prefix = FrozenModelPrefix::capture(&root, vector.root.prefix_message_count)?;
    assert_eq!(root_prefix.digest(), vector.root.expected.prefix_digest);
    assert_eq!(
        root_prefix.message_bytes().len(),
        vector.root.prefix_message_count
    );

    let mut prepared_children = Vec::new();
    for child in &vector.children {
        let child_prepared = prepare(child, vector.limits, &vector.policy)?;
        root_prefix.verify(&child_prepared)?;
        let child_prefix = FrozenModelPrefix::capture(&child_prepared, child.prefix_message_count)?;
        assert_eq!(child_prefix.digest(), child.expected.prefix_digest);
        assert_eq!(child_prefix.message_bytes(), root_prefix.message_bytes());
        assert!(child_prepared.request().messages.len() > child.prefix_message_count);
        prepared_children.push(child_prepared);
    }
    assert_eq!(
        prepared_children.len(),
        2,
        "the fixture must retain two sibling requests"
    );
    assert_eq!(
        prepared_children[0].request().messages[..vector.root.prefix_message_count],
        prepared_children[1].request().messages[..vector.root.prefix_message_count],
        "sibling inherited prefixes must be byte-identical"
    );
    assert_ne!(
        prepared_children[0].request().messages[vector.root.prefix_message_count..],
        prepared_children[1].request().messages[vector.root.prefix_message_count..],
        "sibling suffixes must remain private"
    );

    let grandchild = prepare(&vector.grandchild, vector.limits, &vector.policy)?;
    let child_prefix = FrozenModelPrefix::capture(
        &prepared_children[0],
        vector.grandchild.prefix_message_count,
    )?;
    child_prefix.verify(&grandchild)?;
    assert_eq!(
        child_prefix.digest(),
        vector.grandchild.expected.prefix_digest
    );
    assert_eq!(
        grandchild.request().messages.len(),
        vector.grandchild.prefix_message_count + 2
    );
    Ok(())
}

#[test]
fn native_conformance_rejects_forged_or_reformatted_manifest() -> Result<()> {
    let vector = parse_vector();
    let prepared = PreparedModelInput::prepare_with_policy(
        vector.root.request.clone(),
        vector.limits,
        Some(&vector.policy),
    )?;
    let mut manifest: serde_json::Value = serde_json::from_str(&vector.root.expected.manifest_json)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    manifest["version"] = serde_json::json!(2);
    let forged = serde_json::to_string(&manifest)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert!(
        acyclic_harness::model_input::validate_manifest(
            vector.root.request.clone(),
            vector.limits,
            Some(&vector.policy),
            &forged,
        )
        .is_err()
    );
    let noncanonical = format!(" {}", vector.root.expected.manifest_json);
    assert!(
        acyclic_harness::model_input::validate_manifest(
            prepared.into_request(),
            vector.limits,
            Some(&vector.policy),
            &noncanonical,
        )
        .is_err()
    );
    Ok(())
}
