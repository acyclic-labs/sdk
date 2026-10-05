//! Frozen native model-input vectors used by the cross-target qualification lane.
//!
//! The vector contains the exact canonical request and manifest bytes captured
//! from the provider-neutral request.  This test deliberately exercises the
//! Rust builder directly; a WASM wrapper or a separately reconstructed digest
//! cannot stand in for native admission.

use acyclic_harness::{
    Result,
    conversation::Limits,
    model::{
        ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelOptionPolicy, ModelProvider,
        ModelRequest,
    },
    model_input::{FrozenModelPrefix, PrefixBoundModelProvider, PreparedModelInput},
};
use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, Mutex};

const VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-v3.json"
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

/// A provider used only to prove the production provider boundary receives the
/// frozen request bytes.  It deliberately records the bytes from the
/// `PreparedModelInput` passed to `generate`; the test does not compare an
/// earlier fixture or a separately reconstructed request.
struct CapturingProvider {
    policy: ModelOptionPolicy,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ModelProvider for CapturingProvider {
    fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
        Some(&self.policy)
    }

    fn generate<'a>(&'a self, prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        let result = self
            .requests
            .lock()
            .map(|mut requests| requests.push(prepared.bytes().to_vec()))
            .map_err(|_| acyclic_harness::Error::Storage("capture lock poisoned".into()))
            .map(|()| ModelEvent::Completed {
                metadata: json!({"fixture": "captured"}),
            });
        Box::pin(futures::stream::iter([result]))
    }

    fn reconcile_admitted<'a>(
        &'a self,
        _: PreparedModelInput,
        _: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn production_provider_receives_exact_recursive_requests() -> Result<()> {
    let vector = parse_vector();
    let root = prepare(&vector.root, vector.limits, &vector.policy)?;
    let root_prefix = FrozenModelPrefix::capture(&root, vector.root.prefix_message_count)?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(CapturingProvider {
        policy: vector.policy.clone(),
        requests: requests.clone(),
    });
    let child_provider =
        PrefixBoundModelProvider::new(root_prefix.clone(), vector.limits, provider.clone())?;

    // Both siblings pass through the real prefix-enforcing provider boundary
    // concurrently. Their provider requests still retain the same immutable
    // inherited bytes even though each child has a private suffix.
    let sibling_streams = vector
        .children
        .iter()
        .map(|child| {
            Ok(child_provider.generate(prepare(child, vector.limits, &vector.policy)?))
        })
        .collect::<Result<Vec<_>>>()?;
    futures::future::join_all(sibling_streams.into_iter().map(async |mut stream| {
        while let Some(event) = stream.next().await {
            event?;
        }
        Ok::<(), acyclic_harness::Error>(())
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    // A grandchild is checked against the first child's retained prefix.  Its
    // parent provider is still the same production adapter and the capture is
    // the exact bytes it receives.
    let first_child = prepare(&vector.children[0], vector.limits, &vector.policy)?;
    let first_child_prefix =
        FrozenModelPrefix::capture(&first_child, vector.grandchild.prefix_message_count)?;
    let grandchild_provider =
        PrefixBoundModelProvider::new(first_child_prefix, vector.limits, provider)?;
    let mut stream =
        grandchild_provider.generate(prepare(&vector.grandchild, vector.limits, &vector.policy)?);
    while let Some(event) = stream.next().await {
        event?;
    }

    let captured = requests
        .lock()
        .map_err(|_| acyclic_harness::Error::Storage("capture lock poisoned".into()))?
        .clone();
    assert_eq!(captured.len(), 3);
    assert!(captured.contains(
        &vector.children[0].expected.request_json.as_bytes().to_vec()
    ));
    assert!(captured.contains(
        &vector.children[1].expected.request_json.as_bytes().to_vec()
    ));
    assert_eq!(
        captured[2],
        vector.grandchild.expected.request_json.as_bytes()
    );

    // The parent may continue independently after the fork.  Mutating its
    // history, model settings, and a referenced file identity cannot rewrite
    // any already captured child request.
    let mut changed_parent = vector.root.request;
    changed_parent.model.revision = "parent-after-fork".into();
    changed_parent.messages[0] = serde_json::from_value(json!({
        "role": "system",
        "content": "parent changed after fork"
    }))
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let ModelContent::Parts(parts) = &mut changed_parent.messages[1].content else {
        return Err(acyclic_harness::Error::Invalid(
            "fixture attachment message is not multipart".into(),
        ));
    };
    let Some(ModelContentPart::File { file, .. }) = parts
        .iter_mut()
        .find(|part| matches!(part, ModelContentPart::File { .. }))
    else {
        return Err(acyclic_harness::Error::Invalid(
            "fixture attachment message has no file".into(),
        ));
    };
    let changed_file = acyclic_harness::conversation::FileRef::new(
        file.volume().clone(),
        format!("parent-after-fork/{}", file.path()),
        file.version().to_owned(),
        file.descriptor().clone(),
        file.display_name().to_owned(),
    )?;
    *file = changed_file;
    let changed_parent = PreparedModelInput::prepare_with_policy(
        changed_parent,
        vector.limits,
        Some(&vector.policy),
    )?;
    assert!(
        root_prefix
            .verify(&prepare(
                &vector.children[0],
                vector.limits,
                &vector.policy,
            )?)
            .is_ok()
    );
    assert_ne!(changed_parent.bytes(), captured[0]);
    assert_eq!(
        captured[0],
        vector.children[0].expected.request_json.as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn reopened_prefix_replays_child_bytes_after_parent_changes() -> Result<()> {
    let vector = parse_vector();
    let root = prepare(&vector.root, vector.limits, &vector.policy)?;
    let prefix = FrozenModelPrefix::capture(&root, vector.root.prefix_message_count)?;
    let persisted = serde_json::to_vec(&prefix)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let reopened: FrozenModelPrefix = serde_json::from_slice(&persisted)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, prefix);

    // Continue the parent independently after the fork. These changes are
    // intentionally outside the persisted child prefix.
    let mut changed_parent = vector.root.request;
    changed_parent.model.options["mode"] = json!("creative");
    changed_parent.messages[0] = serde_json::from_value(json!({
        "role": "system",
        "content": "parent changed after persisted fork"
    }))
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let changed_parent = PreparedModelInput::prepare_with_policy(
        changed_parent,
        vector.limits,
        Some(&vector.policy),
    )?;
    assert!(prefix.verify(&changed_parent).is_err());

    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(CapturingProvider {
        policy: vector.policy.clone(),
        requests: requests.clone(),
    });
    let child_provider = PrefixBoundModelProvider::new(reopened, vector.limits, provider)?;
    let mut stream = child_provider.generate(prepare(
        &vector.children[0],
        vector.limits,
        &vector.policy,
    )?);
    while let Some(event) = stream.next().await {
        event?;
    }
    let captured = requests
        .lock()
        .map_err(|_| acyclic_harness::Error::Storage("capture lock poisoned".into()))?;
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0],
        vector.children[0].expected.request_json.as_bytes()
    );
    Ok(())
}

#[test]
fn prefix_admission_rejects_duplicate_reordered_and_revised_tools() -> Result<()> {
    let vector = parse_vector();
    let root = prepare(&vector.root, vector.limits, &vector.policy)?;
    let prefix = FrozenModelPrefix::capture(&root, vector.root.prefix_message_count)?;

    let mut duplicate = vector.root.request.clone();
    duplicate.tools.push(duplicate.tools[0].clone());
    assert!(matches!(
        PreparedModelInput::prepare_with_policy(duplicate, vector.limits, Some(&vector.policy)),
        Err(acyclic_harness::Error::Invalid(message))
            if message.contains("duplicate model tool definition")
    ));

    let mut extra = vector.root.request.tools[0].clone();
    extra.name = "unused_tool".into();
    extra.revision = "unused-tool-1".into();
    let mut reordered = vector.root.request.clone();
    reordered.tools.insert(0, extra);
    let reordered = PreparedModelInput::prepare_with_policy(
        reordered,
        vector.limits,
        Some(&vector.policy),
    )?;
    assert_eq!(reordered.request().tools[0].name, "unused_tool");
    assert_eq!(reordered.request().tools[1].name, "read_file");
    assert_ne!(root.manifest().request_digest, reordered.manifest().request_digest);
    assert_ne!(root.manifest().binding_digest, reordered.manifest().binding_digest);
    assert!(prefix.verify(&reordered).is_err());

    let mut revised = vector.root.request.clone();
    revised.tools[0].revision = "read-file-8".into();
    let revised = PreparedModelInput::prepare_with_policy(
        revised,
        vector.limits,
        Some(&vector.policy),
    )?;
    assert_ne!(root.manifest().request_digest, revised.manifest().request_digest);
    assert_ne!(root.manifest().binding_digest, revised.manifest().binding_digest);
    assert!(prefix.verify(&revised).is_err());
    Ok(())
}
