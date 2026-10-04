//! Native rejection and storage-residency cases for the model-input boundary.
//!
//! These cases deliberately cross the real local content verifier for missing,
//! corrupt, and stale generation references. Model-input admission itself stays
//! provider-neutral: it validates tool exchanges and records references, while
//! the host verifier proves that the referenced bytes are still present and
//! match their pinned descriptor.

#![cfg(feature = "filesystem-local")]

use acyclic_harness::{
    Error, Result,
    conversation::{FileDescriptor, FileRef, Limits},
    filesystem::PersistentLocalHarness,
    model::{
        Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelProvider,
        ModelRequest,
    },
    model_input::PreparedModelInput,
};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::Deserialize;
use std::sync::Arc;

const VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../conformance/vectors/harness/model-input-v3.json"
));

#[derive(Debug, Deserialize)]
struct Vector {
    limits: Limits,
    policy: acyclic_harness::model::ModelOptionPolicy,
    root: Root,
}

#[derive(Debug, Deserialize)]
struct Root {
    request: ModelRequest,
}

struct NoopProvider;

impl ModelProvider for NoopProvider {
    fn generate<'a>(&'a self, _: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        Box::pin(futures::stream::empty())
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn fixture() -> Vector {
    serde_json::from_str(VECTOR).expect("model-input-v3 fixture must decode")
}

#[test]
fn native_admission_rejects_invalid_tool_arguments_and_results() -> Result<()> {
    let vector = fixture();

    let mut invalid_arguments = vector.root.request.clone();
    let ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) =
        &mut invalid_arguments.messages[2].content
    else {
        return Err(Error::Invalid(
            "fixture tool call is not a single part".into(),
        ));
    };
    *arguments = serde_json::json!({
        "path": 42,
        "whitespace": "  preserve  "
    });
    assert!(
        PreparedModelInput::prepare_with_policy(
            invalid_arguments,
            vector.limits,
            Some(&vector.policy),
        )
        .is_err()
    );

    let mut invalid_result = vector.root.request;
    let ModelContent::Part(ModelContentPart::ToolResult { value, .. }) =
        &mut invalid_result.messages[3].content
    else {
        return Err(Error::Invalid(
            "fixture tool result is not a single part".into(),
        ));
    };
    *value = serde_json::json!({
        "bytes": "17",
        "text": "résultat\r\n"
    });
    assert!(
        PreparedModelInput::prepare_with_policy(
            invalid_result,
            vector.limits,
            Some(&vector.policy),
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn local_storage_rejects_missing_corrupt_and_stale_file_references() -> Result<()> {
    let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "residency", "1", serde_json::json!({}))?;
    let session = PersistentLocalHarness::open(
        root.path(),
        model,
        Arc::new(NoopProvider),
        Limits::default(),
    )
    .await?;
    let original_bytes = b"generation-pinned\r\n";
    let original = session
        .storage()
        .stage(
            acyclic_harness::OperationId::new(),
            "src/pinned.txt",
            original_bytes,
            "text/plain",
            "pinned.txt",
        )
        .await?;
    let verifier = session.storage().content_verifier();
    verifier.verify(&original).await?;

    let missing = FileRef::new(
        original.volume().clone(),
        "src/missing.txt",
        original.version(),
        original.descriptor().clone(),
        "missing.txt",
    )?;
    assert!(verifier.verify(&missing).await.is_err());

    let corrupt = FileRef::new(
        original.volume().clone(),
        original.path(),
        original.version(),
        FileDescriptor::from_bytes(b"different bytes", "text/plain")?,
        original.display_name(),
    )?;
    assert!(verifier.verify(&corrupt).await.is_err());

    let replacement = session
        .storage()
        .stage(
            acyclic_harness::OperationId::new(),
            original.path(),
            b"replacement bytes",
            "text/plain",
            original.display_name(),
        )
        .await?;
    assert_ne!(original.version(), replacement.version());
    assert_eq!(session.storage().read(&original).await?, original_bytes);
    assert_eq!(
        session.storage().read(&replacement).await?,
        b"replacement bytes"
    );

    let stale_descriptor = FileRef::new(
        replacement.volume().clone(),
        replacement.path(),
        replacement.version(),
        original.descriptor().clone(),
        replacement.display_name(),
    )?;
    assert!(verifier.verify(&stale_descriptor).await.is_err());
    Ok(())
}
