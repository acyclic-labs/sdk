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

#[cfg(feature = "test-support")]
use acyclic_fs::test_support::{
    corrupt_segment_body_for_test, delete_segment_for_test, locate_segment_body_for_test,
};

const VECTOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/model-input-v3.json"
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
    fn output_token_limit_for_bytes(&self, max_output_bytes: u64) -> Option<u32> {
        u32::try_from(max_output_bytes).ok().filter(|bound| *bound > 0)
    }

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

#[cfg(feature = "test-support")]
#[tokio::test]
async fn local_storage_rejects_physical_corruption_and_deletion_after_restart() -> Result<()> {
    let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "residency", "1", serde_json::json!({}))?;
    // Bodies larger than the Objects inline threshold are persisted in an
    // authenticated immutable segment. This fixture stages one occurrence of
    // the body; the owning probe rejects ambiguous physical duplicates.
    let original_bytes = vec![b'p'; 64 * 1_024 + 1];
    let session = PersistentLocalHarness::open(
        root.path(),
        model.clone(),
        Arc::new(NoopProvider),
        Limits::default(),
    )
    .await?;
    let original = session
        .storage()
        .stage(
            acyclic_harness::OperationId::new(),
            "src/physical-corrupt.txt",
            &original_bytes,
            "text/plain",
            "physical-corrupt.txt",
        )
        .await?;
    session
        .storage()
        .content_verifier()
        .verify(&original)
        .await?;
    drop(session);

    let objects_root = root.path().join("filesystem").join("objects");
    original.descriptor().verify(&original_bytes)?;
    let digest = *blake3::hash(&original_bytes).as_bytes();
    let body = locate_segment_body_for_test(&objects_root, &digest, &original_bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let replacement = vec![0xa5; original_bytes.len()];
    corrupt_segment_body_for_test(&body, &replacement)
        .map_err(|error| Error::Storage(error.to_string()))?;

    let reopened = PersistentLocalHarness::open(
        root.path(),
        model.clone(),
        Arc::new(NoopProvider),
        Limits::default(),
    )
    .await;
    if let Ok(reopened) = reopened {
        assert!(
            reopened
                .storage()
                .content_verifier()
                .verify(&original)
                .await
                .is_err()
        );
        drop(reopened);
    } else if let Err(error) = reopened {
        assert!(
            matches!(
                error,
                Error::Storage(_) | Error::Invalid(_) | Error::Conflict(_)
            ),
            "physical corruption must fail closed with a typed storage outcome: {error:?}"
        );
    }

    // A physical corruption may make the object provider fail closed while it
    // reopens. Use an independent root for deletion so both outcomes are
    // exercised without repairing the first damaged store.
    let deleted_root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let session = PersistentLocalHarness::open(
        deleted_root.path(),
        model.clone(),
        Arc::new(NoopProvider),
        Limits::default(),
    )
    .await?;
    let deleted_bytes = vec![b'd'; 64 * 1_024 + 1];
    let deleted = session
        .storage()
        .stage(
            acyclic_harness::OperationId::new(),
            "src/physical-deleted.txt",
            &deleted_bytes,
            "text/plain",
            "physical-deleted.txt",
        )
        .await?;
    session
        .storage()
        .content_verifier()
        .verify(&deleted)
        .await?;
    drop(session);

    let deleted_objects_root = deleted_root.path().join("filesystem").join("objects");
    deleted.descriptor().verify(&deleted_bytes)?;
    let deleted_digest = *blake3::hash(&deleted_bytes).as_bytes();
    let deleted_body =
        locate_segment_body_for_test(&deleted_objects_root, &deleted_digest, &deleted_bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
    delete_segment_for_test(deleted_body).map_err(|error| Error::Storage(error.to_string()))?;
    let reopened = PersistentLocalHarness::open(
        deleted_root.path(),
        model,
        Arc::new(NoopProvider),
        Limits::default(),
    )
    .await;
    if let Ok(reopened) = reopened {
        assert!(
            reopened
                .storage()
                .content_verifier()
                .verify(&deleted)
                .await
                .is_err()
        );
    } else if let Err(error) = reopened {
        assert!(
            matches!(
                error,
                Error::Storage(_) | Error::Invalid(_) | Error::Conflict(_)
            ),
            "physical deletion must fail closed with a typed storage outcome: {error:?}"
        );
    }
    Ok(())
}
