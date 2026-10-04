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
use std::{
    path::{Path, PathBuf},
    sync::Arc,
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

const SEGMENT_MAGIC: &[u8] = b"ACYCLIC-OBJECT-SEGMENT\0\x02";
const SEGMENT_HEADER_BYTES: usize = SEGMENT_MAGIC.len() + 4;
const SEGMENT_RECORD_BYTES: usize = 32 + 8;
const MAXIMUM_SEGMENT_BODIES: usize = 1_024;

struct LocatedSegmentBody {
    path: PathBuf,
    offset: usize,
    length: usize,
}

/// Resolve the exact immutable Objects segment record for a pinned file.
///
/// The Filesystem facade deliberately keeps the Objects provider private. This
/// test-only physical probe therefore authenticates the provider's published
/// segment format directly: it identifies the BLAKE3 body record derived from
/// the requested bytes, checks the segment's content address, and verifies the
/// descriptor before returning its offset. It cannot accidentally select a
/// journal, metadata frame, or unrelated file containing the same bytes.
fn locate_segment_body(
    root: &Path,
    reference: &FileRef,
    expected_bytes: &[u8],
) -> Result<LocatedSegmentBody> {
    reference.descriptor().verify(expected_bytes)?;
    let expected_digest = *blake3::hash(expected_bytes).as_bytes();
    let expected_length = expected_bytes.len();
    let mut pending = vec![root.to_path_buf()];
    let mut matches = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path)
            .map_err(|error| Error::Storage(format!("read {}: {error}", path.display())))?
        {
            let entry = entry.map_err(|error| Error::Storage(error.to_string()))?;
            let file_type = entry
                .file_type()
                .map_err(|error| Error::Storage(error.to_string()))?;
            let child = entry.path();
            if file_type.is_dir() {
                pending.push(child);
                continue;
            }
            if !file_type.is_file()
                || child.extension().and_then(|value| value.to_str()) != Some("segment")
            {
                continue;
            }
            let physical = std::fs::read(&child)
                .map_err(|error| Error::Storage(format!("read {}: {error}", child.display())))?;
            if physical.len() < SEGMENT_HEADER_BYTES
                || physical.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC)
            {
                return Err(Error::Storage(format!(
                    "segment {} has an invalid header",
                    child.display()
                )));
            }
            let segment_id = blake3::hash(&physical);
            let expected_name = format!("{}.segment", segment_id.to_hex());
            if child.file_name().and_then(|value| value.to_str()) != Some(&expected_name) {
                return Err(Error::Storage(format!(
                    "segment {} is not content addressed",
                    child.display()
                )));
            }
            let count = u32::from_le_bytes(
                physical[SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES]
                    .try_into()
                    .map_err(|_| Error::Storage("segment count is truncated".into()))?,
            ) as usize;
            if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
                return Err(Error::Storage(format!(
                    "segment {} has an invalid body count",
                    child.display()
                )));
            }
            let table_bytes = count
                .checked_mul(SEGMENT_RECORD_BYTES)
                .ok_or_else(|| Error::Storage("segment table length overflowed".into()))?;
            let body_start = SEGMENT_HEADER_BYTES
                .checked_add(table_bytes)
                .ok_or_else(|| Error::Storage("segment body offset overflowed".into()))?;
            if physical.len() < body_start {
                return Err(Error::Storage(format!(
                    "segment {} has a truncated body table",
                    child.display()
                )));
            }
            let mut offset = body_start;
            for index in 0..count {
                let record_start = SEGMENT_HEADER_BYTES + index * SEGMENT_RECORD_BYTES;
                let digest: [u8; 32] = physical[record_start..record_start + 32]
                    .try_into()
                    .map_err(|_| Error::Storage("segment digest is truncated".into()))?;
                let length = u64::from_le_bytes(
                    physical[record_start + 32..record_start + SEGMENT_RECORD_BYTES]
                        .try_into()
                        .map_err(|_| Error::Storage("segment length is truncated".into()))?,
                );
                let length = usize::try_from(length)
                    .map_err(|_| Error::Storage("segment body length is too large".into()))?;
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| Error::Storage("segment body range overflowed".into()))?;
                if end > physical.len() {
                    return Err(Error::Storage(format!(
                        "segment {} has a truncated body",
                        child.display()
                    )));
                }
                if digest == expected_digest
                    && length == expected_length
                    && &physical[offset..end] == expected_bytes
                {
                    matches.push(LocatedSegmentBody {
                        path: child.clone(),
                        offset,
                        length,
                    });
                }
                offset = end;
            }
            if offset != physical.len() {
                return Err(Error::Storage(format!(
                    "segment {} has trailing bytes",
                    child.display()
                )));
            }
        }
    }
    match matches.len() {
        1 => Ok(matches.pop().expect("one segment match")),
        count => Err(Error::Storage(format!(
            "expected one segment body for {} bytes, found {count}",
            expected_length
        ))),
    }
}

fn overwrite_segment_body(location: LocatedSegmentBody, replacement: &[u8]) -> Result<()> {
    if replacement.len() != location.length {
        return Err(Error::Invalid(
            "physical replacement changed body length".into(),
        ));
    }
    let mut physical = std::fs::read(&location.path)
        .map_err(|error| Error::Storage(format!("read {}: {error}", location.path.display())))?;
    let end = location
        .offset
        .checked_add(location.length)
        .ok_or_else(|| Error::Storage("physical body range overflowed".into()))?;
    if physical.get(location.offset..end).is_none() {
        return Err(Error::Storage(
            "physical body disappeared before corruption".into(),
        ));
    }
    physical[location.offset..end].copy_from_slice(replacement);
    std::fs::write(&location.path, physical)
        .map_err(|error| Error::Storage(format!("write {}: {error}", location.path.display())))?;
    Ok(())
}

fn delete_segment_body(location: LocatedSegmentBody) -> Result<()> {
    std::fs::remove_file(&location.path)
        .map_err(|error| Error::Storage(format!("delete {}: {error}", location.path.display())))
}

#[tokio::test]
async fn local_storage_rejects_physical_corruption_and_deletion_after_restart() -> Result<()> {
    let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "residency", "1", serde_json::json!({}))?;
    // Bodies larger than the Objects inline threshold are persisted in an
    // authenticated immutable segment, which lets this test target the exact
    // physical record for the requested FileRef.
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

    let filesystem_root = root.path().join("filesystem");
    let body = locate_segment_body(&filesystem_root, &original, &original_bytes)?;
    let replacement = vec![0xa5; original_bytes.len()];
    overwrite_segment_body(body, &replacement)?;

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

    let deleted_filesystem_root = deleted_root.path().join("filesystem");
    let deleted_body = locate_segment_body(&deleted_filesystem_root, &deleted, &deleted_bytes)?;
    delete_segment_body(deleted_body)?;
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
