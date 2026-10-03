#![cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#![allow(clippy::too_many_lines)]

//! Cold-storage checks for the exact model-input journal references.
//!
//! These cases deliberately mutate the LocalObjects files after the producing
//! harness has been dropped.  They are intended to prove that a missing or
//! corrupted request artifact is rejected before a fresh model provider can
//! be called.

use acyclic_harness::{
    Error, OperationId, Result,
    conversation::{Attachment, Limits},
    executor::{ExecutionEvent, ExecutionJournal},
    filesystem::PersistentLocalHarness,
    model::{Model, ModelAttempt, ModelEvent, ModelProvider},
    model_input::PreparedModelInput,
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
};
use tempfile::{TempDir, tempdir};

const OBJECT_BODY_LIMIT: usize = 64 * 1024;
const ATTACHMENT_COUNT: usize = 900;

#[derive(Default)]
struct CountingProvider {
    calls: AtomicUsize,
}

impl ModelProvider for CountingProvider {
    fn generate<'a>(&'a self, _: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(stream::iter([Ok(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        })]))
    }

    fn reconcile<'a>(
        &'a self,
        _: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

async fn large_input_fixture() -> Result<(TempDir, Model, Limits, Vec<u8>, Vec<u8>)> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("test", "local-storage", "1", serde_json::json!({}))?;
    let limits = Limits::default();
    let provider = Arc::new(CountingProvider::default());
    let session = PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits)
        .await?;
    let operation = OperationId::from_bytes([0x71; 16]);
    let content = session
        .storage()
        .stage(
            operation,
            "turns/input-storage/user.txt",
            b"cold storage exact input",
            "text/plain",
            "prompt.txt",
        )
        .await?;
    let mut attachments = Vec::with_capacity(ATTACHMENT_COUNT);
    for index in 0..ATTACHMENT_COUNT {
        let path = format!(
            "turns/input-storage/attachments/{index:04}-{}",
            "p".repeat(180)
        );
        let file = session
            .storage()
            .stage(
                OperationId::new(),
                &path,
                b"attachment body",
                "text/plain",
                &format!("attachment-{index:04}-{}", "d".repeat(120)),
            )
            .await?;
        attachments.push(Attachment {
            file,
            label: Some(format!("label-{index:04}-{}", "l".repeat(120))),
        });
    }
    session
        .storage()
        .run_conversation(session.bundle(), operation, content, attachments, 1)
        .await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let records = session.storage().journal().replay(operation).await?;
    let (manifest_ref, request_ref) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelInputPrepared {
                manifest, request, ..
            } => Some((manifest.clone(), request.clone())),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("model input preparation record is missing".into()))?;
    let journal = session.storage().journal();
    let request_bytes = journal.load(&request_ref).await?;
    let manifest_bytes = journal.load(&manifest_ref).await?;
    assert!(
        request_bytes.len() > OBJECT_BODY_LIMIT,
        "request must use a separately persisted body"
    );
    assert!(
        manifest_bytes.len() > OBJECT_BODY_LIMIT,
        "manifest must use a separately persisted body"
    );
    drop(session);
    Ok((root, model, limits, request_bytes, manifest_bytes))
}

fn object_files(root: &Path) -> Result<Vec<PathBuf>> {
    fn walk(path: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                walk(&path, files)?;
            } else if path.is_file() {
                files.push(path);
            }
        }
        Ok(())
    }
    let objects = root.join("filesystem").join("objects");
    let mut files = Vec::new();
    walk(&objects, &mut files).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(files)
}

fn body_location(root: &Path, body: &[u8]) -> Result<(PathBuf, usize)> {
    for path in object_files(root)? {
        let bytes = fs::read(&path).map_err(|error| Error::Storage(error.to_string()))?;
        if let Some(offset) = bytes.windows(body.len()).position(|window| window == body) {
            return Ok((path, offset));
        }
    }
    Err(Error::Storage(
        "persisted model-input body was not found in LocalObjects storage".into(),
    ))
}

fn remove_body(root: &Path, body: &[u8]) -> Result<()> {
    let (path, _) = body_location(root, body)?;
    fs::remove_file(path).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

fn corrupt_body(root: &Path, body: &[u8]) -> Result<()> {
    let (path, offset) = body_location(root, body)?;
    let mut bytes = fs::read(&path).map_err(|error| Error::Storage(error.to_string()))?;
    let byte = bytes
        .get_mut(offset + body.len() / 2)
        .ok_or_else(|| Error::Storage("persisted model-input body offset is invalid".into()))?;
    *byte ^= 0x5a;
    fs::write(path, bytes).map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn missing_persisted_request_is_rejected_before_provider_dispatch() -> Result<()> {
    let (root, model, limits, request_bytes, _) = large_input_fixture().await?;
    remove_body(root.path(), &request_bytes)?;
    let provider = Arc::new(CountingProvider::default());
    assert!(
        PersistentLocalHarness::open(root.path(), model, provider.clone(), limits)
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn corrupted_persisted_manifest_is_rejected_before_provider_dispatch() -> Result<()> {
    let (root, model, limits, _, manifest_bytes) = large_input_fixture().await?;
    corrupt_body(root.path(), &manifest_bytes)?;
    let provider = Arc::new(CountingProvider::default());
    assert!(
        PersistentLocalHarness::open(root.path(), model, provider.clone(), limits)
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}
