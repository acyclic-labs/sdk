#![allow(clippy::too_many_lines)]

//! In-crate cold-storage checks for exact model-input journal references.
//!
//! These tests reopen the real LocalStream/LocalFs composition, then inject a
//! typed failure after the selected `FileRef` has been loaded and verified by
//! the real object store. They prove exact replay reaches the pinned request
//! or manifest and fails before provider dispatch. The injected failure is a
//! journal-boundary test seam; it is not a claim that the physical object was
//! deleted or corrupted. Physical LocalFs integrity faults require separate
//! provider-level coverage.

use crate::{
    Error, OperationId, Result,
    conversation::{Attachment, FileRef, Limits},
    executor::{ExecutionEvent, ExecutionJournal},
    filesystem::{PersistentLocalHarness, execution_journal::JournalLoadFault},
    model::{Model, ModelAttempt, ModelEvent, ModelProvider},
    model_input::PreparedModelInput,
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tempfile::{tempdir, TempDir};

const OBJECT_BODY_LIMIT: usize = 64 * 1024;
const ATTACHMENT_COUNT: usize = 900;
const PROMPT: &str = "cold storage exact input";

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

struct Fixture {
    root: TempDir,
    model: Model,
    limits: Limits,
    operation: OperationId,
    content: FileRef,
    attachments: Vec<Attachment>,
    request: FileRef,
    manifest: FileRef,
    survivor: FileRef,
}

async fn large_input_fixture() -> Result<Fixture> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("test", "local-storage", "1", serde_json::json!({}))?;
    let limits = Limits::default();
    assert!(
        limits.attachments >= ATTACHMENT_COUNT,
        "default attachment limit must admit the large-input fixture"
    );
    let provider = Arc::new(CountingProvider::default());
    let session =
        PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits).await?;
    let operation = OperationId::from_bytes([0x71; 16]);
    let content = session
        .storage()
        .stage(
            operation,
            &format!("turns/{operation}/user.txt"),
            PROMPT.as_bytes(),
            "text/plain",
            "prompt.txt",
        )
        .await?;
    let survivor = session
        .storage()
        .stage(
            OperationId::new(),
            "unrelated/survivor.txt",
            b"unrelated durable content",
            "text/plain",
            "survivor.txt",
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
        .run_conversation(
            session.bundle(),
            operation,
            content.clone(),
            attachments.clone(),
            1,
        )
        .await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let records = session.storage().journal().replay(operation).await?;
    let (manifest, request) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelInputPrepared {
                manifest, request, ..
            } => Some((manifest.clone(), request.clone())),
            _ => None,
        })
        .ok_or_else(|| Error::Storage("model input preparation record is missing".into()))?;
    let journal = session.storage().journal();
    let request_bytes = journal.load(&request).await?;
    let manifest_bytes = journal.load(&manifest).await?;
    assert!(
        request_bytes.len() > OBJECT_BODY_LIMIT,
        "request must use a separately persisted body"
    );
    assert!(
        manifest_bytes.len() > OBJECT_BODY_LIMIT,
        "manifest must use a separately persisted body"
    );
    drop(session);
    Ok(Fixture {
        root,
        model,
        limits,
        operation,
        content,
        attachments,
        request,
        manifest,
        survivor,
    })
}

#[derive(Clone, Copy)]
enum Target {
    Request,
    Manifest,
}

async fn exercise_fault(target: Target, fault: JournalLoadFault) -> Result<()> {
    let fixture = large_input_fixture().await?;
    let provider = Arc::new(CountingProvider::default());
    let session = PersistentLocalHarness::open(
        fixture.root.path(),
        fixture.model.clone(),
        provider.clone(),
        fixture.limits,
    )
    .await?;

    // The real LocalFs object store remains usable after reopening, including
    // for a ref outside the execution journal being faulted below.
    assert_eq!(
        session.storage().read(&fixture.survivor).await?,
        b"unrelated durable content"
    );
    let reference = match target {
        Target::Request => fixture.request.clone(),
        Target::Manifest => fixture.manifest.clone(),
    };
    session
        .storage()
        .inject_journal_load_fault(reference, fault)?;
    let error = match session
        .storage()
        .run_conversation(
            session.bundle(),
            fixture.operation,
            fixture.content.clone(),
            fixture.attachments.clone(),
            1,
        )
        .await
    {
        Ok(_) => {
            return Err(Error::Invalid(
                "faulted exact model-input replay unexpectedly succeeded".into(),
            ));
        }
        Err(error) => error,
    };
    match fault {
        JournalLoadFault::Missing => assert!(
            matches!(&error, Error::NotFound(_)),
            "missing exact journal content must remain typed: {error}"
        ),
        JournalLoadFault::Corrupt => assert!(
            matches!(&error, Error::Invalid(_)),
            "corrupt exact journal content must remain typed: {error}"
        ),
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn injected_missing_request_is_rejected_before_provider_dispatch() -> Result<()> {
    exercise_fault(Target::Request, JournalLoadFault::Missing).await
}

#[tokio::test]
async fn injected_corrupt_request_is_rejected_before_provider_dispatch() -> Result<()> {
    exercise_fault(Target::Request, JournalLoadFault::Corrupt).await
}

#[tokio::test]
async fn injected_missing_manifest_is_rejected_before_provider_dispatch() -> Result<()> {
    exercise_fault(Target::Manifest, JournalLoadFault::Missing).await
}

#[tokio::test]
async fn injected_corrupt_manifest_is_rejected_before_provider_dispatch() -> Result<()> {
    exercise_fault(Target::Manifest, JournalLoadFault::Corrupt).await
}
