//! Black-box smoke coverage for the default local Git-capable composition.
#![cfg(feature = "filesystem-local")]

use acyclic_harness::filesystem::PersistentLocalSwarm;
use acyclic_harness::model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider};
use acyclic_harness::{Error, Limits, OperationId, Result};
use futures::stream;
use futures::stream::BoxStream;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tempfile::tempdir;

struct CompletionProvider;

impl ModelProvider for CompletionProvider {
    fn generate<'a>(
        &'a self,
        _prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: "ready".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]))
    }
}

struct GitStatusProvider {
    calls: AtomicUsize,
    result_seen: AtomicBool,
}

impl GitStatusProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            result_seen: AtomicBool::new(false),
        })
    }
}

impl ModelProvider for GitStatusProvider {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request();
        let has_result = request.messages.iter().any(|message| {
            matches!(
                &message.content,
                ModelContent::Part(ModelContentPart::ToolResult { name, .. })
                    if name == "acyclic.git"
            )
        });
        if has_result {
            self.result_seen.store(true, Ordering::SeqCst);
        }
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let events = if first {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "default-git-status".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["status"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else {
            vec![
                Ok(ModelEvent::Content {
                    delta: "git-ready".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}

#[tokio::test]
async fn default_local_swarm_opens_with_the_git_facade_bound() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "git-default-smoke", "1", json!({}))?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model,
        Arc::new(CompletionProvider),
        Limits::default(),
    )
    .await?;

    let output = swarm
        .run_root(
            OperationId::from_bytes([0xD1; 16]),
            "verify default git composition",
        )
        .await?;
    assert_eq!(output.text, "ready");
    assert_eq!(swarm.sessions().await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn default_local_swarm_executes_git_status_and_reopens() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "git-default-status", "1", json!({}))?;
    let provider = GitStatusProvider::new();
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model.clone(),
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let output = swarm
        .run_root(
            OperationId::from_bytes([0xD2; 16]),
            "run the default git status check",
        )
        .await?;
    assert_eq!(output.text, "git-ready");
    assert!(provider.result_seen.load(Ordering::SeqCst));
    drop(swarm);

    let reopened_provider = GitStatusProvider::new();
    let reopened = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model,
        reopened_provider.clone(),
        Limits::default(),
    )
    .await?;
    let output = reopened
        .run_root(
            OperationId::from_bytes([0xD3; 16]),
            "repeat the default git status check after restart",
        )
        .await?;
    assert_eq!(output.text, "git-ready");
    assert!(reopened_provider.result_seen.load(Ordering::SeqCst));
    Ok(())
}
