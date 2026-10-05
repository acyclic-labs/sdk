//! Black-box smoke coverage for the default local Git-capable composition.
#![cfg(feature = "filesystem-local")]

use acyclic_harness::filesystem::PersistentLocalSwarm;
use acyclic_harness::model::{Model, ModelEvent, ModelProvider};
use acyclic_harness::{Error, Limits, OperationId, Result};
use futures::stream;
use futures::stream::BoxStream;
use serde_json::json;
use std::sync::Arc;
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
