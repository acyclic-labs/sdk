//! Local defaults and the root agent host through the public API.
#![cfg(feature = "filesystem-local")]

use crate as acyclic_harness;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use crate::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    context::ModelContextCapacity,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    model::{Model, ModelProvider},
    resources::ProviderRef,
    runtime::ToolContext,
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

mod agents;
mod context;

fn scripted(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(capacity()))
}
pub(super) fn text() -> Arc<ScriptedModelProvider> {
    scripted([ScriptedTurn::Text("done".into())])
}

fn checkout_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (Model, Arc<dyn ModelProvider>),
) -> Result<LocalHarnessConfig> {
    // These scripts exercise filesystem commands, not summary-model requests.
    Ok(crate::test_support::local_harness_config(workspace, model)?
        .compaction(crate::context::CompactionPolicy::Disabled))
}

mod checked_commands {
    use crate::{
        model::{ModelEvent, ToolCall},
        test_support::{ScriptedEvent, ScriptedTurn},
    };

    /// One scripted command call on the named shell tool.
    pub(super) fn call(index: usize, name: &str, command: &str) -> ScriptedTurn {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                format!("checkout-{index}"),
                name.into(),
                serde_json::json!({"command": command}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    }
}

fn capacity() -> ModelContextCapacity {
    ModelContextCapacity {
        context_tokens: 32_768,
        output_tokens: Some(4096),
    }
}

fn model() -> Result<Model> {
    Model::new("scripted".into(), "test".into(), "1".into())
}

#[test]
fn automatic_compaction_fits_small_models_without_rewriting_explicit_budgets() -> Result<()> {
    let capacity = ModelContextCapacity {
        context_tokens: 32_768,
        output_tokens: Some(4096),
    };
    let explicit = acyclic_harness::context::ThresholdCompaction::default();
    let automatic = acyclic_harness::context::ThresholdCompaction::for_capacity(capacity, 24_000)?;
    assert_eq!(
        Some(automatic.validate(capacity, None)?),
        capacity.output_tokens
    );
    assert!(automatic.recent_tokens < explicit.recent_tokens);
    assert!(
        24_000 + automatic.response_reserve_tokens + 2 * automatic.recent_tokens
            <= capacity.context_tokens
    );
    assert!(explicit.validate(capacity, None).is_err());
    assert!(
        acyclic_harness::context::ThresholdCompaction::for_capacity(
            ModelContextCapacity {
                context_tokens: 2,
                output_tokens: Some(1)
            },
            0
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn incompatible_explicit_compaction_fails_before_owner_creation() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let mut config = volume_config(directory.path(), project, provider.clone())?;
    let owner_path = config.resolved_state_directory()?.join("owner.json");
    config = config.compaction(acyclic_harness::context::CompactionPolicy::default());
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Invalid(_))
    ));
    assert!(!owner_path.exists());
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}

#[tokio::test]
async fn oversized_tool_catalog_fails_before_owner_creation_or_model_dispatch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let mut oversized = tool("test.oversized");
    oversized.definition.description = "x".repeat(capacity().context_tokens as usize);
    let config = volume_config(directory.path(), project, provider.clone())?.tool(oversized);
    let owner = config.resolved_state_directory()?.join("owner.json");
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Invalid(_))
    ));
    assert!(!owner.exists());
    assert!(provider.requests().is_empty());
    Ok(())
}

struct Echo;
impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"text":"echo"}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
fn tool(name: &str) -> Tool {
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: "echo".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"object"}),
            projection_schema: acyclic_harness::tool::json_projection_schema(
                json!({"type":"object"}),
            ),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
}

pub(super) fn volume_config(
    directory: &Path,
    volume: VolumeRef,
    provider: Arc<dyn ModelProvider>,
) -> Result<LocalHarnessConfig> {
    let state = directory
        .join("state")
        .join(volume.storage_name(&mut None)?);
    Ok(crate::test_support::local_harness_config(
        directory.join("filesystem"),
        (model()?, provider),
    )?
    .volume(volume)
    .state_directory(state))
}

pub(super) async fn volume(directory: &Path) -> Result<VolumeRef> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new(
        "local-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
        &mut None,
    )?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    FilesystemHost::new(fs, &provider, None)?
        .create_volume(&volume)
        .await?;
    Ok(volume)
}

#[tokio::test]
async fn defaults_are_additive_and_opt_out_retains_custom_tools() -> Result<()> {
    for defaults in [true, false] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let mut config =
            volume_config(directory.path(), project, text())?.tools(vec![tool("test.custom")]);
        if !defaults {
            config = config.without_default_tools();
        }
        let harness = LocalHarness::open(config).await?;
        assert_eq!(
            harness.tools().get("write", None, None)?.is_some(),
            defaults
        );
        assert!(harness.tools().get("test.custom", None, None)?.is_some());
    }
    Ok(())
}
