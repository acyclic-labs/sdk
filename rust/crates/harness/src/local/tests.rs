//! Local defaults and the root agent host through the public API.
#![cfg(feature = "filesystem-local")]

use crate as acyclic_harness;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use crate::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    agent::ConversationEntry,
    context::ModelContextCapacity,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    local::{Event, agent_conversation::Conversation},
    model::{Model, ModelProvider},
    resources::ProviderRef,
    runtime::ToolContext,
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::{path::Path, sync::Arc};

mod agents;
mod context;
mod history_retry;
mod replay;
pub(super) mod settings;

fn scripted(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(capacity()))
}
pub(super) fn text() -> Arc<ScriptedModelProvider> {
    scripted([ScriptedTurn::Text("done".into())])
}

/// One conversation and its observation, which reports each settled entry.
struct ObservedConversation {
    conversation: Arc<Conversation>,
    observer: crate::local::Observer,
}

impl ObservedConversation {
    async fn open(harness: &Arc<LocalHarness>) -> Result<Self> {
        // The root's runtime runs once the harness hosts its agents.
        harness.agents().await?;
        let conversation = Conversation::open(harness.connect().await?);
        Self::observe(conversation, None).await
    }

    async fn observe(conversation: Arc<Conversation>, from: Option<u64>) -> Result<Self> {
        let observer = conversation.observe(from).await?;
        Ok(Self {
            conversation,
            observer,
        })
    }

    /// The entry settled at inbox sequence `inbox`, as the observer reports
    /// it; a finished agent or a closed Session fails the wait.
    async fn settled(&mut self, inbox: u64) -> Result<ConversationEntry> {
        loop {
            match self.observer.next().await? {
                Event::Turn(entry) if entry.inbox == inbox => return Ok(entry),
                Event::Status(status) => {
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) {
                        return Err(Error::Conflict(format!("agent finished: {status:?}")));
                    }
                }
                Event::Turn(_) | Event::Output => {}
            }
        }
    }

    async fn send(&mut self, id: &str, prompt: &str) -> Result<ConversationEntry> {
        let inbox = self.conversation.send(id, prompt).await?;
        self.settled(inbox).await
    }
}

/// Inspect the immutable text bodies delivered through the real content reader.
async fn request_text(
    harness: &LocalHarness,
    request: &crate::model::ModelRequest,
) -> Result<String> {
    let reader = harness.client_reader()?;
    let mut text = String::new();
    for message in &request.messages {
        if let crate::model::ModelContent::Text(body) = &message.content {
            text.push_str(body);
            text.push('\n');
        }
        for part in message.content.parts() {
            if let crate::model::ModelContentPart::Text { text: body, .. } = part {
                text.push_str(body);
                text.push('\n');
            }
        }
        for (file, policy) in message.content.file_parts() {
            if matches!(policy, crate::model::FileProjectionPolicy::BoundedFull) {
                let bytes = reader
                    .read(file, file.descriptor().byte_length(), None, None)?
                    .await?;
                text.push('\n');
                text.push_str(
                    std::str::from_utf8(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?,
                );
            }
        }
    }
    Ok(text)
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
        let configured = || -> Result<LocalHarnessConfig> {
            let mut config = volume_config(directory.path(), project.clone(), text())?
                .tools(vec![tool("test.custom")]);
            config.state_directory = None;
            if !defaults {
                config = config.without_default_tools();
            }
            Ok(config)
        };
        let allocations = super::config::TEMPORARY_STATES.get();
        let refusals = LocalHarness::doctor(configured()?).await;
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(super::config::TEMPORARY_STATES.get(), allocations);
        let harness = LocalHarness::open(configured()?).await?;
        assert_eq!(
            harness.tools().get("write", None, None)?.is_some(),
            defaults
        );
        assert!(harness.tools().get("test.custom", None, None)?.is_some());
    }
    Ok(())
}
