//! Local defaults and the root agent host through the public API.
#![cfg(not(target_arch = "wasm32"))]

use crate as acyclic_harness;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use crate::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    agent::ConversationEntry,
    context::ModelContextCapacity,
    conversation::{ContentResidencyVerifier, VolumeClass, VolumeOwner, VolumeRef},
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
mod signing;
mod transcripts;

pub(super) fn scripted(
    turns: impl IntoIterator<Item = ScriptedTurn>,
) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(capacity()))
}
pub(super) fn text() -> Arc<ScriptedModelProvider> {
    scripted([ScriptedTurn::Text("done".into())])
}

/// Finite scripts model the task's requests, without optional settlement
/// followups. Settlement coverage opens the default composition directly.
pub(super) async fn open_scripted(config: LocalHarnessConfig) -> Result<LocalHarness> {
    LocalHarness::open(config.feedback_plugin(false)).await
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
        Ok(self.settled_at(inbox).await?.1)
    }

    /// As [`Self::settled`], with the entry's committed conversation sequence.
    async fn settled_at(&mut self, inbox: u64) -> Result<(u64, ConversationEntry)> {
        loop {
            match self.observer.next().await? {
                Event::Turn { sequence, entry } if entry.inbox == inbox => {
                    return Ok((sequence, entry));
                }
                Event::Status(status) => {
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) {
                        return Err(Error::Conflict(format!("agent finished: {status:?}")));
                    }
                }
                Event::Turn { .. } | Event::Delta => {}
            }
        }
    }

    async fn send(&mut self, id: &str, prompt: &str) -> Result<ConversationEntry> {
        let inbox = self.conversation.send(id, prompt).await?;
        self.settled(inbox).await
    }
}

/// Read-only host inspection of one agent's journal through authenticated handles.
pub(super) fn journal_content(
    harness: &LocalHarness,
    root: &crate::agent::AgentPath,
) -> Result<crate::runtime::ContentBindings> {
    let journal =
        crate::conversation::journal_volume(harness.project.provider().clone(), &root.id())?;
    let scope = harness
        .root_authority
        .as_ref()
        .expect("scripted journals have local signing custody")
        .bootstrap_scope(
            "journal-inspection",
            &crate::Capabilities::new([
                crate::contract::capability::HARNESS_HOST.to_owned(),
                journal.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes(root.id_bytes())),
            &mut None,
        )?;
    Ok(crate::runtime::ContentBindings {
        reader: crate::filesystem::FilesystemContentVerifier::new(
            harness.files.clone(),
            &harness.verifier,
            &scope,
            harness.limits.file_bytes,
            None,
        )?,
        writer: None,
        volume: journal,
    })
}

/// Inspect the immutable text bodies delivered through the real content reader.
pub(super) async fn request_text(
    harness: &LocalHarness,
    request: &crate::model::ModelRequest,
) -> Result<String> {
    // Requests may name inherited inputs and a child's tool results. Inspect
    // exactly their immutable descriptors, rather than the root's volumes.
    let grants = request
        .messages
        .iter()
        .flat_map(|message| message.content.file_parts())
        .map(|(file, _)| file.read_capability(&mut None))
        .collect::<Result<Vec<_>>>()?;
    let scope = harness
        .root_authority
        .as_ref()
        .expect("scripted requests have local signing custody")
        .bootstrap_scope(
            "request-inspection",
            &crate::Capabilities::new(grants),
            None,
            &mut None,
        )?;
    let reader = crate::filesystem::FilesystemContentVerifier::new(
        Arc::clone(&harness.files),
        &harness.verifier,
        &scope,
        harness.limits.file_bytes,
        None,
    )?;
    let mut text = String::new();
    for message in &request.messages {
        if let Some((notices, _)) = message.content.notice_parts() {
            text.push_str(
                &serde_json::to_string(notices)
                    .map_err(|error| Error::Storage(error.to_string()))?,
            );
            text.push('\n');
        }
        if let crate::model::ModelContent::Text(body) = &message.content {
            text.push_str(body);
            text.push('\n');
        }
        for part in message.content.parts() {
            match part {
                crate::model::ModelContentPart::Text { text: body, .. } => {
                    text.push_str(body);
                    text.push('\n');
                }
                crate::model::ModelContentPart::ToolResult { content, .. } => {
                    text.push_str(
                        &serde_json::to_string(content)
                            .map_err(|error| Error::Storage(error.to_string()))?,
                    );
                    text.push('\n');
                }
                _ => {}
            }
            if let crate::model::ModelContentPart::ToolResult {
                content: crate::model::ToolResultContent::Error { message },
                ..
            } = part
            {
                text.push_str(message);
                text.push('\n');
            }
        }
        for (file, policy) in message.content.file_parts() {
            if matches!(
                policy,
                crate::model::FileProjectionPolicy::BoundedFull
                    | crate::model::FileProjectionPolicy::ProjectInstructions(_)
            ) {
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

pub(super) mod checked_commands {
    use crate::{
        model::{ModelEvent, ToolCall},
        test_support::{ScriptedEvent, ScriptedTurn},
    };

    /// One scripted command call on the named shell tool.
    pub(in crate::local) fn call(index: usize, name: &str, command: &str) -> ScriptedTurn {
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
    for (defaults, explicit_state) in [(true, false), (false, false), (true, true), (false, true)] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let state = directory.path().join("new").join("..").join("state");
        let configured = || -> Result<LocalHarnessConfig> {
            let mut config = volume_config(directory.path(), project.clone(), text())?
                .tools(vec![tool("test.custom")]);
            config.state_directory = explicit_state.then(|| state.clone());
            if !defaults {
                config = config.without_default_tools();
            }
            Ok(config)
        };
        let allocations = super::config::TEMPORARY_STATES.get();
        let refusals = configured()?.preflight();
        assert!(refusals.is_empty(), "{refusals:?}");
        let refusals = LocalHarness::doctor(configured()?).await;
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(super::config::TEMPORARY_STATES.get(), allocations);
        assert!(
            !directory.path().join("new").exists(),
            "doctor created state"
        );
        let harness = LocalHarness::open(configured()?).await?;
        assert_eq!(
            harness.tools().get("write", None, None)?.is_some(),
            defaults
        );
        assert!(harness.tools().get("test.custom", None, None)?.is_some());
    }
    Ok(())
}

mod questions;
mod settlement;
