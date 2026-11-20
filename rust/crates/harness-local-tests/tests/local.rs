//! Local defaults and the root agent host through the public API.
#![cfg(not(target_arch = "wasm32"))]

use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
pub(super) use crate::test_support::{model, seeded, volume, volume_config};
use crate::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    agent::ConversationEntry,
    context::ModelContextCapacity,
    conversation::{ContentResidencyVerifier, VolumeClass, VolumeOwner, VolumeRef},
    local::{Event, agent_conversation::Conversation},
    model::{Model, ModelProvider},
    runtime::ToolContext,
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::{path::Path, sync::Arc};

mod agents;
pub(super) use agents::settled;
mod context;
mod context_documents;
mod history_retry;
mod platform;
mod programs;
mod replay;
pub(super) mod settings;
mod signing;
mod transcripts;
#[path = "local/workspace/mod.rs"]
mod workspace_tests;

pub(crate) fn temporary_directory() -> Result<tempfile::TempDir> {
    // Preserve native filesystem and durability calls without making
    // checkout proofs depend on the shared disk's latency.
    #[cfg(target_os = "linux")]
    if Path::new("/dev/shm").is_dir() {
        return tempfile::tempdir_in("/dev/shm").map_err(acyclic_harness_local::local::storage);
    }
    tempfile::tempdir().map_err(acyclic_harness_local::local::storage)
}

fn with_temporary_checkout_state(config: LocalHarnessConfig) -> Result<LocalHarnessConfig> {
    let directory = temporary_directory()?;
    let signing =
        acyclic_native_runtime::signing_key::MemorySigningStore::new().map_err(acyclic_harness_local::local::storage)?;
    let mut config = config
        .state_directory(directory.path())
        .signing_store(Arc::new(signing));
    config.test_state = Some(acyclic_native_runtime::OwnershipAnchor::new(directory));
    Ok(config)
}

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
pub(super) struct ObservedConversation {
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

    pub(super) async fn observe(
        conversation: Arc<Conversation>,
        from: Option<u64>,
    ) -> Result<Self> {
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
        self.settled_where(|entry| entry.inbox == inbox).await
    }

    async fn settled_where(
        &mut self,
        accepts: impl Fn(&ConversationEntry) -> bool,
    ) -> Result<(u64, ConversationEntry)> {
        loop {
            match self.observer.next().await? {
                Event::Turn { sequence, entry } if accepts(&entry) => {
                    return Ok((sequence, entry));
                }
                Event::Status(status) => {
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) {
                        return Err(Error::Conflict(format!("agent finished: {status:?}")));
                    }
                }
                Event::Turn { .. } | Event::Delta | Event::Input { .. } => {}
            }
        }
    }

    pub(super) async fn settled_child_notice(
        &mut self,
        turn: crate::OperationId,
    ) -> Result<ConversationEntry> {
        let notice = format!("child-settled:{turn}");
        let (_, entry) = self
            .settled_where(|entry| {
                matches!(&entry.input, crate::agent::TurnInput::Notice { id, .. }
                if id.as_str() == notice.as_str())
            })
            .await?;
        if !entry.outcome.is_completed() {
            return Err(Error::Conflict(format!(
                "parent could not handle child settlement: {:?}",
                entry.outcome
            )));
        }
        Ok(entry)
    }

    pub(super) async fn send(&mut self, id: &str, prompt: &str) -> Result<ConversationEntry> {
        Ok(self.send_at(id, prompt).await?.1)
    }

    async fn send_at(&mut self, id: &str, prompt: &str) -> Result<(u64, ConversationEntry)> {
        let inbox = self.conversation.send(id, prompt).await?;
        self.settled_at(inbox).await
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

/// Reads precisely the immutable references a fixture selected, without host custody.
pub(super) fn selected_reader<'a>(
    harness: &LocalHarness,
    files: impl IntoIterator<Item = &'a crate::conversation::FileRef>,
) -> Result<Arc<dyn ContentResidencyVerifier>> {
    let grants = files
        .into_iter()
        .map(|file| file.read_capability(&mut None))
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
    Ok(reader)
}
/// Inspect the immutable text bodies delivered through the real content reader.
pub(super) async fn request_text(
    harness: &LocalHarness,
    request: &crate::model::ModelRequest,
) -> Result<String> {
    let reader = selected_reader(
        harness,
        request
            .messages
            .iter()
            .flat_map(|message| message.content.file_parts())
            .map(|(file, _)| file),
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

fn native_checkout_config(
    workspace: &Path,
    model: (Model, Arc<dyn ModelProvider>),
) -> Result<(LocalHarnessConfig, acyclic_actors::local::ClockControl)> {
    // Keep authority time under the fixture's control while the seeded
    // executor waits for native checkout I/O.
    let (clock, control) = acyclic_actors::local::ControlledClock::new(1_700_000_000_000);
    Ok((
        checkout_config(workspace, model)?.clock(Arc::new(clock)),
        control,
    ))
}

pub(super) mod checked_commands {
    use crate::{
        model::{ModelEvent, ToolCall},
        test_support::{ScriptedEvent, ScriptedTurn},
    };

    /// One scripted command call on the named shell tool.
    pub(crate) fn call(index: usize, name: &str, command: &str) -> ScriptedTurn {
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
        output_shares_context: false,
    }
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
            projection_schema: acyclic_harness_core::tool::json_projection_schema(
                json!({"type":"object"}),
            ),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
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
        let allocations = acyclic_harness_local::local::TEMPORARY_STATES.get();
        let refusals = configured()?.preflight();
        assert!(refusals.is_empty(), "{refusals:?}");
        let refusals = LocalHarness::doctor(configured()?).await.refusals;
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(acyclic_harness_local::local::TEMPORARY_STATES.get(), allocations);
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
