//! Fixtures shared by the crate's own unit tests. Scenarios through the
//! public API live in `acyclic-harness-local-tests`.

use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
pub(super) use crate::test_support::{seeded, volume, volume_config};
use crate::{
    Error, LocalHarness, LocalHarnessConfig, Result,
    agent::{AgentPath, ConversationEntry},
    context::ModelContextCapacity,
    conversation::ContentResidencyVerifier,
    local::{Event, agent_conversation::Conversation},
};
use acyclic_stream::{StreamError, StreamPath, StreamProvider, StreamProviderExt as _};
use std::sync::Arc;

pub(super) mod settings;

pub(super) fn scripted(
    turns: impl IntoIterator<Item = ScriptedTurn>,
) -> Arc<ScriptedModelProvider> {
    Arc::new(
        ScriptedModelProvider::new(turns).with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
            output_shares_context: false,
        }),
    )
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

    async fn settled_where(
        &mut self,
        accepts: impl Fn(&ConversationEntry) -> bool,
    ) -> Result<ConversationEntry> {
        loop {
            match self.observer.next().await? {
                Event::Turn { entry, .. } if accepts(&entry) => return Ok(entry),
                Event::Status(status) => {
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) {
                        return Err(Error::Conflict(format!("agent finished: {status:?}")));
                    }
                }
                Event::Turn { .. } | Event::Delta | Event::Input { .. } => {}
            }
        }
    }

    /// The parent's settled handling of child turn `turn`'s settlement notice.
    pub(super) async fn settled_child_notice(
        &mut self,
        turn: crate::OperationId,
    ) -> Result<ConversationEntry> {
        let notice = format!("child-settled:{turn}");
        let entry = self
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

    /// Sends `prompt` as message `id` and waits for its settled entry; a
    /// finished agent or a closed Session fails the wait.
    pub(super) async fn send(&mut self, id: &str, prompt: &str) -> Result<ConversationEntry> {
        let inbox = self.conversation.send(id, prompt).await?;
        self.settled_where(|entry| entry.inbox == inbox).await
    }
}

/// Reads precisely the immutable references a fixture selected, without host custody.
fn selected_reader<'a>(
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
        .ok_or_else(|| Error::Invalid("scripted requests have local signing custody".into()))?
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
            text.push_str(&serde_json::to_string(notices).map_err(storage)?);
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
                    text.push_str(&serde_json::to_string(content).map_err(storage)?);
                    text.push('\n');
                    if let crate::model::ToolResultContent::Error { message } = content {
                        text.push_str(message);
                        text.push('\n');
                    }
                }
                _ => {}
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
                text.push_str(std::str::from_utf8(&bytes).map_err(storage)?);
            }
        }
    }
    Ok(text)
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

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn records<P: StreamProvider + ?Sized>(
    stream: &Arc<P>,
    path: StreamPath,
) -> Result<Vec<Vec<u8>>> {
    let tail = match stream.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(storage(error)),
    };
    let mut replay = acyclic_stream::Stream::new(Arc::clone(stream), path.as_str())
        .map_err(storage)?
        .replay(0)
        .until(tail);
    let mut values = Vec::new();
    while let Some(page) = replay.next_page().await.map_err(storage)? {
        values.extend(page.into_iter().map(|record| record.value.to_vec()));
    }
    Ok(values)
}

/// The root's conversation once it has settled every input sent before
/// this call, using the original actor checkpoint and runtime terminal event.
pub(super) async fn settled(harness: &Arc<LocalHarness>, root: &AgentPath) -> Result<Vec<Vec<u8>>> {
    let (agents, _) = harness.agents().await?;
    let completion = harness
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("hosted agents".into()))?
        .runtime
        .completion();
    let observation = async {
        let inbox = crate::agent::inbox_tail(&**agents.provider(), root)
            .await
            .map_err(storage)?;
        if let Some(input) = inbox.checked_sub(1) {
            agents.wait_for_input(root, input).await.map_err(storage)?;
        }
        records(agents.provider(), root.conversation().map_err(storage)?).await
    };
    tokio::select! {
        biased;
        result = completion => {
            result?;
            Err(Error::Invalid("runtime completion returned success".into()))
        },
        result = observation => result,
    }
}
