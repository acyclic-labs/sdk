//! Renderer-independent committed state and streaming tails.

use super::{content::MessageContent, usage::Usage};
use crate::{
    Result,
    conversation::ConversationMessage,
    core::{Authority, EventPayload, LifecycleState},
    model::{ModelDataPart, ModelEvent, ToolResultContent},
    wire,
    wire_codec::{decode_event_payload, encode_authority},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One decoded record from a platform follow, addressed to its tracked authority.
/// These are in-process client inputs, not another wire schema.
pub enum ViewUpdate {
    /// Atomic replacements/removals from an authorized document query follow.
    DocumentChange {
        /// Tracked agent whose private volume owns the publication.
        authority: wire::Authority,
        /// Existing file-query publication, including removals and bulk files.
        change: crate::task_volume::Change,
    },
    /// Replacement snapshot from a private-volume live query, including resets.
    DocumentSnapshot {
        /// Tracked agent whose private volume is being followed.
        authority: wire::Authority,
        /// The query whose selected documents are replaced at this cut.
        query: crate::task_volume::Query,
        /// Authenticated atomic snapshot from that query.
        snapshot: crate::task_volume::Snapshot,
    },
    /// One file from the same ordered private-volume follow.
    DocumentFile {
        /// Tracked agent whose private volume owns this file.
        authority: wire::Authority,
        /// Authenticated file; non-document files leave document panels unchanged.
        file: crate::task_volume::FileValue,
    },
    /// Atomic snapshot that installs one bounded shared file-query fold.
    QuerySnapshot {
        /// The selection whose files the fold retains.
        query: crate::task_volume::Query,
        /// Authenticated atomic snapshot from that query.
        snapshot: crate::task_volume::Snapshot,
        /// Maximum retained files.
        maximum_files: usize,
        /// Maximum retained payload bytes.
        maximum_bytes: usize,
    },
    /// A shared file slice or cursor checkpoint from the volume query follow.
    File(crate::task_volume::Change),
    /// One provider-neutral model event.
    Model {
        /// Conversation owning the event.
        authority: wire::Authority,
        /// Admitted turn identity from the model dispatch.
        turn: crate::OperationId,
        /// Exact provider-neutral item.
        event: ModelEvent,
    },
    /// One incremental tool output chunk.
    ToolOutput {
        /// Conversation owning the invocation.
        authority: wire::Authority,
        /// Stable invocation identity.
        call_id: String,
        /// Output accepted by the stream.
        delta: String,
    },
}

/// One conversation's committed transcript and current model tail.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TranscriptView {
    /// Immutable content references in conversation order.
    pub messages: Vec<ConversationMessage>,
    /// Verified text files keyed by canonical message identity; rebuilt on replay.
    pub text: BTreeMap<uuid::Uuid, String>,
    /// Admitted turn owning the current streaming tail.
    pub streaming_turn: Option<crate::OperationId>,
    /// Text received for the current assistant message.
    pub streaming_text: String,
    /// First retained task chunk; a query-window change invalidates render caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_start: Option<crate::task_volume::FileKey>,
    /// First retained reasoning chunk, independent of the text window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_start: Option<crate::task_volume::FileKey>,
    /// Provisional UTF-8 suffix bytes, replaced when the next segment repairs it.
    #[serde(default)]
    pub text_pending: usize,
    /// Independent provisional suffix in reasoning.
    #[serde(default)]
    pub reasoning_pending: usize,
    /// Provider-exposed reasoning for the current step.
    pub reasoning: String,
}

impl TranscriptView {
    pub(crate) fn pending(&self, reasoning: bool) -> usize {
        let (text, pending) = if reasoning {
            (&self.reasoning, self.reasoning_pending)
        } else {
            (&self.streaming_text, self.text_pending)
        };
        if pending == 3 && text.ends_with('\u{fffd}') {
            3
        } else {
            0
        }
    }
}

/// A tool invocation, including incremental stdout/stderr supplied by the host.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallView {
    /// Registered tool name.
    pub name: String,
    /// Model arguments.
    pub arguments: serde_json::Value,
    /// Provisional text chunks, replaced by the canonical model projection on commit.
    pub output: ToolResultContent,
    /// Canonical call message, when committed.
    pub call: Option<ConversationMessage>,
    /// Canonical result message, when committed.
    pub result: Option<ConversationMessage>,
}

impl Default for ToolCallView {
    fn default() -> Self {
        Self {
            name: String::new(),
            arguments: serde_json::Value::Null,
            output: ToolResultContent::Parts { parts: Vec::new() },
            call: None,
            result: None,
        }
    }
}

fn tool_call(tool: &mut ToolCallView, name: String, arguments: serde_json::Value) {
    tool.name = name;
    tool.arguments = arguments;
}

/// Task/subagent lifecycle, independent of terminal or web presentation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskView {
    /// Latest committed lifecycle.
    pub status: LifecycleState,
    /// Operation that committed the latest lifecycle transition, if known.
    pub lifecycle_operation_id: Option<String>,
    /// Optional diagnostic from the lifecycle transition.
    pub reason: Option<String>,
    /// Parent task, if this task was forked.
    pub parent: Option<Authority>,
}

/// Shared view model for ratatui, browser and WASM consumers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClientViews {
    /// Captured-volume query projections, keyed by tracked agent authority.
    pub diffs: BTreeMap<String, super::DiffView>,
    /// GOAL.md and plans/ folded from private-volume live queries.
    pub documents: BTreeMap<String, super::DocumentViews>,
    /// Disposable file-query folds, keyed by volume and selection identity.
    /// Recovery observes fresh files rather than persisting a second payload store.
    pub queries: BTreeMap<([u8; 32], [u8; 32]), super::FileQueryView>,
    pub(in crate::client) goal_sources: BTreeMap<([u8; 32], [u8; 32]), ([u8; 32], [u8; 32])>,
    /// Authority-keyed transcripts; keys are `kind:id`.
    pub transcripts: BTreeMap<String, TranscriptView>,
    /// Authority-keyed tool calls, then provider call identity.
    pub tools: BTreeMap<String, BTreeMap<String, ToolCallView>>,
    /// Root and child aggregate lifecycles.
    pub tasks: BTreeMap<String, TaskView>,
    /// Provider usage metadata, keyed by conversation. Providers retain their exact units.
    pub usage: BTreeMap<String, serde_json::Value>,
    usage_state: BTreeMap<String, Usage>,
    pub(in crate::client) task_folds: BTreeMap<String, super::task_projection::Fold>,
}

impl Serialize for ClientViews {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut result = serializer.serialize_struct("ClientViews", 8)?;
        result.serialize_field("diffs", &self.diffs)?;
        result.serialize_field("documents", &self.documents)?;
        result.serialize_field("goals", &super::goal_view::Goals(self))?;
        result.serialize_field("models", &super::model_view::Models(self))?;
        result.serialize_field("transcripts", &self.transcripts)?;
        result.serialize_field("tools", &self.tools)?;
        result.serialize_field("tasks", &self.tasks)?;
        result.serialize_field("usage", &self.usage)?;
        result.end()
    }
}

pub(crate) fn authority_key(authority: &wire::Authority) -> String {
    let kind = wire::AggregateKind::try_from(authority.kind)
        .map_or("AGGREGATE_KIND_UNSPECIFIED", |kind| kind.as_str_name());
    format!("{kind}:{}", authority.id)
}

impl ClientViews {
    pub(in crate::client) fn apply(
        &mut self,
        authority: &wire::Authority,
        operation_id: &str,
        payload: EventPayload,
        content: Option<MessageContent>,
    ) {
        let key = authority_key(authority);
        match payload {
            EventPayload::LifecycleTransitioned { to, reason, .. } => {
                let view = self.tasks.entry(key).or_insert(TaskView {
                    status: to,
                    lifecycle_operation_id: None,
                    reason: None,
                    parent: None,
                });
                view.status = to;
                view.lifecycle_operation_id = Some(operation_id.to_owned());
                view.reason = reason;
            }
            EventPayload::ConversationMessageAppended { message } => {
                let transcript = self.transcripts.entry(key.clone()).or_default();
                let mut content = content;
                if let Some(MessageContent::Transcript { text, .. }) = &mut content {
                    if let Some(text) = text.take() {
                        transcript.text.insert(message.id, text);
                    }
                }
                if message.kind == crate::conversation::MessageKind::Assistant
                    && transcript.streaming_turn.is_some_and(|turn| {
                        crate::conversation::assistant_message_id(turn) == message.id
                    })
                {
                    transcript.streaming_text.clear();
                    transcript.file_start = None;
                    transcript.text_pending = 0;
                    transcript.reasoning_pending = 0;
                    transcript.reasoning_start = None;
                    transcript.reasoning.clear();
                    transcript.streaming_turn = None;
                }
                if let Some(call_id) = &message.tool_call_id {
                    let tool = self
                        .tools
                        .entry(key)
                        .or_default()
                        .entry(call_id.clone())
                        .or_default();
                    if message.kind == crate::conversation::MessageKind::ToolCall {
                        if let Some(MessageContent::Call { name, arguments }) = content {
                            tool_call(tool, name, arguments);
                        }
                        tool.call = Some(*message.clone());
                    } else {
                        if let Some(MessageContent::Output(output)) = content {
                            tool.output = output;
                        }
                        tool.result = Some(*message.clone());
                    }
                }
                transcript.messages.push(*message);
            }
            EventPayload::ForkPublished { seed } => {
                let child = self
                    .tasks
                    .entry(authority_key(&encode_authority(&seed.child)))
                    .or_insert(TaskView {
                        status: LifecycleState::Pending,
                        lifecycle_operation_id: None,
                        reason: None,
                        parent: None,
                    });
                child.parent = Some(seed.parent);
            }
            _ => {}
        }
    }

    /// Adds a model stream event to the same state that committed replay updates.
    pub fn model_event(
        &mut self,
        authority: &wire::Authority,
        turn: crate::OperationId,
        event: ModelEvent,
    ) -> Result<()> {
        if turn.into_bytes() == [0; 16] {
            return Err(crate::Error::Invalid(
                "streaming turn identity is nil".into(),
            ));
        }
        let key = authority_key(authority);
        if matches!(
            &event,
            ModelEvent::Content { .. } | ModelEvent::Reasoning { .. }
        ) {
            let transcript = self.transcripts.entry(key.clone()).or_default();

            if transcript.streaming_turn != Some(turn) {
                let message_id = crate::conversation::assistant_message_id(turn);
                if transcript.messages.iter().rev().any(|message| {
                    message.kind == crate::conversation::MessageKind::Assistant
                        && message.id == message_id
                }) {
                    return Ok(());
                }
                transcript.streaming_text.clear();
                transcript.file_start = None;
                transcript.text_pending = 0;
                transcript.reasoning_pending = 0;
                transcript.reasoning_start = None;
                transcript.reasoning.clear();
                transcript.streaming_turn = Some(turn);
            }
        }
        match event {
            ModelEvent::Content { delta } => self
                .transcripts
                .entry(key)
                .or_default()
                .streaming_text
                .push_str(&delta),
            ModelEvent::Reasoning { delta } => self
                .transcripts
                .entry(key)
                .or_default()
                .reasoning
                .push_str(&delta),
            ModelEvent::ToolCall(crate::model::ToolCall {
                call_id,
                name,
                arguments,
                ..
            }) => {
                let tool = self
                    .tools
                    .entry(key)
                    .or_default()
                    .entry(call_id)
                    .or_default();
                if tool.call.is_none() {
                    tool_call(tool, name, arguments);
                }
            }
            ModelEvent::ReasoningBlock { .. } => {}
            ModelEvent::Completed { metadata } => {
                let message = crate::conversation::assistant_message_id(turn);
                if self.transcripts.get(&key).is_some_and(|transcript| {
                    transcript
                        .messages
                        .iter()
                        .rev()
                        .any(|item| item.id == message)
                }) {
                    return Ok(());
                }
                let mut usage = self.usage_state.get(&key).cloned().unwrap_or_default();
                let total = usage.complete(message, metadata)?;
                self.set_usage(authority, usage, total);
            }
        }
        Ok(())
    }

    /// Appends output without requiring a renderer to maintain a second tool registry.
    pub fn tool_output(&mut self, authority: &wire::Authority, call_id: &str, delta: &str) {
        let tool = self
            .tools
            .entry(authority_key(authority))
            .or_default()
            .entry(call_id.to_owned())
            .or_default();
        if tool.result.is_some() {
            return;
        }
        if let ToolResultContent::Parts { parts } = &mut tool.output {
            if let Some(ModelDataPart::Text { text }) = parts.last_mut() {
                text.push_str(delta);
            } else {
                parts.push(ModelDataPart::Text {
                    text: delta.to_owned(),
                });
            }
        }
    }

    pub(in crate::client) fn prepare_usage<'a>(
        &self,
        authority: &wire::Authority,
        messages: impl Iterator<Item = &'a (EventPayload, Option<MessageContent>)>,
    ) -> Result<Option<(Usage, serde_json::Value)>> {
        let key = authority_key(authority);
        let mut next = None;
        for (payload, content) in messages {
            if let (
                EventPayload::ConversationMessageAppended { message },
                Some(MessageContent::Transcript {
                    metadata: Some(metadata),
                    ..
                }),
            ) = (payload, content)
            {
                let usage = next
                    .get_or_insert_with(|| self.usage_state.get(&key).cloned().unwrap_or_default());
                usage.commit(message.id, metadata.clone())?;
            }
        }
        next.map(|usage| {
            let total = usage.total()?;
            Ok((usage, total))
        })
        .transpose()
    }

    pub(in crate::client) fn set_usage(
        &mut self,
        authority: &wire::Authority,
        usage: Usage,
        total: serde_json::Value,
    ) {
        let key = authority_key(authority);
        self.usage_state.insert(key.clone(), usage);
        self.usage.insert(key, total);
    }
}

pub(in crate::client) fn payload(event: &wire::EventEnvelope) -> Result<EventPayload> {
    decode_event_payload(&event.event_type, &event.canonical_payload_json, &mut None)
        .map(|(payload, _)| payload)
}
