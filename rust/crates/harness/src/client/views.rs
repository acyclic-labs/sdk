//! Renderer-independent committed state and streaming tails.

use crate::{
    Result,
    conversation::ConversationMessage,
    core::{Authority, EventPayload, LifecycleState},
    model::ModelEvent,
    wire,
    wire_codec::{decode_event_payload, encode_authority},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One decoded record from a platform follow, addressed to its tracked authority.
/// These are in-process client inputs, not another wire schema.
pub enum ViewUpdate {
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
    /// Admitted turn owning the current streaming tail.
    pub streaming_turn: Option<crate::OperationId>,
    /// Text received for the current assistant message.
    pub streaming_text: String,
    /// Provider-exposed reasoning for the current step.
    pub reasoning: String,
}

/// A tool invocation, including incremental stdout/stderr supplied by the host.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolCallView {
    /// Registered tool name.
    pub name: String,
    /// Model arguments.
    pub arguments: serde_json::Value,
    /// Streaming output chunks in arrival order.
    pub output: String,
    /// Canonical call message, when committed.
    pub call: Option<ConversationMessage>,
    /// Canonical result message, when committed.
    pub result: Option<ConversationMessage>,
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
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClientViews {
    /// Authority-keyed transcripts; keys are `kind:id`.
    pub transcripts: BTreeMap<String, TranscriptView>,
    /// Authority-keyed tool calls, then provider call identity.
    pub tools: BTreeMap<String, BTreeMap<String, ToolCallView>>,
    /// Root and child aggregate lifecycles.
    pub tasks: BTreeMap<String, TaskView>,
    /// Provider usage metadata, keyed by conversation. Providers retain their exact units.
    pub usage: BTreeMap<String, serde_json::Value>,
}

pub(super) fn authority_key(authority: &wire::Authority) -> String {
    let kind = wire::AggregateKind::try_from(authority.kind)
        .map_or("AGGREGATE_KIND_UNSPECIFIED", |kind| kind.as_str_name());
    format!("{kind}:{}", authority.id)
}

impl ClientViews {
    pub(super) fn apply(
        &mut self,
        authority: &wire::Authority,
        operation_id: &str,
        payload: EventPayload,
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
                if message.kind == crate::conversation::MessageKind::Assistant
                    && transcript.streaming_turn.is_some_and(|turn| {
                        crate::conversation::assistant_message_id(turn) == message.id
                    })
                {
                    transcript.streaming_text.clear();
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
                        tool.call = Some(*message.clone());
                    } else {
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
            ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                let tool = self
                    .tools
                    .entry(key)
                    .or_default()
                    .entry(call_id)
                    .or_default();
                tool.name = name;
                tool.arguments = arguments;
            }
            ModelEvent::Completed { metadata } => {
                accumulate_usage(
                    self.usage.entry(key).or_insert(serde_json::Value::Null),
                    metadata,
                )?;
            }
        }
        Ok(())
    }

    /// Appends output without requiring a renderer to maintain a second tool registry.
    pub fn tool_output(&mut self, authority: &wire::Authority, call_id: &str, delta: &str) {
        self.tools
            .entry(authority_key(authority))
            .or_default()
            .entry(call_id.to_owned())
            .or_default()
            .output
            .push_str(delta);
    }
}

fn accumulate_usage(total: &mut serde_json::Value, step: serde_json::Value) -> Result<()> {
    use serde_json::Value;
    let overflow = || crate::Error::Invalid("usage counter overflow".into());
    match (total, step) {
        (Value::Object(total), Value::Object(step)) => {
            for (key, value) in step {
                accumulate_usage(total.entry(key).or_insert(Value::Null), value)?;
            }
        }
        (Value::Number(total), Value::Number(step)) => {
            *total = if let (Some(left), Some(right)) = (total.as_u64(), step.as_u64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else if let (Some(left), Some(right)) = (total.as_i64(), step.as_i64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else {
                serde_json::Number::from_f64(
                    total.as_f64().ok_or_else(overflow)? + step.as_f64().ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?
            };
        }
        (total, step) => {
            *total = step;
        }
    }
    Ok(())
}

pub(super) fn payload(event: &wire::EventEnvelope) -> Result<EventPayload> {
    decode_event_payload(&event.event_type, &event.canonical_payload_json, &mut None)
        .map(|(payload, _)| payload)
}
