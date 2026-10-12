//! Provenance-preserving projection of canonical history into model context.

mod artifacts;
mod attachments;
mod selection;
#[cfg(test)]
mod tests;

pub use crate::conversation::ModelContextSelection;
pub use selection::{bounded_model_context_selection, select_turn_delta, validate_model_context_selection};
pub(crate) use selection::select_turn_suffix;
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, ConversationState, Limits, MessageKind},
    executor::{PreparationIo, stock_boundary, with_history_io},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole},
    wire::ErrorCode::{Invalid, Unsupported},
};
use serde::{Deserialize, Serialize};

/// Model-visible messages and their exact canonical provenance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectedModelContext {
    /// Selected source identities at the pinned revision.
    pub selection: ModelContextSelection,
    /// Transient messages for dispatch.
    pub messages: Vec<ModelMessage>,
}

impl SelectedModelContext {
    /// Validates the bounded context before dispatch.
    pub fn validate_for_dispatch(&self, limits: Limits) -> Result<()> {
        limits.validate(&mut None)?;
        if let Some(checkpoint) = &self.selection.checkpoint {
            limits.validate_file(checkpoint, &mut None)?;
        }
        let last = self.messages.last().ok_or_else(|| Error::Invalid(
            "selected context must end with the current user message".into()))?;
        if self.selection.conversation_revision == 0
            || self.messages.len() > limits.context_messages
            || self.messages.len() != self.selection.message_ids.len()
            || last.role != ModelRole::User
        {
            return Err(Error::Invalid("selected context must end with the current user message".into()));
        }
        last.content.validate_user_input(&mut None)?;
        for message in &self.messages { message.content.validate_limits(limits, &mut None)?; }
        Ok(())
    }

    /// Verifies that the final projected message is the current input.
    pub(crate) fn validate_for_input(&self, input: &ModelContent, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native {
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut matches = false;
        if let Some(last) = self.messages.last().filter(|last|
            self.messages.len() == self.selection.message_ids.len() && last.role == ModelRole::User)
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = native {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            }
            matches = &last.content == input;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = native {
                work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
            }
        }
        if matches { Ok(()) } else {
            Err(stock_boundary(io, Invalid, "selected context must end with the current user input"))
        }
    }
}

/// Projects bounded messages from the explicitly pinned conversation revision.
pub async fn select_model_context(
    conversation: &ConversationState, conversation_revision: u64,
    selection: ModelContextSelection, resolver: &dyn ContentResidencyVerifier,
    limits: Limits, maximum_projected_attachments: usize, io: Option<&PreparationIo>,
) -> Result<SelectedModelContext> {
    with_history_io(io, |work|
        validate_model_context_selection(conversation, &selection, conversation_revision, work))?;
    if limits.context_messages == 0 || limits.render_bytes == 0
        || limits.render_bytes > crate::conversation::MAX_EXACT_JS_INTEGER
        || maximum_projected_attachments as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64
        || selection.message_ids.len() > limits.context_messages
    {
        return Err(stock_boundary(io, Invalid, "selected message count exceeds projection limit"));
    }
    let mut messages = crate::model::vec_with_capacity::<ModelMessage>(selection.message_ids.len(), io)?;
    for id in &selection.message_ids {
        crate::model::charge_items(io, 1)?;
        let message = conversation.message(*id).ok_or_else(||
            stock_boundary(io, Invalid, "selected conversation message is missing"))?;
        let (role, content) = match message.kind {
            MessageKind::ToolCall => {
                let call = artifacts::read_invocation(resolver, &message.content, limits.render_bytes, io).await?;
                with_history_io(io, |work|
                    crate::tool::ToolInvocation::validate_identity(&call.call_id, Some(&call.name), work))?;
                if message.tool_call_id.as_deref() != Some(call.call_id.as_str()) {
                    return Err(stock_boundary(io, Invalid, "tool call artifact identity does not match its record"));
                }
                (ModelRole::Assistant, ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: call.call_id, name: call.name, arguments: call.arguments,
                }))
            }
            MessageKind::ToolResult => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(stock_boundary(io, Invalid, "tool result artifact type is invalid"));
                }
                let call_id = message.tool_call_id.as_deref().ok_or_else(||
                    stock_boundary(io, Invalid, "tool result lacks call identity"))?;
                let mut linked = None;
                if let Some(reply) = message.reply_to {
                    for (id, projected) in selection.message_ids[..messages.len()].iter().zip(&messages).rev() {
                        crate::model::charge_items(io, 1)?;
                        if *id == reply { linked = Some(projected); break; }
                    }
                }
                let Some(ModelMessage { content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: linked_call_id, name, ..
                }), .. }) = linked else {
                    return Err(stock_boundary(io, Invalid, "selected tool result lacks its call"));
                };
                if linked_call_id != call_id {
                    return Err(stock_boundary(io, Invalid, "selected tool result lacks its call"));
                }
                let _hold;
                let (items, hold) = attachments::resolve(message, resolver, limits.attachments, io).await?;
                _hold = hold;
                let mut projection = None;
                for item in items.iter() {
                    crate::model::charge_items(io, 1)?;
                    if item.label.as_deref() == Some("model_projection") { projection = Some(item); break; }
                }
                let projection = projection.ok_or_else(||
                    stock_boundary(io, Invalid, "tool result lacks its model projection"))?;
                let content = artifacts::read_result(resolver, &projection.file, limits.render_bytes, io).await?;
                (ModelRole::Tool, ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: crate::executor::stock_string_copy(call_id, io)?,
                    name: crate::executor::stock_string_copy(name, io)?, content,
                }))
            }
            MessageKind::System | MessageKind::User | MessageKind::Assistant => {
                let role = match message.kind {
                    MessageKind::System => ModelRole::System,
                    MessageKind::User => ModelRole::User,
                    _ => ModelRole::Assistant,
                };
                let policy = match message.content.descriptor().media_type() {
                    "image/png" | "image/jpeg" | "image/gif" | "image/webp" => attachments::native_image(limits.render_bytes, io)?,
                    kind if kind.starts_with("text/") && message.content.descriptor().byte_length() <= limits.render_bytes => FileProjectionPolicy::BoundedFull,
                    _ => FileProjectionPolicy::Reference,
                };
                let hold;
                let (items, source_hold) = attachments::resolve(message, resolver, limits.attachments, io).await?;
                hold = source_hold;
                let count = items.len().min(maximum_projected_attachments);
                let omitted = items.len() - count;
                let mut parts = crate::model::vec_with_capacity::<ModelContentPart>(1 + count + usize::from(omitted != 0), io)?;
                parts.push(ModelContentPart::File { file: crate::model::clone_file(&message.content, io)?, policy });
                attachments::append(items, hold, count, &mut parts, limits.render_bytes, io)?;
                if omitted != 0 { parts.push(ModelContentPart::Text { text: attachments::omission_text(omitted, io)? }); }
                (role, ModelContent::Parts(parts))
            }
            _ => return Err(stock_boundary(io, Unsupported, "message kind requires a specialized model projection")),
        };
        messages.push(ModelMessage { role, content });
    }
    Ok(SelectedModelContext { selection, messages })
}
