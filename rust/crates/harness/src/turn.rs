//! Deterministic admission planning for one ref-only conversation turn.

use crate::{
    Error, OperationId, Result,
    conversation::{
        Attachment, ConversationMessage, ConversationState, FileRef, Limits, MessageKind,
        ModelContextSelection, ReferencedAttachments,
    },
    projection::{bounded_model_context_selection, validate_model_context_selection_at_revision},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// Disposition returned after checking a turn's committed selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnDisposition {
    /// No selection existed; the caller may dispatch the model.
    Dispatch,
    /// A selection exists; the caller may reconcile the same operation.
    Reconcile,
    /// A prior model attempt is unresolved and cannot be retried locally.
    Indeterminate,
    /// The caller already has the completed output for this operation.
    Completed,
}

/// Rust-owned result of deterministic turn preparation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPreparation {
    /// Stable user message identity derived from the operation.
    pub user_id: Uuid,
    /// User record to append when this operation has not yet been admitted.
    #[serde(skip)]
    pub user_message: Option<ConversationMessage>,
    /// Whether the caller must append the already validated user payload.
    pub append_user: bool,
    /// Exact bounded context identities to commit or reuse.
    pub selection: ModelContextSelection,
    /// Whether `selection` was synthesized during this call.
    pub selection_is_new: bool,
    /// Whether the caller may dispatch, reconcile, or must stop.
    pub disposition: TurnDisposition,
}

/// Canonical identity for the user record of one local conversation operation.
/// This is the same SHA-256 UUID derivation exposed by the WASM facade, so a
/// native retry and a JavaScript retry address the same message identity.
pub fn canonical_user_message_id(operation_id: OperationId) -> Uuid {
    derived_uuid(operation_id, "user")
}

/// Shared UUID derivation used by the local turn planner and WASM facade.
pub fn derived_uuid(operation_id: OperationId, label: &str) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(operation_id.to_string().as_bytes());
    digest.update(b":");
    digest.update(label.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    for (target, source) in bytes.iter_mut().zip(digest.iter()) {
        *target = *source;
    }
    bytes[6] = (bytes[6] & 15) | 80;
    bytes[8] = (bytes[8] & 63) | 128;
    Uuid::from_bytes(bytes)
}

/// Plans the deterministic portion of one conversation turn.
///
/// `existing_selection` is the reducer's operation-scoped committed
/// selection, if any.  The booleans describe external model state because
/// native journals and JavaScript runtimes own different reconciliation
/// mechanisms; the decision itself remains Rust-owned.
#[expect(
    clippy::too_many_arguments,
    reason = "the planner boundary keeps all deterministic turn inputs explicit"
)]
pub fn prepare_turn(
    conversation: &ConversationState,
    operation_id: OperationId,
    content: FileRef,
    attachments: ReferencedAttachments,
    limits: Limits,
    existing_selection: Option<ModelContextSelection>,
    has_completed_output: bool,
    can_reconcile: bool,
) -> Result<TurnPreparation> {
    prepare_turn_with_user_id(
        conversation,
        operation_id,
        content,
        attachments,
        limits,
        existing_selection,
        has_completed_output,
        can_reconcile,
        None,
    )
}

/// Plans a turn while preserving a host's established message identity
/// namespace. New WASM/TS callers use the canonical identity above; native
/// durable callers may supply a legacy ID for existing histories.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the shared planner owns one auditable deterministic admission sequence"
)]
pub fn prepare_turn_with_user_id(
    conversation: &ConversationState,
    operation_id: OperationId,
    content: FileRef,
    attachments: ReferencedAttachments,
    limits: Limits,
    existing_selection: Option<ModelContextSelection>,
    has_completed_output: bool,
    can_reconcile: bool,
    user_id_override: Option<Uuid>,
) -> Result<TurnPreparation> {
    limits.validate_file(&content)?;
    attachments.validate()?;
    if let ReferencedAttachments::Inline { items } = &attachments {
        if items.len() > limits.attachments {
            return Err(Error::Invalid(
                "turn attachments exceed harness limits".into(),
            ));
        }
        for attachment in items {
            attachment.validate()?;
            limits.validate_file(&attachment.file)?;
        }
    }

    let user_id = user_id_override.unwrap_or_else(|| canonical_user_message_id(operation_id));
    if let Some(last_user) = conversation
        .messages
        .iter()
        .rfind(|message| message.kind == MessageKind::User)
        && last_user.id != user_id
        && !conversation.messages.iter().any(|message| {
            (message.kind == MessageKind::Assistant
                || (message.kind == MessageKind::System
                    && message.extensions.contains_key("acyclic.turn.outcome")))
                && message.reply_to == Some(last_user.id)
        })
    {
        return Err(Error::Conflict(
            "previous conversation turn is unresolved; retry it before admitting another".into(),
        ));
    }

    let current = conversation
        .messages
        .iter()
        .find(|message| message.id == user_id);
    if let Some(outcome) = conversation.messages.iter().find(|message| {
        message.kind == MessageKind::System
            && message.reply_to == Some(user_id)
            && message.extensions.contains_key("acyclic.turn.outcome")
    }) {
        let abandoned_id = derived_uuid(operation_id, "indeterminate-notice");
        return Err(Error::Conflict(
            if outcome.id == abandoned_id {
                "conversation turn was explicitly abandoned after an indeterminate model outcome"
            } else {
                "conversation turn already has a terminal outcome"
            }
            .into(),
        ));
    }

    let user_sequence = current.map_or(conversation.messages.len() as u64 + 1, |message| {
        message.sequence
    });
    let user_message = ConversationMessage {
        id: user_id,
        sequence: user_sequence,
        kind: MessageKind::User,
        content,
        attachments,
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    if let Some(existing) = current
        && (existing.kind != MessageKind::User
            || existing.content != user_message.content
            || existing.attachments != user_message.attachments)
    {
        return Err(Error::Conflict(
            "turn identity belongs to another user message".into(),
        ));
    }

    let mut prepared = conversation.clone();
    let append_user = current.is_none();
    if append_user {
        prepared.append(user_message.clone())?;
    }
    let selection_is_new = existing_selection.is_none();
    let selection = match existing_selection {
        Some(selection) => {
            // A committed selection is anchored to the history observed before
            // the model ran. Later assistant/tool publication may extend the
            // conversation, so validate its provenance against that anchor
            // rather than requiring the current tail length to remain equal.
            if selection.conversation_revision < user_message.sequence
                || selection.conversation_revision > prepared.messages.len() as u64
            {
                return Err(Error::Conflict(
                    "model context selection has a stale conversation revision".into(),
                ));
            }
            validate_model_context_selection_at_revision(
                &prepared,
                &selection,
                selection.conversation_revision,
            )?;
            if selection.message_ids.last() != Some(&user_id) {
                return Err(Error::Conflict(
                    "turn identity is bound to another context selection".into(),
                ));
            }
            selection
        }
        None => bounded_model_context_selection(&prepared, user_id, limits.context_messages)?,
    };
    let disposition = if has_completed_output {
        TurnDisposition::Completed
    } else if selection_is_new {
        TurnDisposition::Dispatch
    } else if can_reconcile {
        TurnDisposition::Reconcile
    } else {
        TurnDisposition::Indeterminate
    };
    Ok(TurnPreparation {
        user_id,
        user_message: append_user.then_some(user_message),
        append_user,
        selection,
        selection_is_new,
        disposition,
    })
}

/// Converts an attachment vector into the inline canonical representation.
pub fn inline_attachments(items: Vec<Attachment>) -> ReferencedAttachments {
    ReferencedAttachments::Inline { items }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };

    fn limits() -> Limits {
        Limits::default()
    }

    fn file(name: &str) -> Result<FileRef> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "memory", "1")?,
            "turns",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::new()),
        )?;
        Ok(FileRef::new(
            volume,
            name,
            "v1",
            FileDescriptor::from_bytes(b"body", "text/plain")?,
            name,
        )?)
    }

    fn conversation() -> Result<ConversationState> {
        let mut value = ConversationState::default();
        value.bind(AgentId::new())?;
        Ok(value)
    }

    #[test]
    fn fresh_plans_bounded_user_and_dispatch() -> Result<()> {
        let operation = OperationId::new();
        let result = prepare_turn(
            &conversation()?,
            operation,
            file("user.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
        )?;
        assert!(result.user_message.is_some());
        assert!(result.selection_is_new);
        assert_eq!(result.disposition, TurnDisposition::Dispatch);
        assert_eq!(result.selection.message_ids, vec![result.user_id]);
        Ok(())
    }

    #[test]
    fn legacy_identity_override_replays_existing_user() -> Result<()> {
        let operation = OperationId::new();
        let legacy = Uuid::from_bytes([42; 16]);
        let first = prepare_turn_with_user_id(
            &conversation()?,
            operation,
            file("user.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
            Some(legacy),
        )?;
        assert_eq!(first.user_id, legacy);
        let mut state = conversation()?;
        let content = first
            .user_message
            .as_ref()
            .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?
            .content
            .clone();
        state.append(
            first
                .user_message
                .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        )?;
        let retry = prepare_turn_with_user_id(
            &state,
            operation,
            content,
            inline_attachments(Vec::new()),
            limits(),
            Some(first.selection),
            false,
            true,
            Some(legacy),
        )?;
        assert_eq!(retry.user_id, legacy);
        assert!(!retry.append_user);
        Ok(())
    }

    #[test]
    fn changed_payload_is_rejected() -> Result<()> {
        let operation = OperationId::new();
        let first = prepare_turn(
            &conversation()?,
            operation,
            file("first.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
        )?;
        let mut state = conversation()?;
        state.append(
            first
                .user_message
                .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        )?;
        let error = match prepare_turn(
            &state,
            operation,
            file("second.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            Some(first.selection),
            false,
            true,
        ) {
            Ok(_) => return Err(Error::Invalid("changed payload was admitted".into())),
            Err(error) => error,
        };
        assert!(error.to_string().contains("another user message"));
        Ok(())
    }

    #[test]
    fn unresolved_prior_turn_is_rejected() -> Result<()> {
        let mut state = conversation()?;
        let first = prepare_turn(
            &state,
            OperationId::new(),
            file("first.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
        )?;
        state.append(
            first
                .user_message
                .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        )?;
        let error = match prepare_turn(
            &state,
            OperationId::new(),
            file("second.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
        ) {
            Ok(_) => return Err(Error::Invalid("unresolved turn was admitted".into())),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("previous conversation turn is unresolved")
        );
        Ok(())
    }

    #[test]
    fn stale_selection_is_rejected_before_reconcile() -> Result<()> {
        let operation = OperationId::new();
        let first = prepare_turn(
            &conversation()?,
            operation,
            file("first.txt")?,
            inline_attachments(Vec::new()),
            limits(),
            None,
            false,
            true,
        )?;
        let mut state = conversation()?;
        let content = first
            .user_message
            .as_ref()
            .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?
            .content
            .clone();
        state.append(
            first
                .user_message
                .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        )?;
        let mut stale = first.selection;
        stale.conversation_revision = 0;
        let error = match prepare_turn(
            &state,
            operation,
            content,
            inline_attachments(Vec::new()),
            limits(),
            Some(stale),
            false,
            true,
        ) {
            Ok(_) => return Err(Error::Invalid("stale selection was admitted".into())),
            Err(error) => error,
        };
        assert!(error.to_string().contains("stale conversation revision"));
        Ok(())
    }
}
