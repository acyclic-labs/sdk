//! Deterministic admission planning for one ref-only conversation turn.

use crate::{
    Error, OperationId, Result,
    conversation::{
        ConversationMessage, ConversationState, FileRef, Limits, MessageKind,
        ModelContextSelection, ReferencedAttachments,
    },
    projection::{select_turn_delta, validate_model_context_selection},
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

/// Prepare one deterministic turn with optional identity and checkpoint.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one deterministic admission sequence"
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
    user_id_override: Option<Uuid>,
    checkpoint: Option<(&crate::context::CanonicalContextCheckpoint, FileRef)>,
) -> Result<TurnPreparation> {
    limits.validate_file(&content, &mut None)?;
    attachments.validate(&mut None)?;
    if let ReferencedAttachments::Inline { items } = &attachments {
        if items.len() > limits.attachments {
            return Err(Error::Invalid(
                "turn attachments exceed harness limits".into(),
            ));
        }
        for attachment in items {
            attachment.validate(&mut None)?;
            limits.validate_file(&attachment.file, &mut None)?;
        }
    } else if let ReferencedAttachments::Manifest {
        manifest,
        item_count,
    } = &attachments
    {
        if *item_count as usize > limits.attachments {
            return Err(Error::Invalid(
                "turn attachments exceed harness limits".into(),
            ));
        }
        limits.validate_file(manifest, &mut None)?;
    }

    let user_id = user_id_override.unwrap_or_else(|| canonical_user_message_id(operation_id));
    if conversation
        .unresolved_user()
        .is_some_and(|id| id != user_id)
    {
        return Err(Error::Conflict(
            "previous conversation turn is unresolved; retry it before admitting another".into(),
        ));
    }
    let current = conversation.message(user_id);
    if let Some(outcome) = conversation.turn_outcome(user_id) {
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

    let user_sequence = current.map_or(conversation.logical_revision() + 1, |message| {
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

    let append_user = current.is_none();
    if append_user {
        conversation.validate_append(&user_message, &mut None)?;
    }
    let selection_is_new = existing_selection.is_none();
    let selection = match existing_selection {
        Some(selection) => {
            // Validate the committed selection against its original revision.
            if selection.conversation_revision < user_message.sequence
                || selection.conversation_revision > conversation.logical_revision()
            {
                return Err(Error::Conflict(
                    "model context selection has a stale conversation revision".into(),
                ));
            }
            validate_model_context_selection(
                conversation,
                &selection,
                selection.conversation_revision,
                &mut None,
            )?;
            if selection.message_ids.last() != Some(&user_id) {
                return Err(Error::Conflict(
                    "turn identity is bound to another context selection".into(),
                ));
            }
            selection
        }
        None => select_turn_delta(conversation, &user_message, append_user, checkpoint, limits)?,
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

#[cfg(test)]
mod tests;
