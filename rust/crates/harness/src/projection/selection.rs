use super::*;
use crate::conversation::{ConversationMessage, FileRef};
use crate::scheduler::diagnostic;
use crate::store::HistoryIo;
use crate::wire::ErrorCode::Conflict;
use uuid::Uuid;

/// Selects the bounded suffix ending in an admitted user message.
pub fn bounded_model_context_selection(
    conversation: &ConversationState,
    user_id: Uuid,
    maximum_messages: usize,
) -> Result<ModelContextSelection> {
    if maximum_messages == 0 {
        return Err(Error::Invalid(
            "model context message limit must be positive".into(),
        ));
    }
    let current = conversation
        .message(user_id)
        .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
    let end = conversation
        .message_position(current.id)
        .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?
        + 1;
    let ids = conversation
        .recent_model_messages(end, maximum_messages)
        .map(|message| message.id)
        .collect::<Vec<_>>();
    validate_tool_pairs(conversation, current, &ids)?;
    let selection = ModelContextSelection {
        checkpoint: None,
        conversation_revision: conversation.logical_revision(),
        message_ids: ids,
    };
    selection.validate(conversation)?;
    if selection.message_ids.last() != Some(&current.id) {
        return Err(Error::Conflict(
            "turn identity is bound to another context selection".into(),
        ));
    }
    Ok(selection)
}

/// Selects the canonical delta after an authenticated checkpoint.
pub fn select_turn_delta(
    conversation: &ConversationState,
    current: &ConversationMessage,
    append: bool,
    checkpoint: Option<(&crate::context::CanonicalContextCheckpoint, FileRef)>,
    limits: Limits,
) -> Result<ModelContextSelection> {
    limits.validate(&mut None)?;
    current.validate(&mut None)?;
    if current.kind != MessageKind::User || limits.context_messages == 0 {
        return Err(Error::Invalid(
            "canonical turn delta requires a bounded user input".into(),
        ));
    }
    if append {
        if conversation.message(current.id).is_some()
            || current.sequence != conversation.logical_revision() + 1
        {
            return Err(Error::Conflict(
                "new user does not extend canonical history".into(),
            ));
        }
    } else if conversation.message(current.id) != Some(current) {
        return Err(Error::Conflict(
            "current user differs from canonical history".into(),
        ));
    }
    let (after, checkpoint) = if let Some((envelope, reference)) = checkpoint {
        envelope.validate(limits, None)?;
        limits.validate_file(&reference, &mut None)?;
        reference
            .descriptor()
            .verify(&crate::contract::canonical_json_bytes(envelope)?, &mut None)?;
        if !conversation
            .checkpoint_covers_archived_prefix(&reference, envelope.selection.conversation_revision)
        {
            validate_model_context_selection(
                conversation,
                &envelope.selection,
                envelope.selection.conversation_revision,
                &mut None,
            )?;
        }
        if envelope.selection.conversation_revision >= current.sequence {
            return Err(Error::Conflict(
                "checkpoint already covers the current user".into(),
            ));
        }
        (envelope.selection.conversation_revision, Some(reference))
    } else {
        (0, None)
    };
    let mut ids = conversation
        .model_messages_between(
            after,
            current.sequence - u64::from(append),
            limits.context_messages - usize::from(append),
        )?
        .map(|message| message.id)
        .collect::<Vec<_>>();
    if append {
        ids.push(current.id);
    }
    validate_tool_pairs(conversation, current, &ids)?;
    if ids.last() != Some(&current.id) {
        return Err(Error::Conflict(
            "canonical turn delta has no current user".into(),
        ));
    }
    Ok(ModelContextSelection {
        conversation_revision: conversation.logical_revision() + u64::from(append),
        message_ids: ids,
        checkpoint,
    })
}

fn validate_tool_pairs(
    conversation: &ConversationState,
    current: &ConversationMessage,
    ids: &[Uuid],
) -> Result<()> {
    for id in ids {
        let message = if *id == current.id {
            Some(current)
        } else {
            conversation.message(*id)
        };
        if message.is_some_and(|message| {
            message.kind == MessageKind::ToolResult
                && !message.reply_to.is_some_and(|call| ids.contains(&call))
        }) {
            return Err(Error::Invalid(
                "context budget splits a tool exchange; compact or change selection".into(),
            ));
        }
    }
    Ok(())
}

/// Validates ordered identities and complete tool exchanges at the pinned revision.
pub fn validate_model_context_selection(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
    }
    if selection.conversation_revision != conversation_revision {
        return Err(diagnostic(
            work,
            Conflict,
            "model context selection has a stale conversation revision",
        ));
    }
    if let Some(checkpoint) = &selection.checkpoint {
        checkpoint.validate(work)?;
    }
    let mut previous = 0;
    for (index, id) in selection.message_ids.iter().enumerate() {
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let message = conversation
            .message(*id)
            .ok_or_else(|| diagnostic(work, Invalid, "selected conversation message is missing"))?;
        if message.sequence <= previous || message.sequence > conversation_revision {
            return Err(diagnostic(
                work,
                Invalid,
                "model context selection is not ordered and unique",
            ));
        }
        previous = message.sequence;
        message.validate(work)?;
        match message.kind {
            MessageKind::ToolCall
                if message.content.descriptor().media_type() != "application/json" =>
            {
                return Err(diagnostic(
                    work,
                    Invalid,
                    "tool artifact type or rendering limit is invalid",
                ));
            }
            MessageKind::ToolCall => {}
            MessageKind::ToolResult => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "tool result artifact type is invalid",
                    ));
                }
                let call_id = message
                    .tool_call_id
                    .as_deref()
                    .ok_or_else(|| diagnostic(work, Invalid, "tool result lacks call identity"))?;
                let mut linked = None;
                if let Some(reply) = message.reply_to {
                    for id in selection.message_ids[..index].iter().rev() {
                        if let Some(observer) = work.as_deref_mut() {
                            observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                                bytes: 0,
                                items: 1,
                            })?;
                        }
                        if *id == reply {
                            linked = conversation
                                .message(reply)
                                .filter(|call| call.kind == MessageKind::ToolCall)
                                .and_then(|call| call.tool_call_id.as_deref());
                            break;
                        }
                    }
                }
                if linked != Some(call_id) {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "selected tool result lacks its call",
                    ));
                }
            }
            MessageKind::System | MessageKind::User | MessageKind::Assistant => {}
            _ => {
                return Err(diagnostic(
                    work,
                    Unsupported,
                    "message kind requires a specialized model projection",
                ));
            }
        }
    }
    Ok(())
}
