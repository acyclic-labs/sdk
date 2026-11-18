use acyclic_harness_core::{
    AgentId, Error, OperationId, Result,
    conversation::{
        ConversationMessage, ConversationState, FileDescriptor, FileRef, Limits, MessageKind,
        ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef,
    },
    model::{ModelContent, ModelMessage, ModelRole},
    projection::{ModelContextSelection, SelectedModelContext, select_turn_delta},
    resources::ProviderRef,
};
use uuid::Uuid;

#[path = "fixture.rs"]
mod fixture;
use fixture::{file, message};

fn append(
    conversation: &mut ConversationState,
    content: &FileRef,
    kind: MessageKind,
    reply: Option<Uuid>,
) -> Result<Uuid> {
    let message = message(
        conversation.logical_revision() + 1,
        kind,
        content.clone(),
        reply,
    );
    let id = message.id;
    conversation.append(message, &mut None)?;
    Ok(id)
}

#[test]
fn canonical_checkpoint_selects_exact_bounded_delta_from_ten_thousand_records() -> Result<()> {
    let agent = AgentId::new();
    let mut conversation = ConversationState::default();
    conversation.bind(agent)?;
    let content = file(agent, "message.txt", "text/plain", b"data")?;
    let mut recent = [Uuid::nil(); 4];
    for _ in 0..5_000 {
        let user = append(&mut conversation, &content, MessageKind::User, None)?;
        let assistant = append(
            &mut conversation,
            &content,
            MessageKind::Assistant,
            Some(user),
        )?;
        let [_, _, previous_user, previous_assistant] = recent;
        recent = [previous_user, previous_assistant, user, assistant];
    }
    let [
        checkpoint_user,
        checkpoint_assistant,
        last_user,
        last_assistant,
    ] = recent;
    let current = message(10_001, MessageKind::User, content, None);
    let limits = Limits {
        context_messages: std::num::NonZeroUsize::new(3),
        ..Limits::default()
    };
    assert!(select_turn_delta(&conversation, &current, true, None, limits).is_err());
    let payload = file(agent, "source.json", "application/json", b"data")?;
    let checkpoint = acyclic_harness_core::context::CanonicalContextCheckpoint {
        operation_id: OperationId::new(),
        selection: ModelContextSelection {
            conversation_revision: 9_998,
            message_ids: vec![checkpoint_user, checkpoint_assistant],
            checkpoint: Some(file(agent, "previous.json", "application/json", b"data")?),
        },
        source: payload.clone(),
        retained: payload.clone(),
        compaction: payload,
    };
    let bytes = acyclic_harness_core::contract::canonical_json_bytes(&checkpoint)?;
    let pinned = file(agent, "checkpoint.json", "application/json", &bytes)?;
    let selected = select_turn_delta(
        &conversation,
        &current,
        true,
        Some((&checkpoint, pinned.clone())),
        limits,
    )?;
    assert_eq!(
        selected.message_ids,
        vec![last_user, last_assistant, current.id]
    );
    assert_eq!(selected.checkpoint, Some(pinned.clone()));
    assert_eq!(selected.conversation_revision, 10_001);
    assert!(
        select_turn_delta(
            &conversation,
            &current,
            true,
            Some((&checkpoint, pinned.clone())),
            Limits {
                context_messages: std::num::NonZeroUsize::new(2),
                ..limits
            }
        )
        .is_err()
    );
    let mut changed = checkpoint.clone();
    changed.operation_id = OperationId::new();
    assert!(
        select_turn_delta(
            &conversation,
            &current,
            true,
            Some((&changed, pinned.clone())),
            limits
        )
        .is_err()
    );
    conversation.append(current.clone(), &mut None)?;
    assert_eq!(
        select_turn_delta(
            &conversation,
            &current,
            false,
            Some((&checkpoint, pinned)),
            limits
        )?,
        selected
    );
    Ok(())
}

#[test]
fn dispatch_admission_checks_bounds_and_current_user() -> Result<()> {
    let mut selected = SelectedModelContext {
        selection: ModelContextSelection {
            checkpoint: None,
            conversation_revision: 1,
            message_ids: vec![Uuid::new_v4()],
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("hello".into()),
        }],
    };
    assert!(selected.validate_for_dispatch(Limits::default()).is_ok());
    assert!(
        selected
            .validate_for_dispatch(Limits {
                render_bytes: Some(4),
                ..Limits::default()
            })
            .is_err()
    );
    selected
        .messages
        .first_mut()
        .ok_or_else(|| Error::Invalid("missing fixture message".into()))?
        .role = ModelRole::Assistant;
    assert!(selected.validate_for_dispatch(Limits::default()).is_err());
    selected
        .messages
        .first_mut()
        .ok_or_else(|| Error::Invalid("missing fixture message".into()))?
        .role = ModelRole::User;
    selected.selection.message_ids.clear();
    assert!(selected.validate_for_dispatch(Limits::default()).is_err());
    Ok(())
}
