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
        ProviderRef::new("test".into(), "memory".into(), "1".into(), &mut None)?,
        "turns".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::new()),
        &mut None,
    )?;
    FileRef::new(
        volume,
        name.into(),
        "v1".into(),
        FileDescriptor::from_bytes(b"body", "text/plain", &mut None)?,
        name.into(),
        &mut None,
    )
}

fn manifest_file(name: &str) -> Result<FileRef> {
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "memory".into(), "1".into(), &mut None)?,
        "turns".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::new()),
        &mut None,
    )?;
    FileRef::new(
        volume,
        name.into(),
        "v1".into(),
        FileDescriptor::from_bytes(
            br"[]",
            "application/vnd.acyclic.harness.attachments+json",
            &mut None,
        )?,
        name.into(),
        &mut None,
    )
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
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        None,
        None,
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
    let first = prepare_turn(
        &conversation()?,
        operation,
        file("user.txt")?,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        Some(legacy),
        None,
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
        &mut None,
    )?;
    let retry = prepare_turn(
        &state,
        operation,
        content,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        Some(first.selection),
        false,
        true,
        Some(legacy),
        None,
    )?;
    assert_eq!(retry.user_id, legacy);
    assert!(!retry.append_user);
    Ok(())
}

#[test]
fn manifest_count_uses_turn_attachment_limit() -> Result<()> {
    let mut limits = limits();
    limits.attachments = 1;
    let Err(error) = prepare_turn(
        &conversation()?,
        OperationId::new(),
        file("user.txt")?,
        ReferencedAttachments::Manifest {
            manifest: manifest_file("attachments.json")?,
            item_count: 2,
        },
        limits,
        None,
        false,
        true,
        None,
        None,
    ) else {
        return Err(Error::Invalid(
            "manifest count above the turn limit was admitted".into(),
        ));
    };
    assert!(
        error
            .to_string()
            .contains("turn attachments exceed harness limits")
    );
    Ok(())
}

#[test]
fn changed_payload_is_rejected() -> Result<()> {
    let operation = OperationId::new();
    let first = prepare_turn(
        &conversation()?,
        operation,
        file("first.txt")?,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        None,
        None,
    )?;
    let mut state = conversation()?;
    state.append(
        first
            .user_message
            .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        &mut None,
    )?;
    let Err(error) = prepare_turn(
        &state,
        operation,
        file("second.txt")?,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        Some(first.selection),
        false,
        true,
        None,
        None,
    ) else {
        return Err(Error::Invalid("changed payload was admitted".into()));
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
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        None,
        None,
    )?;
    state.append(
        first
            .user_message
            .ok_or_else(|| Error::Invalid("planner omitted user message".into()))?,
        &mut None,
    )?;
    let Err(error) = prepare_turn(
        &state,
        OperationId::new(),
        file("second.txt")?,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        None,
        None,
    ) else {
        return Err(Error::Invalid("unresolved turn was admitted".into()));
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
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        None,
        false,
        true,
        None,
        None,
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
        &mut None,
    )?;
    let mut stale = first.selection;
    stale.conversation_revision = 0;
    let Err(error) = prepare_turn(
        &state,
        operation,
        content,
        ReferencedAttachments::Inline { items: Vec::new() },
        limits(),
        Some(stale),
        false,
        true,
        None,
        None,
    ) else {
        return Err(Error::Invalid("stale selection was admitted".into()));
    };
    assert!(error.to_string().contains("stale conversation revision"));
    Ok(())
}
#[test]
fn default_turn_rejects_history_overflow_instead_of_selecting_a_suffix() -> Result<()> {
    let mut state = conversation()?;
    let historical_user = Uuid::new_v4();
    state.append(
        ConversationMessage {
            id: historical_user,
            sequence: 1,
            kind: MessageKind::User,
            content: file("historical-user.txt")?,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        },
        &mut None,
    )?;
    state.append(
        ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 2,
            kind: MessageKind::Assistant,
            content: file("historical-assistant.txt")?,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: Some(historical_user),
            tool_call_id: None,
            extensions: Default::default(),
        },
        &mut None,
    )?;
    let operation = OperationId::new();
    let mut bounded = limits();
    bounded.context_messages = 2;
    assert!(
        prepare_turn(
            &state,
            operation,
            file("current.txt")?,
            ReferencedAttachments::Inline { items: Vec::new() },
            bounded,
            None,
            false,
            true,
            None,
            None,
        )
        .is_err()
    );
    bounded.context_messages = 3;
    let prepared = prepare_turn(
        &state,
        operation,
        file("current.txt")?,
        ReferencedAttachments::Inline { items: Vec::new() },
        bounded,
        None,
        false,
        true,
        None,
        None,
    )?;
    assert_eq!(prepared.selection.message_ids.len(), 3);
    assert_eq!(prepared.selection.message_ids[0], historical_user);
    assert_eq!(
        prepared.selection.message_ids[2],
        canonical_user_message_id(operation)
    );
    assert!(prepared.selection.checkpoint.is_none());
    assert_eq!(state.messages().len(), 2);
    Ok(())
}
