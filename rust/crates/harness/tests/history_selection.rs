//! Public-API regression coverage for bounded candidate history selection.

use acyclic_harness::{
    AgentId, OperationId, Result,
    conversation::{
        ConversationMessage, ConversationState, FileDescriptor, FileRef, Limits, MessageKind,
        ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef,
    },
    projection::bounded_model_context_selection,
    resources::ProviderRef,
    turn::prepare_turn,
};
use uuid::Uuid;

#[test]
fn bounded_candidate_selection_rejects_orphan_results_before_mutating_history() -> Result<()> {
    let agent = AgentId::new();
    let content = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test", "memory", "1")?,
            "history-selection",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?,
        "message.json",
        "1",
        FileDescriptor::from_bytes(b"{}", "application/json")?,
        "message.json",
    )?;
    let call = Uuid::new_v4();
    let result = Uuid::new_v4();
    let mut history = ConversationState::default();
    history.bind(agent)?;
    for (id, sequence, kind, reply_to) in [
        (call, 1, MessageKind::ToolCall, None),
        (result, 2, MessageKind::ToolResult, Some(call)),
    ] {
        history.append(ConversationMessage {
            id,
            sequence,
            kind,
            content: content.clone(),
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to,
            tool_call_id: Some("call".into()),
            extensions: Default::default(),
        })?;
    }
    let wire = serde_json::to_vec(&history)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let reopened: ConversationState = serde_json::from_slice(&wire)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, history);
    let operation = OperationId::new();
    let narrow = Limits {
        context_messages: 2,
        ..Limits::default()
    };
    let error = prepare_turn(
        &reopened,
        operation,
        content.clone(),
        ReferencedAttachments::Inline { items: Vec::new() },
        narrow,
        None,
        false,
        true,
    )
    .expect_err("default turn admitted history beyond its explicit bound");
    assert!(
        error
            .to_string()
            .contains("canonical history delta exceeds context limit")
    );
    assert_eq!(
        serde_json::to_vec(&reopened)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
        wire
    );
    let selected = prepare_turn(
        &reopened,
        operation,
        content,
        ReferencedAttachments::Inline { items: Vec::new() },
        Limits {
            context_messages: 3,
            ..narrow
        },
        None,
        false,
        true,
    )?;
    assert_eq!(
        selected.selection.message_ids,
        vec![call, result, selected.user_id]
    );
    assert!(selected.append_user);
    // Explicit suffix selection still rejects an orphan result independently
    // of the default planner's earlier whole-history overflow rejection.
    let mut candidate = reopened.clone();
    candidate.append(selected.user_message.clone().ok_or_else(|| {
        acyclic_harness::Error::Invalid("new turn omitted its user message".into())
    })?)?;
    let orphan = bounded_model_context_selection(&candidate, selected.user_id, 2)
        .expect_err("explicit suffix admitted an orphan tool result");
    assert!(orphan.to_string().contains("splits a tool exchange"));
    assert_eq!(
        bounded_model_context_selection(&candidate, selected.user_id, 3)?.message_ids,
        selected.selection.message_ids
    );
    assert_eq!(
        serde_json::to_vec(&reopened)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
        wire
    );
    Ok(())
}
