use acyclic_harness_core::{
    AgentId, Error, Result,
    conversation::{
        ConversationMessage, ConversationState, FileDescriptor, FileRef, Limits, MessageKind,
        ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef,
    },
    resources::ProviderRef,
};
use std::collections::BTreeMap;
use uuid::Uuid;

#[path = "fixture.rs"]
mod fixture;
use fixture::file;

#[test]
fn descriptor_checks_exact_bytes() -> Result<()> {
    let descriptor = FileDescriptor::from_bytes(b"", "application/octet-stream", &mut None)?;
    descriptor.verify(b"", &mut None)?;
    assert!(descriptor.verify(b"x", &mut None).is_err());
    let encoded = serde_json::to_value(&descriptor).map_err(|e| Error::Invalid(e.to_string()))?;
    let decoded: FileDescriptor =
        serde_json::from_value(encoded).map_err(|e| Error::Invalid(e.to_string()))?;
    assert_eq!(decoded, descriptor);
    Ok(())
}

#[test]
fn configured_limits_reject_oversized_references() -> Result<()> {
    let path = "x".repeat(8193);
    let original = file(AgentId::new(), &path)?;
    let label = "λ".repeat(1025);
    let media_type = format!("text/{}", "a".repeat(1025));
    let long = FileRef::new(
        original.volume().clone(),
        path.clone(),
        label.clone(),
        FileDescriptor::from_bytes(b"hello", &media_type, &mut None)?,
        label,
        &mut None,
    )?;
    Limits::default().validate_file(&long, &mut None)?;
    let mut selected = Limits {
        path_bytes: std::num::NonZeroUsize::new(4096),
        ..Limits::default()
    };
    assert!(selected.validate_file(&long, &mut None).is_err());
    selected.path_bytes = std::num::NonZeroUsize::new(path.len());
    selected.validate_file(&long, &mut None)?;
    let encoded = serde_json::to_vec(&long).map_err(|error| Error::Invalid(error.to_string()))?;
    let decoded: FileRef =
        serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(decoded, long);
    let file = file(AgentId::new(), "message.txt")?;
    let mut limits = Limits {
        file_bytes: Some(4),
        ..Limits::default()
    };
    limits.validate(&mut None)?;
    assert!(limits.validate_file(&file, &mut None).is_err());
    limits.file_bytes = Some(5);
    limits.validate_file(&file, &mut None)?;
    limits.model_events_per_step = std::num::NonZeroUsize::new(1);
    limits.validate(&mut None)?;
    Limits {
        attachments: std::num::NonZeroUsize::new(65_537),
        model_events_per_step: std::num::NonZeroUsize::new(1_000_001),
        tool_calls_per_step: std::num::NonZeroUsize::new(1_000_001),
        context_messages: std::num::NonZeroUsize::new(1_000_001),
        ..Limits::default()
    }
    .validate(&mut None)?;
    Ok(())
}

#[test]
fn paths_and_ownership_are_validated_on_decode() -> Result<()> {
    let agent = AgentId::new();
    for path in ["/absolute", "../escape", "a//b", "a/./b", "a\\b"] {
        assert!(file(agent, path).is_err(), "{path}");
    }
    let mut encoded = serde_json::to_value(file(agent, "dir/a.txt")?)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    let path = encoded
        .get_mut("path")
        .ok_or_else(|| Error::Invalid("encoded file has no path".into()))?;
    *path = serde_json::json!("../escape");
    assert!(serde_json::from_value::<FileRef>(encoded).is_err());
    Ok(())
}

#[test]
fn conversation_enforces_binding_order_and_call_links() -> Result<()> {
    let agent = AgentId::new();
    let mut state = ConversationState::default();
    state.bind(agent)?;
    assert!(state.bind(agent).is_err());
    let call = ConversationMessage {
        id: Uuid::new_v4(),
        sequence: 1,
        kind: MessageKind::ToolCall,
        content: file(agent, "call.txt")?,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: Some("call-1".into()),
        extensions: BTreeMap::new(),
    };
    state.append(call.clone(), &mut None)?;
    assert!(state.append(call.clone(), &mut None).is_err());
    let result = ConversationMessage {
        id: Uuid::new_v4(),
        sequence: 2,
        kind: MessageKind::ToolResult,
        content: file(agent, "result.txt")?,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: Some(call.id),
        tool_call_id: Some("call-1".into()),
        extensions: BTreeMap::new(),
    };
    state.append(result, &mut None)?;
    assert_eq!(state.messages().len(), 2);
    Ok(())
}
