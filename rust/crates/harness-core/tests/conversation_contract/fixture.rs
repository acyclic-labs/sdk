use super::{
    AgentId, BTreeMap, ConversationMessage, FileDescriptor, FileRef, MessageKind, ProviderRef,
    ReferencedAttachments, Result, Uuid, VolumeClass, VolumeOwner, VolumeRef,
};

pub(crate) fn file(agent: AgentId, path: &str) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?,
        (path).into(),
        ("version-1").into(),
        FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?,
        ("text.txt").into(),
        &mut None,
    )
}

pub(crate) fn message(content: &FileRef, sequence: u64) -> ConversationMessage {
    ConversationMessage {
        id: Uuid::from_u128(u128::from(sequence)),
        sequence,
        kind: if sequence % 2 == 1 {
            MessageKind::User
        } else {
            MessageKind::Assistant
        },
        content: content.clone(),
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: sequence
            .is_multiple_of(2)
            .then(|| Uuid::from_u128(u128::from(sequence - 1))),
        tool_call_id: None,
        extensions: BTreeMap::new(),
    }
}
