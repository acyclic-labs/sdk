use super::{
    AgentId, ConversationMessage, FileDescriptor, FileRef, MessageKind, ProviderRef,
    ReferencedAttachments, Result, VolumeClass, VolumeOwner, VolumeRef,
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) fn file(agent: AgentId, path: &str, media_type: &str, bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?,
        path.into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
        "data".into(),
        &mut None,
    )
}

pub(super) fn message(
    sequence: u64,
    kind: MessageKind,
    content: FileRef,
    reply_to: Option<Uuid>,
) -> ConversationMessage {
    ConversationMessage {
        id: Uuid::new_v4(),
        sequence,
        kind,
        content,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to,
        tool_call_id: None,
        extensions: BTreeMap::new(),
    }
}
