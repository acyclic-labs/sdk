//! Validated conversion between v2 protobuf values and canonical Rust values.

use crate::{
    AgentId, Error, Result,
    conversation::{
        Attachment, ConversationMessage, FileDescriptor, FileRef, MessageKind,
        ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef,
    },
    resources::ProviderRef,
    wire,
};
use std::collections::BTreeMap;
use uuid::Uuid;

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| Error::Invalid(format!("missing v2 field: {name}")))
}

impl TryFrom<wire::ProviderRef> for ProviderRef {
    type Error = Error;
    fn try_from(value: wire::ProviderRef) -> Result<Self> {
        Self::new(value.namespace, value.family, value.version)
    }
}

impl From<&ProviderRef> for wire::ProviderRef {
    fn from(value: &ProviderRef) -> Self {
        Self {
            namespace: value.namespace().into(),
            family: value.family().into(),
            version: value.version().into(),
        }
    }
}

impl TryFrom<wire::VolumeRef> for VolumeRef {
    type Error = Error;
    fn try_from(value: wire::VolumeRef) -> Result<Self> {
        let provider = required(value.provider, "volume.provider")?.try_into()?;
        let class = match wire::VolumeClass::try_from(value.volume_class) {
            Ok(wire::VolumeClass::Project) => VolumeClass::Project,
            Ok(wire::VolumeClass::AgentPrivate) => VolumeClass::AgentPrivate,
            Ok(wire::VolumeClass::SessionShared) => VolumeClass::SessionShared,
            _ => return Err(Error::Invalid("unknown or unspecified volume class".into())),
        };
        let owner = match required(
            required(value.owner, "volume.owner")?.owner,
            "volume.owner.kind",
        )? {
            wire::volume_owner::Owner::Project(id) => VolumeOwner::Project(id),
            wire::volume_owner::Owner::AgentId(id) => VolumeOwner::Agent(AgentId::parse(&id)?),
            wire::volume_owner::Owner::Session(id) => VolumeOwner::Session(id),
        };
        Self::new(provider, value.id, class, owner)
    }
}

impl From<&VolumeRef> for wire::VolumeRef {
    fn from(value: &VolumeRef) -> Self {
        let owner = match value.owner() {
            VolumeOwner::Project(id) => wire::volume_owner::Owner::Project(id.clone()),
            VolumeOwner::Agent(id) => wire::volume_owner::Owner::AgentId(id.to_string()),
            VolumeOwner::Session(id) => wire::volume_owner::Owner::Session(id.clone()),
        };
        Self {
            provider: Some(value.provider().into()),
            id: value.id().into(),
            volume_class: match value.class() {
                VolumeClass::Project => wire::VolumeClass::Project as i32,
                VolumeClass::AgentPrivate => wire::VolumeClass::AgentPrivate as i32,
                VolumeClass::SessionShared => wire::VolumeClass::SessionShared as i32,
            },
            owner: Some(wire::VolumeOwner { owner: Some(owner) }),
        }
    }
}

impl TryFrom<wire::FileDescriptor> for FileDescriptor {
    type Error = Error;
    fn try_from(value: wire::FileDescriptor) -> Result<Self> {
        let digest = value
            .sha256
            .try_into()
            .map_err(|_| Error::Invalid("SHA-256 descriptor must be 32 bytes".into()))?;
        Self::new(digest, value.byte_length, value.media_type)
    }
}

impl From<&FileDescriptor> for wire::FileDescriptor {
    fn from(value: &FileDescriptor) -> Self {
        Self {
            sha256: value.sha256().to_vec(),
            byte_length: value.byte_length(),
            media_type: value.media_type().into(),
        }
    }
}

impl TryFrom<wire::FileRef> for FileRef {
    type Error = Error;
    fn try_from(value: wire::FileRef) -> Result<Self> {
        Self::new(
            required(value.volume, "file.volume")?.try_into()?,
            value.normalized_path,
            value.immutable_version,
            required(value.descriptor, "file.descriptor")?.try_into()?,
            value.display_name,
        )
    }
}

impl From<&FileRef> for wire::FileRef {
    fn from(value: &FileRef) -> Self {
        Self {
            volume: Some(value.volume().into()),
            normalized_path: value.path().into(),
            immutable_version: value.version().into(),
            descriptor: Some(value.descriptor().into()),
            display_name: value.display_name().into(),
        }
    }
}

impl TryFrom<wire::Attachment> for Attachment {
    type Error = Error;
    fn try_from(value: wire::Attachment) -> Result<Self> {
        let result = Self {
            file: required(value.file, "attachment.file")?.try_into()?,
            label: value.label,
        };
        result.validate()?;
        Ok(result)
    }
}

impl From<&Attachment> for wire::Attachment {
    fn from(value: &Attachment) -> Self {
        Self {
            file: Some((&value.file).into()),
            label: value.label.clone(),
        }
    }
}

impl TryFrom<wire::ReferencedAttachments> for ReferencedAttachments {
    type Error = Error;
    fn try_from(value: wire::ReferencedAttachments) -> Result<Self> {
        let result = match required(value.source, "attachments.source")? {
            wire::referenced_attachments::Source::InlineItems(items) => Self::Inline {
                items: items
                    .items
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_>>()?,
            },
            wire::referenced_attachments::Source::Manifest(value) => Self::Manifest {
                manifest: required(value.manifest, "attachments.manifest")?.try_into()?,
                item_count: value.item_count,
            },
        };
        result.validate()?;
        Ok(result)
    }
}

impl From<&ReferencedAttachments> for wire::ReferencedAttachments {
    fn from(value: &ReferencedAttachments) -> Self {
        let source = match value {
            ReferencedAttachments::Inline { items } => {
                wire::referenced_attachments::Source::InlineItems(wire::AttachmentItems {
                    items: items.iter().map(Into::into).collect(),
                })
            }
            ReferencedAttachments::Manifest {
                manifest,
                item_count,
            } => wire::referenced_attachments::Source::Manifest(wire::AttachmentManifest {
                manifest: Some(manifest.into()),
                item_count: *item_count,
            }),
        };
        Self {
            source: Some(source),
        }
    }
}

impl TryFrom<wire::ConversationMessage> for ConversationMessage {
    type Error = Error;
    fn try_from(value: wire::ConversationMessage) -> Result<Self> {
        let kind = match wire::ConversationKind::try_from(value.kind) {
            Ok(wire::ConversationKind::User) => MessageKind::User,
            Ok(wire::ConversationKind::Assistant) => MessageKind::Assistant,
            Ok(wire::ConversationKind::System) => MessageKind::System,
            Ok(wire::ConversationKind::ToolCall) => MessageKind::ToolCall,
            Ok(wire::ConversationKind::ToolResult) => MessageKind::ToolResult,
            Ok(wire::ConversationKind::Interaction) => MessageKind::Interaction,
            Ok(wire::ConversationKind::Permission) => MessageKind::Permission,
            Ok(wire::ConversationKind::Fork) => MessageKind::Fork,
            Ok(wire::ConversationKind::Merge) => MessageKind::Merge,
            _ => {
                return Err(Error::Invalid(
                    "unknown or unspecified conversation kind".into(),
                ));
            }
        };
        let result = Self {
            id: Uuid::parse_str(&value.id).map_err(|error| Error::Invalid(error.to_string()))?,
            sequence: value.sequence,
            kind,
            content: required(value.content, "message.content")?.try_into()?,
            attachments: required(value.attachments, "message.attachments")?.try_into()?,
            reply_to: value
                .reply_to
                .map(|id| Uuid::parse_str(&id).map_err(|error| Error::Invalid(error.to_string())))
                .transpose()?,
            tool_call_id: value.tool_call_id,
            extensions: value
                .extensions
                .into_iter()
                .map(|(name, file)| Ok((name, file.try_into()?)))
                .collect::<Result<BTreeMap<_, _>>>()?,
        };
        result.validate()?;
        Ok(result)
    }
}

impl From<&ConversationMessage> for wire::ConversationMessage {
    fn from(value: &ConversationMessage) -> Self {
        Self {
            id: value.id.to_string(),
            sequence: value.sequence,
            kind: match value.kind {
                MessageKind::User => wire::ConversationKind::User,
                MessageKind::Assistant => wire::ConversationKind::Assistant,
                MessageKind::System => wire::ConversationKind::System,
                MessageKind::ToolCall => wire::ConversationKind::ToolCall,
                MessageKind::ToolResult => wire::ConversationKind::ToolResult,
                MessageKind::Interaction => wire::ConversationKind::Interaction,
                MessageKind::Permission => wire::ConversationKind::Permission,
                MessageKind::Fork => wire::ConversationKind::Fork,
                MessageKind::Merge => wire::ConversationKind::Merge,
            } as i32,
            content: Some((&value.content).into()),
            attachments: Some((&value.attachments).into()),
            reply_to: value.reply_to.map(|id| id.to_string()),
            tool_call_id: value.tool_call_id.clone(),
            extensions: value
                .extensions
                .iter()
                .map(|(name, file)| (name.clone(), file.into()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    #[test]
    fn conversation_fixture_round_trips_strict_v2_protobuf() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/conversation-message.json");
        let canonical: ConversationMessage =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        let encoded = wire::ConversationMessage::from(&canonical).encode_to_vec();
        let decoded = wire::ConversationMessage::decode(encoded.as_slice())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(ConversationMessage::try_from(decoded)?, canonical);
        let mut invalid = wire::ConversationMessage::from(&canonical);
        invalid
            .content
            .as_mut()
            .ok_or_else(|| Error::Invalid("fixture content missing".into()))?
            .descriptor
            .as_mut()
            .ok_or_else(|| Error::Invalid("fixture descriptor missing".into()))?
            .sha256
            .pop();
        assert!(ConversationMessage::try_from(invalid).is_err());
        Ok(())
    }
}
