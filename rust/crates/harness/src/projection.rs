//! Explicit, provenance-preserving selection from canonical history to model context.

pub use crate::conversation::ModelContextSelection;
use crate::{
    Error, Result,
    conversation::{
        Attachment, ContentResidencyVerifier, ConversationMessage, ConversationState, FileRef,
        MessageKind, ReferencedAttachments,
    },
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole},
    tool::ToolInvocation,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use uuid::Uuid;

/// Provider-owned, grant-checked resolver for a complete attachment manifest.
pub trait AttachmentListResolver: Send + Sync {
    /// Verifies the pinned manifest bytes and returns its complete ordered list.
    fn resolve<'a>(
        &'a self,
        manifest: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>>;

    /// Reads one bounded, exact structured artifact for tool-context projection.
    fn read<'a>(&'a self, _file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "tool artifact resolution is unavailable".into(),
            ))
        })
    }
}

impl<T: ContentResidencyVerifier + ?Sized> AttachmentListResolver for T {
    fn resolve<'a>(
        &'a self,
        manifest: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move { self.load_manifest(manifest, item_count).await })
    }

    fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        ContentResidencyVerifier::read(self, file)
    }
}

/// Transient model context with explicit canonical provenance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectedModelContext {
    /// Selection and exact history revision.
    pub selection: ModelContextSelection,
    /// Typed model-visible values; do not append them to conversation history.
    pub messages: Vec<ModelMessage>,
}

impl SelectedModelContext {
    /// Checks that a projected selection preserves one-to-one provenance and
    /// ends in the exact current user input rather than duplicating it.
    pub fn validate_for_input(&self, input: &ModelContent) -> Result<()> {
        if self.messages.is_empty()
            || self.messages.len() != self.selection.message_ids.len()
            || self
                .messages
                .last()
                .is_none_or(|last| last.role != ModelRole::User || &last.content != input)
        {
            return Err(Error::Invalid(
                "selected context must end with the current user input".into(),
            ));
        }
        Ok(())
    }
}

/// Selects a bounded ordered subset. Text and attachments stay as refs;
/// structured tool artifacts are resolved only within the render bound.
pub async fn select_model_context<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
) -> Result<SelectedModelContext> {
    selection.validate(conversation)?;
    if maximum_messages == 0
        || maximum_render_bytes == 0
        || selection.message_ids.len() > maximum_messages
    {
        return Err(Error::Invalid(
            "selected message count exceeds projection limit".into(),
        ));
    }
    let by_id = conversation
        .messages
        .iter()
        .map(|message| (message.id, message))
        .collect::<HashMap<_, _>>();
    if by_id.len() != conversation.messages.len() {
        return Err(Error::Invalid(
            "conversation has duplicate message identities".into(),
        ));
    }
    let mut messages = Vec::with_capacity(selection.message_ids.len());
    let mut tool_calls: HashMap<Uuid, (String, String)> = HashMap::new();
    for id in &selection.message_ids {
        let message = by_id
            .get(id)
            .copied()
            .ok_or_else(|| Error::Invalid("selected conversation message is missing".into()))?;
        message.validate()?;
        if message.kind == MessageKind::ToolCall {
            let invocation: ToolInvocation =
                read_json_artifact(resolver, &message.content, maximum_render_bytes).await?;
            invocation.validate()?;
            if message.tool_call_id.as_deref() != Some(invocation.call_id.as_str()) {
                return Err(Error::Invalid(
                    "tool call artifact identity does not match its record".into(),
                ));
            }
            tool_calls.insert(
                message.id,
                (invocation.call_id.clone(), invocation.name.clone()),
            );
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: invocation.call_id,
                    name: invocation.name,
                    arguments: invocation.arguments,
                }),
            });
            continue;
        }
        if message.kind == MessageKind::ToolResult {
            // The complete result remains an immutable artifact. Only its
            // separately bounded projection is model-visible here.
            if message.content.descriptor().media_type() != "application/json" {
                return Err(Error::Invalid(
                    "tool result artifact type is invalid".into(),
                ));
            }
            let call_id = message
                .tool_call_id
                .as_deref()
                .ok_or_else(|| Error::Invalid("tool result lacks call identity".into()))?;
            let (linked_call_id, name) = message
                .reply_to
                .and_then(|id| tool_calls.get(&id))
                .ok_or_else(|| Error::Invalid("selected tool result lacks its call".into()))?;
            if linked_call_id != call_id {
                return Err(Error::Invalid(
                    "selected tool result has a mismatched call".into(),
                ));
            }
            let attachments = resolve_attachments(message, resolver, maximum_attachments).await?;
            let projection = attachments
                .iter()
                .find(|attachment| attachment.label.as_deref() == Some("model_projection"))
                .ok_or_else(|| Error::Invalid("tool result lacks its model projection".into()))?;
            let value =
                read_json_artifact(resolver, &projection.file, maximum_render_bytes).await?;
            messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: call_id.to_owned(),
                    name: name.clone(),
                    value,
                }),
            });
            continue;
        }
        let role = match message.kind {
            MessageKind::System => ModelRole::System,
            MessageKind::User => ModelRole::User,
            MessageKind::Assistant => ModelRole::Assistant,
            _ => {
                return Err(Error::Unsupported(format!(
                    "message kind {:?} requires a specialized model projection",
                    message.kind
                )));
            }
        };
        let primary_policy = match message.content.descriptor().media_type() {
            "image/png" | "image/jpeg" | "image/gif" | "image/webp" => FileProjectionPolicy::Native,
            kind if kind.starts_with("text/")
                && message.content.descriptor().byte_length() <= maximum_render_bytes =>
            {
                FileProjectionPolicy::BoundedFull
            }
            _ => FileProjectionPolicy::Reference,
        };
        let mut parts = vec![ModelContentPart::File {
            file: message.content.clone(),
            policy: primary_policy,
        }];
        if let ReferencedAttachments::Manifest { item_count, .. } = &message.attachments
            && (*item_count as usize) > maximum_attachments
        {
            return Err(Error::Invalid(
                "selected message exceeds attachment projection limit".into(),
            ));
        }
        let attachments = resolve_attachments(message, resolver, maximum_attachments).await?;
        let projected_count = attachments.len().min(1_022);
        let omitted_count = attachments.len() - projected_count;
        for attachment in attachments.into_iter().take(projected_count) {
            attachment.validate()?;
            let policy = match attachment.file.descriptor().media_type() {
                "image/png" | "image/jpeg" | "image/gif" | "image/webp" => {
                    FileProjectionPolicy::Native
                }
                _ => FileProjectionPolicy::Reference,
            };
            parts.push(ModelContentPart::File {
                file: attachment.file,
                policy,
            });
        }
        if omitted_count > 0 {
            parts.push(ModelContentPart::Text {
                text: format!("[{omitted_count} additional attachments omitted from this bounded model context]"),
            });
        }
        messages.push(ModelMessage {
            role,
            content: ModelContent::Parts(parts),
        });
    }
    Ok(SelectedModelContext {
        selection,
        messages,
    })
}

async fn read_json_artifact<T: DeserializeOwned, R: AttachmentListResolver + ?Sized>(
    resolver: &R,
    file: &FileRef,
    maximum_bytes: u64,
) -> Result<T> {
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(Error::Invalid(
            "tool artifact type or rendering limit is invalid".into(),
        ));
    }
    let bytes = resolver.read(file).await?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "resolved tool artifact exceeds rendering limit".into(),
        ));
    }
    file.descriptor().verify(&bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))
}

async fn resolve_attachments<R: AttachmentListResolver + ?Sized>(
    message: &ConversationMessage,
    resolver: &R,
    maximum_attachments: usize,
) -> Result<Vec<Attachment>> {
    let attachments = match &message.attachments {
        ReferencedAttachments::Inline { items } => items.clone(),
        ReferencedAttachments::Manifest {
            manifest,
            item_count,
        } => {
            if *item_count as usize > maximum_attachments {
                return Err(Error::Invalid(
                    "selected message exceeds attachment projection limit".into(),
                ));
            }
            let items = resolver.resolve(manifest, *item_count).await?;
            if items.len() != *item_count as usize {
                return Err(Error::Invalid(
                    "attachment manifest count does not match its declared count".into(),
                ));
            }
            items
        }
    };
    if attachments.len() > maximum_attachments {
        return Err(Error::Invalid(
            "selected message exceeds attachment projection limit".into(),
        ));
    }
    Ok(attachments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{
        ConversationMessage, FileDescriptor, VolumeClass, VolumeOwner, VolumeRef,
    };
    use crate::{AgentId, resources::ProviderRef};
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use uuid::Uuid;

    struct Resolver {
        items: Vec<Attachment>,
        calls: AtomicUsize,
    }

    impl AttachmentListResolver for Resolver {
        fn resolve<'a>(
            &'a self,
            _manifest: &'a FileRef,
            _item_count: u32,
        ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(self.items.clone()) })
        }
    }

    fn file(agent: AgentId, path: &str, media_type: &str) -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            path,
            "v1",
            FileDescriptor::from_bytes(b"data", media_type)?,
            "data",
        )
    }

    #[tokio::test]
    async fn manifest_projection_checks_limit_before_resolution_and_exact_count_after() -> Result<()>
    {
        let agent = AgentId::new();
        let id = Uuid::new_v4();
        let mut conversation = ConversationState::default();
        conversation.bind(agent)?;
        conversation.append(ConversationMessage {
            id,
            sequence: 1,
            kind: MessageKind::User,
            content: file(agent, "message.txt", "text/plain")?,
            attachments: ReferencedAttachments::Manifest {
                manifest: file(
                    agent,
                    "manifest.json",
                    "application/vnd.acyclic.harness.attachments+json",
                )?,
                item_count: 2,
            },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        })?;
        let selection = ModelContextSelection {
            conversation_revision: 1,
            message_ids: vec![id],
        };
        let attachment = Attachment {
            file: file(agent, "image.png", "image/png")?,
            label: None,
        };
        let resolver = Resolver {
            items: vec![attachment.clone()],
            calls: AtomicUsize::new(0),
        };
        assert!(
            select_model_context(
                &conversation,
                selection.clone(),
                &resolver,
                256,
                1,
                128 * 1024
            )
            .await
            .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            select_model_context(
                &conversation,
                selection.clone(),
                &resolver,
                256,
                2,
                128 * 1024
            )
            .await
            .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        let resolver = Resolver {
            items: vec![attachment.clone(), attachment],
            calls: AtomicUsize::new(0),
        };
        let projected =
            select_model_context(&conversation, selection, &resolver, 256, 2, 128 * 1024).await?;
        assert_eq!(projected.messages.len(), 1);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
