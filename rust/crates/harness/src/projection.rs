//! Explicit, provenance-preserving selection from canonical history to model context.

pub use crate::conversation::ModelContextSelection;
use crate::{
    Error, Result,
    conversation::{
        Attachment, ContentResidencyVerifier, ConversationMessage, ConversationState, FileRef,
        Limits, MessageKind, ReferencedAttachments,
    },
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole},
    tool::ToolInvocation,
};
use crate::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Default maximum bytes read when resolving a file for the provider-neutral
/// projection helper.  Adapters may narrow this bound for their provider.
pub const DEFAULT_PROJECTION_MAX_RESOLVED_BYTES: u64 = 1_024 * 1_024;
/// Default maximum bytes read for an attachment manifest in the projection
/// helper.  The owning provider may choose a lower bound.
pub const DEFAULT_PROJECTION_MAX_MANIFEST_BYTES: u64 = 1_024 * 1_024;
/// Default maximum number of attachments resolved from one selected message.
pub const DEFAULT_PROJECTION_MAX_ATTACHMENTS: usize = 256;
/// Default maximum number of selected messages in one provider request.
pub const DEFAULT_PROJECTION_MAX_MESSAGES: usize = 256;
/// Default maximum bytes rendered into one provider request.
pub const DEFAULT_PROJECTION_MAX_RENDER_BYTES: u64 = 128 * 1_024;
/// Protocol ceiling for attachments materialized into one model projection.
pub const MAX_PROJECTION_PROJECTED_ATTACHMENTS: usize = 1_022;
/// Maximum JSON artifact bytes accepted by the projection parser.
pub const MAX_PROJECTION_JSON_BYTES: u64 = 16 * 1_024 * 1_024;

#[derive(Debug, Deserialize)]
struct ProjectedToolInvocation {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
}

/// Provider-owned, grant-checked resolver for a complete attachment manifest.
pub trait AttachmentListResolver: crate::PlatformServiceBounds {
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

/// Derives the bounded, ordered message identity set for one admitted user
/// turn.  This is deliberately separate from byte projection: callers can
/// commit the selection before invoking an asynchronous model or content
/// provider, and retries can reuse the exact committed identities.
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
        .messages
        .iter()
        .position(|message| message.id == user_id)
        .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
    let prefix = conversation
        .messages
        .get(..=current)
        .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
    let mut ids = prefix
        .iter()
        .filter(|message| {
            matches!(
                message.kind,
                MessageKind::System
                    | MessageKind::User
                    | MessageKind::Assistant
                    | MessageKind::ToolCall
                    | MessageKind::ToolResult
            )
        })
        .map(|message| message.id)
        .collect::<Vec<_>>();
    if ids.len() > maximum_messages {
        ids.drain(..ids.len() - maximum_messages);
    }
    // A bounded suffix can start inside a tool exchange. Never hand a
    // provider a result whose precise call fell outside the window.
    let included = ids.iter().copied().collect::<HashSet<_>>();
    let by_id = prefix
        .iter()
        .map(|message| (message.id, message))
        .collect::<HashMap<_, _>>();
    ids.retain(|id| {
        by_id.get(id).is_some_and(|message| {
            message.kind != MessageKind::ToolResult
                || message
                    .reply_to
                    .is_some_and(|call| included.contains(&call))
        })
    });
    let selection = ModelContextSelection {
        conversation_revision: conversation.messages.len() as u64,
        message_ids: ids,
    };
    selection.validate(conversation)?;
    if selection.message_ids.last() != Some(&user_id) {
        return Err(Error::Conflict(
            "turn identity is bound to another context selection".into(),
        ));
    }
    Ok(selection)
}

impl SelectedModelContext {
    /// Admits a provider-proven context before dispatch without needing the
    /// owning conversation or reimplementing model bounds in a host adapter.
    pub fn validate_for_dispatch(&self, limits: Limits) -> Result<()> {
        limits.validate()?;
        let last = self.messages.last().ok_or_else(|| {
            Error::Invalid("selected context must end with the current user message".into())
        })?;
        if self.selection.conversation_revision == 0
            || self.messages.len() > limits.context_messages
            || self.messages.len() != self.selection.message_ids.len()
            || last.role != ModelRole::User
        {
            return Err(Error::Invalid(
                "selected context must end with the current user message".into(),
            ));
        }
        last.content.validate_user_input()?;
        for message in &self.messages {
            message.content.validate_limits(limits)?;
        }
        Ok(())
    }

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
#[allow(
    clippy::too_many_lines,
    reason = "projection enforces one bounded ordered selection"
)]
pub async fn select_model_context<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
) -> Result<SelectedModelContext> {
    select_model_context_with_projection_limit(
        conversation,
        selection,
        resolver,
        maximum_messages,
        maximum_attachments,
        maximum_render_bytes,
        MAX_PROJECTION_PROJECTED_ATTACHMENTS,
    )
    .await
}

/// Selects a bounded ordered subset with an explicit provider attachment
/// ceiling.  The public native convenience keeps the protocol ceiling of
/// 1,022; remote adapters may choose a lower bound without reimplementing the
/// projection reducer.
#[allow(
    clippy::too_many_lines,
    reason = "projection enforces one bounded ordered selection"
)]
pub async fn select_model_context_with_projection_limit<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
    maximum_projected_attachments: usize,
) -> Result<SelectedModelContext> {
    selection.validate(conversation)?;
    if maximum_messages == 0
        || maximum_render_bytes == 0
        || maximum_projected_attachments > MAX_PROJECTION_PROJECTED_ATTACHMENTS
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
            let invocation: ProjectedToolInvocation =
                read_json_artifact(resolver, &message.content, maximum_render_bytes).await?;
            ToolInvocation::validate_identity(&invocation.call_id, &invocation.name)?;
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
                return Err(Error::Invalid("selected tool result lacks its call".into()));
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
        let projected_count = attachments.len().min(maximum_projected_attachments);
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

/// Selects from a loaded, possibly compacted conversation view whose
/// authoritative revision is retained separately from the number of loaded
/// messages.  The full-history variant above remains strict for native
/// callers; this entry point is the bridge used by remote/WASM views.
pub async fn select_model_context_at_revision<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
    maximum_projected_attachments: usize,
) -> Result<SelectedModelContext> {
    validate_model_context_selection_at_revision(
        conversation,
        &selection,
        selection.conversation_revision,
    )?;
    let loaded_revision = u64::try_from(conversation.messages.len())
        .map_err(|_| Error::Invalid("conversation message count exceeds u64".into()))?;
    let loaded_selection = ModelContextSelection {
        conversation_revision: loaded_revision,
        message_ids: selection.message_ids.clone(),
    };
    let mut selected = select_model_context_with_projection_limit(
        conversation,
        loaded_selection,
        resolver,
        maximum_messages,
        maximum_attachments,
        maximum_render_bytes,
        maximum_projected_attachments,
    )
    .await?;
    selected.selection.conversation_revision = selection.conversation_revision;
    Ok(selected)
}

/// Validates selection order and role/linkage semantics without resolving any
/// owner bytes.  Remote adapters use this before collecting authorized reads.
pub fn validate_model_context_selection_at_revision(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
) -> Result<()> {
    if selection.conversation_revision != conversation_revision {
        return Err(Error::Conflict(
            "model context selection has a stale conversation revision".into(),
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
    let mut previous_sequence = 0;
    let mut tool_calls: HashMap<Uuid, &ConversationMessage> = HashMap::new();
    for id in &selection.message_ids {
        let message = by_id
            .get(id)
            .copied()
            .ok_or_else(|| Error::Invalid("selected conversation message is missing".into()))?;
        if message.sequence <= previous_sequence || message.sequence > conversation_revision {
            return Err(Error::Invalid(
                "model context selection is not ordered and unique".into(),
            ));
        }
        previous_sequence = message.sequence;
        message.validate()?;
        match &message.kind {
            MessageKind::ToolCall => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid(
                        "tool artifact type or rendering limit is invalid".into(),
                    ));
                }
                tool_calls.insert(message.id, message);
            }
            MessageKind::ToolResult => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid(
                        "tool result artifact type is invalid".into(),
                    ));
                }
                let call_id = message
                    .tool_call_id
                    .as_deref()
                    .ok_or_else(|| Error::Invalid("tool result lacks call identity".into()))?;
                let linked_call_id = message
                    .reply_to
                    .and_then(|id| {
                        tool_calls
                            .get(&id)
                            .and_then(|call| call.tool_call_id.as_deref())
                    })
                    .ok_or_else(|| Error::Invalid("selected tool result lacks its call".into()))?;
                if linked_call_id != call_id {
                    return Err(Error::Invalid("selected tool result lacks its call".into()));
                }
            }
            MessageKind::System | MessageKind::User | MessageKind::Assistant => {}
            kind => {
                return Err(Error::Unsupported(format!(
                    "message kind {kind:?} requires a specialized model projection"
                )));
            }
        }
    }
    Ok(())
}

async fn read_json_artifact<T: DeserializeOwned, R: AttachmentListResolver + ?Sized>(
    resolver: &R,
    file: &FileRef,
    maximum_bytes: u64,
) -> Result<T> {
    if file.descriptor().byte_length() > MAX_PROJECTION_JSON_BYTES {
        return Err(Error::Invalid(
            "tool artifact exceeds JSON byte limit".into(),
        ));
    }
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(Error::Invalid(
            "tool artifact type or rendering limit is invalid".into(),
        ));
    }
    let bytes = resolver.read(file).await?;
    if bytes.len() as u64 > MAX_PROJECTION_JSON_BYTES {
        return Err(Error::Invalid(
            "resolved tool artifact exceeds JSON byte limit".into(),
        ));
    }
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "resolved tool artifact exceeds rendering limit".into(),
        ));
    }
    file.descriptor().verify(&bytes)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    validate_model_json_numbers(&value)?;
    serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))
}

fn validate_model_json_numbers(value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Array(values) => values.iter().try_for_each(validate_model_json_numbers),
        serde_json::Value::Object(values) => {
            values.values().try_for_each(validate_model_json_numbers)
        }
        serde_json::Value::Number(number) => {
            const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
            if let Some(value) = number.as_i64() {
                if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
                    return Err(Error::Invalid(
                        "tool artifact contains an inexact number".into(),
                    ));
                }
            } else if let Some(value) = number.as_u64() {
                if value > MAX_SAFE_INTEGER as u64 {
                    return Err(Error::Invalid(
                        "tool artifact contains an inexact number".into(),
                    ));
                }
            } else if let Some(value) = number.as_f64()
                && (!value.is_finite()
                    || (value == 0.0 && value.is_sign_negative())
                    || (value.fract() == 0.0 && value.abs() > 9_007_199_254_740_991.0))
            {
                return Err(Error::Invalid(
                    "tool artifact contains an inexact number".into(),
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
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

    struct ArtifactResolver {
        bytes: Vec<u8>,
    }

    impl AttachmentListResolver for ArtifactResolver {
        fn resolve<'a>(
            &'a self,
            _manifest: &'a FileRef,
            _item_count: u32,
        ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
            Box::pin(async {
                Err(Error::Unsupported(
                    "manifest resolution is unavailable".into(),
                ))
            })
        }

        fn read<'a>(&'a self, _file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            Box::pin(async { Ok(self.bytes.clone()) })
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

    #[test]
    fn projected_context_dispatch_admission_uses_native_model_bounds() {
        let id = Uuid::new_v4();
        let mut selected = SelectedModelContext {
            selection: ModelContextSelection {
                conversation_revision: 1,
                message_ids: vec![id],
            },
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
        };
        assert!(selected.validate_for_dispatch(Limits::default()).is_ok());

        let limits = Limits {
            render_bytes: 4,
            ..Limits::default()
        };
        assert!(selected.validate_for_dispatch(limits).is_err());

        selected.messages[0].role = ModelRole::Assistant;
        assert!(selected.validate_for_dispatch(Limits::default()).is_err());
        selected.messages[0].role = ModelRole::User;
        selected.selection.message_ids.clear();
        assert!(selected.validate_for_dispatch(Limits::default()).is_err());
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

    #[tokio::test]
    async fn json_projection_checks_resolved_bytes_before_parsing() -> Result<()> {
        let agent = AgentId::new();
        let reference = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            "tool.json",
            "v1",
            FileDescriptor::from_bytes(b"{}", "application/json")?,
            "tool.json",
        )?;
        let oversized_len = usize::try_from(MAX_PROJECTION_JSON_BYTES + 1)
            .map_err(|_| Error::Invalid("test artifact size exceeds platform capacity".into()))?;
        let resolver = ArtifactResolver {
            bytes: vec![0; oversized_len],
        };
        let result = read_json_artifact::<serde_json::Value, _>(
            &resolver,
            &reference,
            MAX_PROJECTION_JSON_BYTES,
        )
        .await;
        let Err(error) = result else {
            return Err(Error::Invalid(
                "oversized resolved bytes were accepted".into(),
            ));
        };
        assert!(error.to_string().contains("JSON byte limit"));
        Ok(())
    }
}
