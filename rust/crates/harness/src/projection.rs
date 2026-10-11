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
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Default maximum bytes read when resolving a file for the provider-neutral
/// projection helper.  Adapters may narrow this bound for their provider.
pub const DEFAULT_PROJECTION_MAX_RESOLVED_BYTES: u64 = crate::conversation::MAX_LIMIT_FILE_BYTES;
/// Default maximum bytes read for an attachment manifest in the projection
/// helper.  The owning provider may choose a lower bound.
pub const DEFAULT_PROJECTION_MAX_MANIFEST_BYTES: u64 = crate::conversation::MAX_LIMIT_FILE_BYTES;
/// Default maximum number of attachments resolved from one selected message.
pub const DEFAULT_PROJECTION_MAX_ATTACHMENTS: usize = u32::MAX as usize;
/// Default maximum number of selected messages in one provider request.
pub const DEFAULT_PROJECTION_MAX_MESSAGES: usize = u32::MAX as usize;
/// Default maximum bytes rendered into one provider request.
pub const DEFAULT_PROJECTION_MAX_RENDER_BYTES: u64 = crate::conversation::MAX_LIMIT_RENDER_BYTES;
/// Protocol ceiling for attachments materialized into one model projection.
pub const MAX_PROJECTION_PROJECTED_ATTACHMENTS: usize = crate::conversation::MAX_PORTABLE_COUNT;
/// Largest JSON byte allowance representable by the JavaScript contract.
pub const MAX_PROJECTION_JSON_BYTES: u64 = crate::conversation::MAX_EXACT_JS_INTEGER;

#[derive(Debug, Deserialize, Serialize)]
struct ProjectedToolInvocation {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ProjectedToolInvocation {
    fn maximum_slot_bytes() -> u64 {
        <crate::context::Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(std::mem::size_of::<Self>() as u64)
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for crate::model::ToolResultContent {
    fn maximum_slot_bytes() -> u64 {
        <crate::context::Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(std::mem::size_of::<Self>() as u64)
    }
}

/// Provider-owned, grant-checked resolver for a complete attachment manifest.
pub trait AttachmentListResolver: acyclic_stream::ProviderPlatform {
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

    /// Resolves the original pinned manifest on the caller's native ledger.
    /// Missing custom capabilities reject before allocating a callback frame.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resolve_with_native_work<'a>(
        &'a self,
        _manifest: &'a FileRef,
        _item_count: u32,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        _work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native attachment resolver capability is unavailable",
        })
    }

    /// Reads an exact artifact using the provider's original grant checks.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_with_native_work<'a>(
        &'a self,
        _file: &'a FileRef,
        _maximum_bytes: u64,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
        _work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native tool artifact resolver capability is unavailable",
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resolve_with_native_work<'a>(
        &'a self,
        manifest: &'a FileRef,
        item_count: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        ContentResidencyVerifier::load_manifest_with_native_work(self, manifest, item_count, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_with_native_work<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
        ContentResidencyVerifier::read_with_native_work(self, file, maximum_bytes, work)
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
        .message(user_id)
        .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
    select_turn_suffix(conversation, current, false, maximum_messages)
}

/// Selects the exact canonical history delta after an owner-verified checkpoint.
/// The immutable envelope is cryptographically bound to its pinned `FileRef` here;
/// the owning journal remains responsible for its publication and summary admission.
/// With no checkpoint all model-visible history is selected or the explicit bound rejects.
pub fn select_turn_delta(
    conversation: &ConversationState,
    current: &ConversationMessage,
    append: bool,
    checkpoint: Option<(&crate::context::CanonicalContextCheckpoint, FileRef)>,
    limits: crate::conversation::Limits,
) -> Result<ModelContextSelection> {
    limits.validate()?;
    current.validate()?;
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
        envelope.validate(limits)?;
        limits.validate_file(&reference)?;
        reference
            .descriptor()
            .verify(&crate::contract::canonical_json_bytes(envelope)?)?;
        if !conversation
            .checkpoint_covers_archived_prefix(&reference, envelope.selection.conversation_revision)
        {
            validate_model_context_selection_at_revision(
                conversation,
                &envelope.selection,
                envelope.selection.conversation_revision,
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
    validate_selected_tool_pairs(conversation, current, &ids)?;
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

pub(crate) fn select_turn_suffix(
    conversation: &ConversationState,
    current: &ConversationMessage,
    append: bool,
    maximum_messages: usize,
) -> Result<ModelContextSelection> {
    if maximum_messages == 0 {
        return Err(Error::Invalid(
            "model context message limit must be positive".into(),
        ));
    }
    let end = if append {
        conversation.messages.len()
    } else {
        conversation
            .message_position(current.id)
            .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?
            + 1
    };
    let mut ids = conversation
        .recent_model_messages(end, maximum_messages - usize::from(append))
        .map(|message| message.id)
        .collect::<Vec<_>>();
    if append {
        ids.push(current.id);
    }
    validate_selected_tool_pairs(conversation, current, &ids)?;
    let selection = ModelContextSelection {
        checkpoint: None,
        conversation_revision: conversation.logical_revision() + u64::from(append),
        message_ids: ids,
    };
    if !append {
        selection.validate(conversation)?;
    }
    if selection.message_ids.last() != Some(&current.id) {
        return Err(Error::Conflict(
            "turn identity is bound to another context selection".into(),
        ));
    }
    Ok(selection)
}

fn validate_selected_tool_pairs(
    conversation: &ConversationState,
    current: &ConversationMessage,
    ids: &[Uuid],
) -> Result<()> {
    let included = ids.iter().copied().collect::<HashSet<_>>();
    for id in ids {
        let message = if *id == current.id {
            Some(current)
        } else {
            conversation.message(*id)
        };
        if message.is_some_and(|message| {
            message.kind == MessageKind::ToolResult
                && !message
                    .reply_to
                    .is_some_and(|call| included.contains(&call))
        }) {
            return Err(Error::Invalid(
                "context budget splits a tool exchange; compact or change selection".into(),
            ));
        }
    }
    Ok(())
}

impl SelectedModelContext {
    /// Admits a provider-proven context before dispatch without needing the
    /// owning conversation or reimplementing model bounds in a host adapter.
    pub fn validate_for_dispatch(&self, limits: Limits) -> Result<()> {
        limits.validate()?;
        if let Some(checkpoint) = &self.selection.checkpoint {
            limits.validate_file(checkpoint)?;
        }
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
        self.validate_for_input_with_io(input, None)
    }

    pub(crate) fn validate_for_input_with_io(
        &self, input: &ModelContent, io: Option<&crate::context::ContextPreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(crate::context::ContextPreparationIo::original_native_work);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native {
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let matches = if self.messages.len() == self.selection.message_ids.len() {
            match self.messages.last().filter(|last| last.role == ModelRole::User) {
                Some(last) => {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    if let Some(work) = native {
                        // One bounded logical DTO comparison, not invented byte reads or another Value walk.
                        work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                            ..acyclic_fs::WorkCounters::default() })
                            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        let matches = &last.content == input;
                        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        matches
                    } else {
                        &last.content == input
                    }
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    { &last.content == input }
                }
                None => false,
            }
        } else {
            false
        };
        if !matches {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "selected context must end with the current user input" });
            }
            return Err(Error::Invalid(
                "selected context must end with the current user input".into(),
            ));
        }
        let _ = io;
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
        maximum_attachments,
    )
    .await
}

/// Selects a bounded ordered subset with an explicit provider attachment
/// allowance. The native convenience uses the caller-selected attachment
/// allowance; adapters may explicitly select a narrower projection without
/// reimplementing the projection reducer.
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
    select_model_context_with_projection_limit_and_io(
        conversation, selection, resolver, maximum_messages, maximum_attachments,
        maximum_render_bytes, maximum_projected_attachments, None,
    ).await
}

#[allow(
    clippy::too_many_lines, clippy::too_many_arguments,
    reason = "one canonical ordered reducer shares the original optional preparation IO"
)]
async fn select_model_context_with_projection_limit_and_io<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
    maximum_projected_attachments: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<SelectedModelContext> {
    validate_loaded_selection(conversation, &selection, io)?;
    if maximum_messages == 0
        || maximum_render_bytes == 0
        || maximum_render_bytes > MAX_PROJECTION_JSON_BYTES
        || maximum_projected_attachments as u64 > MAX_PROJECTION_PROJECTED_ATTACHMENTS as u64
        || selection.message_ids.len() > maximum_messages
    {
        return Err(projection_invalid(io, "selected message count exceeds projection limit"));
    }
    let mut messages = projection_vec::<ModelMessage>(selection.message_ids.len(), io)?;
    let mut tool_calls = ProjectionIndex::<usize>::new(io);
    for id in &selection.message_ids {
        projection_step(io, 1)?;
        let message = conversation
            .message(*id)
            .ok_or_else(|| projection_invalid(io, "selected conversation message is missing"))?;
        validate_projection_message(message, io)?;
        if message.kind == MessageKind::ToolCall {
            let invocation: ProjectedToolInvocation =
                read_projected_invocation(resolver, &message.content, maximum_render_bytes, io).await?;
            validate_projection_tool_identity(&invocation.call_id, &invocation.name, io)?;
            if message.tool_call_id.as_deref() != Some(invocation.call_id.as_str()) {
                return Err(projection_invalid(io, "tool call artifact identity does not match its record"));
            }
            tool_calls.insert(&message.id, messages.len())?;
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
                return Err(projection_invalid(io, "tool result artifact type is invalid"));
            }
            let call_id = message
                .tool_call_id
                .as_deref()
                .ok_or_else(|| projection_invalid(io, "tool result lacks call identity"))?;
            let linked = message.reply_to.map(|id| tool_calls.get(&id)).transpose()?
                .flatten().and_then(|position| messages.get(*position))
                .ok_or_else(|| projection_invalid(io, "selected tool result lacks its call"))?;
            let ModelContent::Part(ModelContentPart::ToolCall {
                call_id: linked_call_id, name, ..
            }) = &linked.content else {
                return Err(projection_invalid(io, "selected tool result lacks its call"));
            };
            if linked_call_id != call_id {
                return Err(projection_invalid(io, "selected tool result lacks its call"));
            }
            let attachments = resolve_projection_attachments(message, resolver, maximum_attachments, io).await?;
            let projection = projection_result_attachment(attachments.as_slice(), io)?;
            let value =
                read_projected_tool_result(resolver, &projection.file, maximum_render_bytes, io).await?;
            let call_id = projection_string(call_id, io)?;
            let name = projection_string(name, io)?;
            messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content: value,
                }),
            });
            continue;
        }
        let role = match message.kind {
            MessageKind::System => ModelRole::System,
            MessageKind::User => ModelRole::User,
            MessageKind::Assistant => ModelRole::Assistant,
            _ => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "message kind requires a specialized model projection" });
                }
                return Err(Error::Unsupported(format!(
                    "message kind {:?} requires a specialized model projection",
                    message.kind
                )));
            }
        };
        // This projection explicitly selects native image intent without conversion.
        // The selected adapter must enforce its finite work budget.
        let native_image = || -> Result<FileProjectionPolicy> {
            projection_allocation(std::mem::size_of::<crate::model::NativeMediaPolicy>() as u64, io)?;
            Ok(FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
                intent: crate::model::NativeMediaIntent::Image {
                    detail: crate::model::ImageDetail::Auto,
                },
                maximum_bytes: DEFAULT_PROJECTION_MAX_RESOLVED_BYTES,
                maximum_work: maximum_render_bytes,
                configuration: None,
            })))
        };
        let primary_policy = match message.content.descriptor().media_type() {
            "image/png" | "image/jpeg" | "image/gif" | "image/webp" => native_image()?,
            kind if kind.starts_with("text/")
                && message.content.descriptor().byte_length() <= maximum_render_bytes =>
            {
                FileProjectionPolicy::BoundedFull
            }
            _ => FileProjectionPolicy::Reference,
        };
        if let ReferencedAttachments::Manifest { item_count, .. } = &message.attachments
            && (*item_count as usize) > maximum_attachments
        {
            return Err(projection_invalid(io, "selected message exceeds attachment projection limit"));
        }
        let attachments = resolve_projection_attachments(message, resolver, maximum_attachments, io).await?;
        let projected_count = attachments.as_slice().len().min(maximum_projected_attachments);
        let omitted_count = attachments.as_slice().len() - projected_count;
        let capacity = 1 + projected_count + usize::from(omitted_count != 0);
        let mut parts = projection_vec::<ModelContentPart>(capacity, io)?;
        parts.push(ModelContentPart::File {
            file: projection_file(&message.content, io)?,
            policy: primary_policy,
        });
        for file in attachments.into_projected(projected_count, io)? {
            let file = file?;
            let policy = match file.descriptor().media_type() {
                "image/png" | "image/jpeg" | "image/gif" | "image/webp" => native_image()?,
                _ => FileProjectionPolicy::Reference,
            };
            parts.push(ModelContentPart::File {
                file,
                policy,
            });
        }
        if omitted_count > 0 {
            let text = projection_omission_text(omitted_count, io)?;
            parts.push(ModelContentPart::Text { text });
        }
        messages.push(ModelMessage {
            role,
            content: ModelContent::Parts(parts),
        });
    }
    drop(tool_calls);

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
    select_model_context_at_revision_with_io(
        conversation, selection, resolver, maximum_messages, maximum_attachments,
        maximum_render_bytes, maximum_projected_attachments, None,
    ).await
}

/// Native copies and transferred same-ledger payloads remain owned by `io`.
/// The caller must drop the selected context before its original IO custody.
/// Source-reader guards remain separate until their physical data is retired;
/// a foreign source is copied, never rebound or rejected for ledger identity.
#[allow(clippy::too_many_arguments,
    reason = "the original revision selection ABI adds only its original optional preparation IO")]
pub(crate) async fn select_model_context_at_revision_with_io<R: AttachmentListResolver + ?Sized>(
    conversation: &ConversationState,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
    maximum_projected_attachments: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<SelectedModelContext> {
    validate_model_context_selection_at_revision_with_io(
        conversation,
        &selection,
        selection.conversation_revision,
        io,
    )?;
    let original_revision = selection.conversation_revision;
    let mut loaded_selection = selection;
    loaded_selection.conversation_revision = conversation.logical_revision();
    let mut selected = select_model_context_with_projection_limit_and_io(
        conversation,
        loaded_selection,
        resolver,
        maximum_messages,
        maximum_attachments,
        maximum_render_bytes,
        maximum_projected_attachments,
        io,
    )
    .await?;
    selected.selection.conversation_revision = original_revision;
    Ok(selected)
}

/// Validates selection order and role/linkage semantics without resolving any
/// owner bytes.  Remote adapters use this before collecting authorized reads.
pub fn validate_model_context_selection_at_revision(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
) -> Result<()> {
    validate_model_context_selection_at_revision_with_io(
        conversation, selection, conversation_revision, None,
    )
}

/// Reuses the canonical validation engine for an already admitted history
/// observer; the native facade borrows its original ledger and creates no budget.
pub(crate) fn validate_model_context_selection_at_revision_with_work(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref()
        .and_then(crate::store::history::HistoryIo::original_native_work)
    {
        let io = crate::executor::PreparationIo::from_native_work(native)?;
        return validate_model_context_selection_at_revision_with_io(
            conversation, selection, conversation_revision, Some(&io),
        );
    }
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    validate_model_context_selection_at_revision_with_io(
        conversation, selection, conversation_revision, None,
    )
}

fn validate_model_context_selection_at_revision_with_io(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if selection.conversation_revision != conversation_revision {
        return Err(projection_conflict(io, "model context selection has a stale conversation revision"));
    }
    let mut previous_sequence = 0;
    let mut tool_calls = ProjectionIndex::<&ConversationMessage>::new(io);
    for id in &selection.message_ids {
        projection_step(io, 1)?;
        let message = conversation
            .message(*id)
            .ok_or_else(|| projection_invalid(io, "selected conversation message is missing"))?;
        if message.sequence <= previous_sequence || message.sequence > conversation_revision {
            return Err(projection_invalid(io, "model context selection is not ordered and unique"));
        }
        previous_sequence = message.sequence;
        validate_projection_message(message, io)?;
        match &message.kind {
            MessageKind::ToolCall => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(projection_invalid(io, "tool artifact type or rendering limit is invalid"));
                }
                tool_calls.insert(&message.id, message)?;
            }
            MessageKind::ToolResult => {
                if message.content.descriptor().media_type() != "application/json" {
                    return Err(projection_invalid(io, "tool result artifact type is invalid"));
                }
                let call_id = message
                    .tool_call_id
                    .as_deref()
                    .ok_or_else(|| projection_invalid(io, "tool result lacks call identity"))?;
                let linked_call_id = message.reply_to.map(|id| tool_calls.get(&id)).transpose()?
                    .flatten().and_then(|call| call.tool_call_id.as_deref())
                    .ok_or_else(|| projection_invalid(io, "selected tool result lacks its call"))?;
                if linked_call_id != call_id {
                    return Err(projection_invalid(io, "selected tool result lacks its call"));
                }
            }
            MessageKind::System | MessageKind::User | MessageKind::Assistant => {}
            kind => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "message kind requires a specialized model projection" });
                }
                return Err(Error::Unsupported(format!(
                    "message kind {kind:?} requires a specialized model projection"
                )));
            }
        }
    }
    Ok(())
}

fn projection_invalid(io: Option<&crate::executor::PreparationIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = io;
    Error::Invalid(reason.into())
}

fn projection_conflict(io: Option<&crate::executor::PreparationIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason };
    }
    let _ = io;
    Error::Conflict(reason.into())
}

fn projection_step(io: Option<&crate::executor::PreparationIo>, items: u64) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let next = acyclic_fs::WorkCounters { items_examined: items, ..Default::default() };
        io.preflight(next)?;
        io.charge(next)?;
    }
    let _ = (io, items);
    Ok(())
}

fn projection_allocation(bytes: u64, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let _ = (bytes, io);
    Ok(())
}

fn projection_vec<T>(capacity: usize, io: Option<&crate::executor::PreparationIo>) -> Result<Vec<T>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| projection_invalid(Some(io), "projection vector capacity overflows"))? as u64;
        let work = io.original_native_work().expect("checked native work");
        let mut custody = if bytes == 0 { work.retain_owned(0) } else { work.claim(bytes) }
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity)
            .map_err(|_| projection_invalid(Some(io), "projection vector allocation failed"))?;
        let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| projection_invalid(Some(io), "projection vector capacity overflows"))? as u64;
        if let Err(cause) = custody.reconcile_capacity(actual) {
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        io.absorb_native_custody(custody)?;
        return Ok(values);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| projection_invalid(Some(io), "projection vector capacity overflows"))? as u64;
        projection_allocation(bytes, Some(io))?;
    }
    let _ = io;
    Ok(Vec::with_capacity(capacity))
}

fn projection_string(text: &str, io: Option<&crate::executor::PreparationIo>) -> Result<String> {
    crate::model::model_string_clone_with_io(text, io)
}

fn projection_file(file: &FileRef, io: Option<&crate::executor::PreparationIo>) -> Result<FileRef> {
    crate::model::model_file_clone_with_io(file, io)
}

fn projection_omission_text(count: usize, io: Option<&crate::executor::PreparationIo>) -> Result<String> {
    struct Omission(usize);
    impl std::fmt::Display for Omission {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "[{} additional attachments omitted from this bounded model context]", self.0)
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let (text, custody) = io.original_native_work().expect("checked native work")
            .display(&Omission(count)).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.absorb_native_custody(custody)?;
        return Ok(text);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        use std::fmt::Write as _;
        // This private decimal/literal recipe is exact, not a generic formatter
        // sizing pass or a second encoded buffer.
        let digits = if count == 0 { 1 } else { count.ilog10() as usize + 1 };
        let length = digits + "[".len()
            + " additional attachments omitted from this bounded model context]".len();
        projection_allocation(length as u64, Some(io))?;
        let next = acyclic_fs::WorkCounters { bytes_encoded: length as u64, ..Default::default() };
        io.preflight(next)?;
        let mut text = String::with_capacity(length);
        write!(&mut text, "{}", Omission(count)).expect("literal omission formatter writes into String");
        io.charge(next)?;
        return Ok(text);
    }
    let _ = io;
    Ok(format!("{}", Omission(count)))
}

fn validate_loaded_selection(
    conversation: &ConversationState, selection: &ModelContextSelection,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        io.check_cancellation()?;
        if selection.conversation_revision != conversation.logical_revision() {
            return Err(projection_conflict(Some(io), "model context selection has a stale conversation revision"));
        }
        if let Some(checkpoint) = &selection.checkpoint {
            let mut history = io.history_io();
            checkpoint.validate_with_work(&mut Some(&mut history))?;
        }
        let mut previous_sequence = 0;
        for id in &selection.message_ids {
            projection_step(Some(io), 1)?;
            let sequence = conversation.message(*id)
                .map(|message| message.sequence)
                .ok_or_else(|| projection_invalid(Some(io), "selected conversation message is missing"))?;
            if sequence <= previous_sequence {
                return Err(projection_invalid(Some(io), "model context selection is not ordered and unique"));
            }
            previous_sequence = sequence;
        }
        return Ok(());
    }
    let _ = io;
    selection.validate(conversation)
}

fn validate_projection_message(
    message: &ConversationMessage, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return message.validate_with_work(&mut Some(&mut history));
    }
    let _ = io;
    message.validate()
}

fn validate_projection_attachment(
    attachment: &Attachment, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return attachment.validate_with_work(&mut Some(&mut history));
    }
    let _ = io;
    attachment.validate()
}

fn projection_result_attachment<'a>(
    attachments: &'a [Attachment], io: Option<&crate::executor::PreparationIo>,
) -> Result<&'a Attachment> {
    for attachment in attachments {
        projection_step(io, 1)?;
        if attachment.label.as_deref() == Some("model_projection") {
            return Ok(attachment);
        }
    }
    Err(projection_invalid(io, "tool result lacks its model projection"))
}

fn validate_projection_tool_identity(
    call_id: &str, name: &str, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        projection_step(Some(io), 1)?;
        if !crate::contract::is_valid_component_label(name) {
            return Err(projection_invalid(Some(io), "tool name is invalid"));
        }
        if call_id.is_empty() || call_id.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
            || call_id.chars().any(char::is_control) || call_id.contains('/')
            || call_id.contains('\\') || call_id == "." || call_id == ".."
        {
            return Err(projection_invalid(Some(io), "tool call identity is invalid"));
        }
        io.check_cancellation()?;
        return Ok(());
    }
    let _ = io;
    ToolInvocation::validate_identity(call_id, name)
}

// Only live logical key/value slots are owned here. Std's private hash buckets
// and allocator operations are opaque, not fabricated native SDK observations.
struct ProjectionIndex<'a, T> {
    values: HashMap<Uuid, T>,
    held: ProjectionIndexHold<'a>,
}

struct ProjectionIndexHold<'a> {
    io: Option<&'a crate::executor::PreparationIo>,
    #[cfg(feature = "filesystem")]
    bytes: u64,
}

impl Drop for ProjectionIndexHold<'_> {
    fn drop(&mut self) {
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io {
            io.release(self.bytes).expect("projection index owns its live logical slots");
        }
    }
}

impl<'a, T> ProjectionIndex<'a, T> {
    fn new(io: Option<&'a crate::executor::PreparationIo>) -> Self {
        Self { values: HashMap::new(), held: ProjectionIndexHold {
            io, #[cfg(feature = "filesystem")] bytes: 0,
        } }
    }

    fn insert(&mut self, id: &Uuid, value: T) -> Result<()> {
        projection_step(self.held.io, 1)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.held.io {
            let bytes = std::mem::size_of::<(Uuid, T)>() as u64;
            let total = self.held.bytes.checked_add(bytes)
                .ok_or_else(|| projection_invalid(Some(io), "projection index ownership overflows"))?;
            io.claim(bytes)?;
            self.held.bytes = total;
            let copied = acyclic_fs::WorkCounters {
                bytes_copied: 16, ..Default::default()
            };
            io.preflight(copied)?;
            io.charge(copied)?;
        }
        self.values.insert(*id, value);
        Ok(())
    }

    fn get(&self, id: &Uuid) -> Result<Option<&T>> {
        projection_step(self.held.io, 1)?;
        Ok(self.values.get(id))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeProjectionSource<T> {
    value: T,
    custody: acyclic_fs::NativeAllocation,
}

enum ProjectionAttachments<'a> {
    #[cfg_attr(not(feature = "filesystem"), allow(dead_code,
        reason = "the original optional preparation IO borrows inline attachments only with filesystem"))]
    Borrowed(&'a [Attachment]),
    Owned(Vec<Attachment>),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(NativeProjectionSource<Vec<Attachment>>),
}

impl ProjectionAttachments<'_> {
    fn as_slice(&self) -> &[Attachment] {
        match self {
            Self::Borrowed(items) => items,
            Self::Owned(items) => items,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(source) => &source.value,
        }
    }
}

enum ProjectionFileSource<'a> {
    Borrowed(std::iter::Take<std::slice::Iter<'a, Attachment>>),
    Owned(std::iter::Take<std::vec::IntoIter<Attachment>>),
}

struct ProjectionFiles<'a> {
    source: ProjectionFileSource<'a>,
    io: Option<&'a crate::executor::PreparationIo>,
    move_files: bool,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _retired: Option<acyclic_fs::NativeAllocation>,
}

impl<'a> ProjectionAttachments<'a> {
    fn into_projected(
        self, count: usize, io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<ProjectionFiles<'a>> {
        match self {
            Self::Borrowed(items) => Ok(ProjectionFiles {
                source: ProjectionFileSource::Borrowed(items.iter().take(count)),
                io, move_files: false,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                _retired: None,
            }),
            Self::Owned(items) => Ok(ProjectionFiles {
                source: ProjectionFileSource::Owned(items.into_iter().take(count)),
                io, move_files: io.is_none(),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                _retired: None,
            }),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(source) => {
                // Bind custody first so an early return drops physical data first.
                let NativeProjectionSource { mut custody, value: items } = source;
                let same_ledger = io.and_then(crate::executor::PreparationIo::original_native_work)
                    .is_some_and(|work| work.shares_ledger(custody.work()));
                if same_ledger {
                    let io = io.expect("same-ledger output owner");
                    let mut retained = 0_u64;
                    for attachment in items.iter().take(count) {
                        projection_step(Some(io), 1)?;
                        retained = retained.checked_add(attachment.file.logical_owned_bytes()?)
                            .ok_or_else(|| projection_invalid(Some(io), "projected file ownership overflows"))?;
                    }
                    let output = custody.split_off(retained)
                        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    io.absorb_native_custody(output)?;
                }
                Ok(ProjectionFiles {
                    source: ProjectionFileSource::Owned(items.into_iter().take(count)),
                    io, move_files: same_ledger, _retired: Some(custody),
                })
            }
        }
    }
}

impl Iterator for ProjectionFiles<'_> {
    type Item = Result<FileRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            ProjectionFileSource::Borrowed(items) => {
                let attachment = items.next()?;
                Some((|| {
                    projection_step(self.io, 1)?;
                    validate_projection_attachment(attachment, self.io)?;
                    projection_file(&attachment.file, self.io)
                })())
            }
            ProjectionFileSource::Owned(items) => {
                let attachment = items.next()?;
                Some((|| {
                    projection_step(self.io, 1)?;
                    validate_projection_attachment(&attachment, self.io)?;
                    if self.move_files { Ok(attachment.file) }
                    else { projection_file(&attachment.file, self.io) }
                })())
            }
        }
    }
}

async fn resolve_projection_attachments<'a, R: AttachmentListResolver + ?Sized>(
    message: &'a ConversationMessage, resolver: &'a R, maximum_attachments: usize,
    io: Option<&'a crate::executor::PreparationIo>,
) -> Result<ProjectionAttachments<'a>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        io.check_cancellation()?;
        let attachments = match &message.attachments {
            ReferencedAttachments::Inline { items } => ProjectionAttachments::Borrowed(items),
            ReferencedAttachments::Manifest { manifest, item_count } => {
                if *item_count as usize > maximum_attachments {
                    return Err(projection_invalid(Some(io), "selected message exceeds attachment projection limit"));
                }
                let (items, custody) = resolver.resolve_with_native_work(
                    manifest, *item_count, io.original_native_work().expect("checked native work"),
                )?.await?;
                let source = NativeProjectionSource { value: items, custody };
                if source.value.len() != *item_count as usize {
                    return Err(projection_invalid(Some(io), "attachment manifest count does not match its declared count"));
                }
                ProjectionAttachments::Native(source)
            }
        };
        if attachments.as_slice().len() > maximum_attachments {
            return Err(projection_invalid(Some(io), "selected message exceeds attachment projection limit"));
        }
        return Ok(attachments);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.check_cancellation()?;
        if let ReferencedAttachments::Inline { items } = &message.attachments {
            if items.len() > maximum_attachments {
                return Err(projection_invalid(Some(io), "selected message exceeds attachment projection limit"));
            }
            return Ok(ProjectionAttachments::Borrowed(items));
        } else if let ReferencedAttachments::Manifest { item_count, .. } = &message.attachments {
            if *item_count as usize > maximum_attachments {
                return Err(projection_invalid(Some(io), "selected message exceeds attachment projection limit"));
            }
        }
    }
    let _ = io;
    Ok(ProjectionAttachments::Owned(resolve_attachments(message, resolver, maximum_attachments).await?))
}

async fn read_projected_invocation<R: AttachmentListResolver + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<ProjectedToolInvocation> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let parsed = read_native_projection_value(resolver, file, maximum_bytes, io).await?;
        return convert_native_projection_value(parsed, maximum_bytes, io);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let parsed = read_owned_projection_value(resolver, file, maximum_bytes, io).await?;
        let (value, custody) = parsed.into_parts();
        let output = convert_owned_projection_source::<ProjectedToolInvocation>(
            OwnedProjectionSource { value, custody }, maximum_bytes, io, true,
        )?;
        let OwnedProjectionSource { custody, value } = output;
        custody.into_retained();
        return Ok(value);
    }
    let _ = io;
    read_json_artifact(resolver, file, maximum_bytes).await
}

async fn read_projected_tool_result<R: AttachmentListResolver + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let parsed = read_native_projection_value(resolver, file, maximum_bytes, io).await?;
        let (value, custody) = parsed.into_native_parts()?;
        // Preserve BOTH original consuming conversions: read_json_artifact<Value>
        // first, then its caller's ToolResultContent conversion. Private Raw maps
        // make the intermediate Value conversion observably nonidentity.
        let intermediate = convert_native_projection_source::<serde_json::Value>(
            NativeProjectionSource { value, custody }, maximum_bytes, io,
        )?;
        let output = convert_native_projection_source::<crate::model::ToolResultContent>(
            intermediate, maximum_bytes, io,
        )?;
        let NativeProjectionSource { custody, value } = output;
        io.absorb_native_custody(custody)?;
        return Ok(value);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let parsed = read_owned_projection_value(resolver, file, maximum_bytes, io).await?;
        let (value, custody) = parsed.into_parts();
        let intermediate = convert_owned_projection_source::<serde_json::Value>(
            OwnedProjectionSource { value, custody }, maximum_bytes, io, true,
        )?;
        let output = convert_owned_projection_source::<crate::model::ToolResultContent>(
            intermediate, maximum_bytes, io, false,
        )?;
        let OwnedProjectionSource { custody, value } = output;
        custody.into_retained();
        return Ok(value);
    }
    let _ = io;
    let value = read_json_artifact(resolver, file, maximum_bytes).await?;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

// Private, closed source DTOs only. This is not a DeserializeOwned theorem.
#[cfg(feature = "filesystem")]
trait NativeProjectionArtifact: DeserializeOwned {
    fn value_schema() -> crate::contract::JsonValueSchema;
    fn heap_owned_bytes(&self, io: &crate::executor::PreparationIo) -> Result<u64>;
}

#[cfg(feature = "filesystem")]
impl NativeProjectionArtifact for serde_json::Value {
    fn value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::Root
    }
    fn heap_owned_bytes(&self, _io: &crate::executor::PreparationIo) -> Result<u64> {
        crate::model::model_value_owned_bytes(self)
    }
}

#[cfg(feature = "filesystem")]
impl NativeProjectionArtifact for ProjectedToolInvocation {
    fn value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ProjectedToolInvocation
    }
    fn heap_owned_bytes(&self, io: &crate::executor::PreparationIo) -> Result<u64> {
        let arguments = crate::model::model_value_owned_bytes(&self.arguments)?;
        (self.call_id.capacity() as u64).checked_add(self.name.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(arguments))
            .ok_or_else(|| projection_invalid(Some(io), "projected tool invocation ownership overflows"))
    }
}

#[cfg(feature = "filesystem")]
impl NativeProjectionArtifact for crate::model::ToolResultContent {
    fn value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ToolResultContent
    }
    fn heap_owned_bytes(&self, io: &crate::executor::PreparationIo) -> Result<u64> {
        self.logical_owned_bytes()?.checked_sub(std::mem::size_of::<Self>() as u64)
            .ok_or_else(|| projection_invalid(Some(io), "projected tool result ownership overflows"))
    }
}

#[cfg(feature = "filesystem")]
fn projection_conversion_allowance<T: NativeProjectionArtifact>(
    shape: crate::contract::JsonValueShape,
    io: &crate::executor::PreparationIo,
) -> Result<u64> {
    use serde::__private229::de::Content;
    use std::mem::size_of;
    // ToolResultContent -> ModelDataPart -> VolumeOwner/NativeMediaIntent is
    // the longest real tagged-buffer chain. Projection arguments have none.
    // Vec capacities and Content map PAIRS are quota, not allocator observations.
    let overflow = || projection_invalid(Some(io), "tool projection conversion allowance overflows");
    let nodes = shape.lexical_items().checked_add(shape.virtual_items()).ok_or_else(overflow)?;
    let slots = nodes.checked_mul(10).ok_or_else(overflow)?;
    let typed_result = T::value_schema() == crate::contract::JsonValueSchema::ToolResultContent;
    let content_slot = size_of::<Content<'static>>()
        .max(size_of::<(Content<'static>, Content<'static>)>()) as u64;
    let typed_slot = [
        size_of::<crate::model::ModelDataPart>(), size_of::<FileRef>(),
        size_of::<String>(), size_of::<u8>(), size_of::<(String, serde_json::Value)>(),
    ].into_iter().max().expect("closed projection slots") as u64;
    let content = if typed_result {
        slots.checked_mul(content_slot).and_then(|bytes| bytes.checked_mul(3)).ok_or_else(overflow)?
    } else { 0 };
    let typed = if typed_result { slots.checked_mul(typed_slot).ok_or_else(overflow)? } else { 0 };
    let boxes = if typed_result {
        nodes.checked_mul(size_of::<crate::model::NativeMediaPolicy>() as u64).ok_or_else(overflow)?
    } else { 0 };
    let strings = if typed_result { shape.input_bytes().checked_mul(3).ok_or_else(overflow)? } else { 0 };
    // Only NEW conversion containers/raw scratch are added. Resident strings
    // move under the original decoded Value's already-paid guard.
    let values = crate::contract::json_owned_value_conversion_additional_bytes(shape)?;
    let labels = [
        "serde_json::Value", "a valid JSON value", "a string containing valid JSON",
        "ProjectedToolInvocation", "call_id", "name", "arguments", "ToolResultContent", "json",
        "value", "parts", "ModelDataPart", "kind", "text", "file", "policy", "reference",
        "bounded_full", "native", "NativeMediaPolicy", "intent", "maximum_bytes", "maximum_work",
        "configuration", "NativeMediaIntent", "image", "detail", "auto", "low", "high", "audio",
        "maximum_duration_ms", "video", "maximum_frames", "document", "maximum_pages",
        "NativeConfigurationBinding", "source", "configuration", "implementation_digest",
        "EventReference", "stream", "sequence", "digest", "ExtensionConfiguration", "extension",
        "schema_digest", "content", "ExtensionIdentity", "name", "version", "FileRefWire", "volume",
        "path", "version", "descriptor", "display_name", "FileDescriptorWire", "sha256", "byte_length",
        "media_type", "VolumeRefWire", "provider", "id", "class", "owner", "project", "agent_private",
        "session_shared", "VolumeOwner", "project", "agent", "session", "ProviderRefWire", "namespace",
        "family", "version", "ResourceRefWire", "provider", "kind", "key", "version", "AgentId",
        "SHA-256 digest must contain exactly 32 bytes", "provider identity is invalid",
        "resource key is empty", "resource version is invalid", "typed resource kind mismatch",
        "file or volume label is invalid", "content path is invalid", "file media type is invalid",
    ].iter().map(|label| label.len() as u64 + 4).sum();
    let diagnostic = crate::contract::audited_serde_diagnostic_bytes(shape.input_bytes(), labels)?;
    [content, typed, boxes, strings, values, diagnostic, size_of::<T>() as u64]
        .into_iter().try_fold(0_u64, |bytes, next| bytes.checked_add(next).ok_or_else(overflow))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn convert_native_projection_value<T: NativeProjectionArtifact>(
    parsed: crate::filesystem::ReservedDecoded<'_, serde_json::Value>, maximum_bytes: u64,
    io: &crate::executor::PreparationIo,
) -> Result<T> {
    let (value, custody) = parsed.into_native_parts()?;
    let output = convert_native_projection_source::<T>(
        NativeProjectionSource { value, custody }, maximum_bytes, io,
    )?;
    let NativeProjectionSource { custody, value } = output;
    io.absorb_native_custody(custody)?;
    Ok(value)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn convert_native_projection_source<T: NativeProjectionArtifact>(
    mut source: NativeProjectionSource<serde_json::Value>, maximum_bytes: u64,
    io: &crate::executor::PreparationIo,
) -> Result<NativeProjectionSource<T>> {
    let shape = crate::contract::json_owned_value_shape_bounded(
        &source.value,
        crate::contract::JsonBounds { maximum_bytes, maximum_items: io.remaining()?.items_examined,
            maximum_depth: 128 },
        T::value_schema(),
        &mut |next| match next {
            crate::contract::JsonCodecCharge::Inspect { items } => projection_step(Some(io), items),
            _ => Err(projection_invalid(Some(io), "owned projection inspection reported nonresident work")),
        },
    )?;
    let work = io.original_native_work().expect("native projection converter");
    // Admit only new conversion payload/diagnostics before allocating. The
    // original decoded guard is transferred exactly, not inferred from shared
    // IO counters or claimed again for resident strings.
    let mut additional = work.retain_owned(projection_conversion_allowance::<T>(shape, io)?)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if let Err(cause) = source.custody.absorb(&mut additional) {
        let NativeProjectionSource { value, custody } = source;
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    // The consuming converter runs unchanged, with both old and new storage
    // paid. On error it has physically retired the complete original tree.
    let value: T = match serde_json::from_value(source.value) {
        Ok(value) => value,
        Err(error) => return Err(Error::NativeDecode {
            code: crate::wire::ErrorCode::Invalid, source: error, custody: Some(source.custody),
        }),
    };
    let mut output = NativeProjectionSource { value, custody: source.custody };
    let retained = output.value.heap_owned_bytes(io)?;
    if let Err(cause) = output.custody.substitute(retained) {
        let NativeProjectionSource { value, custody } = output;
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok(output)
}

#[cfg(feature = "filesystem")]
struct OwnedProjectionSource<'a, T> {
    value: T,
    custody: crate::filesystem::DecodeReservation<'a>,
}

#[cfg(feature = "filesystem")]
fn convert_owned_projection_source<'a, T: NativeProjectionArtifact>(
    source: OwnedProjectionSource<'a, serde_json::Value>, maximum_bytes: u64,
    io: &'a crate::executor::PreparationIo, artifact_error_prefix: bool,
) -> Result<OwnedProjectionSource<'a, T>> {
    let shape = crate::contract::json_owned_value_shape_bounded(
        &source.value,
        crate::contract::JsonBounds { maximum_bytes, maximum_items: io.remaining()?.items_examined,
            maximum_depth: 128 },
        T::value_schema(),
        &mut |next| match next {
            crate::contract::JsonCodecCharge::Inspect { items } => projection_step(Some(io), items),
            _ => Err(projection_invalid(Some(io), "owned projection inspection reported nonresident work")),
        },
    )?;
    let additional = io.reserve_decode(projection_conversion_allowance::<T>(shape, io)?)?;
    let total = source.custody.bytes().checked_add(additional.bytes())
        .ok_or_else(|| projection_invalid(Some(io), "owned tool conversion allowance overflows"))?;
    let value: T = match serde_json::from_value(source.value) {
        Ok(value) => value,
        Err(error) => {
            let message = if artifact_error_prefix { format!("tool artifact is invalid: {error}") }
                else { error.to_string() };
            additional.into_retained();
            return Err(Error::Invalid(message));
        }
    };
    let retained = value.heap_owned_bytes(io)?;
    let released = total.checked_sub(retained)
        .ok_or_else(|| projection_invalid(Some(io), "owned tool conversion exceeds admission"))?;
    // Transfer the exact two paid scopes, never snapshot shared IO ownership.
    source.custody.into_retained();
    additional.into_retained();
    io.release(released)?;
    let custody = io.guard_owned(retained)?;
    Ok(OwnedProjectionSource { value, custody })
}

#[cfg(feature = "filesystem")]
async fn read_owned_projection_value<'io, R: AttachmentListResolver + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: &'io crate::executor::PreparationIo,
) -> Result<crate::filesystem::ReservedDecoded<'io, serde_json::Value>> {
    io.check_cancellation()?;
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(projection_invalid(Some(io), "tool artifact type or rendering limit is invalid"));
    }
    // The ordinary provider's opaque frame/backend are outside the native
    // envelope. Its admitted raw payload and the SDK's actual bounded parser
    // still use this original owned IO, rather than a second allowance.
    let _source_hold = io.reserve_decode(file.descriptor().byte_length())?;
    let bytes = resolver.read(file).await?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(projection_invalid(Some(io), "resolved tool artifact exceeds rendering limit"));
    }
    let mut history = io.history_io();
    file.descriptor().verify_with_work(&bytes, &mut Some(&mut history))?;
    let parsed = io.decode_preparation_with_code::<serde_json::Value>(
        &bytes, crate::wire::ErrorCode::Invalid,
    )?;
    validate_model_json_numbers_with_io(&parsed.value, Some(io))?;
    Ok(parsed)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
async fn read_native_projection_value<'io, R: AttachmentListResolver + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: &'io crate::executor::PreparationIo,
) -> Result<crate::filesystem::ReservedDecoded<'io, serde_json::Value>> {
    io.check_cancellation()?;
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(projection_invalid(Some(io), "tool artifact type or rendering limit is invalid"));
    }
    let work = io.original_native_work().expect("native projection source");
    let (bytes, custody) = resolver.read_with_native_work(file, maximum_bytes, work)?.await?;
    let source = NativeProjectionSource { value: bytes, custody };
    if source.value.len() as u64 > maximum_bytes {
        return Err(projection_invalid(Some(io), "resolved tool artifact exceeds rendering limit"));
    }
    file.descriptor().verify_with_native_work(&source.value, work)?;
    let parsed = io.decode_preparation_with_code::<serde_json::Value>(
        &source.value, crate::wire::ErrorCode::Invalid,
    )?;
    validate_model_json_numbers_with_io(&parsed.value, Some(io))?;
    Ok(parsed)
}

async fn read_json_artifact<T: DeserializeOwned, R: AttachmentListResolver + ?Sized>(
    resolver: &R,
    file: &FileRef,
    maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<T> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(Error::Invalid(
            "tool artifact type or rendering limit is invalid".into(),
        ));
    }
    #[cfg(feature = "filesystem")]
    let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _bytes_hold;
    let bytes = resolver.read(file).await?;
    #[cfg(feature = "filesystem")]
    { _bytes_hold = io.map(|io| io.guard_owned(io.owned_bytes()? - before)).transpose()?; }
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "resolved tool artifact exceeds rendering limit".into(),
        ));
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
        io.preflight(hash)?;
        file.descriptor().verify(&bytes)?;
        io.charge(hash)?;
        let parsed = io.decode_preparation::<serde_json::Value>(&bytes)?;
        validate_model_json_numbers_with_io(&parsed, Some(io))?;
        let _encoded_hold;
        let encoded = io.encode_json(&parsed.value, io.remaining()?.bytes_encoded, false)?;
        _encoded_hold = io.guard_owned(encoded.len() as u64)?;
        return Ok(io.decode_preparation::<T>(&encoded)?.into_retained());
    }
    file.descriptor().verify(&bytes)?;
    let value: serde_json::Value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    validate_model_json_numbers(&value)?;
    serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))
}

fn validate_model_json_numbers(value: &serde_json::Value) -> Result<()> {
    validate_model_json_numbers_with_io(value, None)
}

fn validate_model_json_numbers_with_io(
    value: &serde_json::Value, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    projection_step(io, 1)?;
    match value {
        serde_json::Value::Array(values) => values.iter()
            .try_for_each(|value| validate_model_json_numbers_with_io(value, io)),
        serde_json::Value::Object(values) => {
            values.values().try_for_each(|value| validate_model_json_numbers_with_io(value, io))
        }
        serde_json::Value::Number(number) => {
            if !crate::conversation::is_exact_js_integer(number)
                || number
                    .as_f64()
                    .is_some_and(|value| !crate::conversation::is_exact_js_number(value))
            {
                return Err(projection_invalid(io, "tool artifact contains an inexact number"));
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
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Vec<Attachment>> {
    let attachments = match &message.attachments {
        ReferencedAttachments::Inline { items } => crate::executor::clone_preparation(items, io)?,
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

    fn append_history_message(
        conversation: &mut ConversationState,
        content: &FileRef,
        kind: MessageKind,
        reply_to: Option<Uuid>,
    ) -> Result<Uuid> {
        let id = Uuid::new_v4();
        conversation.append(ConversationMessage {
            id,
            sequence: conversation.logical_revision() + 1,
            kind,
            content: content.clone(),
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        })?;
        Ok(id)
    }

    #[test]
    fn canonical_checkpoint_selects_exact_bounded_delta_from_ten_thousand_records() -> Result<()> {
        let agent = AgentId::new();
        let mut conversation = ConversationState::default();
        conversation.bind(agent)?;
        let content = file(agent, "message.txt", "text/plain")?;
        for _ in 0..5_000 {
            let user =
                append_history_message(&mut conversation, &content, MessageKind::User, None)?;
            append_history_message(
                &mut conversation,
                &content,
                MessageKind::Assistant,
                Some(user),
            )?;
        }
        let current = ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 10_001,
            kind: MessageKind::User,
            content,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let limits = Limits {
            context_messages: 3,
            ..Limits::default()
        };
        assert!(select_turn_delta(&conversation, &current, true, None, limits).is_err());
        // This fixture checks the envelope/source-selection boundary and indexed delta,
        // not publication or summary proof. Those are explicitly owner-verified inputs.
        let payload = file(agent, "source.json", "application/json")?;
        let checkpoint = crate::context::CanonicalContextCheckpoint {
            operation_id: crate::OperationId::new(),
            selection: ModelContextSelection {
                conversation_revision: 9_998,
                message_ids: vec![
                    conversation.messages[9_996].id,
                    conversation.messages[9_997].id,
                ],
                checkpoint: Some(file(agent, "previous-checkpoint.json", "application/json")?),
            },
            source: payload.clone(),
            retained: payload.clone(),
            compaction: payload.clone(),
        };
        let bytes = crate::contract::canonical_json_bytes(&checkpoint)?;
        let pinned = FileRef::new(
            payload.volume().clone(),
            "checkpoint.json",
            "v1",
            FileDescriptor::from_bytes(&bytes, "application/json")?,
            "checkpoint.json",
        )?;
        let selected = select_turn_delta(
            &conversation,
            &current,
            true,
            Some((&checkpoint, pinned.clone())),
            limits,
        )?;
        assert_eq!(
            selected.message_ids,
            vec![
                conversation.messages[9_998].id,
                conversation.messages[9_999].id,
                current.id
            ]
        );
        assert_eq!(selected.checkpoint, Some(pinned.clone()));
        assert_eq!(selected.conversation_revision, 10_001);
        let smaller = Limits {
            context_messages: 2,
            ..limits
        };
        assert!(
            select_turn_delta(
                &conversation,
                &current,
                true,
                Some((&checkpoint, pinned.clone())),
                smaller
            )
            .is_err()
        );
        let mut changed = checkpoint.clone();
        changed.operation_id = crate::OperationId::new();
        assert!(
            select_turn_delta(
                &conversation,
                &current,
                true,
                Some((&changed, pinned)),
                limits
            )
            .is_err()
        );
        conversation.append(current.clone())?;
        assert_eq!(
            select_turn_delta(
                &conversation,
                &current,
                false,
                Some((&checkpoint, selected.checkpoint.clone().unwrap())),
                limits
            )?,
            selected
        );
        Ok(())
    }

    #[test]
    fn projected_context_dispatch_admission_uses_native_model_bounds() {
        let id = Uuid::new_v4();
        let mut selected = SelectedModelContext {
            selection: ModelContextSelection {
                checkpoint: None,
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
            checkpoint: None,
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
        let oversized_len = 1_025;
        let resolver = ArtifactResolver {
            bytes: vec![0; oversized_len],
        };
        let result = read_json_artifact::<serde_json::Value, _>(&resolver, &reference, 1_024, None).await;
        let Err(error) = result else {
            return Err(Error::Invalid(
                "oversized resolved bytes were accepted".into(),
            ));
        };
        assert!(error.to_string().contains("rendering limit"));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn projection_native_work() -> Result<acyclic_fs::NativeMaterializationWork> {
        acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: 16 * 1024 * 1024,
                allocation_operations: 1024,
                source_bytes_read: 1024 * 1024,
                bytes_copied: 1024 * 1024,
                bytes_hashed: 1024 * 1024,
                bytes_encoded: 1024 * 1024,
                items_examined: 1024,
                ..Default::default()
            },
            &acyclic_fs::CancellationToken::new(),
        ).map_err(|cause| Error::NativeWork { cause, custody: None })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_manifest_resolution_never_calls_the_ordinary_resolver() -> Result<()> {
        struct NativeResolver {
            ordinary: AtomicUsize,
            native: AtomicUsize,
        }
        impl AttachmentListResolver for NativeResolver {
            fn resolve<'a>(
                &'a self, _: &'a FileRef, _: u32,
            ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
                self.ordinary.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Vec::new()) })
            }
            fn resolve_with_native_work<'a>(
                &'a self, _: &'a FileRef, _: u32,
                work: &'a acyclic_fs::NativeMaterializationWork,
            ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
                crate::NativeFutureBundle::new(work, || async move {
                    self.native.fetch_add(1, Ordering::SeqCst);
                    let custody = work.retain_owned(0)
                        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    Ok((Vec::new(), custody))
                })
            }
        }
        let agent = AgentId::new();
        let message = ConversationMessage {
            id: Uuid::new_v4(), sequence: 1, kind: MessageKind::User,
            content: file(agent, "message.txt", "text/plain")?,
            attachments: ReferencedAttachments::Manifest {
                manifest: file(agent, "manifest.json", "application/vnd.acyclic.harness.attachments+json")?,
                item_count: 1,
            },
            reply_to: None, tool_call_id: None, extensions: BTreeMap::new(),
        };
        let resolver = NativeResolver { ordinary: AtomicUsize::new(0), native: AtomicUsize::new(0) };
        let work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let error = resolve_projection_attachments(&message, &resolver, 1, Some(&io))
            .await.err().expect("complete native manifest count mismatch");
        assert!(matches!(error, Error::NativeBoundary {
            reason: "attachment manifest count does not match its declared count", ..
        }));
        assert_eq!(resolver.ordinary.load(Ordering::SeqCst), 0);
        assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
        work.cancellation().cancel();
        assert!(resolve_projection_attachments(&message, &resolver, 1, Some(&io)).await.is_err());
        assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_foreign_manifest_payload_is_copied_without_rebinding_its_guard() -> Result<()> {
        let source_work = projection_native_work()?;
        let output_work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&output_work)?;
        let (reference, mut custody) = file(AgentId::new(), "attachment.txt", "text/plain")?
            .clone_with_native_work(&source_work)?;
        let mut vector_custody = source_work.claim(std::mem::size_of::<Attachment>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let items = vec![Attachment { file: reference, label: None }];
        custody.absorb(&mut vector_custody)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let expected = items[0].file.clone();
        let baseline = source_work.remaining()
            .map_err(|cause| Error::NativeWork { cause, custody: None })?.peak_allocation_bytes;
        let source = ProjectionAttachments::Native(NativeProjectionSource { value: items, custody });
        let mut files = source.into_projected(1, Some(&io))?;
        let projected = files.next().expect("selected file")?;
        assert_eq!(projected, expected);
        assert!(output_work.receipt().bytes_copied > 0);
        assert_eq!(source_work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?
            .peak_allocation_bytes, baseline);
        drop(files);
        assert!(source_work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?
            .peak_allocation_bytes > baseline);
        drop(projected);
        drop(io);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_same_ledger_manifest_moves_the_original_paid_file_payload() -> Result<()> {
        let work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let (reference, mut custody) = file(AgentId::new(), "attachment.txt", "text/plain")?
            .clone_with_native_work(&work)?;
        let retained = reference.logical_owned_bytes()?;
        let mut vector_custody = work.claim(std::mem::size_of::<Attachment>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let items = vec![Attachment { file: reference, label: None }];
        custody.absorb(&mut vector_custody)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = work.receipt();
        let source = ProjectionAttachments::Native(NativeProjectionSource { value: items, custody });
        let mut files = source.into_projected(1, Some(&io))?;
        let projected = files.next().expect("selected file")?;
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(io.owned_bytes()?, retained);
        drop(files);
        assert_eq!(io.owned_bytes()?, retained);
        drop(projected);
        drop(io);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_tool_conversion_preserves_original_owned_map_raw_semantics_without_reencoding() -> Result<()> {
        let work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        // Wire-first z is ordinary during the original slice decode. Actual
        // owned-map iteration makes the private key first in from_value.
        let input = br#"{"call_id":"ok","name":"x","arguments":{"z":true,"$serde_json::private::RawValue":"{\"captured\":7}"}}"#;
        let parsed = io.decode_preparation_with_code::<serde_json::Value>(
            input, crate::wire::ErrorCode::Invalid,
        )?;
        validate_model_json_numbers_with_io(&parsed.value, Some(&io))?;
        let source_reads = work.receipt().source_bytes_read;
        let original: serde_json::Value = crate::contract::json_from_slice(input)?;
        let expected = serde_json::from_value::<ProjectedToolInvocation>(original);
        let projected = convert_native_projection_value::<ProjectedToolInvocation>(
            parsed, input.len() as u64, &io,
        );
        match (projected, expected) {
            (Ok(projected), Ok(expected)) => {
                assert_eq!(projected.call_id, expected.call_id);
                assert_eq!(projected.name, expected.name);
                assert_eq!(projected.arguments, expected.arguments);
                assert_eq!(io.owned_bytes()?, projected.heap_owned_bytes(&io)?);
                drop(projected);
            }
            (Err(Error::NativeDecode { source, custody: Some(custody), .. }), Err(expected)) => {
                assert_eq!(source.classify(), expected.classify());
                assert_eq!(source.to_string(), expected.to_string());
                assert!(custody.work().shares_ledger(&work));
                drop(source);
                drop(custody);
            }
            _ => panic!("native converter must preserve the original owned-Value result"),
        }
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert_eq!(work.receipt().source_bytes_read, source_reads);
        drop(io);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_tool_conversion_preserves_original_decode_cause_and_paid_diagnostic_guard() -> Result<()> {
        let work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let input = br#"{"kind":"parts","parts":[{"kind":"file","file":123,"policy":"reference"}]}"#;
        let parsed = io.decode_preparation_with_code::<serde_json::Value>(
            input, crate::wire::ErrorCode::Invalid,
        )?;
        let error = convert_native_projection_value::<crate::model::ToolResultContent>(
            parsed, input.len() as u64, &io,
        ).err().expect("original typed converter rejects integer FileRef");
        let Error::NativeDecode { source, custody: Some(custody), .. } = error else {
            panic!("typed conversion must retain its original serde cause and admitted guard");
        };
        assert!(source.is_data());
        assert!(custody.bytes() > 0);
        assert!(custody.work().shares_ledger(&work));
        assert_eq!(io.owned_bytes()?, 0);
        assert_eq!(work.receipt().bytes_encoded, 0);
        drop(source);
        drop(custody);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_tool_result_runs_both_original_converters_after_one_funded_read() -> Result<()> {
        struct NativeArtifact {
            bytes: &'static [u8],
            ordinary: AtomicUsize,
            native: AtomicUsize,
        }
        impl AttachmentListResolver for NativeArtifact {
            fn resolve<'a>(
                &'a self, _: &'a FileRef, _: u32,
            ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
                Box::pin(async { Err(Error::Invalid("unused manifest resolver".into())) })
            }
            fn read<'a>(&'a self, _: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
                self.ordinary.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(Error::Invalid("ordinary read must not run".into())) })
            }
            fn read_with_native_work<'a>(
                &'a self, _: &'a FileRef, _: u64,
                work: &'a acyclic_fs::NativeMaterializationWork,
            ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
                crate::NativeFutureBundle::new(work, || async move {
                    self.native.fetch_add(1, Ordering::SeqCst);
                    let copied = acyclic_fs::WorkCounters {
                        bytes_copied: self.bytes.len() as u64, ..Default::default()
                    };
                    work.admit(copied).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    let mut custody = work.claim(self.bytes.len() as u64)
                        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    let bytes = self.bytes.to_vec();
                    if let Err(cause) = custody.reconcile_capacity(bytes.capacity() as u64) {
                        return Err(Error::NativeWork { cause, custody: Some(custody) });
                    }
                    work.charge(copied).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    Ok((bytes, custody))
                })
            }
        }
        let input = br#"{"kind":"json","value":{"z":true,"$serde_json::private::RawValue":"{\"z\":false,\"$serde_json::private::RawValue\":\"{\\\"captured\\\":7}\"}"}}"#;
        let ordinary: serde_json::Value = crate::contract::json_from_slice(input)?;
        let expected = serde_json::from_value::<serde_json::Value>(ordinary)
            .and_then(serde_json::from_value::<crate::model::ToolResultContent>);
        let agent = AgentId::new();
        let template = file(agent, "result.json", "application/json")?;
        let reference = FileRef::new(
            template.volume().clone(), "result.json", "v1",
            FileDescriptor::from_bytes(input, "application/json")?, "result.json",
        )?;
        let resolver = NativeArtifact {
            bytes: input, ordinary: AtomicUsize::new(0), native: AtomicUsize::new(0),
        };
        let work = projection_native_work()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let actual = read_projected_tool_result(&resolver, &reference, input.len() as u64, Some(&io)).await;
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(actual, expected);
                drop(actual);
            }
            (Err(Error::NativeDecode { source, custody: Some(custody), .. }), Err(expected)) => {
                assert_eq!(source.to_string(), expected.to_string());
                drop(source);
                drop(custody);
            }
            _ => panic!("native tool result must preserve both original consuming converters"),
        }
        assert_eq!(resolver.ordinary.load(Ordering::SeqCst), 0);
        assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
        assert_eq!(work.receipt().bytes_encoded, 0);
        Ok(())
    }
}
