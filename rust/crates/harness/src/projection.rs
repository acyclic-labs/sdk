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
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

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
    fn diagnostic_label_bytes() -> u64 {
        const PRIVATE_LABEL_BYTES: u64 =
            concat!("ProjectedToolInvocation", "call_id", "name", "arguments").len() as u64 + 4 * 4;
        crate::contract::preparation_serde_diagnostic_label_bytes() + PRIVATE_LABEL_BYTES
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ProjectedToolInvocation
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

/// Derives the bounded, ordered message identity set for one admitted user turn.
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
pub fn select_turn_delta(
    conversation: &ConversationState,
    current: &ConversationMessage,
    append: bool,
    checkpoint: Option<(&crate::context::CanonicalContextCheckpoint, FileRef)>,
    limits: crate::conversation::Limits,
) -> Result<ModelContextSelection> {
    limits.validate(&mut None)?;
    current.validate(&mut None)?;
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
        envelope.validate(limits, None)?;
        limits.validate_file(&reference, &mut None)?;
        reference
            .descriptor()
            .verify(&crate::contract::canonical_json_bytes(envelope)?, &mut None)?;
        if !conversation
            .checkpoint_covers_archived_prefix(&reference, envelope.selection.conversation_revision)
        {
            validate_model_context_selection_at_revision(conversation, &envelope.selection, envelope.selection.conversation_revision, None)?;
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
    /// Admits a provider-proven context before dispatch without needing the owning conversation or reimplementing model bounds in a host adapter.
    pub fn validate_for_dispatch(&self, limits: Limits) -> Result<()> {
        limits.validate(&mut None)?;
        if let Some(checkpoint) = &self.selection.checkpoint {
            limits.validate_file(checkpoint, &mut None)?;
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
        last.content.validate_user_input(&mut None)?;
        for message in &self.messages {
            message.content.validate_limits(limits, &mut None)?;
        }
        Ok(())
    }

    /// Checks selection provenance and its final current user input.
    pub(crate) fn validate_for_input(
        &self, input: &ModelContent, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(crate::executor::PreparationIo::original_native_work);
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


/// Projects a selection from the explicitly pinned conversation revision.
#[allow(clippy::too_many_arguments,
    reason = "the original revision selection ABI adds only its original optional preparation IO")]
pub async fn select_model_context<R: ContentResidencyVerifier + ?Sized>(
    conversation: &ConversationState,
    conversation_revision: u64,
    selection: ModelContextSelection,
    resolver: &R,
    maximum_messages: usize,
    maximum_attachments: usize,
    maximum_render_bytes: u64,
    maximum_projected_attachments: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<SelectedModelContext> {
    validate_model_context_selection_at_revision(conversation, &selection, conversation_revision, io)?;
    if maximum_messages == 0
        || maximum_render_bytes == 0
        || maximum_render_bytes > crate::conversation::MAX_EXACT_JS_INTEGER
        || maximum_projected_attachments as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64
        || selection.message_ids.len() > maximum_messages
    {
        return Err(projection_invalid(io, "selected message count exceeds projection limit"));
    }
    let mut messages = crate::model::vec_with_capacity::<ModelMessage>(selection.message_ids.len(), io)?;
    let mut tool_calls = ProjectionIndex::<usize>::new(io);
    for id in &selection.message_ids {
        projection_step(io, 1)?;
        let message = conversation
            .message(*id)
            .ok_or_else(|| projection_invalid(io, "selected conversation message is missing"))?;
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
            let call_id = crate::executor::stock_string_copy(call_id, io)?;
            let name = crate::executor::stock_string_copy(name, io)?;
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
        let primary_policy = match message.content.descriptor().media_type() {
            "image/png" | "image/jpeg" | "image/gif" | "image/webp" => projection_native_image(maximum_render_bytes, io)?,
            kind if kind.starts_with("text/")
                && message.content.descriptor().byte_length() <= maximum_render_bytes =>
            {
                FileProjectionPolicy::BoundedFull
            }
            _ => FileProjectionPolicy::Reference,
        };
        let attachments = resolve_projection_attachments(message, resolver, maximum_attachments, io).await?;
        let projected_count = attachments.as_slice().len().min(maximum_projected_attachments);
        let omitted_count = attachments.as_slice().len() - projected_count;
        let capacity = 1 + projected_count + usize::from(omitted_count != 0);
        let mut parts = crate::model::vec_with_capacity::<ModelContentPart>(capacity, io)?;
        parts.push(ModelContentPart::File {
            file: crate::model::clone_file(&message.content, io)?,
            policy: primary_policy,
        });
        attachments.append_projected(projected_count, &mut parts, maximum_render_bytes, io)?;
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

/// Reuses the canonical validation engine for an already admitted history observer; the native facade borrows its original ledger and creates no budget.
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
        return validate_model_context_selection_at_revision(
            conversation, selection, conversation_revision, Some(&io),
        );
    }
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    validate_model_context_selection_at_revision(
        conversation, selection, conversation_revision, None,
    )
}

/// Validates ordered source identities and tool linkage at the pinned revision.
pub fn validate_model_context_selection_at_revision(
    conversation: &ConversationState,
    selection: &ModelContextSelection,
    conversation_revision: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if selection.conversation_revision != conversation_revision {
        return Err(projection_conflict(io, "model context selection has a stale conversation revision"));
    }
    if let Some(checkpoint) = &selection.checkpoint {
        match io {
            #[cfg(feature = "filesystem")]
            Some(io) => checkpoint.validate(&mut Some(&mut io.history_io()))?,
            _ => checkpoint.validate(&mut None)?,
        }
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
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let _ = (bytes, io);
    Ok(())
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


fn validate_projection_message(
    message: &ConversationMessage, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return message.validate(&mut Some(&mut history));
    }
    let _ = io;
    message.validate(&mut None)
}

fn validate_projection_attachment(
    attachment: &Attachment, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return attachment.validate(&mut Some(&mut history));
    }
    let _ = io;
    attachment.validate(&mut None)
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

struct ProjectionIndex<'a, T> {
    values: HashMap<Uuid, T>,
    io: Option<&'a crate::executor::PreparationIo>,
}

impl<T> Drop for ProjectionIndex<'_, T> {
    fn drop(&mut self) {
        #[cfg(feature = "filesystem")]
        let bytes = self.values.len() * std::mem::size_of::<(Uuid, T)>();
        drop(std::mem::take(&mut self.values));
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io {
            io.release(bytes as u64).expect("projection index owns its live logical slots");
        }
    }
}

impl<'a, T> ProjectionIndex<'a, T> {
    fn new(io: Option<&'a crate::executor::PreparationIo>) -> Self {
        Self { values: HashMap::new(), io }
    }

    // Callers validate unique ordered message ids before insertion.
    fn insert(&mut self, id: &Uuid, value: T) -> Result<()> {
        projection_step(self.io, 1)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io {
            let bytes = std::mem::size_of::<(Uuid, T)>() as u64;
            self.values.len().checked_add(1)
                .and_then(|count| count.checked_mul(std::mem::size_of::<(Uuid, T)>()))
                .ok_or_else(|| projection_invalid(Some(io), "projection index ownership overflows"))?;
            let slot = io.reserve_decode(bytes)?;
            let copied = acyclic_fs::WorkCounters {
                bytes_copied: 16, ..Default::default()
            };
            io.preflight(copied)?;
            io.charge(copied)?;
            slot.into_retained();
        }
        self.values.insert(*id, value);
        Ok(())
    }

    fn get(&self, id: &Uuid) -> Result<Option<&T>> {
        projection_step(self.io, 1)?;
        Ok(self.values.get(id))
    }
}

enum ProjectionAttachments<'a> {
    Borrowed(&'a [Attachment]),
    Owned(Vec<Attachment>, Option<crate::conversation::ContentHold<'a>>),
}

impl ProjectionAttachments<'_> {
    fn as_slice(&self) -> &[Attachment] {
        match self {
            Self::Borrowed(items) => items,
            Self::Owned(items, _) => items,
        }
    }
}

impl ProjectionAttachments<'_> {
    fn append_projected(
        self, count: usize, parts: &mut Vec<ModelContentPart>, maximum_render_bytes: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let mut append = |file: FileRef| -> Result<()> {
            let policy = match file.descriptor().media_type() {
                "image/png" | "image/jpeg" | "image/gif" | "image/webp" =>
                    projection_native_image(maximum_render_bytes, io)?,
                _ => FileProjectionPolicy::Reference,
            };
            parts.push(ModelContentPart::File { file, policy });
            Ok(())
        };
        // Both holds outlive the manifest data, including on early failure.
        let source_hold;
        let _hold;
        let items = match self {
            Self::Borrowed(items) => {
                for attachment in items.iter().take(count) {
                    projection_step(io, 1)?;
                    validate_projection_attachment(attachment, io)?;
                    append(crate::model::clone_file(&attachment.file, io)?)?;
                }
                return Ok(());
            }
            Self::Owned(items, hold) => { source_hold = hold; items },
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let move_files = if let Some(io) = io.filter(|io| io.uses_native_work()) {
            let hold = source_hold.as_ref().expect("native manifest retains ownership");
            let mut retained = 0_u64;
            for attachment in items.iter().take(count) {
                projection_step(Some(io), 1)?;
                retained = retained.checked_add(attachment.file.logical_owned_bytes()?)
                    .ok_or_else(|| projection_invalid(Some(io), "projected file ownership overflows"))?;
            }
            let discarded = hold.bytes().checked_sub(retained)
                .ok_or_else(|| projection_invalid(Some(io), "projected files exceed manifest ownership"))?;
            // Manifest and output use the same projection observer.
            _hold = Some(io.guard_owned(discarded)?);
            source_hold.expect("checked manifest ownership").into_retained();
            true
        } else { _hold = source_hold; io.is_none() };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let move_files = { _hold = source_hold; io.is_none() };
        for attachment in items.into_iter().take(count) {
            projection_step(io, 1)?;
            validate_projection_attachment(&attachment, io)?;
            let file = if move_files { attachment.file }
                else { crate::model::clone_file(&attachment.file, io)? };
            append(file)?;
        }
        Ok(())
    }
}

fn projection_native_image(
    maximum_work: u64, io: Option<&crate::executor::PreparationIo>,
) -> Result<FileProjectionPolicy> {
    projection_allocation(std::mem::size_of::<crate::model::NativeMediaPolicy>() as u64, io)?;
    Ok(FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
        intent: crate::model::NativeMediaIntent::Image { detail: crate::model::ImageDetail::Auto },
        maximum_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
        maximum_work,
        configuration: None,
    })))
}

async fn resolve_projection_attachments<'a, R: ContentResidencyVerifier + ?Sized>(
    message: &'a ConversationMessage, resolver: &'a R, maximum_attachments: usize,
    io: Option<&'a crate::executor::PreparationIo>,
) -> Result<ProjectionAttachments<'a>> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.check_cancellation()?; }
    let attachments = match &message.attachments {
        ReferencedAttachments::Inline { items } => ProjectionAttachments::Borrowed(items),
        ReferencedAttachments::Manifest { manifest, item_count } => {
            if *item_count as usize > maximum_attachments {
                return Err(projection_invalid(io, "selected message exceeds attachment projection limit"));
            }
            let (items, hold) = crate::conversation::load_manifest(resolver, manifest, *item_count, io).await?;
            let loaded = ProjectionAttachments::Owned(items, hold);
            if loaded.as_slice().len() != *item_count as usize {
                return Err(projection_invalid(io, "attachment manifest count does not match its declared count"));
            }
            loaded
        }
    };
    if attachments.as_slice().len() > maximum_attachments {
        return Err(projection_invalid(io, "selected message exceeds attachment projection limit"));
    }
    Ok(attachments)
}

async fn read_projected_invocation<R: ContentResidencyVerifier + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<ProjectedToolInvocation> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let parsed = read_projection_value(resolver, file, maximum_bytes, io).await?;
        let (value, custody) = convert_projection_value(parsed, maximum_bytes, io)?;
        custody.into_retained();
        return Ok(value);
    }
    let _ = io;
    read_json_artifact(resolver, file, maximum_bytes).await
}

async fn read_projected_tool_result<R: ContentResidencyVerifier + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let parsed = read_projection_value(resolver, file, maximum_bytes, io).await?;
        let intermediate = convert_projection_value::<serde_json::Value>(parsed, maximum_bytes, io)?;
        let (value, custody) = convert_projection_value(intermediate, maximum_bytes, io)?;
        custody.into_retained();
        return Ok(value);
    }
    let _ = io;
    let value = read_json_artifact(resolver, file, maximum_bytes).await?;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for ProjectedToolInvocation {
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ProjectedToolInvocation
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let arguments = crate::contract::json_value_logical_owned_bytes(&self.arguments)?;
        (self.call_id.capacity() as u64).checked_add(self.name.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(arguments))
            .ok_or_else(|| {
                #[cfg(not(target_arch = "wasm32"))]
                { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "projected tool invocation ownership overflows" } }
                #[cfg(target_arch = "wasm32")]
                { Error::Invalid("projected tool invocation ownership overflows".into()) }
            })
    }
}

#[cfg(feature = "filesystem")]
fn projection_conversion_allowance<T: crate::filesystem::UploadRecord>(
    shape: crate::contract::JsonValueShape,
    io: &crate::executor::PreparationIo,
) -> Result<u64> {
    use serde::__private229::de::Content;
    use std::mem::size_of;
    let overflow = || projection_invalid(Some(io), "tool projection conversion allowance overflows");
    let nodes = shape.lexical_items().checked_add(shape.virtual_items()).ok_or_else(overflow)?;
    let slots = nodes.checked_mul(10).ok_or_else(overflow)?;
    let typed_result = T::json_value_schema() == crate::contract::JsonValueSchema::ToolResultContent;
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

#[cfg(feature = "filesystem")]
fn convert_projection_value<'a, T: crate::filesystem::UploadRecord>(
    source: (serde_json::Value, crate::filesystem::DecodeReservation<'a>), maximum_bytes: u64,
    io: &'a crate::executor::PreparationIo,
) -> Result<(T, crate::filesystem::DecodeReservation<'a>)> {
    let mut reservation = Some(source.1);
    #[cfg(not(target_arch = "wasm32"))]
    let mut native_custody = None;
    let input = source.0;
    let shape = crate::contract::json_owned_value_shape_bounded(
        &input,
        crate::contract::JsonBounds { maximum_bytes, maximum_items: io.remaining()?.items_examined,
            maximum_depth: 128 },
        T::json_value_schema(),
        &mut |next| match next {
            crate::contract::JsonCodecCharge::Inspect { items } => projection_step(Some(io), items),
            _ => Err(projection_invalid(Some(io), "owned projection inspection reported nonresident work")),
        },
    )?;
    let mut additional = Some(io.reserve_decode(projection_conversion_allowance::<T>(shape, io)?)?);
    let total = reservation.as_ref().expect("source reservation").bytes()
        .checked_add(additional.as_ref().expect("conversion reservation").bytes())
        .ok_or_else(|| projection_invalid(Some(io), "owned tool conversion allowance overflows"))?;
    #[cfg(not(target_arch = "wasm32"))]
    if io.uses_native_work() {
        reservation.take().expect("source reservation").into_retained();
        additional.take().expect("conversion reservation").into_retained();
        native_custody = Some(io.take_native_custody(total)?);
    }
    let value: T = match serde_json::from_value(input) {
        Ok(value) => value,
        Err(error) => {
            #[cfg(not(target_arch = "wasm32"))]
            if native_custody.is_some() {
                return Err(Error::NativeDecode {
                    code: crate::wire::ErrorCode::Invalid, source: error, custody: native_custody,
                });
            }
            let message = error.to_string();
            additional.take().expect("ordinary conversion reservation").into_retained();
            return Err(Error::Invalid(message));
        }
    };
    let retained = value.logical_owned_bytes()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(mut custody) = native_custody.take() {
        if let Err(cause) = custody.substitute(retained) {
            drop(value);
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        io.absorb_native_custody(custody)?;
        return Ok((value, io.guard_owned(retained)?));
    }
    let released = total.checked_sub(retained)
        .ok_or_else(|| projection_invalid(Some(io), "owned tool conversion exceeds admission"))?;
    reservation.take().expect("ordinary source reservation").into_retained();
    additional.take().expect("ordinary conversion reservation").into_retained();
    io.release(released)?;
    Ok((value, io.guard_owned(retained)?))
}

#[cfg(feature = "filesystem")]
async fn read_projection_value<'io, R: ContentResidencyVerifier + ?Sized>(
    resolver: &R, file: &FileRef, maximum_bytes: u64,
    io: &'io crate::executor::PreparationIo,
) -> Result<(serde_json::Value, crate::filesystem::DecodeReservation<'io>)> {
    io.check_cancellation()?;
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum_bytes
    {
        return Err(projection_invalid(Some(io), "tool artifact type or rendering limit is invalid"));
    }
    let _source_hold;
    let bytes = resolver.read(file, maximum_bytes, Some(io))?.await?;
    _source_hold = if io.uses_native_work() { None } else {
        bytes.retained_capacity().map(|capacity| io.reserve_decode(capacity as u64)).transpose()?
    };
    if bytes.len() as u64 > maximum_bytes {
        return Err(projection_invalid(Some(io), "resolved tool artifact exceeds rendering limit"));
    }
    crate::executor::with_history_io(Some(io), |work| file.descriptor().verify(&bytes, work))?;
    let parsed = io.decode_preparation::<serde_json::Value>(
        &bytes, crate::wire::ErrorCode::Invalid,
    )?;
    validate_model_json_numbers(&parsed.0, Some(io))?;
    Ok(parsed)
}

async fn read_json_artifact<T: DeserializeOwned, R: ContentResidencyVerifier + ?Sized>(
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
    let bytes = resolver.read(file, maximum_bytes, None)?.await?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "resolved tool artifact exceeds rendering limit".into(),
        ));
    }
    file.descriptor().verify(&bytes, &mut None)?;
    let value: serde_json::Value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    validate_model_json_numbers(&value, None)?;
    serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))
}

fn validate_model_json_numbers(
    value: &serde_json::Value, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    projection_step(io, 1)?;
    match value {
        serde_json::Value::Array(values) => values.iter()
            .try_for_each(|value| validate_model_json_numbers(value, io)),
        serde_json::Value::Object(values) => {
            values.values().try_for_each(|value| validate_model_json_numbers(value, io))
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

    #[derive(Default)]
    struct Resolver {
        calls: AtomicUsize,
        bytes: Vec<u8>,
    }

    impl ContentResidencyVerifier for Resolver {
        fn read<'a>(&'a self, _file: &'a FileRef, _maximum_bytes: u64,
            io: Option<&'a crate::executor::PreparationIo>)
            -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.bytes.clone().into())
            })
        }
    }

    fn file(agent: AgentId, path: &str, media_type: &str, bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent), &mut None
            )?,
            (path).into(),
            "v1".into(),
            FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
            "data".into(), &mut None
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
        }, &mut None)?;
        Ok(id)
    }

    #[test]
    fn canonical_checkpoint_selects_exact_bounded_delta_from_ten_thousand_records() -> Result<()> {
        let agent = AgentId::new();
        let mut conversation = ConversationState::default();
        conversation.bind(agent)?;
        let content = file(agent, "message.txt", "text/plain", b"data")?;
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
        let payload = file(agent, "source.json", "application/json", b"data")?;
        let checkpoint = crate::context::CanonicalContextCheckpoint {
            operation_id: crate::OperationId::new(),
            selection: ModelContextSelection {
                conversation_revision: 9_998,
                message_ids: vec![
                    conversation.messages[9_996].id,
                    conversation.messages[9_997].id,
                ],
                checkpoint: Some(file(agent, "previous-checkpoint.json", "application/json", b"data")?),
            },
            source: payload.clone(),
            retained: payload.clone(),
            compaction: payload.clone(),
        };
        let bytes = crate::contract::canonical_json_bytes(&checkpoint)?;
        let pinned = FileRef::new(
            payload.volume().clone(),
            "checkpoint.json".into(),
            "v1".into(),
            FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?,
            "checkpoint.json".into(), &mut None
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
        conversation.append(current.clone(), &mut None)?;
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

    #[tokio::test]
    async fn manifest_projection_checks_limit_before_resolution_and_exact_count_after() -> Result<()>
    {
        let agent = AgentId::new();
        let id = Uuid::new_v4();
        let attachment = Attachment { file: file(agent, "image.png", "image/png", b"data")?, label: None };
        let bytes = crate::conversation::encode_attachment_manifest(&[attachment.clone()])?;
        let mut message = ConversationMessage {
            id, sequence: 1, kind: MessageKind::User,
            content: file(agent, "message.txt", "text/plain", b"data")?,
            attachments: ReferencedAttachments::Manifest {
                manifest: file(agent, "manifest.json", "application/vnd.acyclic.harness.attachments+json", &bytes)?,
                item_count: 2,
            },
            reply_to: None, tool_call_id: None, extensions: BTreeMap::new(),
        };
        let mut conversation = ConversationState::default();
        conversation.bind(agent)?;
        conversation.append(message.clone(), &mut None)?;
        let selection = ModelContextSelection {
            checkpoint: None, conversation_revision: 1, message_ids: vec![id],
        };
        let resolver = Resolver { bytes, ..Resolver::default() };
        assert!(
            select_model_context(
                &conversation,
                (&conversation).logical_revision(),
                selection.clone(),
                &resolver,
                256,
                1,
                128 * 1024,
                1,
                None,
            )
            .await
            .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            select_model_context(
                &conversation,
                (&conversation).logical_revision(),
                selection.clone(),
                &resolver,
                256,
                2,
                128 * 1024,
                2,
                None,
            )
            .await
            .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        let bytes = crate::conversation::encode_attachment_manifest(&[attachment.clone(), attachment])?;
        message.attachments = ReferencedAttachments::Manifest {
            manifest: file(agent, "manifest.json", "application/vnd.acyclic.harness.attachments+json", &bytes)?, item_count: 2,
        };
        let mut conversation = ConversationState::default();
        conversation.bind(agent)?;
        conversation.append(message, &mut None)?;
        let resolver = Resolver { bytes, ..Resolver::default() };
        let projected =
            select_model_context(&conversation, (&conversation).logical_revision(), selection, &resolver, 256, 2, 128 * 1024, 2, None).await?;
        assert_eq!(projected.messages.len(), 1);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn json_projection_checks_resolved_bytes_before_parsing() -> Result<()> {
        let agent = AgentId::new();
        let reference = file(agent, "tool.json", "application/json", b"{}")?;
        let oversized_len = 1_025;
        let resolver = Resolver {
            bytes: vec![0; oversized_len],
            ..Resolver::default()
        };
        assert!(matches!(
            read_json_artifact::<serde_json::Value, _>(&resolver, &reference, 1_024).await,
            Err(Error::Invalid(_))
        ));
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
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_manifest_resolution_never_calls_the_ordinary_resolver() -> Result<()> {
        struct NativeResolver {
            ordinary: AtomicUsize,
            native: AtomicUsize,
        }
        impl ContentResidencyVerifier for NativeResolver {
            fn supports_read(&self, _volume: &VolumeRef, io: Option<&crate::executor::PreparationIo>) -> Result<bool> {
                if let Some(io) = io { io.check_cancellation()?; }
                Ok(true)
            }
            fn read<'a>(&'a self, _: &'a FileRef, _: u64, io: Option<&'a crate::executor::PreparationIo>)
                -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
                crate::conversation::BoundedContentFuture::new(io, || async move {
                    let counter = if io.is_some_and(|io| io.uses_native_work()) { &self.native } else { &self.ordinary };
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(acyclic_stream::OwnedBytes::from_static(b"[]"))
                })
            }
        }
        let agent = AgentId::new();
        let message = ConversationMessage {
            id: Uuid::new_v4(), sequence: 1, kind: MessageKind::User,
            content: file(agent, "message.txt", "text/plain", b"data")?,
            attachments: ReferencedAttachments::Manifest {
                manifest: file(agent, "manifest.json", "application/vnd.acyclic.harness.attachments+json", b"[]")?,
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
            code: crate::wire::ErrorCode::Invalid, ..
        }));
        assert_eq!(resolver.ordinary.load(Ordering::SeqCst), 0);
        assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
        work.cancellation().cancel();
        assert!(resolve_projection_attachments(&message, &resolver, 1, Some(&io)).await.is_err());
        assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
        Ok(())
    }

}
