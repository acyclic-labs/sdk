//! Canonical, payload-free conversation and file references.

use crate::{
    AgentId, Error, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    resources::{GenerationRef, ProviderRef},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

/// Maximum normalized UTF-8 path length admitted by the Harness protocol.
pub const MAX_PATH_BYTES: usize = MAX_PORTABLE_COUNT;
/// Maximum UTF-8 bytes in a protocol label or display name.
pub const MAX_LABEL_BYTES: usize = crate::COMPONENT_LABEL_MAX_BYTES;
/// Largest integer that can be represented exactly by a JavaScript number.
pub const MAX_EXACT_JS_INTEGER: u64 = (1_u64 << 53) - 1;
#[allow(clippy::cast_precision_loss, reason = "2^53 - 1 is exact in f64")]
const MAX_EXACT_JS_FLOAT: f64 = MAX_EXACT_JS_INTEGER as f64;

/// Whether a JavaScript Number round-trips this value exactly: finite, not negative zero, and not an integral value beyond the safe-integer range.
pub(crate) fn is_exact_js_number(value: f64) -> bool {
    value.is_finite()
        && !(value == 0.0 && value.is_sign_negative())
        && (value.fract() != 0.0 || value.abs() <= MAX_EXACT_JS_FLOAT)
}

/// Whether a JSON integer, if `number` is one, is within the safe-integer range.
pub(crate) fn is_exact_js_integer(number: &serde_json::Number) -> bool {
    number.as_i64().map_or_else(
        || {
            number
                .as_u64()
                .is_none_or(|value| value <= MAX_EXACT_JS_INTEGER)
        },
        |value| value.unsigned_abs() <= MAX_EXACT_JS_INTEGER,
    )
}
mod json_value;
pub(crate) use json_value::validate_json_value;
pub use json_value::validate_portable_json;

/// Largest count representable by both this platform and the numeric wire format.
pub const MAX_PORTABLE_COUNT: usize =
    usize::MAX >> usize::BITS.saturating_sub(f64::MANTISSA_DIGITS);
/// Representable conversation page allowance; the caller chooses its budget.
pub const MAX_CONVERSATION_PAGE_MESSAGES: usize = u32::MAX as usize;
/// Default conversation read batch, replaceable by the caller.
pub const DEFAULT_CONVERSATION_PAGE_MESSAGES: u32 = 1_024;

/// Admission and rendering bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Maximum bytes in one referenced file admitted to a conversation.
    pub file_bytes: u64,
    /// Maximum normalized UTF-8 path bytes.
    pub path_bytes: usize,
    /// Maximum attachments in one message, inline or manifest-backed.
    pub attachments: usize,
    /// Maximum bytes rendered directly into one model context part.
    pub render_bytes: u64,
    /// Maximum model round trips in the stock agent loop.
    pub model_steps: usize,
    /// Maximum streamed observations admitted in one model round trip.
    pub model_events_per_step: usize,
    /// Maximum distinct tool calls admitted in one model round trip.
    pub tool_calls_per_step: usize,
    /// Maximum canonical messages selected into one model request.
    pub context_messages: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            file_bytes: MAX_EXACT_JS_INTEGER,
            path_bytes: u32::MAX as usize,
            attachments: u32::MAX as usize,
            render_bytes: MAX_EXACT_JS_INTEGER,
            model_steps: u32::MAX as usize,
            model_events_per_step: u32::MAX as usize,
            tool_calls_per_step: u32::MAX as usize,
            context_messages: u32::MAX as usize,
        }
    }
}

impl Limits {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if [self.file_bytes, self.render_bytes]
            .into_iter()
            .any(|limit| limit == 0 || limit > MAX_EXACT_JS_INTEGER)
            || [
                self.path_bytes,
                self.attachments,
                self.model_steps,
                self.model_events_per_step,
                self.tool_calls_per_step,
                self.context_messages,
            ]
            .into_iter()
            .any(|limit| limit == 0 || limit as u64 > MAX_PORTABLE_COUNT as u64)
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "harness limits are invalid",
            ));
        }
        Ok(())
    }

    /// Checks an immutable file against these limits.
    pub fn validate_file(
        &self,
        file: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        file.validate(work)?;
        if file.path().len() > self.path_bytes || file.descriptor().byte_length() > self.file_bytes
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "file exceeds harness limits",
            ));
        }
        Ok(())
    }

    /// Checks every directly named file and attachment count before admission.
    pub fn validate_message(
        &self,
        message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        message.validate(work)?;
        self.validate_file(&message.content, work)?;
        let (inline, manifest, count) = match &message.attachments {
            ReferencedAttachments::Inline { items } => (items.as_slice(), None, items.len()),
            ReferencedAttachments::Manifest {
                manifest,
                item_count,
            } => (&[][..], Some(manifest), *item_count as usize),
        };
        if count > self.attachments {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "attachments exceed harness limits",
            ));
        }
        for file in inline
            .iter()
            .map(|item| &item.file)
            .chain(manifest)
            .chain(message.extensions.values())
        {
            self.validate_file(file, work)?;
        }
        Ok(())
    }
}

mod append;
mod attachments;
mod buffers;
mod clone;
mod content;
mod directory;
mod files;
mod history;
mod message;
mod outcome;
mod selection;
mod state;
mod volumes;
pub use attachments::{
    Attachment, ReferencedAttachments, decode_attachment_manifest, encode_attachment_manifest,
    load_manifest, verified_attachment_manifest, verified_content_bytes,
};
use buffers::*;
pub(crate) use content::content_error;
pub use content::{
    BoundedContentFuture, CompositeContentVerifier, ContentHold, ContentIo, ContentMountResolver,
    ContentPublisher, ContentResidencyVerifier, LineWindow,
};
pub use directory::{
    MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
    PrivateDirectoryPage,
};
pub use files::{FileDescriptor, FileRef};
pub use message::{ConversationMessage, MessageKind};
pub use outcome::TaskOutcomeRecord;
pub use volumes::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
pub(crate) use volumes::{
    contains_content_label, content_label, is_inherited_context_path, is_internal_path,
};
#[cfg(test)]
mod tests;

/// Projection of one agent-owned conversation.
#[derive(Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ConversationStateWire")]
pub struct ConversationState {
    /// Set once at admission.
    pub agent: Option<AgentId>,
    /// Loaded canonical suffix; older records remain in the authoritative Stream.
    pub(crate) messages: Vec<ConversationMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_prefix: Option<ArchivedConversationPrefix>,
    #[serde(skip)]
    prefix_digests: Vec<[u8; 32]>,
    #[serde(skip)]
    by_id: BTreeMap<Uuid, (Option<usize>, Option<usize>)>,
    #[serde(skip)]
    pending_user: Option<Uuid>,
    #[serde(skip)]
    native_custody: NativeConversationCustody,
}

/// Only an issuer-authenticated reducer snapshot makes this a trusted prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchivedConversationPrefix {
    through_sequence: u64,
    history_digest: [u8; 32],
    checkpoint: FileRef,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConversationStateWire {
    pub(crate) agent: Option<AgentId>,
    pub(crate) messages: Vec<ConversationMessage>,
    #[serde(default)]
    pub(crate) archived_prefix: Option<ArchivedConversationPrefix>,
}

#[derive(Debug, Default)]
struct NativeConversationCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    messages: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    digests: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    archive: Option<acyclic_fs::NativeAllocation>,
    // The index drops before these tickets; their vector drops before its buffer owner.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    indexes: Vec<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    index_buffer: Option<acyclic_fs::NativeAllocation>,
}

const INDEX_ROW_BYTES: usize = std::mem::size_of::<(Uuid, (Option<usize>, Option<usize>))>();

impl PartialEq for NativeConversationCustody {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for NativeConversationCustody {}

fn conversation_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

impl TryFrom<ConversationStateWire> for ConversationState {
    type Error = Error;

    fn try_from(wire: ConversationStateWire) -> Result<Self> {
        Self::from_wire(wire, 1, &mut None).map(|(state, _)| state)
    }
}

/// Exact history revision and ordered subset selected for one model request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContextSelection {
    /// Number of canonical messages observed when selecting.
    pub conversation_revision: u64,
    /// Ordered, unique message identities; omitted history is deliberate.
    pub message_ids: Vec<Uuid>,
    /// Immutable canonical checkpoint covering history before this selection's delta.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<FileRef>,
}

impl ModelContextSelection {
    /// Rejects stale, missing, duplicated, or out-of-order source identities.
    pub fn validate(&self, conversation: &ConversationState) -> Result<()> {
        if self.conversation_revision != conversation.logical_revision() {
            return Err(Error::Conflict(
                "model context selection has a stale conversation revision".into(),
            ));
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate(&mut None)?;
        }
        let mut previous_sequence = 0;
        for id in &self.message_ids {
            let sequence = conversation
                .message(*id)
                .map(|message| message.sequence)
                .ok_or_else(|| Error::Invalid("selected conversation message is missing".into()))?;
            if sequence <= previous_sequence {
                return Err(Error::Invalid(
                    "model context selection is not ordered and unique".into(),
                ));
            }
            previous_sequence = sequence;
        }
        Ok(())
    }
}

fn settle_turn(
    pending_user: &mut Option<Uuid>,
    by_id: &mut BTreeMap<Uuid, (Option<usize>, Option<usize>)>,
    message: &ConversationMessage,
    position: usize,
) {
    if let Some(parent) = turn_outcome_parent(message) {
        by_id.entry(parent).or_default().1.get_or_insert(position);
    }
    if ((message.kind == MessageKind::Assistant
        && !message.extensions.contains_key("acyclic.model.reasoning"))
        || turn_outcome_parent(message).is_some())
        && *pending_user == message.reply_to
    {
        *pending_user = None;
    }
}

fn model_visible(kind: &MessageKind) -> bool {
    matches!(
        kind,
        MessageKind::System
            | MessageKind::User
            | MessageKind::Assistant
            | MessageKind::ToolCall
            | MessageKind::ToolResult
    )
}

fn turn_outcome_parent(message: &ConversationMessage) -> Option<Uuid> {
    (message.kind == MessageKind::System && message.extensions.contains_key("acyclic.turn.outcome"))
        .then_some(message.reply_to)
        .flatten()
}

fn advance_history_digest(
    previous: [u8; 32],
    message: &ConversationMessage,
    maximum_encoded_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let digest =
        crate::executor::json::canonical_json_digest(message, maximum_encoded_bytes, work)?;
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes(b"harness:conversation-prefix:v3\0".len() as u64 + 64)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness:conversation-prefix:v3\0");
    hash.update(&previous);
    hash.update(&digest);
    Ok(*hash.finalize().as_bytes())
}

fn deserialize_inline_attachments<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Vec<Attachment>, D::Error> {
    let items = Vec::<Attachment>::deserialize(decoder)?;
    for item in &items {
        item.validate(&mut None).map_err(serde::de::Error::custom)?;
    }
    Ok(items)
}

fn deserialize_manifest_file<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<FileRef, D::Error> {
    let file = FileRef::deserialize(decoder)?;
    if file.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(serde::de::Error::custom(
            "attachment manifest metadata is invalid",
        ));
    }
    Ok(file)
}

fn deserialize_label<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    validate_label(&value, &mut None).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_path<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    validate_content_path(&value, &mut None).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_media_type<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    if let Some(reason) = FileDescriptor::validation_fields(0, &value) {
        return Err(serde::de::Error::custom(reason));
    }
    Ok(value)
}

fn deserialize_file_length<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<u64, D::Error> {
    let value = u64::deserialize(decoder)?;
    if value > MAX_EXACT_JS_INTEGER {
        return Err(serde::de::Error::custom(
            "file byte length exceeds cross-language precision",
        ));
    }
    Ok(value)
}

fn validate_label(
    value: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    if value.is_empty()
        || value.len() as u64 > MAX_LABEL_BYTES as u64
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
    {
        return Err(conversation_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "file or volume label is invalid",
        ));
    }
    Ok(())
}

/// Checks a normalized volume-relative path.
pub fn validate_content_path(
    path: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    if path.is_empty()
        || path.len() as u64 > MAX_PATH_BYTES as u64
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(conversation_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "volume-relative path is invalid",
        ));
    }
    Ok(())
}

mod identity;
pub use identity::assistant_message_id;
#[cfg(feature = "filesystem")]
pub(crate) use identity::derived_operation_id;
