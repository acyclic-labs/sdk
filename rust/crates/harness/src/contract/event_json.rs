//! Admission for the concrete history EventPayload schema, not arbitrary serde.
//!
//! This audit is intentionally coupled to serde 1.0.229's Content carrier. A
//! serde upgrade must re-audit tagged buffering rather than silently changing
//! the logical reservation. The original EventPayload Deserialize implementation
//! remains the only decoder; this module does not interpret event variants.

use super::{Error, JsonBounds, JsonCodecCharge, Result};
use crate::{
    AgentId,
    conversation::{Attachment, ConversationMessage, FileRef, VolumeOperation},
    core::{ExtensionConfiguration, ExtensionDependency},
    fork::{CapturedResource, ForkOmission, ForkSeed, ReferenceGrant, SharedGrant},
    merge::ProjectMergeReceipt,
};
use serde::__private229::de::Content;
use serde_json::Value;
use std::mem::size_of;
use uuid::Uuid;

/// A real logical-quota reservation basis. These are upper bounds, not claimed
/// allocator observations or allocation attempts. The caller holds its quota
/// guard across serde decoding and substitutes observed retained payload bytes
/// without double counting. Rejection/cancellation drops that guard.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EventPayloadJsonAdmission {
    pub input_bytes: u64,
    pub lexical_items: u64,
    pub maximum_depth: u32,
    pub maximum_owned_bytes: u64,
    pub serde_content_bytes: u64,
    pub typed_collection_bytes: u64,
    pub value_overlap_bytes: u64,
    pub boxed_payload_bytes: u64,
    pub string_payload_bytes: u64,
    pub scratch_bytes: u64,
    pub diagnostic_bytes: u64,
}

// Largest concrete chain which can buffer tagged enum contents, even when an
// untrusted document puts the adjacent content before its discriminator:
// EventPayload -> Capture -> ResourceRevision -> VolumeOwner. Plain structs do
// not create another Content pass. ReferencedAttachmentsWire and EffectStatus
// are shorter chains. JSON Content has no Some/Newtype boxes: those variants
// require non-JSON visitor methods. Duplicate keys remain in Content vectors.
const CONTENT_LAYERS: u64 = 4;

// All variable-width typed collection elements reachable from EventPayload.
// Fixed [u8;32] digests themselves are inline, but FileDescriptorWire first
// deserializes Vec<u8>, which is covered by u8. BTree entries' key/value payload
// is included; opaque BTree nodes/allocator headers are not. Fork paths are
// String, never PathBuf/native paths. ResourceRef bodies remain opaque Vec<u8>.
const fn maximum(values: &[usize]) -> usize {
    let mut result = 0;
    let mut index = 0;
    while index < values.len() {
        if values[index] > result { result = values[index]; }
        index += 1;
    }
    result
}

const TYPED_COLLECTION_SLOT_BYTES: usize = maximum(&[
        size_of::<ExtensionDependency>(),
        size_of::<ExtensionConfiguration>(),
        size_of::<AgentId>(),
        size_of::<CapturedResource>(),
        size_of::<ForkOmission>(),
        size_of::<FileRef>(),
        size_of::<SharedGrant>(),
        size_of::<ReferenceGrant>(),
        size_of::<Attachment>(),
        size_of::<String>(),
        size_of::<Uuid>(),
        size_of::<u8>(),
        size_of::<VolumeOperation>(),
        size_of::<(String, FileRef)>(),
]);

// Exactly one root payload box exists in ForkPublished, ProjectMergePublished,
// ConversationMessageAppended or InteractionOutcome::Answered. Their pointee
// carriers are heap-owned and must not be dismissed as fixed stack ABI data.
const BOXED_PAYLOAD_BYTES: usize = maximum(&[
    size_of::<ForkSeed>(), size_of::<ProjectMergeReceipt>(),
    size_of::<ConversationMessage>(), size_of::<FileRef>(),
]);

// Closed-schema expected-field/discriminator labels and fixed diagnostic
// templates. Their aggregate length bounds any one expected-field list, rather
// than assuming malformed-input error strings have fixed-size payloads. The
// dynamic interpolated input is accounted separately below. Upgrades/additions
// to reachable custom Deserialize implementations require updating this audit.
const DIAGNOSTIC_LABELS: &[&str] = &[
    "lifecycle_transitioned", "custom", "extension_state_migrated", "extensions_selected",
    "extension_configured", "effect_planned", "effect_dispatched", "effect_resolved",
    "fork_published", "project_merge_published", "conversation_bound",
    "conversation_message_appended", "model_context_selected", "interaction_opened", "interaction_resolved",
    "operation_id", "parent", "parent_revision", "child", "child_agent", "attached_agents",
    "resources", "omissions", "child_private_volume", "child_private_generation", "inherited_context",
    "summary", "inherited_through_sequence", "shared_grants", "reference_grants", "history_witness",
    "attachment_manifests", "boundary", "provider", "namespace", "family", "version", "kind", "id",
    "class", "owner", "key", "volume", "path", "descriptor", "display_name", "sha256", "byte_length", "media_type",
    "from", "to", "reason", "record", "migration", "previous", "selected", "configurations", "extension",
    "name", "schema_digest", "implementation_digest", "fork_policy", "content", "effect_id", "guarantee",
    "effect_kind", "request", "request_digest", "result_schema", "result_schema_digest", "attempt_id", "observation",
    "status", "proof", "state", "result", "message", "seed", "receipt", "agent", "selection", "ticket", "resolution",
    "source", "revision", "required", "outcome", "evidence", "reader", "attachment_manifest", "operations",
    "checkpoint", "limits", "history_limits", "context", "payloads", "references", "source_project", "source_generation",
    "target_project", "expected_target_generation", "result_generation", "provider_operation_id", "provider_proof", "notice",
    "format", "statement", "sequence", "attachments", "items", "manifest", "item_count", "reply_to", "tool_call_id", "extensions",
    "conversation_revision", "message_ids", "deadline_unix_ms", "approval", "action_digest", "expected_version", "detail", "answer",
    "reference", "value", "paths", "history", "project", "private_volume", "process", "artifact", "shared_volume",
    "captured", "unsupported", "in_flight", "indeterminate", "planned", "dispatched", "succeeded", "failed",
    "inline", "answered", "approved", "declined", "cancelled", "expired", "denied", "session", "agent_private", "session_shared",
    "user", "assistant", "system", "tool_call", "tool_result", "interaction", "permission", "fork", "merge",
    "question", "choice", "form", "approval", "inherit", "reset", "omit", "at_most_once", "idempotent_retry", "exactly_once",
    "pending", "active", "waiting", "completed", "failed", "cancelled", "conversation", "session", "agent", "turn", "task",
    "file_bytes", "path_bytes", "attachments", "render_bytes", "model_steps", "model_events_per_step", "tool_calls_per_step",
    "context_messages", "maximum_events", "maximum_bytes",
    "read", "write", "list", "fork", "merge", "EventPayload", "ForkSeed", "ProjectMergeReceipt", "ConversationMessage",
    "ResourceRevision", "VolumeOwner", "ReferencedAttachmentsWire", "EffectStatus", "FileRefWire", "ProviderRefWire", "ResourceRefWire",
    "unknown variant ``, expected one of ", "unknown field ``, expected one of ", "duplicate field ``", "missing field ``",
    "invalid type: , expected ", "invalid value: , expected ", "invalid length , expected ", " at line  column ",
    "a sequence", "a map", "a string", "a byte array", "an integer", "an array of length 32", "a valid UUID string",
    "invalid request: ", "storage failure: ", "interaction outcome must be an object", "interaction outcome kind must be a string",
    "unknown interaction outcome field ", "SHA-256 digest must contain exactly 32 bytes", "typed resource kind mismatch",
    "provider identity is invalid", "resource key is empty", "resource version is invalid", "attachment manifest metadata is invalid",
];

const fn diagnostic_label_bytes() -> u64 {
    let mut result = 0;
    let mut index = 0;
    while index < DIAGNOSTIC_LABELS.len() {
        result += DIAGNOSTIC_LABELS[index].len() as u64 + 4;
        index += 1;
    }
    result
}
const DIAGNOSTIC_LABEL_BYTES: u64 = diagnostic_label_bytes();

const COMMON_DIAGNOSTIC_TEXT: &str = concat!(
    "unknown variant ``, expected one of unknown field ``, expected one of ",
    "duplicate field ``missing field ``invalid type: , expected ",
    "invalid value: , expected invalid length , expected  at line  column ",
    "a sequencea mapa stringa byte arrayan integeran array of length 32a valid UUID string",
    "invalid request: storage failure: ",
);

/// For audited serde-derived records and the moving reference converters only:
/// source interpolation is debug-escaped at <=6 bytes/input byte, with at most
/// two diagnostic buffers overlapping during the original error conversions.
/// `schema_label_bytes` must include the concrete field/discriminator/type
/// labels, their list punctuation, and custom static diagnostics. Arbitrary
/// custom Deserialize/Display code cannot use this as a heap guarantee.
pub(crate) fn audited_serde_diagnostic_bytes(input_bytes: u64, schema_label_bytes: u64) -> Result<u64> {
    let static_bytes = sum(&[schema_label_bytes, COMMON_DIAGNOSTIC_TEXT.len() as u64,
        2 * u64::from(usize::BITS)])?;
    sum(&[multiply(input_bytes, 6 * 2)?, multiply(static_bytes, 2)?])
}

fn multiply(value: u64, factor: u64) -> Result<u64> {
    value.checked_mul(factor).ok_or_else(|| Error::Invalid("event JSON admission overflow".into()))
}
fn sum(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(0u64, |total, value| total.checked_add(*value)
        .ok_or_else(|| Error::Invalid("event JSON admission overflow".into())))
}

/// Admits lexical structure and reserves the original concrete EventPayload
/// decode's logical quota BEFORE serde can buffer tagged fields or diagnostics.
/// Admission must become a real caller-owned quota guard/highwater, not merely
/// an allocator claim or an ignored check. No physical source read is invented.
/// This excludes opaque allocator/BTree node overhead and process RSS, but
/// includes all variable serde/typed payloads, temporary Value overlap and boxes.
pub(crate) fn event_payload_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> Result<EventPayloadJsonAdmission> {
    let bounds = JsonBounds {
        maximum_bytes: bounds.maximum_bytes.min(acyclic_stream::MAX_RECORD_BYTES as u64),
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let mut input_bytes = 0u64;
    let mut lexical_items = 0u64;
    super::json::admit_json(bytes, bounds, &mut |event| {
        match event {
            JsonCodecCharge::Read { bytes } => input_bytes = input_bytes.checked_add(bytes)
                .ok_or_else(|| Error::Invalid("event JSON byte count overflow".into()))?,
            JsonCodecCharge::Work { items, .. } => lexical_items = lexical_items.checked_add(items)
                .ok_or_else(|| Error::Invalid("event JSON item count overflow".into()))?,
            _ => {}
        }
        charge(event)
    })?;
    let admission = event_payload_ownership(input_bytes, lexical_items, bounds.maximum_depth)?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes: admission.maximum_owned_bytes })?;
    Ok(admission)
}

// The Snapshot schema contains these same concrete payloads, potentially many
// times. Reuse their audited tagged buffers/Value/string/error overlap without
// rerunning the lexer or issuing a second quota reservation.
pub(super) fn event_payload_ownership(
    input_bytes: u64, lexical_items: u64, maximum_depth: u32,
) -> Result<EventPayloadJsonAdmission> {
    // Across each concrete collection pass, total element count <= tokens and
    // nonempty collection count <= tokens. Standard Vec capacity is bounded by
    // two times its elements plus at most eight initial slots per collection.
    // Content map entries are pairs, not one scalar slot.
    let slots = multiply(lexical_items, 2 + 8)?;
    let content_slot = size_of::<(Content<'static>, Content<'static>)>()
        .max(size_of::<Content<'static>>()) as u64;
    let serde_content_bytes = multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?;
    let typed_collection_bytes = multiply(slots, TYPED_COLLECTION_SLOT_BYTES as u64)?;
    // InteractionOutcome buffers Value and then consumes its answer through
    // from_value; provider join proofs also retain arbitrary Value. Cover that
    // Value pass concurrently with Content and newly owned typed output.
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let value_overlap_bytes = multiply(slots, value_slot)?;
    let boxed_payload_bytes = BOXED_PAYLOAD_BYTES as u64;
    // Each Content pass, the Value pass, and typed output can own at most the
    // total decoded UTF-8 string bytes in the input. JSON escapes never expand
    // beyond their encoded source bytes. Borrowed input is not owned storage.
    let string_payload_bytes = multiply(input_bytes, CONTENT_LAYERS + 1 + 1)?;
    let scratch_bytes = multiply(input_bytes.max(8), 2)?;
    // Unexpected string debug escaping expands an ASCII byte to at most six
    // bytes. At most two diagnostic buffers overlap across custom from_value /
    // D::Error::custom / outer storage-error conversion. Include static schema
    // labels, punctuation, and line/column digits before interpolation happens.
    let diagnostic_bytes = audited_serde_diagnostic_bytes(input_bytes, DIAGNOSTIC_LABEL_BYTES)?;
    let maximum_owned_bytes = sum(&[serde_content_bytes, typed_collection_bytes, value_overlap_bytes,
        boxed_payload_bytes, string_payload_bytes, scratch_bytes, diagnostic_bytes])?;
    Ok(EventPayloadJsonAdmission { input_bytes, lexical_items, maximum_depth,
        maximum_owned_bytes, serde_content_bytes, typed_collection_bytes, value_overlap_bytes,
        boxed_payload_bytes, string_payload_bytes, scratch_bytes, diagnostic_bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventPayload, LifecycleState};

    fn bounds(bytes: &[u8]) -> JsonBounds {
        JsonBounds { maximum_bytes: bytes.len() as u64, maximum_items: bytes.len() as u64, maximum_depth: 256 }
    }

    #[test]
    fn concrete_guard_runs_before_original_event_payload_decoder() -> Result<()> {
        let payload = EventPayload::LifecycleTransitioned { from: LifecycleState::Pending,
            to: LifecycleState::Active, reason: Some("escape\n\"\\\u{1f600}".into()) };
        let bytes = super::super::canonical_json_bytes(&payload)?;
        let mut reserved = 0;
        let admission = event_payload_json_admission(&bytes, bounds(&bytes), &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event { reserved = maximum_owned_bytes; }
            Ok(())
        })?;
        assert_eq!(admission.input_bytes, bytes.len() as u64);
        assert_eq!(reserved, admission.maximum_owned_bytes);
        let decoded: EventPayload = super::super::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(decoded, payload);
        assert_eq!(super::super::canonical_json_bytes(&decoded)?, bytes);
        Ok(())
    }

    #[test]
    fn duplicate_key_buffering_and_diagnostic_interpolation_are_reserved() -> Result<()> {
        let bytes = br#"{"kind":"lifecycle_transitioned","from":"pending","to":"active","reason":"old","reason":"new"}"#;
        let admission = event_payload_json_admission(bytes, bounds(bytes), &mut |_| Ok(()))?;
        assert!(admission.serde_content_bytes > 0);
        assert!(admission.diagnostic_bytes >= 12 * bytes.len() as u64);
        assert!(super::super::json_from_slice::<EventPayload>(bytes).is_err());
        Ok(())
    }

    #[test]
    fn stack_and_cancellation_reject_before_schema_decode() {
        let bytes = format!("{}0{}", "[".repeat(300), "]".repeat(300));
        let mut reserved = false;
        assert!(event_payload_json_admission(bytes.as_bytes(), bounds(bytes.as_bytes()), &mut |event| {
            reserved |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        }).is_err());
        assert!(!reserved);
        let bytes = br#"{"kind":"conversation_bound","agent":"00000000-0000-0000-0000-000000000001"}"#;
        let error = Error::Conflict("original cancellation".into());
        assert_eq!(event_payload_json_admission(bytes, bounds(bytes), &mut |_| Err(error.clone())).unwrap_err(), error);
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use crate::core::EventPayload;

    proptest::proptest! {
        #[test]
        fn original_typed_collection_decode_fits_concrete_quota(
            names in proptest::collection::vec(proptest::prelude::any::<String>(), 0..24),
        ) {
            let dependencies: Vec<_> = names.into_iter().map(|name| ExtensionDependency { name, version: 1 }).collect();
            let payload = EventPayload::ExtensionsSelected {
                previous: dependencies.clone(), selected: dependencies, configurations: Vec::new(),
            };
            let bytes = super::super::canonical_json_bytes(&payload).expect("canonical production event");
            let bounds = JsonBounds { maximum_bytes: bytes.len() as u64, maximum_items: bytes.len() as u64, maximum_depth: 256 };
            let admission = event_payload_json_admission(&bytes, bounds, &mut |_| Ok(())).expect("audited schema quota");
            let decoded: EventPayload = super::super::json_from_slice(&bytes).expect("original typed decoder");
            let EventPayload::ExtensionsSelected { previous, selected, configurations } = &decoded else { unreachable!() };
            let actual_slots = (previous.capacity() + selected.capacity()) * size_of::<ExtensionDependency>()
                + configurations.capacity() * size_of::<ExtensionConfiguration>();
            let actual_strings: usize = previous.iter().chain(selected).map(|value| value.name.capacity()).sum();
            proptest::prop_assert!(admission.maximum_owned_bytes >= (actual_slots + actual_strings) as u64);
            proptest::prop_assert_eq!(super::super::canonical_json_bytes(&decoded).expect("mandatory reencode"), bytes);
        }
    }
}
