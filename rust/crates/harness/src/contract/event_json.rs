//! Admission for the concrete history EventPayload schema, not arbitrary serde.
//!
//! This audit is intentionally coupled to serde 1.0.229's Content carrier. A
//! serde upgrade must re-audit tagged buffering rather than silently changing
//! the logical reservation. The original EventPayload Deserialize implementation
//! remains the only decoder; this module does not interpret event variants.

use super::{Error, JsonBounds, JsonCodecCharge, Result};
use super::json::{JsonValueSchema, JsonValueShape};
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
    "authorized_source", "native_snapshot", "caller_task", "caller_admission_digest", "caller_fence",
    "source_task", "graph_root", "maximum_work", "policy_scope", "capabilities", "issuer", "agent",
    "parent_proof", "ProjectSourceAuthorization", "Scope", "LeaseFence", "reservation_id", "placement",
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

// Finite stock record/command graphs plus registered recursive/resource/task,
// configured Snapshot/conversation and native process/MCP stdio graphs below.
// Concrete owners supply the actual declaration/custom-static literal closure.
// Event labels stay a separate scope so ordinary Event admission is unchanged.
// Other private DTO owners add their own manifest, not a generic-T guess.
const PREPARATION_DIAGNOSTIC_LABELS: &[&str] = &[
    "Model", "ModelMessage", "ModelRole", "ModelContent", "ModelContentPart",
    "FileProjectionPolicy", "ModelDataPart", "ToolResultContent", "NativeConfigurationBinding",
    "ImageDetail", "NativeMediaIntent", "NativeMediaPolicy", "ModelRequest", "ModelEvent",
    "ToolDefinition", "EventReference", "ExtensionDependency", "ExtensionConfiguration",
    "Context", "CanonicalContextCheckpoint", "CompactionReference", "CompactionRetention",
    "ContextSummary", "ResponseProjection", "InheritedConversationPrefix", "ModelContextSelection",
    "WorkflowCommand", "MachineIdentity", "MachineStatus", "MachineTransition",
    "MachineCheckpoint", "WorkflowRecord", "WorkflowAdmission", "FileDescriptorWire",
    "ResourceKind", "workspace", "generation", "artifact", "sandbox", "checkpoint", "stream", "context", "run",
    "model", "messages", "tools", "options", "max_output_tokens", "role", "text", "file",
    "policy", "call_id", "callId", "arguments", "json", "parts", "metadata", "delta", "reasoning",
    "input_schema", "output_schema", "projection_schema", "description", "configuration",
    "intent", "image", "audio", "video", "document", "auto", "low", "high",
    "maximum_duration_ms", "maximum_frames", "maximum_pages", "bounded_full", "native",
    "format_version", "parent_agent", "through_sequence", "message_digest",
    "current_input_index", "source_digest", "maximum_messages", "retention", "retained",
    "roles", "native_media", "step", "source_messages", "output", "canonical", "compaction",
    "machine", "digest", "commands", "suspended", "idempotency_key", "input_digest",
    "prior", "input", "transition", "next", "initial", "state", "value", "Capabilities",
    "file byte length exceeds cross-language precision", "media type is invalid",
    "file or volume label is invalid", "volume-relative path is invalid",
    "data did not match any variant of untagged enum ModelContent",
    "struct ", "struct variant ", "tuple variant ", "newtype variant ", "unit variant ",
    // Registered recursive/resource/task DTO closure; declared by the concrete
    // record owners, including their fork summary/grant/reference wrappers.
    "RecursiveForkSourceIntent", "RecursiveForkToolPlan", "RecursiveMergeToolPlan",
    "ForkToolArguments", "MergeToolArguments", "MergeToolSelection", "MergeToolSide",
    "Plan", "Stage", "State", "InitialPlan", "SourcePin", "Receipt",
    "ConfiguredRecursiveSourceCheckpoint", "SourceCheckpoint", "CommandResults", "CommandResult",
    "ResourceCreationProofRef", "ResourcePreparationRequest", "TaskAdmissionRecord",
    "ComponentIdentity", "ExecutionPlacement", "TaskRunLimits", "ExtensionAdmission",
    "Limits",
    "ExtensionConfiguration", "ExtensionDependency", "EventReference",
    "ForkRequest", "ForkHistoryPolicy", "ForkPreparation", "ForkSelection", "AttestedBoundary",
    "ForkReport", "ForkSeed", "Capture", "CapturedResource", "ForkOmission",
    "SharedGrant", "ReferenceGrant", "SummaryForkSelection", "SummaryForkCapture",
    "ResourceRevision", "WorkflowCommand", "MachineIdentity",
    "arguments", "caller", "source_task", "child_operation_id", "allocation_digest", "request",
    "preparation", "child_message_pointer", "child_message_file_pointer", "source_checkpoint",
    "operation_id", "destination", "source", "source_project", "source_generation", "authorized_source",
    "target_project", "expected_target_generation", "notice", "id", "msg", "selections", "conflict", "side",
    "kind", "plan", "phase", "stage", "intent", "command", "report", "receipt", "value", "next",
    "snapshot", "request_digest", "commands", "provider", "resource", "evidence", "child", "resources",
    "creation_grant", "maximum_resources", "maximum_work", "maximum_bytes", "payload", "input_files",
    "read_authorization", "task", "machine", "input", "input_schema", "output_schema", "parent",
    "resource_proofs", "grants", "limits", "run_limits", "policy", "extensions", "execution",
    "name", "version", "digest", "build", "environment", "readiness_revision", "concurrency", "max_steps",
    "deadline_epoch_ms", "selected", "configurations", "extension", "schema_digest", "content",
    "parent_revision", "child_agent", "attached_agents", "boundary", "child_project_volume",
    "child_private_volume", "child_result_volume", "inherited_through_sequence", "maximum_inherited_messages",
    "maximum_inherited_bytes", "maximum_inherited_references", "summary", "required", "revision",
    "reference", "volume", "generation", "paths", "implementation_digest", "captures",
    "child_private_generation", "child_result_generation", "inherited_context", "shared_grants",
    "reference_grants", "history_witness", "attachment_manifests", "selection", "outcome", "omissions",
    "operations", "file", "reader", "attachment_manifest", "checkpoint", "history_limits", "context",
    "payloads", "references",
    "fork", "merge", "prepare", "observe", "fork_publish", "child_admit", "merge_apply", "merge_publish",
    "ready", "source_pinned", "waiting", "complete", "settled", "base", "target", "fresh",
    "history", "project", "private_volume", "process", "artifact", "shared_volume", "extension",
    "captured", "unsupported", "in_flight", "indeterminate",
    // Actual SnapshotWire and conversation Deserialize closure reached by the
    // configured source checkpoint; native custody fields are serde-skipped.
    "Snapshot", "SnapshotWire", "format_version", "authority", "revision", "events",
    "projection", "state_digest", "attestation", "Projection", "lifecycle",
    "active_extensions", "active_extension_revision", "extension_states",
    "configured_extensions", "active_configurations", "effects", "indexed_terminal_effects",
    "effects_archived", "forks", "published_merges", "conversation",
    "latest_context_checkpoint", "interactions", "bindings",
    "VolumeRefWire", "provider", "id", "class", "owner", "VolumeClass",
    "project", "agent_private", "session_shared", "VolumeOwner", "kind", "agent", "session",
    "FileDescriptorWire", "sha256", "byte_length", "media_type",
    "SHA-256 digest must contain exactly 32 bytes", "FileRefWire", "volume", "path",
    "version", "descriptor", "display_name", "Attachment", "file", "label",
    "ReferencedAttachmentsWire", "inline", "manifest", "items", "item_count",
    "MessageKind", "user", "assistant", "system", "tool_call", "tool_result",
    "interaction", "permission", "fork", "merge", "ConversationMessage",
    "sequence", "content", "attachments", "reply_to", "tool_call_id", "extensions",
    "ConversationState", "ConversationStateWire", "messages", "archived_prefix",
    "ArchivedConversationPrefix", "through_sequence", "history_digest", "checkpoint",
    "invalid request: ", "file or volume label is invalid", "volume-relative path is invalid",
    "media type is invalid", "attachment manifest metadata is invalid",
    "message identity is nil", "message sequence must start at one",
    "extension key must be namespaced", "tool linkage does not match message kind",
    "archived conversation prefix is unbound", "conversation order or identity is invalid",
    // Projection's actual intermediate types are distinct from EventPayload.
    "Authority", "AggregateKind", "kind", "id", "agent", "conversation", "session", "turn", "task",
    "LifecycleState", "pending", "active", "waiting", "completed", "failed", "cancelled",
    "EffectGuarantee", "exactly_once", "idempotent_retry", "at_most_once",
    "EffectStatus", "state", "planned", "dispatched", "succeeded", "result", "message", "indeterminate",
    "EffectState", "provider", "guarantee", "effect_kind", "request", "request_digest",
    "result_schema", "result_schema_digest", "status", "attempts",
    "ExtensionForkPolicy", "inherit", "reset", "reject", "ExtensionRecord",
    "name", "version", "schema_digest", "implementation_digest", "fork_policy", "content",
    "ExtensionDependency", "ExtensionConfiguration", "extension",
    "InteractionTicket", "deadline_unix_ms", "approval", "ApprovalBinding",
    "operation_id", "action_digest", "InteractionKind", "question", "choice", "form",
    "InteractionResolution", "expected_version", "outcome", "detail", "InteractionOutcome",
    "answered", "answer", "approved", "declined", "expired", "denied",
    "interaction outcome must be an object", "interaction outcome kind must be a string",
    "unknown interaction outcome field ",
    // Registered native request/result, including the real WorkBudget alias
    // (WorkCounters), lazy snapshot primitives and MCP stdio parameter carrier.
    // Runtime validation/exchange errors are not Deserialize diagnostics.
    "NativeProcessRequest", "NativeProcessResult", "NativeProcessStop", "NativeProcessStopKind",
    "NativeViewOptions", "NativeVolumeMapping", "NativeViewManifest", "NativeVolumeNamespace",
    "executable", "argv", "cwd", "environment", "timeout_ms", "control_timeout_ms",
    "cancellation_poll_ms", "maximum_output_bytes", "maximum_result_bytes", "mcp_stdio",
    "standard_input", "view", "root", "maximum_volumes", "work_per_volume", "options",
    "volumes", "volume", "generation", "namespace", "path", "writable", "authority_revision",
    "materialized_directory", "lazy_mount", "snapshot", "success", "exit_code", "stdout",
    "stderr", "stop", "kind", "message", "cleanup_completed",
    "timeout", "output_limit", "control_stopped", "capture_failed",
    "McpStdioRequest", "McpStdioMethod", "initialization", "operation", "method", "params",
    "maximum_bytes", "tools/list", "tools/call",
    "LazySnapshotRef", "SourceReference", "id", "workspace_id", "authored_generation",
    "source", "overlay", "shadows", "identity", "epoch",
    "WorkCounters", "authorityRecordsRead", "authorityRecordsAppended", "authorityBytesRead",
    "authorityBytesWritten", "objectProbes", "backendReadOperations", "backendWriteOperations",
    "durabilityOperations", "pageReads", "pageWrites", "objectBytesRead", "objectBytesWritten",
    "bytesHashed", "bytesCopied", "bytesEncoded", "sourceBytesRead", "sourcePathComponents",
    "sourceEntriesVisited", "outputBytes", "itemsExamined", "itemsReturned", "allocationOperations",
    "peakAllocationBytes", "materializations", "path string", "an array of length 16",
    // Actual stock command wrappers and SelectedModelContext; their nested
    // model/context/ref graphs are the concrete declarations covered above.
    "TimerTaskCommand", "TaskObserveCommand", "TaskAdmitCommand", "ModelTaskCommand",
    "ToolTaskCommand", "MailSendTaskCommand", "MailReceiveTaskCommand", "SelectedModelContext",
    "deadline_unix_ms", "admission_operation", "name", "version", "input",
    "selected_context", "max_steps", "max_output_tokens", "revision", "arguments",
    "recipient", "payload", "after", "selection", "messages",
];

/// Compile-time label/punctuation upper bound for the concrete preparation
/// graphs listed above: stock records/commands, recursive/resource/task DTOs,
/// configured Snapshot/conversation, native process/MCP, refs and Event/SourceAuth.
/// Covers their actual derived type/variant/field names and audited moving ref
/// constructors' static errors, not arbitrary custom Deserialize/Display code.
/// Other private/external DTOs add their actual manifest. No authority is granted.
pub const fn preparation_serde_diagnostic_label_bytes() -> u64 {
    let mut result = DIAGNOSTIC_LABEL_BYTES;
    let mut index = 0;
    while index < PREPARATION_DIAGNOSTIC_LABELS.len() {
        result += PREPARATION_DIAGNOSTIC_LABELS[index].len() as u64 + 4;
        index += 1;
    }
    result
}

/// Diagnostic capacity for the finite concrete SDK preparation graphs above.
/// Other private records add their actual field/type/custom-static manifest.
/// No unknown custom Deserialize/Display message is supported by this inventory.
pub(crate) fn audited_preparation_diagnostic_bytes(
    input_bytes: u64, native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    diagnostic_capacity_bytes(input_bytes, preparation_serde_diagnostic_label_bytes(), native_code)
}

/// Pure quota arithmetic for an SDK-issued shape and an audited closed DTO.
/// `schema_label_bytes` MUST cover actual field/discriminator/type names, list
/// punctuation and custom STATIC diagnostics. Arbitrary custom Deserialize or
/// Display messages are unsupported. The shape grants no authority and this is
/// not an allocator/RSS, generic serde heap, CPU or stack theorem.
/// Slice input bytes or resident UTF-8 payload plus visited nodes/keys bound the
/// actual source interpolation. Both overlapping formatter buffers include
/// conservative capacity growth. None preserves ordinary Invalid; Some(code)
/// selects the inline native carrier at the actual native entry.
pub fn json_typed_diagnostic_owned_bytes(
    shape: JsonValueShape, schema_label_bytes: u64, native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    diagnostic_capacity_bytes(shape.input_bytes(), schema_label_bytes, native_code)
}

fn diagnostic_capacity_bytes(
    input_bytes: u64, schema_label_bytes: u64, native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    // Existing six-byte debug expansion and two overlapping formatters, with
    // another factor two for their geometric String capacities, not fake ops.
    multiply_with_code(diagnostic_bytes(input_bytes, schema_label_bytes, native_code)?, 2, native_code)
}

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
    diagnostic_bytes(input_bytes, schema_label_bytes, None)
}

fn diagnostic_bytes(input_bytes: u64, schema_label_bytes: u64, native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    let sum = |values: &[u64]| sum_with_code(values, native_code);
    let multiply = |value, factor| multiply_with_code(value, factor, native_code);
    let static_bytes = sum(&[schema_label_bytes, COMMON_DIAGNOSTIC_TEXT.len() as u64,
        2 * u64::from(usize::BITS)])?;
    sum(&[multiply(input_bytes, 6 * 2)?, multiply(static_bytes, 2)?])
}

fn ownership_overflow(native_code: Option<crate::wire::ErrorCode>) -> Error {
    match native_code {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(code) => Error::NativeBoundary { code, reason: "event JSON admission overflow" },
        _ => Error::Invalid("event JSON admission overflow".into()),
    }
}

fn multiply_with_code(value: u64, factor: u64, native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    value.checked_mul(factor).ok_or_else(|| ownership_overflow(native_code))
}
fn sum_with_code(values: &[u64], native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    values.iter().try_fold(0u64, |total, value| total.checked_add(*value)
        .ok_or_else(|| ownership_overflow(native_code)))
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
    let value_shape = super::json::admit_json_value_shape(
        bytes, bounds, JsonValueSchema::EventPayload, &mut |event| charge(event),
    )?;
    let admission = event_payload_ownership(value_shape, bounds.maximum_depth, None)?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes: admission.maximum_owned_bytes, value_shape })?;
    Ok(admission)
}

// The Snapshot schema contains these same concrete payloads, potentially many
// times. Reuse their audited tagged buffers/Value/string/error overlap without
// rerunning the lexer or issuing a second quota reservation.
pub(crate) fn event_payload_ownership(
    shape: JsonValueShape, maximum_depth: u32, native_code: Option<crate::wire::ErrorCode>,
) -> Result<EventPayloadJsonAdmission> {
    let multiply = |value, factor| multiply_with_code(value, factor, native_code);
    let sum = |values: &[u64]| sum_with_code(values, native_code);
    let input_bytes = shape.input_bytes();
    let lexical_items = shape.lexical_items();
    // Across each concrete collection pass, total element count <= tokens and
    // nonempty collection count <= tokens. Standard Vec capacity is bounded by
    // two times its elements plus at most eight initial slots per collection.
    // Content map entries are pairs, not one scalar slot.
    let slots = multiply(lexical_items, 2 + 8)?;
    let content_slot = size_of::<(Content<'static>, Content<'static>)>()
        .max(size_of::<Content<'static>>()) as u64;
    let serde_content_bytes = multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?;
    let typed_collection_bytes = multiply(slots, TYPED_COLLECTION_SLOT_BYTES as u64)?;
    // InteractionOutcome buffers one Value tree, then consumes answer as FileRef
    // or operation_id as UUID. Those typed from_value converters do not decode
    // another Value tree. Cover the one tree concurrently with Content and typed
    // output; only selected Raw sources add hidden nodes and live buffers.
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let value_overlap_bytes = sum(&[
        multiply(slots, value_slot)?,
        super::json::json_value_virtual_owned_bytes(shape).map_err(|error| match (native_code, error) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (Some(code), Error::NativeBoundary { reason, .. }) => Error::NativeBoundary { code, reason },
            (_, error) => error,
        })?,
    ])?;
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
    let diagnostic_bytes = diagnostic_bytes(input_bytes, DIAGNOSTIC_LABEL_BYTES, native_code)?;
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
    use crate::interaction::{InteractionOutcome, InteractionResolution};

    fn multiply(value: u64, factor: u64) -> Result<u64> { multiply_with_code(value, factor, None) }
    fn sum(values: &[u64]) -> Result<u64> { sum_with_code(values, None) }

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
            if let JsonCodecCharge::Admission { maximum_owned_bytes, .. } = event { reserved = maximum_owned_bytes; }
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
        let operation = crate::OperationId::from_bytes([92; 16]);
        let mut error = Some(Error::Indeterminate(operation));
        let result = event_payload_json_admission(bytes, bounds(bytes), &mut |_| {
            Err(error.take().expect("admission must stop after its first failure"))
        });
        assert!(matches!(result, Err(Error::Indeterminate(original)) if original == operation));
    }

    fn raw_event_bytes(raw: &str) -> Result<Vec<u8>> {
        // Normal serde fixture construction, not another admission parser.
        serde_json::to_vec(&serde_json::json!({
            "kind": "interaction_resolved",
            "resolution": {
                "id": "00000000-0000-0000-0000-000000000001",
                "expected_version": 1,
                "outcome": { "$serde_json::private::RawValue": raw },
            },
        })).map_err(|error| Error::Storage(error.to_string()))
    }

    fn assert_normal_quota(admission: EventPayloadJsonAdmission) -> Result<()> {
        let slots = multiply(admission.lexical_items, 10)?;
        let content_slot = size_of::<(Content<'static>, Content<'static>)>()
            .max(size_of::<Content<'static>>()) as u64;
        let expected = [
            multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?,
            multiply(slots, TYPED_COLLECTION_SLOT_BYTES as u64)?,
            multiply(slots, size_of::<Value>().max(size_of::<(String, Value)>()) as u64)?,
            BOXED_PAYLOAD_BYTES as u64,
            multiply(admission.input_bytes, CONTENT_LAYERS + 2)?,
            multiply(admission.input_bytes.max(8), 2)?,
            audited_serde_diagnostic_bytes(admission.input_bytes, DIAGNOSTIC_LABEL_BYTES)?,
        ];
        assert_eq!([
            admission.serde_content_bytes, admission.typed_collection_bytes,
            admission.value_overlap_bytes, admission.boxed_payload_bytes,
            admission.string_payload_bytes, admission.scratch_bytes, admission.diagnostic_bytes,
        ], expected);
        assert_eq!(admission.maximum_owned_bytes, sum(&expected)?);
        Ok(())
    }

    #[test]
    fn ordinary_outcomes_keep_normal_quota_and_original_typed_converters() -> Result<()> {
        use crate::{
            conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
            resources::ProviderRef,
        };
        let answer = FileRef::new(
            VolumeRef::new(ProviderRef::new("test", "filesystem", "2")?,
                "interaction-private", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([8; 16])))?,
            "answer.json", "immutable-1",
            FileDescriptor::from_bytes(b"null", "application/json")?, "interaction.json",
        )?;
        for outcome in [
            InteractionOutcome::Answered { answer: Box::new(answer) },
            InteractionOutcome::Indeterminate { operation_id: crate::OperationId::from_bytes([1; 16]) },
        ] {
            let payload = EventPayload::InteractionResolved { resolution: InteractionResolution {
                id: Uuid::from_bytes([2; 16]), expected_version: 1, outcome, detail: None,
            } };
            let bytes = serde_json::to_vec(&payload).map_err(|error| Error::Storage(error.to_string()))?;
            let mut read = 0;
            let mut work = 0;
            let admission = event_payload_json_admission(&bytes, bounds(&bytes), &mut |event| {
                match event {
                    JsonCodecCharge::Read { bytes } => read += bytes,
                    JsonCodecCharge::Work { items, .. } => work += items,
                    JsonCodecCharge::Inspect { .. } => panic!("ordinary outcome has no Raw inspection"),
                    JsonCodecCharge::Admission { value_shape, .. } => {
                        assert_eq!(value_shape.virtual_items(), 0);
                        assert!(!value_shape.unresolved_raw());
                    }
                    _ => {}
                }
                Ok(())
            })?;
            assert_eq!(read, bytes.len() as u64);
            assert_eq!(work, admission.lexical_items);
            assert_normal_quota(admission)?;
            // FileRef/UUID consume the existing Value, not another Value decoder.
            let decoded: EventPayload = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(decoded, payload);
        }
        Ok(())
    }

    #[test]
    fn escaped_raw_outcome_is_guarded_before_original_serde() -> Result<()> {
        let bytes = br#"{"kind":"interaction_resolved","resolution":{"id":"00000000-0000-0000-0000-000000000001","expected_version":1,"outcome":{"\u0024serde_json::private::RawValue":"{\"kind\":\"approved\"}"}}}"#;
        let mut read = 0;
        let mut work = 0;
        let mut inspect = 0;
        let mut admitted_shape = None;
        let admission = event_payload_json_admission(bytes, bounds(bytes), &mut |event| {
            match event {
                JsonCodecCharge::Read { bytes } => read += bytes,
                JsonCodecCharge::Work { items, .. } => work += items,
                JsonCodecCharge::Inspect { items } => inspect += items,
                JsonCodecCharge::Admission { value_shape, .. } => admitted_shape = Some(value_shape),
                _ => {}
            }
            Ok(())
        })?;
        let shape = admitted_shape.expect("actual production admission");
        assert_eq!(read, bytes.len() as u64);
        assert_eq!(work, shape.lexical_items());
        assert_eq!(admission.lexical_items, work);
        assert!(inspect > 0);
        assert!(shape.virtual_items() > 0);
        let normal_value = multiply(multiply(shape.lexical_items(), 10)?,
            size_of::<Value>().max(size_of::<(String, Value)>()) as u64)?;
        assert_eq!(admission.value_overlap_bytes,
            sum(&[normal_value, super::super::json::json_value_virtual_owned_bytes(shape)?])?);
        assert!(admission.value_overlap_bytes > normal_value);
        let decoded: EventPayload = serde_json::from_slice(bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(matches!(decoded, EventPayload::InteractionResolved {
            resolution: InteractionResolution { outcome: InteractionOutcome::Approved, .. },
        }));
        Ok(())
    }

    #[test]
    fn hidden_raw_items_and_depth_reject_before_serde_and_reservation() -> Result<()> {
        let wide = format!(r#"{{"kind":"approved","extra":[{}]}}"#, vec!["0"; 96].join(","));
        let deep = format!(r#"{{"kind":"approved","extra":{}0{}}}"#, "[".repeat(40), "]".repeat(40));
        for (raw, restrict_items) in [(wide, true), (deep, false)] {
            let bytes = raw_event_bytes(&raw)?;
            let mut shape = None;
            event_payload_json_admission(&bytes, bounds(&bytes), &mut |event| {
                if let JsonCodecCharge::Admission { value_shape, .. } = event { shape = Some(value_shape); }
                Ok(())
            })?;
            let shape = shape.expect("actual source shape");
            assert!(shape.virtual_items() > 0);
            let mut limited = bounds(&bytes);
            if restrict_items { limited.maximum_items = shape.lexical_items(); }
            else { limited.maximum_depth = 16; }
            let mut reserved = false;
            let mut decoded = false;
            let result = event_payload_json_admission(&bytes, limited, &mut |event| {
                reserved |= matches!(event, JsonCodecCharge::Admission { .. });
                Ok(())
            }).and_then(|_| {
                decoded = true;
                serde_json::from_slice::<EventPayload>(&bytes)
                    .map_err(|error| Error::Storage(error.to_string()))
            });
            assert!(result.is_err());
            assert!(!reserved);
            assert!(!decoded);
        }
        Ok(())
    }

    #[test]
    fn raw_inspection_and_quota_callbacks_keep_original_first_cause() -> Result<()> {
        let bytes = raw_event_bytes(r#"{"kind":"approved"}"#)?;
        for stop_at_admission in [false, true] {
            let mut original = Some(Error::Conflict("original Raw callback cancellation".into()));
            let mut reached = false;
            let mut reserved = false;
            let result = event_payload_json_admission(&bytes, bounds(&bytes), &mut |event| {
                let admission = matches!(event, JsonCodecCharge::Admission { .. });
                reserved |= admission;
                if (stop_at_admission && admission)
                    || (!stop_at_admission && matches!(event, JsonCodecCharge::Inspect { .. })) {
                    reached = true;
                    return Err(original.take().expect("original cause moves once"));
                }
                Ok(())
            });
            assert!(reached);
            assert!(matches!(result.unwrap_err(), Error::Conflict(message) if message == "original Raw callback cancellation"));
            assert_eq!(reserved, stop_at_admission);
        }
        Ok(())
    }

    #[test]
    fn raw_magic_in_ignored_non_value_field_is_not_interpreted() -> Result<()> {
        let bytes = br#"{"kind":"lifecycle_transitioned","from":"pending","to":"active","ignored":{"$serde_json::private::RawValue":"not JSON"},"reason":"$serde_json::private::RawValue"}"#;
        let admission = event_payload_json_admission(bytes, bounds(bytes), &mut |event| {
            assert!(!matches!(event, JsonCodecCharge::Inspect { .. }));
            if let JsonCodecCharge::Admission { value_shape, .. } = event {
                assert_eq!(value_shape.virtual_items(), 0);
                assert!(!value_shape.unresolved_raw());
            }
            Ok(())
        })?;
        assert_normal_quota(admission)?;
        assert!(matches!(serde_json::from_slice::<EventPayload>(bytes)
            .map_err(|error| Error::Storage(error.to_string()))?,
            EventPayload::LifecycleTransitioned { .. }));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn typed_diagnostic_capacity_reuses_real_model_errors_and_original_admission() -> Result<()> {
        let labels = preparation_serde_diagnostic_label_bytes();
        assert!(labels > DIAGNOSTIC_LABEL_BYTES, "stock graph labels extend, not replace, the Event inventory");
        for source in [
            serde_json::json!({ "kind": "\0\n\"\\😀".repeat(4096) }),
            serde_json::json!({ "kind": "text", "text": "ok", ("\0\n\"\\😀".repeat(4096)): true }),
        ] {
            let issued = super::super::json::json_owned_value_shape_bounded(
                &source, JsonBounds { maximum_bytes: 1024 * 1024, maximum_items: 1024 * 1024, maximum_depth: 256 },
                JsonValueSchema::None, &mut |_| Ok(()),
            )?;
            let expected = audited_serde_diagnostic_bytes(issued.input_bytes(), labels)?.checked_mul(2).unwrap();
            let mut admitted = 0;
            let error = super::super::json::json_from_value_bounded_native_trusted::<crate::model::ModelContentPart, _>(
                &source, JsonBounds { maximum_bytes: 1024 * 1024, maximum_items: 1024 * 1024, maximum_depth: 256 },
                0, crate::wire::ErrorCode::Invalid, JsonValueSchema::ModelContentPart, &mut |event| {
                    if let JsonCodecCharge::Admission { value_shape, .. } = event {
                        admitted = json_typed_diagnostic_owned_bytes(value_shape, labels,
                            Some(crate::wire::ErrorCode::Invalid))?;
                    }
                    Ok(())
                },
            ).unwrap_err();
            assert_eq!(admitted, expected, "same issued source shape, no second parser or formatter recipe");
            let Error::NativeDecode { source: actual, .. } = error else { panic!("original typed serde source") };
            let oracle = <crate::model::ModelContentPart as serde::Deserialize>::deserialize(&source).unwrap_err();
            assert_eq!(actual.to_string(), oracle.to_string());
            // Test-only Display observes the original message, not opaque
            // formatter capacities or a fabricated allocation operation.
            assert!(admitted >= actual.to_string().len() as u64);
            assert_eq!(audited_preparation_diagnostic_bytes(issued.input_bytes(),
                Some(crate::wire::ErrorCode::Invalid))?, expected);
            assert!(matches!(json_typed_diagnostic_owned_bytes(issued, u64::MAX, None), Err(Error::Invalid(_))));
            assert!(matches!(json_typed_diagnostic_owned_bytes(issued, u64::MAX,
                Some(crate::wire::ErrorCode::Storage)),
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. })));
        }
        Ok(())
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
