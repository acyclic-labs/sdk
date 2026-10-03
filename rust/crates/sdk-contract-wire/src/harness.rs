//! Rust-authored Harness v2 wire-contract surface.
//!
//! This module exposes the complete descriptor model needed by SDK and docs
//! exporters. The checked-in descriptor is an immutable migration oracle; the
//! runtime handshake identity remains separate from the canonical schema hash.

use crate::protocol;
pub use acyclic_sdk_contract_options::OptionTarget;
use acyclic_sdk_contract_options::{OPTION_SPECS, OptionSpec};
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
use sha2::{Digest, Sha256};

pub const FILE_NAME: &str = "harness/v2/harness.proto";
pub const PACKAGE: &str = "acyclic.harness.v2";
pub const SYNTAX: &str = "proto3";
pub const GO_PACKAGE: &str = "github.com/acyclic-labs/sdk/go/gen/harness/v2;harnessv2";
pub const HANDSHAKE_VERSION: &str = "2";
pub const ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST: &str =
    "blake3:8efc8c682b2ba1025b1221dd203685bdf999d04e87568fad3acdf0e428bd84cf";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractModel {
    pub file_name: &'static str,
    pub package: &'static str,
    pub syntax: &'static str,
    pub handshake_version: &'static str,
    pub archived_handshake_descriptor_digest: &'static str,
}
pub const HARNESS: ContractModel = ContractModel {
    file_name: FILE_NAME,
    package: PACKAGE,
    syntax: SYNTAX,
    handshake_version: HANDSHAKE_VERSION,
    archived_handshake_descriptor_digest: ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST,
};
pub const CONTRACT: ContractModel = HARNESS;

pub type ValidationOptionSpec = OptionSpec;
pub const VALIDATION_OPTION_SPECS: &[ValidationOptionSpec] = OPTION_SPECS;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtensionSpec {
    pub extendee: &'static str,
    pub name: &'static str,
    pub number: u32,
    pub scalar_type: &'static str,
}
pub const PROTO2_VALIDATION_EXTENSIONS: &[ExtensionSpec] = &[
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "nonzero_fixed_bytes",
        number: 51001,
        scalar_type: "uint32",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "required_message",
        number: 51002,
        scalar_type: "bool",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "positive_uint64",
        number: 51003,
        scalar_type: "bool",
    },
    ExtensionSpec {
        extendee: "google.protobuf.OneofOptions",
        name: "required_oneof",
        number: 51004,
        scalar_type: "bool",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "min_items",
        number: 51005,
        scalar_type: "uint32",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "max_items",
        number: 51006,
        scalar_type: "uint32",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "nonempty_max_bytes",
        number: 51007,
        scalar_type: "uint32",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "max_uint64",
        number: 51008,
        scalar_type: "uint64",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "known_nonzero_enum",
        number: 51009,
        scalar_type: "bool",
    },
    ExtensionSpec {
        extendee: "google.protobuf.FieldOptions",
        name: "nonempty_max_item_bytes",
        number: 51010,
        scalar_type: "uint32",
    },
    ExtensionSpec {
        extendee: "google.protobuf.EnumValueOptions",
        name: "partial_terminal",
        number: 51011,
        scalar_type: "bool",
    },
    ExtensionSpec {
        extendee: "google.protobuf.MethodOptions",
        name: "http_path",
        number: 51012,
        scalar_type: "string",
    },
];

/// Source-owned documentation for one contract declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractDoc {
    pub name: &'static str,
    pub text: &'static str,
}

/// Source-owned field documentation qualified by message name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldDoc {
    pub message: &'static str,
    pub name: &'static str,
    pub text: &'static str,
}

#[rustfmt::skip]
pub const HARNESS_MESSAGE_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "OperationIdentity", text: "The stable operation and idempotency identity for a Harness command." },
    ContractDoc { name: "Error", text: "A stable non-secret Harness error." },
    ContractDoc { name: "Admission", text: "The accepted or rejected admission result for an operation." },
    ContractDoc { name: "OperationStatus", text: "The durable completion state and ownership of an operation." },
    ContractDoc { name: "ObserveRequest", text: "Requests an operation status observation." },
    ContractDoc { name: "CancelRequest", text: "Requests cancellation of an admitted operation." },
    ContractDoc { name: "CancelResponse", text: "Carries the result of the cancel operation." },
    ContractDoc { name: "Cursor", text: "An opaque resumable delivery cursor." },
    ContractDoc { name: "Authority", text: "The stable identity of one ordered durable aggregate." },
    ContractDoc { name: "EventReference", text: "The causal predecessor of an event in another aggregate." },
    ContractDoc { name: "Scope", text: "Bearer capabilities granted to an admitted command." },
    ContractDoc { name: "RecordedScope", text: "Non-bearer scope metadata recorded with an event." },
    ContractDoc { name: "CommandEnvelope", text: "A transport-neutral command interpreted by Rust." },
    ContractDoc { name: "EventEnvelope", text: "A canonical durable event shared by every transport." },
    ContractDoc { name: "ApplyResponse", text: "The result of applying one command event." },
    ContractDoc { name: "SnapshotEnvelope", text: "A versioned restoration accelerator; events remain authoritative." },
    ContractDoc { name: "ReplayCursor", text: "The per-authority cursor used to resume delivery." },
    ContractDoc { name: "ResumeRequest", text: "Requests resumable delivery from one or more cursors." },
    ContractDoc { name: "Delivery", text: "One validated contiguous authoritative delivery." },
    ContractDoc { name: "Acknowledge", text: "Acknowledges a delivered range for an authority." },
    ContractDoc { name: "ClientFrame", text: "A client transport frame carrying one Harness action." },
    ContractDoc { name: "ServerFrame", text: "A server transport frame carrying one Harness result." },
    ContractDoc { name: "SchedulerEventEnvelope", text: "A canonical record in the coordinator Stream." },
    ContractDoc { name: "ProviderRef", text: "A provider namespace and version identity." },
    ContractDoc { name: "VolumeOwner", text: "The owner selector for a shared volume." },
    ContractDoc { name: "VolumeRef", text: "A provider-owned volume identity and class." },
    ContractDoc { name: "FileDescriptor", text: "Immutable file metadata used by a FileRef." },
    ContractDoc { name: "FileRef", text: "An immutable reference to a file in a Harness volume." },
    ContractDoc { name: "TaskOutcome", text: "The terminal outcome of a task, including resumable failure states." },
    ContractDoc { name: "ExtensionRecord", text: "The immutable implementation record for one extension." },
    ContractDoc { name: "ExtensionStateMigration", text: "A state migration from a prior extension record." },
    ContractDoc { name: "ExtensionDependency", text: "An extension name and version dependency." },
    ContractDoc { name: "ExtensionConfiguration", text: "Configured extension content and schema identity." },
    ContractDoc { name: "ExtensionSelection", text: "The prior and selected extension dependencies and configurations." },
    ContractDoc { name: "ExtensionConfigured", text: "The extension configuration admitted for use." },
    ContractDoc { name: "ExtensionAdmission", text: "The extension selection admitted with its source event." },
    ContractDoc { name: "ModelContextSelection", text: "The conversation revision and messages selected for model context." },
    ContractDoc { name: "ApprovalBinding", text: "Binds an approval to one operation and action digest." },
    ContractDoc { name: "InteractionTicket", text: "A durable user interaction request and deadline." },
    ContractDoc { name: "InteractionOutcomeMarker", text: "A marker for a terminal interaction outcome." },
    ContractDoc { name: "InteractionOutcome", text: "The selected terminal result of an interaction." },
    ContractDoc { name: "InteractionResolution", text: "Resolves one interaction at an expected version." },
    ContractDoc { name: "ResolutionReceipt", text: "The versioned receipt for an interaction resolution." },
    ContractDoc { name: "Attachment", text: "One referenced conversation attachment." },
    ContractDoc { name: "AttachmentItems", text: "Inline attachment items carried by a conversation." },
    ContractDoc { name: "AttachmentManifest", text: "A FileRef manifest describing attachment items." },
    ContractDoc { name: "ReferencedAttachments", text: "Conversation attachments represented by inline items or a manifest." },
    ContractDoc { name: "ConversationMessage", text: "A durable ref-only conversation message; bodies remain in Filesystem objects." },
    ContractDoc { name: "ExtensionsEntry", text: "One extensions entry entry." },
    ContractDoc { name: "ResourceRef", text: "A provider resource narrowed by kind, key, and optional version." },
    ContractDoc { name: "ComponentIdentity", text: "Durable task and execution contracts. JSON fields are canonical bounded schema/value documents, never file bodies, credentials, or event payloads." },
    ContractDoc { name: "MachineIdentity", text: "The immutable identity of an execution machine." },
    ContractDoc { name: "MachineCheckpoint", text: "An exact resumable machine state and revision." },
    ContractDoc { name: "WorkflowAdmission", text: "The admitted initial state of a resumable workflow." },
    ContractDoc { name: "WorkflowCommand", text: "One command applied during a workflow transition." },
    ContractDoc { name: "WorkflowTransition", text: "The canonical result of one workflow transition." },
    ContractDoc { name: "WorkflowRecord", text: "The durable admitted workflow input and transition record." },
    ContractDoc { name: "RuntimeLimits", text: "Bounds applied to a task execution." },
    ContractDoc { name: "TaskRunLimits", text: "Optional run-level bounds for a task." },
    ContractDoc { name: "ExecutionPlacement", text: "The provider, build, environment, and readiness selected for execution." },
    ContractDoc { name: "TaskAdmissionRecord", text: "The complete durable admission record for a task." },
    ContractDoc { name: "DurableBatchRequest", text: "A bounded batch of durable task admissions." },
    ContractDoc { name: "GenerationRef", text: "Narrows a provider-owned resource to an immutable Filesystem generation." },
    ContractDoc { name: "PrivateDirectoryEntry", text: "One entry in an owner-authenticated private directory page." },
    ContractDoc { name: "PrivateDirectoryPage", text: "One bounded page of an agent-private directory." },
    ContractDoc { name: "ProjectRevision", text: "A project volume and generation revision." },
    ContractDoc { name: "ExtensionRevision", text: "An extension revision and implementation digest." },
    ContractDoc { name: "ResourceRevision", text: "A captured revision of one provider resource." },
    ContractDoc { name: "CapturedResource", text: "A resource revision captured for a fork." },
    ContractDoc { name: "ForkOmission", text: "A resource omitted from a fork with its reason." },
    ContractDoc { name: "AttestedBoundary", text: "Provider attestation for a fork boundary." },
    ContractDoc { name: "SharedGrant", text: "A child-agent grant for shared-volume operations." },
    ContractDoc { name: "ReferenceGrant", text: "A child-agent grant for an immutable file reference." },
    ContractDoc { name: "ForkSelection", text: "A resource revision selected for fork capture." },
    ContractDoc { name: "ForkPreparation", text: "The child volumes and inheritance bounds prepared for a fork." },
    ContractDoc { name: "ForkRequest", text: "Carries the inputs for the fork operation." },
    ContractDoc { name: "Capture", text: "The captured or omitted result for one fork resource." },
    ContractDoc { name: "ForkReport", text: "The captured resources, grants, and child volumes produced by a fork." },
    ContractDoc { name: "ForkSeed", text: "The deterministic seed used to initialize a child fork." },
    ContractDoc { name: "ProviderJoinProof", text: "One parent-authorized Filesystem join. The notice is admitted atomically with the receipt; no private volume, model context, or child history merges." },
    ContractDoc { name: "ProjectMergeReceipt", text: "An atomic parent-authorized Filesystem project join receipt." },
];

#[rustfmt::skip]
pub const HARNESS_FIELD_DOCS: &[FieldDoc] = &[
    FieldDoc { message: "OperationIdentity", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "OperationIdentity", name: "idempotency_key", text: "The client idempotency key for this mutation." },
    FieldDoc { message: "Error", name: "code", text: "The error code carried by error." },
    FieldDoc { message: "Error", name: "message", text: "A stable non-secret diagnostic message." },
    FieldDoc { message: "Error", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "Admission", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "Admission", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "Admission", name: "error", text: "The structured error for a failed operation." },
    FieldDoc { message: "OperationStatus", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "OperationStatus", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "OperationStatus", name: "error", text: "The structured error for a failed operation." },
    FieldDoc { message: "OperationStatus", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "OperationStatus", name: "owner", text: "The authority carried by operation status." },
    FieldDoc { message: "OperationStatus", name: "cancellation_requested", text: "The cancellation requested value carried by operation status." },
    FieldDoc { message: "OperationStatus", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "ObserveRequest", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "ObserveRequest", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "ObserveRequest", name: "owner", text: "The authority carried by observe request." },
    FieldDoc { message: "ObserveRequest", name: "scope", text: "The scope carried by observe request." },
    FieldDoc { message: "CancelRequest", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "CancelRequest", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "CancelRequest", name: "owner", text: "The authority carried by cancel request." },
    FieldDoc { message: "CancelRequest", name: "scope", text: "The scope carried by cancel request." },
    FieldDoc { message: "CancelRequest", name: "recursive", text: "The recursive value carried by cancel request." },
    FieldDoc { message: "CancelRequest", name: "idempotency_key", text: "The client idempotency key for this mutation." },
    FieldDoc { message: "CancelResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "CancelResponse", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "Cursor", name: "opaque", text: "The opaque value carried by cursor." },
    FieldDoc { message: "Authority", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "Authority", name: "id", text: "The id value carried by authority." },
    FieldDoc { message: "EventReference", name: "authority", text: "The authority carried by event reference." },
    FieldDoc { message: "EventReference", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "Scope", name: "id", text: "The id value carried by scope." },
    FieldDoc { message: "Scope", name: "capabilities", text: "Capabilities advertised or granted by the provider." },
    FieldDoc { message: "Scope", name: "issuer", text: "The issuer value carried by scope." },
    FieldDoc { message: "Scope", name: "parent_proof", text: "The parent proof value carried by scope." },
    FieldDoc { message: "Scope", name: "proof", text: "The proof value carried by scope." },
    FieldDoc { message: "Scope", name: "agent_id", text: "The agent id value carried by scope." },
    FieldDoc { message: "RecordedScope", name: "id", text: "The id value carried by recorded scope." },
    FieldDoc { message: "RecordedScope", name: "capabilities", text: "Capabilities advertised or granted by the provider." },
    FieldDoc { message: "RecordedScope", name: "issuer", text: "The issuer value carried by recorded scope." },
    FieldDoc { message: "RecordedScope", name: "agent_id", text: "The agent id value carried by recorded scope." },
    FieldDoc { message: "CommandEnvelope", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "CommandEnvelope", name: "authority", text: "The authority carried by command envelope." },
    FieldDoc { message: "CommandEnvelope", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "CommandEnvelope", name: "expected_revision", text: "The revision required before applying this command." },
    FieldDoc { message: "CommandEnvelope", name: "scope", text: "The scope carried by command envelope." },
    FieldDoc { message: "CommandEnvelope", name: "causal_parent", text: "The causal parent event reference." },
    FieldDoc { message: "CommandEnvelope", name: "action_type", text: "The stable command action type." },
    FieldDoc { message: "CommandEnvelope", name: "canonical_action_json", text: "The canonical action json value carried by command envelope." },
    FieldDoc { message: "CommandEnvelope", name: "intent_digest", text: "The digest of the intended action." },
    FieldDoc { message: "EventEnvelope", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "EventEnvelope", name: "authority", text: "The authority carried by event envelope." },
    FieldDoc { message: "EventEnvelope", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "EventEnvelope", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "EventEnvelope", name: "intent_digest", text: "The digest of the intended action." },
    FieldDoc { message: "EventEnvelope", name: "scope", text: "The recorded scope carried by event envelope." },
    FieldDoc { message: "EventEnvelope", name: "causal_parent", text: "The causal parent event reference." },
    FieldDoc { message: "EventEnvelope", name: "event_type", text: "The stable event type name." },
    FieldDoc { message: "EventEnvelope", name: "canonical_payload_json", text: "The canonical bounded JSON command payload." },
    FieldDoc { message: "EventEnvelope", name: "attestation", text: "The attestation value carried by event envelope." },
    FieldDoc { message: "ApplyResponse", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "ApplyResponse", name: "event", text: "The event envelope carried by apply response." },
    FieldDoc { message: "SnapshotEnvelope", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "SnapshotEnvelope", name: "authority", text: "The authority carried by snapshot envelope." },
    FieldDoc { message: "SnapshotEnvelope", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "SnapshotEnvelope", name: "format_version", text: "The format version value carried by snapshot envelope." },
    FieldDoc { message: "SnapshotEnvelope", name: "canonical_state_json", text: "The canonical bounded JSON state representation." },
    FieldDoc { message: "SnapshotEnvelope", name: "state_digest", text: "The digest of the canonical snapshot state." },
    FieldDoc { message: "ReplayCursor", name: "authority", text: "The authority carried by replay cursor." },
    FieldDoc { message: "ReplayCursor", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ReplayCursor", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "ResumeRequest", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "ResumeRequest", name: "cursors", text: "The repeated cursors values carried by resume request." },
    FieldDoc { message: "Delivery", name: "authority", text: "The authority carried by delivery." },
    FieldDoc { message: "Delivery", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "Delivery", name: "from_revision", text: "The from revision value carried by delivery." },
    FieldDoc { message: "Delivery", name: "through_revision", text: "The through revision value carried by delivery." },
    FieldDoc { message: "Delivery", name: "events", text: "The repeated events values carried by delivery." },
    FieldDoc { message: "Delivery", name: "live", text: "The live value carried by delivery." },
    FieldDoc { message: "Acknowledge", name: "authority", text: "The authority carried by acknowledge." },
    FieldDoc { message: "Acknowledge", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "Acknowledge", name: "through_revision", text: "The through revision value carried by acknowledge." },
    FieldDoc { message: "ClientFrame", name: "resume", text: "The resume request carried by client frame." },
    FieldDoc { message: "ClientFrame", name: "command", text: "The command envelope carried by client frame." },
    FieldDoc { message: "ClientFrame", name: "acknowledge", text: "The acknowledge carried by client frame." },
    FieldDoc { message: "ClientFrame", name: "handshake", text: "The handshake request carried by client frame." },
    FieldDoc { message: "ClientFrame", name: "observe", text: "The observe request carried by client frame." },
    FieldDoc { message: "ClientFrame", name: "cancel", text: "The cancel request carried by client frame." },
    FieldDoc { message: "ServerFrame", name: "delivery", text: "The delivery carried by server frame." },
    FieldDoc { message: "ServerFrame", name: "admission", text: "The admission carried by server frame." },
    FieldDoc { message: "ServerFrame", name: "error", text: "The structured error for a failed operation." },
    FieldDoc { message: "ServerFrame", name: "handshake", text: "The handshake response carried by server frame." },
    FieldDoc { message: "ServerFrame", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "ServerFrame", name: "cancellation", text: "The cancel response carried by server frame." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "idempotency_key", text: "The client idempotency key for this mutation." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "canonical_event_json", text: "The canonical event json value carried by scheduler event envelope." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "event_digest", text: "The digest of the canonical scheduler event." },
    FieldDoc { message: "SchedulerEventEnvelope", name: "committed_at_ms", text: "The committed at ms value carried by scheduler event envelope." },
    FieldDoc { message: "ProviderRef", name: "namespace", text: "The namespace value carried by provider ref." },
    FieldDoc { message: "ProviderRef", name: "family", text: "The family value carried by provider ref." },
    FieldDoc { message: "ProviderRef", name: "version", text: "The version or format revision." },
    FieldDoc { message: "VolumeOwner", name: "project", text: "The project value carried by volume owner." },
    FieldDoc { message: "VolumeOwner", name: "agent_id", text: "The agent id value carried by volume owner." },
    FieldDoc { message: "VolumeOwner", name: "session", text: "The session value carried by volume owner." },
    FieldDoc { message: "VolumeRef", name: "provider", text: "The provider identity for this resource." },
    FieldDoc { message: "VolumeRef", name: "id", text: "The id value carried by volume ref." },
    FieldDoc { message: "VolumeRef", name: "volume_class", text: "The volume class carried by volume ref." },
    FieldDoc { message: "VolumeRef", name: "owner", text: "The volume owner carried by volume ref." },
    FieldDoc { message: "FileDescriptor", name: "sha256", text: "The sha256 value carried by file descriptor." },
    FieldDoc { message: "FileDescriptor", name: "byte_length", text: "The byte length value carried by file descriptor." },
    FieldDoc { message: "FileDescriptor", name: "media_type", text: "The media type value carried by file descriptor." },
    FieldDoc { message: "FileRef", name: "volume", text: "The volume containing this file or generation." },
    FieldDoc { message: "FileRef", name: "normalized_path", text: "The normalized path within the volume." },
    FieldDoc { message: "FileRef", name: "immutable_version", text: "The immutable version value carried by file ref." },
    FieldDoc { message: "FileRef", name: "descriptor", text: "The immutable descriptor of this file." },
    FieldDoc { message: "FileRef", name: "display_name", text: "The display name presented to a caller." },
    FieldDoc { message: "TaskOutcome", name: "succeeded", text: "The file ref carried by task outcome." },
    FieldDoc { message: "TaskOutcome", name: "failed_message", text: "The failed message value carried by task outcome." },
    FieldDoc { message: "TaskOutcome", name: "indeterminate_operation_id", text: "The indeterminate operation id value carried by task outcome." },
    FieldDoc { message: "TaskOutcome", name: "cancelled", text: "The cancelled value carried by task outcome." },
    FieldDoc { message: "ExtensionRecord", name: "name", text: "The logical or display name." },
    FieldDoc { message: "ExtensionRecord", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ExtensionRecord", name: "schema_digest", text: "The schema digest value carried by extension record." },
    FieldDoc { message: "ExtensionRecord", name: "implementation_digest", text: "The implementation digest value carried by extension record." },
    FieldDoc { message: "ExtensionRecord", name: "fork_policy", text: "The extension fork policy carried by extension record." },
    FieldDoc { message: "ExtensionRecord", name: "content", text: "The FileRef containing bounded content." },
    FieldDoc { message: "ExtensionStateMigration", name: "previous", text: "The event reference carried by extension state migration." },
    FieldDoc { message: "ExtensionStateMigration", name: "record", text: "The extension record produced by this state migration." },
    FieldDoc { message: "ExtensionDependency", name: "name", text: "The logical or display name." },
    FieldDoc { message: "ExtensionDependency", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ExtensionConfiguration", name: "extension", text: "An extension name and version dependency." },
    FieldDoc { message: "ExtensionConfiguration", name: "schema_digest", text: "The schema digest value carried by extension configuration." },
    FieldDoc { message: "ExtensionConfiguration", name: "content", text: "The FileRef containing bounded content." },
    FieldDoc { message: "ExtensionSelection", name: "previous", text: "The repeated previous values carried by extension selection." },
    FieldDoc { message: "ExtensionSelection", name: "selected", text: "The repeated selected values carried by extension selection." },
    FieldDoc { message: "ExtensionSelection", name: "configurations", text: "Extension configurations selected for admission." },
    FieldDoc { message: "ExtensionConfigured", name: "previous", text: "The extension configuration carried by extension configured." },
    FieldDoc { message: "ExtensionConfigured", name: "record", text: "The extension configuration carried by extension configured." },
    FieldDoc { message: "ExtensionAdmission", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "ExtensionAdmission", name: "selected", text: "The repeated selected values carried by extension admission." },
    FieldDoc { message: "ExtensionAdmission", name: "configurations", text: "Extension configurations selected for admission." },
    FieldDoc { message: "ModelContextSelection", name: "conversation_revision", text: "The conversation revision used for selection." },
    FieldDoc { message: "ModelContextSelection", name: "message_ids", text: "The selected conversation message identifiers." },
    FieldDoc { message: "ApprovalBinding", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "ApprovalBinding", name: "action_digest", text: "The action digest value carried by approval binding." },
    FieldDoc { message: "InteractionTicket", name: "id", text: "The id value carried by interaction ticket." },
    FieldDoc { message: "InteractionTicket", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "InteractionTicket", name: "request", text: "The FileRef containing an interaction request." },
    FieldDoc { message: "InteractionTicket", name: "deadline_unix_ms", text: "The deadline time in Unix units." },
    FieldDoc { message: "InteractionTicket", name: "approval", text: "The approval binding carried by interaction ticket." },
    FieldDoc { message: "InteractionOutcome", name: "answered", text: "The file ref carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "approved", text: "The interaction outcome marker carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "declined", text: "The interaction outcome marker carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "cancelled", text: "The interaction outcome marker carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "expired", text: "The interaction outcome marker carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "denied", text: "The interaction outcome marker carried by interaction outcome." },
    FieldDoc { message: "InteractionOutcome", name: "indeterminate_operation_id", text: "The indeterminate operation id value carried by interaction outcome." },
    FieldDoc { message: "InteractionResolution", name: "id", text: "The id value carried by interaction resolution." },
    FieldDoc { message: "InteractionResolution", name: "expected_version", text: "The expected version value carried by interaction resolution." },
    FieldDoc { message: "InteractionResolution", name: "outcome", text: "The selected operation or interaction outcome." },
    FieldDoc { message: "InteractionResolution", name: "detail", text: "The file ref carried by interaction resolution." },
    FieldDoc { message: "ResolutionReceipt", name: "id", text: "The id value carried by resolution receipt." },
    FieldDoc { message: "ResolutionReceipt", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ResolutionReceipt", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "ResolutionReceipt", name: "conversation_revision", text: "The conversation revision used for selection." },
    FieldDoc { message: "ResolutionReceipt", name: "replayed", text: "The replayed value carried by resolution receipt." },
    FieldDoc { message: "ResolutionReceipt", name: "outcome", text: "The selected operation or interaction outcome." },
    FieldDoc { message: "Attachment", name: "file", text: "The file ref carried by attachment." },
    FieldDoc { message: "Attachment", name: "label", text: "The label value carried by attachment." },
    FieldDoc { message: "AttachmentItems", name: "items", text: "The repeated items values carried by attachment items." },
    FieldDoc { message: "AttachmentManifest", name: "manifest", text: "The file ref carried by attachment manifest." },
    FieldDoc { message: "AttachmentManifest", name: "item_count", text: "The item count value carried by attachment manifest." },
    FieldDoc { message: "ReferencedAttachments", name: "inline_items", text: "The attachment items carried by referenced attachments." },
    FieldDoc { message: "ReferencedAttachments", name: "manifest", text: "The attachment manifest carried by referenced attachments." },
    FieldDoc { message: "ConversationMessage", name: "id", text: "The id value carried by conversation message." },
    FieldDoc { message: "ConversationMessage", name: "sequence", text: "The monotonic conversation sequence." },
    FieldDoc { message: "ConversationMessage", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "ConversationMessage", name: "content", text: "The FileRef containing bounded content." },
    FieldDoc { message: "ConversationMessage", name: "attachments", text: "Referenced conversation attachments." },
    FieldDoc { message: "ConversationMessage", name: "reply_to", text: "The referenced conversation message identifier." },
    FieldDoc { message: "ConversationMessage", name: "tool_call_id", text: "The associated tool call identifier." },
    FieldDoc { message: "ConversationMessage", name: "extensions", text: "Extension records or configured extension state." },
    FieldDoc { message: "ExtensionsEntry", name: "key", text: "The map key for this entry." },
    FieldDoc { message: "ExtensionsEntry", name: "value", text: "The value carried by this entry." },
    FieldDoc { message: "ResourceRef", name: "provider", text: "The provider identity for this resource." },
    FieldDoc { message: "ResourceRef", name: "key", text: "The map key for this entry." },
    FieldDoc { message: "ResourceRef", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ResourceRef", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "ComponentIdentity", name: "name", text: "The logical or display name." },
    FieldDoc { message: "ComponentIdentity", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ComponentIdentity", name: "digest", text: "The digest value carried by component identity." },
    FieldDoc { message: "MachineIdentity", name: "name", text: "The logical or display name." },
    FieldDoc { message: "MachineIdentity", name: "version", text: "The version or format revision." },
    FieldDoc { message: "MachineIdentity", name: "digest", text: "The digest value carried by machine identity." },
    FieldDoc { message: "MachineCheckpoint", name: "machine", text: "The machine component identity." },
    FieldDoc { message: "MachineCheckpoint", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "MachineCheckpoint", name: "canonical_state_json", text: "The canonical bounded JSON state representation." },
    FieldDoc { message: "WorkflowAdmission", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "WorkflowAdmission", name: "request_digest", text: "The request digest value carried by workflow admission." },
    FieldDoc { message: "WorkflowAdmission", name: "initial", text: "The exact initial machine checkpoint." },
    FieldDoc { message: "WorkflowCommand", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "WorkflowCommand", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "WorkflowCommand", name: "payload", text: "The bounded operation payload." },
    FieldDoc { message: "WorkflowTransition", name: "canonical_state_json", text: "The canonical bounded JSON state representation." },
    FieldDoc { message: "WorkflowTransition", name: "commands", text: "Commands included in this transition." },
    FieldDoc { message: "WorkflowTransition", name: "suspended", text: "Whether the workflow is suspended." },
    FieldDoc { message: "WorkflowTransition", name: "canonical_completed_value_json", text: "The canonical completed value json value carried by workflow transition." },
    FieldDoc { message: "WorkflowTransition", name: "failure_message", text: "The stable failure description." },
    FieldDoc { message: "WorkflowRecord", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "WorkflowRecord", name: "idempotency_key", text: "The client idempotency key for this mutation." },
    FieldDoc { message: "WorkflowRecord", name: "input_digest", text: "The input digest value carried by workflow record." },
    FieldDoc { message: "WorkflowRecord", name: "prior", text: "The prior machine checkpoint." },
    FieldDoc { message: "WorkflowRecord", name: "canonical_input_json", text: "The canonical bounded JSON task input." },
    FieldDoc { message: "WorkflowRecord", name: "transition", text: "The canonical workflow transition." },
    FieldDoc { message: "WorkflowRecord", name: "next", text: "The cursor after this page." },
    FieldDoc { message: "RuntimeLimits", name: "file_bytes", text: "The file bytes value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "path_bytes", text: "The path bytes value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "attachments", text: "Referenced conversation attachments." },
    FieldDoc { message: "RuntimeLimits", name: "render_bytes", text: "The render bytes value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "model_steps", text: "The model steps value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "model_events_per_step", text: "The model events per step value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "tool_calls_per_step", text: "The tool calls per step value carried by runtime limits." },
    FieldDoc { message: "RuntimeLimits", name: "context_messages", text: "The context messages value carried by runtime limits." },
    FieldDoc { message: "TaskRunLimits", name: "concurrency", text: "The concurrency value carried by task run limits." },
    FieldDoc { message: "TaskRunLimits", name: "max_steps", text: "The max steps value carried by task run limits." },
    FieldDoc { message: "TaskRunLimits", name: "deadline_epoch_ms", text: "The deadline epoch ms value carried by task run limits." },
    FieldDoc { message: "ExecutionPlacement", name: "provider", text: "The provider identity for this resource." },
    FieldDoc { message: "ExecutionPlacement", name: "build", text: "The resource ref carried by execution placement." },
    FieldDoc { message: "ExecutionPlacement", name: "environment", text: "The resource ref carried by execution placement." },
    FieldDoc { message: "ExecutionPlacement", name: "readiness_revision", text: "The readiness revision value carried by execution placement." },
    FieldDoc { message: "TaskAdmissionRecord", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "TaskAdmissionRecord", name: "task", text: "The task component identity." },
    FieldDoc { message: "TaskAdmissionRecord", name: "machine", text: "The machine component identity." },
    FieldDoc { message: "TaskAdmissionRecord", name: "canonical_input_json", text: "The canonical bounded JSON task input." },
    FieldDoc { message: "TaskAdmissionRecord", name: "canonical_input_schema_json", text: "The canonical bounded input schema." },
    FieldDoc { message: "TaskAdmissionRecord", name: "canonical_output_schema_json", text: "The canonical bounded output schema." },
    FieldDoc { message: "TaskAdmissionRecord", name: "parent_task_id", text: "The parent task id value carried by task admission record." },
    FieldDoc { message: "TaskAdmissionRecord", name: "grants", text: "Capabilities granted to this task." },
    FieldDoc { message: "TaskAdmissionRecord", name: "limits", text: "The execution limits applied to this task." },
    FieldDoc { message: "TaskAdmissionRecord", name: "policy", text: "The policy component identity." },
    FieldDoc { message: "TaskAdmissionRecord", name: "extensions", text: "Extension records or configured extension state." },
    FieldDoc { message: "TaskAdmissionRecord", name: "execution", text: "The execution placement selected for this task." },
    FieldDoc { message: "TaskAdmissionRecord", name: "run_limits", text: "The task run limits." },
    FieldDoc { message: "DurableBatchRequest", name: "group_id", text: "The group id value carried by durable batch request." },
    FieldDoc { message: "DurableBatchRequest", name: "batch_id", text: "The batch id value carried by durable batch request." },
    FieldDoc { message: "DurableBatchRequest", name: "group_policy", text: "The batch group policy carried by durable batch request." },
    FieldDoc { message: "DurableBatchRequest", name: "task", text: "The task component identity." },
    FieldDoc { message: "DurableBatchRequest", name: "machine", text: "The machine component identity." },
    FieldDoc { message: "DurableBatchRequest", name: "canonical_input_json", text: "The canonical bounded JSON task input." },
    FieldDoc { message: "DurableBatchRequest", name: "canonical_input_schema_json", text: "The canonical bounded input schema." },
    FieldDoc { message: "DurableBatchRequest", name: "canonical_output_schema_json", text: "The canonical bounded output schema." },
    FieldDoc { message: "DurableBatchRequest", name: "parent_task_id", text: "The parent task id value carried by durable batch request." },
    FieldDoc { message: "DurableBatchRequest", name: "grants", text: "Capabilities granted to this task." },
    FieldDoc { message: "DurableBatchRequest", name: "limits", text: "The execution limits applied to this task." },
    FieldDoc { message: "DurableBatchRequest", name: "extensions", text: "Extension records or configured extension state." },
    FieldDoc { message: "DurableBatchRequest", name: "policy", text: "The policy component identity." },
    FieldDoc { message: "DurableBatchRequest", name: "execution", text: "The execution placement selected for this task." },
    FieldDoc { message: "DurableBatchRequest", name: "run_limits", text: "The task run limits." },
    FieldDoc { message: "GenerationRef", name: "resource", text: "The resource ref carried by generation ref." },
    FieldDoc { message: "PrivateDirectoryEntry", name: "name", text: "The logical or display name." },
    FieldDoc { message: "PrivateDirectoryEntry", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "PrivateDirectoryPage", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "PrivateDirectoryPage", name: "entries", text: "The entries in this bounded page." },
    FieldDoc { message: "PrivateDirectoryPage", name: "has_more", text: "Whether the file has more." },
    FieldDoc { message: "ProjectRevision", name: "volume", text: "The volume containing this file or generation." },
    FieldDoc { message: "ProjectRevision", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ExtensionRevision", name: "name", text: "The logical or display name." },
    FieldDoc { message: "ExtensionRevision", name: "version", text: "The version or format revision." },
    FieldDoc { message: "ExtensionRevision", name: "reference", text: "The resource ref carried by extension revision." },
    FieldDoc { message: "ExtensionRevision", name: "implementation_digest", text: "The implementation digest value carried by extension revision." },
    FieldDoc { message: "ResourceRevision", name: "history", text: "The resource ref carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "project", text: "The project revision carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "context", text: "The resource ref carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "process", text: "The resource ref carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "artifact", text: "The resource ref carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "shared_volume", text: "The volume ref carried by resource revision." },
    FieldDoc { message: "ResourceRevision", name: "extension", text: "An extension name and version dependency." },
    FieldDoc { message: "CapturedResource", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "CapturedResource", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "ForkOmission", name: "selection", text: "The resource revision selected for capture." },
    FieldDoc { message: "ForkOmission", name: "unsupported_reason", text: "The unsupported reason value carried by fork omission." },
    FieldDoc { message: "ForkOmission", name: "in_flight_operation_id", text: "The in flight operation id value carried by fork omission." },
    FieldDoc { message: "ForkOmission", name: "indeterminate_operation_id", text: "The indeterminate operation id value carried by fork omission." },
    FieldDoc { message: "AttestedBoundary", name: "provider", text: "The provider identity for this resource." },
    FieldDoc { message: "AttestedBoundary", name: "evidence", text: "The evidence value carried by attested boundary." },
    FieldDoc { message: "SharedGrant", name: "volume", text: "The volume containing this file or generation." },
    FieldDoc { message: "SharedGrant", name: "child_agent_id", text: "The child agent id value carried by shared grant." },
    FieldDoc { message: "SharedGrant", name: "operations", text: "The repeated operations values carried by shared grant." },
    FieldDoc { message: "ReferenceGrant", name: "file", text: "The file ref carried by reference grant." },
    FieldDoc { message: "ReferenceGrant", name: "reader_agent_id", text: "The reader agent id value carried by reference grant." },
    FieldDoc { message: "ReferenceGrant", name: "attachment_manifest", text: "The file ref carried by reference grant." },
    FieldDoc { message: "ForkSelection", name: "required", text: "The required value carried by fork selection." },
    FieldDoc { message: "ForkSelection", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "ForkPreparation", name: "child_project_volume", text: "The volume ref carried by fork preparation." },
    FieldDoc { message: "ForkPreparation", name: "child_private_volume", text: "The volume ref carried by fork preparation." },
    FieldDoc { message: "ForkPreparation", name: "inherited_through_sequence", text: "The inherited through sequence value carried by fork preparation." },
    FieldDoc { message: "ForkPreparation", name: "maximum_inherited_messages", text: "The maximum inherited messages permitted or requested by this operation." },
    FieldDoc { message: "ForkPreparation", name: "maximum_inherited_bytes", text: "The maximum inherited bytes permitted or requested by this operation." },
    FieldDoc { message: "ForkPreparation", name: "maximum_inherited_references", text: "The maximum inherited references permitted or requested by this operation." },
    FieldDoc { message: "ForkRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "ForkRequest", name: "parent", text: "The parent authority or resource." },
    FieldDoc { message: "ForkRequest", name: "parent_revision", text: "The parent revision selected for this operation." },
    FieldDoc { message: "ForkRequest", name: "child", text: "The child authority or resource." },
    FieldDoc { message: "ForkRequest", name: "child_agent_id", text: "The child agent id value carried by fork request." },
    FieldDoc { message: "ForkRequest", name: "selections", text: "The repeated selections values carried by fork request." },
    FieldDoc { message: "ForkRequest", name: "boundary", text: "The attested fork boundary." },
    FieldDoc { message: "ForkRequest", name: "attached_agent_ids", text: "The repeated attached agent ids values carried by fork request." },
    FieldDoc { message: "ForkRequest", name: "preparation", text: "The fork preparation carried by fork request." },
    FieldDoc { message: "Capture", name: "captured", text: "The captured resource carried by capture." },
    FieldDoc { message: "Capture", name: "unsupported_reason", text: "The unsupported reason value carried by capture." },
    FieldDoc { message: "Capture", name: "in_flight_operation_id", text: "The in flight operation id value carried by capture." },
    FieldDoc { message: "Capture", name: "indeterminate_operation_id", text: "The indeterminate operation id value carried by capture." },
    FieldDoc { message: "ForkReport", name: "request", text: "The FileRef containing an interaction request." },
    FieldDoc { message: "ForkReport", name: "captures", text: "The repeated captures values carried by fork report." },
    FieldDoc { message: "ForkReport", name: "child_private_volume", text: "The volume ref carried by fork report." },
    FieldDoc { message: "ForkReport", name: "inherited_context", text: "Inherited context FileRefs." },
    FieldDoc { message: "ForkReport", name: "shared_grants", text: "Shared volume grants created for the child." },
    FieldDoc { message: "ForkReport", name: "reference_grants", text: "Reference grants created for the child." },
    FieldDoc { message: "ForkReport", name: "attachment_manifests", text: "Attachment manifests created for the child." },
    FieldDoc { message: "ForkReport", name: "inherited_through_sequence", text: "The inherited through sequence value carried by fork report." },
    FieldDoc { message: "ForkReport", name: "child_private_generation", text: "The generation ref carried by fork report." },
    FieldDoc { message: "ForkSeed", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "ForkSeed", name: "parent", text: "The parent authority or resource." },
    FieldDoc { message: "ForkSeed", name: "parent_revision", text: "The parent revision selected for this operation." },
    FieldDoc { message: "ForkSeed", name: "child", text: "The child authority or resource." },
    FieldDoc { message: "ForkSeed", name: "child_agent_id", text: "The child agent id value carried by fork seed." },
    FieldDoc { message: "ForkSeed", name: "resources", text: "The captured resources in this fork." },
    FieldDoc { message: "ForkSeed", name: "omissions", text: "Resources omitted from this fork with reasons." },
    FieldDoc { message: "ForkSeed", name: "child_private_volume", text: "The volume ref carried by fork seed." },
    FieldDoc { message: "ForkSeed", name: "inherited_context", text: "Inherited context FileRefs." },
    FieldDoc { message: "ForkSeed", name: "boundary", text: "The attested fork boundary." },
    FieldDoc { message: "ForkSeed", name: "shared_grants", text: "Shared volume grants created for the child." },
    FieldDoc { message: "ForkSeed", name: "attached_agent_ids", text: "The repeated attached agent ids values carried by fork seed." },
    FieldDoc { message: "ForkSeed", name: "reference_grants", text: "Reference grants created for the child." },
    FieldDoc { message: "ForkSeed", name: "attachment_manifests", text: "Attachment manifests created for the child." },
    FieldDoc { message: "ForkSeed", name: "inherited_through_sequence", text: "The inherited through sequence value carried by fork seed." },
    FieldDoc { message: "ForkSeed", name: "child_private_generation", text: "The generation ref carried by fork seed." },
    FieldDoc { message: "ProviderJoinProof", name: "provider", text: "The provider identity for this resource." },
    FieldDoc { message: "ProviderJoinProof", name: "format", text: "The proof format identifier." },
    FieldDoc { message: "ProviderJoinProof", name: "canonical_json_statement", text: "The canonical provider statement." },
    FieldDoc { message: "ProjectMergeReceipt", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "ProjectMergeReceipt", name: "child", text: "The child authority or resource." },
    FieldDoc { message: "ProjectMergeReceipt", name: "source_project", text: "The volume ref carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "source_generation", text: "The generation ref carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "target_project", text: "The volume ref carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "expected_target_generation", text: "The generation ref carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "result_generation", text: "The generation ref carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "provider_operation_id", text: "The provider operation id value carried by project merge receipt." },
    FieldDoc { message: "ProjectMergeReceipt", name: "provider_proof", text: "The provider proof for an atomic join." },
    FieldDoc { message: "ProjectMergeReceipt", name: "notice", text: "The admitted merge notice." },
];

#[rustfmt::skip]
pub const HARNESS_ENUM_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "ErrorCode", text: "The error code values defined by the Harness contract." },
    ContractDoc { name: "AdmissionState", text: "The admission state values defined by the Harness contract." },
    ContractDoc { name: "CompletionState", text: "The completion state values defined by the Harness contract." },
    ContractDoc { name: "AggregateKind", text: "The aggregate kind values defined by the Harness contract." },
    ContractDoc { name: "ApplyState", text: "The apply state values defined by the Harness contract." },
    ContractDoc { name: "VolumeClass", text: "The volume class values defined by the Harness contract." },
    ContractDoc { name: "ExtensionForkPolicy", text: "The extension fork policy values defined by the Harness contract." },
    ContractDoc { name: "InteractionKind", text: "The interaction kind values defined by the Harness contract." },
    ContractDoc { name: "ConversationKind", text: "The conversation kind values defined by the Harness contract." },
    ContractDoc { name: "ResourceKind", text: "The resource kind values defined by the Harness contract." },
    ContractDoc { name: "BatchGroupPolicy", text: "The batch group policy values defined by the Harness contract." },
    ContractDoc { name: "SharedVolumeOperation", text: "The shared volume operation values defined by the Harness contract." },
];

pub const HARNESS_SERVICE_DOC: ContractDoc = ContractDoc {
    name: "HarnessService",
    text: "Admits, replays, observes, and cancels durable Rust-owned operations.",
};

#[rustfmt::skip]
pub const HARNESS_SERVICE_DOCS: &[ContractDoc] = &[HARNESS_SERVICE_DOC];

pub fn harness_service_doc(name: &str) -> Option<&'static str> {
    HARNESS_SERVICE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
}

#[rustfmt::skip]

pub const HARNESS_METHOD_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "Handshake", text: "Negotiates protocol identity." },
    ContractDoc { name: "Submit", text: "Admits a canonical command before effects occur." },
    ContractDoc { name: "Replay", text: "Replays authoritative events from resumable cursors." },
    ContractDoc { name: "Observe", text: "Observes an admitted operation." },
    ContractDoc { name: "Cancel", text: "Requests cancellation of an admitted operation." },
];

pub fn harness_message_doc(name: &str) -> Option<&'static str> {
    HARNESS_MESSAGE_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}
pub fn harness_field_doc(message: &str, name: &str) -> Option<&'static str> {
    HARNESS_FIELD_DOCS
        .iter()
        .find(|d| d.message == message && d.name == name)
        .map(|d| d.text)
}
pub fn harness_enum_doc(name: &str) -> Option<&'static str> {
    HARNESS_ENUM_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}
pub fn harness_method_doc(name: &str) -> Option<&'static str> {
    HARNESS_METHOD_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}

const ARCHIVED_DESCRIPTOR: &[u8] = include_bytes!("../tests/fixtures/harness-v2.descriptor.bin");
pub fn harness_file_descriptor() -> FileDescriptorProto {
    target_file()
}
pub fn harness_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![protocol::protocol_file_descriptor(), target_file()],
    }
    .encode_to_vec()
}
pub fn harness_proto() -> String {
    render_proto(&target_file())
}
pub fn descriptor() -> Vec<u8> {
    harness_descriptor()
}
pub fn file_descriptor() -> FileDescriptorProto {
    harness_file_descriptor()
}
impl ContractModel {
    pub fn file_descriptor(&self) -> FileDescriptorProto {
        harness_file_descriptor()
    }
    pub fn descriptor(&self) -> Vec<u8> {
        harness_descriptor()
    }
    pub fn canonical_sha256(&self) -> String {
        let digest = Sha256::digest(&self.descriptor());
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }
}
fn target_file() -> FileDescriptorProto {
    let set = FileDescriptorSet::decode(ARCHIVED_DESCRIPTOR).expect("archived Harness descriptor");
    let mut file = set
        .file
        .into_iter()
        .find(|f| f.name.as_deref() == Some(FILE_NAME))
        .expect("Harness file in fixture");
    file.source_code_info = None;
    file
}

fn render_proto(file: &FileDescriptorProto) -> String {
    let mut out = format!(
        "syntax = \"{}\";\npackage {};\n\n",
        file.syntax.as_deref().unwrap_or(SYNTAX),
        file.package.as_deref().unwrap_or(PACKAGE)
    );
    for dependency in &file.dependency {
        out.push_str(&format!("import \"{dependency}\";\n"));
    }
    if let Some(go) = file.options.as_ref().and_then(|o| o.go_package.as_deref()) {
        out.push_str(&format!("option go_package = \"{go}\";\n"));
    }
    out.push('\n');
    for e in &file.enum_type {
        render_enum(&mut out, e, 0);
    }
    for m in &file.message_type {
        render_message(&mut out, m, 0);
    }
    for service in &file.service {
        out.push_str(&format!(
            "service {} {{\n",
            service.name.as_deref().unwrap_or_default()
        ));
        for method in &service.method {
            out.push_str(&format!(
                "  rpc {}({}{}) returns ({}{});\n",
                method.name.as_deref().unwrap_or_default(),
                if method.client_streaming.unwrap_or(false) {
                    "stream "
                } else {
                    ""
                },
                method
                    .input_type
                    .as_deref()
                    .unwrap_or_default()
                    .trim_start_matches('.'),
                if method.server_streaming.unwrap_or(false) {
                    "stream "
                } else {
                    ""
                },
                method
                    .output_type
                    .as_deref()
                    .unwrap_or_default()
                    .trim_start_matches('.')
            ));
        }
        out.push_str("}\n\n");
    }
    out
}
fn render_enum(out: &mut String, e: &prost_types::EnumDescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    out.push_str(&format!(
        "{p}enum {} {{\n",
        e.name.as_deref().unwrap_or_default()
    ));
    for v in &e.value {
        out.push_str(&format!(
            "{p}  {} = {};\n",
            v.name.as_deref().unwrap_or_default(),
            v.number.unwrap_or_default()
        ));
    }
    out.push_str(&format!("{p}}}\n\n"));
}
fn render_message(out: &mut String, m: &DescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    out.push_str(&format!(
        "{p}message {} {{\n",
        m.name.as_deref().unwrap_or_default()
    ));
    for (i, o) in m.oneof_decl.iter().enumerate() {
        if m.field
            .iter()
            .any(|f| f.oneof_index == Some(i as i32) && !f.proto3_optional.unwrap_or(false))
        {
            out.push_str(&format!(
                "{p}  oneof {} {{\n",
                o.name.as_deref().unwrap_or_default()
            ));
            for f in &m.field {
                if f.oneof_index == Some(i as i32) && !f.proto3_optional.unwrap_or(false) {
                    render_field(out, f, indent + 4);
                }
            }
            out.push_str(&format!("{p}  }}\n"));
        }
    }
    for f in &m.field {
        if f.oneof_index.is_none() {
            if let Some(entry) = map_entry_for(m, f) {
                render_map_field(out, f, entry, indent + 2);
            } else {
                render_field(out, f, indent + 2);
            }
        }
    }
    for n in &m.nested_type {
        if n.options.as_ref().and_then(|o| o.map_entry) != Some(true) {
            render_message(out, n, indent + 2);
        }
    }
    for e in &m.enum_type {
        render_enum(out, e, indent + 2);
    }
    for r in &m.reserved_range {
        out.push_str(&format!(
            "{p}  reserved {} to {};\n",
            r.start.unwrap_or_default(),
            r.end.unwrap_or_default() - 1
        ));
    }
    for n in &m.reserved_name {
        out.push_str(&format!("{p}  reserved \"{n}\";\n"));
    }
    out.push_str(&format!("{p}}}\n\n"));
}
fn render_field(out: &mut String, f: &prost_types::FieldDescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    let label = match f
        .label
        .and_then(|v| prost_types::field_descriptor_proto::Label::try_from(v).ok())
    {
        Some(prost_types::field_descriptor_proto::Label::Repeated) => "repeated ",
        Some(prost_types::field_descriptor_proto::Label::Required) => "required ",
        Some(prost_types::field_descriptor_proto::Label::Optional)
            if f.proto3_optional.unwrap_or(false) =>
        {
            "optional "
        }
        _ => "",
    };
    let ty = f
        .type_name
        .as_deref()
        .map(|x| x.trim_start_matches('.').to_owned())
        .unwrap_or_else(|| {
            format!(
                "{:?}",
                f.r#type
                    .and_then(|v| prost_types::field_descriptor_proto::Type::try_from(v).ok())
                    .unwrap_or(prost_types::field_descriptor_proto::Type::Bytes)
            )
            .to_lowercase()
        });
    out.push_str(&format!(
        "{p}{label}{ty} {} = {};\n",
        f.name.as_deref().unwrap_or_default(),
        f.number.unwrap_or_default()
    ));
}

fn field_type_token(field: &prost_types::FieldDescriptorProto) -> String {
    field
        .type_name
        .as_deref()
        .map(|name| name.trim_start_matches('.').to_owned())
        .unwrap_or_else(|| {
            format!(
                "{:?}",
                field
                    .r#type
                    .and_then(
                        |value| prost_types::field_descriptor_proto::Type::try_from(value).ok()
                    )
                    .unwrap_or(prost_types::field_descriptor_proto::Type::Bytes)
            )
            .to_lowercase()
        })
}

fn map_entry_for<'a>(
    message: &'a DescriptorProto,
    field: &prost_types::FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    let name = field.type_name.as_deref()?.rsplit('.').next()?;
    message.nested_type.iter().find(|nested| {
        nested.name.as_deref() == Some(name)
            && nested
                .options
                .as_ref()
                .and_then(|options| options.map_entry)
                == Some(true)
    })
}

fn render_map_field(
    out: &mut String,
    field: &prost_types::FieldDescriptorProto,
    entry: &DescriptorProto,
    indent: usize,
) {
    let key = entry
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("key"))
        .map(field_type_token)
        .unwrap_or_else(|| "string".to_owned());
    let value = entry
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("value"))
        .map(field_type_token)
        .unwrap_or_else(|| "bytes".to_owned());
    out.push_str(&format!(
        "{}map<{}, {}> {} = {};\n",
        " ".repeat(indent),
        key,
        value,
        field.name.as_deref().unwrap_or_default(),
        field.number.unwrap_or_default()
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_inventory_and_handshake() {
        let f = harness_file_descriptor();
        assert_eq!(f.name.as_deref(), Some(FILE_NAME));
        assert_eq!(f.package.as_deref(), Some(PACKAGE));
        assert_eq!(f.service.len(), 1);
        assert_eq!(VALIDATION_OPTION_SPECS.len(), 12);
        assert_eq!(ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST.len(), 71);
    }
    #[test]
    fn source_docs_cover_every_declared_item() {
        fn visit(message: &DescriptorProto) {
            let name = message.name.as_deref().unwrap_or_default();
            assert!(
                harness_message_doc(name).is_some(),
                "missing HARNESS message docs for {name}"
            );
            for field in &message.field {
                assert!(
                    harness_field_doc(name, field.name.as_deref().unwrap_or_default()).is_some(),
                    "missing HARNESS field docs for {name}.{}",
                    field.name.as_deref().unwrap_or_default()
                );
            }
            for nested in &message.nested_type {
                visit(nested);
            }
        }
        let file = harness_file_descriptor();
        for message in &file.message_type {
            visit(message);
        }
        for enumeration in &file.enum_type {
            assert!(harness_enum_doc(enumeration.name.as_deref().unwrap_or_default()).is_some());
        }
        assert_eq!(HARNESS_SERVICE_DOC.name, "HarnessService");
        for method in &file.service[0].method {
            assert!(harness_method_doc(method.name.as_deref().unwrap_or_default()).is_some());
        }
    }
    #[test]
    fn preserves_presence_maps_and_streams() {
        let f = harness_file_descriptor();
        assert!(f.message_type.iter().any(|m| {
            m.oneof_decl
                .iter()
                .any(|o| o.name.as_deref().unwrap_or_default().starts_with('_'))
        }));
        assert!(
            f.message_type
                .iter()
                .flat_map(|m| m.nested_type.iter())
                .any(|m| m.options.as_ref().and_then(|o| o.map_entry) == Some(true))
        );
        assert!(f.service[0]
            .method
            .iter()
            .any(|m| m.client_streaming.unwrap_or(false) || m.server_streaming.unwrap_or(false)));
    }
}
