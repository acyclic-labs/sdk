//! Rust-owned Filesystem v2 wire-contract surface.
//!
//! The descriptor fixture is an immutable migration oracle.  The public model
//! keeps the complete descriptor surface available to Rust exporters: maps,
//! nested types, presence markers, enum values, reserved identities, and RPC
//! streaming flags are never reconstructed from generated bindings.

use crate::{OperationPolicy, protocol};
pub use acyclic_sdk_contract_options::OptionTarget;
use acyclic_sdk_contract_options::{OPTION_SPECS, OptionSpec};
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
use sha2::{Digest, Sha256};

pub const FILE_NAME: &str = "filesystem/v2/filesystem.proto";
pub const PACKAGE: &str = "acyclic.filesystem.v2";
pub const SYNTAX: &str = "proto3";
pub const GO_PACKAGE: &str = "github.com/acyclic-labs/sdk/go/gen/filesystem/v2;filesystemv2";
pub const HANDSHAKE_VERSION: &str = "1";
/// Digest used by the deployed Filesystem handshake fixture.
pub const ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST: &str =
    "blake3:ece4a6bb58779d216707a426a0ebc5375b5a7a99b14178ce2f9d4f9dd781df60";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractModel {
    pub file_name: &'static str,
    pub package: &'static str,
    pub syntax: &'static str,
    pub handshake_version: &'static str,
    pub archived_handshake_descriptor_digest: &'static str,
}

pub const FILESYSTEM: ContractModel = ContractModel {
    file_name: FILE_NAME,
    package: PACKAGE,
    syntax: SYNTAX,
    handshake_version: HANDSHAKE_VERSION,
    archived_handshake_descriptor_digest: ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST,
};
pub const CONTRACT: ContractModel = FILESYSTEM;

/// Availability metadata for one Rust-owned Filesystem service surface.
///
/// These entries describe compiled Rust adapters and feature/target gates;
/// they do not imply an HTTP route or a deployed endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceAvailability {
    pub name: &'static str,
    pub feature: &'static str,
    pub targets: &'static str,
    pub transport: &'static str,
}

pub const FILESYSTEM_SERVICE_AVAILABILITY: &[ServiceAvailability] = &[
    ServiceAvailability {
        name: "portable-model",
        feature: "always",
        targets: "native and target-compatible wasm32",
        transport: "in-process Rust",
    },
    ServiceAvailability {
        name: "embedded-local",
        feature: "local (default; native only)",
        targets: "native",
        transport: "in-process Rust",
    },
    ServiceAvailability {
        name: "in-memory-distributed",
        feature: "memory + distributed",
        targets: "feature-enabled targets",
        transport: "in-process Rust",
    },
    ServiceAvailability {
        name: "hosted-client",
        feature: "native target",
        targets: "native",
        transport: "transport-neutral hosted client",
    },
    ServiceAvailability {
        name: "grpc-wire-service",
        feature: "native build",
        targets: "native",
        transport: "tonic gRPC",
    },
    ServiceAvailability {
        name: "native-watch",
        feature: "native-watch",
        targets: "native",
        transport: "in-process Rust",
    },
    ServiceAvailability {
        name: "native-mount",
        feature: "native-mount",
        targets: "native",
        transport: "native mount adapter",
    },
    ServiceAvailability {
        name: "s3-http",
        feature: "s3-http",
        targets: "native",
        transport: "S3-compatible HTTP adapter",
    },
];

const FILESYSTEM_ERRORS: &[&str] = &[
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "FAILED_PRECONDITION",
    "CANCELLED",
    "RESOURCE_EXHAUSTED",
    "UNAVAILABLE",
    "UNIMPLEMENTED",
    "DATA_LOSS",
];
const FILESYSTEM_READ_CAPABILITIES: &[&str] = &["filesystem.read"];
const FILESYSTEM_WRITE_CAPABILITIES: &[&str] =
    &["filesystem.write", "filesystem.idempotent_mutation"];
const FILESYSTEM_OPERATION_CAPABILITIES: &[&str] = &["filesystem.operation"];
const FILESYSTEM_MOUNT_CAPABILITIES: &[&str] = &["filesystem.credentials.mount"];
const FILESYSTEM_S3_CAPABILITIES: &[&str] = &["filesystem.credentials.s3"];
const FILESYSTEM_SOURCE_CAPABILITIES: &[&str] = &["filesystem.source"];

/// Rust-owned behavior metadata for every FilesystemService RPC.
#[rustfmt::skip]
pub const FILESYSTEM_OPERATION_POLICIES: &[OperationPolicy] = &[
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Handshake", capabilities: &["filesystem"], errors: FILESYSTEM_ERRORS, validations: &["protocol.version.exact", "descriptor_digest.matches", "request.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/CreateWorkspace", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["operation.idempotency_key.16_bytes", "workspace_name.valid", "profile.supported"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/OpenWorkspace", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.selector.required", "workspace.identity.matches"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/DeleteWorkspace", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/GetHead", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "workspace.identity.matches"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/GetGeneration", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "generation_id.32_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Read", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "path.valid", "maximum_bytes.bounded", "range.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Stat", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "path.valid"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ListDirectory", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "page.maximum_items.bounded", "cursor.valid"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ReadLink", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "path.valid", "maximum_bytes.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/PlanExtents", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "path.valid", "range.valid"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ApplyTransaction", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "operation.idempotency_key.16_bytes", "mutation.oneof", "transaction.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/RebaseTransaction", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "operation.idempotency_key.16_bytes", "transaction.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ForkWorkspace", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "generation.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Diff", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.references.required", "diff.bounds.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Rebase", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "generation.reference.required", "rebase.bounds.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/PlanJoin", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.references.required", "join.history.valid", "join.bounds.bounded"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ApplyJoin", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["join.plan_identity.matches", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Checkpoint", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Pin", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Export", capabilities: FILESYSTEM_READ_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["generation.reference.required", "export.bounds.bounded", "cursor.valid"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Import", capabilities: FILESYSTEM_WRITE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["import.stream.nonempty", "object.order.exact", "cursor.valid", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/IssueMountCredential", capabilities: FILESYSTEM_MOUNT_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "generation.reference.required", "credential.expiry.bounded", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/IssueS3Credential", capabilities: FILESYSTEM_S3_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "generation.reference.required", "credential.expiry.bounded", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/GetSourceState", capabilities: FILESYSTEM_SOURCE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/ReconcileSource", capabilities: FILESYSTEM_SOURCE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/RescanSource", capabilities: FILESYSTEM_SOURCE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/SealSource", capabilities: FILESYSTEM_SOURCE_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["workspace.reference.required", "operation.idempotency_key.16_bytes"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Observe", capabilities: FILESYSTEM_OPERATION_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["operation_id.16_bytes", "response.identity.matches"] },
    OperationPolicy { rpc: "acyclic.filesystem.v2.FilesystemService/Cancel", capabilities: FILESYSTEM_OPERATION_CAPABILITIES, errors: FILESYSTEM_ERRORS, validations: &["operation_id.16_bytes", "operation.idempotency_key.16_bytes", "response.identity.matches"] },
];

/// Compatibility aliases retained for Filesystem/Harness callers while the
/// option identity itself is owned by the shared immutable oracle.
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
pub const FILESYSTEM_MESSAGE_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "WorkspaceRef", text: "Identifies a Filesystem workspace." },
    ContractDoc { name: "GenerationRef", text: "Identifies an immutable Filesystem generation." },
    ContractDoc { name: "WorkspaceContextRoot", text: "Durable context control-plane records. Paths are UTF-8 native absolute paths; file contents and credentials never travel in these records. The repeated roots are ordered by root_id, and duplicate IDs are invalid." },
    ContractDoc { name: "WorkspaceContextRoots", text: "The ordered workspace roots in a durable context snapshot." },
    ContractDoc { name: "WorkspaceContextSnapshot", text: "An immutable workspace context snapshot." },
    ContractDoc { name: "WorkspaceContextDiscard", text: "Discards selected workspace contexts." },
    ContractDoc { name: "OperationOptions", text: "Idempotency controls for a Filesystem mutation." },
    ContractDoc { name: "PageOptions", text: "Bounds and cursor for a Filesystem page." },
    ContractDoc { name: "ByteRange", text: "An offset and length within a file." },
    ContractDoc { name: "OptionalU32", text: "A uint32 value with explicit unavailable or present state." },
    ContractDoc { name: "OptionalU64", text: "A uint64 value with explicit unavailable or present state." },
    ContractDoc { name: "OptionalI64", text: "An int64 value with explicit unavailable or present state." },
    ContractDoc { name: "Metadata", text: "Portable and provider-specific file metadata." },
    ContractDoc { name: "FileStat", text: "The kind, identity, size, and metadata of a file." },
    ContractDoc { name: "DirectoryEntry", text: "A named directory entry and its stat." },
    ContractDoc { name: "DirectoryPage", text: "A bounded page of directory entries." },
    ContractDoc { name: "Extent", text: "A byte range classified as a hole, allocated zero, or content." },
    ContractDoc { name: "Capabilities", text: "Filesystem limits, profiles, and optional provider capabilities." },
    ContractDoc { name: "SourceStateRequest", text: "Carries the inputs for the source state operation." },
    ContractDoc { name: "SourceOperationRequest", text: "Carries the inputs for the source operation operation." },
    ContractDoc { name: "SourceResponse", text: "Carries the result of the source operation." },
    ContractDoc { name: "Workspace", text: "A named Filesystem workspace and its current head." },
    ContractDoc { name: "HandshakeRequest", text: "Carries the inputs for the handshake operation." },
    ContractDoc { name: "HandshakeResponse", text: "Carries the result of the handshake operation." },
    ContractDoc { name: "CreateWorkspaceRequest", text: "Carries the inputs for the create workspace operation." },
    ContractDoc { name: "OpenWorkspaceRequest", text: "Carries the inputs for the open workspace operation." },
    ContractDoc { name: "WorkspaceResponse", text: "Carries the result of the workspace operation." },
    ContractDoc { name: "DeleteWorkspaceRequest", text: "Carries the inputs for the delete workspace operation." },
    ContractDoc { name: "MutationResponse", text: "The committed generation, observed head, and conflicts for a mutation." },
    ContractDoc { name: "GetHeadRequest", text: "Carries the inputs for the get head operation." },
    ContractDoc { name: "GetGenerationRequest", text: "Carries the inputs for the get generation operation." },
    ContractDoc { name: "GenerationResponse", text: "Carries the result of the generation operation." },
    ContractDoc { name: "ReadRequest", text: "Carries the inputs for the read operation." },
    ContractDoc { name: "ReadResponse", text: "Carries the result of the read operation." },
    ContractDoc { name: "StatRequest", text: "Carries the inputs for the stat operation." },
    ContractDoc { name: "StatResponse", text: "Carries the result of the stat operation." },
    ContractDoc { name: "ListDirectoryRequest", text: "Carries the inputs for the list directory operation." },
    ContractDoc { name: "ListDirectoryResponse", text: "Carries the result of the list directory operation." },
    ContractDoc { name: "ReadLinkRequest", text: "Carries the inputs for the read link operation." },
    ContractDoc { name: "PlanExtentsRequest", text: "Carries the inputs for the plan extents operation." },
    ContractDoc { name: "PlanExtentsResponse", text: "Carries the result of the plan extents operation." },
    ContractDoc { name: "CreateFile", text: "A mutation payload that creates one regular file." },
    ContractDoc { name: "CreateDirectory", text: "A mutation payload that creates one directory." },
    ContractDoc { name: "CreateSymbolicLink", text: "A mutation payload that creates one symbolic link." },
    ContractDoc { name: "Remove", text: "A mutation payload that removes one path." },
    ContractDoc { name: "Rename", text: "A mutation payload that renames one path, optionally replacing the destination." },
    ContractDoc { name: "HardLink", text: "A mutation payload that creates one hard link." },
    ContractDoc { name: "Write", text: "A mutation payload that writes bounded bytes at a file offset." },
    ContractDoc { name: "Resize", text: "A mutation payload that changes a file logical length." },
    ContractDoc { name: "ZeroRange", text: "A mutation payload that zeroes a byte range." },
    ContractDoc { name: "Preallocate", text: "A mutation payload that preallocates a byte range." },
    ContractDoc { name: "CloneRange", text: "A mutation payload that copies a byte range within the Filesystem." },
    ContractDoc { name: "SetMetadata", text: "A mutation payload that updates file metadata." },
    ContractDoc { name: "CreateDirectories", text: "A mutation payload that creates missing parent directories." },
    ContractDoc { name: "PutFile", text: "A mutation payload that creates or replaces a file with bounded contents." },
    ContractDoc { name: "CopyFile", text: "A mutation payload that copies one file to another path." },
    ContractDoc { name: "Mutation", text: "One mutually exclusive Filesystem mutation operation." },
    ContractDoc { name: "ApplyTransactionRequest", text: "Carries the inputs for the apply transaction operation." },
    ContractDoc { name: "RebaseTransactionRequest", text: "Carries the inputs for the rebase transaction operation." },
    ContractDoc { name: "RebaseTransactionResponse", text: "Carries the result of the rebase transaction operation." },
    ContractDoc { name: "ForkWorkspaceRequest", text: "Carries the inputs for the fork workspace operation." },
    ContractDoc { name: "DiffRequest", text: "Carries the inputs for the diff operation." },
    ContractDoc { name: "LogicalName", text: "A path name represented in an explicit byte encoding." },
    ContractDoc { name: "FileRecordSnapshot", text: "An immutable file record snapshot snapshot." },
    ContractDoc { name: "FileRecordChange", text: "One file record change change in a reconciliation result." },
    ContractDoc { name: "TreeEntrySnapshot", text: "An immutable tree entry snapshot snapshot." },
    ContractDoc { name: "DirectoryBindingChange", text: "One directory binding change change in a reconciliation result." },
    ContractDoc { name: "WorkCounters", text: "Aggregated counters for bounded provider and source work." },
    ContractDoc { name: "DiffResponse", text: "Carries the result of the diff operation." },
    ContractDoc { name: "RebaseRequest", text: "Carries the inputs for the rebase operation." },
    ContractDoc { name: "FileConflict", text: "Describes a file conflict conflict." },
    ContractDoc { name: "ContentConflict", text: "Describes a content conflict conflict." },
    ContractDoc { name: "SparseConflict", text: "Describes a sparse conflict conflict." },
    ContractDoc { name: "DirectoryNameConflict", text: "Describes a directory name conflict conflict." },
    ContractDoc { name: "DirectoryRangeConflict", text: "Describes a directory range conflict conflict." },
    ContractDoc { name: "Conflict", text: "One conflict region or directory identity observed during reconciliation." },
    ContractDoc { name: "RebaseResponse", text: "Carries the result of the rebase operation." },
    ContractDoc { name: "PlanJoinRequest", text: "Carries the inputs for the plan join operation." },
    ContractDoc { name: "JoinPlan", text: "The planned changes and conflicts for joining one generation into another." },
    ContractDoc { name: "ApplyJoinRequest", text: "Carries the inputs for the apply join operation." },
    ContractDoc { name: "JoinResponse", text: "Carries the result of the join operation." },
    ContractDoc { name: "RetainGenerationRequest", text: "Carries the inputs for the retain generation operation." },
    ContractDoc { name: "RetainGenerationResponse", text: "Carries the result of the retain generation operation." },
    ContractDoc { name: "ExportRequest", text: "Carries the inputs for the export operation." },
    ContractDoc { name: "ExportChunk", text: "One bounded object chunk in a generation export." },
    ContractDoc { name: "ImportChunk", text: "One bounded object chunk in a generation import." },
    ContractDoc { name: "ImportResponse", text: "Carries the result of the import operation." },
    ContractDoc { name: "CredentialRequest", text: "Carries the inputs for the credential operation." },
    ContractDoc { name: "CredentialResponse", text: "A provider credential or endpoint issued for a bounded operation." },
    ContractDoc { name: "S3Credential", text: "Temporary S3-compatible credential material." },
    ContractDoc { name: "ObserveRequest", text: "Carries the inputs for the observe operation." },
    ContractDoc { name: "ObserveResponse", text: "The current state and outcome of an asynchronous Filesystem operation." },
    ContractDoc { name: "CancelRequest", text: "Carries the inputs for the cancel operation." },
    ContractDoc { name: "CancelResponse", text: "The observed result of a cancellation request." },
];

#[rustfmt::skip]
pub const FILESYSTEM_FIELD_DOCS: &[FieldDoc] = &[
    FieldDoc { message: "WorkspaceRef", name: "workspace_id", text: "The stable workspace identifier." },
    FieldDoc { message: "WorkspaceRef", name: "name", text: "The logical or display name." },
    FieldDoc { message: "GenerationRef", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "GenerationRef", name: "generation_id", text: "The stable generation identifier." },
    FieldDoc { message: "WorkspaceContextRoot", name: "root_id", text: "The stable context-root identifier." },
    FieldDoc { message: "WorkspaceContextRoot", name: "source_path", text: "The native source path associated with the workspace root." },
    FieldDoc { message: "WorkspaceContextRoot", name: "workspace_id", text: "The stable workspace identifier." },
    FieldDoc { message: "WorkspaceContextRoot", name: "workspace_name", text: "The workspace name value carried by workspace context root." },
    FieldDoc { message: "WorkspaceContextRoot", name: "parent_workspace_id", text: "The parent workspace id value carried by workspace context root." },
    FieldDoc { message: "WorkspaceContextRoot", name: "mount_path", text: "The mount path value carried by workspace context root." },
    FieldDoc { message: "WorkspaceContextRoots", name: "roots", text: "The repeated roots values carried by workspace context roots." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "version", text: "The version or format revision." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "revision", text: "The monotonic revision of the observed aggregate or generation." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "context_id", text: "The context id value carried by workspace context snapshot." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "parent_context_id", text: "The parent context id value carried by workspace context snapshot." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "roots", text: "The repeated roots values carried by workspace context snapshot." },
    FieldDoc { message: "WorkspaceContextSnapshot", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "WorkspaceContextDiscard", name: "context_ids", text: "The repeated context ids values carried by workspace context discard." },
    FieldDoc { message: "OperationOptions", name: "idempotency_key", text: "The client idempotency key for this mutation." },
    FieldDoc { message: "PageOptions", name: "maximum_items", text: "The maximum number of items requested in this page." },
    FieldDoc { message: "PageOptions", name: "after", text: "The cursor or name after which to continue." },
    FieldDoc { message: "ByteRange", name: "offset", text: "The offset value carried by byte range." },
    FieldDoc { message: "ByteRange", name: "length", text: "The length value carried by byte range." },
    FieldDoc { message: "OptionalU32", name: "present", text: "The present value carried by optional u32." },
    FieldDoc { message: "OptionalU32", name: "unavailable", text: "The unavailable value carried by optional u32." },
    FieldDoc { message: "OptionalU64", name: "present", text: "The present value carried by optional u64." },
    FieldDoc { message: "OptionalU64", name: "unavailable", text: "The unavailable value carried by optional u64." },
    FieldDoc { message: "OptionalI64", name: "present", text: "The present value carried by optional i64." },
    FieldDoc { message: "OptionalI64", name: "unavailable", text: "The unavailable value carried by optional i64." },
    FieldDoc { message: "Metadata", name: "posix_mode", text: "The optional u32 carried by metadata." },
    FieldDoc { message: "Metadata", name: "posix_uid", text: "The optional u32 carried by metadata." },
    FieldDoc { message: "Metadata", name: "posix_gid", text: "The optional u32 carried by metadata." },
    FieldDoc { message: "Metadata", name: "posix_flags", text: "The optional u64 carried by metadata." },
    FieldDoc { message: "Metadata", name: "windows_attributes", text: "The optional u32 carried by metadata." },
    FieldDoc { message: "Metadata", name: "created_ns", text: "The optional i64 carried by metadata." },
    FieldDoc { message: "Metadata", name: "modified_ns", text: "The optional i64 carried by metadata." },
    FieldDoc { message: "Metadata", name: "accessed_ns", text: "The optional i64 carried by metadata." },
    FieldDoc { message: "Metadata", name: "changed_ns", text: "The optional i64 carried by metadata." },
    FieldDoc { message: "Metadata", name: "has_named_attributes", text: "Whether the file has named attributes." },
    FieldDoc { message: "Metadata", name: "has_acl", text: "Whether the file has acl." },
    FieldDoc { message: "Metadata", name: "has_security_descriptor", text: "Whether the file has security descriptor." },
    FieldDoc { message: "FileStat", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "FileStat", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "FileStat", name: "link_count", text: "The link count value carried by file stat." },
    FieldDoc { message: "FileStat", name: "logical_bytes", text: "The optional u64 carried by file stat." },
    FieldDoc { message: "FileStat", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "DirectoryEntry", name: "name", text: "The logical or display name." },
    FieldDoc { message: "DirectoryEntry", name: "stat", text: "The file stat for this entry." },
    FieldDoc { message: "DirectoryPage", name: "entries", text: "The entries in this bounded page." },
    FieldDoc { message: "DirectoryPage", name: "next", text: "The cursor after this page." },
    FieldDoc { message: "Extent", name: "range", text: "The byte range carried by extent." },
    FieldDoc { message: "Extent", name: "kind", text: "The enum discriminator for this value." },
    FieldDoc { message: "Capabilities", name: "contract_version", text: "The contract version value carried by capabilities." },
    FieldDoc { message: "Capabilities", name: "profiles", text: "The repeated profiles values carried by capabilities." },
    FieldDoc { message: "Capabilities", name: "maximum_request_bytes", text: "The maximum request bytes permitted or requested by this operation." },
    FieldDoc { message: "Capabilities", name: "maximum_response_bytes", text: "The maximum response bytes permitted or requested by this operation." },
    FieldDoc { message: "Capabilities", name: "maximum_transaction_mutations", text: "The maximum transaction mutations permitted or requested by this operation." },
    FieldDoc { message: "Capabilities", name: "maximum_page_items", text: "The maximum page items permitted or requested by this operation." },
    FieldDoc { message: "Capabilities", name: "native_mount_credentials", text: "The native mount credentials value carried by capabilities." },
    FieldDoc { message: "Capabilities", name: "s3_credentials", text: "The s3 credentials value carried by capabilities." },
    FieldDoc { message: "Capabilities", name: "source_reconciliation", text: "The source reconciliation value carried by capabilities." },
    FieldDoc { message: "SourceStateRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "SourceOperationRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "SourceOperationRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "SourceResponse", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "SourceResponse", name: "reason", text: "The reason associated with the current state." },
    FieldDoc { message: "SourceResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "Workspace", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "Workspace", name: "name", text: "The logical or display name." },
    FieldDoc { message: "Workspace", name: "profile", text: "The filesystem profile carried by workspace." },
    FieldDoc { message: "Workspace", name: "head", text: "The generation ref carried by workspace." },
    FieldDoc { message: "Workspace", name: "deleted", text: "The deleted value carried by workspace." },
    FieldDoc { message: "HandshakeRequest", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "HandshakeResponse", name: "protocol", text: "The protocol identity used for the handshake." },
    FieldDoc { message: "HandshakeResponse", name: "capabilities", text: "Capabilities advertised or granted by the provider." },
    FieldDoc { message: "CreateWorkspaceRequest", name: "name", text: "The logical or display name." },
    FieldDoc { message: "CreateWorkspaceRequest", name: "profile", text: "The filesystem profile carried by create workspace request." },
    FieldDoc { message: "CreateWorkspaceRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "OpenWorkspaceRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "OpenWorkspaceRequest", name: "name", text: "The logical or display name." },
    FieldDoc { message: "WorkspaceResponse", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "WorkspaceResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "DeleteWorkspaceRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "DeleteWorkspaceRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "MutationResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "MutationResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "MutationResponse", name: "actual_head", text: "The generation ref carried by mutation response." },
    FieldDoc { message: "MutationResponse", name: "conflicts", text: "Conflicts observed while applying or reconciling the operation." },
    FieldDoc { message: "MutationResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "GetHeadRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "GetGenerationRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "GenerationResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "GenerationResponse", name: "parents", text: "The repeated parents values carried by generation response." },
    FieldDoc { message: "ReadRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ReadRequest", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "ReadRequest", name: "range", text: "The byte range carried by read request." },
    FieldDoc { message: "ReadRequest", name: "maximum_bytes", text: "The maximum response bytes requested." },
    FieldDoc { message: "ReadResponse", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "StatRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "StatRequest", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "StatResponse", name: "stat", text: "The file stat for this entry." },
    FieldDoc { message: "ListDirectoryRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ListDirectoryRequest", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "ListDirectoryRequest", name: "page", text: "The page options carried by list directory request." },
    FieldDoc { message: "ListDirectoryResponse", name: "page", text: "The directory page carried by list directory response." },
    FieldDoc { message: "ReadLinkRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ReadLinkRequest", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "ReadLinkRequest", name: "maximum_bytes", text: "The maximum response bytes requested." },
    FieldDoc { message: "PlanExtentsRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "PlanExtentsRequest", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "PlanExtentsRequest", name: "range", text: "The byte range carried by plan extents request." },
    FieldDoc { message: "PlanExtentsRequest", name: "maximum_extents", text: "The maximum extents permitted or requested by this operation." },
    FieldDoc { message: "PlanExtentsResponse", name: "extents", text: "The repeated extents values carried by plan extents response." },
    FieldDoc { message: "PlanExtentsResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "CreateFile", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "CreateFile", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "CreateFile", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "CreateDirectory", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "CreateDirectory", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "CreateSymbolicLink", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "CreateSymbolicLink", name: "target", text: "The symbolic link target or operation target." },
    FieldDoc { message: "CreateSymbolicLink", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "Remove", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "Rename", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "Rename", name: "destination", text: "The destination path or generation." },
    FieldDoc { message: "Rename", name: "replace", text: "The replace value carried by rename." },
    FieldDoc { message: "HardLink", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "HardLink", name: "destination", text: "The destination path or generation." },
    FieldDoc { message: "Write", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "Write", name: "offset", text: "The offset value carried by write." },
    FieldDoc { message: "Write", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "Resize", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "Resize", name: "logical_bytes", text: "The logical bytes value carried by resize." },
    FieldDoc { message: "ZeroRange", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "ZeroRange", name: "range", text: "The byte range carried by zero range." },
    FieldDoc { message: "ZeroRange", name: "allocated", text: "The allocated value carried by zero range." },
    FieldDoc { message: "ZeroRange", name: "extend", text: "The extend value carried by zero range." },
    FieldDoc { message: "Preallocate", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "Preallocate", name: "range", text: "The byte range carried by preallocate." },
    FieldDoc { message: "Preallocate", name: "keep_size", text: "The keep size value carried by preallocate." },
    FieldDoc { message: "CloneRange", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "CloneRange", name: "source_offset", text: "The source offset value carried by clone range." },
    FieldDoc { message: "CloneRange", name: "destination", text: "The destination path or generation." },
    FieldDoc { message: "CloneRange", name: "destination_offset", text: "The destination offset value carried by clone range." },
    FieldDoc { message: "CloneRange", name: "length", text: "The length value carried by clone range." },
    FieldDoc { message: "SetMetadata", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "SetMetadata", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "CreateDirectories", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "PutFile", name: "path", text: "The logical path within the selected workspace or generation." },
    FieldDoc { message: "PutFile", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "CopyFile", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "CopyFile", name: "destination", text: "The destination path or generation." },
    FieldDoc { message: "Mutation", name: "create_file", text: "The create file carried by mutation." },
    FieldDoc { message: "Mutation", name: "create_directory", text: "The create directory carried by mutation." },
    FieldDoc { message: "Mutation", name: "create_symbolic_link", text: "The create symbolic link carried by mutation." },
    FieldDoc { message: "Mutation", name: "remove", text: "The remove carried by mutation." },
    FieldDoc { message: "Mutation", name: "rename", text: "The rename carried by mutation." },
    FieldDoc { message: "Mutation", name: "hard_link", text: "The hard link carried by mutation." },
    FieldDoc { message: "Mutation", name: "write", text: "The write carried by mutation." },
    FieldDoc { message: "Mutation", name: "resize", text: "The resize carried by mutation." },
    FieldDoc { message: "Mutation", name: "zero_range", text: "The zero range carried by mutation." },
    FieldDoc { message: "Mutation", name: "preallocate", text: "The preallocate carried by mutation." },
    FieldDoc { message: "Mutation", name: "clone_range", text: "The clone range carried by mutation." },
    FieldDoc { message: "Mutation", name: "set_metadata", text: "The set metadata carried by mutation." },
    FieldDoc { message: "Mutation", name: "create_directories", text: "The create directories carried by mutation." },
    FieldDoc { message: "Mutation", name: "put_file", text: "The put file carried by mutation." },
    FieldDoc { message: "Mutation", name: "copy_file", text: "The copy file carried by mutation." },
    FieldDoc { message: "ApplyTransactionRequest", name: "base", text: "The generation ref carried by apply transaction request." },
    FieldDoc { message: "ApplyTransactionRequest", name: "mutations", text: "The repeated mutations values carried by apply transaction request." },
    FieldDoc { message: "ApplyTransactionRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "ApplyTransactionRequest", name: "maximum_conflicts", text: "The maximum conflicts to return." },
    FieldDoc { message: "RebaseTransactionRequest", name: "base", text: "The generation ref carried by rebase transaction request." },
    FieldDoc { message: "RebaseTransactionRequest", name: "mutations", text: "The repeated mutations values carried by rebase transaction request." },
    FieldDoc { message: "RebaseTransactionRequest", name: "maximum_conflicts", text: "The maximum conflicts to return." },
    FieldDoc { message: "RebaseTransactionRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "RebaseTransactionResponse", name: "base", text: "The generation ref carried by rebase transaction response." },
    FieldDoc { message: "RebaseTransactionResponse", name: "conflicts", text: "Conflicts observed while applying or reconciling the operation." },
    FieldDoc { message: "RebaseTransactionResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "ForkWorkspaceRequest", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "ForkWorkspaceRequest", name: "destination_name", text: "The destination name value carried by fork workspace request." },
    FieldDoc { message: "ForkWorkspaceRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "DiffRequest", name: "from", text: "The generation ref carried by diff request." },
    FieldDoc { message: "DiffRequest", name: "to", text: "The generation ref carried by diff request." },
    FieldDoc { message: "DiffRequest", name: "maximum_changes", text: "The maximum changes to return." },
    FieldDoc { message: "LogicalName", name: "encoding", text: "The name encoding carried by logical name." },
    FieldDoc { message: "LogicalName", name: "bytes", text: "The encoded name or bounded byte payload." },
    FieldDoc { message: "FileRecordSnapshot", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "FileRecordSnapshot", name: "file_kind", text: "The file kind carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "link_count", text: "The link count value carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "metadata_object", text: "The metadata object value carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "payload_kind", text: "The payload kind value carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "logical_bytes", text: "The optional u64 carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "payload_object", text: "The payload object value carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "inline_bytes", text: "The inline bytes value carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "device_major", text: "The optional u32 carried by file record snapshot." },
    FieldDoc { message: "FileRecordSnapshot", name: "device_minor", text: "The optional u32 carried by file record snapshot." },
    FieldDoc { message: "FileRecordChange", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "FileRecordChange", name: "before", text: "The file record snapshot carried by file record change." },
    FieldDoc { message: "FileRecordChange", name: "after", text: "The cursor or name after which to continue." },
    FieldDoc { message: "TreeEntrySnapshot", name: "name", text: "The logical or display name." },
    FieldDoc { message: "TreeEntrySnapshot", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "TreeEntrySnapshot", name: "file_kind", text: "The file kind carried by tree entry snapshot." },
    FieldDoc { message: "DirectoryBindingChange", name: "directory_id", text: "The stable directory identifier." },
    FieldDoc { message: "DirectoryBindingChange", name: "name", text: "The logical or display name." },
    FieldDoc { message: "DirectoryBindingChange", name: "before", text: "The tree entry snapshot carried by directory binding change." },
    FieldDoc { message: "DirectoryBindingChange", name: "after", text: "The cursor or name after which to continue." },
    FieldDoc { message: "WorkCounters", name: "authority_records_read", text: "The authority records read value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "authority_records_appended", text: "The authority records appended value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "authority_bytes_read", text: "The authority bytes read value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "authority_bytes_written", text: "The authority bytes written value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "object_probes", text: "The object probes value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "backend_read_operations", text: "The backend read operations value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "backend_write_operations", text: "The backend write operations value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "durability_operations", text: "The durability operations value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "page_reads", text: "The page reads value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "page_writes", text: "The page writes value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "object_bytes_read", text: "The object bytes read value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "object_bytes_written", text: "The object bytes written value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "bytes_hashed", text: "The bytes hashed value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "bytes_copied", text: "The bytes copied value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "bytes_encoded", text: "The bytes encoded value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "source_bytes_read", text: "The source bytes read value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "output_bytes", text: "The output bytes value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "items_examined", text: "The items examined value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "items_returned", text: "The items returned value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "allocation_operations", text: "The allocation operations value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "peak_allocation_bytes", text: "The peak allocation bytes value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "materializations", text: "The materializations value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "source_path_components", text: "The source path components value carried by work counters." },
    FieldDoc { message: "WorkCounters", name: "source_entries_visited", text: "The source entries visited value carried by work counters." },
    FieldDoc { message: "DiffResponse", name: "from", text: "The generation ref carried by diff response." },
    FieldDoc { message: "DiffResponse", name: "to", text: "The generation ref carried by diff response." },
    FieldDoc { message: "DiffResponse", name: "files", text: "The repeated files values carried by diff response." },
    FieldDoc { message: "DiffResponse", name: "bindings", text: "The repeated bindings values carried by diff response." },
    FieldDoc { message: "DiffResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "DiffResponse", name: "work", text: "The work counters carried by diff response." },
    FieldDoc { message: "RebaseRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "RebaseRequest", name: "maximum_conflicts", text: "The maximum conflicts to return." },
    FieldDoc { message: "RebaseRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "RebaseRequest", name: "maximum_generations", text: "The maximum generations to inspect." },
    FieldDoc { message: "RebaseRequest", name: "maximum_changes", text: "The maximum changes to return." },
    FieldDoc { message: "FileConflict", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "ContentConflict", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "ContentConflict", name: "range", text: "The byte range carried by content conflict." },
    FieldDoc { message: "SparseConflict", name: "file_id", text: "The stable file identifier." },
    FieldDoc { message: "SparseConflict", name: "offset", text: "The offset value carried by sparse conflict." },
    FieldDoc { message: "SparseConflict", name: "target", text: "The symbolic link target or operation target." },
    FieldDoc { message: "DirectoryNameConflict", name: "directory_id", text: "The stable directory identifier." },
    FieldDoc { message: "DirectoryNameConflict", name: "name", text: "The logical or display name." },
    FieldDoc { message: "DirectoryRangeConflict", name: "directory_id", text: "The stable directory identifier." },
    FieldDoc { message: "DirectoryRangeConflict", name: "after", text: "The cursor or name after which to continue." },
    FieldDoc { message: "DirectoryRangeConflict", name: "maximum_entries", text: "The maximum entries permitted or requested by this operation." },
    FieldDoc { message: "Conflict", name: "file_record", text: "The file conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "metadata", text: "Portable and provider-specific metadata for the file." },
    FieldDoc { message: "Conflict", name: "file_length", text: "The file conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "content_range", text: "The content conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "sparse_seek", text: "The sparse conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "directory_name", text: "The directory name conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "directory_range", text: "The directory range conflict carried by conflict." },
    FieldDoc { message: "Conflict", name: "use", text: "The conflict use carried by conflict." },
    FieldDoc { message: "Conflict", name: "expected_digest", text: "The expected digest value carried by conflict." },
    FieldDoc { message: "Conflict", name: "actual_digest", text: "The actual digest value carried by conflict." },
    FieldDoc { message: "RebaseResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "RebaseResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "RebaseResponse", name: "conflicts", text: "Conflicts observed while applying or reconciling the operation." },
    FieldDoc { message: "RebaseResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "PlanJoinRequest", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "PlanJoinRequest", name: "target", text: "The symbolic link target or operation target." },
    FieldDoc { message: "PlanJoinRequest", name: "maximum_changes", text: "The maximum changes to return." },
    FieldDoc { message: "PlanJoinRequest", name: "maximum_conflicts", text: "The maximum conflicts to return." },
    FieldDoc { message: "PlanJoinRequest", name: "maximum_generations", text: "The maximum generations to inspect." },
    FieldDoc { message: "PlanJoinRequest", name: "history", text: "The join history carried by plan join request." },
    FieldDoc { message: "JoinPlan", name: "plan_id", text: "The plan id value carried by join plan." },
    FieldDoc { message: "JoinPlan", name: "source", text: "The source generation or path for this operation." },
    FieldDoc { message: "JoinPlan", name: "expected_target", text: "The generation ref carried by join plan." },
    FieldDoc { message: "JoinPlan", name: "file_changes", text: "The repeated file changes values carried by join plan." },
    FieldDoc { message: "JoinPlan", name: "conflicts", text: "Conflicts observed while applying or reconciling the operation." },
    FieldDoc { message: "JoinPlan", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "JoinPlan", name: "common_ancestor", text: "The generation ref carried by join plan." },
    FieldDoc { message: "JoinPlan", name: "maximum_generations", text: "The maximum generations to inspect." },
    FieldDoc { message: "JoinPlan", name: "maximum_changes", text: "The maximum changes to return." },
    FieldDoc { message: "JoinPlan", name: "maximum_conflicts", text: "The maximum conflicts to return." },
    FieldDoc { message: "JoinPlan", name: "history", text: "The join history carried by join plan." },
    FieldDoc { message: "JoinPlan", name: "binding_changes", text: "The repeated binding changes values carried by join plan." },
    FieldDoc { message: "ApplyJoinRequest", name: "plan", text: "The join plan carried by apply join request." },
    FieldDoc { message: "ApplyJoinRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "JoinResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "JoinResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "JoinResponse", name: "conflicts", text: "Conflicts observed while applying or reconciling the operation." },
    FieldDoc { message: "JoinResponse", name: "truncated", text: "Whether additional results were omitted by a bound." },
    FieldDoc { message: "RetainGenerationRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "RetainGenerationRequest", name: "identity", text: "The identity used to retain or identify this resource." },
    FieldDoc { message: "RetainGenerationRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "RetainGenerationResponse", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "RetainGenerationResponse", name: "identity", text: "The identity used to retain or identify this resource." },
    FieldDoc { message: "RetainGenerationResponse", name: "status", text: "The current operation or mutation status." },
    FieldDoc { message: "ExportRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "ExportRequest", name: "after", text: "The cursor or name after which to continue." },
    FieldDoc { message: "ExportRequest", name: "maximum_objects", text: "The maximum objects permitted or requested by this operation." },
    FieldDoc { message: "ExportRequest", name: "maximum_bytes", text: "The maximum response bytes requested." },
    FieldDoc { message: "ExportChunk", name: "cursor", text: "The cursor value carried by export chunk." },
    FieldDoc { message: "ExportChunk", name: "object_id", text: "The immutable object identifier." },
    FieldDoc { message: "ExportChunk", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "ExportChunk", name: "terminal", text: "The terminal value carried by export chunk." },
    FieldDoc { message: "ImportChunk", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "ImportChunk", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "ImportChunk", name: "cursor", text: "The cursor value carried by import chunk." },
    FieldDoc { message: "ImportChunk", name: "object_id", text: "The immutable object identifier." },
    FieldDoc { message: "ImportChunk", name: "contents", text: "The bounded file contents carried by this frame." },
    FieldDoc { message: "ImportChunk", name: "terminal", text: "The terminal value carried by import chunk." },
    FieldDoc { message: "ImportResponse", name: "outcome", text: "The selected operation or interaction outcome." },
    FieldDoc { message: "CredentialRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "CredentialRequest", name: "generation", text: "The immutable generation selected for this operation." },
    FieldDoc { message: "CredentialRequest", name: "writable", text: "The writable value carried by credential request." },
    FieldDoc { message: "CredentialRequest", name: "expires_after_seconds", text: "The expires after seconds value carried by credential request." },
    FieldDoc { message: "CredentialRequest", name: "operation", text: "Operation options or identity for this request." },
    FieldDoc { message: "CredentialResponse", name: "endpoint", text: "The endpoint value carried by credential response." },
    FieldDoc { message: "CredentialResponse", name: "expires_at_unix_seconds", text: "The expires at time in Unix units." },
    FieldDoc { message: "CredentialResponse", name: "bearer_token", text: "The bearer token value carried by credential response." },
    FieldDoc { message: "CredentialResponse", name: "s3", text: "The s3 credential carried by credential response." },
    FieldDoc { message: "S3Credential", name: "bucket", text: "The bucket value carried by s3 credential." },
    FieldDoc { message: "S3Credential", name: "region", text: "The region value carried by s3 credential." },
    FieldDoc { message: "S3Credential", name: "access_key_id", text: "The access key id value carried by s3 credential." },
    FieldDoc { message: "S3Credential", name: "secret_access_key", text: "The secret access key value carried by s3 credential." },
    FieldDoc { message: "S3Credential", name: "session_token", text: "The session token value carried by s3 credential." },
    FieldDoc { message: "ObserveRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "ObserveRequest", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "ObserveResponse", name: "state", text: "The current state discriminator." },
    FieldDoc { message: "ObserveResponse", name: "outcome", text: "The selected operation or interaction outcome." },
    FieldDoc { message: "CancelRequest", name: "workspace", text: "The workspace selected for this operation." },
    FieldDoc { message: "CancelRequest", name: "operation_id", text: "The stable operation identifier." },
    FieldDoc { message: "CancelResponse", name: "operation", text: "Operation options or identity for this request." },
];

#[rustfmt::skip]
pub const FILESYSTEM_ENUM_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "WorkspaceContextState", text: "The workspace context state values defined by the Filesystem contract." },
    ContractDoc { name: "FilesystemProfile", text: "The filesystem profile values defined by the Filesystem contract." },
    ContractDoc { name: "FileKind", text: "The file kind values defined by the Filesystem contract." },
    ContractDoc { name: "MutationStatus", text: "The mutation status values defined by the Filesystem contract." },
    ContractDoc { name: "JoinHistory", text: "The join history values defined by the Filesystem contract." },
    ContractDoc { name: "ConflictUse", text: "The conflict use values defined by the Filesystem contract." },
    ContractDoc { name: "SparseTarget", text: "The sparse target values defined by the Filesystem contract." },
    ContractDoc { name: "ExtentKind", text: "The extent kind values defined by the Filesystem contract." },
    ContractDoc { name: "SourceState", text: "The source state values defined by the Filesystem contract." },
    ContractDoc { name: "SourceInvalidationReason", text: "The source invalidation reason values defined by the Filesystem contract." },
    ContractDoc { name: "NameEncoding", text: "The name encoding values defined by the Filesystem contract." },
    ContractDoc { name: "RebaseStatus", text: "The rebase status values defined by the Filesystem contract." },
    ContractDoc { name: "JoinStatus", text: "The join status values defined by the Filesystem contract." },
];

pub const FILESYSTEM_SERVICE_DOC: ContractDoc = ContractDoc {
    name: "FilesystemService",
    text: "Provides generation-exact workspace, file, transaction, source, and operation control.",
};

#[rustfmt::skip]
pub const FILESYSTEM_SERVICE_DOCS: &[ContractDoc] = &[FILESYSTEM_SERVICE_DOC];

pub fn filesystem_service_doc(name: &str) -> Option<&'static str> {
    FILESYSTEM_SERVICE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
}

#[rustfmt::skip]

pub const FILESYSTEM_METHOD_DOCS: &[ContractDoc] = &[
    ContractDoc { name: "Handshake", text: "Negotiates protocol identity and Filesystem capabilities." },
    ContractDoc { name: "CreateWorkspace", text: "Creates a named workspace." },
    ContractDoc { name: "OpenWorkspace", text: "Opens a workspace by stable reference or name." },
    ContractDoc { name: "DeleteWorkspace", text: "Deletes a workspace with an idempotent mutation." },
    ContractDoc { name: "GetHead", text: "Returns the current workspace head generation." },
    ContractDoc { name: "GetGeneration", text: "Reads an immutable generation and its parents." },
    ContractDoc { name: "Read", text: "Reads bounded file contents from a generation." },
    ContractDoc { name: "Stat", text: "Returns file metadata from a generation." },
    ContractDoc { name: "ListDirectory", text: "Lists a bounded directory page." },
    ContractDoc { name: "ReadLink", text: "Reads a symbolic-link target." },
    ContractDoc { name: "PlanExtents", text: "Plans sparse extents for a file range." },
    ContractDoc { name: "ApplyTransaction", text: "Applies an ordered transaction against a generation." },
    ContractDoc { name: "RebaseTransaction", text: "Rebases transaction mutations onto the current head." },
    ContractDoc { name: "ForkWorkspace", text: "Forks a workspace from a generation." },
    ContractDoc { name: "Diff", text: "Compares two generations." },
    ContractDoc { name: "Rebase", text: "Rebases workspace changes and reports conflicts." },
    ContractDoc { name: "PlanJoin", text: "Plans a join from one generation into another." },
    ContractDoc { name: "ApplyJoin", text: "Applies a previously planned join." },
    ContractDoc { name: "Checkpoint", text: "Retains a named generation checkpoint." },
    ContractDoc { name: "Pin", text: "Retains a named generation pin." },
    ContractDoc { name: "Export", text: "Exports bounded generation objects." },
    ContractDoc { name: "Import", text: "Imports bounded generation objects." },
    ContractDoc { name: "IssueMountCredential", text: "Issues a bounded native mount credential." },
    ContractDoc { name: "IssueS3Credential", text: "Issues a bounded S3 credential." },
    ContractDoc { name: "GetSourceState", text: "Reads source reconciliation state." },
    ContractDoc { name: "ReconcileSource", text: "Reconciles pending source changes." },
    ContractDoc { name: "RescanSource", text: "Rescans the source workspace." },
    ContractDoc { name: "SealSource", text: "Seals the source workspace." },
    ContractDoc { name: "Observe", text: "Observes an asynchronous operation." },
    ContractDoc { name: "Cancel", text: "Requests cancellation of an asynchronous operation." },
];

pub fn filesystem_message_doc(name: &str) -> Option<&'static str> {
    FILESYSTEM_MESSAGE_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}
pub fn filesystem_field_doc(message: &str, name: &str) -> Option<&'static str> {
    FILESYSTEM_FIELD_DOCS
        .iter()
        .find(|d| d.message == message && d.name == name)
        .map(|d| d.text)
}
pub fn filesystem_enum_doc(name: &str) -> Option<&'static str> {
    FILESYSTEM_ENUM_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}
pub fn filesystem_method_doc(name: &str) -> Option<&'static str> {
    FILESYSTEM_METHOD_DOCS
        .iter()
        .find(|d| d.name == name)
        .map(|d| d.text)
}

const ARCHIVED_DESCRIPTOR: &[u8] = include_bytes!("../tests/fixtures/filesystem-v2.descriptor.bin");

/// Return the source-info-free Filesystem file while retaining all descriptor fields.
pub fn filesystem_file_descriptor() -> FileDescriptorProto {
    target_file()
}
pub fn filesystem_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![protocol::protocol_file_descriptor(), target_file()],
    }
    .encode_to_vec()
}
pub fn filesystem_proto() -> String {
    render_proto(&target_file())
}
pub fn descriptor() -> Vec<u8> {
    filesystem_descriptor()
}
pub fn file_descriptor() -> FileDescriptorProto {
    filesystem_file_descriptor()
}

impl ContractModel {
    pub fn file_descriptor(&self) -> FileDescriptorProto {
        filesystem_file_descriptor()
    }
    pub fn descriptor(&self) -> Vec<u8> {
        filesystem_descriptor()
    }
    pub fn canonical_sha256(&self) -> String {
        let digest = Sha256::digest(self.descriptor());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Return the Rust-owned behavior metadata for every Filesystem RPC.
    pub const fn operation_policies(&self) -> &'static [OperationPolicy] {
        FILESYSTEM_OPERATION_POLICIES
    }

    /// Return compiled service and feature availability metadata.
    pub const fn service_availability(&self) -> &'static [ServiceAvailability] {
        FILESYSTEM_SERVICE_AVAILABILITY
    }
}

fn target_file() -> FileDescriptorProto {
    let set =
        FileDescriptorSet::decode(ARCHIVED_DESCRIPTOR).expect("archived Filesystem descriptor");
    let mut file = set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(FILE_NAME))
        .expect("Filesystem file in fixture");
    // Source locations belong to the migration oracle.  They are not part of
    // the canonical contract identity and can change when the Rust renderer
    // is reorganized.
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
    for enum_ in &file.enum_type {
        render_enum(&mut out, enum_, 0);
    }
    for message in &file.message_type {
        render_message(&mut out, message, 0);
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
fn render_enum(out: &mut String, enum_: &prost_types::EnumDescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    out.push_str(&format!(
        "{p}enum {} {{\n",
        enum_.name.as_deref().unwrap_or_default()
    ));
    for value in &enum_.value {
        out.push_str(&format!(
            "{p}  {} = {};\n",
            value.name.as_deref().unwrap_or_default(),
            value.number.unwrap_or_default()
        ));
    }
    out.push_str(&format!("{p}}}\n\n"));
}
fn render_message(out: &mut String, message: &DescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    out.push_str(&format!(
        "{p}message {} {{\n",
        message.name.as_deref().unwrap_or_default()
    ));
    for (index, oneof) in message.oneof_decl.iter().enumerate() {
        if message
            .field
            .iter()
            .any(|f| f.oneof_index == Some(index as i32) && !f.proto3_optional.unwrap_or(false))
        {
            out.push_str(&format!(
                "{p}  oneof {} {{\n",
                oneof.name.as_deref().unwrap_or_default()
            ));
            for field in &message.field {
                if field.oneof_index == Some(index as i32)
                    && !field.proto3_optional.unwrap_or(false)
                {
                    render_field(out, field, indent + 4);
                }
            }
            out.push_str(&format!("{p}  }}\n"));
        }
    }
    for field in &message.field {
        if field.oneof_index.is_none() {
            if let Some(entry) = map_entry_for(message, field) {
                render_map_field(out, field, entry, indent + 2);
            } else {
                render_field(out, field, indent + 2);
            }
        }
    }
    for nested in &message.nested_type {
        if nested.options.as_ref().and_then(|o| o.map_entry) != Some(true) {
            render_message(out, nested, indent + 2);
        }
    }
    for enum_ in &message.enum_type {
        render_enum(out, enum_, indent + 2);
    }
    for range in &message.reserved_range {
        out.push_str(&format!(
            "{p}  reserved {} to {};\n",
            range.start.unwrap_or_default(),
            range.end.unwrap_or_default() - 1
        ));
    }
    for name in &message.reserved_name {
        out.push_str(&format!("{p}  reserved \"{name}\";\n"));
    }
    out.push_str(&format!("{p}}}\n\n"));
}
fn render_field(out: &mut String, field: &prost_types::FieldDescriptorProto, indent: usize) {
    let p = " ".repeat(indent);
    let label = match field
        .label
        .and_then(|v| prost_types::field_descriptor_proto::Label::try_from(v).ok())
    {
        Some(prost_types::field_descriptor_proto::Label::Repeated) => "repeated ",
        Some(prost_types::field_descriptor_proto::Label::Required) => "required ",
        Some(prost_types::field_descriptor_proto::Label::Optional)
            if field.proto3_optional.unwrap_or(false) =>
        {
            "optional "
        }
        _ => "",
    };
    let ty = field
        .type_name
        .as_deref()
        .map(|x| x.trim_start_matches('.').to_owned())
        .unwrap_or_else(|| {
            format!(
                "{:?}",
                field
                    .r#type
                    .and_then(|v| prost_types::field_descriptor_proto::Type::try_from(v).ok())
                    .unwrap_or(prost_types::field_descriptor_proto::Type::Bytes)
            )
            .to_lowercase()
        });
    out.push_str(&format!(
        "{p}{label}{ty} {} = {};\n",
        field.name.as_deref().unwrap_or_default(),
        field.number.unwrap_or_default()
    ));
}
fn map_entry_for<'a>(
    message: &'a DescriptorProto,
    field: &prost_types::FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    let type_name = field.type_name.as_deref()?.rsplit('.').next()?;
    message.nested_type.iter().find(|nested| {
        nested.name.as_deref() == Some(type_name)
            && nested.options.as_ref().and_then(|o| o.map_entry) == Some(true)
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
        .find(|f| f.name.as_deref() == Some("key"))
        .map(field_type_token)
        .unwrap_or_else(|| "string".to_owned());
    let value = entry
        .field
        .iter()
        .find(|f| f.name.as_deref() == Some("value"))
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
fn field_type_token(field: &prost_types::FieldDescriptorProto) -> String {
    field
        .type_name
        .as_deref()
        .map(|x| x.trim_start_matches('.').to_owned())
        .unwrap_or_else(|| {
            format!(
                "{:?}",
                field
                    .r#type
                    .and_then(|v| prost_types::field_descriptor_proto::Type::try_from(v).ok())
                    .unwrap_or(prost_types::field_descriptor_proto::Type::Bytes)
            )
            .to_lowercase()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_sdk_contract_options::option_spec;
    #[test]
    fn preserves_inventory_and_handshake() {
        let file = filesystem_file_descriptor();
        assert_eq!(file.name.as_deref(), Some(FILE_NAME));
        assert_eq!(file.package.as_deref(), Some(PACKAGE));
        assert_eq!(file.service.len(), 1);
        assert_eq!(VALIDATION_OPTION_SPECS, OPTION_SPECS);
        assert_eq!(PROTO2_VALIDATION_EXTENSIONS.len(), OPTION_SPECS.len());
        for extension in PROTO2_VALIDATION_EXTENSIONS {
            let spec = option_spec(extension.number).expect("shared option oracle");
            assert_eq!(
                (spec.name, spec.scalar_type),
                (extension.name, extension.scalar_type)
            );
        }
        assert_eq!(ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST.len(), 71);
    }
    #[test]
    fn source_docs_cover_every_declared_item() {
        fn visit(message: &DescriptorProto) {
            let name = message.name.as_deref().unwrap_or_default();
            assert!(
                filesystem_message_doc(name).is_some(),
                "missing FILESYSTEM message docs for {name}"
            );
            for field in &message.field {
                assert!(
                    filesystem_field_doc(name, field.name.as_deref().unwrap_or_default()).is_some(),
                    "missing FILESYSTEM field docs for {name}.{}",
                    field.name.as_deref().unwrap_or_default()
                );
            }
            for nested in &message.nested_type {
                visit(nested);
            }
        }
        let file = filesystem_file_descriptor();
        for message in &file.message_type {
            visit(message);
        }
        for enumeration in &file.enum_type {
            assert!(filesystem_enum_doc(enumeration.name.as_deref().unwrap_or_default()).is_some());
        }
        assert_eq!(FILESYSTEM_SERVICE_DOC.name, "FilesystemService");
        for method in &file.service[0].method {
            assert!(filesystem_method_doc(method.name.as_deref().unwrap_or_default()).is_some());
        }
    }
    #[test]
    fn preserves_presence_nested_and_streams() {
        let file = filesystem_file_descriptor();
        assert!(file.message_type.iter().any(|m| {
            m.oneof_decl
                .iter()
                .any(|o| o.name.as_deref().unwrap_or_default().starts_with('_'))
        }));
        assert!(file.message_type.iter().any(|m| !m.oneof_decl.is_empty()));
        assert!(file.service[0].method.iter().any(|m| m.client_streaming.unwrap_or(false) || m.server_streaming.unwrap_or(false)));
    }
}
