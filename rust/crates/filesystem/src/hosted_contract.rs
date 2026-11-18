//! Typed authority for the hosted Filesystem enum contract.
//!
//! The hosted TypeScript adapter needs two pieces of information for every
//! protobuf enum: the wire enum value and the public string (when one exists).
//! Keep those values here as typed protobuf variants.  The contract generator
//! derives the protobuf names with `as_str_name()`, so a renamed or removed
//! protobuf variant cannot silently leave a stale string table behind.

use crate::kernel::{
    DependencyUse, ExtentSeekTarget, FileKind as EngineFileKind, FilePayloadKind, NameEncoding,
};
use crate::model::FilesystemProfile as EngineProfile;
use crate::performance::WorkCounters;
use crate::workspace::{JoinHistory, WorkspaceExtentKind};
use acyclic_fs_wire::wire::filesystem::v2 as wire;
use serde_json::{Value, json};
use strum::IntoEnumIterator;

/// Smallest encoded response that can contain the protocol handshake.
pub const MINIMUM_HANDSHAKE_RESPONSE_BYTES: usize = 512;
/// Largest encoded byte-bearing protocol envelope around its contents.
pub const MAX_BYTE_RESPONSE_ENVELOPE_BYTES: u64 = 10 + 35 + 11 + 2;

/// One typed wire enum value and its optional public representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedEnumEntry<E> {
    /// The generated protobuf enum value.
    pub wire: E,
    /// Public string exposed by the high-level adapter, if representable.
    pub public: Option<&'static str>,
}

/// Creates one hosted enum contract entry.
pub const fn entry<E>(wire: E, public: Option<&'static str>) -> HostedEnumEntry<E> {
    HostedEnumEntry { wire, public }
}

/// Trait used to obtain the exact protobuf field name without duplicating it.
pub trait HostedWireEnum: Copy {
    /// Returns the generated protobuf field name.
    fn as_str_name(self) -> &'static str;
}

macro_rules! hosted_wire_enum {
    ($($type:ty),+ $(,)?) => {
        $(
            impl HostedWireEnum for $type {
                fn as_str_name(self) -> &'static str {
                    <$type>::as_str_name(&self)
                }
            }
        )+
    };
}

hosted_wire_enum!(
    wire::FilesystemProfile,
    wire::FileKind,
    wire::MutationStatus,
    wire::JoinHistory,
    wire::ConflictUse,
    wire::SparseTarget,
    wire::ExtentKind,
    wire::SourceState,
    wire::SourceInvalidationReason,
    wire::NameEncoding,
    wire::RebaseStatus,
    wire::JoinStatus,
);

/// Typed filesystem profile mapping.
pub const FILESYSTEM_PROFILE: &[HostedEnumEntry<wire::FilesystemProfile>] = &[
    entry(wire::FilesystemProfile::Unspecified, None),
    entry(wire::FilesystemProfile::Portable, Some("portable")),
    entry(wire::FilesystemProfile::Posix, Some("posix")),
    entry(wire::FilesystemProfile::Windows, Some("windows")),
    entry(wire::FilesystemProfile::Browser, Some("browser")),
];

/// Typed file kind mapping.
pub const FILE_KIND: &[HostedEnumEntry<wire::FileKind>] = &[
    entry(wire::FileKind::Unspecified, None),
    entry(wire::FileKind::Regular, Some("regular")),
    entry(wire::FileKind::Directory, Some("directory")),
    entry(wire::FileKind::SymbolicLink, Some("symbolic-link")),
    entry(wire::FileKind::Fifo, Some("fifo")),
    entry(wire::FileKind::Socket, Some("socket")),
    entry(wire::FileKind::CharacterDevice, Some("character-device")),
    entry(wire::FileKind::BlockDevice, Some("block-device")),
    entry(wire::FileKind::ReparsePoint, Some("reparse-point")),
    entry(wire::FileKind::MountBoundary, Some("mount-boundary")),
];

/// Typed mutation status mapping used by commit operations.
pub const MUTATION_STATUS_COMMIT: &[HostedEnumEntry<wire::MutationStatus>] = &[
    entry(wire::MutationStatus::Unspecified, None),
    entry(wire::MutationStatus::Committed, Some("committed")),
    entry(
        wire::MutationStatus::AlreadyCommitted,
        Some("already-committed"),
    ),
    entry(wire::MutationStatus::Conflict, Some("conflict")),
    entry(wire::MutationStatus::Fenced, Some("fenced")),
    entry(
        wire::MutationStatus::IdempotencyConflict,
        Some("idempotency-conflict"),
    ),
    entry(wire::MutationStatus::Indeterminate, None),
];

/// Typed mutation status mapping used by delete operations.
pub const MUTATION_STATUS_DELETE: &[HostedEnumEntry<wire::MutationStatus>] = &[
    entry(wire::MutationStatus::Unspecified, None),
    entry(wire::MutationStatus::Committed, Some("deleted")),
    entry(
        wire::MutationStatus::AlreadyCommitted,
        Some("already-deleted"),
    ),
    entry(wire::MutationStatus::Conflict, Some("conflict")),
    entry(wire::MutationStatus::Fenced, None),
    entry(
        wire::MutationStatus::IdempotencyConflict,
        Some("idempotency-conflict"),
    ),
    entry(wire::MutationStatus::Indeterminate, None),
];

/// Typed join history mapping.
pub const JOIN_HISTORY: &[HostedEnumEntry<wire::JoinHistory>] = &[
    entry(wire::JoinHistory::Unspecified, None),
    entry(wire::JoinHistory::Merge, Some("merge")),
    entry(wire::JoinHistory::Rebase, Some("rebase")),
    entry(wire::JoinHistory::Squash, Some("squash")),
    entry(wire::JoinHistory::CherryPick, Some("cherry-pick")),
];

/// Typed conflict usage mapping.
pub const CONFLICT_USE: &[HostedEnumEntry<wire::ConflictUse>] = &[
    entry(wire::ConflictUse::Unspecified, None),
    entry(wire::ConflictUse::Observation, Some("observation")),
    entry(wire::ConflictUse::Mutation, Some("mutation")),
    entry(
        wire::ConflictUse::ObservationAndMutation,
        Some("observation-and-mutation"),
    ),
];

/// Typed sparse target mapping.
pub const SPARSE_TARGET: &[HostedEnumEntry<wire::SparseTarget>] = &[
    entry(wire::SparseTarget::Unspecified, None),
    entry(wire::SparseTarget::Data, Some("data")),
    entry(wire::SparseTarget::Hole, Some("hole")),
];

/// Typed extent kind mapping.
pub const EXTENT_KIND: &[HostedEnumEntry<wire::ExtentKind>] = &[
    entry(wire::ExtentKind::Unspecified, None),
    entry(wire::ExtentKind::Hole, Some("hole")),
    entry(wire::ExtentKind::AllocatedZero, Some("allocated-zero")),
    entry(wire::ExtentKind::Content, Some("content")),
];

/// Typed source state mapping.
pub const SOURCE_STATE: &[HostedEnumEntry<wire::SourceState>] = &[
    entry(wire::SourceState::Unspecified, None),
    entry(wire::SourceState::Clean, Some("clean")),
    entry(wire::SourceState::PendingCapture, Some("pending-capture")),
    entry(wire::SourceState::NeedsRescan, Some("needs-rescan")),
    entry(wire::SourceState::Conflict, Some("conflict")),
    entry(wire::SourceState::Sealed, Some("sealed")),
];

/// Typed source invalidation mapping.
pub const SOURCE_INVALIDATION_REASON: &[HostedEnumEntry<wire::SourceInvalidationReason>] = &[
    entry(wire::SourceInvalidationReason::Unspecified, None),
    entry(
        wire::SourceInvalidationReason::InitialSnapshotRequired,
        Some("initial-snapshot-required"),
    ),
    entry(
        wire::SourceInvalidationReason::QueueOverflow,
        Some("queue-overflow"),
    ),
    entry(
        wire::SourceInvalidationReason::NativeRescanRequired,
        Some("native-rescan-required"),
    ),
    entry(
        wire::SourceInvalidationReason::BackendError,
        Some("backend-error"),
    ),
    entry(
        wire::SourceInvalidationReason::UnrepresentablePath,
        Some("unrepresentable-path"),
    ),
    entry(
        wire::SourceInvalidationReason::AmbiguousRename,
        Some("ambiguous-rename"),
    ),
    entry(
        wire::SourceInvalidationReason::RootChanged,
        Some("root-changed"),
    ),
];

/// Typed name encoding mapping.
pub const NAME_ENCODING: &[HostedEnumEntry<wire::NameEncoding>] = &[
    entry(wire::NameEncoding::Unspecified, None),
    entry(wire::NameEncoding::Utf8, Some(NameEncoding::Utf8.as_str())),
    entry(
        wire::NameEncoding::PosixBytes,
        Some(NameEncoding::PosixBytes.as_str()),
    ),
    entry(
        wire::NameEncoding::WindowsUtf16le,
        Some(NameEncoding::WindowsUtf16Le.as_str()),
    ),
];

/// Typed rebase status mapping.
pub const REBASE_STATUS: &[HostedEnumEntry<wire::RebaseStatus>] = &[
    entry(wire::RebaseStatus::Unspecified, None),
    entry(wire::RebaseStatus::Rebased, Some("rebased")),
    entry(wire::RebaseStatus::AlreadyRebased, Some("already-rebased")),
    entry(wire::RebaseStatus::Current, Some("current")),
    entry(wire::RebaseStatus::Stale, Some("stale")),
    entry(wire::RebaseStatus::Conflicted, Some("conflicted")),
    entry(wire::RebaseStatus::Fenced, Some("fenced")),
    entry(
        wire::RebaseStatus::IdempotencyConflict,
        Some("idempotency-conflict"),
    ),
];

/// Typed join status mapping.
pub const JOIN_STATUS: &[HostedEnumEntry<wire::JoinStatus>] = &[
    entry(wire::JoinStatus::Unspecified, None),
    entry(wire::JoinStatus::Applied, Some("applied")),
    entry(wire::JoinStatus::AlreadyApplied, Some("already-applied")),
    entry(wire::JoinStatus::NoChanges, Some("no-changes")),
    entry(wire::JoinStatus::StaleTarget, Some("stale-target")),
    entry(wire::JoinStatus::Conflicted, Some("conflicted")),
    entry(wire::JoinStatus::Fenced, Some("fenced")),
    entry(
        wire::JoinStatus::IdempotencyConflict,
        Some("idempotency-conflict"),
    ),
];

/// Returns every profile advertised by the hosted wire service.
pub fn supported_profiles() -> Vec<i32> {
    FILESYSTEM_PROFILE
        .iter()
        .filter(|entry| entry.wire != wire::FilesystemProfile::Unspecified)
        .map(|entry| entry.wire as i32)
        .collect()
}

/// Converts the engine profile to its one wire representation.
pub const fn profile(value: EngineProfile) -> wire::FilesystemProfile {
    match value {
        EngineProfile::Portable => wire::FilesystemProfile::Portable,
        EngineProfile::Posix => wire::FilesystemProfile::Posix,
        EngineProfile::Windows => wire::FilesystemProfile::Windows,
        EngineProfile::Browser => wire::FilesystemProfile::Browser,
    }
}

/// Converts a wire profile to the engine profile, rejecting unspecified.
pub const fn profile_from_wire(value: wire::FilesystemProfile) -> Option<EngineProfile> {
    match value {
        wire::FilesystemProfile::Portable => Some(EngineProfile::Portable),
        wire::FilesystemProfile::Posix => Some(EngineProfile::Posix),
        wire::FilesystemProfile::Windows => Some(EngineProfile::Windows),
        wire::FilesystemProfile::Browser => Some(EngineProfile::Browser),
        wire::FilesystemProfile::Unspecified => None,
    }
}

/// Converts the engine file kind to its one wire representation.
pub const fn file_kind(value: EngineFileKind) -> wire::FileKind {
    match value {
        EngineFileKind::Regular => wire::FileKind::Regular,
        EngineFileKind::Directory => wire::FileKind::Directory,
        EngineFileKind::SymbolicLink => wire::FileKind::SymbolicLink,
        EngineFileKind::Fifo => wire::FileKind::Fifo,
        EngineFileKind::Socket => wire::FileKind::Socket,
        EngineFileKind::CharacterDevice => wire::FileKind::CharacterDevice,
        EngineFileKind::BlockDevice => wire::FileKind::BlockDevice,
        EngineFileKind::ReparsePoint => wire::FileKind::ReparsePoint,
        EngineFileKind::MountBoundary => wire::FileKind::MountBoundary,
    }
}

/// Converts the engine name encoding to its one wire representation.
pub const fn name_encoding(value: NameEncoding) -> wire::NameEncoding {
    match value {
        NameEncoding::Utf8 => wire::NameEncoding::Utf8,
        NameEncoding::PosixBytes => wire::NameEncoding::PosixBytes,
        NameEncoding::WindowsUtf16Le => wire::NameEncoding::WindowsUtf16le,
    }
}

/// Converts a wire name encoding to the engine encoding, rejecting unspecified.
pub const fn name_encoding_from_wire(value: wire::NameEncoding) -> Option<NameEncoding> {
    match value {
        wire::NameEncoding::Utf8 => Some(NameEncoding::Utf8),
        wire::NameEncoding::PosixBytes => Some(NameEncoding::PosixBytes),
        wire::NameEncoding::WindowsUtf16le => Some(NameEncoding::WindowsUtf16Le),
        wire::NameEncoding::Unspecified => None,
    }
}

/// Converts the transaction sparse seek class to its one wire representation.
pub const fn sparse_target(value: ExtentSeekTarget) -> wire::SparseTarget {
    match value {
        ExtentSeekTarget::Data => wire::SparseTarget::Data,
        ExtentSeekTarget::Hole => wire::SparseTarget::Hole,
    }
}

/// Converts dependency usage to its one wire representation.
pub const fn conflict_use(value: DependencyUse) -> wire::ConflictUse {
    match value {
        DependencyUse::Observation => wire::ConflictUse::Observation,
        DependencyUse::Mutation => wire::ConflictUse::Mutation,
        DependencyUse::ObservationAndMutation => wire::ConflictUse::ObservationAndMutation,
    }
}

/// Converts an extent kind to its one wire representation.
pub const fn extent_kind(value: WorkspaceExtentKind) -> wire::ExtentKind {
    match value {
        WorkspaceExtentKind::Hole => wire::ExtentKind::Hole,
        WorkspaceExtentKind::AllocatedZero => wire::ExtentKind::AllocatedZero,
        WorkspaceExtentKind::Content => wire::ExtentKind::Content,
    }
}

/// Converts the public join history to its one wire representation.
pub const fn join_history(value: JoinHistory) -> wire::JoinHistory {
    match value {
        JoinHistory::Merge => wire::JoinHistory::Merge,
        JoinHistory::Rebase => wire::JoinHistory::Rebase,
        JoinHistory::Squash => wire::JoinHistory::Squash,
        JoinHistory::CherryPick => wire::JoinHistory::CherryPick,
    }
}

/// Converts a wire join history to the public value, rejecting unspecified.
pub const fn join_history_from_wire(value: wire::JoinHistory) -> Option<JoinHistory> {
    match value {
        wire::JoinHistory::Merge => Some(JoinHistory::Merge),
        wire::JoinHistory::Rebase => Some(JoinHistory::Rebase),
        wire::JoinHistory::Squash => Some(JoinHistory::Squash),
        wire::JoinHistory::CherryPick => Some(JoinHistory::CherryPick),
        wire::JoinHistory::Unspecified => None,
    }
}

fn map<E: HostedWireEnum>(entries: &[HostedEnumEntry<E>]) -> Vec<Value> {
    entries
        .iter()
        .map(|entry| json!({ "wire": entry.wire.as_str_name(), "public": entry.public }))
        .collect()
}

/// Serializes the complete Rust-owned hosted contract for the TypeScript generator.
pub fn contract_json() -> Value {
    let file_kinds = FILE_KIND
        .iter()
        .filter_map(|entry| entry.public)
        .map(|kind| Value::String(kind.to_owned()))
        .collect::<Vec<_>>();
    let payload_kinds = FilePayloadKind::iter()
        .map(|kind| Value::String(kind.as_ref().to_owned()))
        .collect::<Vec<_>>();
    #[allow(
        clippy::expect_used,
        reason = "WorkCounters derives Serialize and has no fallible field serializer"
    )]
    let work_counter_keys = serde_json::to_value(WorkCounters::default())
        .expect("WorkCounters serialization cannot fail")
        .as_object()
        .map(|fields| fields.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    json!({
        "maps": {
            "filesystem_profile": map(FILESYSTEM_PROFILE),
            "file_kind": map(FILE_KIND),
            "mutation_status_commit": map(MUTATION_STATUS_COMMIT),
            "mutation_status_delete": map(MUTATION_STATUS_DELETE),
            "join_history": map(JOIN_HISTORY),
            "conflict_use": map(CONFLICT_USE),
            "sparse_target": map(SPARSE_TARGET),
            "extent_kind": map(EXTENT_KIND),
            "source_state": map(SOURCE_STATE),
            "source_invalidation_reason": map(SOURCE_INVALIDATION_REASON),
            "name_encoding": map(NAME_ENCODING),
            "rebase_status": map(REBASE_STATUS),
            "join_status": map(JOIN_STATUS),
        },
        "file_kinds": file_kinds,
        "file_payload_kinds": payload_kinds,
        "work_counter_keys": work_counter_keys,
    })
}
