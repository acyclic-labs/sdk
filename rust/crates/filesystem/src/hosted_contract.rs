//! Typed authority for the hosted Filesystem enum contract.
//!
//! The hosted TypeScript adapter needs two pieces of information for every
//! protobuf enum: the wire enum value and the public string (when one exists).
//! Keep those values here as typed protobuf variants.  The contract generator
//! derives the protobuf names with `as_str_name()`, so a renamed or removed
//! protobuf variant cannot silently leave a stale string table behind.

use crate::kernel::{FileKind as EngineFileKind, NameEncoding};
use crate::model::FilesystemProfile as EngineProfile;
use crate::wire::filesystem::v2 as wire;
use crate::workspace::{
    JoinHistory, TransactionDependencyUse, TransactionSparseSeek, WorkspaceExtentKind,
};
use serde_json::{Value, json};

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
    entry(wire::NameEncoding::Utf8, Some("utf8")),
    entry(wire::NameEncoding::PosixBytes, Some("posix-bytes")),
    entry(wire::NameEncoding::WindowsUtf16le, Some("windows-utf16le")),
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
pub const fn sparse_target(value: TransactionSparseSeek) -> wire::SparseTarget {
    match value {
        TransactionSparseSeek::Data => wire::SparseTarget::Data,
        TransactionSparseSeek::Hole => wire::SparseTarget::Hole,
    }
}

/// Converts dependency usage to its one wire representation.
pub const fn conflict_use(value: TransactionDependencyUse) -> wire::ConflictUse {
    match value {
        TransactionDependencyUse::Observation => wire::ConflictUse::Observation,
        TransactionDependencyUse::Mutation => wire::ConflictUse::Mutation,
        TransactionDependencyUse::ObservationAndMutation => {
            wire::ConflictUse::ObservationAndMutation
        }
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

/// Validates one positive page-sized request bound against the negotiated
/// hosted service limit.
pub const fn validate_page_bound(value: u32, maximum: u32) -> Result<(), &'static str> {
    if value == 0 || value > maximum {
        Err("page bound is invalid")
    } else {
        Ok(())
    }
}

/// Validates the shared transaction mutation and conflict bounds.
///
/// Transaction staging, rebasing, and commit requests all use this same
/// policy. Keeping it in the Rust contract prevents a client facade from
/// accepting a request that the canonical service must reject.
pub const fn validate_transaction_bounds(
    mutation_count: usize,
    maximum_mutations: u32,
    maximum_conflicts: u32,
    maximum_page_items: u32,
) -> Result<(), &'static str> {
    if mutation_count == 0
        || mutation_count > maximum_mutations as usize
        || maximum_conflicts == 0
        || maximum_conflicts > maximum_page_items
    {
        Err("transaction bounds are invalid")
    } else {
        Ok(())
    }
}

/// Validates the shared generation, diff, and conflict bounds used by live
/// rebase and join planning/application.
pub const fn validate_generation_bounds(
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
    maximum_page_items: u32,
) -> Result<(), &'static str> {
    if maximum_generations == 0
        || maximum_generations > maximum_page_items
        || maximum_changes == 0
        || maximum_changes > maximum_page_items
        || maximum_conflicts == 0
        || maximum_conflicts > maximum_page_items
    {
        Err("generation bounds are invalid")
    } else {
        Ok(())
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_names_are_derived_from_typed_wire_values() {
        assert_eq!(
            FILESYSTEM_PROFILE[1].wire.as_str_name(),
            "FILESYSTEM_PROFILE_PORTABLE"
        );
        assert_eq!(FILE_KIND[9].wire.as_str_name(), "FILE_KIND_MOUNT_BOUNDARY");
        assert_eq!(
            JOIN_HISTORY[4].wire.as_str_name(),
            "JOIN_HISTORY_CHERRY_PICK"
        );
        assert_eq!(
            NAME_ENCODING[3].wire.as_str_name(),
            "NAME_ENCODING_WINDOWS_UTF16LE"
        );
    }

    #[test]
    fn engine_converters_share_the_typed_wire_values() {
        assert_eq!(profile(EngineProfile::Portable), FILESYSTEM_PROFILE[1].wire);
        assert_eq!(file_kind(EngineFileKind::MountBoundary), FILE_KIND[9].wire);
        assert_eq!(name_encoding(NameEncoding::Utf8), NAME_ENCODING[1].wire);
        assert_eq!(
            sparse_target(TransactionSparseSeek::Hole),
            SPARSE_TARGET[2].wire
        );
        assert_eq!(
            conflict_use(TransactionDependencyUse::ObservationAndMutation),
            CONFLICT_USE[3].wire
        );
        assert_eq!(
            extent_kind(WorkspaceExtentKind::AllocatedZero),
            EXTENT_KIND[2].wire
        );
        assert_eq!(join_history(JoinHistory::CherryPick), JOIN_HISTORY[4].wire);
    }

    #[test]
    fn shared_bounds_are_positive_and_negotiated() {
        assert!(validate_page_bound(1, 2).is_ok());
        assert!(validate_page_bound(0, 2).is_err());
        assert!(validate_page_bound(3, 2).is_err());
        assert!(validate_transaction_bounds(1, 2, 1, 2).is_ok());
        assert!(validate_transaction_bounds(0, 2, 1, 2).is_err());
        assert!(validate_transaction_bounds(3, 2, 1, 2).is_err());
        assert!(validate_transaction_bounds(1, 2, 3, 2).is_err());
        assert!(validate_generation_bounds(1, 1, 1, 1).is_ok());
        assert!(validate_generation_bounds(0, 1, 1, 1).is_err());
        assert!(validate_generation_bounds(1, 2, 1, 1).is_err());
    }

    #[test]
    fn generated_contract_contains_all_maps_and_preserves_public_values() -> Result<(), &'static str>
    {
        let contract = contract_json();
        let Some(maps) = contract.get("maps").and_then(Value::as_object) else {
            return Err("generated hosted contract has no maps object");
        };
        assert_eq!(maps.len(), 13);
        assert_public_values(
            "filesystem_profile",
            FILESYSTEM_PROFILE,
            &[
                None,
                Some("portable"),
                Some("posix"),
                Some("windows"),
                Some("browser"),
            ],
        );
        assert_public_values(
            "file_kind",
            FILE_KIND,
            &[
                None,
                Some("regular"),
                Some("directory"),
                Some("symbolic-link"),
                Some("fifo"),
                Some("socket"),
                Some("character-device"),
                Some("block-device"),
                Some("reparse-point"),
                Some("mount-boundary"),
            ],
        );
        assert_public_values(
            "mutation_status_commit",
            MUTATION_STATUS_COMMIT,
            &[
                None,
                Some("committed"),
                Some("already-committed"),
                Some("conflict"),
                Some("fenced"),
                Some("idempotency-conflict"),
                None,
            ],
        );
        assert_public_values(
            "mutation_status_delete",
            MUTATION_STATUS_DELETE,
            &[
                None,
                Some("deleted"),
                Some("already-deleted"),
                Some("conflict"),
                None,
                Some("idempotency-conflict"),
                None,
            ],
        );
        assert_public_values(
            "join_history",
            JOIN_HISTORY,
            &[
                None,
                Some("merge"),
                Some("rebase"),
                Some("squash"),
                Some("cherry-pick"),
            ],
        );
        assert_public_values(
            "conflict_use",
            CONFLICT_USE,
            &[
                None,
                Some("observation"),
                Some("mutation"),
                Some("observation-and-mutation"),
            ],
        );
        assert_public_values(
            "sparse_target",
            SPARSE_TARGET,
            &[None, Some("data"), Some("hole")],
        );
        assert_public_values(
            "extent_kind",
            EXTENT_KIND,
            &[None, Some("hole"), Some("allocated-zero"), Some("content")],
        );
        assert_public_values(
            "source_state",
            SOURCE_STATE,
            &[
                None,
                Some("clean"),
                Some("pending-capture"),
                Some("needs-rescan"),
                Some("conflict"),
                Some("sealed"),
            ],
        );
        assert_public_values(
            "source_invalidation_reason",
            SOURCE_INVALIDATION_REASON,
            &[
                None,
                Some("initial-snapshot-required"),
                Some("queue-overflow"),
                Some("native-rescan-required"),
                Some("backend-error"),
                Some("unrepresentable-path"),
                Some("ambiguous-rename"),
                Some("root-changed"),
            ],
        );
        assert_public_values(
            "name_encoding",
            NAME_ENCODING,
            &[
                None,
                Some("utf8"),
                Some("posix-bytes"),
                Some("windows-utf16le"),
            ],
        );
        assert_public_values(
            "rebase_status",
            REBASE_STATUS,
            &[
                None,
                Some("rebased"),
                Some("already-rebased"),
                Some("current"),
                Some("stale"),
                Some("conflicted"),
                Some("fenced"),
                Some("idempotency-conflict"),
            ],
        );
        assert_public_values(
            "join_status",
            JOIN_STATUS,
            &[
                None,
                Some("applied"),
                Some("already-applied"),
                Some("no-changes"),
                Some("stale-target"),
                Some("conflicted"),
                Some("fenced"),
                Some("idempotency-conflict"),
            ],
        );
        Ok(())
    }

    fn assert_public_values<E: HostedWireEnum>(
        name: &str,
        entries: &[HostedEnumEntry<E>],
        expected: &[Option<&'static str>],
    ) {
        assert_eq!(
            entries.iter().map(|entry| entry.public).collect::<Vec<_>>(),
            expected,
            "public semantics changed for hosted mapping {name}",
        );
    }

    #[test]
    fn advertised_profiles_are_the_typed_non_unspecified_values() {
        assert_eq!(
            supported_profiles(),
            FILESYSTEM_PROFILE[1..]
                .iter()
                .map(|entry| entry.wire as i32)
                .collect::<Vec<_>>()
        );
    }
}
