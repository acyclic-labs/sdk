#![allow(missing_docs)]

//! Emits the Rust-owned hosted enum mapping tables consumed by the
//! TypeScript hosted adapter generator.

use serde_json::{Value, json};

type Entry = (&'static str, Option<&'static str>);

fn map(entries: &[Entry]) -> Vec<Value> {
    entries
        .iter()
        .map(|(wire, public)| json!({ "wire": wire, "public": public }))
        .collect()
}

const FILESYSTEM_PROFILE: &[Entry] = &[
    ("FILESYSTEM_PROFILE_UNSPECIFIED", None),
    ("FILESYSTEM_PROFILE_PORTABLE", Some("portable")),
    ("FILESYSTEM_PROFILE_POSIX", Some("posix")),
    ("FILESYSTEM_PROFILE_WINDOWS", Some("windows")),
    ("FILESYSTEM_PROFILE_BROWSER", Some("browser")),
];
const FILE_KIND: &[Entry] = &[
    ("FILE_KIND_UNSPECIFIED", None),
    ("FILE_KIND_REGULAR", Some("regular")),
    ("FILE_KIND_DIRECTORY", Some("directory")),
    ("FILE_KIND_SYMBOLIC_LINK", Some("symbolic-link")),
    ("FILE_KIND_FIFO", Some("fifo")),
    ("FILE_KIND_SOCKET", Some("socket")),
    ("FILE_KIND_CHARACTER_DEVICE", Some("character-device")),
    ("FILE_KIND_BLOCK_DEVICE", Some("block-device")),
    ("FILE_KIND_REPARSE_POINT", Some("reparse-point")),
    ("FILE_KIND_MOUNT_BOUNDARY", Some("mount-boundary")),
];
const MUTATION_STATUS_COMMIT: &[Entry] = &[
    ("MUTATION_STATUS_UNSPECIFIED", None),
    ("MUTATION_STATUS_COMMITTED", Some("committed")),
    (
        "MUTATION_STATUS_ALREADY_COMMITTED",
        Some("already-committed"),
    ),
    ("MUTATION_STATUS_CONFLICT", Some("conflict")),
    ("MUTATION_STATUS_FENCED", Some("fenced")),
    (
        "MUTATION_STATUS_IDEMPOTENCY_CONFLICT",
        Some("idempotency-conflict"),
    ),
    ("MUTATION_STATUS_INDETERMINATE", None),
];
const MUTATION_STATUS_DELETE: &[Entry] = &[
    ("MUTATION_STATUS_UNSPECIFIED", None),
    ("MUTATION_STATUS_COMMITTED", Some("deleted")),
    ("MUTATION_STATUS_ALREADY_COMMITTED", Some("already-deleted")),
    ("MUTATION_STATUS_CONFLICT", Some("conflict")),
    ("MUTATION_STATUS_FENCED", None),
    (
        "MUTATION_STATUS_IDEMPOTENCY_CONFLICT",
        Some("idempotency-conflict"),
    ),
    ("MUTATION_STATUS_INDETERMINATE", None),
];
const JOIN_HISTORY: &[Entry] = &[
    ("JOIN_HISTORY_UNSPECIFIED", None),
    ("JOIN_HISTORY_MERGE", Some("merge")),
    ("JOIN_HISTORY_REBASE", Some("rebase")),
    ("JOIN_HISTORY_SQUASH", Some("squash")),
    ("JOIN_HISTORY_CHERRY_PICK", Some("cherry-pick")),
];
const CONFLICT_USE: &[Entry] = &[
    ("CONFLICT_USE_UNSPECIFIED", None),
    ("CONFLICT_USE_OBSERVATION", Some("observation")),
    ("CONFLICT_USE_MUTATION", Some("mutation")),
    (
        "CONFLICT_USE_OBSERVATION_AND_MUTATION",
        Some("observation-and-mutation"),
    ),
];
const SPARSE_TARGET: &[Entry] = &[
    ("SPARSE_TARGET_UNSPECIFIED", None),
    ("SPARSE_TARGET_DATA", Some("data")),
    ("SPARSE_TARGET_HOLE", Some("hole")),
];
const EXTENT_KIND: &[Entry] = &[
    ("EXTENT_KIND_UNSPECIFIED", None),
    ("EXTENT_KIND_HOLE", Some("hole")),
    ("EXTENT_KIND_ALLOCATED_ZERO", Some("allocated-zero")),
    ("EXTENT_KIND_CONTENT", Some("content")),
];
const SOURCE_STATE: &[Entry] = &[
    ("SOURCE_STATE_UNSPECIFIED", None),
    ("SOURCE_STATE_CLEAN", Some("clean")),
    ("SOURCE_STATE_PENDING_CAPTURE", Some("pending-capture")),
    ("SOURCE_STATE_NEEDS_RESCAN", Some("needs-rescan")),
    ("SOURCE_STATE_CONFLICT", Some("conflict")),
    ("SOURCE_STATE_SEALED", Some("sealed")),
];
const SOURCE_INVALIDATION_REASON: &[Entry] = &[
    ("SOURCE_INVALIDATION_REASON_UNSPECIFIED", None),
    (
        "SOURCE_INVALIDATION_REASON_INITIAL_SNAPSHOT_REQUIRED",
        Some("initial-snapshot-required"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_QUEUE_OVERFLOW",
        Some("queue-overflow"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_NATIVE_RESCAN_REQUIRED",
        Some("native-rescan-required"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_BACKEND_ERROR",
        Some("backend-error"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_UNREPRESENTABLE_PATH",
        Some("unrepresentable-path"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_AMBIGUOUS_RENAME",
        Some("ambiguous-rename"),
    ),
    (
        "SOURCE_INVALIDATION_REASON_ROOT_CHANGED",
        Some("root-changed"),
    ),
];
const NAME_ENCODING: &[Entry] = &[
    ("NAME_ENCODING_UNSPECIFIED", None),
    ("NAME_ENCODING_UTF8", Some("utf8")),
    ("NAME_ENCODING_POSIX_BYTES", Some("posix-bytes")),
    ("NAME_ENCODING_WINDOWS_UTF16LE", Some("windows-utf16le")),
];
const REBASE_STATUS: &[Entry] = &[
    ("REBASE_STATUS_UNSPECIFIED", None),
    ("REBASE_STATUS_REBASED", Some("rebased")),
    ("REBASE_STATUS_ALREADY_REBASED", Some("already-rebased")),
    ("REBASE_STATUS_CURRENT", Some("current")),
    ("REBASE_STATUS_STALE", Some("stale")),
    ("REBASE_STATUS_CONFLICTED", Some("conflicted")),
    ("REBASE_STATUS_FENCED", Some("fenced")),
    (
        "REBASE_STATUS_IDEMPOTENCY_CONFLICT",
        Some("idempotency-conflict"),
    ),
];
const JOIN_STATUS: &[Entry] = &[
    ("JOIN_STATUS_UNSPECIFIED", None),
    ("JOIN_STATUS_APPLIED", Some("applied")),
    ("JOIN_STATUS_ALREADY_APPLIED", Some("already-applied")),
    ("JOIN_STATUS_NO_CHANGES", Some("no-changes")),
    ("JOIN_STATUS_STALE_TARGET", Some("stale-target")),
    ("JOIN_STATUS_CONFLICTED", Some("conflicted")),
    ("JOIN_STATUS_FENCED", Some("fenced")),
    (
        "JOIN_STATUS_IDEMPOTENCY_CONFLICT",
        Some("idempotency-conflict"),
    ),
];

fn main() {
    let contract = json!({
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
    });
    println!("{contract}");
}
