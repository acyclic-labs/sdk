//! Integration behaviour tests.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(not(loom))]
mod core_group_commit;
#[cfg(not(loom))]
mod distributed;
#[cfg(feature = "local")]
#[cfg(not(loom))]
mod linux_ext4_perf;
#[cfg(not(loom))]
mod logical_objects;
#[cfg(feature = "local")]
#[cfg(not(loom))]
mod macos_apfs_perf;
#[cfg(not(loom))]
mod measured_fork;
#[cfg(not(loom))]
mod merge_lineage;
#[cfg(not(loom))]
mod recovery_concurrency;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/projfs_enumeration.rs"]
#[cfg(not(loom))]
mod projfs_enumeration;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/projfs_profile.rs"]
#[cfg(not(loom))]
mod projfs_profile;

#[cfg(all(feature = "local", feature = "memory", feature = "native-mount"))]
#[path = "cases/projfs_metadata.rs"]
#[cfg(not(loom))]
mod projfs_metadata;

#[path = "cases/access.rs"]
#[cfg(not(loom))]
mod access;

#[path = "cases/path_properties.rs"]
#[cfg(not(loom))]
mod path_properties;

#[cfg(feature = "fuzzing")]
#[path = "cases/workspace_names.rs"]
#[cfg(not(loom))]
mod workspace_names;

#[cfg(feature = "memory")]
#[path = "cases/cloud_contract.rs"]
#[cfg(not(loom))]
mod cloud_contract;

#[cfg(feature = "native-watch")]
#[path = "cases/native_owner_registry.rs"]
#[cfg(not(loom))]
mod native_owner_registry;

#[cfg(feature = "native-mount")]
#[path = "cases/projfs_names.rs"]
#[cfg(not(loom))]
mod projfs_names;

#[cfg(feature = "native-mount")]
#[path = "cases/projfs_paths.rs"]
#[cfg(not(loom))]
mod projfs_paths;

#[cfg(feature = "memory")]
#[path = "cases/accounting_reconciliation.rs"]
#[cfg(not(loom))]
mod accounting_reconciliation;

#[path = "cases/accounting_layout.rs"]
#[cfg(not(loom))]
mod accounting_layout;

#[cfg(feature = "memory")]
#[path = "cases/workspace_reads.rs"]
#[cfg(not(loom))]
mod workspace_reads;

#[cfg(feature = "native-watch")]
#[path = "cases/native_owner_notifications.rs"]
#[cfg(not(loom))]
mod native_owner_notifications;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/native_source_watch_custody.rs"]
#[cfg(not(loom))]
mod native_source_watch_custody;

#[cfg(all(feature = "memory", feature = "session"))]
#[path = "cases/session_fs.rs"]
#[cfg(not(loom))]
mod session_fs;

// Included production regressions use these crate-root API imports.
#[cfg(all(windows, feature = "local", feature = "native-mount"))]
#[cfg(not(loom))]
use acyclic_fs::{OperationReceipt, PublicationPermit, WorkCounters, WorkspaceError, kernel};

#[cfg(all(
    feature = "conformance",
    feature = "memory",
    not(target_arch = "wasm32")
))]
#[path = "cases/conformance.rs"]
#[cfg(not(loom))]
mod public_conformance;

#[cfg(all(not(target_arch = "wasm32"), feature = "native-mount"))]
#[path = "../src/sync.rs"]
mod sync;

#[cfg(all(
    not(loom),
    target_arch = "wasm32",
    feature = "memory",
    feature = "browser"
))]
mod browser_commit;
#[cfg(all(not(loom), feature = "memory", feature = "distributed"))]
mod fs_concurrency_bench;
#[cfg(all(not(loom), feature = "memory", feature = "native-mount"))]
mod materialization_bootstrap;
#[cfg(not(loom))]
mod mount_qualification;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-mount"))]
mod native_binding_model;
#[cfg(all(
    loom,
    not(target_arch = "wasm32"),
    feature = "native-mount",
    feature = "model-checking"
))]
mod native_stage_loom;
#[cfg(all(
    loom,
    not(target_arch = "wasm32"),
    feature = "native-mount",
    feature = "model-checking"
))]
mod native_view_table_loom;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod package_install;
#[cfg(all(
    loom,
    feature = "memory",
    feature = "session",
    feature = "model-checking"
))]
mod session_fs_model;
#[cfg(not(loom))]
mod shared_storage_contract;
#[cfg(all(not(loom), feature = "memory", feature = "distributed"))]
mod transaction_create_batch;
#[cfg(all(not(loom), windows, feature = "local", feature = "native-mount"))]
mod windows_ntfs_perf;
