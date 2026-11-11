//! Integration behaviour tests.
#[cfg(loom)]
use acyclic_fs::NativeMountError;
#[cfg(all(
    not(loom),
    not(target_arch = "wasm32"),
    any(target_os = "linux", windows)
))]
mod native_watch_events;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(not(loom))]
mod core_group_commit;
#[cfg(not(loom))]
mod distributed;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
mod linux_ext4_perf;
#[cfg(not(loom))]
mod logical_objects;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
mod macos_apfs_perf;
#[cfg(not(loom))]
mod measured_fork;
#[cfg(not(loom))]
mod merge_lineage;
#[cfg(not(loom))]
mod recovery_concurrency;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/projfs_enumeration.rs"]
#[cfg(not(loom))]
mod projfs_enumeration;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/projfs_profile.rs"]
#[cfg(not(loom))]
mod projfs_profile;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/projfs_metadata/mod.rs"]
#[cfg(not(loom))]
mod projfs_metadata;

#[path = "cases/access.rs"]
#[cfg(not(loom))]
mod access;

#[path = "cases/path_properties.rs"]
#[cfg(not(loom))]
mod path_properties;

#[cfg(all(not(target_arch = "wasm32"), not(loom)))]
#[path = "cases/directory_cursor.rs"]
mod directory_cursor;

#[cfg(feature = "fuzzing")]
#[path = "cases/workspace_names.rs"]
#[cfg(not(loom))]
mod workspace_names;

#[path = "cases/cloud_contract.rs"]
#[cfg(not(loom))]
mod cloud_contract;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/native_owner_retirement.rs"]
#[cfg(not(loom))]
mod native_owner_retirement;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/projfs_names.rs"]
#[cfg(not(loom))]
mod projfs_names;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/projfs_paths.rs"]
#[cfg(not(loom))]
mod projfs_paths;

#[path = "cases/accounting_reconciliation.rs"]
#[cfg(not(loom))]
mod accounting_reconciliation;

#[path = "cases/accounting_layout.rs"]
#[cfg(not(loom))]
mod accounting_layout;

#[path = "cases/workspace_reads.rs"]
#[cfg(not(loom))]
mod workspace_reads;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/native_owner_notifications.rs"]
#[cfg(not(loom))]
mod native_owner_notifications;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/native_source_watch_custody.rs"]
#[cfg(not(loom))]
mod native_source_watch_custody;

#[path = "cases/session_fs.rs"]
#[cfg(not(loom))]
mod session_fs;

// Included production regressions use these crate-root API imports.
#[cfg(all(windows, not(target_arch = "wasm32")))]
#[cfg(not(loom))]
use acyclic_fs::{OperationReceipt, PublicationPermit, WorkCounters, WorkspaceError, kernel};

#[cfg(all(feature = "conformance", not(target_arch = "wasm32")))]
#[path = "cases/conformance.rs"]
#[cfg(not(loom))]
mod public_conformance;

#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "../src/sync.rs"]
mod sync;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod sync {
    pub(crate) use acyclic_native_runtime::InlineMutex as Mutex;
}

#[cfg(not(loom))]
mod fs_concurrency_bench;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod materialization_bootstrap;
#[cfg(not(loom))]
mod mount_qualification;
#[cfg(not(target_arch = "wasm32"))]
mod native_binding_model;
#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod native_stage_loom;
#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod native_view_table_loom;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod package_install;
#[cfg(all(loom, feature = "model-checking"))]
mod session_fs_model;
#[cfg(not(loom))]
mod shared_storage_contract;
#[cfg(not(loom))]
mod transaction_create_batch;
#[cfg(all(not(loom), windows, not(target_arch = "wasm32")))]
mod windows_ntfs_perf;
