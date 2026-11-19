//! Integration behaviour tests.
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod native_cut_custody;
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

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod computed_folds;
#[cfg(not(loom))]
mod core_group_commit;
#[cfg(not(loom))]
mod distributed;
#[cfg(all(not(loom), not(target_arch = "wasm32"), feature = "test-support"))]
mod hosted_sources;
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
use {
    acyclic_fs::{PublicationPermit, WorkspaceError, kernel},
    acyclic_fs_work::performance::{OperationReceipt, WorkCounters},
};

#[cfg(all(feature = "conformance", not(target_arch = "wasm32")))]
#[path = "cases/conformance.rs"]
#[cfg(not(loom))]
mod public_conformance;

#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "../src/sync.rs"]
mod sync;

#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "../src/native_sync.rs"]
mod native_sync;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod materialization_bootstrap;
#[cfg(not(loom))]
mod mount_qualification;
#[cfg(not(target_arch = "wasm32"))]
mod native_binding_model;
#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod native_view_table_loom;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod package_install;
#[cfg(all(loom, feature = "model-checking"))]
mod session_fs_model;
#[cfg(not(loom))]
mod shared_storage_contract;
#[cfg(all(not(loom), windows, not(target_arch = "wasm32")))]
mod windows_ntfs_perf;

use acyclic_fs_native_contract::NativeRootIdentity;

#[cfg(all(not(loom), any(target_os = "linux", target_os = "macos", windows)))]
#[path = "../src/native_sync/locks.rs"]
mod native_sync;

#[cfg(feature = "test-support")]
#[path = "blob_io.rs"]
mod blob_io;
#[cfg(feature = "browser")]
#[path = "browser_blob_custody.rs"]
mod browser_blob_custody;
#[cfg(feature = "browser")]
#[path = "browser_commit.rs"]
mod browser_commit;
#[path = "checkout_cancellation.rs"]
mod checkout_cancellation;
#[path = "checkout_content.rs"]
mod checkout_content;
#[path = "checkout_live.rs"]
mod checkout_live;
#[path = "checkout_merge.rs"]
mod checkout_merge;
#[path = "checkout_profiles.rs"]
mod checkout_profiles;
#[path = "checkout_publication.rs"]
mod checkout_publication;
#[path = "checkout_read.rs"]
mod checkout_read;
#[path = "checkout_tracking.rs"]
mod checkout_tracking;
#[path = "checkout_transactions.rs"]
mod checkout_transactions;
#[path = "measured_creation.rs"]
mod creation;
#[path = "darwin_mount_owner.rs"]
mod darwin_mount_owner;
#[cfg(feature = "test-support")]
#[path = "directory_tree.rs"]
mod directory_tree;
#[cfg(feature = "test-support")]
#[path = "export_codec.rs"]
mod export_codec;
#[cfg(feature = "test-support")]
#[path = "extent_clone.rs"]
mod extent_clone;
#[path = "extent_codec.rs"]
mod extent_codec;
#[path = "fs_concurrency_bench.rs"]
mod fs_concurrency_bench;
#[cfg(feature = "test-support")]
#[path = "generation_transfer.rs"]
mod generation_transfer;
#[path = "git_history.rs"]
mod git_history;
#[path = "git_marker_contracts.rs"]
mod git_marker_contracts;
#[path = "local_commit_model.rs"]
mod local_commit_model;
#[path = "local_facade.rs"]
mod local_facade;
#[path = "memory_authority.rs"]
mod memory_authority;
#[path = "memory_objects.rs"]
mod memory_objects;
#[path = "merge_driver_contracts.rs"]
mod merge_driver_contracts;
#[path = "metadata_codec.rs"]
mod metadata_codec;
#[path = "metadata_stamps.rs"]
mod metadata_stamps;
#[path = "mount_routing.rs"]
mod mount_routing;
#[path = "native_checkpoint.rs"]
mod native_checkpoint;
#[path = "native_root_identity.rs"]
mod native_root_identity;
#[path = "measured_native_source.rs"]
mod native_source;
#[path = "native_view_table.rs"]
mod native_view_table;
#[path = "operation_claim.rs"]
mod operation_claim;
#[cfg(feature = "model-checking")]
#[path = "operation_claim_model.rs"]
mod operation_claim_model;
#[path = "s3_contracts.rs"]
mod s3_contracts;
#[path = "source_lifecycle.rs"]
mod source_lifecycle;
#[path = "tree_codec.rs"]
mod tree_codec;
#[path = "wire_contracts.rs"]
mod wire_contracts;
#[path = "wire_error_contracts.rs"]
mod wire_error_contracts;
#[path = "wire_rpc.rs"]
mod wire_rpc;
#[path = "workspace_append.rs"]
mod workspace_append;
#[path = "workspace_context_contracts.rs"]
mod workspace_context_contracts;
#[path = "workspace_contracts.rs"]
mod workspace_contracts;
#[path = "workspace_creation_race.rs"]
mod workspace_creation_race;
#[path = "workspace_fork_selection.rs"]
mod workspace_fork_selection;
#[path = "workspace_join_history.rs"]
mod workspace_join_history;
#[path = "workspace_materialization.rs"]
mod workspace_materialization;
#[path = "workspace_merge.rs"]
mod workspace_merge;
#[path = "workspace_rebase.rs"]
mod workspace_rebase;
#[path = "workspace_restore.rs"]
mod workspace_restore;
#[path = "workspace_simulation.rs"]
mod workspace_simulation;
#[path = "workspace_transactions.rs"]
mod workspace_transactions;
