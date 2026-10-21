//! Integration behaviour tests.
#[cfg(not(target_arch = "wasm32"))]
acyclic_test_support::counting_allocator!();

mod distributed;
mod linux_ext4_perf;
mod logical_objects;
mod macos_apfs_perf;
mod measured_creation;
mod measured_fork;
mod measured_native_source;
mod merge_lineage;
mod recovery_concurrency;
mod windows_ntfs_perf;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/projfs_enumeration.rs"]
mod projfs_enumeration;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/projfs_profile.rs"]
mod projfs_profile;

#[cfg(all(feature = "local", feature = "memory", feature = "native-mount"))]
#[path = "cases/projfs_metadata.rs"]
mod projfs_metadata;

#[path = "cases/access.rs"]
mod access;

#[path = "cases/path_properties.rs"]
mod path_properties;

#[cfg(feature = "fuzzing")]
#[path = "cases/workspace_names.rs"]
mod workspace_names;

#[cfg(feature = "memory")]
#[path = "cases/cloud_contract.rs"]
mod cloud_contract;

#[cfg(feature = "native-watch")]
#[path = "cases/native_owner_registry.rs"]
mod native_owner_registry;

#[cfg(feature = "native-mount")]
#[path = "cases/projfs_names.rs"]
mod projfs_names;

#[cfg(feature = "native-mount")]
#[path = "cases/projfs_paths.rs"]
mod projfs_paths;

#[cfg(feature = "memory")]
#[path = "cases/accounting_reconciliation.rs"]
mod accounting_reconciliation;

#[path = "cases/accounting_layout.rs"]
mod accounting_layout;

#[cfg(feature = "memory")]
#[path = "cases/workspace_reads.rs"]
mod workspace_reads;

#[cfg(feature = "native-watch")]
#[path = "cases/native_owner_notifications.rs"]
mod native_owner_notifications;

#[cfg(all(feature = "local", feature = "native-mount"))]
#[path = "cases/native_source_watch_custody.rs"]
mod native_source_watch_custody;

#[cfg(all(feature = "memory", feature = "session"))]
#[path = "cases/session_fs.rs"]
mod session_fs;

// Included production regressions use these crate-root API imports.
#[cfg(all(windows, feature = "local", feature = "native-mount"))]
use acyclic_fs::{OperationReceipt, PublicationPermit, WorkCounters, WorkspaceError, kernel};
