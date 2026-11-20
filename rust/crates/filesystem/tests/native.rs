//! Native host, source and mount contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(all(not(loom), not(target_arch = "wasm32"), feature = "test-support"))]
mod hosted_sources;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
mod linux_ext4_perf;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
mod macos_apfs_perf;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod native_cut_custody;
#[cfg(all(
    not(loom),
    not(target_arch = "wasm32"),
    any(target_os = "linux", windows)
))]
mod native_watch_events;
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

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/native_owner_notifications.rs"]
#[cfg(not(loom))]
mod native_owner_notifications;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/native_source_watch_custody.rs"]
#[cfg(not(loom))]
mod native_source_watch_custody;

// The included ProjFS enumeration regression uses these crate-root API imports.
#[cfg(all(windows, not(target_arch = "wasm32")))]
#[cfg(not(loom))]
use {
    acyclic_fs::{PublicationPermit, WorkspaceError, kernel},
    acyclic_fs_work::performance::{OperationReceipt, WorkCounters},
};

#[path = "darwin_mount_owner.rs"]
mod darwin_mount_owner;
#[path = "measured_native_source.rs"]
mod native_source;
#[cfg(all(not(loom), windows, not(target_arch = "wasm32")))]
mod windows_ntfs_perf;
