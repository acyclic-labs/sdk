//! Links the vendored C transport into the macOS filesystem adapter.

// The filesystem adapter retires its mounts through the shared Disk Arbitration retirement.
#[cfg(target_os = "macos")]
use acyclic_darwin_mount_retire as _;

/// Native executable that owns the kernel mount beyond the SDK process.
#[cfg(target_os = "macos")]
pub static MOUNT_OWNER_IMAGE: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/darwin-mount-owner"));

#[cfg(all(test, target_os = "macos"))]
#[allow(unsafe_code, reason = "calls the linked DarwinFUSE transport fixture")]
mod tests;
