//! Links the vendored C transport into the macOS filesystem adapter.

// The filesystem adapter retires its mounts through the shared Disk Arbitration retirement.
#[cfg(target_os = "macos")]
use acyclic_darwin_mount_retire as _;

/// Native executable that owns the kernel mount beyond the SDK process.
#[cfg(target_os = "macos")]
pub static MOUNT_OWNER_IMAGE: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/darwin-mount-owner"));

#[cfg(all(target_os = "macos", any(test, feature = "test-support")))]
#[allow(unsafe_code, reason = "calls the linked DarwinFUSE transport fixture")]
mod mounted_io;

/// Reports when the native probe confirms that this host denies the process
/// data access to its own NFS mounts (the macOS network-volume privacy
/// permission). The probe mounts one real view and creates a directory in it,
/// once per process; only a permission error skips. Any other outcome runs the
/// caller, whose own assertions then report it.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn skip_if_network_volume_data_denied() -> bool {
    #[cfg(target_os = "macos")]
    if let Some(error) = mounted_io::denied() {
        eprintln!(
            "skipped: required-host-capability=macos-network-volume-data-access unavailable on this host (probe: mkdir in a mounted view: {error})"
        );
        return true;
    }
    false
}

#[cfg(all(test, target_os = "macos"))]
mod tests;
