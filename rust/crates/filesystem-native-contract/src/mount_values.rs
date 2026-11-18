//! Shared request and node values exposed by native mounts.

use crate::{MountNodeKind, NativeMountKind};
use acyclic_fs_format::identity::{FileId, MountId, VolumeId};
use acyclic_fs_format::metadata::FileMetadata;
use std::path::PathBuf;

/// Non-destructive compile-time and host capability facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeMountCapabilities {
    /// Driver compiled for this target, if any.
    pub kind: Option<NativeMountKind>,
    /// Stable bounded reason when unavailable.
    pub unavailable_reason: Option<&'static str>,
}

impl NativeMountCapabilities {
    /// Whether the host has the required device or framework.
    pub fn available(&self) -> bool {
        self.unavailable_reason.is_none()
    }

    /// Whether the driver observes I/O issued by its provider process.
    pub fn provider_process_io_observable(&self) -> bool {
        self.available()
            && matches!(
                self.kind,
                Some(NativeMountKind::LinuxFuse | NativeMountKind::MacOsNfs)
            )
    }
}

/// One exact mount request; cross-volume routing remains in the SDK mount table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeMountRequest {
    /// Process-unique mount identity.
    pub mount_id: MountId,
    /// Projected volume.
    pub volume_id: VolumeId,
    /// Empty, caller-owned destination.
    pub destination: PathBuf,
    /// Whether authored effects may be captured.
    pub writable: bool,
    /// Lists the view in the process's shared native instance at the
    /// destination's parent, named by the destination's last component,
    /// instead of mounting it at the destination.
    pub shared: bool,
}

/// Platform-neutral node facts required by kernel callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountNode {
    /// Stable path-independent identity.
    pub file_id: FileId,
    /// Canonical node kind.
    pub kind: MountNodeKind,
    /// Logical regular-file length; zero for non-files.
    pub logical_bytes: u64,
    /// Exact namespace binding count for this path-independent file.
    pub link_count: u64,
    /// Native device identity for character/block devices only.
    pub device: Option<(u32, u32)>,
}

/// One exact mounted path result with complete authenticated metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountLookup {
    /// Stable file identity, kind, length, and link count.
    pub node: MountNode,
    /// Complete metadata; unavailable fields are never fabricated.
    pub metadata: FileMetadata,
}
