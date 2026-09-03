//! Native FUSE, FUSE-T, and `ProjFS` projection boundary.
//!
//! Drivers project one canonical SDK checkout. They own kernel handles and
//! callback cursors only; filesystem truth, COW state, and publication remain
//! in `acyclic-fs`.

use crate::kernel::FileMetadata;
use crate::{FileId, MountId, VolumeId};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};
use thiserror::Error;

mod adapter;
pub use adapter::{CheckoutMountSource, SharedCheckout, SharedCheckoutState};

mod routed;
pub use routed::RoutedMountSource;

mod customer;
pub use customer::{Mount, MountLifecycleError, MountOptions, MountPublication};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod device;

pub use crate::{
    CaptureError, CaptureOptions, CaptureReceipt, WatchCaptureReceipt, capture_baseline,
    capture_paths, capture_root_identity, capture_watch_batch,
};

mod materialize;
pub use materialize::{
    MaterializationReceipt, MaterializeError, MaterializeOptions, materialize_checkout,
};

mod publication;
pub use publication::seal_checkout;

mod storage_capabilities;
pub use storage_capabilities::{
    NativeStorageCapabilities, NativeStorageCapabilityError, probe_native_storage_capabilities,
};

mod storage_accelerations;
pub use storage_accelerations::{
    NativeBlockCloneAccelerationEvidence, NativeSparseAccelerationEvidence,
    NativeStorageAccelerationError, NativeStorageAccelerationEvidence,
    probe_native_storage_accelerations,
};

#[cfg(target_os = "linux")]
mod fuse;
#[cfg(target_os = "macos")]
mod fuse_t;
#[cfg(target_os = "windows")]
mod usn;
#[cfg(target_os = "windows")]
pub use usn::{
    WindowsUsnCheckpoint, WindowsUsnContinuity, WindowsUsnDiscontinuity, WindowsUsnError,
    capture_windows_usn_checkpoint, validate_windows_usn_checkpoint,
};

#[cfg(target_os = "windows")]
mod projfs;

/// Concrete namespace mechanism selected for this binary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMountKind {
    /// Linux kernel FUSE through `/dev/fuse`.
    LinuxFuse,
    /// macOS FUSE-T/libfuse session.
    MacOsFuseT,
    /// Windows Projected File System provider.
    WindowsProjFs,
}

/// Native-driver isolation required for simultaneous mount sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMountSessionIsolation {
    /// The driver safely owns multiple live sessions in one process.
    SharedProcess,
    /// Every live session requires a distinct operating-system process.
    ProcessIsolated,
}

/// Non-destructive compile-time and host capability facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeMountCapabilities {
    /// Driver compiled for this target, if any.
    pub kind: Option<NativeMountKind>,
    /// Whether a live host probe admitted the required device/framework.
    pub available: bool,
    /// Whether exact authored effects are observable on this mechanism.
    pub writable: bool,
    /// Whether I/O issued by the provider process itself is observable.
    ///
    /// `ProjFS` deliberately suppresses provider-process notifications. Embedded
    /// callers must use the SDK mutation surface for their own writes or place
    /// the provider in `fsd`; ordinary child and external processes remain
    /// fully observable through the mount.
    pub provider_process_io_observable: bool,
    /// Required process isolation for simultaneous sessions.
    pub session_isolation: NativeMountSessionIsolation,
    /// Stable bounded reason when unavailable.
    pub unavailable_reason: Option<String>,
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

/// Namespace kinds representable by native projections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountNodeKind {
    /// Regular byte stream.
    Regular,
    /// Directory.
    Directory,
    /// Symbolic link.
    SymbolicLink,
    /// POSIX named pipe.
    Fifo,
    /// POSIX socket node.
    Socket,
    /// POSIX character device.
    CharacterDevice,
    /// POSIX block device.
    BlockDevice,
    /// A canonical kind this driver cannot project safely.
    Unsupported,
}

/// Native sparse-file seek target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountSeekTarget {
    /// First allocated-zero or content byte.
    Data,
    /// First unallocated hole byte, including logical end-of-file.
    Hole,
}

/// Exact sparse-allocation operation requested by a native mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountRangeAllocation {
    /// Deallocates the range without changing logical size.
    PunchHole,
    /// Replaces the range with allocated canonical zeros.
    ZeroRange {
        /// Permit logical growth to the range end.
        extend: bool,
    },
    /// Allocates only holes while preserving all existing content.
    Preallocate {
        /// Preserve logical file length.
        keep_size: bool,
    },
}

/// One data span within a bounded sparse open-file snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountSparseSpan {
    /// Offset relative to the requested snapshot range.
    pub relative_offset: u64,
    /// Exact allocated bytes; holes are omitted.
    pub bytes: Bytes,
}

/// Bounded sparse range transferred between path-independent native handles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountSparseRange {
    /// Logical bytes represented, including omitted holes.
    pub logical_bytes: u64,
    /// Ordered, non-overlapping allocated spans.
    pub spans: Vec<MountSparseSpan>,
}

/// Path-independent regular-file handle used for every native open operation.
///
/// Attached implementations mutate the authenticated checkout by stable
/// [`FileId`], so rename and unlink of one hard-link name
/// cannot stale the handle. After final-link removal, drivers replace all
/// handles for the inode with one shared detached implementation.
pub trait MountOpenFile: Send + Sync + 'static {
    /// Returns current identity, size, and metadata for `fstat`-style calls.
    ///
    /// # Errors
    ///
    /// Returns stale, cancellation, or authenticated engine failures.
    fn lookup(&self) -> Result<MountLookup, MountSourceError>;
    /// Reads one exact bounded logical range.
    ///
    /// # Errors
    ///
    /// Returns range, storage, authentication, cancellation, or work failures.
    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError>;
    /// Finds the next sparse data or hole boundary.
    ///
    /// # Errors
    ///
    /// Returns sparse-tree, storage, authentication, cancellation, or work failures.
    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
    /// Replaces one exact byte range in the detached file.
    ///
    /// # Errors
    ///
    /// Returns range, storage, cancellation, allocation, or work failures.
    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError>;
    /// Changes the detached logical file length.
    ///
    /// # Errors
    ///
    /// Returns sparse-tree, storage, cancellation, allocation, or work failures.
    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError>;
    /// Applies one exact sparse allocation operation.
    ///
    /// # Errors
    ///
    /// Returns unsupported, range, storage, cancellation, allocation, or work failures.
    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError>;
    /// Replaces complete metadata and optionally logical size atomically from
    /// the native handle's perspective.
    ///
    /// # Errors
    ///
    /// Returns metadata, sparse-tree, storage, cancellation, allocation, or work failures.
    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError>;
    /// Reads one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns absence, unsupported-profile, storage, authentication, or work failures.
    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError>;
    /// Lists one bounded page of native named-attribute names.
    ///
    /// # Errors
    ///
    /// Returns unsupported-profile, pagination, storage, authentication, or work failures.
    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError>;
    /// Inserts or replaces one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns precondition, unsupported-profile, storage, or work failures.
    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError>;
    /// Removes one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns absence, unsupported-profile, storage, or work failures.
    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError>;

    /// Reads one bounded sparse range without materializing holes.
    ///
    /// # Errors
    ///
    /// Returns stale, range, storage, authentication, cancellation, allocation,
    /// or bounded-work failures.
    fn read_sparse_range(
        &self,
        offset: u64,
        length: u32,
    ) -> Result<MountSparseRange, MountSourceError> {
        let logical_bytes = u64::from(length);
        let end = offset.checked_add(logical_bytes).ok_or_else(|| {
            MountSourceError::Invalid("sparse range overflows the file address space".to_owned())
        })?;
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let Some(data_offset) = self.seek(cursor, MountSeekTarget::Data)? else {
                break;
            };
            if data_offset >= end {
                break;
            }
            let hole_offset = self
                .seek(data_offset, MountSeekTarget::Hole)?
                .unwrap_or(end)
                .min(end);
            if hole_offset <= data_offset {
                return Err(MountSourceError::Engine(
                    "sparse seek made no forward progress".to_owned(),
                ));
            }
            let span_length = u32::try_from(hole_offset - data_offset).map_err(|_| {
                MountSourceError::Invalid("sparse span exceeds callback bounds".to_owned())
            })?;
            let bytes = self.read_range(data_offset, span_length)?;
            if bytes.len() != usize::try_from(span_length).unwrap_or(usize::MAX) {
                return Err(MountSourceError::Stale);
            }
            spans.push(MountSparseSpan {
                relative_offset: data_offset - offset,
                bytes,
            });
            cursor = hole_offset;
        }
        Ok(MountSparseRange {
            logical_bytes,
            spans,
        })
    }

    /// Replaces one range from a sparse snapshot, preserving every hole.
    ///
    /// # Errors
    ///
    /// Returns stale, range, sparse-tree, storage, cancellation, allocation,
    /// or bounded-work failures. Native callers treat partial failures as a
    /// short copy and never report unverified completion.
    fn write_sparse_range(
        &self,
        offset: u64,
        range: &MountSparseRange,
    ) -> Result<(), MountSourceError> {
        let end = offset.checked_add(range.logical_bytes).ok_or_else(|| {
            MountSourceError::Invalid("sparse range overflows the file address space".to_owned())
        })?;
        let current = self.lookup()?.node.logical_bytes;
        if end > current {
            self.resize(end)?;
        }
        self.allocate_range(offset, range.logical_bytes, MountRangeAllocation::PunchHole)?;
        for span in &range.spans {
            let destination = offset.checked_add(span.relative_offset).ok_or_else(|| {
                MountSourceError::Invalid("sparse span offset overflows".to_owned())
            })?;
            self.write_range(destination, span.bytes.clone())?;
        }
        Ok(())
    }
}

/// One bounded directory item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountDirectoryEntry {
    /// Exact path-component bytes in the volume profile.
    pub name: Vec<u8>,
    /// Stable node facts.
    pub node: MountNode,
    /// Complete authenticated metadata for native stat/enumeration projection.
    pub metadata: FileMetadata,
}

/// One independently resumable directory-handle page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountDirectoryPage {
    /// Ordered entries after the supplied opaque cursor.
    pub entries: Vec<MountDirectoryEntry>,
    /// Opaque continuation owned by this open directory handle.
    pub next_cursor: Option<Vec<u8>>,
}

/// One bounded page of exact native named-attribute names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountAttributePage {
    /// Strictly ordered native names after the supplied cursor.
    pub names: Vec<Vec<u8>>,
    /// Exact last name when another page exists.
    pub next_cursor: Option<Vec<u8>>,
}

/// Native named-attribute write precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountAttributeWriteMode {
    /// Insert or replace.
    Upsert,
    /// Require absence.
    Create,
    /// Require presence.
    Replace,
}

/// Exact platform-native absolute path used only at the kernel callback edge.
///
/// Components contain raw Unix name bytes on FUSE/FUSE-T and little-endian
/// UTF-16 code units on `ProjFS`. The checkout adapter converts them into the
/// volume's declared logical-name representation without lossy text routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountPath {
    components: Vec<Vec<u8>>,
}

impl MountPath {
    /// Returns the volume root.
    #[must_use]
    pub const fn root() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Appends one exact platform-native component.
    #[must_use]
    pub fn child(&self, component: Vec<u8>) -> Self {
        let mut components = self.components.clone();
        components.push(component);
        Self { components }
    }

    /// Borrows exact root-to-leaf components.
    #[must_use]
    pub fn components(&self) -> &[Vec<u8>] {
        &self.components
    }
}

/// Errors returned by the canonical checkout callback bridge.
#[derive(Clone, Debug, Error)]
pub enum MountSourceError {
    /// Path is authentically absent.
    #[error("path is absent")]
    NotFound,
    /// Create-only operation found an existing object.
    #[error("filesystem object already exists")]
    AlreadyExists,
    /// Operation is invalid for the resolved kind or profile.
    #[error("invalid filesystem operation: {0}")]
    Invalid(String),
    /// Operation is not exactly observable or representable on this platform.
    #[error("filesystem operation is unsupported: {0}")]
    Unsupported(String),
    /// Canonical engine operation failed.
    #[error("filesystem engine failure: {0}")]
    Engine(String),
    /// This mount or callback was fenced or cancelled.
    #[error("mount callback is stale or cancelled")]
    Stale,
}

/// Synchronous callback surface implemented by an embedded checkout adapter.
///
/// Native kernels invoke callbacks on foreign threads, so this intentionally
/// exposes a blocking boundary. Implementations may enter an async runtime but
/// must enforce finite work and timeout limits internally.
pub trait MountFilesystem: Send + Sync + 'static {
    /// Looks up one absolute portable path without following links.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without changing checkout state.
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError>;
    /// Opens one regular file as an attached path-independent handle.
    ///
    /// # Errors
    ///
    /// Returns absence, non-regular-kind, storage, authentication,
    /// cancellation, or bounded-work failures.
    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
    /// Captures one regular file as a detached path-independent open-file view.
    ///
    /// The driver calls this immediately before removing the final namespace
    /// binding while one or more native handles remain open.
    ///
    /// # Errors
    ///
    /// Returns absence, non-regular-kind, storage, authentication,
    /// cancellation, or bounded-work failures.
    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
    /// Reads one symbolic link's exact opaque target bytes without following it.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure when the path is absent or is not a link.
    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError>;
    /// Reads at most one exact logical range.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial output.
    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError>;
    /// Finds the first sparse data or hole byte at/after one offset.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure; `None` means no matching byte exists.
    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError>;
    /// Reads one bounded page using a handle-owned opaque cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without advancing the cursor.
    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError>;
    /// Creates one empty regular file.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates one empty directory.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates one symbolic link with an exact opaque target.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates one FIFO, socket, character device, or block device.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError>;
    /// Atomically replaces complete metadata and optionally logical size.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError>;
    /// Reads one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns typed absence, unsupported-profile, or engine failures.
    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError>;
    /// Lists one bounded page of native named-attribute names.
    ///
    /// # Errors
    ///
    /// Returns a typed failure without advancing the supplied cursor.
    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError>;
    /// Inserts or replaces one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns a typed failure without partial mutation.
    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError>;
    /// Removes one exact native named attribute.
    ///
    /// # Errors
    ///
    /// Returns typed absence or engine failure without partial mutation.
    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError>;
    /// Replaces one exact file range.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError>;
    /// Changes one file's logical length.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
    /// Applies one exact sparse allocation operation.
    ///
    /// # Errors
    ///
    /// Returns a typed failure without partial mutation.
    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError>;
    /// Clones one immutable range between regular files without reading bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed failure without partial mutation.
    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError>;
    /// Clones one logical range between stable attached file identities.
    ///
    /// # Errors
    ///
    /// Returns stale identity, sparse-tree, storage, cancellation, allocation,
    /// or bounded-work failures. Detached identities are rejected.
    #[allow(clippy::too_many_arguments)]
    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError>;
    /// Removes one namespace binding.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError>;
    /// Renames one binding atomically within the volume.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError>;
    /// Creates one same-volume hard link.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError>;
    /// Seals all pending authored mutations through the checkout's configured
    /// fenced publication path before acknowledging native durability.
    ///
    /// # Errors
    ///
    /// Returns stale or engine failure when authority cannot acknowledge the
    /// exact candidate generation.
    fn flush(&self) -> Result<(), MountSourceError>;
    /// Reconciles exact final host state for one projected path and seals it.
    ///
    /// This is used by native mechanisms such as `ProjFS` that report an exact
    /// close-after-modification boundary but do not expose write byte ranges.
    ///
    /// # Errors
    ///
    /// Returns a typed capture or publication failure without acknowledging
    /// the native notification.
    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
}

/// Native mount admission and lifecycle failures.
#[derive(Debug, Error)]
pub enum NativeMountError {
    /// This target has no supported native namespace mechanism.
    #[error("native mounts are unsupported on this target")]
    UnsupportedTarget,
    /// Required host driver/device is unavailable.
    #[error("native mount capability is unavailable: {0}")]
    CapabilityUnavailable(String),
    /// Destination must already exist and be empty.
    #[error("mount destination must be an existing empty directory")]
    InvalidDestination,
    /// Another process owns this exact native destination.
    #[error("mount destination is already owned by another process")]
    DestinationBusy,
    /// Requested writable semantics cannot be captured exactly.
    #[error("writable native mount is unavailable: {0}")]
    WritableUnavailable(String),
    /// Volume name representation cannot be projected exactly on this target.
    #[error("native mount cannot represent the {profile} volume profile on {platform}")]
    ProfileUnavailable {
        /// Declared volume profile.
        profile: &'static str,
        /// Current native target.
        platform: &'static str,
    },
    /// Platform driver operation failed.
    #[error("native mount driver failed: {0}")]
    Driver(String),
}

enum DriverSession {
    #[cfg(target_os = "linux")]
    Fuse(fuse::FuseSession),
    #[cfg(target_os = "macos")]
    FuseT(fuse_t::FuseTSession),
    #[cfg(target_os = "windows")]
    ProjFs(projfs::ProjFsSession),
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    Unsupported,
}

/// Process-owned mounted namespace. Drop performs one idempotent stop.
pub struct NativeMountSession {
    mount_id: MountId,
    destination: PathBuf,
    driver: Option<DriverSession>,
    destination_guard: Option<MountDestinationGuard>,
}

impl NativeMountSession {
    /// Stable mount identity.
    #[must_use]
    pub const fn mount_id(&self) -> MountId {
        self.mount_id
    }

    /// Exact mounted destination.
    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Stops the native projection exactly once. A second call is a no-op.
    ///
    /// # Errors
    ///
    /// Returns a platform error if the kernel namespace cannot be detached.
    /// Drops the kernel's cached entry/attributes for one mount-relative
    /// path (leading `/` optional), making a projection change — such as a
    /// removed route — visible immediately instead of after a cache timeout.
    ///
    /// # Errors
    ///
    /// Returns a driver error when the transport cannot invalidate (the
    /// change then becomes visible at the cache's own expiry).
    pub fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        match &self.driver {
            #[cfg(target_os = "macos")]
            Some(DriverSession::FuseT(session)) => session.invalidate(path),
            #[cfg(not(target_os = "macos"))]
            Some(_) => {
                let _ = path;
                Err(NativeMountError::Driver(
                    "invalidation is not implemented for this transport".to_owned(),
                ))
            }
            None => Err(NativeMountError::Driver("session is stopped".to_owned())),
        }
    }

    /// Detaches the kernel namespace and ends the driver session; returns
    /// whether a live session was actually stopped.
    ///
    /// # Errors
    ///
    /// Returns a platform error if the kernel namespace cannot be detached.
    pub fn stop(&mut self) -> Result<bool, NativeMountError> {
        stop_owned_session(
            &mut self.driver,
            &mut self.destination_guard,
            stop_driver,
            MountDestinationGuard::release,
        )
    }
}

fn stop_owned_session<D, G, E>(
    driver: &mut Option<D>,
    destination_guard: &mut Option<G>,
    mut stop: impl FnMut(&mut D) -> Result<(), E>,
    mut release: impl FnMut(&mut G) -> Result<(), E>,
) -> Result<bool, E> {
    if driver.is_none() && destination_guard.is_none() {
        return Ok(false);
    }
    if let Some(active) = driver.as_mut() {
        stop(active)?;
        driver.take();
    }
    if let Some(guard) = destination_guard.as_mut() {
        release(guard)?;
        destination_guard.take();
    }
    Ok(true)
}

impl Drop for NativeMountSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Probes the target's one canonical native namespace mechanism.
#[must_use]
pub fn probe_native_mount() -> NativeMountCapabilities {
    platform::probe()
}

/// Starts a real native projection over the supplied canonical checkout.
///
/// # Errors
///
/// Rejects absent capabilities, non-empty destinations, unsupported writable
/// capture, or any platform-driver startup failure.
pub fn mount_native(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<NativeMountSession, NativeMountError> {
    let capabilities = probe_native_mount();
    if !capabilities.available {
        return Err(NativeMountError::CapabilityUnavailable(
            capabilities
                .unavailable_reason
                .unwrap_or_else(|| "unavailable".to_owned()),
        ));
    }
    if request.writable && !capabilities.writable {
        return Err(NativeMountError::WritableUnavailable(
            "the selected kernel API cannot prove exact authored effects".to_owned(),
        ));
    }
    let (driver, destination_guard) =
        start_owned_session(&request.destination, || start_driver(&request, source))?;
    Ok(NativeMountSession {
        mount_id: request.mount_id,
        destination: request.destination,
        driver: Some(driver),
        destination_guard: Some(destination_guard),
    })
}

/// Reclaims the process fence for one externally detached destination.
///
/// This is the supervisor-side crash-recovery boundary. A live owner keeps the
/// fence locked and causes [`NativeMountError::DestinationBusy`]; an unlocked
/// crash-left fence is acquired and removed under the parent registry lock.
/// Reclaiming an already-removed fence succeeds idempotently.
/// The caller remains responsible for proving that the kernel namespace has
/// already detached before invoking this operation.
///
/// # Errors
///
/// Rejects a missing or non-canonical destination, a live owner, unsafe fence
/// metadata, or a failure to remove the reclaimed fence.
pub fn reclaim_native_mount_destination_fence(destination: &Path) -> Result<(), NativeMountError> {
    let canonical = destination
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    MountDestinationGuard::reclaim_canonical(&canonical)
}

/// Reclaims every unlocked native-mount destination fence in one parent.
///
/// The parent registry is held for the complete bounded scan, so a destination
/// cannot be admitted while its persistent fence is being classified or
/// removed. Fences held by live providers remain untouched. Malformed reserved
/// names, links, special files, or an excessive directory fail closed.
///
/// # Errors
///
/// Returns a typed destination or driver error when the parent is invalid, the
/// reserved fence namespace is unsafe, or the bounded scan cannot complete.
pub fn reclaim_stale_native_mount_destination_fences(
    parent: &Path,
) -> Result<u32, NativeMountError> {
    const MAXIMUM_PARENT_ENTRIES: usize = 4_096;
    const PREFIX: &str = ".acyclic-fs-mount-";
    const SUFFIX: &str = ".lock";

    let parent = parent
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    let metadata =
        std::fs::symlink_metadata(&parent).map_err(|_| NativeMountError::InvalidDestination)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    let registry_path = parent.join(".acyclic-fs-mount-registry.lock");
    let registry = acquire_mount_lock(&registry_path, true)?;
    let mut reclaimed = 0_u32;
    for (index, entry) in std::fs::read_dir(&parent)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?
        .enumerate()
    {
        if index >= MAXIMUM_PARENT_ENTRIES {
            return Err(NativeMountError::Driver(
                "native mount parent exceeds its bounded entry count".to_owned(),
            ));
        }
        let entry = entry.map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == ".acyclic-fs-mount-registry.lock" || !name.starts_with(PREFIX) {
            continue;
        }
        let digest = name
            .strip_prefix(PREFIX)
            .and_then(|value| value.strip_suffix(SUFFIX))
            .ok_or(NativeMountError::InvalidDestination)?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(NativeMountError::InvalidDestination);
        }
        let path = entry.path();
        let lock = match acquire_existing_mount_lock(&path) {
            Ok(lock) => lock,
            Err(NativeMountError::DestinationBusy) => continue,
            Err(error) => return Err(error),
        };
        drop(lock);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                reclaimed = reclaimed.checked_add(1).ok_or_else(|| {
                    NativeMountError::Driver("native mount fence count overflow".to_owned())
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NativeMountError::Driver(error.to_string())),
        }
    }
    drop(registry);
    Ok(reclaimed)
}

/// Recovers one implementation-owned disposable native destination after its
/// provider process dies.
///
/// The exact destination fence is acquired before any detach attempt, so a
/// surviving owner is never unmounted. Recovery then performs the platform's
/// bounded detach, removes only the now-exposed destination directory, and
/// releases the persistent fence under the parent registry lock. A nonempty
/// ordinary directory is retained and rejected rather than recursively erased.
///
/// # Errors
///
/// Returns [`NativeMountError::DestinationBusy`] for a live owner and a typed
/// driver or destination failure if detach, bounded verification, directory
/// removal, or fence release cannot complete exactly.
pub fn recover_native_mount_destination(destination: &Path) -> Result<(), NativeMountError> {
    let mut guard = MountDestinationGuard::acquire_for_recovery(destination)?;
    recover_platform_destination(destination).map_err(|error| {
        NativeMountError::Driver(format!("native platform recovery failed: {error}"))
    })?;
    match std::fs::remove_dir(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(NativeMountError::Driver(format!(
                "native recovered destination removal failed for {}: {error}",
                destination.display()
            )));
        }
    }
    guard.release()
}

/// Detaches and unfences one crash-left native destination while retaining its
/// underlying directory and authored host residue for an explicit caller
/// decision.
///
/// This is the daemon restart boundary: a live owner is rejected before any
/// detach, while a dead owner's kernel projection and persistent path fence are
/// removed. Call [`recover_native_mount_destination`] when the destination
/// itself is an implementation-owned disposable root.
///
/// # Errors
///
/// Returns a typed busy, detach, verification, or fence-release failure.
pub fn detach_native_mount_destination_after_crash(
    destination: &Path,
) -> Result<(), NativeMountError> {
    let mut guard = MountDestinationGuard::acquire_for_recovery(destination)?;
    detach_platform_destination_after_crash(destination)?;
    guard.release()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn detach_platform_destination_after_crash(destination: &Path) -> Result<(), NativeMountError> {
    recover_platform_destination(destination)
}

#[cfg(target_os = "windows")]
#[allow(clippy::unnecessary_wraps)]
fn detach_platform_destination_after_crash(_destination: &Path) -> Result<(), NativeMountError> {
    // ProjFS binds the virtualization context to its provider process. Process
    // termination detaches that context, while the authenticated local cache
    // and any authored residue remain at the virtualization root.
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn detach_platform_destination_after_crash(_destination: &Path) -> Result<(), NativeMountError> {
    Err(NativeMountError::UnsupportedTarget)
}

fn start_owned_session<D>(
    destination: &Path,
    start: impl FnOnce() -> Result<D, NativeMountError>,
) -> Result<(D, MountDestinationGuard), NativeMountError> {
    let destination_guard = admit_destination(destination)?;
    let driver = start()?;
    Ok((driver, destination_guard))
}

fn admit_destination(destination: &Path) -> Result<MountDestinationGuard, NativeMountError> {
    let guard = MountDestinationGuard::acquire(destination)?;
    validate_destination(destination)?;
    Ok(guard)
}

#[derive(Debug)]
struct MountDestinationGuard {
    file: Option<File>,
    lock_path: PathBuf,
    registry_path: PathBuf,
}

impl MountDestinationGuard {
    fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let canonical = destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?;
        Self::acquire_canonical(&canonical)
    }

    fn acquire_for_recovery(destination: &Path) -> Result<Self, NativeMountError> {
        let name = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent = destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?;
        Self::acquire_canonical(&parent.join(name))
    }

    fn acquire_canonical(canonical: &Path) -> Result<Self, NativeMountError> {
        let (lock_path, registry_path) = Self::paths_for_canonical(canonical)?;
        let registry = acquire_mount_lock(&registry_path, true)?;
        let file = acquire_mount_lock(&lock_path, false)?;
        drop(registry);
        Ok(Self {
            file: Some(file),
            lock_path,
            registry_path,
        })
    }

    fn paths_for_canonical(canonical: &Path) -> Result<(PathBuf, PathBuf), NativeMountError> {
        let parent = canonical
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic-fs/native-mount-destination/v1\0");
        update_path_identity(&mut identity, canonical);
        let lock_path = parent.join(format!(
            ".acyclic-fs-mount-{}.lock",
            identity.finalize().to_hex()
        ));
        let registry_path = parent.join(".acyclic-fs-mount-registry.lock");
        Ok((lock_path, registry_path))
    }

    fn reclaim_canonical(canonical: &Path) -> Result<(), NativeMountError> {
        let (lock_path, registry_path) = Self::paths_for_canonical(canonical)?;
        let registry = acquire_mount_lock(&registry_path, true)?;
        if !lock_path
            .try_exists()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?
        {
            return Ok(());
        }
        let file = acquire_existing_mount_lock(&lock_path)?;
        drop(file);
        match std::fs::remove_file(&lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NativeMountError::Driver(error.to_string())),
        }
        drop(registry);
        Ok(())
    }

    fn release(&mut self) -> Result<(), NativeMountError> {
        let registry = acquire_mount_lock(&self.registry_path, true)?;
        self.file.take();
        match std::fs::remove_file(&self.lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NativeMountError::Driver(error.to_string())),
        }
        drop(registry);
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn recover_platform_destination(destination: &Path) -> Result<(), NativeMountError> {
    use std::process::{Command, Stdio};

    if !linux_destination_is_mounted(destination)? {
        return Ok(());
    }
    let helper = [
        "/usr/bin/fusermount3",
        "/bin/fusermount3",
        "/usr/bin/fusermount",
        "/bin/fusermount",
    ]
    .into_iter()
    .find(|candidate| Path::new(candidate).is_file())
    .ok_or_else(|| {
        NativeMountError::CapabilityUnavailable(
            "no canonical fusermount helper is available for crash recovery".to_owned(),
        )
    })?;
    let mut child = Command::new(helper)
        .args(["-u", "-q", "-z", "--"])
        .arg(destination)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if child
            .try_wait()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?
            .is_some()
        {
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(NativeMountError::Driver(
                "fusermount crash recovery exceeded 30 seconds".to_owned(),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if linux_destination_is_mounted(destination)? {
        return Err(NativeMountError::Driver(
            "FUSE destination remained mounted after crash recovery".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_destination_is_mounted(destination: &Path) -> Result<bool, NativeMountError> {
    use std::os::unix::fs::MetadataExt;

    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?
        .metadata()
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    match destination.metadata() {
        Ok(metadata) => Ok(metadata.dev() != parent.dev()),
        Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(NativeMountError::Driver(error.to_string())),
    }
}

#[cfg(target_os = "macos")]
fn recover_platform_destination(destination: &Path) -> Result<(), NativeMountError> {
    fuse_t::recover_destination(destination)
}

#[cfg(target_os = "windows")]
fn recover_platform_destination(destination: &Path) -> Result<(), NativeMountError> {
    projfs::recover_cache_only_destination(destination)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn recover_platform_destination(_destination: &Path) -> Result<(), NativeMountError> {
    Err(NativeMountError::UnsupportedTarget)
}

impl Drop for MountDestinationGuard {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

fn acquire_mount_lock(path: &Path, blocking: bool) -> Result<File, NativeMountError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let lock_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    if lock_metadata.file_type().is_symlink() || !file.metadata().is_ok_and(|value| value.is_file())
    {
        return Err(NativeMountError::InvalidDestination);
    }
    if blocking {
        fs2::FileExt::lock_exclusive(&file)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    } else {
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            if lock_is_contended(&error) {
                NativeMountError::DestinationBusy
            } else {
                NativeMountError::Driver(error.to_string())
            }
        })?;
    }
    Ok(file)
}

fn acquire_existing_mount_lock(path: &Path) -> Result<File, NativeMountError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let lock_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    if lock_metadata.file_type().is_symlink() || !file.metadata().is_ok_and(|value| value.is_file())
    {
        return Err(NativeMountError::InvalidDestination);
    }
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        if lock_is_contended(&error) {
            NativeMountError::DestinationBusy
        } else {
            NativeMountError::Driver(error.to_string())
        }
    })?;
    Ok(file)
}

fn lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    {
        // LockFileEx reports ERROR_LOCK_VIOLATION rather than WouldBlock.
        error.raw_os_error() == Some(33)
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[cfg(unix)]
fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    hasher.update(path.as_os_str().as_bytes());
}

#[cfg(windows)]
fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::windows::ffi::OsStrExt;
    for unit in path.as_os_str().encode_wide() {
        hasher.update(&unit.to_le_bytes());
    }
}

#[cfg(not(any(unix, windows)))]
fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    hasher.update(path.to_string_lossy().as_bytes());
}

fn validate_destination(destination: &Path) -> Result<(), NativeMountError> {
    let metadata =
        std::fs::symlink_metadata(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    let mut entries =
        std::fs::read_dir(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if entries.next().is_some() {
        return Err(NativeMountError::InvalidDestination);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, NativeMountError> {
    fuse::FuseSession::start(request, source).map(DriverSession::Fuse)
}

#[cfg(target_os = "macos")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, NativeMountError> {
    fuse_t::FuseTSession::start(request, source).map(DriverSession::FuseT)
}

#[cfg(target_os = "windows")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, NativeMountError> {
    projfs::ProjFsSession::start(request, source).map(DriverSession::ProjFs)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_driver(
    _request: &NativeMountRequest,
    _source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, NativeMountError> {
    Err(NativeMountError::UnsupportedTarget)
}

fn stop_driver(driver: &mut DriverSession) -> Result<(), NativeMountError> {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.stop(),
        #[cfg(target_os = "macos")]
        DriverSession::FuseT(session) => session.stop(),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.stop(),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => Err(NativeMountError::UnsupportedTarget),
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{NativeMountCapabilities, NativeMountKind, NativeMountSessionIsolation};

    pub(super) fn probe() -> NativeMountCapabilities {
        let available = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .is_ok();
        NativeMountCapabilities {
            kind: Some(NativeMountKind::LinuxFuse),
            available,
            writable: available,
            provider_process_io_observable: available,
            session_isolation: NativeMountSessionIsolation::SharedProcess,
            unavailable_reason: (!available).then(|| "/dev/fuse is not usable".to_owned()),
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{NativeMountCapabilities, NativeMountKind, NativeMountSessionIsolation};
    use std::os::unix::fs::PermissionsExt;

    const FUSE_T_RUNTIME: &str = "/usr/local/bin/go-nfsv4";
    const FUSE_T_LIBRARY: &str = "/usr/local/lib/libfuse-t.dylib";

    fn executable_file(path: &str) -> bool {
        std::fs::metadata(path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
    }

    pub(super) fn probe() -> NativeMountCapabilities {
        let available =
            executable_file(FUSE_T_RUNTIME) && std::path::Path::new(FUSE_T_LIBRARY).is_file();
        NativeMountCapabilities {
            kind: Some(NativeMountKind::MacOsFuseT),
            available,
            writable: available,
            provider_process_io_observable: available,
            session_isolation: NativeMountSessionIsolation::ProcessIsolated,
            unavailable_reason: (!available)
                .then(|| "FUSE-T runtime or compatibility library is unavailable".to_owned()),
        }
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use super::{NativeMountCapabilities, NativeMountKind, NativeMountSessionIsolation};

    pub(super) fn probe() -> NativeMountCapabilities {
        let system_root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
        let available = std::path::Path::new(&system_root)
            .join("System32")
            .join("ProjectedFSLib.dll")
            .is_file();
        NativeMountCapabilities {
            kind: Some(NativeMountKind::WindowsProjFs),
            available,
            writable: available,
            provider_process_io_observable: false,
            session_isolation: NativeMountSessionIsolation::SharedProcess,
            unavailable_reason: (!available)
                .then(|| "ProjectedFSLib.dll is unavailable".to_owned()),
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use super::{NativeMountCapabilities, NativeMountSessionIsolation};

    pub(super) fn probe() -> NativeMountCapabilities {
        NativeMountCapabilities {
            kind: None,
            available: false,
            writable: false,
            provider_process_io_observable: false,
            session_isolation: NativeMountSessionIsolation::ProcessIsolated,
            unavailable_reason: Some("unsupported target".to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{
        AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
        FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
        VolumeConfig, VolumeLimits,
    };
    use crate::path::PortablePath;
    use crate::{
        ByteRange, CancellationToken, Fs, VolumeId, WatchBatch, WatchChange, WatchEpoch,
        WatchSequence, WorkBudget,
    };
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    fn portable_path(
        limits: VolumeLimits,
        components: &[&[u8]],
    ) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::new(
            components
                .iter()
                .map(|name| {
                    LogicalName::new(
                        NameEncoding::Utf8,
                        name.to_vec(),
                        limits.maximum_component_bytes,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            limits,
        )?)
    }

    #[test]
    fn probe_names_exactly_one_target_driver() {
        let probe = probe_native_mount();
        assert!(probe.kind.is_some());
        assert_eq!(probe.available, probe.unavailable_reason.is_none());
    }

    #[test]
    fn destination_admission_rejects_nonempty_directories() {
        let root =
            std::env::temp_dir().join(format!("acyclic-fs-mount-admission-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        assert!(std::fs::create_dir_all(&root).is_ok());
        assert!(std::fs::write(root.join("occupied"), b"x").is_ok());
        assert!(matches!(
            validate_destination(&root),
            Err(NativeMountError::InvalidDestination)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn crash_recovery_never_removes_live_or_contaminated_destinations()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("native-destination");
        std::fs::create_dir(&destination)?;

        let live_guard = MountDestinationGuard::acquire(&destination)?;
        assert!(matches!(
            recover_native_mount_destination(&destination),
            Err(NativeMountError::DestinationBusy)
        ));
        assert!(destination.is_dir());
        drop(live_guard);

        std::fs::write(destination.join("unowned-content"), b"retain")?;
        assert!(matches!(
            recover_native_mount_destination(&destination),
            Err(NativeMountError::Driver(_))
        ));
        assert_eq!(
            std::fs::read(destination.join("unowned-content"))?,
            b"retain"
        );
        std::fs::remove_file(destination.join("unowned-content"))?;

        recover_native_mount_destination(&destination)?;
        assert!(!destination.exists());
        let residue = std::fs::read_dir(parent.path())?
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name != ".acyclic-fs-mount-registry.lock")
            .collect::<Vec<_>>();
        assert!(residue.is_empty());
        Ok(())
    }

    #[test]
    fn failed_destination_admission_releases_ownership() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("occupied"), b"x")?;
        assert!(matches!(
            admit_destination(&destination),
            Err(NativeMountError::InvalidDestination)
        ));
        std::fs::remove_file(destination.join("occupied"))?;
        let _recovered = admit_destination(&destination)?;
        Ok(())
    }

    #[test]
    fn failed_driver_start_releases_destination_ownership() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;

        let failed = start_owned_session::<()>(&destination, || {
            Err(NativeMountError::Driver(
                "injected startup failure".to_owned(),
            ))
        });
        assert!(matches!(failed, Err(NativeMountError::Driver(_))));
        let recovered_after_error = MountDestinationGuard::acquire(&destination)?;
        drop(recovered_after_error);

        Ok(())
    }

    #[test]
    fn destination_ownership_is_process_external_and_recoverable()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let first = MountDestinationGuard::acquire(&destination)?;
        let second = MountDestinationGuard::acquire(&destination);
        assert!(
            matches!(second, Err(NativeMountError::DestinationBusy)),
            "unexpected second-owner result: {second:?}"
        );
        let independent = temporary.path().join("independent");
        std::fs::create_dir(&independent)?;
        let _independent = MountDestinationGuard::acquire(&independent)?;
        drop(first);
        let _recovered = MountDestinationGuard::acquire(&destination)?;
        Ok(())
    }

    #[test]
    fn orderly_destination_release_removes_per_mount_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let guard = MountDestinationGuard::acquire(&destination)?;
        drop(guard);

        let mut residue = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|value| value.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        residue.sort();
        assert_eq!(
            residue,
            vec![
                std::ffi::OsString::from(".acyclic-fs-mount-registry.lock"),
                std::ffi::OsString::from("mount"),
            ]
        );
        Ok(())
    }

    #[test]
    fn repeated_admission_teardown_has_constant_persistent_resource_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destinations = [
            temporary.path().join("mount-a"),
            temporary.path().join("mount-b"),
        ];
        for destination in &destinations {
            std::fs::create_dir(destination)?;
        }
        for ordinal in 0..1_024 {
            let guard = MountDestinationGuard::acquire(&destinations[ordinal % 2])?;
            drop(guard);
        }
        let mut residue = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|value| value.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        residue.sort();
        assert_eq!(
            residue,
            vec![
                std::ffi::OsString::from(".acyclic-fs-mount-registry.lock"),
                std::ffi::OsString::from("mount-a"),
                std::ffi::OsString::from("mount-b"),
            ]
        );
        for destination in destinations {
            std::fs::remove_dir(destination)?;
        }
        Ok(())
    }

    #[test]
    fn supervisor_reclaims_crash_left_fence_but_never_a_live_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mut crashed = MountDestinationGuard::acquire(&destination)?;
        assert!(matches!(
            reclaim_native_mount_destination_fence(&destination),
            Err(NativeMountError::DestinationBusy)
        ));
        let crash_left_path = crashed.lock_path.clone();
        crashed.file.take();
        std::mem::forget(crashed);
        assert!(crash_left_path.is_file());

        reclaim_native_mount_destination_fence(&destination)?;
        assert!(!crash_left_path.exists());
        reclaim_native_mount_destination_fence(&destination)?;
        Ok(())
    }

    #[test]
    fn bounded_parent_sweep_reclaims_stale_fences_and_preserves_live_owners()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let live_destination = temporary.path().join("live");
        let stale_destination = temporary.path().join("stale");
        std::fs::create_dir(&live_destination)?;
        std::fs::create_dir(&stale_destination)?;
        let live = MountDestinationGuard::acquire(&live_destination)?;
        let mut stale = MountDestinationGuard::acquire(&stale_destination)?;
        let live_path = live.lock_path.clone();
        let stale_path = stale.lock_path.clone();
        stale.file.take();
        std::mem::forget(stale);

        assert_eq!(
            reclaim_stale_native_mount_destination_fences(temporary.path())?,
            1
        );
        assert!(live_path.is_file());
        assert!(!stale_path.exists());
        drop(live);
        assert_eq!(
            reclaim_stale_native_mount_destination_fences(temporary.path())?,
            0
        );
        Ok(())
    }

    #[test]
    fn parent_sweep_rejects_malformed_reserved_fence_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join(".acyclic-fs-mount-invalid.lock"), b"")?;
        assert!(matches!(
            reclaim_stale_native_mount_destination_fences(temporary.path()),
            Err(NativeMountError::InvalidDestination)
        ));
        Ok(())
    }

    #[test]
    fn destination_reclaim_child_waits_for_external_release()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(destination) = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION") else {
            return Ok(());
        };
        let ready = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_READY")
            .ok_or("reclaim child readiness path is absent")?;
        let release = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_RELEASE")
            .ok_or("reclaim child release path is absent")?;
        std::fs::write(ready, b"ready")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !Path::new(&release).is_file() {
            if Instant::now() >= deadline {
                return Err("reclaim child release timed out".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        reclaim_native_mount_destination_fence(Path::new(&destination))?;
        Ok(())
    }

    #[test]
    fn independent_supervisors_concurrently_reclaim_one_crash_left_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        struct KillChildren(Vec<std::process::Child>);

        impl Drop for KillChildren {
            fn drop(&mut self) {
                for child in &mut self.0 {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }

        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        let release = temporary.path().join("release");
        std::fs::create_dir(&destination)?;
        let mut crashed = MountDestinationGuard::acquire(&destination)?;
        let crash_left_path = crashed.lock_path.clone();
        crashed.file.take();
        std::mem::forget(crashed);

        let executable = std::env::current_exe()?;
        let mut children = KillChildren(Vec::new());
        let mut readiness = Vec::new();
        for ordinal in 0..2 {
            let ready = temporary.path().join(format!("ready-{ordinal}"));
            let child = std::process::Command::new(&executable)
                .args([
                    "--exact",
                    "native_mount::tests::destination_reclaim_child_waits_for_external_release",
                    "--nocapture",
                ])
                .env("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION", &destination)
                .env("ACYCLIC_FS_RECLAIM_CHILD_READY", &ready)
                .env("ACYCLIC_FS_RECLAIM_CHILD_RELEASE", &release)
                .spawn()?;
            readiness.push(ready);
            children.0.push(child);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while readiness.iter().any(|path| !path.is_file()) {
            for child in &mut children.0 {
                if let Some(status) = child.try_wait()? {
                    return Err(format!("reclaim child exited before release: {status}").into());
                }
            }
            if Instant::now() >= deadline {
                return Err("reclaim children did not become ready".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        std::fs::write(&release, b"release")?;
        for child in &mut children.0 {
            let status = child.wait()?;
            if !status.success() {
                return Err(format!("reclaim child failed: {status}").into());
            }
        }
        children.0.clear();
        assert!(!crash_left_path.exists());
        assert!(destination.is_dir());
        Ok(())
    }

    #[test]
    fn destination_ownership_child_holds_lock_until_killed()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(destination) = std::env::var_os("ACYCLIC_FS_LOCK_CHILD_DESTINATION") else {
            return Ok(());
        };
        let ready = std::env::var_os("ACYCLIC_FS_LOCK_CHILD_READY")
            .ok_or("lock child readiness path is absent")?;
        let _guard = MountDestinationGuard::acquire(Path::new(&destination))?;
        std::fs::write(ready, b"ready")?;
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn destination_ownership_is_reclaimed_after_process_death()
    -> Result<(), Box<dyn std::error::Error>> {
        struct KillOnDrop(std::process::Child);

        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        let ready = temporary.path().join("ready");
        std::fs::create_dir(&destination)?;
        let child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "native_mount::tests::destination_ownership_child_holds_lock_until_killed",
                "--nocapture",
            ])
            .env("ACYCLIC_FS_LOCK_CHILD_DESTINATION", &destination)
            .env("ACYCLIC_FS_LOCK_CHILD_READY", &ready)
            .spawn()?;
        let mut child = KillOnDrop(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.is_file() {
            if let Some(status) = child.0.try_wait()? {
                return Err(format!("lock child exited before readiness: {status}").into());
            }
            if Instant::now() >= deadline {
                return Err("lock child readiness timed out".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(
            MountDestinationGuard::acquire(&destination),
            Err(NativeMountError::DestinationBusy)
        ));
        child.0.kill()?;
        let status = child.0.wait()?;
        assert!(
            !status.success(),
            "killed lock child unexpectedly succeeded"
        );
        let _reclaimed = MountDestinationGuard::acquire(&destination)?;
        Ok(())
    }

    #[test]
    fn failed_stop_retains_driver_and_destination_ownership_for_exact_retry() {
        let mut driver = Some(7_u8);
        let mut destination_guard = Some(11_u8);
        let failure = stop_owned_session(
            &mut driver,
            &mut destination_guard,
            |_| Err("busy"),
            |_| Ok(()),
        );
        assert_eq!(failure, Err("busy"));
        assert_eq!(driver, Some(7));
        assert_eq!(destination_guard, Some(11));

        let stopped = stop_owned_session(
            &mut driver,
            &mut destination_guard,
            |_| Ok::<_, &str>(()),
            |_| Ok(()),
        );
        assert_eq!(stopped, Ok(true));
        assert_eq!(driver, None);
        assert_eq!(destination_guard, None);
        assert_eq!(
            stop_owned_session(
                &mut driver,
                &mut destination_guard,
                |_| Ok::<_, &str>(()),
                |_| Ok(()),
            ),
            Ok(false)
        );
    }

    #[test]
    fn failed_fence_release_is_retried_without_restarting_the_driver() {
        let mut driver = Some(7_u8);
        let mut destination_guard = Some(11_u8);
        let mut first_release = true;
        let failure = stop_owned_session(
            &mut driver,
            &mut destination_guard,
            |_| Ok::<_, &str>(()),
            |_| {
                if first_release {
                    first_release = false;
                    Err("registry busy")
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(failure, Err("registry busy"));
        assert_eq!(driver, None);
        assert_eq!(destination_guard, Some(11));

        let stopped = stop_owned_session(
            &mut driver,
            &mut destination_guard,
            |_| Err("driver must not restart"),
            |_| Ok(()),
        );
        assert_eq!(stopped, Ok(true));
        assert_eq!(destination_guard, None);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn explicit_materialization_and_capture_round_trip_sparse_checkout()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let config = VolumeConfig {
            profile: FilesystemProfile::Portable,
            concurrency: ConcurrencyMode::Optimistic,
            lifecycle: Lifecycle::Ephemeral,
            case_sensitivity: CaseSensitivity::Sensitive,
            unicode: UnicodePolicy::Preserve,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
            limits,
        };
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume_with_id(
                VolumeId::from_bytes([91; 16]),
                config,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let file = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::Utf8,
                b"sparse.bin".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        checkout
            .create_file(
                file.clone(),
                Bytes::from_static(b"head"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        checkout
            .resize_file(
                file.clone(),
                1024 * 1024,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        checkout
            .write_file(
                file.clone(),
                1024 * 1024 - 4,
                Bytes::from_static(b"tail"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;

        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("view");
        std::fs::create_dir(&destination)?;
        let materialized = Box::pin(materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?;
        assert_eq!(materialized.value.files, 1);
        let host_file = destination.join("sparse.bin");
        assert_eq!(std::fs::metadata(&host_file)?.len(), 1024 * 1024);
        let body = std::fs::read(&host_file)?;
        assert_eq!(&body[..4], b"head");
        assert_eq!(&body[body.len() - 4..], b"tail");

        let sparse_capture = capture_paths(
            &mut checkout,
            std::slice::from_ref(&file),
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert!(sparse_capture.value.staged_file_bytes < 1024 * 1024);
        let sparse_plan = checkout
            .plan_file_extents(
                &file,
                ByteRange {
                    offset: 0,
                    length: 1024 * 1024,
                },
                16,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .ok_or("captured sparse file returned an inline plan")?;
        assert!(
            sparse_plan
                .spans
                .iter()
                .any(|span| matches!(span.kind, crate::kernel::ExtentKind::Hole))
        );

        std::fs::write(&host_file, b"captured")?;
        let captured = capture_paths(
            &mut checkout,
            std::slice::from_ref(&file),
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(captured.value.changed_paths, 1);
        let read = checkout
            .read_file_range(
                &file,
                ByteRange {
                    offset: 0,
                    length: 8,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(read.value.bytes.as_ref(), b"captured");

        let before_rename = checkout
            .lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("captured file disappeared")?;
        let renamed = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::Utf8,
                b"renamed.bin".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        std::fs::rename(&host_file, destination.join("renamed.bin"))?;
        let watch = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(7),
                first_sequence: WatchSequence::from_u64(11),
                next_sequence: WatchSequence::from_u64(12),
                changes: vec![WatchChange::Renamed {
                    from: file.clone(),
                    to: renamed.clone(),
                }],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(watch.value.next_sequence.get(), 12);
        let old = checkout
            .lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert!(old.value.record.is_none());
        let new = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("renamed file disappeared")?;
        assert_eq!(new.file_id, before_rename.file_id);

        let moved = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::Utf8,
                b"moved.bin".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        std::fs::rename(
            destination.join("renamed.bin"),
            destination.join("moved.bin"),
        )?;
        std::fs::write(destination.join("renamed.bin"), b"replacement")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(7),
                first_sequence: WatchSequence::from_u64(12),
                next_sequence: WatchSequence::from_u64(14),
                changes: vec![
                    WatchChange::Renamed {
                        from: renamed.clone(),
                        to: moved.clone(),
                    },
                    WatchChange::Created(renamed.clone()),
                ],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let moved_record = checkout
            .lookup_no_follow(&moved, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("moved file disappeared")?;
        let replacement_record = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("replacement file disappeared")?;
        assert_eq!(moved_record.file_id, before_rename.file_id);
        assert_ne!(replacement_record.file_id, before_rename.file_id);
        let replacement = checkout
            .read_file_range(
                &renamed,
                ByteRange {
                    offset: 0,
                    length: 11,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(replacement.value.bytes.as_ref(), b"replacement");

        std::fs::write(destination.join("renamed.bin"), b"replaced-again")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(7),
                first_sequence: WatchSequence::from_u64(14),
                next_sequence: WatchSequence::from_u64(15),
                changes: vec![WatchChange::Created(renamed.clone())],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let replaced_again = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("standalone create replacement disappeared")?;
        assert_ne!(replaced_again.file_id, replacement_record.file_id);

        std::fs::write(destination.join("renamed.bin"), b"compound-replacement")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(7),
                first_sequence: WatchSequence::from_u64(15),
                next_sequence: WatchSequence::from_u64(17),
                changes: vec![
                    WatchChange::Created(renamed.clone()),
                    WatchChange::Modified(renamed.clone()),
                ],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let compound_replacement = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("compound replacement disappeared")?;
        assert_ne!(compound_replacement.file_id, replaced_again.file_id);

        let dirty_baseline = capture_baseline(
            &mut checkout,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 16,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        let Err(dirty_baseline) = dirty_baseline else {
            return Err("dirty baseline replaced private mutations".into());
        };
        assert!(matches!(dirty_baseline.error, CaptureError::DirtyCheckout));

        seal_checkout(
            &mut checkout,
            crate::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

        std::fs::remove_file(destination.join("renamed.bin"))?;
        std::fs::remove_file(destination.join("moved.bin"))?;
        std::fs::create_dir(destination.join("nested"))?;
        std::fs::write(destination.join("nested").join("new.txt"), b"baseline")?;
        let baseline = capture_baseline(
            &mut checkout,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 16,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(baseline.value.examined_paths, 4);
        let removed = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert!(removed.value.record.is_none());
        let removed_moved = checkout
            .lookup_no_follow(&moved, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert!(removed_moved.value.record.is_none());
        let nested = PortablePath::parse("/nested/new.txt", limits)?;
        let nested = NamespacePath::from_portable(&nested, limits)?;
        let nested_read = checkout
            .read_file_range(
                &nested,
                ByteRange {
                    offset: 0,
                    length: 8,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(nested_read.value.bytes.as_ref(), b"baseline");

        let missing =
            NamespacePath::from_portable(&PortablePath::parse("/missing.tmp", limits)?, limits)?;
        let arrived =
            NamespacePath::from_portable(&PortablePath::parse("/arrived.txt", limits)?, limits)?;
        std::fs::write(destination.join("arrived.txt"), b"arrival")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(8),
                first_sequence: WatchSequence::from_u64(0),
                next_sequence: WatchSequence::from_u64(1),
                changes: vec![WatchChange::Renamed {
                    from: missing,
                    to: arrived.clone(),
                }],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(
            checkout
                .read_file_range(
                    &arrived,
                    ByteRange {
                        offset: 0,
                        length: 7,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value
                .bytes
                .as_ref(),
            b"arrival"
        );

        let prior_arrival = checkout
            .lookup_no_follow(&arrived, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("arrival disappeared")?;
        let temporary_name =
            NamespacePath::from_portable(&PortablePath::parse("/arrival.tmp", limits)?, limits)?;
        std::fs::write(destination.join("arrival.tmp"), b"replacement-arrival")?;
        std::fs::remove_file(destination.join("arrived.txt"))?;
        std::fs::rename(
            destination.join("arrival.tmp"),
            destination.join("arrived.txt"),
        )?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(8),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![
                    WatchChange::Created(temporary_name.clone()),
                    WatchChange::Renamed {
                        from: temporary_name,
                        to: arrived.clone(),
                    },
                ],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let replaced_arrival = checkout
            .lookup_no_follow(&arrived, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("replacement arrival disappeared")?;
        assert_ne!(replaced_arrival.file_id, prior_arrival.file_id);

        let created_and_modified = NamespacePath::from_portable(
            &PortablePath::parse("/created-and-modified.txt", limits)?,
            limits,
        )?;
        std::fs::write(
            destination.join("created-and-modified.txt"),
            b"created-and-modified",
        )?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(8),
                first_sequence: WatchSequence::from_u64(3),
                next_sequence: WatchSequence::from_u64(5),
                changes: vec![
                    WatchChange::Created(created_and_modified.clone()),
                    WatchChange::Modified(created_and_modified.clone()),
                ],
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination.clone(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(
            checkout
                .read_file_range(
                    &created_and_modified,
                    ByteRange {
                        offset: 0,
                        length: 20,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value
                .bytes
                .as_ref(),
            b"created-and-modified"
        );

        let expected_root_identity = capture_root_identity(&destination)?;
        let displaced = temporary.path().join("displaced-view");
        std::fs::rename(&destination, &displaced)?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("arrived.txt"), b"untrusted-root")?;
        let swapped = capture_paths(
            &mut checkout,
            std::slice::from_ref(&arrived),
            &CaptureOptions {
                source_root: destination,
                expected_root_identity,
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        let Err(swapped) = swapped else {
            return Err("capture accepted a replacement root".into());
        };
        assert!(matches!(swapped.error, CaptureError::RootChanged));
        Ok(())
    }

    #[tokio::test]
    async fn coalesced_parent_child_removals_capture_deepest_first()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let limits = config.limits;
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("capture");
        std::fs::create_dir(&root)?;
        let options = CaptureOptions {
            expected_root_identity: capture_root_identity(&root)?,
            source_root: root.clone(),
            maximum_paths: 8,
            maximum_extent_spans: 8,
        };
        capture_baseline(
            &mut checkout,
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let parent = portable_path(limits, &[b"parent"])?;
        let child = portable_path(limits, &[b"parent", b"child.txt"])?;
        std::fs::create_dir(root.join("parent"))?;
        std::fs::write(root.join("parent").join("child.txt"), b"child")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(0),
                next_sequence: WatchSequence::from_u64(1),
                changes: vec![WatchChange::Created(parent.clone())],
            },
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert!(
            checkout
                .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .is_some()
        );
        std::fs::remove_file(root.join("parent").join("child.txt"))?;
        std::fs::remove_dir(root.join("parent"))?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![
                    WatchChange::Removed(parent.clone()),
                    WatchChange::Removed(child.clone()),
                ],
            },
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert!(
            checkout
                .lookup_no_follow(&parent, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .is_none()
        );
        Ok(())
    }

    // FSEvents can replay creation hints for paths that existed before the
    // watcher's baseline. Stale created hints must not replace a directory
    // that is still a directory: that remove is illegal while the directory
    // has children, and hints only select which paths to authenticate.
    #[tokio::test]
    async fn stale_created_hints_for_existing_directory_capture_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let limits = config.limits;
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let nested = portable_path(limits, &[b"nested"])?;
        let child = portable_path(limits, &[b"nested", b"child.txt"])?;
        let created = portable_path(limits, &[b"watch.txt"])?;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("nested"))?;
        std::fs::write(temporary.path().join("nested/child.txt"), b"child")?;
        let options = CaptureOptions {
            source_root: temporary.path().to_path_buf(),
            expected_root_identity: capture_root_identity(temporary.path())?,
            maximum_paths: 16,
            maximum_extent_spans: 16,
        };
        capture_baseline(
            &mut checkout,
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let directory_before = checkout
            .lookup_no_follow(&nested, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("captured directory disappeared")?;

        std::fs::write(temporary.path().join("watch.txt"), b"created")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(0),
                next_sequence: WatchSequence::from_u64(6),
                changes: vec![
                    WatchChange::Created(child.clone()),
                    WatchChange::MetadataChanged(child.clone()),
                    WatchChange::Created(nested.clone()),
                    WatchChange::MetadataChanged(nested.clone()),
                    WatchChange::Created(created.clone()),
                    WatchChange::Modified(created.clone()),
                ],
            },
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let directory_after = checkout
            .lookup_no_follow(&nested, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("directory disappeared after stale hints")?;
        assert_eq!(directory_after.file_id, directory_before.file_id);
        for (path, expected) in [(&child, b"child".as_slice()), (&created, b"created")] {
            let read = checkout
                .read_file_range(
                    path,
                    ByteRange {
                        offset: 0,
                        length: u64::try_from(expected.len())?,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?;
            assert_eq!(read.value.bytes.as_ref(), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn windows_created_and_modified_hint_captures_one_new_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = FilesystemProfile::Windows;
        let limits = config.limits;
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let name = "watch.txt"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let path = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::WindowsUtf16Le,
                name,
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("watch.txt"), b"created")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(0),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![
                    WatchChange::Created(path.clone()),
                    WatchChange::Modified(path.clone()),
                ],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 16,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let read = checkout
            .read_file_range(
                &path,
                ByteRange {
                    offset: 0,
                    length: 7,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(read.value.bytes.as_ref(), b"created");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn posix_fifo_and_socket_materialize_and_capture_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::FileTypeExt;

        let limits = VolumeLimits::default();
        let config = VolumeConfig {
            profile: FilesystemProfile::Posix,
            concurrency: ConcurrencyMode::Optimistic,
            lifecycle: Lifecycle::Ephemeral,
            case_sensitivity: CaseSensitivity::Sensitive,
            unicode: UnicodePolicy::Preserve,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
            limits,
        };
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let source = fs
            .create_volume_with_id(
                VolumeId::from_bytes([92; 16]),
                config,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut source_checkout = source
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let fifo = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::PosixBytes,
                b"pipe".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        let socket = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::PosixBytes,
                b"socket".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        source_checkout
            .create_empty_special(
                fifo.clone(),
                crate::kernel::FileKind::Fifo,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        source_checkout
            .create_empty_special(
                socket.clone(),
                crate::kernel::FileKind::Socket,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;

        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("special-view");
        std::fs::create_dir(&destination)?;
        let materialized = Box::pin(materialize_checkout(
            &mut source_checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?;
        assert_eq!(materialized.value.special_files, 2);
        assert!(
            std::fs::symlink_metadata(destination.join("pipe"))?
                .file_type()
                .is_fifo()
        );
        assert!(
            std::fs::symlink_metadata(destination.join("socket"))?
                .file_type()
                .is_socket()
        );

        let captured = fs
            .create_volume_with_id(
                VolumeId::from_bytes([93; 16]),
                config,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut captured_checkout = captured
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let receipt = capture_baseline(
            &mut captured_checkout,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination,
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(receipt.value.changed_paths, 2);
        assert_eq!(
            captured_checkout
                .lookup_no_follow(&fifo, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("captured FIFO is absent")?
                .kind,
            crate::kernel::FileKind::Fifo
        );
        assert_eq!(
            captured_checkout
                .lookup_no_follow(&socket, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("captured socket is absent")?
                .kind,
            crate::kernel::FileKind::Socket
        );
        Ok(())
    }
}
