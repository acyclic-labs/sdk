//! Native Linux FUSE, macOS NFS, and Windows `ProjFS` projection boundary.
//!
//! Drivers project one canonical SDK checkout. They own kernel handles and
//! callback cursors only; filesystem truth, COW state, and publication remain
//! in `acyclic-fs`.
//!
//! # macOS
//!
//! The macOS mount is an `NFSv4` filesystem that only the kernel's NFS client
//! can reach, over a local socket in a private directory. Where NFS defines
//! behavior, it differs from APFS:
//!
//! - Cached names and attributes are revalidated at every open and within
//!   one second otherwise, so a change made around the mount becomes visible
//!   no later than that; [`NativeMountSession::revalidate`] waits it out.
//!   Positive `access(2)` answers are cached for up to a minute, but the
//!   server checks access again at every open.
//! - Advisory locks exclude other processes, but the NFS client keys every
//!   lock by process: two descriptors that one process opened never exclude
//!   each other with `flock`, whereas APFS gives each open file description
//!   its own `flock`. `fcntl` record locks are per process on both.
//!
//! # A lazy mount's source
//!
//! A lazy mount projects a source directory that changes outside it. A
//! source on a local file system reports each change (inotify, `FSEvents`,
//! or `ReadDirectoryChangesExW`), and the mount records the report in its
//! view exactly as it records its own changes: every answer the mount or
//! the kernel remembered holds until a change to what it depends on is
//! reported, and drivers drop what the report superseded, as they do for
//! any change made around the mount. A change takes effect in the mount
//! when its report is recorded, shortly after the host operation returned;
//! [`NativeMountSession::revalidate`] (and `LazyMount::revalidate`) waits
//! until every change completed before it is visible. See the
//! `source_watch` module for the exactness argument. A source the host
//! cannot watch (a network or user-space file system) is read afresh
//! behind every remembered answer, and nothing of it is cached.

use crate::kernel::FileMetadata;
use crate::demand::{NativeMountAllocationCustody, NativeMountControlAllocation};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use crate::kernel::MetadataField;
use crate::{FileId, MountId, VolumeId};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::time::SystemTime;
use thiserror::Error;

/// Enters one driver callback's `acyclic.fs.mount.<callback>` span; the
/// callback records `errno` on it when it fails.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
macro_rules! callback_span {
    ($level:ident, $callback:ident $(, $field:ident = $value:expr)* $(,)?) => {
        tracing::span!(
            tracing::Level::$level,
            concat!("acyclic.fs.mount.", stringify!($callback)),
            $($field = $value,)*
            errno = crate::obs::Empty,
        )
        .entered()
    };
}

mod adapter;
pub use adapter::{
    CheckoutMountSource, NativeSourceIoLease, SharedCheckout, SharedCheckoutGuard, SharedCheckoutState,
    initialize_native_mount_services,
};

mod view_gate;
pub(crate) use view_gate::ViewGateReadAdmissions;

mod view_ledger;
pub(crate) use view_ledger::ViewObserverAdmissions;
pub(crate) use view_ledger::{ViewLedger, ViewLedgerAdmission, ViewLedgerAdmissions};
pub use view_ledger::{ViewObserver, ViewOrigin, ViewStamp};

mod lazy;
pub use lazy::{LazyMountSource, NativeMountSourceWatchOwner};

mod customer;
pub use customer::{
    LazyMount, Mount, MountLifecycleError, MountOptions, MountPublication, NativeWorkingSet, NativeMountDrainProof,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod device;

pub use crate::{
    CaptureError, CaptureOptions, CaptureReceipt, WatchCaptureReceipt, capture_baseline,
    capture_paths, capture_root_identity, capture_subtree, capture_watch_batch,
};

mod materialize;
pub use materialize::{
    HostPathReplacement, HostPathRestore, MaterializationReceipt, MaterializeError,
    MaterializeOptions, materialize_checkout, materialize_checkout_host_path,
    materialize_checkout_path, materialize_checkout_paths, restore_checkout_host_path,
};
pub(crate) use materialize::{
    MaterializeMode, materialize_checkout_paths_with_mode, materialize_checkout_with_mode,
    NativeMaterializationActionPhase, materialize_checkout_host_paths_with_work_guarded,
    materialize_checkout_paths_with_mode_and_work,
};
pub use materialize::materialize_checkout_with_work;

mod publication;
pub use publication::{seal_checkout, seal_checkout_with_permit};

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

#[cfg(target_os = "macos")]
mod darwin_mount;
#[cfg(target_os = "macos")]
pub use darwin_mount::DarwinTeardownFailure;
#[cfg(target_os = "linux")]
mod fuse;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn metadata_or<T>(field: MetadataField<T>, unavailable: T) -> T {
    match field {
        MetadataField::Unavailable => unavailable,
        MetadataField::Value(value) => value,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn system_time_ns(value: SystemTime) -> Result<i64, i32> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_nanos()).map_err(|_| libc::EOVERFLOW),
        Err(error) => i64::try_from(error.duration().as_nanos())
            .map(|nanos| -nanos)
            .map_err(|_| libc::EOVERFLOW),
    }
}

/// Entries a driver requests from the source per directory page.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
const DIRECTORY_PAGE_SIZE: u32 = 256;
/// Named attributes a driver requests from the source per page.
#[cfg(any(target_os = "linux", target_os = "macos"))]
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
#[cfg(any(target_os = "linux", target_os = "macos"))]
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;

/// Metadata for a node created through a POSIX mount by `uid`/`gid` now.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn create_metadata(mode: u32, kind: u32, uid: u32, gid: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(uid),
        posix_gid: MetadataField::Value(gid),
        posix_flags: MetadataField::Value(0),
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Value(now),
        modified_ns: MetadataField::Value(now),
        accessed_ns: MetadataField::Value(now),
        changed_ns: MetadataField::Value(now),
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
fn errno(error: MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Allocation(_) => libc::ENOMEM,
        MountSourceError::Io(error) | MountSourceError::NativeRecordIo(error, _) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(error) => native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => native_workspace_errno(error),
        MountSourceError::WorkspacePrefixFailure { source, .. } => native_workspace_errno(source),
        MountSourceError::NativeWorkspaceCause(cause) => native_workspace_errno(&cause.source),
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::NativeText { kind, .. } | MountSourceError::NativeStaticText { kind, .. } =>
            native_source_text_errno(*kind),
        MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ESTALE,
        MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        MountSourceError::Stale | MountSourceError::Cancelled => libc::ESTALE,
        MountSourceError::Engine(_) | MountSourceError::Work(_)
        | MountSourceError::Filesystem(_) | MountSourceError::ServicesNotInitialized
        | MountSourceError::ServiceUnavailable(_) | MountSourceError::Retained(_)
        | MountSourceError::MutationFinalization { .. }
        | MountSourceError::OriginalSnapshotUnavailable | MountSourceError::Indeterminate
        | MountSourceError::SelectedPublication(_) | MountSourceError::PublicationRetryLimit
        | MountSourceError::NativeRecordJson(_, _) | MountSourceError::NativeRecordLockTimeout(_, _)
        | MountSourceError::NativeRecordOwnershipConflict(_, _)
        | MountSourceError::NativeRecordMaterializationInProgress
        | MountSourceError::NativeRecordIntegrity | MountSourceError::NativeRecordContextTransaction(_, _)
        | MountSourceError::NativeRecordCorrupt(_, _) => libc::EIO,
        #[cfg(target_os = "linux")]
        MountSourceError::LinuxMetadata(_) => libc::EIO,
        #[cfg(target_os = "macos")]
        MountSourceError::MacMetadata(_) => libc::EIO,
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        MountSourceError::SourceWatchStartup(_) | MountSourceError::NativeWatchIo(_)
        | MountSourceError::NativeWatchBackend(_) | MountSourceError::NativeWatchOriginalIo(_)
        | MountSourceError::NativeWatchOriginalBackend(_)
        | MountSourceError::NativeWatchStartupFailed { .. }
        | MountSourceError::NativeWatchSourceStartupFailed { .. } => libc::EIO,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
fn source_error(error: MountSourceError) -> NativeMountError {
    error.into()
}

#[cfg(target_os = "windows")]
mod projfs;
#[cfg(target_os = "windows")]
mod provider_stack;
#[cfg(test)]
mod stack_budget;

/// Concrete namespace mechanism selected for this binary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMountKind {
    /// Linux kernel FUSE through `/dev/fuse`.
    LinuxFuse,
    /// Self-contained macOS loopback-NFS session.
    MacOsNfs,
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
    /// A provider may still reject cross-root operations it cannot observe.
    pub writable: bool,
    /// Whether I/O issued by the provider process itself is observable.
    ///
    /// `ProjFS` suppresses provider-process notifications. Embedded callers
    /// must use the SDK mutation surface for their own writes or place the
    /// provider in a dedicated host process.
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

/// Receives content one piece at a time: the piece's logical offset and its
/// canonical owned bytes.
pub type ContentSink<'a> = dyn FnMut(u64, acyclic_stream::OwnedBytes) -> Result<(), MountSourceError> + 'a;

/// Measured pinned content keeps the producer's exact live capacity through the
/// actual native completion. Payload custody remains in the byte carrier.
pub type NativeMountContentSink<'a> =
    dyn FnMut(u64, crate::kernel::OwnedSourceChunk) -> Result<(), MountSourceError> + 'a;

/// Opaque evidence of the exact external content one lookup observed.
///
/// Only the issuing source interprets it. A driver whose projected metadata
/// outlives the callback that produced it (a `ProjFS` placeholder) stores the
/// pin with that metadata and later reads exactly the promised content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountContentPin(pub [u8; 32]);

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
    pub bytes: acyclic_stream::OwnedBytes,
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
    /// Looks up through the operative shared native invocation.
    ///
    /// # Errors
    ///
    /// Preserves original authentication, cancellation, and work failures.
    fn lookup_with_meter(&self, work: &NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
    /// Reads an exact owned range admitted on the original invocation.
    ///
    /// # Errors
    ///
    /// Preserves the actual read and owned-allocation prefix.
    fn read_range_with_meter(&self, offset: u64, length: u32, work: &NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads up to the original bound without losing the producer's capacity.
    ///
    /// # Errors
    ///
    /// Preserves original read, range, and allocation failures.
    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Finds the actual sparse boundary through the original source engine.
    ///
    /// # Errors
    ///
    /// Preserves sparse-tree, storage, and work failures.
    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &NativeMountMeterWork)
        -> Result<Option<u64>, MountSourceError>;
    /// Consumes the caller's exact already-admitted owned payload.
    ///
    /// # Errors
    ///
    /// Retains the actual mutation prefix and any physically retained payload.
    fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Changes length through the same operative source invocation.
    ///
    /// # Errors
    ///
    /// Preserves actual sparse-tree, storage, and work failures.
    fn resize_with_meter(&self, logical_bytes: u64, work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Applies the actual sparse allocation operation.
    ///
    /// # Errors
    ///
    /// Preserves unsupported, storage, range, and work failures.
    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Mutates exact metadata and optional size through the same engine.
    ///
    /// # Errors
    ///
    /// Preserves actual mutation and admission failures.
    fn set_attributes_with_meter(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Reads an owned named attribute without erasing capacity.
    ///
    /// # Errors
    ///
    /// Preserves original attribute, authentication, and work failures.
    fn read_attribute_with_meter(&self, name: &[u8], work: &NativeMountMeterWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned native attribute page.
    ///
    /// # Errors
    ///
    /// Preserves cursor and owned-allocation failures.
    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMountMeterWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted attribute value.
    ///
    /// # Errors
    ///
    /// Preserves actual mutation and ownership on failure.
    fn write_attribute_with_meter(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Removes the actual named attribute through the same engine.
    ///
    /// # Errors
    ///
    /// Preserves original attribute, storage, and work failures.
    fn remove_attribute_with_meter(&self, name: &[u8], work: &NativeMountMeterWork)
        -> Result<(), MountSourceError>;
    /// Settles this actual handle's queued effects before durability acknowledgement.
    ///
    /// # Errors
    ///
    /// Preserves actual queued, durability, and work failures.
    fn settle_with_meter(&self, work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Returns the owned sparse range and its original producer's exact claim.
    ///
    /// # Errors
    ///
    /// Preserves actual sparse-tree, read, and owned-allocation failures.
    fn read_sparse_range_with_meter(&self, offset: u64, length: u32, work: &NativeMountMeterWork)
        -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>;
    /// Consumes the exact already-admitted sparse payload, including capacities
    /// known before each span's conversion to `Bytes`.
    ///
    /// # Errors
    ///
    /// Preserves every actual mutation and physically retained payload.
    fn write_sparse_range_with_meter(&self, offset: u64, range: (MountSparseRange, crate::NativeAllocation),
        work: &NativeMountMeterWork) -> Result<(), MountSourceError>;

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
    fn read_range(&self, offset: u64, length: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
    /// Reads at most `maximum_bytes`, clipping to EOF in the same coherent view.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`Self::read_range`].
    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
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
    fn read_attribute(&self, name: &[u8]) -> Result<Option<acyclic_stream::OwnedBytes>, MountSourceError>;
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
    /// Applies whatever this handle holds back from the view, as a
    /// durability request must; most handles hold nothing back.
    ///
    /// # Errors
    ///
    /// Returns the storage, cancellation, or work failure of applying it.
    fn settle(&self) -> Result<(), MountSourceError> {
        Ok(())
    }

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
            self.write_range(destination, Bytes::copy_from_slice(span.bytes.as_ref()))?;
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
    /// The content pin a [`MountFilesystem::lookup_pinned`] of this entry
    /// issues in the same view, so a projection may answer that lookup from
    /// the listing.
    pub pin: Option<MountContentPin>,
}

/// One independently resumable directory-handle page.
pub struct MountDirectoryPage {
    /// Ordered entries after the supplied opaque cursor.
    entries: Vec<MountDirectoryEntry>,
    /// Opaque continuation owned by this open directory handle.
    next_cursor: Option<Vec<u8>>,
    ownership: crate::demand::SourcePayloadOwnership,
}

impl MountDirectoryPage {
    /// Wraps an ordinary externally-owned page without claiming borrowed work.
    #[must_use]
    pub fn new(entries: Vec<MountDirectoryEntry>, next_cursor: Option<Vec<u8>>) -> Self {
        Self::with_ownership(entries, next_cursor, crate::demand::SourcePayloadOwnership::default())
    }

    /// Moves an already-admitted page's genuine original allocation custody.
    /// No new adoption, allocation, or budget is created by this transfer.
    #[must_use]
    pub fn with_native_allocation(
        entries: Vec<MountDirectoryEntry>, next_cursor: Option<Vec<u8>>,
        allocation: crate::NativeAllocation,
    ) -> Self {
        Self::with_ownership(
            entries, next_cursor,
            crate::demand::SourcePayloadOwnership::from_native_allocation(allocation),
        )
    }

    pub(crate) fn with_ownership(
        entries: Vec<MountDirectoryEntry>, next_cursor: Option<Vec<u8>>,
        ownership: crate::demand::SourcePayloadOwnership,
    ) -> Self {
        Self { entries, next_cursor, ownership }
    }

    /// Borrows entries without detaching their physical capacity custody.
    #[must_use]
    pub fn entries(&self) -> &[MountDirectoryEntry] { &self.entries }

    /// Borrows this page's opaque continuation.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&[u8]> { self.next_cursor.as_deref() }

    /// Internal physical projections transfer data and its exact custody
    /// together, splitting that custody only for actual escaping payloads.
    pub(crate) fn into_parts(self) -> (
        Vec<MountDirectoryEntry>, Option<Vec<u8>>, crate::demand::SourcePayloadOwnership,
    ) {
        (self.entries, self.next_cursor, self.ownership)
    }

    /// Known owned capacities of this exact page, excluding borrowed metadata
    /// and opaque allocator overhead. Custody remains live until actual drop.
    ///
    /// # Errors
    ///
    /// Returns overflow rather than truncating any capacity.
    pub fn logical_owned_bytes(&self) -> Result<u64, crate::WorkError> {
        let entries = self.entries.capacity().checked_mul(std::mem::size_of::<MountDirectoryEntry>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.entries.iter().try_fold(entries, |bytes, entry| {
            bytes.checked_add(entry.name.capacity() as u64).ok_or(crate::WorkError::Overflow)
        })?.checked_add(self.next_cursor.as_ref().map_or(0, |cursor| cursor.capacity() as u64))
            .ok_or(crate::WorkError::Overflow)
    }
}

impl std::fmt::Debug for MountDirectoryPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MountDirectoryPage")
            .field("entries", &self.entries).field("next_cursor", &self.next_cursor).finish()
    }
}

impl PartialEq for MountDirectoryPage {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries && self.next_cursor == other.next_cursor
    }
}

impl Eq for MountDirectoryPage {}

/// One bounded page of exact native named-attribute names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountAttributePage {
    /// Strictly ordered native names after the supplied cursor.
    pub names: Vec<Vec<u8>>,
    /// Exact last name when another page exists.
    pub next_cursor: Option<Vec<u8>>,
}

impl MountAttributePage {
    /// Exact known owned capacities of this page, excluding allocator overhead.
    ///
    /// # Errors
    ///
    /// Returns overflow before a caller adopts any payload claim.
    pub fn logical_owned_bytes(&self) -> Result<u64, crate::WorkError> {
        let slots = self.names.capacity().checked_mul(std::mem::size_of::<Vec<u8>>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.names.iter().try_fold(slots, |bytes, name| {
            bytes.checked_add(name.capacity() as u64).ok_or(crate::WorkError::Overflow)
        })?.checked_add(self.next_cursor.as_ref().map_or(0, |cursor| cursor.capacity() as u64))
            .ok_or(crate::WorkError::Overflow)
    }
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
/// Components contain raw Unix name bytes on Linux FUSE/macOS NFS and little-endian
/// UTF-16 code units on `ProjFS`. The checkout adapter converts them into the
/// volume's declared logical-name representation without lossy text routing.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

    /// Returns the containing directory, or `None` at the volume root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let (_, parent) = self.components.split_last()?;
        Some(Self {
            components: parent.to_vec(),
        })
    }
}


pub(crate) trait MountSourceFailureCause: Send + Sync {
    fn semantic_error(&self) -> &MountSourceError;
}

impl std::fmt::Debug for dyn MountSourceFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.semantic_error(), formatter)
    }
}
/// Existing source diagnostic classification; it grants no operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeSourceTextKind {
    /// Original backend diagnostic.
    Engine,
    /// Original invalid input diagnostic.
    Invalid,
    /// Original unsupported policy diagnostic.
    Unsupported,
}

/// Errors returned by the canonical checkout callback bridge.
#[derive(Debug, Error, strum::IntoStaticStr)]
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
    /// The canonical filesystem retains its exact original typed cause.
    #[error(transparent)]
    Filesystem(crate::FsError),
    /// The acknowledged operation has no original logical snapshot evidence.
    #[error("original selected-import snapshot is unavailable")]
    OriginalSnapshotUnavailable,
    /// The acknowledged original native publication has not finished reconciliation.
    #[error("original native publication is indeterminate")]
    Indeterminate,
    /// The original SDK seal returned an exact semantic decision.
    #[error("selected import publication was not committed: {0:?}")]
    SelectedPublication(crate::CheckoutCommitOutcome),
    /// Canonical namespace validation retains its exact typed cause.
    #[error(transparent)]
    NamespacePath(crate::kernel::NamespacePathError),
    /// Original native OS cause, moved without allocating a replacement error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// This mount or callback was fenced or cancelled.
    #[error("mount callback is stale or cancelled")]
    Stale,
    /// An explicitly admitted native operation was cancelled.
    #[error("native operation was cancelled")]
    Cancelled,
    /// Canonical publication exceeded its exact work allowance.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Original native filesystem work cause, without formatting or reboxing.
    #[error(transparent)]
    NativeWork(#[from] crate::NativeTreeMaterializationError),
    /// Original Linux metadata cause, moved without formatting a diagnostic.
    #[cfg(target_os = "linux")]
    #[error("filesystem engine failure: source demand failed: {0}")]
    LinuxMetadata(#[source] crate::native_host::LinuxMetadataError),
    /// Original macOS metadata cause, moved without formatting a diagnostic.
    #[cfg(target_os = "macos")]
    #[error("filesystem engine failure: source demand failed: {0}")]
    MacMetadata(#[source] crate::native_host::MacMetadataError),
    /// Original Windows metadata cause, moved without formatting a diagnostic.
    #[cfg(windows)]
    #[error("filesystem engine failure: source demand failed: {0}")]
    WindowsMetadata(#[source] crate::native_host::WindowsMetadataError),
    /// Original preallocated source-watch failure and its physical owner.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: {0}")]
    SourceWatchStartup(#[source] Arc<crate::source_watch::SourceWatchStartupFailure>),
    /// Existing ordinary watch diagnostic, moved without another string.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native watcher I/O failed: {0}")]
    NativeWatchIo(String),
    /// Existing ordinary backend diagnostic, moved without another string.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native watcher backend failed: {0}")]
    NativeWatchBackend(String),
    /// Original finite watcher I/O cause, with no replacement error box.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native watcher I/O failed: {0}")]
    NativeWatchOriginalIo(#[source] std::io::Error),
    /// Original finite watcher backend cause, moved without formatting.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native watcher backend failed: {0}")]
    NativeWatchOriginalBackend(#[source] notify::Error),
    /// Same actual started root watcher retained after later admission failure.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native watcher startup failed: {cause}")]
    NativeWatchStartupFailed {
        /// Original typed cause, moved without another allocation.
        #[source]
        cause: crate::NativeTreeMaterializationError,
        /// Actual same root watcher to retain through measured terminal cleanup.
        watch: crate::watch::NativeWatch,
    },
    /// Same complete root watcher and already-retained source startup cause.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: native source watcher startup failed: {cause}")]
    NativeWatchSourceStartupFailed {
        /// Original preallocated partial source failure.
        #[source]
        cause: Arc<crate::source_watch::SourceWatchStartupFailure>,
        /// Actual same complete root watcher to retain for bounded cleanup.
        watch: crate::watch::NativeWatch,
    },
    /// An actual mutation failed and its original completion accounting also failed.
    /// The primary slot is funded before the first effect and always contains its cause.
    #[error("mutation failed; original completion accounting also failed")]
    MutationFinalization {
        /// Original typed mutation cause in its already admitted physical slot.
        primary: Box<Option<MountSourceError>>,
        /// Original completion accounting cause, moved without replacing the primary.
        cleanup: Box<Option<crate::demand::DemandError>>,
        /// The physical primary slot drops before its original allocation custody.
        custody: Option<crate::NativeAllocation>,
    },
    /// Original authenticated workspace cause, moved without formatting.
    #[error(transparent)]
    WorkspaceCause(crate::WorkspaceError),
    /// Original workspace failure and the original prefix-accounting failure.
    #[error("workspace operation and its prefix accounting failed")]
    WorkspacePrefixFailure {
        /// Complete original workspace cause.
        #[source]
        source: crate::WorkspaceError,
        /// Complete scalar prefix failure; neither cause replaces the other.
        accounting: crate::WorkspaceError,
    },
    /// Whole original workspace cause and its already funded outer custody.
    #[error(transparent)]
    NativeWorkspaceCause(crate::lazy_workspace::NativeLazyWorkspaceCause),
    /// Original authorization denial in its already funded callback slot.
    /// The physical cause drops before its required original allocation guard.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native callback authorization failed")]
    NativeCallbackDenied {
        /// Complete original typed callback cause; no replacement box is made.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Original custody of the same physical failure slot.
        custody: crate::NativeAllocation,
    },
    /// Original funded SDK diagnostic; data physically drops before custody.
    #[error("{text}")]
    NativeText {
        /// Original diagnostic classification.
        kind: NativeSourceTextKind,
        /// Already constructed and measured diagnostic.
        text: String,
        /// Always funded at construction; taken only for owned SDK restoration.
        allocation: Option<crate::NativeAllocation>,
    },
    /// Actual static SDK reason; construction allocates no diagnostic payload.
    #[error("{text}")]
    NativeStaticText {
        /// Original diagnostic classification.
        kind: NativeSourceTextKind,
        /// Original constant reason, without string allocation.
        text: &'static str,
    },
    /// Original record IO cause and its already funded data custody.
    /// The cause drops before the original allocation guard.
    #[error("native record IO error: {0}")]
    NativeRecordIo(#[source] std::io::Error, Option<crate::NativeAllocation>),
    /// Original owned native record parser cause; no diagnostic allocation.
    #[error("native record JSON error: {0}")]
    NativeRecordJson(#[source] serde_json::Error, Option<crate::NativeAllocation>),
    /// Actual record lock acquisition timed out.
    #[error("native record lock timed out: {}", .0.display())]
    NativeRecordLockTimeout(PathBuf, Option<crate::NativeAllocation>),
    /// The existing record belongs to a different original owner.
    #[error("native record ownership conflict: {}", .0.display())]
    NativeRecordOwnershipConflict(PathBuf, Option<crate::NativeAllocation>),
    /// The existing journal still holds an incomplete materialization.
    #[error("native record materialization remains in progress")]
    NativeRecordMaterializationInProgress,
    /// Existing native record integrity verification failed.
    #[error("native record integrity verification failed")]
    NativeRecordIntegrity,
    /// Original context-transaction diagnostic already owned by its producer.
    #[error("native record context transaction failed: {0}")]
    NativeRecordContextTransaction(String, Option<crate::NativeAllocation>),
    /// Original corrupt-record location, moved without copying its path.
    #[error("native record is corrupt: {}", .0.display())]
    NativeRecordCorrupt(PathBuf, Option<crate::NativeAllocation>),
    /// A native operation is unsupported before dispatch.
    #[error("native operation is unsupported")]
    NativeUnsupported,
    /// Existing publication attempt exhausted its bounded rebase loop.
    /// Its original operation remains retained for the caller's explicit retry.
    #[error("publication raced with repeated head advances; retry retained operation")]
    PublicationRetryLimit,
    /// Original fallible allocation failure, without allocating a diagnostic.
    #[error("native source allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// Explicit native process-shared services are cold.
    #[error("native mount services are not initialized")]
    ServicesNotInitialized,
    /// Original service-start cause already owned by explicit bootstrap.
    #[error("native mount service startup failed: {0}")]
    ServiceUnavailable(#[source] Arc<std::io::Error>),
    /// Original cause in an already admitted pending/completion owner.
    #[error("{}", .0.semantic_error())]
    Retained(Arc<dyn MountSourceFailureCause>),
}

impl MountSourceError {
    pub fn semantic_error(&self) -> &Self {
        let mut error = self;
        loop {
            match error {
                Self::Retained(cause) => error = cause.semantic_error(),
                Self::MutationFinalization { primary, .. } if primary.is_some() =>
                    error = primary.as_ref().as_ref().expect("a completed mutation has its original primary"),
                _ => break,
            }
        }
        error
    }

    /// Moves only the allocation guard already owned by these native causes.
    /// The caller retains it outside any original-work fatal-cause slot; this
    /// does not allocate, clone data, or manufacture custody for borrowed errors.
    pub(crate) fn take_native_allocation(&mut self) -> Option<crate::NativeAllocation> {
        self.native_allocation_mut().and_then(Option::take)
    }

    pub(crate) fn native_allocation_mut(&mut self) -> Option<&mut Option<crate::NativeAllocation>> {
        match self {
            Self::NativeRecordIo(_, allocation) | Self::NativeRecordJson(_, allocation)
            | Self::NativeRecordLockTimeout(_, allocation) | Self::NativeRecordOwnershipConflict(_, allocation)
            | Self::NativeRecordContextTransaction(_, allocation) | Self::NativeRecordCorrupt(_, allocation) =>
                Some(allocation),
            Self::NativeText { allocation, .. } => Some(allocation),
            Self::MutationFinalization { custody, .. } => Some(custody),
            Self::NativeWorkspaceCause(cause) => Some(&mut cause.custody),
            Self::NativeWork(error) => error.native_failure_allocation_mut(),
            _ => None,
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn native_materialization_errno(error: &crate::NativeTreeMaterializationError) -> i32 {
    match error {
        crate::NativeTreeMaterializationError::Io(error) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        crate::NativeTreeMaterializationError::Cancelled => libc::ESTALE,
        crate::NativeTreeMaterializationError::AllocationFailed(_) => libc::ENOMEM,
        crate::NativeTreeMaterializationError::PayloadTooLarge { .. } => libc::EFBIG,
        _ => libc::EIO,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn native_source_text_errno(kind: NativeSourceTextKind) -> i32 {
    match kind {
        NativeSourceTextKind::Engine => libc::EIO,
        NativeSourceTextKind::Invalid => libc::EINVAL,
        NativeSourceTextKind::Unsupported => libc::EOPNOTSUPP,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn native_workspace_errno(error: &crate::WorkspaceError) -> i32 {
    use crate::WorkspaceError;
    match error {
        WorkspaceError::Cancelled(_) | WorkspaceError::Filesystem(crate::FsError::Cancelled(_))
        | WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => libc::ECANCELED,
        WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) => native_materialization_errno(error),
        WorkspaceError::NotFound | WorkspaceError::Filesystem(crate::FsError::NotFound) => libc::ENOENT,
        WorkspaceError::NotDirectory | WorkspaceError::Filesystem(crate::FsError::NotDirectory) => libc::ENOTDIR,
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => libc::EIO,
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed)
        | WorkspaceError::Filesystem(crate::FsError::HandleAllocationFailed) => libc::ENOMEM,
        WorkspaceError::Name(_) | WorkspaceError::Path(_) | WorkspaceError::NotRegularFile
        | WorkspaceError::NamespacePath(_) => libc::EINVAL,
        WorkspaceError::ReadLimitExceeded => libc::EFBIG,
        WorkspaceError::StaleGeneration | WorkspaceError::StaleSource | WorkspaceError::StaleIdentity
        | WorkspaceError::StaleTarget | WorkspaceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        _ => libc::EIO,
    }
}

pub(crate) fn native_workspace_failure_is_fatal(error: &crate::WorkspaceError) -> bool {
    if let crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) = error {
        return native_materialization_failure_is_fatal(error);
    }
    matches!(crate::workspace::workspace_bounded_error(error),
        Some(crate::WorkspaceError::Work(_) | crate::WorkspaceError::Cancelled(_)
            | crate::WorkspaceError::Filesystem(crate::FsError::RootReadWorkMerge { .. })))
        || matches!(error,
            crate::WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed)
            | crate::WorkspaceError::Filesystem(crate::FsError::HandleAllocationFailed))
}

pub(crate) fn native_materialization_failure_is_fatal(
    error: &crate::NativeTreeMaterializationError,
) -> bool {
    matches!(error,
        crate::NativeTreeMaterializationError::Work(_)
        | crate::NativeTreeMaterializationError::Cancelled
        | crate::NativeTreeMaterializationError::AllocationFailed(_)
        | crate::NativeTreeMaterializationError::IndeterminatePreparation
        | crate::NativeTreeMaterializationError::ExternalMutation(..)
        | crate::NativeTreeMaterializationError::AllocationChanged { .. })
}


impl From<NativeMountStartupAdmissionError> for MountSourceError {
    fn from(error: NativeMountStartupAdmissionError) -> Self {
        match error {
            NativeMountStartupAdmissionError::Work(error) => Self::Work(error),
            NativeMountStartupAdmissionError::Materialization(error) => Self::NativeWork(error),
            NativeMountStartupAdmissionError::Demand(error) => lazy::mount_demand_error(error),
            NativeMountStartupAdmissionError::Allocation(error) => Self::Allocation(error),
            NativeMountStartupAdmissionError::ServicesCold => Self::ServicesNotInitialized,
            NativeMountStartupAdmissionError::ServiceUnavailable(error) => Self::ServiceUnavailable(error),
        }
    }
}

/// An owned permit that pins one coherent projected source view.
///
/// Native drivers retain this value through every externally visible callback
/// result. Sources that can rebind must make rebinding wait for outstanding
/// permits, eliminating check-then-emit races at the kernel boundary.
pub trait MountViewLease: Send + 'static {}

impl MountViewLease for () {}

/// Borrowed admission context for one native lifecycle invocation.
///
/// Source callbacks and the platform driver use the same allocation ledger,
/// cancellation roots, and counters. The carrier itself owns no allocation.
pub struct NativeMountWork<'a> {
    pub(crate) work: &'a crate::NativeMaterializationWork,
}

/// An existing shared native invocation, borrowed by source callbacks without
/// another ledger loan or a work mutex held across queued native waits.
#[derive(Clone)]
pub struct NativeMountMeterWork {
    meter: Arc<crate::demand::NativeDemandReadMeter>,
    resource_allowance: Option<Arc<crate::materializer::NativeResourceAllowance>>,
    capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    invocation: Option<Arc<crate::materializer::NativeInvocationView>>,
    execution: Option<Arc<crate::materializer::NativeSourceExecutionView>>,
}

impl NativeMountMeterWork {
    /// Aliases the same original native ledger for an external owner guard.
    ///
    /// The complete work, allocation custody and cancellation roots remain in
    /// their original shared carrier; no budget or receipt is reconstructed.
    #[must_use]
    pub fn native_materialization_work(&self) -> crate::NativeMaterializationWork {
        crate::materializer::NativeMaterializationWork::from_native_mount_meter_work(self)
    }

    pub(crate) fn from_meter(meter: Arc<crate::demand::NativeDemandReadMeter>) -> Self {
        Self { meter, resource_allowance: None, capture_origin: None, invocation: None, execution: None }
    }

    pub(crate) fn from_native_materialization_work(work: &crate::NativeMaterializationWork) -> Self {
        Self {
            meter: Arc::clone(work.native_read_meter()),
            resource_allowance: work.resource_allowance().cloned(),
            capture_origin: work.capture_origin().cloned(),
            invocation: work.invocation_view_state().cloned(),
            execution: work.source_execution_view().cloned(),
        }
    }

    pub(crate) fn resource_allowance(&self) -> Option<&Arc<crate::materializer::NativeResourceAllowance>> {
        self.resource_allowance.as_ref()
    }
    pub(crate) fn capture_origin(&self) -> Option<&std::sync::Weak<crate::demand::CaptureTransientCustody>> {
        self.capture_origin.as_ref()
    }
    pub(crate) fn invocation_view_state(&self) -> Option<&Arc<crate::materializer::NativeInvocationView>> {
        self.invocation.as_ref()
    }
    pub(crate) fn source_execution_view(&self) -> Option<&Arc<crate::materializer::NativeSourceExecutionView>> {
        self.execution.as_ref()
    }
    pub(crate) fn meter(&self) -> &Arc<crate::demand::NativeDemandReadMeter> { &self.meter }
    pub(crate) fn run_with_meter<T>(
        &self, operation: impl FnOnce(&NativeMountMeterWork) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        operation(self)
    }
}

/// Authentic source-owner authorization checked before a native SDK callback.
///
/// Direct host-configured sources need no task guard. Task-backed sources must
/// install their live owner, fence, exact grants and purpose before preparation.
/// The accounting header is not authorization evidence.
pub trait NativeMountCallbackGuard: Send + Sync {
    /// Admits the real verification future on the callback's original context.
    ///
    /// # Errors
    ///
    /// Preserves admission failures without allocating another failure carrier.
    fn check<'a>(
        &'a self, work: &'a NativeMountMeterWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
    /// Verifies current authored namespace WRITE permission on the same original
    /// task owner, fence, exact source and purpose. Read authorization, accounting,
    /// SDK cache hydration and physical retirement cannot confer this permission.
    ///
    /// Read-only owners must reject before constructing a write dispatch or
    /// issuing authored source IO.
    fn check_write<'a>(
        &'a self, work: &'a NativeMountMeterWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
}

/// One directly erased authorization future and its original funded custody.
///
/// Destruction always drops the physical future before its allocation guard.
/// It uses the existing callback runtime, not another executor or request queue.
pub struct NativeMountCallbackFuture<'a> {
    future: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a>>,
    _custody: crate::NativeAllocation,
}

impl<'a> NativeMountCallbackFuture<'a> {
    /// Admits the concrete boxed future before invoking its builder.
    ///
    /// Only its actual heap payload is charged here. This outer carrier stays
    /// inline; a caller owning it in another funded heap frame accounts for
    /// that frame's compiled metadata rather than charging it twice here.
    ///
    /// # Errors
    ///
    /// A denied claim never constructs or boxes the verification future.
    pub fn new<F, B>(work: &NativeMountMeterWork, build: B) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F,
    {
        let future_bytes = std::mem::size_of::<F>();
        let allocation = work.native_materialization_work().claim_allocation(
            future_bytes as u64, u64::from(future_bytes != 0),
        )?;
        let future = Box::pin(build());
        Ok(Self { future, _custody: allocation })
    }

    /// Preallocates the exact typed failure slot before authorization starts.
    ///
    /// A failed verification moves its original cause into that already-funded
    /// slot and unsizes the same box without allocating a replacement carrier.
    /// The returned source chain retains the slot's original allocation custody.
    ///
    /// # Errors
    ///
    /// Preserves slot and future admission failures before invoking the builder.
    pub fn new_guarded<E, F, B>(
        work: &NativeMountMeterWork, build: B,
    ) -> Result<Self, NativeMountError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: std::future::Future<Output = Result<(), E>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = NativeMountCallbackReservation::<E>::prepare(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => {
                    drop(failure);
                    Ok(())
                }
                Err(error) => Err(failure.into_error(error)),
            }
        })
    }
    /// Reuses an already funded native denial rather than nesting its custody
    /// inside another cause slot. Other native causes use the same reservation.
    pub(crate) fn new_native_guarded<F, B>(
        work: &NativeMountMeterWork, build: B,
    ) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = NativeMountCallbackReservation::<NativeMountError>::prepare(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => Ok(()),
                Err(error @ NativeMountError::CallbackDenied { .. }) => Err(error),
                Err(error) => Err(failure.into_error(error)),
            }
        })
    }
}

impl std::future::Future for NativeMountCallbackFuture<'_> {
    type Output = Result<(), NativeMountError>;

    fn poll(
        self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

/// One callback's preadmitted failure payload, never a shared diagnostic slot.
struct NativeMountCallbackCause<E> {
    source: Option<E>,
}

impl<E> std::fmt::Debug for NativeMountCallbackCause<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeMountCallbackCause")
    }
}

impl<E> std::fmt::Display for NativeMountCallbackCause<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("native callback authorization failed")
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativeMountCallbackCause<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| source as &dyn std::error::Error)
    }
}

/// Keeps the actual boxed slot before its guard, including before the
/// verification future is built and while that future owns the reservation.
struct NativeMountCallbackReservation<E> {
    slot: Box<NativeMountCallbackCause<E>>,
    custody: crate::NativeAllocation,
}

impl<E: std::error::Error + Send + Sync + 'static> NativeMountCallbackReservation<E> {
    fn prepare(work: &NativeMountMeterWork) -> Result<Self, NativeMountError> {
        let slot_bytes = std::mem::size_of::<NativeMountCallbackCause<E>>();
        let allocation = work.native_materialization_work().claim_allocation(
            slot_bytes as u64, u64::from(slot_bytes != 0),
        )?;
        Ok(Self {
            slot: Box::new(NativeMountCallbackCause { source: None }),
            custody: allocation,
        })
    }

    fn into_error(mut self, error: E) -> NativeMountError {
        self.slot.source = Some(error);
        let source: Box<dyn std::error::Error + Send + Sync> = self.slot;
        NativeMountError::CallbackDenied { source, custody: self.custody }
    }
}

/// One existing physical driver's accounting activation, not task authority.
/// The fixed inline depth permits same-meter scopes to finish out of order.
#[derive(Default)]
pub(crate) struct NativeMountMeterActivation {
    meter: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
    prior: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
    resource_allowance: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    prior_resource_allowance: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    prior_capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    prior_invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    execution: Option<std::sync::Weak<crate::materializer::NativeSourceExecutionView>>,
    prior_execution: Option<std::sync::Weak<crate::materializer::NativeSourceExecutionView>>,
    depth: u64,
    generation: u64,
}

impl NativeMountMeterActivation {
    pub(crate) fn upgrade(&self) -> Option<NativeMountMeterWork> {
        let meter = self.meter.upgrade()?;
        let resource_allowance = match self.resource_allowance.as_ref() {
            Some(resource) => Some(resource.upgrade()?),
            None => None,
        };
        let invocation = match self.invocation.as_ref() {
            Some(invocation) => Some(invocation.upgrade()?),
            None => None,
        };
        let execution = match self.execution.as_ref() {
            Some(execution) => Some(execution.upgrade()?),
            None => None,
        };
        Some(NativeMountMeterWork {
            meter, resource_allowance, capture_origin: self.capture_origin.clone(), invocation, execution,
        })
    }

    fn matches(&self, work: &NativeMountMeterWork) -> bool {
        self.meter.as_ptr() == Arc::as_ptr(&work.meter)
            && match (&self.resource_allowance, &work.resource_allowance) {
                (None, None) => true,
                (Some(left), Some(right)) => left.as_ptr() == Arc::as_ptr(right),
                _ => false,
            }
            && match (&self.capture_origin, &work.capture_origin) {
                (None, None) => true,
                (Some(left), Some(right)) => left.ptr_eq(right),
                _ => false,
            }
            && match (&self.execution, &work.execution) {
                (None, None) => true,
                (Some(left), Some(right)) => left.as_ptr() == Arc::as_ptr(right),
                _ => false,
            }
            && match (&self.invocation, &work.invocation) {
                (None, None) => true,
                (Some(left), Some(right)) => left.as_ptr() == Arc::as_ptr(right),
                _ => false,
            }
    }

    pub(crate) fn enter(
        &mut self, work: &NativeMountMeterWork,
    ) -> Result<u64, NativeMountError> {
        if self.depth != 0 && !self.matches(work) {
            return Err(NativeMountError::ControlPending);
        }
        let depth = self.depth.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        if self.depth == 0 {
            let generation = self.generation.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            self.prior = std::mem::replace(&mut self.meter, Arc::downgrade(&work.meter));
            self.prior_resource_allowance = std::mem::replace(
                &mut self.resource_allowance, work.resource_allowance.as_ref().map(Arc::downgrade),
            );
            self.prior_capture_origin = std::mem::replace(
                &mut self.capture_origin, work.capture_origin.clone(),
            );
            self.prior_invocation = std::mem::replace(
                &mut self.invocation, work.invocation.as_ref().map(Arc::downgrade),
            );
            self.prior_execution = std::mem::replace(
                &mut self.execution, work.execution.as_ref().map(Arc::downgrade),
            );
            self.generation = generation;
        }
        self.depth = depth;
        Ok(self.generation)
    }

    pub(crate) fn leave(
        &mut self, work: &NativeMountMeterWork, generation: u64,
    ) {
        assert!(self.depth != 0 && self.generation == generation && self.matches(work),
            "native accounting scope must retire its own activation");
        self.depth -= 1;
        if self.depth == 0 {
            self.meter = std::mem::take(&mut self.prior);
            self.resource_allowance = std::mem::take(&mut self.prior_resource_allowance);
            self.capture_origin = std::mem::take(&mut self.prior_capture_origin);
            self.invocation = std::mem::take(&mut self.prior_invocation);
            self.execution = std::mem::take(&mut self.prior_execution);
        }
    }
}

/// Actual SDK callback failure record. The existing context admits this inline
/// slot before native startup, so denial never allocates a counters Box.
#[derive(Debug)]
pub(crate) struct NativeMountCallbackFailure {
    pub(crate) cause: NativeMountError,
    pub(crate) work: crate::WorkCounters,
}

/// Executes one installed SDK handler request with the original constructor's
/// copied full grants and cancellation, through the existing native queue.
pub(crate) fn run_native_mount_handler<T>(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    operation: impl FnOnce(&NativeMountMeterWork) -> Result<T, NativeMountError>,
) -> Result<crate::OperationReceipt<T>, NativeMountCallbackFailure> {
    let work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| NativeMountCallbackFailure { cause: cause.into(), work })?;
    let native = work.native_mount_work();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        operation(&native)
    })).unwrap_or_else(|panic| Err(NativeMountError::DriverPanicked {
        operation: "SDK callback", panic: std::sync::Mutex::new(panic),
    }));
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(cause) => Err(NativeMountCallbackFailure { cause, work: work.receipt() }),
    }
}

/// Startup/revalidation handlers use the already operative invocation. Their
/// counters stay in that original meter; no second ledger or receipt is merged.
pub(crate) fn run_native_mount_handler_on_meter<T>(
    work: &NativeMountMeterWork,
    operation: impl FnOnce(&NativeMountMeterWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        operation(work)
    })).unwrap_or_else(|panic| Err(NativeMountError::DriverPanicked {
        operation: "SDK callback", panic: std::sync::Mutex::new(panic),
    }))
}


impl NativeMountAllocationCustody {
    /// Metadata-only admission on the original shared ledger. No OS request,
    /// queued wait or native callback occurs while this guard is held.
    pub(crate) fn new_with_meter(
        work: &NativeMountMeterWork,
    ) -> Result<Self, NativeMountStartupAdmissionError> {
        Self::new_on_original(&work.native_materialization_work().original_demand_work())
            .map_err(Into::into)
    }

    pub(crate) fn claim_with_meter(
        &self, bytes: u64, work: &NativeMountMeterWork,
    ) -> Result<(), NativeMountStartupAdmissionError> {
        work.native_materialization_work().original_demand_work()
            .with_original_leaf(|original| self.claim(bytes, original))?
            .map_err(Into::into)
    }

    pub(crate) fn retain_admitted_with_meter(
        &self, bytes: u64, work: &NativeMountMeterWork,
    ) -> Result<(), NativeMountStartupAdmissionError> {
        work.native_materialization_work().original_demand_work()
            .with_original_leaf(|original| self.retain_admitted(bytes, original))?
            .map_err(Into::into)
    }

    pub(crate) fn from_admitted_with_meter(
        bytes: u64, work: &NativeMountMeterWork,
    ) -> Result<Self, NativeMountStartupAdmissionError> {
        let custody = Self::new_with_meter(work)?;
        custody.retain_admitted_with_meter(bytes, work)?;
        Ok(custody)
    }

    pub(crate) fn is_admitted_with_meter(
        &self, work: &NativeMountMeterWork,
    ) -> Result<bool, NativeMountStartupAdmissionError> {
        work.native_materialization_work().original_demand_work()
            .with_original_leaf(|original| self.is_admitted(original))?
            .map_err(Into::into)
    }

    pub(crate) fn attach_known_control_with_meter(
        &self, owner: NativeMountControlAllocation, work: &NativeMountMeterWork,
    ) -> Result<(), NativeMountStartupAdmissionError> {
        work.native_materialization_work().original_demand_work()
            .with_original_leaf(|original| self.attach_known_control(owner, original))?
            .map_err(Into::into)
    }

}

impl<'a> NativeMountWork<'a> {
    /// Borrows the complete original producer and its resource allowance.
    #[must_use]
    pub const fn new(work: &'a crate::NativeMaterializationWork) -> Self {
        Self { work }
    }

    /// Returns the original producer without lending or reconstructing its ledger.
    #[must_use]
    pub const fn native_materialization_work(&self) -> &'a crate::NativeMaterializationWork {
        self.work
    }

    pub(crate) fn run_with_meter<T>(
        &mut self, operation: impl FnOnce(&NativeMountMeterWork) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        operation(&self.work.native_mount_work())
    }

    /// Original caller cancellation; this never creates a cleanup authority.
    #[must_use]
    pub fn cancellation(&self) -> &crate::CancellationToken {
        self.work.cancellation()
    }

    /// Allowance still available to an independently measured leaf.
    ///
    /// # Errors
    ///
    /// Returns the original exact admission error.
    pub fn remaining(&self) -> Result<crate::WorkBudget, crate::NativeTreeMaterializationError> {
        self.work.remaining()
    }
}

/// Borrowed stock-source consistency data, not namespace or task authority.
///
/// Only canonical SDK adapters construct this value. Its actual checkout
/// owner remains borrowed while the caller retains the source, preventing
/// allocator-address reuse from masquerading as the original lineage.
pub struct NativeMountSourceLineage<'a> {
    owner: &'a dyn NativeMountLineageAlias,
    root: &'a crate::kernel::NamespacePath,
    native_original: Option<&'a crate::NativeMaterializationWork>,
    lazy_owner: Option<&'a dyn NativeMountLineageAlias>,
    lazy_root: Option<&'a str>,
    limits: crate::VolumeLimits,
    capture_gate: &'a std::sync::Mutex<adapter::CaptureSourceGate>,
    capture_cancellation: &'a crate::CancellationToken,
}

trait NativeMountLineageAlias: Send + Sync {
    fn retain(&self) -> Arc<dyn std::any::Any + Send + Sync>;
}
impl<T: std::any::Any + Send + Sync> NativeMountLineageAlias for Arc<T> {
    fn retain(&self) -> Arc<dyn std::any::Any + Send + Sync> { self.clone() }
}

#[cfg(windows)]
struct NativeMountNamespaceBinding {
    owner: Arc<dyn std::any::Any + Send + Sync>,
    root: crate::kernel::NamespacePath,
    native_original: Option<crate::NativeMaterializationWork>,
    lazy_owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    lazy_root: Option<String>,
    _custody: crate::NativeAllocation,
}

#[cfg(windows)]
impl NativeMountNamespaceBinding {
    fn matches(&self, next: &NativeMountSourceLineage<'_>) -> bool {
        Arc::ptr_eq(&self.owner, &next.owner.retain()) && self.root == *next.root
            && match (&self.native_original, next.native_original) {
                (Some(old), Some(new)) => old.shares_ledger(new) && old.shares_resource_allowance(new),
                (None, None) => true,
                _ => false,
            }
            && match (&self.lazy_owner, next.lazy_owner) {
                (Some(old), Some(new)) => Arc::ptr_eq(old, &new.retain()),
                (None, None) => true,
                _ => false,
            }
            && self.lazy_root.as_deref() == next.lazy_root
    }
}

/// Synchronous callback surface implemented by an embedded checkout adapter.
///
/// Native kernels invoke callbacks on foreign threads, so this intentionally
/// exposes a blocking boundary. Implementations may enter an async runtime but
/// must enforce finite work and timeout limits internally.
pub trait MountFilesystem: Send + Sync + 'static {
    /// Returns the sealed original native accounting and lifetime context.
    ///
    /// Ordinary or external implementations have no installed native context.
    /// This context grants no task, effect, or filesystem authority.
    ///
    /// # Errors
    ///
    /// A revoked installed context is rejected before provider work.
    fn native_original_work(&self) -> Result<Option<NativeMountMeterWork>, MountSourceError> {
        Ok(None)
    }

    /// Returns an already-funded live authorization guard for a task source.
    ///
    /// `None` is the explicit direct-host role, not evidence of task permission.
    /// Wrappers preserve the original guard rather than installing it late.
    fn native_callback_guard(&self) -> Option<Arc<dyn NativeMountCallbackGuard>> {
        None
    }

    /// Borrows the actual SDK checkout owner and canonical selected root.
    /// This is consistency data only; it cannot issue a namespace owner or
    /// replace the current callback guard. Unknown custom sources cannot
    /// participate in the owned warm continuation.
    fn native_namespace_lineage(&self) -> Option<NativeMountSourceLineage<'_>> {
        None
    }

    /// Looks up through an already shared original native invocation.
    ///
    /// # Errors
    ///
    /// Preserves the original source cause and completed work.
    fn lookup_with_meter(
        &self, path: &MountPath, work: &NativeMountMeterWork,
    ) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up and pins through the same existing invocation.
    ///
    /// # Errors
    ///
    /// Preserves source failures without borrowing a mutex across waits.
    fn lookup_pinned_with_meter(
        &self, path: &MountPath, work: &NativeMountMeterWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError>;
    /// Registers the actual observer on the same original allocation ledger.
    ///
    /// # Errors
    ///
    /// Preserves exact allocation, cancellation, and source failures.
    fn observe_view_with_meter(
        &self, observer: std::sync::Weak<dyn ViewObserver>, work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Fences actual source changes without another ledger or queue.
    ///
    /// # Errors
    ///
    /// Preserves settlement and original admission failures.
    fn fence_changes_with_meter(&self, work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Acquires the actual source binding lease with its physical drop proof.
    ///
    /// # Errors
    ///
    /// Preserves original allocation, epoch, and cancellation failures.
    fn acquire_binding_lease_with_meter(
        &self, expected_epoch: Option<u64>, work: &NativeMountMeterWork,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError>;
    /// Captures exact paths through the existing original shared invocation.
    ///
    /// # Errors
    ///
    /// Preserves native prefixes, identity, and publication failures.
    fn capture_host_paths_with_meter(
        &self, root: &Path, paths: &[MountPath], expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Captures one subtree through the same actual capture producer.
    ///
    /// # Errors
    ///
    /// Retains the original work prefix and root identity failures.
    fn capture_host_subtree_with_meter(
        &self, root: &Path, path: &MountPath, expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Reads a pinned range without erasing the actual owned chunk capacity.
    ///
    /// # Errors
    ///
    /// Preserves read, sink, and admission failures and their completed work.
    fn read_pinned_with_meter(
        &self, path: &MountPath, pin: MountContentPin, offset: u64, length: u64,
        piece: u32, sink: &mut NativeMountContentSink<'_>, work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Reads actual link bytes and retains their producer-known capacity.
    ///
    /// # Errors
    ///
    /// Preserves original source and cancellation failures.
    fn read_link_with_meter(
        &self, path: &MountPath, work: &NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads the exact range using the existing shared native queue.
    ///
    /// # Errors
    ///
    /// Retains physically completed reads and real buffer custody.
    fn read_range_with_meter(
        &self, path: &MountPath, offset: u64, length: u32, work: &NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads a page with the original ledger and actual capacity custody.
    ///
    /// # Errors
    ///
    /// Preserves cursor, source, cancellation, and allocation failures.
    fn read_directory_with_meter(
        &self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMountMeterWork,
    ) -> Result<MountDirectoryPage, MountSourceError>;

    /// Removes the exact binding on a shared original invocation.
    ///
    /// # Errors
    ///
    /// Preserves actual mutation, authority, and admission failures.
    fn remove_with_meter(
        &self, path: &MountPath, expected: Option<FileId>, work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Same remove producer on borrowed lifecycle work.
    ///
    /// # Errors
    ///
    /// Preserves original mutation and work failures.
    fn remove_with_work(
        &self, path: &MountPath, expected: Option<FileId>, work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;
    /// Renames the exact binding with original source grants.
    ///
    /// # Errors
    ///
    /// Retains the actual authority and mutation prefix.
    fn rename_with_meter(
        &self, source: &MountPath, destination: &MountPath, replace: bool,
        work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Same rename producer on borrowed lifecycle work.
    ///
    /// # Errors
    ///
    /// Preserves original authority, allocation, and cancellation failures.
    fn rename_with_work(
        &self, source: &MountPath, destination: &MountPath, replace: bool,
        work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;
    /// Mutates actual metadata and optional length through the same source.
    ///
    /// # Errors
    ///
    /// Preserves actual storage, authority, and work failures.
    fn set_attributes_with_meter(
        &self, path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &NativeMountMeterWork,
    ) -> Result<(), MountSourceError>;
    /// Same metadata producer on borrowed lifecycle work.
    ///
    /// # Errors
    ///
    /// Preserves actual partial effects and original causes.
    fn set_attributes_with_work(
        &self, path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;
    /// Flushes the actual candidate on the shared original invocation.
    ///
    /// # Errors
    ///
    /// Preserves durability, authority, and settlement failures.
    fn flush_with_meter(&self, work: &NativeMountMeterWork) -> Result<(), MountSourceError>;
    /// Same candidate flush producer on borrowed lifecycle work.
    ///
    /// # Errors
    ///
    /// Preserves all actual completed work before failure.
    fn flush_with_work(&self, work: &mut NativeMountWork<'_>) -> Result<(), MountSourceError>;

    /// Opens an actual attached handle and reports its pre-erasure SDK payload
    /// claim. The caller retains the Arc in its existing physical-custody owner.
    ///
    /// # Errors
    ///
    /// Preserves actual binding, allocation, authentication, and work failures.
    fn open_file_with_meter(&self, path: &MountPath, work: &NativeMountMeterWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Opens and proves the handle in the same coherent original invocation.
    ///
    /// # Errors
    ///
    /// Preserves original open and lookup failures.
    fn open_file_with_lookup_with_meter(&self, path: &MountPath, work: &NativeMountMeterWork)
        -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError>;
    /// Opens the actual identity just created, without resolving a replacement.
    ///
    /// # Errors
    ///
    /// Preserves binding, authentication, and allocation failures.
    fn open_created_with_meter(&self, path: &MountPath, created: &MountLookup, work: &NativeMountMeterWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Captures the actual detached open-file identity with its original claim.
    ///
    /// # Errors
    ///
    /// Preserves actual capture, allocation, storage, and work failures.
    fn detach_file_with_meter(&self, path: &MountPath, work: &NativeMountMeterWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Creates an empty regular file through the original source engine.
    ///
    /// # Errors
    ///
    /// Preserves actual authority, storage, and work failures.
    fn create_file_with_meter(&self, path: &MountPath, metadata: FileMetadata, work: &NativeMountMeterWork)
        -> Result<MountLookup, MountSourceError>;
    /// Preserves the source's actual absent-since proof while creating.
    ///
    /// # Errors
    ///
    /// Preserves absence, authority, storage, and work failures.
    fn create_absent_file_with_meter(&self, path: &MountPath, metadata: FileMetadata,
        absent_since: ViewStamp, work: &NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
    /// Creates an actual directory through the same source engine.
    ///
    /// # Errors
    ///
    /// Preserves actual authority, storage, and work failures.
    fn create_directory_with_meter(&self, path: &MountPath, metadata: FileMetadata, work: &NativeMountMeterWork)
        -> Result<MountLookup, MountSourceError>;
    /// Consumes the exact already-admitted opaque symbolic-link target.
    ///
    /// # Errors
    ///
    /// Preserves actual authority, mutation, and retained-payload failures.
    fn create_symbolic_link_with_meter(&self, path: &MountPath, target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata, work: &NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
    /// Creates the exact native special-file kind and device identity.
    ///
    /// # Errors
    ///
    /// Preserves actual unsupported, authority, storage, and work failures.
    fn create_special_with_meter(&self, path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>,
        metadata: FileMetadata, work: &NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
    /// Reads the exact owned attribute on the operative source invocation.
    ///
    /// # Errors
    ///
    /// Preserves actual authentication, read, and owned-allocation failures.
    fn read_attribute_with_meter(&self, path: &MountPath, name: &[u8], work: &NativeMountMeterWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned attribute page.
    ///
    /// # Errors
    ///
    /// Preserves exact cursor and owned-allocation failures.
    fn list_attributes_with_meter(&self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMountMeterWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted native attribute value.
    ///
    /// # Errors
    ///
    /// Preserves actual mutation, authority, and retained-payload failures.
    fn write_attribute_with_meter(&self, path: &MountPath, name: &[u8],
        value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode, work: &NativeMountMeterWork)
        -> Result<(), MountSourceError>;
    /// Removes the exact native attribute.
    ///
    /// # Errors
    ///
    /// Preserves actual authority, mutation, and work failures.
    fn remove_attribute_with_meter(&self, path: &MountPath, name: &[u8], work: &NativeMountMeterWork)
        -> Result<(), MountSourceError>;
    /// Clones the actual immutable range between authenticated file identities.
    ///
    /// # Errors
    ///
    /// Preserves detached, stale, storage, authority, and work failures.
    fn clone_range_by_id_with_meter(&self, source_file_id: FileId, source_offset: u64,
        destination_file_id: FileId, destination_offset: u64, length: u64, work: &NativeMountMeterWork)
        -> Result<(), MountSourceError>;
    /// Creates the actual same-volume hard-link binding.
    ///
    /// # Errors
    ///
    /// Preserves actual authority, source identity, and storage failures.
    fn hard_link_with_meter(&self, source: &MountPath, destination: &MountPath, work: &NativeMountMeterWork)
        -> Result<(), MountSourceError>;
    /// Folds the actual source representation and reports exact owned capacity.
    ///
    /// # Errors
    ///
    /// Preserves representation, allocation, and work failures.
    fn folded_path_with_meter(&self, path: &MountPath, work: &NativeMountMeterWork)
        -> Result<Option<(MountPath, u64)>, MountSourceError>;
    /// Queries actual source stability on the operative invocation.
    ///
    /// # Errors
    ///
    /// Preserves original source and work failures.
    fn view_is_stable_with_meter(&self, work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;
    /// Queries the actual source stamp.
    ///
    /// # Errors
    ///
    /// Preserves original source and work failures.
    fn view_stamp_with_meter(&self, work: &NativeMountMeterWork) -> Result<Option<ViewStamp>, MountSourceError>;
    /// Queries the actual writable binding epoch.
    ///
    /// # Errors
    ///
    /// Preserves original source and work failures.
    fn binding_epoch_with_meter(&self, work: &NativeMountMeterWork) -> Result<Option<u64>, MountSourceError>;
    /// Traverses actual source change history under the original item limits.
    ///
    /// # Errors
    ///
    /// Preserves original history, cancellation, and work failures.
    fn unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;
    /// Traverses only actual reported changes through the same history engine.
    ///
    /// # Errors
    ///
    /// Preserves original history, cancellation, and work failures.
    fn reported_unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;
    /// Stops the actual owned source watcher; success confirms physical
    /// quiescence (or that this source owns no active watcher). `bool` is progress.
    ///
    /// # Errors
    ///
    /// Preserves actual pending watcher/join custody and every completed leaf.
    fn shutdown_with_meter(&self, work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;
    /// Retires this held source using only its constructor-owned cleanup credit.
    /// The token bounds this wait; it grants no new I/O or namespace authority.
    ///
    /// # Errors
    ///
    /// Keeps actual pending producers and physical owners available for retry.
    /// Custom sources without an accepted retirement path fail before effects.
    fn shutdown_with_cleanup_wait(
        &self, _wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Completes this source's already accepted physical retirement.
    ///
    /// Only constructor-owned cleanup credit is consumed. This does not admit
    /// callbacks, allocation, source reads, or new namespace mutations.
    ///
    /// # Errors
    ///
    /// Retains physical owners and the original cause until actual retirement.
    /// Custom sources without that original producer fail before effects.
    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Traverses actual changes to this file identity under original limits.
    ///
    /// # Errors
    ///
    /// Preserves history, cancellation, and work failures.
    fn node_unchanged_since_with_meter(&self, file_id: FileId, stamp: ViewStamp,
        work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;
    /// Traverses actual changes to this directory's immediate listing.
    ///
    /// # Errors
    ///
    /// Preserves original history, cancellation, and work failures.
    fn listing_unchanged_since_with_meter(&self, path: &MountPath, stamp: ViewStamp,
        work: &NativeMountMeterWork) -> Result<bool, MountSourceError>;

    /// Whether this source can represent native POSIX named attributes.
    ///
    /// macOS must know this before mounting: advertising `namedattr` for a
    /// profile that rejects POSIX attributes turns ordinary metadata probes
    /// into I/O errors instead of allowing the OS compatibility fallback.
    fn supports_posix_named_attributes(&self) -> bool {
        false
    }

    /// Whether a native handle close is itself a publication boundary.
    ///
    /// Coordinated overlays may batch close-visible mutations until their
    /// explicit lifecycle boundary; direct checkouts publish on close.
    fn flush_on_handle_close(&self) -> bool {
        true
    }

    /// Whether [`Self::flush`] can publish anything. When it cannot, as
    /// under manual publication, every acknowledged mutation is already as
    /// durable as a native durability request (`fsync`) would make it.
    fn flush_publishes(&self) -> bool {
        true
    }

    /// Whether a lookup may observe one coherent source view right now.
    /// Drivers must retry or fail stale while this is false.
    fn view_is_stable(&self) -> bool {
        true
    }

    /// Samples this source's position in the order of view changes, before a
    /// lookup whose result a driver may cache.
    ///
    /// Sources that cannot invalidate exactly return `None`; drivers must then
    /// resolve every lookup through the source.
    fn view_stamp(&self) -> Option<ViewStamp> {
        None
    }

    /// Whether a lookup of `path`, which resolved to `file_id` (`None`: to
    /// nothing) after `stamp` was sampled, still describes the view.
    ///
    /// It does until a change after `stamp` rebinds any component of `path`,
    /// changes the listing or attributes of `path` itself, or changes the
    /// node `file_id` under any of its names. A directory page cached at
    /// `stamp` is current while its directory and every listed entry are.
    fn unchanged_since(
        &self,
        _path: &MountPath,
        _file_id: Option<FileId>,
        _stamp: ViewStamp,
    ) -> bool {
        false
    }

    /// [`Self::unchanged_since`], counting only changes the source reported:
    /// facts read after `stamp` that a driver read again since, with the
    /// same result, still describe the view across changes the source could
    /// not yet confirm ([`super::demand::SourceChange::Unconfirmed`]).
    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.unchanged_since(path, file_id, stamp)
    }

    /// Whether this source reports every change to the node `file_id`, so
    /// facts about it may be kept (by a kernel, or an NFS client) until a
    /// reported change supersedes them. Facts about a node it does not are
    /// read afresh on every use and never kept, whatever else vouches for
    /// them; [`Self::unchanged_since`] and [`Self::node_unchanged_since`]
    /// never vouch for them either.
    fn reports_changes_to(&self, _file_id: FileId) -> bool {
        self.view_stamp().is_some()
    }

    /// Whether facts about the node `file_id`, read after `stamp` was
    /// sampled, still describe it: no change after `stamp` touched the node
    /// under any of its names. Facts that follow from a directory's listing
    /// also depend on its path, which only [`Self::unchanged_since`] checks.
    fn node_unchanged_since(&self, _file_id: FileId, _stamp: ViewStamp) -> bool {
        false
    }

    /// Whether the listing of the directory at `path` is unchanged since
    /// `stamp`: no name directly beneath it was bound, unbound, or rebound
    /// since. Its own binding, and anything deeper, do not count.
    fn listing_unchanged_since(&self, _path: &MountPath, _stamp: ViewStamp) -> bool {
        false
    }

    /// The spelling every equivalent spelling of `path` resolves to, when
    /// this source folds names; `None` when every spelling is distinct.
    ///
    /// Drivers key their records by it, so all spellings of one name share
    /// one record, and never let a case-sensitive kernel cache a spelling on
    /// its own: a change through one spelling could not reach the others.
    fn folded_path(&self, _path: &MountPath) -> Option<MountPath> {
        None
    }

    /// Tells `observer` of every change to this source's view from now on,
    /// together with the origin that made it.
    ///
    /// Drivers keep kernel caches for as long as [`Self::unchanged_since`]
    /// holds, so a source that returns view stamps must report every change
    /// here. Sources without stamps are never cached and need not.
    fn observe_view(&self, _observer: std::sync::Weak<dyn ViewObserver>) {}

    /// Registers an observer on the original native invocation ledger.
    ///
    /// # Errors
    ///
    /// Rejects actual registration storage before allocation; no hidden fallback.
    fn observe_view_with_work(
        &self, observer: std::sync::Weak<dyn ViewObserver>, work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;

    /// Returns once every change made to what this source projects outside
    /// every view of it, and completed before the call, is recorded in the
    /// view. A driver calls it before it brings kernel caches in line with
    /// the view. Sources whose content changes only through their own
    /// methods, or that are never cached, have nothing to wait for.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the changes cannot be read.
    fn fence_changes(&self) -> Result<(), MountSourceError> {
        Ok(())
    }

    /// Fences actual source changes using the same native lifecycle context.
    ///
    /// # Errors
    ///
    /// Preserves completed source/control work and the original typed failure.
    fn fence_changes_with_work(&self, work: &mut NativeMountWork<'_>) -> Result<(), MountSourceError>;

    /// Changes only when existing path or handle bindings may be replaced by
    /// an external source transition. Ordinary mutations never change it;
    /// they invalidate exactly what they change through [`Self::view_stamp`].
    /// Sources without that distinction return `None`.
    fn binding_epoch(&self) -> Option<u64> {
        None
    }

    /// Pins the current coherent projection view for one native callback.
    ///
    /// Stable sources need no retained state; rebindable sources override this
    /// method with an owned synchronization permit.
    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        if !self.view_is_stable() {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Pins source binding identity without treating ordinary native writes
    /// as an external rebind. A callback must separately validate its cache
    /// epoch before reusing a lookup result.
    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        if !self.view_is_stable() || self.binding_epoch() != expected_epoch {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Acquires an actual callback binding permit on the original ledger.
    ///
    /// # Errors
    ///
    /// Rejects changed bindings, cancellation, or owned permit admission.
    fn acquire_binding_lease_with_work(
        &self, expected_epoch: Option<u64>, work: &mut NativeMountWork<'_>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError>;

    /// Looks up one absolute portable path without following links.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without changing checkout state.
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up a path using the caller's original lifecycle admission context.
    ///
    /// # Errors
    ///
    /// Returns the real source failure with completed leaf work retained in
    /// `work`; no unmeasured lookup fallback is permitted.
    fn lookup_with_work(
        &self,
        path: &MountPath,
        work: &mut NativeMountWork<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>;

    /// Looks up one path like [`Self::lookup`], also pinning the exact
    /// external content a regular file's later [`Self::read_pinned`] must
    /// reproduce.
    ///
    /// Sources whose file content changes only through their own mutation
    /// methods have nothing external to pin and return `None`.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without changing checkout state.
    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        Ok(self.lookup(path)?.map(|lookup| (lookup, None)))
    }

    /// Pins source content through the original native admission context.
    ///
    /// # Errors
    ///
    /// Retains actual lookup, pin, and owned-carrier work on every failure.
    fn lookup_pinned_with_work(
        &self, path: &MountPath, work: &mut NativeMountWork<'_>,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError>;
    /// Streams one range of the content a [`Self::lookup_pinned`] pin
    /// promised for `path` to `sink`, in ascending gap-free pieces of at
    /// most `piece` bytes, resolving and opening the source once for the
    /// whole range. Content that ends inside the range ends the stream
    /// with a short or missing last piece.
    ///
    /// # Errors
    ///
    /// Returns `Stale` when that exact content is no longer available, the
    /// first failure `sink` returns, or another typed source failure.
    /// Pieces already delivered stay delivered.
    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let _ = (path, pin, offset, length, piece, sink);
        Err(MountSourceError::Stale)
    }

    /// Streams the exact pinned range on the same native ledger and cancellation.
    ///
    /// # Errors
    ///
    /// Preserves all physically read/delivered prefixes and real owned buffers.
    fn read_pinned_with_work(
        &self, path: &MountPath, pin: MountContentPin, offset: u64, length: u64,
        piece: u32, sink: &mut NativeMountContentSink<'_>, work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;
    /// Opens one regular file as an attached path-independent handle.
    ///
    /// # Errors
    ///
    /// Returns absence, non-regular-kind, storage, authentication,
    /// cancellation, or bounded-work failures.
    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
    /// Opens `path` as [`Self::open_file`] does, with the facts the opened
    /// file had: a source whose open already proved them reports them
    /// without asking the file again.
    ///
    /// # Errors
    ///
    /// Returns the failures of [`Self::open_file`] and [`MountOpenFile::lookup`].
    fn open_file_with_lookup(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        let file = self.open_file(path)?;
        let lookup = file.lookup()?;
        Ok((file, lookup))
    }
    /// Opens `created`, the regular file [`Self::create_file`] just made at
    /// `path`. A source that can address the file by identity binds the
    /// handle to it without resolving `path` again, as a native `O_CREAT`
    /// descriptor names the file it created.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::open_file`].
    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let _ = created;
        self.open_file(path)
    }
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
    fn read_link(&self, path: &MountPath) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;

    /// Reads exact link bytes using the same native lifecycle ledger.
    ///
    /// # Errors
    ///
    /// Returns the original source error with real completed work retained.
    fn read_link_with_work(
        &self, path: &MountPath, work: &mut NativeMountWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
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
    ) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;

    /// Reads one bounded logical range without resetting the native ledger.
    ///
    /// # Errors
    ///
    /// Preserves actual read work and buffer custody on every failure.
    fn read_range_with_work(
        &self, path: &MountPath, offset: u64, length: u32, work: &mut NativeMountWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
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

    /// Reads one actual directory page on the original native context.
    ///
    /// # Errors
    ///
    /// Admits every owned entry/name/cursor and keeps the actual partial prefix.
    fn read_directory_with_work(
        &self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &mut NativeMountWork<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>;
    /// Creates one empty regular file, failing with
    /// [`MountSourceError::AlreadyExists`] when `path` names anything.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates one empty regular file at `path`, which the caller found
    /// absent in the view as of `absent_since`; the source verifies the
    /// absence still holds as it creates, and fails with
    /// [`MountSourceError::AlreadyExists`] otherwise.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure without partial mutation.
    fn create_absent_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        let _ = absent_since;
        self.create_file(path, metadata)
    }
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
    ) -> Result<Option<acyclic_stream::OwnedBytes>, MountSourceError>;
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
    /// Reconciles exact final host states together, preserving hard-link
    /// topology and admitting one bounded checkout candidate.
    ///
    /// # Errors
    ///
    /// Returns a typed capture or publication failure without a partial
    /// checkout candidate.
    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError>;
    /// Reconciles one imported directory and every descendant atomically.
    ///
    /// # Errors
    ///
    /// Returns a typed bounded-capture or publication failure.
    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
    /// Captures exact host paths against the held physical root identity using
    /// the same lifecycle ledger, not an independently reset capture budget.
    ///
    /// # Errors
    ///
    /// Retains completed physical work on cancellation or source failure.
    fn capture_host_paths_with_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: crate::NativeRootIdentity,
        work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;

    /// Captures a host subtree using the same original lifecycle context.
    ///
    /// # Errors
    ///
    /// Retains completed physical work and never publishes a partial candidate.
    fn capture_host_subtree_with_work(
        &self,
        source_root: &Path,
        path: &MountPath,
        expected_root_identity: crate::NativeRootIdentity,
        work: &mut NativeMountWork<'_>,
    ) -> Result<(), MountSourceError>;
}

/// Closed, allocation-free cause set for admission before startup failure
/// storage exists. A supplied source is retained separately, never boxed anew
/// after its original invocation has denied memory or been cancelled.
#[derive(Debug, Error)]
pub enum NativeMountStartupAdmissionError {
    /// Original quota or arithmetic failure.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Original native scope, resource, or cancellation cause, moved intact.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original cancellation or demand admission failure.
    #[error("native startup demand failed: {0}")]
    Demand(#[source] crate::DemandError),
    /// Original fallible SDK metadata allocation failure.
    #[error("native startup allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// Explicit process-shared services have not been initialized.
    #[error("native mount services are not initialized")]
    ServicesCold,
    /// Explicit startup previously failed with this original shared cause.
    #[error("native mount service startup failed: {0}")]
    ServiceUnavailable(#[source] Arc<std::io::Error>),
}

impl From<crate::DemandError> for NativeMountStartupAdmissionError {
    fn from(error: crate::DemandError) -> Self {
        match error {
            crate::DemandError::Work(error) => Self::Work(error),
            error => Self::Demand(error),
        }
    }
}

impl From<NativeMountStartupAdmissionError> for NativeMountError {
    fn from(error: NativeMountStartupAdmissionError) -> Self {
        match error {
            NativeMountStartupAdmissionError::Work(error) => Self::Work(error),
            NativeMountStartupAdmissionError::Materialization(error) => Self::Materialization(error),
            NativeMountStartupAdmissionError::Demand(error) => error.into(),
            NativeMountStartupAdmissionError::Allocation(error) => Self::Allocation(error),
            NativeMountStartupAdmissionError::ServicesCold => Self::ServicesNotInitialized,
            NativeMountStartupAdmissionError::ServiceUnavailable(error) => Self::ServiceUnavailable(error),
        }
    }
}

/// Native mount admission and lifecycle failures.
#[derive(Error)]
pub enum NativeMountError {
    /// This target has no supported native namespace mechanism.
    #[error("native mounts are unsupported on this target")]
    UnsupportedTarget,
    /// Required host driver/device is unavailable.
    /// Static probe diagnostics borrow their literal; an already-owned dynamic
    /// diagnostic moves here without allocating another failure carrier.
    #[error("native mount capability is unavailable: {0}")]
    CapabilityUnavailable(std::borrow::Cow<'static, str>),
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
    /// A native child process exited with its actual unsuccessful OS status.
    #[error("native mount {operation} exited with {status}")]
    DriverExit {
        /// Exact driver operation whose child exited.
        operation: &'static str,
        /// Actual child status, without allocating a replacement diagnostic.
        status: std::process::ExitStatus,
    },
    /// Actual panic payload returned by the driver or thread join.
    #[error("native mount {operation} panicked")]
    DriverPanicked {
        /// Exact driver operation whose panic was observed.
        operation: &'static str,
        /// Original already-owned SDK/thread panic, moved without another Box.
        panic: std::sync::Mutex<Box<dyn std::any::Any + Send>>,
    },
    /// An admitted native constructor or mutation exceeded its exact work bound.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Bounded construction never initializes process-shared services.
    #[error("native mount services are not initialized")]
    ServicesNotInitialized,
    /// Explicit process-shared service startup failed with its original cause.
    #[error("native mount service startup failed: {0}")]
    ServiceUnavailable(#[source] Arc<std::io::Error>),
    /// Native admission, cancellation, or observation failed with its typed cause.
    #[error("native mount demand failed: {0}")]
    Demand(#[source] crate::DemandError),
    /// A native lifecycle leaf failed with its original OS kind and cause.
    #[error("native mount I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Original native materialization cause, moved without a new error box.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original source failure, without allocating a replacement diagnostic.
    #[error("native mount source failed: {0}")]
    Source(#[source] MountSourceError),
    /// Authentic callback authorization failure in its preallocated typed slot.
    /// Its already-funded physical box drops before the outer custody guard.
    #[error("native callback authorization failed")]
    CallbackDenied {
        /// Original typed cause in the same preallocated and unsized box.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Actual slot allocation custody, retired only after box destruction.
        custody: crate::NativeAllocation,
    },
    /// Static driver diagnostic produced by an actual lifecycle failure.
    #[error("native mount lifecycle failed: {0}")]
    Lifecycle(&'static str),
    /// Observed projected or foreign reparse residue is not disposable.
    #[error("native recovery destination is not disposable (reparse tag {reparse_tag:?})")]
    UnsafeRecoveryDestination {
        /// Actual observed reparse tag, if present.
        reparse_tag: Option<u32>,
    },
    /// An accepted native control is still retained and not terminal.
    #[error("native mount cleanup retains a pending control")]
    ControlPending,
    /// Ordinary recovery failed while retaining its actual original owner.
    #[error("native mount recovery failed: {error}")]
    RecoveryFailed {
        /// Original typed cause.
        #[source]
        error: Box<NativeMountError>,
        /// Same fence and control owner to retain for retry.
        owner: NativeMountRecoveryOwner,
    },
    /// A fallible native owned-buffer allocation failed with its actual cause.
    #[error("native mount allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// Startup failed after retaining its actual driver and destination fence.
    #[error("native mount startup failed: {0}")]
    StartupFailed(#[source] NativeMountStartupFailure),
    /// No admitted startup-storage allocation exists yet. The supplied actual
    /// source (and its watcher) remains owned for explicit measured shutdown.
    #[error("native mount startup admission failed: {cause}")]
    StartupAdmissionFailed {
        /// Exact original pre-storage admission cause.
        #[source]
        cause: NativeMountStartupAdmissionError,
        /// Actual supplied source, retained without allocating another owner.
        source: Arc<dyn MountFilesystem>,
    },
    /// Watch construction completed physically but a later source admission
    /// failed. The original watch and sink remain owned for measured cleanup.
    #[error("native mount source watch startup failed: {cause}")]
    SourceWatchStartupFailed {
        /// Exact original late constructor failure.
        #[source]
        cause: crate::DemandError,
        /// Actual original watch and its admitted allocation custody.
        owner: NativeMountSourceWatchOwner,
    },
    /// The actual FUSE shutdown previously failed; it cannot be called complete.
    #[cfg(target_os = "linux")]
    #[error("FUSE shutdown previously failed")]
    FuseShutdownFailed,
    /// The actual FUSE invalidator previously failed.
    #[cfg(target_os = "linux")]
    #[error("FUSE invalidator previously failed")]
    FuseInvalidatorFailed,
    /// Exact native Darwin teardown outcome, retained without a new diagnostic.
    #[cfg(target_os = "macos")]
    #[error("Darwin native teardown failed: {0}")]
    DarwinTeardown(#[source] DarwinTeardownFailure),
    /// The actual FUSE SDK rejected these precise capability flags.
    #[cfg(target_os = "linux")]
    #[error("FUSE rejected capabilities {0:?}")]
    FuseUnsupportedCapabilities(fuser::InitFlags),
}

impl std::fmt::Debug for NativeMountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartupAdmissionFailed { cause, .. } => formatter
                .debug_struct("StartupAdmissionFailed").field("cause", cause).finish_non_exhaustive(),
            _ => std::fmt::Display::fmt(self, formatter),
        }
    }
}

impl From<crate::DemandError> for NativeMountError {
    fn from(error: crate::DemandError) -> Self {
        match error {
            crate::DemandError::Work(error) => Self::Work(error),
            crate::DemandError::Io(error) => Self::Io(error),
            error => Self::Demand(error),
        }
    }
}

impl From<MountSourceError> for NativeMountError {
    fn from(error: MountSourceError) -> Self {
        match error {
            MountSourceError::Work(error) => Self::Work(error),
            MountSourceError::Io(error) => Self::Io(error),
            MountSourceError::NativeWork(error) => Self::Materialization(error),
            MountSourceError::Allocation(error) => Self::Allocation(error),
            MountSourceError::ServicesNotInitialized => Self::ServicesNotInitialized,
            MountSourceError::ServiceUnavailable(error) => Self::ServiceUnavailable(error),
            error => Self::Source(error),
        }
    }
}

enum DriverSession {
    #[cfg(target_os = "linux")]
    Fuse(fuse::FuseSession),
    #[cfg(target_os = "macos")]
    DarwinMount(darwin_mount::DarwinMountSession),
    #[cfg(target_os = "windows")]
    ProjFs(projfs::ProjFsSession),
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    Unsupported,
}

struct DriverStartFailure {
    error: NativeMountError,
    preserve_destination_fence: bool,
    retained_driver: Option<DriverSession>,
}

impl DriverStartFailure {
    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    fn preserving_destination_fence(error: NativeMountError) -> Self {
        Self {
            error,
            preserve_destination_fence: true,
            retained_driver: None,
        }
    }

    fn retaining_driver(error: NativeMountError, driver: DriverSession) -> Self {
        Self { error, preserve_destination_fence: true, retained_driver: Some(driver) }
    }
}

impl From<NativeMountError> for DriverStartFailure {
    fn from(error: NativeMountError) -> Self {
        Self {
            error,
            preserve_destination_fence: false,
            retained_driver: None,
        }
    }
}

impl std::fmt::Debug for DriverStartFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DriverStartFailure")
            .field("error", &self.error)
            .field("preserve_destination_fence", &self.preserve_destination_fence)
            .field("driver_retained", &self.retained_driver.is_some()).finish()
    }
}
impl std::fmt::Display for DriverStartFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, formatter)
    }
}
impl std::error::Error for DriverStartFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.error) }
}

/// Real cleanup custody and driver evidence for one retained native session.
///
/// This is not a physical filesystem probe. A viewer that needs current host
/// facts must query `destination` through its own measured native access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMountCleanupState<'a> {
    /// Stable identity of the actual mounted owner.
    pub mount_id: MountId,
    /// Original destination, borrowed without creating another path carrier.
    pub destination: &'a Path,
    /// Driver, callback, or outstanding native-control custody remains owned.
    pub driver_retained: bool,
    /// The original destination fence remains owned.
    pub destination_fence_retained: bool,
    /// The supplied source and any actually owned watcher remain retained.
    pub source_retained: bool,
    /// The driver has actual successful namespace-detach evidence.
    /// This does not imply callbacks, pending controls, or fence cleanup ended.
    pub namespace_detach_confirmed: bool,
    /// The last measured cleanup failed; retained custody requires explicit retry.
    pub measured_cleanup_failed: bool,
    /// Driver, source/watcher, and destination fence were actually released.
    pub cleanup_complete: bool,
}

/// Borrowed actual recovery custody, not a physical root-presence probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMountRecoveryCleanupState<'a> {
    /// Original destination.
    pub destination: &'a Path,
    /// Identity queried from the actual original held fence file.
    pub fence_identity: crate::NativeRootIdentity,
    /// Original destination fence remains owned.
    pub destination_fence_retained: bool,
    /// Actual native task/process/SDK cleanup remains owned.
    pub controls_pending: bool,
    /// Actual driver evidence; false never invents a namespace-presence result.
    pub namespace_detach_confirmed: bool,
    /// Requested empty-directory removal completed or proved the name absent.
    pub destination_removed: bool,
    /// Last actual measured action failed.
    pub measured_cleanup_failed: bool,
    /// Native controls and the original destination fence were released.
    pub cleanup_complete: bool,
}

#[cfg(target_os = "linux")]
type NativeRecoveryControl = fuse::FuseRecoveryControl;
#[cfg(target_os = "macos")]
type NativeRecoveryControl = darwin_mount::DarwinRecoveryControl;
#[cfg(target_os = "windows")]
type NativeRecoveryControl = projfs::ProjFsRecoveryControl;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[derive(Default)]
struct NativeRecoveryControl;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
impl NativeRecoveryControl {
    fn controls_pending(&self) -> bool { false }
    fn namespace_detach_confirmed(&self) -> bool { false }
    fn retire_controls_with_work(&mut self, _original: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        Ok(())
    }
}

struct NativeRecoveryState {
    #[cfg(windows)]
    destination_identity: Option<crate::NativeRootIdentity>,
    guard: Option<MountDestinationGuard>,
    control: NativeRecoveryControl,
    driver_step_complete: bool,
    destination_removed: bool,
    measured_cleanup_failed: bool,
    panic: Option<Box<dyn std::any::Any + Send>>,
    retirement_error: Option<NativeMountError>,
}

#[derive(Clone, Copy)]
struct NativeRecoverySnapshot {
    fence_identity: crate::NativeRootIdentity,
    destination_fence_retained: bool,
    controls_pending: bool,
    namespace_detach_confirmed: bool,
    destination_removed: bool,
    measured_cleanup_failed: bool,
}

type NativeRecoveryActionResult = Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)>;

enum NativeRecoveryCommand {
    Acquire,
    Action { remove_destination: bool, work: crate::NativeMaterializationWork },
    LastDrop,
}

enum NativeRecoveryReply {
    Acquire(Result<(), NativeMountError>),
    Action(NativeRecoveryActionResult),
}

struct NativeRecoveryMailbox {
    command: Option<NativeRecoveryCommand>,
    reply: Option<NativeRecoveryReply>,
    snapshot: Option<NativeRecoverySnapshot>,
    // Actual state DATA has dropped; this is not a std thread-join proof.
    state_retired: bool,
}

struct NativeRecoveryShared {
    destination: PathBuf,
    mailbox: std::sync::Mutex<NativeRecoveryMailbox>,
    wake: std::sync::Condvar,
    // Physical path, slot, and reply DATA precede their original custody.
    _path_custody: crate::NativeAllocation,
    _custody: crate::NativeAllocation,
}

struct NativeRecoveryWorker {
    shared: Arc<NativeRecoveryShared>,
    state: Box<NativeRecoveryState>,
    original: crate::NativeMaterializationWork,
    final_close: crate::NativeWorkReservation,
    _state_custody: crate::NativeAllocation,
    _closure_custody: crate::NativeAllocation,
}

/// Original recovery control and destination fence, acquired once before action.
///
/// Retain this owner on failure and retry it without acquiring another lease.
/// One administrative thread accepts acquisition before fence birth. Its same
/// slot executes ordinary finite actions and retires real controls on LastDrop;
/// it never grants task authority to a mounted native source.
pub struct NativeMountRecoveryOwner {
    shared: Arc<NativeRecoveryShared>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for NativeMountRecoveryOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeMountRecoveryOwner")
            .field("cleanup", &self.cleanup_state()).finish()
    }
}

impl NativeMountRecoveryOwner {
    /// Administratively acquires the existing destination fence once.
    ///
    /// This explicit unbounded acquisition is outside finite recovery actions.
    /// All known administrative path/slot custody and physical close credit are
    /// accepted before the single worker, and before it acquires a fence.
    /// Initialize native I/O services before a measured macOS action.
    ///
    /// # Errors
    ///
    /// Rejects a live owner or preserves the original acquisition failure.
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| NativeMountError::from(error))?;
        Self::acquire_with_original(destination, original)
    }

    fn acquire_with_original(
        destination: &Path, original: crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let destination = crate::native_host::clone_path_with_work(destination, &mut &original)?;
        let shared_custody = original.claim_allocation(std::mem::size_of::<NativeRecoveryShared>() as u64, 1)?;
        let state_custody = original.claim_allocation(std::mem::size_of::<NativeRecoveryState>() as u64, 1)?;
        let closure_custody = original.claim_allocation(std::mem::size_of::<NativeRecoveryWorker>() as u64, 1)?;
        let final_close = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let control = NativeRecoveryControl::default();
        #[cfg(windows)]
        let control = {
            let mut control = control;
            control.prepare_retirement(&original)?;
            control
        };
        let shared = Arc::new(NativeRecoveryShared {
            destination: destination.0,
            mailbox: std::sync::Mutex::new(NativeRecoveryMailbox {
                command: Some(NativeRecoveryCommand::Acquire), reply: None, snapshot: None, state_retired: false,
            }),
            wake: std::sync::Condvar::new(),
            _path_custody: destination.1, _custody: shared_custody,
        });
        let worker = NativeRecoveryWorker {
            shared: Arc::clone(&shared),
            state: Box::new(NativeRecoveryState {
                #[cfg(windows)]
                destination_identity: None,
                guard: None, control, driver_step_complete: false,
                destination_removed: false, measured_cleanup_failed: false, panic: None, retirement_error: None,
            }),
            original, final_close,
            _state_custody: state_custody, _closure_custody: closure_custody,
        };
        // std's scheduler, native thread stack, Arc headers, and JoinHandle
        // internals are opaque; known SDK closure/slot/path DATA are funded.
        crate::native_host::NativeHostWork::started_io(&mut &worker.original, crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let thread = std::thread::Builder::new().spawn(move || worker.run())?;
        let owner = Self { shared, worker: Some(thread) };
        let result = {
            let mut mailbox = owner.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            while mailbox.reply.is_none() {
                mailbox = owner.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
            }
            match mailbox.reply.take().expect("actual acquisition reply") {
                NativeRecoveryReply::Acquire(result) => result,
                NativeRecoveryReply::Action(_) => unreachable!("acquisition owns the original slot"),
            }
        };
        result?;
        Ok(owner)
    }

    /// Borrows the real original identity, evidence, and retained resources.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountRecoveryCleanupState<'_> {
        let state = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner())
            .snapshot.expect("only an actually acquired owner is published");
        NativeMountRecoveryCleanupState {
            destination: &self.shared.destination,
            fence_identity: state.fence_identity,
            destination_fence_retained: state.destination_fence_retained,
            controls_pending: state.controls_pending,
            namespace_detach_confirmed: state.namespace_detach_confirmed,
            destination_removed: state.destination_removed,
            measured_cleanup_failed: state.measured_cleanup_failed,
            cleanup_complete: !state.destination_fence_retained && !state.controls_pending,
        }
    }
    /// Recovers through the same engine with an explicit unbounded allowance.
    ///
    /// # Errors
    ///
    /// Keeps this original owner available on every failure.
    pub fn recover(&mut self) -> Result<bool, NativeMountError> { self.ordinary_action(true) }

    /// Detaches while preserving the underlying destination and authored residue.
    ///
    /// # Errors
    ///
    /// Keeps the same native controls and fence owned on failure.
    pub fn detach(&mut self) -> Result<bool, NativeMountError> { self.ordinary_action(false) }

    fn ordinary_action(&mut self, remove_destination: bool) -> Result<bool, NativeMountError> {
        if !self.cleanup_state().destination_fence_retained { return Ok(false); }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.mark_failed();
            return Err(error.into());
        }
        self.measured_action(remove_destination, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|(error, _work)| error)
    }

    /// Recovers and removes only an empty disposable destination on finite work.
    ///
    /// # Errors
    ///
    /// Preserves actual work and original control/fence custody. Admission never
    /// claims to abort or roll back an already-entered cooperative native call.
    pub fn recover_measured(
        &mut self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        self.measured_action(true, budget, cancellation)
    }

    /// Detaches on finite work without removing the destination.
    ///
    /// # Errors
    ///
    /// Retains all actual pending controls and the same original fence.
    pub fn detach_measured(
        &mut self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        self.measured_action(false, budget, cancellation)
    }

    fn measured_action(
        &mut self, remove_destination: bool, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.mark_failed();
                return Err((error.into(), work));
            }
        };
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            debug_assert!(mailbox.command.is_none() && mailbox.reply.is_none());
            mailbox.command = Some(NativeRecoveryCommand::Action { remove_destination, work });
            self.shared.wake.notify_one();
        }
        let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
        while mailbox.reply.is_none() {
            mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
        }
        match mailbox.reply.take().expect("actual action reply") {
            NativeRecoveryReply::Action(result) => result,
            NativeRecoveryReply::Acquire(_) => unreachable!("action owns the original slot"),
        }
    }

    fn mark_failed(&mut self) {
        if let Some(snapshot) = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner()).snapshot.as_mut() {
            snapshot.measured_cleanup_failed = true;
        }
    }
}

impl NativeRecoveryState {
    fn finish_with_work(
        &mut self, destination: &Path, remove_destination: bool, work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            if self.guard.is_none() { return Ok(false); }
            if !self.driver_step_complete {
                recover_driver_with_work(&mut self.control, destination, remove_destination, work)?;
                if self.control.controls_pending() { return Err(NativeMountError::ControlPending); }
                self.driver_step_complete = true;
            }
            if remove_destination && !self.destination_removed {
                #[cfg(windows)]
                projfs::remove_empty_recovery_destination_measured(
                    destination, self.destination_identity, &mut &*work,
                )?;
                #[cfg(not(windows))]
                remove_empty_recovery_destination(destination, work)?;
                self.destination_removed = true;
            }
            self.guard.as_mut().expect("original retained fence")
                .release_with_meter(&work.native_mount_work())?;
            self.guard.take();
            Ok(true)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }

    fn snapshot(&self, identity: crate::NativeRootIdentity) -> NativeRecoverySnapshot {
        NativeRecoverySnapshot {
            fence_identity: identity,
            destination_fence_retained: self.guard.is_some(),
            controls_pending: self.control.controls_pending(),
            namespace_detach_confirmed: self.control.namespace_detach_confirmed(),
            destination_removed: self.destination_removed,
            measured_cleanup_failed: self.measured_cleanup_failed,
        }
    }
}

impl NativeRecoveryWorker {
    fn acquire(&mut self) -> Result<crate::NativeRootIdentity, NativeMountError> {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        MountDestinationGuard::acquire_host(
            &self.shared.destination, true, &mut &self.original, Some(&self.original), &mut self.state.guard, true,
        )?;
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        return Err(NativeMountError::UnsupportedTarget);
        let (identity, _) = crate::native_host::file_identity_and_length_with_work(
            self.state.guard.as_ref().and_then(|guard| guard.file.as_ref())
                .ok_or(NativeMountError::InvalidDestination)?, &mut &self.original,
        )?;
        #[cfg(windows)]
        {
            self.state.destination_identity = match crate::native_host::root_path_identity_with_work(
                &self.shared.destination, &mut &self.original,
            ) {
                Ok(identity) => Some(identity),
                Err(crate::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if let Some(identity) = self.state.destination_identity { self.state.control.bind_root_identity(identity); }
        }
        Ok(identity)
    }

    fn retire(&mut self) {
        // This is the same real termination/reap engine, never namespace
        // recovery. An uninterruptible native task keeps its actual fence.
        while self.state.control.controls_pending() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.state.control.retire_controls_with_work(&self.original)
            })) {
                Ok(Ok(())) if !self.state.control.controls_pending() => break,
                Ok(Err(error)) => {
                    if self.state.retirement_error.is_none() { self.state.retirement_error = Some(error); }
                }
                Ok(Ok(())) => {}
                Err(panic) => self.state.panic = Some(panic),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        if let Some(guard) = self.state.guard.as_mut() {
            guard.close_administrative_fence(&mut self.final_close);
        }
        // No unlink, directory removal, or namespace-detach assertion.
        self.state.guard.take();
    }

    fn run(mut self) {
        let mut identity = None;
        loop {
            let command = {
                let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                while mailbox.command.is_none() {
                    mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
                }
                mailbox.command.take().expect("original administrative slot")
            };
            let reply = match command {
                NativeRecoveryCommand::Acquire => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.acquire()));
                    let result = match result {
                        Ok(Ok(actual)) => { identity = Some(actual); Ok(()) }
                        Ok(Err(error)) => Err(error),
                        Err(panic) => {
                            self.state.panic = Some(panic);
                            Err(NativeMountError::Lifecycle("administrative acquisition panicked"))
                        }
                    };
                    if result.is_err() {
                        self.retire();
                        drop(self.state);
                        drop(self._state_custody);
                        let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                        mailbox.reply = Some(NativeRecoveryReply::Acquire(result));
                        mailbox.state_retired = true;
                        self.shared.wake.notify_all();
                        return;
                    }
                    NativeRecoveryReply::Acquire(result)
                }
                NativeRecoveryCommand::Action { remove_destination, work } => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.state.finish_with_work(&self.shared.destination, remove_destination, &work)
                    }));
                    let result = match result {
                        Ok(result) => result,
                        Err(panic) => {
                            self.state.panic = Some(panic);
                            self.state.measured_cleanup_failed = true;
                            Err(NativeMountError::Lifecycle("administrative action panicked"))
                        }
                    };
                    NativeRecoveryReply::Action(match result {
                        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
                        Err(error) => Err((error, work.receipt())),
                    })
                }
                NativeRecoveryCommand::LastDrop => {
                    self.retire();
                    // State DATA is physically destroyed outside the metadata
                    // mutex, before its genuine allocation guard.
                    drop(self.state);
                    drop(self._state_custody);
                    let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                    mailbox.state_retired = true;
                    self.shared.wake.notify_all();
                    return;
                }
            };
            let snapshot = self.state.snapshot(identity.expect("actual original fence identity"));
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            mailbox.snapshot = Some(snapshot);
            mailbox.reply = Some(reply);
            self.shared.wake.notify_all();
        }
    }
}

impl Drop for NativeMountRecoveryOwner {
    fn drop(&mut self) {
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            if !mailbox.state_retired {
                mailbox.command = Some(NativeRecoveryCommand::LastDrop);
                self.shared.wake.notify_one();
            }
        }
        // Dropping a std JoinHandle detaches, never joins the destructor.
        // The accepted worker keeps all actual controls and original custody.
        self.worker.take();
    }
}

fn recover_driver_with_work(
    control: &mut NativeRecoveryControl, destination: &Path,
    disposable: bool, work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    { let _ = disposable; control.recover_measured(destination, &work.native_mount_work()) }
    #[cfg(target_os = "windows")]
    { control.recover_measured(destination, disposable, &mut &*work) }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    { let _ = (control, destination, disposable, work); Err(NativeMountError::UnsupportedTarget) }
}

#[cfg(not(windows))]
fn remove_empty_recovery_destination(
    destination: &Path, work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
    let name = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    let mut host = work;
    let result = crate::native_host::open_root_directory_with_work(parent, &mut host)
        .and_then(|directory| crate::native_host::remove_empty_directory_held_with_work(
            &directory, Path::new(name), &mut host,
        ));
    match result {
        Ok(()) => Ok(()),
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct NativeMountStartupFailureState {
    error: NativeMountError,
    owner: NativeMountSession,
    retired: Option<NativeMountSession>,
}

/// Actual failed constructor custody. Its original error and session remain
/// available for inspection and bounded retry; no rollback is fabricated.
pub struct NativeMountStartupFailure {
    state: Option<Box<NativeMountStartupFailureState>>,
    custody: Option<crate::NativeAllocation>,
}

impl NativeMountStartupFailure {
    /// Original typed constructor failure.
    #[must_use]
    pub fn error(&self) -> &NativeMountError { &self.state.as_ref().expect("live startup failure").error }
    /// Original driver and physical destination fence.
    #[must_use]
    pub fn owner(&self) -> &NativeMountSession { &self.state.as_ref().expect("live startup failure").owner }
    /// Retains the original owner for a bounded cleanup retry.
    pub fn owner_mut(&mut self) -> &mut NativeMountSession { &mut self.state.as_mut().expect("live startup failure").owner }
    /// Original warm-continuation owner, including all unresolved DATA.
    #[must_use]
    pub fn retired_owner(&self) -> Option<&NativeMountSession> {
        self.state.as_ref().expect("live startup failure").retired.as_ref()
    }
    /// Retains the old physical owner for an explicit cleanup retry.
    pub fn retired_owner_mut(&mut self) -> Option<&mut NativeMountSession> {
        self.state.as_mut().expect("live startup failure").retired.as_mut()
    }
    /// Moves the original cause, new source owner, and complete old outcome.
    #[must_use]
    pub fn into_parts(mut self) -> (NativeMountError, NativeMountSession, Option<NativeMountSession>) {
        let NativeMountStartupFailureState { error, owner, retired } = *self.state.take().expect("live startup failure");
        self.retire_storage();
        (error, owner, retired)
    }
    fn retire_storage(&mut self) {
        drop(self.custody.take());
    }
}

impl std::fmt::Debug for NativeMountStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeMountStartupFailure")
            .field("error", self.error()).field("owner", &self.owner().cleanup_state()).finish()
    }
}
impl std::fmt::Display for NativeMountStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}
impl std::error::Error for NativeMountStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.error()) }
}
impl Drop for NativeMountStartupFailure {
    fn drop(&mut self) {
        drop(self.state.take());
        self.retire_storage();
    }
}

struct NativeMountStartupStorage {
    buffer: Option<Box<std::mem::MaybeUninit<NativeMountStartupFailureState>>>,
    custody: Option<crate::NativeAllocation>,
    retired: Option<NativeMountSession>,
}
impl NativeMountStartupStorage {
    fn new(work: &crate::NativeMaterializationWork) -> Result<Self, NativeMountStartupAdmissionError> {
        let custody = work.claim_allocation(
            std::mem::size_of::<NativeMountStartupFailureState>() as u64, 1,
        )?;
        Ok(Self { buffer: Some(Box::new(std::mem::MaybeUninit::uninit())), custody: Some(custody), retired: None })
    }
    fn new_with_meter(
        work: &NativeMountMeterWork,
    ) -> Result<Self, NativeMountStartupAdmissionError> {
        Self::new(&work.native_materialization_work())
    }
    #[allow(unsafe_code, reason = "transfers fully initialized preallocated failure storage")]
    fn failed(mut self, error: NativeMountError, owner: NativeMountSession) -> NativeMountStartupFailure {
        let mut buffer = self.buffer.take().expect("admitted constructor storage");
        buffer.write(NativeMountStartupFailureState { error, owner, retired: self.retired.take() });
        // SAFETY: write initialized this exact admitted allocation completely;
        // conversion transfers it without allocating or copying a second owner.
        let state = unsafe { buffer.assume_init() };
        NativeMountStartupFailure { state: Some(state), custody: self.custody.take() }
    }
}
impl Drop for NativeMountStartupStorage {
    fn drop(&mut self) {
        drop(self.buffer.take());
        drop(self.retired.take());
        drop(self.custody.take());
    }
}

/// Actual preadmitted failure storage. Preparing before an owned source watcher
/// starts prevents a later memory denial from forcing that source into Drop.
pub struct PreparedNativeMountStart {
    storage: NativeMountStartupStorage,
}

/// Admits the existing constructor's actual failure-control allocation without
/// opening the destination or creating another startup engine.
///
/// # Errors
///
/// Preserves original cold-service, cancellation, and allocation failures.
pub fn prepare_native_mount_start_with_work(
    work: &mut NativeMountWork<'_>,
) -> Result<PreparedNativeMountStart, NativeMountError> {
    prepare_native_mount_start(work.work).map_err(Into::into)
}

fn prepare_native_mount_start(
    work: &crate::NativeMaterializationWork,
) -> Result<PreparedNativeMountStart, NativeMountStartupAdmissionError> {
    let _services = adapter::CallbackRuntime::existing_startup()?;
    work.admit(crate::WorkCounters::UNCHARGED)?;
    Ok(PreparedNativeMountStart { storage: NativeMountStartupStorage::new(work)? })
}

/// Admits the same actual startup storage on an already shared original ledger.
///
/// # Errors
///
/// Preserves exact service, cancellation, allocation and quota causes.
pub fn prepare_native_mount_start_with_meter(
    work: &NativeMountMeterWork,
) -> Result<PreparedNativeMountStart, NativeMountError> {
    prepare_native_mount_start(&work.native_materialization_work()).map_err(Into::into)
}

/// Stops a retained source on the caller's original lifecycle ledger. This is
/// also the reachable cleanup path for an inline early startup-admission owner.
///
/// # Errors
///
/// Keeps the source borrowed and owned by its caller after any denied or
/// cancelled native join; no Drop cleanup or second operation ledger is used.
pub fn shutdown_native_mount_source_with_work(
    source: &Arc<dyn MountFilesystem>, work: &mut NativeMountWork<'_>,
) -> Result<bool, NativeMountError> {
    source.shutdown_with_meter(&work.work.native_mount_work()).map_err(Into::into)
}

/// Runs the same source shutdown producer with a finite policy and returns its
/// actual completed prefix while the supplied source remains caller-owned.
///
/// # Errors
///
/// Preserves original source/native failures and pending physical custody.
pub fn shutdown_native_mount_source_measured(
    source: &Arc<dyn MountFilesystem>, budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
    let work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| (error.into(), work))?;
    match shutdown_native_mount_source_with_work(source, &mut NativeMountWork::new(&work)) {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

impl PreparedNativeMountStart {
    /// Starts the same actual platform constructor with its preadmitted storage.
    ///
    /// # Errors
    ///
    /// Every failure retains the supplied source in the original session owner,
    /// including cancellation and destination failures before a driver exists.
    pub fn start_with_work(
        self, request: NativeMountRequest, source: Arc<dyn MountFilesystem>,
        work: &mut NativeMountWork<'_>,
    ) -> Result<NativeMountSession, NativeMountError> {
        start_native_mount_prepared(request, source, false, work.work, self.storage)
    }

    /// Starts the same actual constructor on an already operative original Arc.
    ///
    /// # Errors
    ///
    /// Every late failure retains the original source, driver and fence owner.
    pub fn start_with_meter(
        self, request: NativeMountRequest, source: Arc<dyn MountFilesystem>,
        work: &NativeMountMeterWork,
    ) -> Result<NativeMountSession, NativeMountError> {
        start_native_mount_prepared_with_meter(request, source, false, work, self.storage, None)
    }

    /// Continues only this SDK owner's retired Windows namespace with a freshly
    /// constructed stock source. Old capture DATA must already be settled.
    ///
    /// # Errors
    ///
    /// The original preborn carrier retains the complete old owner and the new
    /// source on every refusal. Before path admission the new owner's destination
    /// is empty; the old owner alone retains the authentic destination.
    pub fn restart_with_meter(
        mut self, retired: NativeMountSession, source: Arc<dyn MountFilesystem>,
        work: &NativeMountMeterWork,
    ) -> Result<NativeMountSession, NativeMountError> {
        let mut next = NativeMountSession {
            mount_id: retired.mount_id, volume_id: retired.volume_id, writable: retired.writable,
            destination: PathBuf::new(), driver: None, destination_guard: None,
            retained_source: Some(source.clone()), startup_source_watch: None,
            preserve_on_drop: true, measured_cleanup_failed: false,
            namespace_detach_confirmed: false, namespace_control_pending: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
            #[cfg(windows)]
            namespace_binding: None,
            _destination_custody: None,
        };
        self.storage.retired = Some(retired);
        let admitted = (|| -> Result<(), NativeMountError> {
            let original = work.native_materialization_work();
            let storage_work = self.storage.custody.as_ref().expect("original preborn custody").work();
            if !storage_work.shares_ledger(&original) || !storage_work.shares_resource_allowance(&original) {
                return Err(NativeMountError::Lifecycle("restart differs from its preborn original work"));
            }
            original.admit(crate::WorkCounters::UNCHARGED)?;
            let old = self.storage.retired.as_ref().expect("complete original owner");
            if !old.cleanup_state().cleanup_complete {
                return Err(NativeMountError::ControlPending);
            }
            #[cfg(not(windows))]
            return Err(NativeMountError::UnsupportedTarget);
            #[cfg(windows)]
            {
                let outcome = old.namespace_outcome.as_ref().ok_or(
                    NativeMountError::Lifecycle("restart has no original SDK namespace outcome"),
                )?;
                if outcome.qualification().is_none() || !outcome.continuation_ready() {
                    return Err(NativeMountError::Lifecycle("original namespace capture DATA is not settled"));
                }
                let lineage = source.native_namespace_lineage().ok_or(
                    NativeMountError::Lifecycle("restart source has no sealed stock lineage"),
                )?;
                if !adapter::namespace_gate_is_fresh(&lineage) || !old.namespace_binding.as_ref().is_some_and(|binding| binding.matches(&lineage)) {
                    return Err(NativeMountError::Lifecycle("restart requires fresh capture gate on original checkout/root/work"));
                }
                if old.retired_callback_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_some() {
                    return Err(NativeMountError::Lifecycle("original callback outcome is not settled"));
                }
                let (destination, custody) = crate::native_host::clone_path_with_work(
                    &old.destination, &mut &original,
                )?;
                next.destination = destination;
                next._destination_custody = Some(custody);
                Ok(())
            }
        })();
        if let Err(error) = admitted {
            return Err(NativeMountError::StartupFailed(self.storage.failed(error, next)));
        }
        let request = NativeMountRequest {
            mount_id: next.mount_id, volume_id: next.volume_id, writable: next.writable,
            destination: std::mem::take(&mut next.destination),
        };
        start_native_mount_prepared_with_meter(
            request, source, true, work, self.storage, next._destination_custody.take(),
        )
    }

    /// Moves an already funded destination into the same native constructor.
    /// No second buffer allocation or adoption is charged.
    ///
    /// # Errors
    ///
    /// Retains destination data and its original guard in every late owner.
    pub fn start_with_native_work(
        self, request: NativeMountRequest, destination_custody: crate::NativeAllocation,
        source: Arc<dyn MountFilesystem>, work: &NativeMountMeterWork,
    ) -> Result<NativeMountSession, NativeMountError> {
        start_native_mount_prepared_with_meter(
            request, source, false, work, self.storage, Some(destination_custody),
        )
    }

    /// Starts a task-backed source whose live guard was installed before capture.
    ///
    /// The supplied guard is the exact already-funded source owner, not a task
    /// permission inferred from its accounting header or destination spelling.
    ///
    /// # Errors
    ///
    /// A missing or different guard returns the preadmitted failure owner with
    /// the original source and destination custody. No startup rollback occurs.
    pub fn start_with_native_work_and_guard(
        self, request: NativeMountRequest, destination_custody: crate::NativeAllocation,
        source: Arc<dyn MountFilesystem>, work: &NativeMountMeterWork,
        guard: Arc<dyn NativeMountCallbackGuard>,
    ) -> Result<NativeMountSession, NativeMountError> {
        let admitted = (|| {
            let installed = source.native_callback_guard().ok_or(
                NativeMountError::Lifecycle("task source callback guard is not installed"),
            )?;
            if !Arc::ptr_eq(&installed, &guard) {
                return Err(NativeMountError::Lifecycle(
                    "task source callback guard differs from its prepared owner",
                ));
            }
            if source.native_original_work()?.is_none() {
                return Err(NativeMountError::Lifecycle(
                    "task source has no original native invocation",
                ));
            }
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        })();
        if let Err(error) = admitted {
            let owner = NativeMountSession {
                mount_id: request.mount_id, destination: request.destination,
                volume_id: request.volume_id, writable: request.writable,
                driver: None, destination_guard: None, retained_source: Some(source),
                startup_source_watch: None,
                preserve_on_drop: true, measured_cleanup_failed: false,
                namespace_detach_confirmed: false, namespace_control_pending: false,
                completed_future_callback_work: crate::WorkCounters::UNCHARGED,
                retired_callback_failure: std::sync::Mutex::new(None),
                #[cfg(windows)]
                namespace_outcome: None,
                #[cfg(windows)]
                namespace_binding: None,
                _destination_custody: Some(destination_custody),
            };
            return Err(NativeMountError::StartupFailed(self.storage.failed(error, owner)));
        }
        start_native_mount_prepared_with_meter(
            request, source, false, work, self.storage, Some(destination_custody),
        )
    }

    /// Places an original failed source constructor into this precreated owner.
    /// The caller must keep the returned session until actual watcher cleanup.
    /// No source Arc or failure Box is constructed after the failure.
    #[must_use]
    pub fn retain_source_startup_failure(
        self, request: NativeMountRequest, destination_custody: crate::NativeAllocation,
        error: NativeMountError,
    ) -> NativeMountError {
        let (startup_source_watch, error) = match error {
            NativeMountError::SourceWatchStartupFailed { cause, owner } =>
                (Some(owner), NativeMountError::Demand(cause)),
            error => (None, error),
        };
        let owner = NativeMountSession {
            mount_id: request.mount_id, destination: request.destination,
            volume_id: request.volume_id, writable: request.writable,
            driver: None, destination_guard: None, retained_source: None,
            startup_source_watch,
            preserve_on_drop: true, measured_cleanup_failed: true,
            namespace_detach_confirmed: false, namespace_control_pending: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
            #[cfg(windows)]
            namespace_binding: None,
            _destination_custody: Some(destination_custody),
        };
        NativeMountError::StartupFailed(self.storage.failed(error, owner))
    }
}

/// Upper lifecycle payload held by the existing physical driver loop.
///
/// The containing driver's original admission covers this inline slot. It
/// never owns that driver or its runtime, and cannot authorize root deletion.
pub(super) struct NativeMountRetirement {
    pub(super) retained_source: Option<Arc<dyn MountFilesystem>>,
    pub(super) destination_guard: Option<MountDestinationGuard>,
    pub(super) startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    #[cfg(windows)]
    warm_namespace: Option<projfs::ProjFsNamespaceQualification>,
}

impl NativeMountRetirement {
    /// Called only after the driver has actually drained namespace callbacks.
    pub(super) fn retire_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<bool, NativeMountError> {
        self.retire_with_cleanup(Some(wait))
    }

    /// Continues only the original producer's already accepted retirement.
    pub(super) fn retire_accepted_cleanup(&mut self) -> Result<bool, NativeMountError> {
        self.retire_with_cleanup(None)
    }

    fn retire_with_cleanup(
        &mut self, wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, NativeMountError> {
        let mut changed = self.retire_source_and_watch_with_cleanup(wait)?;
        if let Some(guard) = self.destination_guard.as_mut() {
            match wait {
                Some(wait) => guard.release_with_cleanup_wait(wait),
                None => guard.release_accepted_cleanup(),
            }?;
            drop(self.destination_guard.take());
            changed = true;
        }
        Ok(changed)
    }

    /// Drains source producers after callback admission closes, before the
    /// driver's own namespace teardown. The destination fence remains held.
    pub(super) fn retire_source_and_watch_with_cleanup(
        &mut self, wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, NativeMountError> {
        let mut changed = false;
        if let Some(source) = self.retained_source.as_ref() {
            match wait {
                Some(wait) => source.shutdown_with_cleanup_wait(wait),
                None => source.shutdown_accepted_cleanup(),
            }?;
            drop(self.retained_source.take());
            changed = true;
        }
        if let Some(watch) = self.startup_source_watch.as_mut() {
            let result = if watch.is_stopped() { Ok(false) } else {
                match wait {
                    Some(wait) => watch.stop_with_cleanup_wait(wait),
                    None => watch.stop_accepted_cleanup(),
                }
            };
            if watch.is_stopped() {
                drop(self.startup_source_watch.take());
                changed = true;
            }
            result?;
        }
        Ok(changed)
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retained_source.is_none() && self.startup_source_watch.is_none()
            && self.destination_guard.is_none()
    }
}

/// Process-owned mounted namespace. Its accepted driver loop owns retirement;
/// dropping this facade signals that owner without joining or retrying I/O.
pub struct NativeMountSession {
    mount_id: MountId,
    destination: PathBuf,
    volume_id: VolumeId,
    writable: bool,
    driver: Option<DriverSession>,
    destination_guard: Option<MountDestinationGuard>,
    retained_source: Option<Arc<dyn MountFilesystem>>,
    // A genuine source-constructor failure can precede a completed Source Arc.
    startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    preserve_on_drop: bool,
    measured_cleanup_failed: bool,
    namespace_detach_confirmed: bool,
    // Separate actual detach evidence from an unresolved, ownerless control.
    namespace_control_pending: bool,
    completed_future_callback_work: crate::WorkCounters,
    retired_callback_failure: std::sync::Mutex<Option<NativeMountCallbackFailure>>,
    // SDK namespace DATA can outlive physical retirement without retaining
    // a source, context, root handle, or provider callback.
    #[cfg(windows)]
    namespace_outcome: Option<projfs::ProjFsNamespaceOutcome>,
    #[cfg(windows)]
    namespace_binding: Option<NativeMountNamespaceBinding>,
    // Destination data and all native owners physically drop before its guard.
    _destination_custody: Option<crate::NativeAllocation>,
}

/// Owned accounting activation for this existing physical namespace. It owns
/// only aliases of the original SDK context and work; it is not a task grant.
/// Keep it alive until accepted native I/O has physically settled, including
/// cancellation. Other clients of this namespace may consume the same budget.
#[must_use]
pub struct OwnedNativeMountOriginalWorkScope {
    _driver: NativeDriverOriginalWorkScope,
}

impl OwnedNativeMountOriginalWorkScope {
    /// Retains this same driver's activation in an accepted native I/O payload.
    /// Clones only preexisting context/work aliases and increments the existing
    /// inline depth; it creates no heap allocation or additional task authority.
    ///
    /// # Errors
    ///
    /// Preserves a competing activation or exhausted inline depth before I/O.
    pub fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        let driver = match &self._driver {
            #[cfg(target_os = "linux")]
            NativeDriverOriginalWorkScope::Fuse(scope) =>
                NativeDriverOriginalWorkScope::Fuse(scope.try_clone_for_io()?),
            #[cfg(target_os = "macos")]
            NativeDriverOriginalWorkScope::Darwin(scope) =>
                NativeDriverOriginalWorkScope::Darwin(scope.try_clone_for_io()?),
            #[cfg(target_os = "windows")]
            NativeDriverOriginalWorkScope::ProjFs(scope) =>
                NativeDriverOriginalWorkScope::ProjFs(scope.try_clone_for_io()?),
        };
        Ok(Self { _driver: driver })
    }
}


enum NativeDriverOriginalWorkScope {
    #[cfg(target_os = "linux")]
    Fuse(fuse::FuseStartupMeterScope),
    #[cfg(target_os = "macos")]
    Darwin(darwin_mount::DarwinStartupMeterScope),
    #[cfg(target_os = "windows")]
    ProjFs(projfs::ProjectionWorkScope),
}

impl NativeMountSession {
    /// Activates the original meter without borrowing this session across I/O.
    /// Path, volume and current task authorization must be checked separately.
    ///
    /// # Errors
    ///
    /// A different active original meter is rejected before any operation.
    pub fn begin_original_work(
        &self, work: &NativeMountMeterWork,
    ) -> Result<OwnedNativeMountOriginalWorkScope, NativeMountError> {
        let driver = match self.driver.as_ref() {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) =>
                NativeDriverOriginalWorkScope::Fuse(session.begin_original_work(work)?),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) =>
                NativeDriverOriginalWorkScope::Darwin(session.begin_original_work(work)?),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) =>
                NativeDriverOriginalWorkScope::ProjFs(session.begin_original_work(work)?),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => {
                let _ = work;
                return Err(NativeMountError::UnsupportedTarget);
            }
            None => return Err(NativeMountError::Io(std::io::ErrorKind::NotConnected.into())),
        };
        Ok(OwnedNativeMountOriginalWorkScope { _driver: driver })
    }

    /// Runs native I/O under this original accounting scope. Each accepted I/O
    /// payload must own a `try_clone_for_io()` fork through physical completion:
    /// dropping this caller future alone does not drain native jobs.
    /// This is not client isolation or permission: all namespace activity during
    /// the activation can consume its budget, and reply delivery stays unknown.
    ///
    /// # Errors
    ///
    /// Preserves a rejected activation without executing `operation`.
    pub async fn with_original_work<T, F: std::future::Future<Output = T>>(
        &self, work: &NativeMountMeterWork,
        operation: impl FnOnce(&OwnedNativeMountOriginalWorkScope) -> F,
    ) -> Result<T, NativeMountError> {
        let scope = self.begin_original_work(work)?;
        let value = operation(&scope).await;
        drop(scope);
        Ok(value)
    }

    /// Authenticated active kernel-root identity captured by this driver during
    /// its original constructor. This getter performs no path reopen or stat.
    #[must_use]
    pub fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        match self.driver.as_ref() {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.native_root_identity(),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.native_root_identity(),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.native_root_identity(),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => None,
            None => None,
        }
    }

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

    /// Protects this actual owner during an enclosing measured lifecycle.
    ///
    /// No I/O, allocation, or completed/failed-detach evidence is manufactured.
    pub fn retain_measured_cleanup_owner(&mut self) {
        self.preserve_on_drop = true;
    }

    /// Records a genuine failure in the enclosing measured cleanup stage.
    ///
    /// The same driver/fence remains owned; Drop never retries outside the
    /// caller's receipt.
    pub fn record_measured_cleanup_failure(&mut self) {
        self.preserve_on_drop = true;
        self.measured_cleanup_failed = true;
    }

    /// Borrows actual cleanup identity, retained ownership, and detach evidence.
    ///
    /// Failure never publishes a fabricated stopped namespace. Keep this owner
    /// for an explicit measured retry while `cleanup_complete` is false.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountCleanupState<'_> {
        let (driver_fence, driver_source) = self.driver.as_ref()
            .map_or((false, false), driver_retirement_ownership);
        NativeMountCleanupState {
            mount_id: self.mount_id,
            destination: &self.destination,
            driver_retained: self.driver.is_some(),
            destination_fence_retained: self.destination_guard.is_some() || driver_fence,
            source_retained: self.retained_source.is_some() || self.startup_source_watch.is_some() || driver_source,
            namespace_detach_confirmed: self.driver.as_ref().map_or(
                self.namespace_detach_confirmed, driver_namespace_detach_confirmed,
            ),
            measured_cleanup_failed: self.measured_cleanup_failed,
            cleanup_complete: self.driver.is_none() && self.destination_guard.is_none()
                && self.retained_source.is_none() && self.startup_source_watch.is_none()
                && !self.namespace_control_pending,
        }
    }

    #[cfg(windows)]
    pub(super) fn take_retired_namespace_outcome(
        &mut self,
    ) -> Option<projfs::ProjFsNamespaceOutcome> {
        if !self.cleanup_state().cleanup_complete { return None; }
        self.namespace_outcome.take()
    }

    /// Moves the first retained fatal SDK callback failure and its actual
    /// completed request work. Draining it does not replenish grants, clear
    /// cancellation, or re-enable an unreconciled mutation.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        let retained = self.retired_callback_failure.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(failure) = retained {
            return Some((failure.cause, failure.work));
        }
        let failure: Option<NativeMountCallbackFailure> = match self.driver.as_ref() {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.take_callback_failure(),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.take_callback_failure(),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.take_callback_failure(),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => None,
            None => None,
        };
        failure.map(|failure| (failure.cause, failure.work))
    }

    /// Completed future SDK request work, including ordinary errno answers.
    /// Active startup/revalidation/stop work remains on its original ledger and
    /// is never folded twice. Peaks are maxima of per-operation logical scopes;
    /// the aggregate is not a concurrent live-memory or RSS measurement.
    #[must_use]
    pub fn completed_callback_work(&self) -> crate::WorkCounters {
        self.driver.as_ref().map_or(
            self.completed_future_callback_work, driver_completed_callback_work,
        )
    }

    /// Client receipt of a provider reply is not observable through the native
    /// SDK reply APIs. `None` means unknown delivery, not an observed failure.
    /// Borrowed terminal provider IPC is outside source/backend work counters.
    #[must_use]
    pub fn callback_reply_delivery(&self) -> Option<bool> {
        None
    }

    pub(crate) fn flush_callbacks(&self) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.flush_callbacks(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Some(_) => Ok(()),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => Err(NativeMountError::UnsupportedTarget),
            None => Ok(()),
        }
    }

    /// Flushes callback work on the enclosing original lifecycle ledger.
    ///
    /// # Errors
    ///
    /// Preserves the actual partial callback/control work and typed cause.
    pub fn flush_callbacks_with_work(
        &self, work: &mut NativeMountWork<'_>,
    ) -> Result<(), NativeMountError> {
        self.flush_callbacks_with_meter(&work.work.native_mount_work())
    }

    /// Flushes the actual SDK callbacks without borrowing work across a wait.
    ///
    /// # Errors
    ///
    /// Preserves all actual original-meter control and callback prefixes.
    pub fn flush_callbacks_with_meter(
        &self, work: &NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.flush_callbacks_with_meter(work),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Some(_) => Ok(()),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => {
                let _ = work;
                Err(NativeMountError::UnsupportedTarget)
            }
            None => Ok(()),
        }
    }

    /// Revalidates through the existing physical driver and original ledger.
    ///
    /// # Errors
    ///
    /// Retains actual completed invalidation/probe work on every failure.
    pub fn revalidate_with_work(
        &self, work: &mut NativeMountWork<'_>,
    ) -> Result<(), NativeMountError> {
        self.revalidate_with_meter(&work.work.native_mount_work())
    }

    /// Revalidates on the same original shared callback and control meter.
    ///
    /// # Errors
    ///
    /// Retains exact completed SDK/source work on every error.
    pub fn revalidate_with_meter(
        &self, work: &NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.revalidate_with_meter(work),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.revalidate_with_meter(work),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.revalidate_with_meter(work),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => {
                let _ = work;
                Err(NativeMountError::UnsupportedTarget)
            }
            None => Err(NativeMountError::Io(std::io::ErrorKind::NotConnected.into())),
        }
    }

    /// Drops the kernel's cached entry/attributes for one mount-relative
    /// path (leading `/` optional). Linux FUSE also invalidates resident
    /// file data and the parent's cached listing; Windows `ProjFS` drops an
    /// unmodified projected placeholder and every cached absence. A change made
    /// to the source around the mount, such as a removed route, becomes
    /// visible through this call.
    /// Linux FUSE supports nested entries when the parent directory has a
    /// cached inode. The macOS NFS client cannot be told to drop its caches:
    /// it sees the change at its next revalidation, which every open performs
    /// and cached names and attributes undergo within one second.
    ///
    /// # Errors
    ///
    /// Returns a driver error when the transport cannot invalidate.
    pub fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        match &self.driver {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.invalidate(path),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.invalidate(path),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.invalidate(path),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => {
                let _ = path;
                Err(NativeMountError::UnsupportedTarget)
            }
            None => Err(NativeMountError::Driver("session is stopped".to_owned())),
        }
    }

    /// Brings kernel caches in line with a changed source view.
    ///
    /// A mount owner that rebinds or advances its source calls this before
    /// exposing the new view, even when the advance failed part way. Once it
    /// returns, no access through the mount observes a name, attribute, or
    /// file content that a change made around the mount before the call
    /// superseded. Linux FUSE retains entries, attributes, and file data
    /// until invalidated and drops what every change made around the mount
    /// superseded as the source reports it; this waits until every change
    /// reported so far has reached the kernel. Windows `ProjFS` forgets the
    /// absences it caches without expiry. The macOS NFS client cannot be
    /// told to drop anything, so this waits until everything it cached
    /// before such a change has expired (at most the one-second attribute
    /// timeout plus a reply-delivery allowance; see the module docs for the
    /// `access(2)` exception). Views that only the mount itself changed
    /// make this a no-op.
    ///
    /// # Errors
    ///
    /// Returns a driver error when the kernel rejects an invalidation.
    pub fn revalidate(&self) -> Result<(), NativeMountError> {
        match &self.driver {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.revalidate(),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.revalidate(),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.revalidate(),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => Ok(()),

            None => Err(NativeMountError::Driver("session is stopped".to_owned())),
        }
    }

    /// Takes the operation of every request the FUSE session served so far.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn take_fuse_requests(&self) -> Vec<&'static str> {
        match &self.driver {
            Some(DriverSession::Fuse(session)) => session.take_requests(),
            None => Vec::new(),
        }
    }

    /// Detaches the kernel namespace and ends the driver session; returns
    /// whether a live session was actually stopped.
    ///
    /// # Errors
    ///
    /// Returns a platform error if the kernel namespace cannot be detached.
    pub fn stop(&mut self) -> Result<bool, NativeMountError> {
        if self.driver.is_none() && self.destination_guard.is_none()
            && self.retained_source.is_none() && self.startup_source_watch.is_none() {
            return Ok(false);
        }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.preserve_on_drop = true;
            self.measured_cleanup_failed = true;
            return Err(error.into());
        }
        self.stop_measured(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|(error, _work)| error)
    }

    /// Detaches through the existing platform driver and destination-fence
    /// machinery, admitting every actual native leaf on one original ledger.
    ///
    /// SDK operations count at their concrete API boundaries; opaque SDK/OS
    /// internal memory and RSS are not guessed. Native calls are cooperative:
    /// admission and cancellation do not imply an already-entered OS call was
    /// aborted or its physical effects rolled back.
    ///
    /// # Errors
    ///
    /// Preserves the completed work prefix and remaining driver/fence custody.
    /// Callers must retain this session for retry or inspection. Drop after a
    /// measured failure never performs an unbounded cleanup outside the receipt.
    pub fn stop_measured(
        &mut self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.preserve_on_drop = true;
                self.measured_cleanup_failed = true;
                return Err((error.into(), work));
            }
        };
        match self.stop_with_work(&mut NativeMountWork::new(&work)) {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
            Err(error) => Err((error, work.receipt())),
        }
    }

    /// Continues cleanup on an enclosing lifecycle's original borrowed ledger.
    ///
    /// # Errors
    ///
    /// Keeps all completed leaf counters in `work` and retains the original
    /// driver/fence owner for explicit retry. No child ledger is constructed.
    pub fn stop_with_work(
        &mut self,
        work: &mut NativeMountWork<'_>,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_meter(&work.work.native_mount_work())
    }

    /// Continues the same physical cleanup engine on the original shared Arc.
    ///
    /// # Errors
    ///
    /// Retains the actual source, driver, pending controls and fence for retry.
    pub fn stop_with_meter(
        &mut self, work: &NativeMountMeterWork,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::Meter(work))
    }

    /// Retires the original namespace using its before-start accepted controls.
    /// The token limits waiting; no root budget, cancellation, or grant is reset.
    ///
    /// # Errors
    ///
    /// Preserves the actual remaining driver, source, watch and destination
    /// fence when physical retirement is incomplete.
    pub fn stop_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::AcceptedWait(wait))
    }

    fn stop_with_context(
        &mut self, work: NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        if self.driver.is_none() && self.namespace_control_pending {
            self.preserve_on_drop = true;
            self.measured_cleanup_failed = true;
            return Err(NativeMountError::ControlPending);
        }
        let result = (|| {
            let mut changed = false;
            if let Some(driver) = self.driver.as_mut() {
                match &work {
                    NativeSessionStopWork::Meter(meter) => stop_driver_with_meter(driver, meter)?,
                    NativeSessionStopWork::AcceptedWait(wait) => stop_driver_with_cleanup_wait(driver, wait)?,
                }
                self.namespace_detach_confirmed = driver_namespace_detach_confirmed(driver);
                self.namespace_control_pending = false;
                // The physical driver has settled callbacks before this copy.
                // Preserve both totals and the original fault before SDK data
                // and their context are actually dropped.
                self.completed_future_callback_work = driver_completed_callback_work(driver);
                *self.retired_callback_failure.get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    driver_take_callback_failure(driver);
                #[cfg(windows)]
                {
                    let DriverSession::ProjFs(session) = driver;
                    self.namespace_outcome = session.take_namespace_outcome();
                }
                drop(self.driver.take());
                changed = true;
            }
            if let Some(source) = self.retained_source.as_ref() {
                match &work {
                    NativeSessionStopWork::Meter(meter) => source.shutdown_with_meter(meter),
                    NativeSessionStopWork::AcceptedWait(wait) => source.shutdown_with_cleanup_wait(wait),
                }.map_err(NativeMountError::from)?;
                // The source confirmed actual watcher quiescence before this
                // owner releases its Arc; failed/cancelled joins keep it intact.
                drop(self.retained_source.take());
                changed = true;
            }
            if let Some(owner) = self.startup_source_watch.as_mut() {
                let stopped = if owner.is_stopped() { Ok(false) } else {
                    match &work {
                        NativeSessionStopWork::Meter(meter) => owner.stop_with_meter(meter),
                        NativeSessionStopWork::AcceptedWait(wait) => owner.stop_with_cleanup_wait(wait).map_err(NativeMountError::from),
                    }
                };
                if owner.is_stopped() {
                    // Terminal transfer of already owned data is not a new
                    // native permission, and survives a late admission error.
                    drop(self.startup_source_watch.take());
                    changed = true;
                }
                stopped?;
            }
            if let Some(guard) = self.destination_guard.as_mut() {
                match &work {
                    NativeSessionStopWork::Meter(meter) => guard.release_with_meter(meter)?,
                    NativeSessionStopWork::AcceptedWait(wait) => guard.release_with_cleanup_wait(wait)?,
                }
                drop(self.destination_guard.take());
                changed = true;
            }
            Ok(changed)
        })();
        self.preserve_on_drop = result.is_err();
        self.measured_cleanup_failed = result.is_err();
        result
    }
}

enum NativeSessionStopWork<'a> {
    Meter(&'a NativeMountMeterWork),
    AcceptedWait(&'a crate::CancellationToken),
}

#[cfg(test)]
fn stop_owned_session<D, G, E>(
    driver: &mut Option<D>,
    destination_guard: &mut Option<G>,
    mut stop: impl FnMut(&mut D) -> Result<(), E>,
    mut release: impl FnMut(&mut G) -> Result<(), E>,
) -> Result<bool, E> {
    stop_owned_session_with_work(
        driver, destination_guard, &mut (),
        |driver, ()| stop(driver), |guard, ()| release(guard),
    )
}

fn stop_owned_session_with_work<D, G, W, E>(
    driver: &mut Option<D>,
    destination_guard: &mut Option<G>,
    work: &mut W,
    mut stop: impl FnMut(&mut D, &mut W) -> Result<(), E>,
    mut release: impl FnMut(&mut G, &mut W) -> Result<(), E>,
) -> Result<bool, E> {
    if driver.is_none() && destination_guard.is_none() {
        return Ok(false);
    }
    if let Some(active) = driver.as_mut() {
        stop(active, work)?;
        driver.take();
    }
    if let Some(guard) = destination_guard.as_mut() {
        release(guard, work)?;
        destination_guard.take();
    }
    Ok(true)
}

#[cfg(test)]
fn drop_owned_session<D, G, E>(
    driver: &mut Option<D>,
    destination_guard: &mut Option<G>,
    stop: impl FnMut(&mut D) -> Result<(), E>,
    release: impl FnMut(&mut G) -> Result<(), E>,
) {
    // Failed retirement leaves the real values in the caller's existing slots
    // for an explicit retry; this control never leaks their ownership.
    let _ = stop_owned_session(driver, destination_guard, stop, release);
}


impl Drop for NativeMountSession {
    fn drop(&mut self) {
        // Each accepted driver loop retains the source, destination fence and
        // native handles through physical retirement. Its own Drop only sends
        // the already admitted stop signal, including failed startup.
        drop(self.driver.take());
        // Before driver acceptance no destination fence has been acquired.
        // Source/watch birth producers retain their own physical retirement.
        drop(self.startup_source_watch.take());
        drop(self.retained_source.take());
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
    mount_native_ordinary(request, source, false)
}

fn mount_native_ordinary(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>, allow_nonempty: bool,
) -> Result<NativeMountSession, NativeMountError> {
    let cancellation = crate::CancellationToken::new();
    let work = match crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation) {
        Ok(work) => work,
        Err((error, _work)) => return Err(NativeMountError::StartupAdmissionFailed {
            cause: error.into(), source,
        }),
    };
    // Ordinary bootstrap is explicit, but the supplied source still needs
    // genuine failure custody if that bootstrap cannot complete.
    let storage = match NativeMountStartupStorage::new(&work) {
        Ok(storage) => storage,
        Err(cause) => return Err(NativeMountError::StartupAdmissionFailed { cause, source }),
    };
    let initialized = initialize_native_mount_services().and_then(|()| {
        acyclic_native_runtime::initialize_native_io_services().map_err(Into::into)
    });
    if let Err(error) = initialized {
        let owner = NativeMountSession {
            mount_id: request.mount_id, destination: request.destination,
            volume_id: request.volume_id, writable: request.writable,
            driver: None, destination_guard: None, retained_source: Some(source),
            startup_source_watch: None,
            preserve_on_drop: true, measured_cleanup_failed: false,
            namespace_detach_confirmed: false, namespace_control_pending: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
            #[cfg(windows)]
            namespace_binding: None,
            _destination_custody: None,
        };
        return Err(NativeMountError::StartupFailed(storage.failed(error, owner)));
    }
    start_native_mount_prepared(request, source, allow_nonempty, &work, storage)
}

/// Starts the real platform driver on the caller's original lifecycle ledger.
/// Process-shared services must have been explicitly initialized beforehand.
/// Owned request/source carriers are transferred, not counted as new buffers.
///
/// # Errors
///
/// Admits every actual destination-fence and platform leaf. A partial driver
/// startup returns [`NativeMountError::StartupFailed`] with its original owner;
/// no hidden unbounded rollback occurs and cancellation is cooperative.
pub fn mount_native_with_work(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>,
    work: &mut NativeMountWork<'_>,
) -> Result<NativeMountSession, NativeMountError> {
    mount_native_start_with_work(request, source, false, work.work)
}

/// Finite projection of the same native constructor and actual work prefix.
///
/// # Errors
///
/// Retains original causes and any partially started driver/fence owner.
pub fn mount_native_measured(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>,
    budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<NativeMountSession>, (NativeMountError, crate::WorkCounters)> {
    let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
        Ok(work) => work,
        Err((error, work)) => return Err((NativeMountError::StartupAdmissionFailed {
            cause: error.into(), source,
        }, work)),
    };
    match mount_native_with_work(request, source, &mut NativeMountWork::new(&work)) {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

fn mount_native_start_with_work(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>, allow_nonempty: bool,
    work: &crate::NativeMaterializationWork,
) -> Result<NativeMountSession, NativeMountError> {
    let prepared = match prepare_native_mount_start(work) {
        Ok(prepared) => prepared,
        Err(cause) => return Err(NativeMountError::StartupAdmissionFailed { cause, source }),
    };
    start_native_mount_prepared(request, source, allow_nonempty, work, prepared.storage)
}

fn start_native_mount_prepared(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>, allow_nonempty: bool,
    work: &crate::NativeMaterializationWork, storage: NativeMountStartupStorage,
) -> Result<NativeMountSession, NativeMountError> {
    start_native_mount_prepared_with_meter(
        request, source, allow_nonempty, &work.native_mount_work(), storage, None,
    )
}

fn start_native_mount_prepared_with_meter(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>, allow_nonempty: bool,
    work: &NativeMountMeterWork, storage: NativeMountStartupStorage,
    destination_custody: Option<crate::NativeAllocation>,
) -> Result<NativeMountSession, NativeMountError> {
    // The existing driver loop accepts its lifetime before this stack factory
    // acquires a physical fence. The factory never escapes or allocates a frame.
    let mut retirement = Some(NativeMountRetirement {
        retained_source: Some(Arc::clone(&source)),
        destination_guard: None, startup_source_watch: None,
        #[cfg(windows)]
        warm_namespace: storage.retired.as_ref().and_then(|old|
            old.namespace_outcome.as_ref().and_then(projfs::ProjFsNamespaceOutcome::qualification)),
    });
    let mut driver = None;
    #[cfg(windows)]
    let mut namespace_binding = None;
    #[cfg(windows)]
    let lineage_admission = source.native_namespace_lineage().map(|lineage|
        adapter::retain_namespace_lineage(&lineage, work, storage.retired.is_some())
            .map(|binding| namespace_binding = Some(binding))
    ).transpose().map(|_| ()).map_err(NativeMountError::from);
    #[cfg(not(windows))]
    let lineage_admission: Result<(), NativeMountError> = Ok(());
    let result = lineage_admission.map_err(DriverStartFailure::from).and_then(|()| start_driver_into(
        &request, source, work, &mut driver, &mut retirement, |packet| {
            let _services = adapter::CallbackRuntime::existing()?;
            work.meter.lock().check()?;
            MountDestinationGuard::acquire_with_meter(
                &request.destination, allow_nonempty, work, &mut packet.destination_guard,
            )
        },
    ));
    // A prebirth rejection leaves its packet here. An accepted loop owns it
    // even on failure; the retained driver signals that same owner to retire.
    let (destination_guard, retained_source, startup_source_watch) = match retirement {
        Some(packet) => (packet.destination_guard, packet.retained_source, packet.startup_source_watch),
        None => (None, None, None),
    };
    let mut owner = NativeMountSession {
        mount_id: request.mount_id, destination: request.destination, driver,
        volume_id: request.volume_id, writable: request.writable,
        destination_guard, retained_source, startup_source_watch,
        preserve_on_drop: result.is_err(), measured_cleanup_failed: false,
        namespace_detach_confirmed: false, namespace_control_pending: false,
        completed_future_callback_work: crate::WorkCounters::UNCHARGED,
        retired_callback_failure: std::sync::Mutex::new(None),
        #[cfg(windows)]
        namespace_outcome: None,
        #[cfg(windows)]
        namespace_binding,
        _destination_custody: destination_custody,
    };
    match result {
        Ok(()) => {
            drop(storage);
            Ok(owner)
        },
        Err(mut failure) => {
            if owner.driver.is_none() { owner.driver = failure.retained_driver.take(); }
            owner.namespace_control_pending = failure.preserve_destination_fence;
            Err(NativeMountError::StartupFailed(storage.failed(failure.error, owner)))
        },
    }
}

/// Starts a native projection mounted directly over an existing,
/// non-empty directory — its contents are shadowed for the mount's
/// duration and reappear once the returned session stops. Windows `ProjFS`
/// cannot shadow local entries and this operation is unavailable there.
///
/// For callers that must project a checkout at a specific real path that
/// already has content (for example, redirecting an entire tool session
/// into a fork at the project's own root), as opposed to [`mount_native`]'s
/// ordinary empty-destination contract. Process-fencing against concurrent
/// mounts of the same destination is identical to `mount_native`.
///
/// # Errors
///
/// Same as [`mount_native`], except the destination may be non-empty; it
/// must still be an existing, non-symlink directory not already admitted
/// by another live mount. Windows returns `WritableUnavailable` because
/// `ProjFS` cannot shadow local entries.
pub fn mount_native_over_existing(
    request: NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<NativeMountSession, NativeMountError> {
    #[cfg(windows)]
    {
        let _ = (request, source);
        Err(NativeMountError::WritableUnavailable(
            "Windows ProjFS cannot shadow an existing directory".to_owned(),
        ))
    }
    #[cfg(not(windows))]
    {
        mount_native_ordinary(request, source, true)
    }
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
///
/// Action failure returns [`NativeMountError::RecoveryFailed`] with the actual
/// original owner. Retain it for retry, or acquire a [`NativeMountRecoveryOwner`]
/// explicitly before action when recovery may need multiple attempts.
pub fn recover_native_mount_destination(destination: &Path) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.recover() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed { error: Box::new(error), owner }),
    }
}

/// Reclaims a Windows provider's dead mount path without deleting authored residue.
///
/// The caller must own this implementation-managed destination. A stale `ProjFS`
/// cache, or an ordinary nonempty directory left by fail-open tools, is moved
/// to a unique sibling before the destination is reused. The returned path is
/// the preserved residue; `None` means the destination was already empty or
/// absent. A live provider remains fenced and cannot be recovered.
///
/// # Errors
///
/// Rejects a live owner, an unexpected reparse point, or a failed quarantine.
#[cfg(windows)]
pub fn recover_native_mount_destination_preserving_residue(
    destination: &Path,
) -> Result<Option<PathBuf>, NativeMountError> {
    let name = destination
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    let destination = parent.join(name);
    let mut guard = MountDestinationGuard::acquire_for_recovery(&destination)?;
    let preserved = projfs::quarantine_crashed_destination(&destination)?;
    guard.release()?;
    Ok(preserved)
}

/// Detaches and unfences one crash-left native destination while retaining its
/// underlying directory and authored host residue for an explicit caller
/// decision.
///
/// This is the host restart boundary: a live owner is rejected before any
/// detach, while a dead owner's kernel projection and persistent path fence are
/// removed. Call [`recover_native_mount_destination`] when the destination
/// itself is an implementation-owned disposable root.
///
/// # Errors
///
/// Returns a typed busy, detach, verification, or fence-release failure.
///
/// A failure retains the actual same fence and platform control in
/// [`NativeMountError::RecoveryFailed`]; no fresh lease is created on retry.
pub fn detach_native_mount_destination_after_crash(
    destination: &Path,
) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.detach() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed { error: Box::new(error), owner }),
    }
}


#[cfg(test)]
fn admit_destination(destination: &Path) -> Result<MountDestinationGuard, NativeMountError> {
    let guard = MountDestinationGuard::acquire(destination)?;
    validate_destination(destination)?;
    Ok(guard)
}


const NATIVE_FENCE_PREFIX: &str = ".acyclic-fs-mount-";
const NATIVE_FENCE_SUFFIX: &str = ".lock";
const NATIVE_FENCE_NAME_BYTES: usize = NATIVE_FENCE_PREFIX.len() + 64 + NATIVE_FENCE_SUFFIX.len();
const NATIVE_REGISTRY_NAME: &str = ".acyclic-fs-mount-registry.lock";


#[derive(Debug)]
struct MountDestinationGuard {
    file: Option<File>,
    lock_path: PathBuf,
    registry_path: PathBuf,
    // Finite constructors retain physical parent authority, not a spelling
    // that may later resolve through a replaced alias. The leaf name is fixed
    // stack data; no long-lived path allocation is invented.
    parent_directory: Option<cap_std::fs::Dir>,
    fence_name: Option<[u8; NATIVE_FENCE_NAME_BYTES]>,
    registry_file: Option<File>,
    cleanup_name: Option<crate::native_host::NativePreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>>,
    birth_cleanup: Option<crate::NativeWorkReservation>,
}

/// The accepted fence path has no allocator or new cleanup-reservation ingress.
/// Only already-prepared names and the original closed work token can be used.
struct NativeFenceReleaseWork<'a, 'credit> {
    loan: &'a crate::materializer::NativeCleanupLoan<'credit>,
    pending: Option<crate::NativeWorkReservation>,
}

impl crate::native_host::NativeHostWork for NativeFenceReleaseWork<'_, '_> {
    type Error = crate::NativeTreeMaterializationError;
    type Allocation = std::convert::Infallible;
    type CleanupWrite = std::convert::Infallible;

    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error { Self::Error::Io(error) => Some(error.kind()), _ => None }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        Self::Error::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error { Self::Error::InvalidLayout }
    fn check(&self) -> Result<(), Self::Error> { self.loan.check_wait() }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        if self.pending.is_some() { return Err(Self::Error::InvalidLayout); }
        self.pending = Some(self.loan.admit_completed(delta)?);
        Ok(())
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.pending.take().ok_or(Self::Error::InvalidLayout)?.record_completed(delta)
    }
    fn started_io(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.loan.started_io(delta)
    }
    fn claim(&mut self, _bytes: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn reserve_opaque_decode(&mut self, _bytes: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, _actual: u64) -> Result<(), Self::Error> {
        match *allocation {}
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> { match allocation {} }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) { match reservation {} }
    fn attempt_cleanup_write<T>(
        &mut self, reservation: Self::CleanupWrite, _operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) { match reservation {} }
}

impl MountDestinationGuard {
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    fn acquire_with_work(
        destination: &Path, allow_nonempty: bool, work: &mut crate::demand::DemandWork,
    ) -> Result<Self, NativeMountError> {
        let mut guard = None;
        Self::acquire_host(destination, allow_nonempty, work, None, &mut guard, false)?;
        Ok(guard.expect("successful ordinary fence admission retains its actual owner"))
    }

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    fn acquire_with_meter(
        destination: &Path, allow_nonempty: bool, meter: &NativeMountMeterWork,
        guard: &mut Option<Self>,
    ) -> Result<(), NativeMountError> {
        let native = meter.native_materialization_work();
        Self::acquire_host(destination, allow_nonempty, &mut &native, Some(&native), guard, false)
    }

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    fn acquire_host<W: crate::native_host::NativeHostWork>(
        destination: &Path, allow_nonempty: bool, work: &mut W,
        native_cleanup: Option<&crate::NativeMaterializationWork>, guard: &mut Option<Self>,
        recovery: bool,
    ) -> Result<(), NativeMountError>
    where NativeMountError: From<W::Error>,
    {
        if guard.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let recovery_parent = if recovery {
            let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
            if parent.as_os_str().is_empty() { return Err(NativeMountError::InvalidDestination); }
            Some(parent)
        } else { None };
        let directory = crate::native_host::open_root_directory_with_work(recovery_parent.unwrap_or(destination), work)?;
        if !allow_nonempty {
            let (mut reader, allocation) = crate::native_host::read_dir_held_with_work(&directory, work)?;
            #[cfg(unix)]
            let result = reader.next_with_work(work);
            #[cfg(windows)]
            let result = reader.next_wide_with_work(work);
            drop(reader);
            work.release(allocation)?;
            match result? {
                None => {}
                Some((entry, allocation)) => {
                    drop(entry);
                    work.release(allocation)?;
                    return Err(NativeMountError::InvalidDestination);
                }
            }
        }
        #[cfg(unix)]
        let canonical_owner = crate::native_host::canonical_directory_path_held_with_work(directory, work)?;
        #[cfg(windows)]
        let canonical_owner = crate::native_host::canonical_directory_wide_with_work(&directory, work)?;
        // The actual native allocation guard stays after the path in this
        // tuple, including early-error and panic destruction.
        let canonical = &canonical_owner.0;
        #[cfg(unix)]
        let canonical_bytes = canonical.as_os_str().len() as u64;
        #[cfg(windows)]
        let canonical_bytes = canonical.len().checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let leaf = if recovery {
            Some(destination.file_name().ok_or(NativeMountError::InvalidDestination)?)
        } else { None };
        #[cfg(unix)]
        let separator = recovery && !canonical.as_os_str().as_encoded_bytes().ends_with(b"/");
        #[cfg(windows)]
        let separator = recovery && !canonical.last().is_some_and(|unit| *unit == b'\\' as u16 || *unit == b'/' as u16);
        #[cfg(unix)]
        let suffix_bytes = leaf.map_or(0, |leaf| leaf.len() as u64) + u64::from(separator);
        #[cfg(windows)]
        let suffix_bytes = {
            use std::os::windows::ffi::OsStrExt;
            leaf.map_or(0, |leaf| leaf.encode_wide().count() as u64 * 2) + u64::from(separator) * 2
        };
        let hashed = crate::WorkCounters {
            bytes_hashed: canonical_bytes.checked_add(suffix_bytes)
                .and_then(|bytes| bytes.checked_add(b"acyclic-fs/native-mount-destination/v1\0".len() as u64))
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        };
        let result = (|| {
            work.check()?;
            work.admit(hashed)?;
            let mut identity = blake3::Hasher::new();
            identity.update(b"acyclic-fs/native-mount-destination/v1\0");
            #[cfg(unix)]
            update_path_identity(&mut identity, canonical);
            #[cfg(windows)]
            for unit in canonical { identity.update(&unit.to_le_bytes()); }
            if let Some(leaf) = leaf {
                #[cfg(unix)]
                if separator { identity.update(b"/"); }
                #[cfg(windows)]
                if separator { identity.update(&(b'\\' as u16).to_le_bytes()); }
                update_path_identity(&mut identity, Path::new(leaf));
            }
            work.record(hashed)?;
            let hex = identity.finalize().to_hex();
            let mut name = [0u8; NATIVE_FENCE_NAME_BYTES];
            name[..NATIVE_FENCE_PREFIX.len()].copy_from_slice(NATIVE_FENCE_PREFIX.as_bytes());
            name[NATIVE_FENCE_PREFIX.len()..NATIVE_FENCE_PREFIX.len() + 64].copy_from_slice(hex.as_bytes());
            name[NATIVE_FENCE_PREFIX.len() + 64..].copy_from_slice(NATIVE_FENCE_SUFFIX.as_bytes());
            Ok::<_, NativeMountError>(name)
        })();
        let recovery_directory: Result<Option<cap_std::fs::Dir>, NativeMountError> = if recovery && result.is_ok() {
            #[cfg(unix)]
            { crate::native_host::open_root_directory_with_work(canonical, work).map(Some).map_err(Into::into) }
            #[cfg(windows)]
            { Ok(Some(directory)) }
        } else { Ok(None) };
        let (canonical, canonical_allocation) = canonical_owner;
        drop(canonical);
        work.release(canonical_allocation)?;
        let name = result?;
        // Prepare all retirement names and accepted controls before either
        // registry or fence creation can mutate the destination namespace.
        let cleanup = if let Some(native) = native_cleanup {
            let mut terminated = [0u8; NATIVE_FENCE_NAME_BYTES + 1];
            let mut copied = native.reserve_work(crate::WorkCounters {
                bytes_copied: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            terminated[..NATIVE_FENCE_NAME_BYTES].copy_from_slice(&name);
            copied.record_completed(crate::WorkCounters {
                bytes_copied: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let prepared = crate::native_host::NativePreparedAsciiLeaf::new(terminated, native)?;
            let credit = native.reserve_work(crate::WorkCounters {
                backend_write_operations: 2,
                #[cfg(windows)]
                backend_read_operations: 1,
                #[cfg(windows)]
                items_examined: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            Some((prepared, credit))
        } else { None };
        let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
        let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        let parent_directory = match recovery_directory? {
            Some(directory) => directory,
            None => crate::native_host::open_root_directory_with_work(parent, work)?,
        };
        let registry = crate::native_host::open_leaf_with_work(
            &parent_directory, Path::new(NATIVE_REGISTRY_NAME),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        let file = crate::native_host::open_leaf_with_work(
            &parent_directory, Path::new(std::str::from_utf8(&name).expect("literal prefix, hex and suffix are UTF-8")),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        let (cleanup_name, birth_cleanup) = match cleanup {
            Some((name, credit)) => (Some(name), Some(credit)),
            None => (None, None),
        };
        let admitted = Self { file: Some(file), lock_path: PathBuf::new(), registry_path: PathBuf::new(),
            parent_directory: Some(parent_directory), fence_name: Some(name),
            registry_file: Some(registry), cleanup_name, birth_cleanup };
        if recovery {
            // Even a post-open lock failure belongs to the preborn admin
            // worker. It closes the actual file using its original credit.
            *guard = Some(admitted);
            lock_mount_file_with_work(
                guard.as_ref().and_then(|guard| guard.file.as_ref()).expect("actual opened administrative fence"),
                false, work,
            )?;
        } else {
            lock_mount_file_with_work(admitted.file.as_ref().expect("actual destination fence"), false, work)?;
            *guard = Some(admitted);
        }
        let registry = guard.as_ref().and_then(|guard| guard.registry_file.as_ref())
            .expect("the admitted fence retains its actual registry owner");
        work.attempt_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }, || fs2::FileExt::unlock(registry))?;
        Ok(())
    }

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
            parent_directory: None,
            fence_name: None,
            registry_file: None,
            cleanup_name: None,
            birth_cleanup: None,
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
        if self.file.is_none() { return Ok(()); }
        let cancellation = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        self.release_measured(&mut work)
    }

    fn release_measured(&mut self, work: &mut crate::demand::DemandWork) -> Result<(), NativeMountError> {
        self.release_host(work)
    }

    fn release_with_meter(&mut self, meter: &NativeMountMeterWork) -> Result<(), NativeMountError> {
        let native = meter.native_materialization_work();
        self.release_host(&mut &native)
    }

    /// Administrative LastDrop closes only the original held fence. The
    /// namespace marker is deliberately preserved, including after EPERM.
    /// Call only after every actual recovery control is physically terminal.
    fn close_administrative_fence(&mut self, original: &mut crate::NativeWorkReservation) {
        if self.file.is_some() {
            original.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(self.file.take());
        }
    }

    fn release_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        self.release_with_cleanup(Some(wait))
    }

    fn release_accepted_cleanup(&mut self) -> Result<(), NativeMountError> {
        self.release_with_cleanup(None)
    }

    fn release_with_cleanup(
        &mut self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        if self.file.is_none() { return Ok(()); }
        let invalid = || NativeMountError::Materialization(crate::NativeTreeMaterializationError::InvalidLayout);
        let directory = self.parent_directory.as_ref().ok_or_else(invalid)?;
        let registry = self.registry_file.as_ref().ok_or_else(invalid)?;
        let name = self.cleanup_name.as_ref().ok_or_else(invalid)?;
        let credit = self.birth_cleanup.as_mut().ok_or_else(invalid)?;
        let loan = match wait {
            Some(wait) => credit.cleanup_loan(wait),
            None => credit.accepted_cleanup_loan(),
        };
        let mut work = NativeFenceReleaseWork { loan: &loan, pending: None };
        // A contended registry is a real pending control. Do not spend an
        // unbounded sequence of new lock attempts after cancellation.
        lock_mount_file_with_work(registry, false, &mut work)?;
        release_owned_fence(&mut self.file, || {
            match crate::native_host::remove_prepared_ascii_leaf_with_work(directory, name, &mut work) {
                Ok(()) => Ok(()),
                Err(crate::NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(NativeMountError::from(error)),
            }
        })?;
        Ok(())
    }

    fn release_host<W: crate::native_host::NativeHostWork>(
        &mut self, work: &mut W,
    ) -> Result<(), NativeMountError>
    where NativeMountError: From<W::Error>,
    {
        if self.file.is_none() {
            return Ok(());
        }
        let opened_directory;
        let directory = match self.parent_directory.as_ref() {
            Some(directory) => directory,
            None => {
                let parent = self.lock_path.parent().ok_or(NativeMountError::InvalidDestination)?;
                opened_directory = crate::native_host::open_root_directory_with_work(parent, work)?;
                &opened_directory
            }
        };
        let registry_name = if self.fence_name.is_some() { Path::new(NATIVE_REGISTRY_NAME) }
            else { Path::new(self.registry_path.file_name().ok_or(NativeMountError::InvalidDestination)?) };
        let lock_name = match self.fence_name.as_ref() {
            Some(name) => Path::new(std::str::from_utf8(name).expect("literal prefix, hex and suffix are UTF-8")),
            None => Path::new(self.lock_path.file_name().ok_or(NativeMountError::InvalidDestination)?),
        };
        let registry = crate::native_host::open_leaf_with_work(
            directory, registry_name,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        release_owned_fence(&mut self.file, || {
            match crate::native_host::remove_leaf_with_work(directory, lock_name, work) {
                Ok(()) => Ok(()),
                Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => Ok(()),
                Err(error) => Err(error.into()),
            }
        })?;
        drop(registry);
        Ok(())
    }
}

fn lock_mount_file_with_work<W: crate::native_host::NativeHostWork>(
    file: &File, blocking: bool, work: &mut W,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error>,
{
    loop {
        let delta = crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED };
        let result = work.attempt_io(delta, || {
            match fs2::FileExt::try_lock_exclusive(file) {
                Ok(()) => Ok(true),
                Err(error) if lock_is_contended(&error) => Ok(false),
                Err(error) => Err(error),
            }
        });
        match result {
            Ok(true) => return Ok(()),
            Ok(false) if blocking => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Ok(false) => return Err(NativeMountError::DestinationBusy),
            Err(error) => return Err(error.into()),
        }
    }
}

fn release_owned_fence<T, E>(
    owner: &mut Option<T>,
    release: impl FnOnce() -> Result<(), E>,
) -> Result<(), E> {
    release()?;
    owner.take();
    Ok(())
}


impl Drop for MountDestinationGuard {
    fn drop(&mut self) {
        // A finite fence is retired only by its original invocation. Dropping
        // it must not invent an unbounded cleanup engine after denied work.
        if self.parent_directory.is_none() {
            let _ = self.release();
        }
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

#[cfg(test)]
fn validate_destination(destination: &Path) -> Result<(), NativeMountError> {
    validate_directory(destination)?;
    let mut entries =
        std::fs::read_dir(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if entries.next().is_some() {
        return Err(NativeMountError::InvalidDestination);
    }
    Ok(())
}

/// Ordinary directory validation used only by legacy admission fixtures.
#[cfg(test)]
fn validate_directory(destination: &Path) -> Result<(), NativeMountError> {
    let metadata =
        std::fs::symlink_metadata(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn start_driver_into<F>(
    request: &NativeMountRequest, source: Arc<dyn MountFilesystem>, work: &NativeMountMeterWork,
    out: &mut Option<DriverSession>, retirement: &mut Option<NativeMountRetirement>, prepare: F,
) -> Result<(), DriverStartFailure>
where F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError> {
    let mut driver = None;
    let result = fuse::FuseSession::start_into(request, source, work, &mut driver, retirement, prepare);
    *out = driver.map(DriverSession::Fuse);
    result
}

#[cfg(target_os = "macos")]
fn start_driver_into<F>(
    request: &NativeMountRequest, source: Arc<dyn MountFilesystem>, work: &NativeMountMeterWork,
    out: &mut Option<DriverSession>, retirement: &mut Option<NativeMountRetirement>, prepare: F,
) -> Result<(), DriverStartFailure>
where F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError> {
    let mut driver = None;
    let result = darwin_mount::DarwinMountSession::start_into(request, source, work, &mut driver, retirement, prepare);
    *out = driver.map(DriverSession::DarwinMount);
    result
}

#[cfg(target_os = "windows")]
fn start_driver_into<F>(
    request: &NativeMountRequest, source: Arc<dyn MountFilesystem>, work: &NativeMountMeterWork,
    out: &mut Option<DriverSession>, retirement: &mut Option<NativeMountRetirement>, prepare: F,
) -> Result<(), DriverStartFailure>
where F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError> {
    let mut driver = None;
    let result = projfs::ProjFsSession::start_into(request, source, work, &mut driver, retirement, prepare);
    *out = driver.map(DriverSession::ProjFs);
    result
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_driver_into<F>(
    _request: &NativeMountRequest, _source: Arc<dyn MountFilesystem>, _work: &NativeMountMeterWork,
    _out: &mut Option<DriverSession>, _retirement: &mut Option<NativeMountRetirement>, _prepare: F,
) -> Result<(), DriverStartFailure>
where F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError> {
    Err(NativeMountError::UnsupportedTarget.into())
}

fn driver_retirement_ownership(driver: &DriverSession) -> (bool, bool) {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.retirement_ownership(),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.retirement_ownership(),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.retirement_ownership(),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => (false, false),
    }
}

fn stop_driver_with_meter(
    driver: &mut DriverSession,
    work: &NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.stop_with_meter(work),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.stop_with_meter(work),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.stop_with_meter(work),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => {
            let _ = work;
            Err(NativeMountError::UnsupportedTarget)
        }
    }
}

fn stop_driver_with_cleanup_wait(
    driver: &mut DriverSession, wait: &crate::CancellationToken,
) -> Result<(), NativeMountError> {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.stop_with_cleanup_wait(wait),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.stop_with_cleanup_wait(wait),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.stop_with_cleanup_wait(wait),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => {
            let _ = wait;
            Err(NativeMountError::UnsupportedTarget)
        }
    }
}

fn driver_completed_callback_work(driver: &DriverSession) -> crate::WorkCounters {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.completed_callback_work(),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.completed_callback_work(),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.completed_callback_work(),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => crate::WorkCounters::UNCHARGED,
    }
}

fn driver_take_callback_failure(driver: &DriverSession) -> Option<NativeMountCallbackFailure> {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.take_callback_failure(),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.take_callback_failure(),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.take_callback_failure(),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => None,
    }
}

fn driver_namespace_detach_confirmed(driver: &DriverSession) -> bool {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.namespace_detach_confirmed(),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.namespace_detach_confirmed(),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.namespace_detach_confirmed(),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => false,
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

    pub(super) fn probe() -> NativeMountCapabilities {
        let available = std::path::Path::new("/sbin/mount_nfs").is_file();
        NativeMountCapabilities {
            kind: Some(NativeMountKind::MacOsNfs),
            available,
            writable: available,
            provider_process_io_observable: available,
            session_isolation: NativeMountSessionIsolation::SharedProcess,
            unavailable_reason: (!available)
                .then(|| "the built-in macOS NFS mount utility is unavailable".to_owned()),
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

    #[cfg(windows)]
    #[test]
    fn projfs_cannot_shadow_an_existing_directory() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("existing.txt"), b"real")?;
        let request = NativeMountRequest {
            mount_id: MountId::new(),
            volume_id: VolumeId::new(),
            destination: root.path().to_path_buf(),
            writable: true,
        };
        let fs = Fs::memory();
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
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
                .await
                .map(|receipt| receipt.value)
        })?;
        let source = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::new(checkout)),
            config,
        )?);
        assert!(matches!(
            mount_native_over_existing(request, source),
            Err(NativeMountError::WritableUnavailable(_))
        ));
        assert_eq!(std::fs::read(root.path().join("existing.txt"))?, b"real");
        Ok(())
    }

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

    fn await_recovery_worker(thread: std::thread::JoinHandle<()>) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !thread.is_finished() {
            if Instant::now() >= deadline { return Err("actual administrative worker did not terminate".into()); }
            std::thread::sleep(Duration::from_millis(1));
        }
        thread.join().map_err(|_| "actual administrative worker panicked")?;
        Ok(())
    }

    #[test]
    fn administrative_last_drop_closes_real_fence_without_touching_residue()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("mount");
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("authored"), b"retain")?;
        let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
        let mut owner = NativeMountRecoveryOwner::acquire_with_original(&destination, original.clone())?;
        let identity = owner.cleanup_state().fence_identity;
        let borrowed_path = owner.cleanup_state().destination.as_os_str().as_encoded_bytes().as_ptr();
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let (error, receipt) = owner.detach_measured(budget, &cancellation).expect_err("the actual native observation is denied");
        assert!(matches!(error, NativeMountError::Work(_) | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))));
        assert_eq!(receipt.backend_read_operations, 0);
        assert!(owner.cleanup_state().destination_fence_retained);
        assert!(owner.cleanup_state().measured_cleanup_failed);
        assert_eq!(owner.cleanup_state().fence_identity, identity);
        assert_eq!(owner.cleanup_state().destination.as_os_str().as_encoded_bytes().as_ptr(), borrowed_path);
        assert!(matches!(acquire_existing_mount_lock(&lock_path), Err(NativeMountError::DestinationBusy)));
        let before_close = original.receipt().backend_write_operations;
        let thread = owner.worker.take().expect("one actual accepted administrative worker");
        drop(owner);
        await_recovery_worker(thread)?;
        assert_eq!(original.receipt().backend_write_operations, before_close + 1);
        assert!(lock_path.is_file(), "LastDrop must preserve the actual marker");
        let unlocked = acquire_existing_mount_lock(&lock_path)?;
        let (after_identity, _) = crate::native_host::file_identity_and_length_with_work(&unlocked, &mut &original)?;
        assert_eq!(after_identity, identity);
        drop(unlocked);
        assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
        assert!(destination.is_dir());
        Ok(())
    }

    #[test]
    fn administrative_post_lock_acquisition_denial_retires_actual_opened_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("mount");
        std::fs::create_dir(&destination)?;
        let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
        let baseline = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
        let mut owner = NativeMountRecoveryOwner::acquire_with_original(&destination, baseline.clone())?;
        let writes = baseline.receipt().backend_write_operations;
        let thread = owner.worker.take().expect("actual baseline worker");
        drop(owner);
        await_recovery_worker(thread)?;
        let mut budget = WorkBudget::UNBOUNDED;
        // Two guard cleanup writes and the original final-close write remain
        // reserved. This denies the real registry unlock AFTER fence locking.
        budget.backend_write_operations = writes + 2 + u64::from(cfg!(windows));
        let denied = crate::NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
        let error = NativeMountRecoveryOwner::acquire_with_original(&destination, denied.clone())
            .expect_err("the actual post-lock registry unlock is denied");
        assert!(matches!(error, NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded { counter: "backend_write_operations", .. }
        ))));
        assert_eq!(denied.receipt().backend_write_operations, writes, "actual prefix plus original physical fence close");
        assert!(lock_path.exists(), "failed acquisition preserves its on-disk marker");
        drop(acquire_existing_mount_lock(&lock_path)?);
        assert!(destination.is_dir());
        Ok(())
    }

    #[test]
    fn administrative_retry_uses_same_fence_and_distinct_action_ledgers()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("mount");
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("authored"), b"retain")?;
        let mut owner = NativeMountRecoveryOwner::acquire(&destination)?;
        let identity = owner.cleanup_state().fence_identity;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        assert!(owner.detach_measured(budget, &CancellationToken::new()).is_err());
        let receipt = owner.detach_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
        assert!(receipt.value);
        assert!(receipt.work.backend_read_operations > 0);
        assert_eq!(owner.cleanup_state().fence_identity, identity);
        assert!(owner.cleanup_state().cleanup_complete);
        let thread = owner.worker.take().expect("same original worker");
        drop(owner);
        await_recovery_worker(thread)?;
        assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
        Ok(())
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
    fn validate_directory_admits_nonempty_directories_but_still_rejects_non_directories()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "acyclic-fs-mount-over-existing-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root)?;
        std::fs::write(root.join("occupied"), b"x")?;
        assert!(validate_directory(&root).is_ok());

        let file = root.join("occupied");
        assert!(matches!(
            validate_directory(&file),
            Err(NativeMountError::InvalidDestination)
        ));

        let missing = root.join("does-not-exist");
        assert!(matches!(
            validate_directory(&missing),
            Err(NativeMountError::InvalidDestination)
        ));

        let _ = std::fs::remove_dir_all(&root);
        Ok(())
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
        let mut recovery = NativeMountRecoveryOwner::acquire(&destination)?;
        let original_fence = recovery.cleanup_state().fence_identity;
        assert!(recovery.recover().is_err());
        assert!(recovery.cleanup_state().destination_fence_retained);
        assert!(recovery.cleanup_state().measured_cleanup_failed);
        assert_eq!(
            std::fs::read(destination.join("unowned-content"))?,
            b"retain"
        );
        std::fs::remove_file(destination.join("unowned-content"))?;

        recovery.recover()?;
        assert_eq!(recovery.cleanup_state().fence_identity, original_fence);
        assert!(recovery.cleanup_state().cleanup_complete);
        assert!(!destination.exists());
        let residue = std::fs::read_dir(parent.path())?
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name != ".acyclic-fs-mount-registry.lock")
            .collect::<Vec<_>>();
        assert!(residue.is_empty());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn preserving_recovery_fences_live_owner_and_retains_ordinary_residue()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("mount");
        std::fs::create_dir(&destination)?;
        let live_guard = MountDestinationGuard::acquire(&destination)?;
        assert!(matches!(
            recover_native_mount_destination_preserving_residue(&destination),
            Err(NativeMountError::DestinationBusy)
        ));
        drop(live_guard);

        std::fs::write(destination.join("authored.txt"), b"retain")?;
        let preserved = recover_native_mount_destination_preserving_residue(&destination)?
            .ok_or("nonempty destination was not preserved")?;
        assert!(!destination.exists());
        assert_eq!(std::fs::read(preserved.join("authored.txt"))?, b"retain");
        assert!(recover_native_mount_destination_preserving_residue(&destination)?.is_none());
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
    fn measured_destination_write_denial_keeps_actual_read_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 0;
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        let error = match MountDestinationGuard::acquire_with_work(&destination, false, &mut work) {
            Err(error) => error,
            Ok(_) => panic!("actual registry creation must be admitted"),
        };
        assert!(matches!(error, NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert!(work.work.backend_read_operations > 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(!temporary.path().join(NATIVE_REGISTRY_NAME).exists());
        // No fabricated driver failure or unbounded rollback: no destination
        // lock was created, and the real ordinary engine can acquire it.
        drop(MountDestinationGuard::acquire(&destination)?);
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
        drop(stale);

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
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        let release = temporary.path().join("release");
        std::fs::create_dir(&destination)?;
        let mut crashed = MountDestinationGuard::acquire(&destination)?;
        let crash_left_path = crashed.lock_path.clone();
        crashed.file.take();
        drop(crashed);

        let executable = std::env::current_exe()?;
        let mut children = Vec::new();
        let mut readiness = Vec::new();
        for ordinal in 0..2 {
            let ready = temporary.path().join(format!("ready-{ordinal}"));
            let child = acyclic_native_runtime::spawn_process_tree(
                std::process::Command::new(&executable)
                    .args([
                        "--exact",
                        "native_mount::tests::destination_reclaim_child_waits_for_external_release",
                        "--nocapture",
                    ])
                    .env("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION", &destination)
                    .env("ACYCLIC_FS_RECLAIM_CHILD_READY", &ready)
                    .env("ACYCLIC_FS_RECLAIM_CHILD_RELEASE", &release),
            )?;
            readiness.push(ready);
            children.push(child);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while readiness.iter().any(|path| !path.is_file()) {
            for child in &mut children {
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
        for child in &mut children {
            let status = child.wait(Duration::from_secs(5))?;
            if !status.success() {
                return Err(format!("reclaim child failed: {status}").into());
            }
        }
        children.clear();
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
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        let ready = temporary.path().join("ready");
        std::fs::create_dir(&destination)?;
        let child = acyclic_native_runtime::spawn_process_tree(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "native_mount::tests::destination_ownership_child_holds_lock_until_killed",
                    "--nocapture",
                ])
                .env("ACYCLIC_FS_LOCK_CHILD_DESTINATION", &destination)
                .env("ACYCLIC_FS_LOCK_CHILD_READY", &ready),
        )?;
        let mut child = child;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.is_file() {
            if let Some(status) = child.try_wait()? {
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
        assert!(matches!(
            reclaim_native_mount_destination_fence(&destination),
            Err(NativeMountError::DestinationBusy)
        ));
        child.terminate_descendants()?;
        let status = child.wait(Duration::from_secs(5))?;
        assert!(
            !status.success(),
            "killed lock child unexpectedly succeeded"
        );
        reclaim_native_mount_destination_fence(&destination)?;
        reclaim_native_mount_destination_fence(&destination)?;
        let _reclaimed = MountDestinationGuard::acquire(&destination)?;
        Ok(())
    }

    #[test]
    fn shared_destination_fence_retains_original_owner_after_denial()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let cancellation = crate::CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation))));
        let meter = NativeMountMeterWork::from_meter(Arc::clone(&original));
        let mut guard = None;
        MountDestinationGuard::acquire_with_meter(&destination, false, &meter, &mut guard)?;
        let mut guard = guard.expect("actual fence was admitted");
        assert!(Arc::ptr_eq(meter.meter(), &original));
        assert!(guard.parent_directory.is_some());
        assert!(guard.file.is_some());
        let mut competing_guard = None;
        assert!(matches!(MountDestinationGuard::acquire_with_meter(&destination, false, &meter, &mut competing_guard),
            Err(NativeMountError::DestinationBusy)));
        assert!(competing_guard.is_none(), "a competing lock never becomes an owned cleanup target");
        {
            let mut work = original.lock();
            work.budget.backend_write_operations = work.work.backend_write_operations;
        }
        let before = original.lock().work;
        let error = guard.release_with_meter(&meter).expect_err("original write allowance denies cleanup");
        assert!(matches!(error, NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert!(guard.file.is_some());
        assert_eq!(original.lock().work.backend_write_operations, before.backend_write_operations);
        original.lock().budget.backend_write_operations = u64::MAX;
        guard.release_with_meter(&meter)?;
        assert!(guard.file.is_none());
        assert_eq!(original.lock().live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn accepted_destination_fence_cleanup_survives_original_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let cancelled = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 64,
            backend_write_operations: 64,
            items_examined: 1_024,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancelled).map_err(|(error, _)| error)?;
        let mut guard = None;
        MountDestinationGuard::acquire_with_meter(
            &destination, false, &work.native_mount_work(), &mut guard)?;
        let mut guard = guard.expect("actual fence was admitted");
        let before = work.receipt();
        cancelled.cancel();
        let refused_wait = crate::CancellationToken::new();
        refused_wait.cancel();
        assert!(guard.release_with_cleanup_wait(&refused_wait).is_err());
        assert!(guard.file.is_some(), "denied wait preserves the actual fence");
        assert_eq!(work.receipt(), before, "no accepted operation starts on a denied wait");
        guard.release_with_cleanup_wait(&crate::CancellationToken::new())?;
        assert!(guard.file.is_none(), "the same held fence physically retired");
        let after = work.receipt();
        assert!(after.backend_write_operations > before.backend_write_operations);
        assert_eq!(after.allocation_operations, before.allocation_operations,
            "accepted cleanup does not allocate a path or control");
        assert!(work.cancellation().is_cancelled(), "original cancellation is never replaced");
        drop(guard);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn destination_fence_cleanup_credit_is_admitted_before_namespace_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(error, _)| error)?;
        let mut guard = None;
        assert!(MountDestinationGuard::acquire_with_meter(
            &destination, false, &work.native_mount_work(), &mut guard).is_err());
        assert!(guard.is_none(), "denied admission does not create an owner");
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(std::fs::read_dir(temporary.path())?.count(), 1,
            "neither registry nor fence is born after cleanup admission fails");
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn measured_fence_release_uses_exact_actual_leaf_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        std::fs::create_dir(&first)?;
        std::fs::create_dir(&second)?;
        let cancellation = crate::CancellationToken::new();
        let mut first_guard = MountDestinationGuard::acquire(&first)?;
        let mut baseline = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        first_guard.release_measured(&mut baseline)?;
        assert!(baseline.work.backend_read_operations > 0);
        assert!(baseline.work.backend_write_operations > 0);
        assert_eq!(baseline.live_allocation_bytes(), 0);
        let mut second_guard = MountDestinationGuard::acquire(&second)?;
        let mut exact = crate::demand::DemandWork::new(baseline.work, &cancellation);
        second_guard.release_measured(&mut exact)?;
        assert_eq!(exact.work, baseline.work);
        assert_eq!(exact.live_allocation_bytes(), 0);
        assert!(second_guard.file.is_none());
        assert!(!second_guard.lock_path.exists());
        Ok(())
    }

    #[test]
    fn measured_fence_write_refusal_keeps_real_owner_and_read_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mut guard = MountDestinationGuard::acquire(&destination)?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 0;
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        let error = guard.release_measured(&mut work).expect_err("registry creation must be admitted");
        assert!(matches!(error, NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert!(work.work.backend_read_operations > 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(guard.file.is_some());
        assert!(guard.lock_path.is_file());
        assert!(matches!(MountDestinationGuard::acquire(&destination), Err(NativeMountError::DestinationBusy)));
        let mut retry = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        guard.release_measured(&mut retry)?;
        assert!(guard.file.is_none());
        Ok(())
    }

    #[test]
    fn measured_registry_contention_counts_each_real_lock_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mut guard = MountDestinationGuard::acquire(&destination)?;
        let registry_owner = acquire_mount_lock(&guard.registry_path, true)?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 2;
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        let error = guard.release_measured(&mut work).expect_err("real contended retry must exhaust finite I/O");
        assert!(matches!(error, NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert_eq!(work.work.backend_write_operations, 2);
        assert!(guard.file.is_some());
        assert!(guard.lock_path.is_file());
        drop(registry_owner);
        let mut retry = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        guard.release_measured(&mut retry)?;
        Ok(())
    }

    #[test]
    fn measured_fence_native_delete_failure_preserves_typed_cause_and_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mut guard = MountDestinationGuard::acquire(&destination)?;
        std::fs::remove_file(&guard.lock_path)?;
        std::fs::create_dir(&guard.lock_path)?;
        let cancellation = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let error = guard.release_measured(&mut work).expect_err("native file deletion cannot remove a directory");
        assert!(matches!(error, NativeMountError::Io(_)));
        assert!(work.work.backend_read_operations > 0);
        assert!(work.work.backend_write_operations > 0);
        assert!(guard.file.is_some());
        assert!(guard.lock_path.is_dir());
        Ok(())
    }

    #[test]
    fn measured_fence_cancellation_never_starts_a_native_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mut guard = MountDestinationGuard::acquire(&destination)?;
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let error = guard.release_measured(&mut work).expect_err("original cancellation must win before native work");
        assert!(matches!(error, NativeMountError::Demand(crate::DemandError::Cancelled)));
        assert_eq!(work.work, crate::WorkCounters::default());
        assert!(guard.file.is_some());
        assert!(guard.lock_path.is_file());
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
    fn failed_final_stop_keeps_driver_and_destination_fence_until_process_exit() {
        struct DropProbe(Arc<std::sync::atomic::AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut driver = Some(DropProbe(Arc::clone(&drops)));
        let mut destination_guard = Some(DropProbe(Arc::clone(&drops)));
        drop_owned_session(
            &mut driver,
            &mut destination_guard,
            |_| Err("kernel detach failed"),
            |_| Ok(()),
        );
        assert!(driver.is_none());
        assert!(destination_guard.is_none());
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn failed_fence_unlink_retains_the_operating_system_lock() {
        let mut fence = Some(7_u8);
        let failure = release_owned_fence(&mut fence, || Err("unlink failed"));
        assert_eq!(failure, Err("unlink failed"));
        assert_eq!(fence, Some(7));
        assert_eq!(
            release_owned_fence(&mut fence, || Ok::<_, &str>(())),
            Ok(())
        );
        assert_eq!(fence, None);
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
    async fn capture_host_hard_links_preserves_shared_file_identity()
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
                VolumeId::from_bytes([92; 16]),
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
        let host = tempfile::tempdir()?;
        std::fs::write(host.path().join("a"), b"shared")?;
        std::fs::hard_link(host.path().join("a"), host.path().join("b"))?;
        let path = |name: &str| -> Result<NamespacePath, Box<dyn std::error::Error>> {
            Ok(NamespacePath::new(
                vec![LogicalName::new(
                    NameEncoding::Utf8,
                    name.as_bytes().to_vec(),
                    limits.maximum_component_bytes,
                )?],
                limits,
            )?)
        };
        let paths = [path("a")?, path("b")?];
        crate::native_capture::capture_paths_batched_with_baseline(
            &mut checkout,
            &paths,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?;
        let first = checkout
            .lookup_no_follow(&paths[0], WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("first captured link missing")?;
        let second = checkout
            .lookup_no_follow(&paths[1], WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("second captured link missing")?;
        assert_eq!(first.file_id, second.file_id);
        std::fs::hard_link(host.path().join("a"), host.path().join("z"))?;
        let additional = path("z")?;
        capture_paths(
            &mut checkout,
            &[paths[0].clone(), additional.clone()],
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let additional_id = checkout
            .lookup_no_follow(&additional, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("additional link missing")?
            .file_id;
        assert_eq!(additional_id, first.file_id);
        std::fs::hard_link(host.path().join("a"), host.path().join("0"))?;
        let earlier_alias = path("0")?;
        capture_paths(
            &mut checkout,
            &[paths[0].clone(), earlier_alias.clone()],
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let earlier_id = checkout
            .lookup_no_follow(&earlier_alias, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("earlier alias missing")?
            .file_id;
        assert_eq!(earlier_id, first.file_id);
        std::fs::hard_link(host.path().join("a"), host.path().join("c"))?;
        let third = path("c")?;
        let watch = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![WatchChange::Created(third.clone())],
                original: Default::default(),
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        assert!(matches!(
            watch,
            Err(crate::OperationFailure {
                error: CaptureError::RescanRequired { .. },
                ..
            })
        ));
        assert!(
            checkout
                .lookup_no_follow(&third, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .is_none()
        );
        std::fs::write(host.path().join("existing"), b"old")?;
        let existing = path("existing")?;
        capture_paths(
            &mut checkout,
            std::slice::from_ref(&existing),
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let old_id = checkout
            .lookup_no_follow(&existing, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("existing path missing")?
            .file_id;
        std::fs::remove_file(host.path().join("existing"))?;
        std::fs::hard_link(host.path().join("a"), host.path().join("existing"))?;
        let watch = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(2),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![WatchChange::Created(existing.clone())],
                original: Default::default(),
            },
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 1,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        assert!(matches!(
            watch,
            Err(crate::OperationFailure {
                error: CaptureError::RescanRequired { .. },
                ..
            })
        ));
        assert_eq!(
            checkout
                .lookup_no_follow(&existing, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("replacement changed checkout")?
                .file_id,
            old_id
        );
        Ok(())
    }

    #[tokio::test]
    async fn baseline_path_limit_stops_unbounded_traversal()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let fs = Fs::memory();
        let volume = fs
            .create_volume_with_id(
                VolumeId::from_bytes([94; 16]),
                VolumeConfig::portable(Lifecycle::Ephemeral),
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
        let host = tempfile::tempdir()?;
        for index in 0..3 {
            std::fs::write(host.path().join(format!("file-{index}")), b"x")?;
        }
        let result = capture_baseline(
            &mut checkout,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(host.path())?,
                source_root: host.path().to_path_buf(),
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        let Err(error) = result else {
            return Err("path cap unexpectedly permitted traversal".into());
        };
        assert!(matches!(error.error, CaptureError::InvalidOptions));
        assert!(error.work.items_examined <= 2);
        Ok(())
    }

    /// Drives host mutations through finite access/sharing-denial windows.
    /// Only the fixture's host I/O is retried, never SDK capture or publication.
    fn mutate_as_a_user_would(
        mut operation: impl FnMut() -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match operation() {
                Err(error)
                    if (error.kind() == std::io::ErrorKind::PermissionDenied
                        || cfg!(windows) && matches!(error.raw_os_error(), Some(32 | 33)))
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                result => return result,
            }
        }
    }

    #[tokio::test]
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
        mutate_as_a_user_would(|| std::fs::create_dir(&destination))?;
        let materialized = materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                expected_root_identity: None,
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| std::io::Error::other(format!("sparse materialization: {error}")))?;
        assert_eq!(materialized.value.files, 1);
        let host_file = destination.join("sparse.bin");
        assert_eq!(
            std::fs::metadata(&host_file)
                .map_err(|error| std::io::Error::other(format!("materialized metadata: {error}")))?
                .len(),
            1024 * 1024
        );
        let body = std::fs::read(&host_file)
            .map_err(|error| std::io::Error::other(format!("materialized read: {error}")))?;
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
        .await
        .map_err(|error| std::io::Error::other(format!("sparse recapture: {error}")))?;
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

        mutate_as_a_user_would(|| std::fs::write(&host_file, b"captured"))
            .map_err(|error| std::io::Error::other(format!("host rewrite: {error}")))?;
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
        mutate_as_a_user_would(|| std::fs::rename(&host_file, destination.join("renamed.bin")))
            .map_err(|error| std::io::Error::other(format!("host rename: {error}")))?;
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
                original: Default::default(),
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
        mutate_as_a_user_would(|| {
            std::fs::rename(
                destination.join("renamed.bin"),
                destination.join("moved.bin"),
            )
        })
        .map_err(|error| std::io::Error::other(format!("second host rename: {error}")))?;
        mutate_as_a_user_would(|| std::fs::write(destination.join("renamed.bin"), b"replacement"))
            .map_err(|error| std::io::Error::other(format!("host replacement create: {error}")))?;
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
                original: Default::default(),
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

        mutate_as_a_user_would(|| {
            std::fs::write(destination.join("renamed.bin"), b"replaced-again")
        })
        .map_err(|error| std::io::Error::other(format!("host replacement rewrite: {error}")))?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(7),
                first_sequence: WatchSequence::from_u64(14),
                next_sequence: WatchSequence::from_u64(15),
                changes: vec![WatchChange::Created(renamed.clone())],
                original: Default::default(),
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

        mutate_as_a_user_would(|| {
            std::fs::write(destination.join("renamed.bin"), b"compound-replacement")
        })
        .map_err(|error| std::io::Error::other(format!("host compound rewrite: {error}")))?;
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
                original: Default::default(),
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

        mutate_as_a_user_would(|| std::fs::remove_file(destination.join("renamed.bin")))
            .map_err(|error| std::io::Error::other(format!("host replacement removal: {error}")))?;
        mutate_as_a_user_would(|| std::fs::remove_file(destination.join("moved.bin")))
            .map_err(|error| std::io::Error::other(format!("host moved removal: {error}")))?;
        mutate_as_a_user_would(|| std::fs::create_dir(destination.join("nested")))
            .map_err(|error| std::io::Error::other(format!("host nested directory: {error}")))?;
        mutate_as_a_user_would(|| {
            std::fs::write(destination.join("nested").join("new.txt"), b"baseline")
        })
        .map_err(|error| std::io::Error::other(format!("host baseline write: {error}")))?;
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
                original: Default::default(),
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
                original: Default::default(),
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
                original: Default::default(),
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
                original: Default::default(),
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
                original: Default::default(),
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
                original: Default::default(),
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
                original: Default::default(),
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
        let materialized = materialize_checkout(
            &mut source_checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                expected_root_identity: None,
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
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
