//! Native Linux FUSE, macOS NFS, and Windows `ProjFS` projection boundary.

use crate::NativeMaterializationWork;
use crate::kernel::FileMetadata;
use crate::demand::NativeMountAllocationCustody;
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

/// Enters one driver callback's `acyclic.fs.mount.<callback>` span; the callback records `errno` on it when it fails.
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
pub(crate) use view_ledger::{ViewLedgerAdmission, ViewLedgerAdmissions};
pub use view_ledger::{ViewObserver, ViewOrigin, ViewStamp};

mod lazy;
pub use lazy::{LazyMountSource, NativeMountSourceWatchOwner};

mod customer;
pub use customer::{
    LazyMount, Mount, MountLifecycleError, MountOptions, MountPublication, NativeWorkingSet, NativeMountDrainProof, NativeMountPublicationFailure,
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
    MaterializeOptions, materialize_checkout,
    materialize_checkout_paths, restore_checkout_host_path,
};
pub(crate) use materialize::{
    MaterializeMode,
    NativeMaterializationActionPhase, materialize_checkout_host_paths_with_work_guarded,
    materialize_reader_paths,
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
        MountSourceError::Io(error, _) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(error) => native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => native_workspace_errno(error),
        MountSourceError::WorkspacePrefixFailure { source, .. } => native_workspace_errno(source),
        MountSourceError::NativeWorkspaceCause(cause) => native_workspace_errno(&cause.source),
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::NativeText { kind, .. } =>
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
        | MountSourceError::NativeRecordContextTransaction(_, _)
        | MountSourceError::NativeRecordCorrupt(_, _) => libc::EIO,
        MountSourceError::Metadata(_) => libc::EIO,
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        MountSourceError::SourceWatchStartup(_) | MountSourceError::NativeWatchOriginalIo(_)
        | MountSourceError::NativeWatchOriginalBackend(_)
        | MountSourceError::NativeWatchStartupFailed { .. }
        | MountSourceError::NativeWatchSourceStartupFailed { .. } => libc::EIO,
    }
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
    pub fn available(&self) -> bool { self.unavailable_reason.is_none() }

    /// Whether the driver observes I/O issued by its provider process.
    pub fn provider_process_io_observable(&self) -> bool {
        self.available() && matches!(self.kind, Some(NativeMountKind::LinuxFuse | NativeMountKind::MacOsNfs))
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

/// Receives content one piece at a time: the piece's logical offset and its canonical owned bytes.
pub type ContentSink<'a> = dyn FnMut(u64, acyclic_stream::OwnedBytes) -> Result<(), MountSourceError> + 'a;

/// Measured pinned content keeps the producer's exact live capacity through the actual native completion.
pub type NativeMountContentSink<'a> =
    dyn FnMut(u64, crate::kernel::OwnedSourceChunk) -> Result<(), MountSourceError> + 'a;

/// Opaque evidence of the exact external content one lookup observed.
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
pub trait MountOpenFile: Send + Sync + 'static {
    /// Looks up through the operative shared native invocation.
    fn lookup_with_meter(&self, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
    /// Reads an exact owned range admitted on the original invocation.
    fn read_range_with_meter(&self, offset: u64, length: u32, work: &NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads up to the original bound without losing the producer's capacity.
    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Finds the actual sparse boundary through the original source engine.
    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &NativeMaterializationWork)
        -> Result<Option<u64>, MountSourceError>;
    /// Consumes the caller's exact already-admitted owned payload.
    fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Changes length through the same operative source invocation.
    fn resize_with_meter(&self, logical_bytes: u64, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Applies the actual sparse allocation operation.
    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Mutates exact metadata and optional size through the same engine.
    fn set_attributes(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Reads an owned named attribute without erasing capacity.
    fn read_attribute(&self, name: &[u8], work: &NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned native attribute page.
    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted attribute value.
    fn write_attribute(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Removes the actual named attribute through the same engine.
    fn remove_attribute(&self, name: &[u8], work: &NativeMaterializationWork)
        -> Result<(), MountSourceError>;
    /// Settles this actual handle's queued effects before durability acknowledgement.
    fn settle(&self, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Returns the owned sparse range and its original producer's exact claim.
    fn read_sparse_range(&self, offset: u64, length: u32, work: &NativeMaterializationWork)
        -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>;
    /// Consumes the exact already-admitted sparse payload, including capacities known before each span's conversion to `Bytes`.
    fn write_sparse_range(&self, offset: u64, range: (MountSparseRange, crate::NativeAllocation),
        work: &NativeMaterializationWork) -> Result<(), MountSourceError>;

    /// Returns current identity, size, and metadata for `fstat`-style calls.
    fn lookup(&self) -> Result<MountLookup, MountSourceError>;
    /// Reads one exact bounded logical range.
    fn read_range(&self, offset: u64, length: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
    /// Reads at most `maximum_bytes`, clipping to EOF in the same coherent view.
    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
    /// Finds the next sparse data or hole boundary.
    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
    /// Replaces one exact byte range in the detached file.
    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError>;
    /// Changes the detached logical file length.
    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError>;
    /// Applies one exact sparse allocation operation.
    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError>;
    /// Lists one bounded page of native named-attribute names.
    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError>;
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
    /// The content pin a [`MountFilesystem::lookup_pinned`] of this entry issues in the same view, so a projection may answer that lookup from the listing.
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
    /// Retains a page and any original allocation custody.
    #[must_use]
    pub fn new(
        entries: Vec<MountDirectoryEntry>, next_cursor: Option<Vec<u8>>,
        allocation: Option<crate::NativeAllocation>,
    ) -> Self {
        let ownership = allocation.map_or_else(
            crate::demand::SourcePayloadOwnership::default,
            crate::demand::SourcePayloadOwnership::from_native_allocation,
        );
        Self { entries, next_cursor, ownership }
    }

    /// Borrows entries without detaching their physical capacity custody.
    #[must_use]
    pub fn entries(&self) -> &[MountDirectoryEntry] { &self.entries }

    /// Borrows this page's opaque continuation.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&[u8]> { self.next_cursor.as_deref() }

    /// Internal physical projections transfer data and its exact custody together, splitting that custody only for actual escaping payloads.
    pub(crate) fn into_parts(self) -> (
        Vec<MountDirectoryEntry>, Option<Vec<u8>>, crate::demand::SourcePayloadOwnership,
    ) {
        (self.entries, self.next_cursor, self.ownership)
    }

    /// Known owned capacities of this exact page, excluding borrowed metadata and opaque allocator overhead.
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

/// Retains a source failure and its semantic classification.
pub trait MountSourceFailureCause: Send + Sync {
    /// Returns the retained semantic failure.
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
    /// The original native operation has not finished reconciliation.
    #[error("original native operation is indeterminate")]
    Indeterminate,
    /// The original SDK seal returned an exact semantic decision.
    #[error("selected import publication was not committed: {0:?}")]
    SelectedPublication(crate::CheckoutCommitOutcome),
    /// Canonical namespace validation retains its exact typed cause.
    #[error(transparent)]
    NamespacePath(crate::kernel::NamespacePathError),
    /// Original OS cause and any already funded diagnostic custody.
    #[error("{0}")]
    Io(#[source] std::io::Error, Option<crate::NativeAllocation>),
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
    /// Original host metadata cause, moved without formatting a diagnostic.
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    #[error("filesystem engine failure: source demand failed: {0}")]
    Metadata(#[source] crate::native_host::NativeMetadataError),
    /// Original preallocated source-watch failure and its physical owner.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("filesystem engine failure: source demand failed: {0}")]
    SourceWatchStartup(#[source] Arc<crate::source_watch::SourceWatchStartupFailure>),
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
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native callback authorization failed")]
    NativeCallbackDenied {
        /// Complete original typed callback cause; no replacement box is made.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Original custody of the same physical failure slot.
        custody: crate::NativeAllocation,
    },
    /// Actual static SDK reason; construction allocates no diagnostic payload.
    #[error("{text}")]
    NativeText {
        /// Original diagnostic classification.
        kind: NativeSourceTextKind,
        /// Original constant reason, without string allocation.
        text: &'static str,
    },
    /// Original owned native record parser cause; no diagnostic allocation.
    #[error("native record JSON error: {0}")]
    NativeRecordJson(#[source] serde_json::Error, Option<crate::NativeAllocation>),
    /// Actual record lock acquisition timed out.
    #[error("native record lock timed out: {}", .0.display())]
    NativeRecordLockTimeout(PathBuf, Option<crate::NativeAllocation>),
    /// The existing record belongs to a different original owner.
    #[error("native record ownership conflict: {}", .0.display())]
    NativeRecordOwnershipConflict(PathBuf, Option<crate::NativeAllocation>),
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

    pub(crate) fn native_allocation_mut(&mut self) -> Option<&mut Option<crate::NativeAllocation>> {
        match self {
            Self::Io(_, allocation) | Self::NativeRecordJson(_, allocation)
            | Self::NativeRecordLockTimeout(_, allocation) | Self::NativeRecordOwnershipConflict(_, allocation)
            | Self::NativeRecordContextTransaction(_, allocation) | Self::NativeRecordCorrupt(_, allocation) =>
                Some(allocation),
            Self::MutationFinalization { custody, .. } => Some(custody),
            Self::NativeWorkspaceCause(cause) => Some(&mut cause.custody),
            Self::NativeWork(error) => error.native_failure_allocation_mut(),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MountSourceError {
    fn from(error: std::io::Error) -> Self { Self::Io(error, None) }
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
            NativeMountStartupAdmissionError::ServicesCold => Self::ServicesNotInitialized,
            NativeMountStartupAdmissionError::ServiceUnavailable(error) => Self::ServiceUnavailable(error),
        }
    }
}

/// Authentic source-owner authorization checked before a native SDK callback.
pub trait NativeMountCallbackGuard: Send + Sync {
    /// Admits the real verification future on the callback's original context.
    fn check<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
    /// Verifies current authored namespace WRITE permission on the same original task owner, fence, exact source and purpose.
    fn check_write<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError>;
}

/// One directly erased authorization future and its original funded custody.
pub struct NativeMountCallbackFuture<'a> {
    future: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a>>,
    _custody: crate::NativeAllocation,
}

impl<'a> NativeMountCallbackFuture<'a> {
    /// Admits the concrete boxed future before invoking its builder.
    pub fn new<F, B>(work: &NativeMaterializationWork, build: B) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F,
    {
        let future_bytes = std::mem::size_of::<F>();
        let allocation = work.claim_allocation(
            future_bytes as u64, u64::from(future_bytes != 0), crate::WorkCounters::UNCHARGED)?;
        let future = Box::pin(build());
        Ok(Self { future, _custody: allocation })
    }

    /// Preallocates the exact typed failure slot before authorization starts.
    pub fn new_guarded<E, F, B>(
        work: &NativeMaterializationWork, build: B,
    ) -> Result<Self, NativeMountError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: std::future::Future<Output = Result<(), E>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = callback_failure_slot::<E>(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => {
                    drop(failure);
                    Ok(())
                }
                Err(error) => Err(callback_failure(failure, error)),
            }
        })
    }
    /// Reuses an already funded native denial rather than nesting its custody inside another cause slot.
    pub(crate) fn new_native_guarded<F, B>(
        work: &NativeMaterializationWork, build: B,
    ) -> Result<Self, NativeMountError>
    where
        F: std::future::Future<Output = Result<(), NativeMountError>> + Send + 'a,
        B: FnOnce() -> F + Send + 'a,
    {
        let failure = callback_failure_slot::<NativeMountError>(work)?;
        Self::new(work, move || async move {
            match build().await {
                Ok(()) => Ok(()),
                Err(error @ NativeMountError::CallbackDenied { .. }) => Err(error),
                Err(error) => Err(callback_failure(failure, error)),
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

fn callback_failure_slot<E>(
    work: &NativeMaterializationWork,
) -> Result<(Box<std::mem::MaybeUninit<E>>, crate::NativeAllocation), NativeMountError> {
    let slot_bytes = std::mem::size_of::<E>();
    let allocation = work.claim_allocation(
        slot_bytes as u64, u64::from(slot_bytes != 0), crate::WorkCounters::UNCHARGED,
    )?;
    // Tuple fields retire the boxed slot before its allocation.
    Ok((Box::new(std::mem::MaybeUninit::uninit()), allocation))
}

fn callback_failure<E: std::error::Error + Send + Sync + 'static>(
    (source, custody): (Box<std::mem::MaybeUninit<E>>, crate::NativeAllocation), error: E,
) -> NativeMountError {
    let source = Box::write(source, error);
    NativeMountError::CallbackDenied { source, custody }
}

/// One existing physical driver's accounting activation, not task authority.
#[derive(Default)]
pub(crate) struct NativeMountMeterActivation {
    meter: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
    resource_allowance: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    execution: Option<std::sync::Weak<crate::materializer::NativeSourceExecutionView>>,
    depth: u64,
    generation: u64,
}

impl NativeMountMeterActivation {
    pub(crate) fn upgrade(&self) -> Option<NativeMaterializationWork> {
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
        let cancellation = meter.lock().cancellation().clone();
        Some(NativeMaterializationWork {
            cancellation,
            state: meter, resource: resource_allowance, capture_origin: self.capture_origin.clone(), invocation, execution,
        })
    }

    fn matches(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.as_ptr() == Arc::as_ptr(&work.state)
            && self.resource_allowance.as_ref().map(std::sync::Weak::as_ptr) == work.resource.as_ref().map(Arc::as_ptr)
            && self.capture_origin.as_ref().map(std::sync::Weak::as_ptr) == work.capture_origin.as_ref().map(std::sync::Weak::as_ptr)
            && self.execution.as_ref().map(std::sync::Weak::as_ptr) == work.execution.as_ref().map(Arc::as_ptr)
            && self.invocation.as_ref().map(std::sync::Weak::as_ptr) == work.invocation.as_ref().map(Arc::as_ptr)
    }

    pub(crate) fn enter(
        &mut self, work: &NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        if self.depth != 0 && !self.matches(work) {
            return Err(NativeMountError::ControlPending);
        }
        let depth = self.depth.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        if self.depth == 0 {
            let generation = self.generation.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            self.meter = Arc::downgrade(&work.state);
            self.resource_allowance = work.resource.as_ref().map(Arc::downgrade);
            self.capture_origin = work.capture_origin.clone();
            self.invocation = work.invocation.as_ref().map(Arc::downgrade);
            self.execution = work.execution.as_ref().map(Arc::downgrade);
            self.generation = generation;
        }
        self.depth = depth;
        Ok(self.generation)
    }

    pub(crate) fn leave(
        &mut self, work: &NativeMaterializationWork, generation: u64,
    ) {
        assert!(self.depth != 0 && self.generation == generation && self.matches(work),
            "native accounting scope must retire its own activation");
        self.depth -= 1;
        if self.depth == 0 {
            self.meter = std::sync::Weak::new();
            self.resource_allowance = None;
            self.capture_origin = None;
            self.invocation = None;
            self.execution = None;
        }
    }
}

/// Executes one installed SDK handler request with the original constructor's copied full grants and cancellation, through the existing native queue.
pub(crate) fn run_native_mount_handler<T>(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeMountError>,
) -> Result<crate::OperationReceipt<T>, (NativeMountError, crate::WorkCounters)> {
    let work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| (cause.into(), work))?;
    let result = run_native_mount_handler_on_meter(&work, operation);
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(cause) => Err((cause, work.receipt())),
    }
}

/// Startup/revalidation handlers use the already operative invocation.
pub(crate) fn run_native_mount_handler_on_meter<T>(
    work: &NativeMaterializationWork,
    operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        operation(work)
    })).unwrap_or_else(|panic| Err(NativeMountError::DriverPanicked {
        operation: "SDK callback", panic: std::sync::Mutex::new(panic),
    }))
}

/// Borrowed stock-source consistency data, not namespace or task authority.
pub struct NativeMountSourceLineage<'a> {
    owner: Arc<dyn std::any::Any + Send + Sync>,
    root: &'a crate::kernel::NamespacePath,
    native_original: Option<&'a crate::NativeMaterializationWork>,
    lazy_owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    lazy_root: Option<&'a str>,
    limits: crate::VolumeLimits,
    capture_gate: &'a std::sync::Mutex<adapter::CaptureSourceGate>,
    capture_cancellation: &'a crate::CancellationToken,
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
        Arc::ptr_eq(&self.owner, &next.owner) && self.root == *next.root
            && match (&self.native_original, next.native_original) {
                (Some(old), Some(new)) => old.shares_ledger(new) && old.shares_resource_allowance(new),
                (None, None) => true,
                _ => false,
            }
            && match (&self.lazy_owner, &next.lazy_owner) {
                (Some(old), Some(new)) => Arc::ptr_eq(old, new),
                (None, None) => true,
                _ => false,
            }
            && self.lazy_root.as_deref() == next.lazy_root
    }
}

/// Synchronous callback surface implemented by an embedded checkout adapter.
pub trait MountFilesystem: Send + Sync + 'static {
    /// Returns the sealed original native accounting and lifetime context.
    fn native_original_work(&self) -> Result<Option<NativeMaterializationWork>, MountSourceError> {
        Ok(None)
    }

    /// Returns an already-funded live authorization guard for a task source.
    fn native_callback_guard(&self) -> Option<Arc<dyn NativeMountCallbackGuard>> {
        None
    }

    /// Borrows the actual SDK checkout owner and canonical selected root.
    fn native_namespace_lineage(&self) -> Option<NativeMountSourceLineage<'_>> {
        None
    }

    /// Looks up through an already shared original native invocation.
    fn lookup_with_meter(
        &self, path: &MountPath, work: &NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up and pins through the same existing invocation.
    fn lookup_pinned_with_meter(
        &self, path: &MountPath, work: &NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError>;
    /// Registers the actual observer on the same original allocation ledger.
    fn observe_view_with_meter(
        &self, observer: std::sync::Weak<dyn ViewObserver>, work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Fences actual source changes without another ledger or queue.
    fn fence_changes_with_meter(&self, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Acquires the actual source binding lease with its physical drop proof.
    fn acquire_binding_lease_with_meter(
        &self, expected_epoch: Option<u64>, work: &NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError>;
    /// Captures exact paths through the existing original shared invocation.
    fn capture_host_paths_with_meter(
        &self, root: &Path, paths: &[MountPath], expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Captures one subtree through the same actual capture producer.
    fn capture_host_subtree_with_meter(
        &self, root: &Path, path: &MountPath, expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Reads a pinned range without erasing the actual owned chunk capacity.
    fn read_pinned_with_meter(
        &self, path: &MountPath, pin: MountContentPin, offset: u64, length: u64,
        piece: u32, sink: &mut NativeMountContentSink<'_>, work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Reads actual link bytes and retains their producer-known capacity.
    fn read_link(
        &self, path: &MountPath, work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads the exact range using the existing shared native queue.
    fn read_range_with_meter(
        &self, path: &MountPath, offset: u64, length: u32, work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads a page with the original ledger and actual capacity custody.
    fn read_directory_with_meter(
        &self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError>;

    /// Removes the exact binding on a shared original invocation.
    fn remove(
        &self, path: &MountPath, expected: Option<FileId>, work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Renames the exact binding with original source grants.
    fn rename(
        &self, source: &MountPath, destination: &MountPath, replace: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Mutates actual metadata and optional length through the same source.
    fn set_attributes(
        &self, path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Flushes the actual candidate on the shared original invocation.
    fn flush(&self, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Opens an actual attached handle and reports its pre-erasure SDK payload claim.
    fn open_file_with_meter(&self, path: &MountPath, work: &NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Opens and proves the handle in the same coherent original invocation.
    fn open_file_with_lookup_with_meter(&self, path: &MountPath, work: &NativeMaterializationWork)
        -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError>;
    /// Opens the actual identity just created, without resolving a replacement.
    fn open_created_with_meter(&self, path: &MountPath, created: &MountLookup, work: &NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Captures the actual detached open-file identity with its original claim.
    fn detach_file_with_meter(&self, path: &MountPath, work: &NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Creates an empty file, preserving an optional absent-since proof.
    fn create_file(&self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
    /// Creates an actual directory through the same source engine.
    fn create_directory(&self, path: &MountPath, metadata: FileMetadata, work: &NativeMaterializationWork)
        -> Result<MountLookup, MountSourceError>;
    /// Consumes the exact already-admitted opaque symbolic-link target.
    fn create_symbolic_link(&self, path: &MountPath, target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
    /// Creates the exact native special-file kind and device identity.
    fn create_special(&self, path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>,
        metadata: FileMetadata, work: &NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
    /// Reads the exact owned attribute on the operative source invocation.
    fn read_attribute(&self, path: &MountPath, name: &[u8], work: &NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned attribute page.
    fn list_attributes_with_meter(&self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted native attribute value.
    fn write_attribute(&self, path: &MountPath, name: &[u8],
        value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode, work: &NativeMaterializationWork)
        -> Result<(), MountSourceError>;
    /// Removes the exact native attribute.
    fn remove_attribute(&self, path: &MountPath, name: &[u8], work: &NativeMaterializationWork)
        -> Result<(), MountSourceError>;
    /// Clones the actual immutable range between authenticated file identities.
    fn clone_range_by_id(&self, source_file_id: FileId, source_offset: u64,
        destination_file_id: FileId, destination_offset: u64, length: u64, work: &NativeMaterializationWork)
        -> Result<(), MountSourceError>;
    /// Creates the actual same-volume hard-link binding.
    fn hard_link(&self, source: &MountPath, destination: &MountPath, work: &NativeMaterializationWork)
        -> Result<(), MountSourceError>;
    /// Folds the actual source representation and reports exact owned capacity.
    fn folded_path_with_meter(&self, path: &MountPath, work: &NativeMaterializationWork)
        -> Result<Option<(MountPath, u64)>, MountSourceError>;
    /// Queries actual source stability on the operative invocation.
    fn view_is_stable_with_meter(&self, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
    /// Queries the actual source stamp.
    fn view_stamp_with_meter(&self, work: &NativeMaterializationWork) -> Result<Option<ViewStamp>, MountSourceError>;
    /// Queries the actual writable binding epoch.
    fn binding_epoch_with_meter(&self, work: &NativeMaterializationWork) -> Result<Option<u64>, MountSourceError>;
    /// Traverses actual source change history under the original item limits.
    fn unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
    /// Traverses only actual reported changes through the same history engine.
    fn reported_unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
    /// Stops the actual owned source watcher; success confirms physical quiescence (or that this source owns no active watcher).
    fn shutdown_with_meter(&self, work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
    /// Retires this held source using only its constructor-owned cleanup credit.
    fn shutdown_with_cleanup_wait(
        &self, _wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Completes this source's already accepted physical retirement.
    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Traverses actual changes to this file identity under original limits.
    fn node_unchanged_since_with_meter(&self, file_id: FileId, stamp: ViewStamp,
        work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;
    /// Traverses actual changes to this directory's immediate listing.
    fn listing_unchanged_since_with_meter(&self, path: &MountPath, stamp: ViewStamp,
        work: &NativeMaterializationWork) -> Result<bool, MountSourceError>;

    /// Whether this source can represent native POSIX named attributes.
    fn supports_posix_named_attributes(&self) -> bool {
        false
    }

    /// Whether a native handle close is itself a publication boundary.
    fn flush_on_handle_close(&self) -> bool {
        true
    }

    /// Whether [`Self::flush`] can publish anything.
    fn flush_publishes(&self) -> bool {
        true
    }

    /// Whether a lookup may observe one coherent source view right now.
    fn view_is_stable(&self) -> bool {
        true
    }

    /// Samples this source's position in the order of view changes, before a lookup whose result a driver may cache.
    fn view_stamp(&self) -> Option<ViewStamp> {
        None
    }

    /// Whether a lookup of `path`, which resolved to `file_id` (`None`: to nothing) after `stamp` was sampled, still describes the view.
    fn unchanged_since(
        &self,
        _path: &MountPath,
        _file_id: Option<FileId>,
        _stamp: ViewStamp,
    ) -> bool {
        false
    }

    /// [`Self::unchanged_since`], counting only changes the source reported: facts read after `stamp` that a driver read again since, with the same result, still describe the view across changes the source could not yet confirm ([`super::demand::SourceChange::Unconfirmed`]).
    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.unchanged_since(path, file_id, stamp)
    }

    /// Whether this source reports every change to the node `file_id`, so facts about it may be kept (by a kernel, or an NFS client) until a reported change supersedes them.
    fn reports_changes_to(&self, _file_id: FileId) -> bool {
        self.view_stamp().is_some()
    }

    /// Whether facts about the node `file_id`, read after `stamp` was sampled, still describe it: no change after `stamp` touched the node under any of its names.
    fn node_unchanged_since(&self, _file_id: FileId, _stamp: ViewStamp) -> bool {
        false
    }

    /// Whether the listing of the directory at `path` is unchanged since `stamp`: no name directly beneath it was bound, unbound, or rebound since.
    fn listing_unchanged_since(&self, _path: &MountPath, _stamp: ViewStamp) -> bool {
        false
    }

    /// The spelling every equivalent spelling of `path` resolves to, when this source folds names; `None` when every spelling is distinct.
    fn folded_path(&self, _path: &MountPath) -> Option<MountPath> {
        None
    }

    /// Tells `observer` of every change to this source's view from now on, together with the origin that made it.
    fn observe_view(&self, _observer: std::sync::Weak<dyn ViewObserver>) {}

    /// Returns once every change made to what this source projects outside every view of it, and completed before the call, is recorded in the view.
    fn fence_changes(&self) -> Result<(), MountSourceError> {
        Ok(())
    }

    /// Changes only when existing path or handle bindings may be replaced by an external source transition.
    fn binding_epoch(&self) -> Option<u64> {
        None
    }

    /// Pins the current coherent projection view for one native callback.
    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        if !self.view_is_stable() {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Pins source binding identity without treating ordinary native writes as an external rebind.
    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        if !self.view_is_stable() || self.binding_epoch() != expected_epoch {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Looks up one absolute portable path without following links.
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up one path like [`Self::lookup`], also pinning the exact external content a regular file's later [`Self::read_pinned`] must reproduce.
    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        Ok(self.lookup(path)?.map(|lookup| (lookup, None)))
    }

    /// Streams one range of the content a [`Self::lookup_pinned`] pin promised for `path` to `sink`, in ascending gap-free pieces of at most `piece` bytes, resolving and opening the source once for the whole range.
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

    /// Opens one regular file as an attached path-independent handle.
    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
    /// Opens `path` as [`Self::open_file`] does, with the facts the opened file had: a source whose open already proved them reports them without asking the file again.
    fn open_file_with_lookup(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        let file = self.open_file(path)?;
        let lookup = file.lookup()?;
        Ok((file, lookup))
    }
    /// Opens `created`, the regular file [`Self::create_file`] just made at `path`.
    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let _ = created;
        self.open_file(path)
    }
    /// Captures one regular file as a detached path-independent open-file view.
    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;

    /// Reads at most one exact logical range.
    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;

    /// Finds the first sparse data or hole byte at/after one offset.
    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError>;
    /// Reads one bounded page using a handle-owned opaque cursor.
    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError>;

    /// Lists one bounded page of native named-attribute names.
    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError>;
    /// Replaces one exact file range.
    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError>;
    /// Changes one file's logical length.
    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
    /// Applies one exact sparse allocation operation.
    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError>;
    /// Clones one immutable range between regular files without reading bytes.
    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError>;
    /// Reconciles exact final host state for one projected path and seals it.
    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
    /// Reconciles exact final host states together, preserving hard-link topology and admitting one bounded checkout candidate.
    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError>;
    /// Reconciles one imported directory and every descendant atomically.
    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
}

/// Closed, allocation-free cause set for admission before startup failure storage exists.
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
    #[error("native mount capability is unavailable: {0}")]
    CapabilityUnavailable(&'static str),
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
    /// No admitted startup-storage allocation exists yet.
    #[error("native mount startup admission failed: {cause}")]
    StartupAdmissionFailed {
        /// Exact original pre-storage admission cause.
        #[source]
        cause: NativeMountStartupAdmissionError,
        /// Actual supplied source, retained without allocating another owner.
        source: Arc<dyn MountFilesystem>,
    },
    /// Watch construction completed physically but a later source admission failed.
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
            MountSourceError::Io(error, None) => Self::Io(error),
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

macro_rules! with_driver {
    ($driver:expr, $session:ident, $operation:expr, $unsupported:expr $(,)?) => {
        with_driver!($driver, $session, $operation, $operation, $unsupported)
    };
    ($driver:expr, $session:ident, $operation:expr, $windows:expr, $unsupported:expr $(,)?) => {
        match $driver {
            #[cfg(target_os = "linux")]
            DriverSession::Fuse($session) => $operation,
            #[cfg(target_os = "macos")]
            DriverSession::DarwinMount($session) => $operation,
            #[cfg(target_os = "windows")]
            DriverSession::ProjFs($session) => $windows,
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            DriverSession::Unsupported => $unsupported,
        }
    };
}

/// Real cleanup custody and driver evidence for one retained native session.
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
    pub namespace_detach_confirmed: bool,
    /// The last measured cleanup failed; retained custody requires explicit retry.
    pub measured_cleanup_failed: bool,
}

impl NativeMountCleanupState<'_> {
    /// Whether driver, source/watcher, and destination fence were released.
    pub fn cleanup_complete(&self) -> bool { !self.driver_retained && !self.destination_fence_retained && !self.source_retained }
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

enum NativeRecoveryCommand {
    Action { remove_destination: bool, work: crate::NativeMaterializationWork },
    LastDrop,
}

struct NativeRecoveryMailbox {
    command: Option<NativeRecoveryCommand>,
    reply: Option<Result<bool, NativeMountError>>,
    snapshot: Option<NativeRecoverySnapshot>,
    // Actual state DATA has dropped; this is not a std thread-join proof.
    state_retired: bool,
}

struct NativeRecoveryShared {
    destination: PathBuf,
    mailbox: std::sync::Mutex<NativeRecoveryMailbox>,
    wake: std::sync::Condvar,
    // Physical path, slot, and reply DATA precede their original custody.
    _custody: crate::NativeAllocation,
}

struct NativeRecoveryWorker {
    shared: Arc<NativeRecoveryShared>,
    state: NativeRecoveryState,
    original: crate::NativeMaterializationWork,
    final_close: crate::NativeWorkReservation,
    _closure_custody: crate::NativeAllocation,
}

/// Original recovery control and destination fence, acquired once before action.
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
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| NativeMountError::from(error))?;
        Self::acquire_with_original(destination, original)
    }

    fn acquire_with_original(
        destination: &Path, original: crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let mut destination = crate::native_host::clone_path_with_work(destination, &mut &original)?;
        let mut shared_custody = original.claim_allocation(std::mem::size_of::<NativeRecoveryShared>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        destination.1.absorb(&mut shared_custody)?;
        let closure_custody = original.claim_allocation(std::mem::size_of::<NativeRecoveryWorker>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
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
                command: None, reply: None, snapshot: None, state_retired: false,
            }),
            wake: std::sync::Condvar::new(),
            _custody: destination.1,
        });
        let worker = NativeRecoveryWorker {
            shared: Arc::clone(&shared),
            state: NativeRecoveryState {
                #[cfg(windows)]
                destination_identity: None,
                guard: None, control, driver_step_complete: false,
                destination_removed: false, measured_cleanup_failed: false, panic: None, retirement_error: None,
            },
            original, final_close,
            _closure_custody: closure_custody,
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
            mailbox.reply.take().expect("actual acquisition reply")
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
        }
    }
    /// Recovers through the same engine with an explicit unbounded allowance.
    pub fn recover(&mut self) -> Result<bool, NativeMountError> { self.ordinary_action(true) }

    /// Detaches while preserving the underlying destination and authored residue.
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

    /// Detaches on finite work without removing the destination.
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
            mailbox.command = Some(NativeRecoveryCommand::Action { remove_destination, work: work.clone() });
            self.shared.wake.notify_one();
        }
        let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
        while mailbox.reply.is_none() {
            mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
        }
        let result = mailbox.reply.take().expect("actual action reply");
        drop(mailbox);
        let spent = work.receipt();
        result.map(|value| crate::OperationReceipt { value, work: spent }).map_err(|error| (error, spent))
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
                .release(&mut &*work)?;
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
        MountDestinationGuard::acquire_with_work(
            &self.shared.destination, true, &self.original, &mut self.state.guard, true,
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
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                { self.state.control.retire_controls(&self.original) }
                #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                { self.state.control.retire_controls_with_work(&self.original) }
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
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.acquire())) {
            Ok(result) => result,
            Err(panic) => {
                self.state.panic = Some(panic);
                Err(NativeMountError::Lifecycle("administrative acquisition panicked"))
            }
        };
        let identity = match result {
            Ok(identity) => identity,
            Err(error) => {
                self.retire();
                drop(self.state);
                let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                mailbox.reply = Some(Err(error));
                mailbox.state_retired = true;
                self.shared.wake.notify_all();
                return;
            }
        };
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            mailbox.snapshot = Some(self.state.snapshot(identity));
            mailbox.reply = Some(Ok(true));
            self.shared.wake.notify_all();
        }
        loop {
            let command = {
                let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                while mailbox.command.is_none() {
                    mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
                }
                mailbox.command.take().expect("original administrative slot")
            };
            let reply = match command {
                NativeRecoveryCommand::Action { remove_destination, work } => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.state.finish_with_work(&self.shared.destination, remove_destination, &work)
                    }));
                    match result {
                        Ok(result) => result,
                        Err(panic) => {
                            self.state.panic = Some(panic);
                            self.state.measured_cleanup_failed = true;
                            Err(NativeMountError::Lifecycle("administrative action panicked"))
                        }
                    }
                }
                NativeRecoveryCommand::LastDrop => {
                    self.retire();
                    // Drop state data outside the mutex, before the worker's allocation guard.
                    drop(self.state);
                    let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                    mailbox.state_retired = true;
                    self.shared.wake.notify_all();
                    return;
                }
            };
            let snapshot = self.state.snapshot(identity);
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
    { let _ = disposable; control.recover(destination, work) }
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

/// Actual failed constructor custody.
pub struct NativeMountStartupFailure {
    state: Box<NativeMountStartupFailureState>,
    custody: crate::NativeAllocation,
}

impl NativeMountStartupFailure {
    /// Original typed constructor failure.
    #[must_use]
    pub fn error(&self) -> &NativeMountError { &self.state.error }
    /// Original driver and physical destination fence.
    #[must_use]
    pub fn owner(&self) -> &NativeMountSession { &self.state.owner }
    /// Retains the original owner for a bounded cleanup retry.
    pub fn owner_mut(&mut self) -> &mut NativeMountSession { &mut self.state.owner }
    /// Original warm-continuation owner, including all unresolved DATA.
    #[must_use]
    pub fn retired_owner(&self) -> Option<&NativeMountSession> {
        self.state.retired.as_ref()
    }
    /// Moves the original cause, new source owner, and complete old outcome.
    #[must_use]
    pub fn into_parts(self) -> (NativeMountError, NativeMountSession, Option<NativeMountSession>) {
        let NativeMountStartupFailureState { error, owner, retired } = *self.state;
        drop(self.custody);
        (error, owner, retired)
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
/// Actual preadmitted failure storage.
pub struct PreparedNativeMountStart {
    buffer: Box<std::mem::MaybeUninit<NativeMountStartupFailureState>>,
    retired: Option<NativeMountSession>,
    custody: crate::NativeAllocation,
}
impl PreparedNativeMountStart {
    fn new(work: &crate::NativeMaterializationWork) -> Result<Self, NativeMountStartupAdmissionError> {
        let custody = work.claim_allocation(
            std::mem::size_of::<NativeMountStartupFailureState>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        Ok(Self { buffer: Box::new(std::mem::MaybeUninit::uninit()), retired: None, custody })
    }
    #[allow(unsafe_code, reason = "transfers fully initialized preallocated failure storage")]
    fn failed(self, error: NativeMountError, owner: NativeMountSession) -> NativeMountStartupFailure {
        let Self { mut buffer, retired, custody } = self;
        buffer.write(NativeMountStartupFailureState { error, owner, retired });
        // SAFETY: write initialized this preallocated box; conversion preserves the allocation.
        let state = unsafe { buffer.assume_init() };
        NativeMountStartupFailure { state, custody }
    }
}

/// Admits startup failure storage on the original ledger.
pub fn prepare_native_mount_start(
    work: &crate::NativeMaterializationWork,
) -> Result<PreparedNativeMountStart, NativeMountStartupAdmissionError> {
    let _services = adapter::CallbackRuntime::existing_startup()?;
    work.admit(crate::WorkCounters::UNCHARGED)?;
    PreparedNativeMountStart::new(work)
}

impl PreparedNativeMountStart {
    /// Starts the same actual constructor on an already operative original Arc.
    pub fn start(
        self, request: NativeMountRequest, source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork, destination_custody: Option<crate::NativeAllocation>,
    ) -> Result<NativeMountSession, NativeMountError> {
        start_native_mount_prepared_with_meter(request, source, false, work, self, destination_custody)
    }

    /// Continues only this SDK owner's retired Windows namespace with a freshly constructed stock source.
    pub fn restart_with_meter(
        mut self, retired: NativeMountSession, source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeMountSession, NativeMountError> {
        let mut next = NativeMountSession::new(NativeMountRequest {
            destination: PathBuf::new(),
            ..retired.request
        }, Some(source.clone()), None);
        self.retired = Some(retired);
        let admitted = (|| -> Result<(), NativeMountError> {
            let original = work.clone();
            let storage_work = self.custody.work();
            if !storage_work.shares_ledger(&original) || !storage_work.shares_resource_allowance(&original) {
                return Err(NativeMountError::Lifecycle("restart differs from its preborn original work"));
            }
            original.admit(crate::WorkCounters::UNCHARGED)?;
            let old = self.retired.as_ref().expect("complete original owner");
            if !old.cleanup_state().cleanup_complete() {
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
                    &old.request.destination, &mut &original,
                )?;
                next.request.destination = destination;
                next._destination_custody = Some(custody);
                Ok(())
            }
        })();
        if let Err(error) = admitted {
            return Err(NativeMountError::StartupFailed(self.failed(error, next)));
        }
        start_native_mount_prepared_with_meter(
            next.request, source, true, work, self, next._destination_custody,
        )
    }

    /// Places an original failed source constructor into this precreated owner.
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
        let mut owner = NativeMountSession::new(request, None, Some(destination_custody));
        owner.startup_source_watch = startup_source_watch;
        owner.measured_cleanup_failed = true;
        NativeMountError::StartupFailed(self.failed(error, owner))
    }
}

/// Upper lifecycle payload held by the existing physical driver loop.
pub(super) struct NativeMountRetirement {
    pub(super) retained_source: Option<Arc<dyn MountFilesystem>>,
    pub(super) destination_guard: Option<MountDestinationGuard>,
    pub(super) startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    #[cfg(windows)]
    warm_namespace: Option<projfs::ProjFsNamespaceQualification>,
}

impl NativeMountRetirement {
    /// Called only after the driver has actually drained namespace callbacks.
    pub(super) fn retire(
        &mut self, wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, NativeMountError> {
        let mut changed = self.retire_source_and_watch_with_cleanup(wait)?;
        if let Some(guard) = self.destination_guard.as_mut() {
            guard.release_cleanup(wait)?;
            drop(self.destination_guard.take());
            changed = true;
        }
        Ok(changed)
    }

    /// Drains source producers after callback admission closes, before the driver's own namespace teardown.
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

/// Process-owned mounted namespace.
pub struct NativeMountSession {
    // Native producers retire before source data and the destination fence.
    driver: Option<DriverSession>,
    startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    retained_source: Option<Arc<dyn MountFilesystem>>,
    request: NativeMountRequest,
    destination_guard: Option<MountDestinationGuard>,
    measured_cleanup_failed: bool,
    namespace_detach_confirmed: bool,
    completed_future_callback_work: crate::WorkCounters,
    retired_callback_failure: std::sync::Mutex<Option<(NativeMountError, crate::WorkCounters)>>,
    // SDK namespace DATA can outlive physical retirement without retaining
    // a source, context, root handle, or provider callback.
    #[cfg(windows)]
    namespace_outcome: Option<projfs::ProjFsNamespaceOutcome>,
    #[cfg(windows)]
    namespace_binding: Option<NativeMountNamespaceBinding>,
    // Destination data and all native owners physically drop before its guard.
    _destination_custody: Option<crate::NativeAllocation>,
}

/// Owned accounting activation for this existing physical namespace.
#[must_use]
pub struct OwnedNativeMountOriginalWorkScope {
    #[cfg(target_os = "linux")]
    _driver: fuse::FuseStartupMeterScope,
    #[cfg(target_os = "macos")]
    _driver: darwin_mount::DarwinStartupMeterScope,
    #[cfg(target_os = "windows")]
    _driver: projfs::ProjectionWorkScope,
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    _driver: std::convert::Infallible,
}

impl OwnedNativeMountOriginalWorkScope {
    /// Retains this same driver's activation in an accepted native I/O payload.
    pub fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        { Ok(Self { _driver: self._driver.try_clone_for_io()? }) }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        match self._driver {}
    }
}

impl NativeMountSession {
    fn new(request: NativeMountRequest, retained_source: Option<Arc<dyn MountFilesystem>>,
        destination_custody: Option<crate::NativeAllocation>) -> Self {
        Self {
            request,
            driver: None, destination_guard: None, retained_source, startup_source_watch: None,
            measured_cleanup_failed: false,
            namespace_detach_confirmed: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
            #[cfg(windows)]
            namespace_binding: None,
            _destination_custody: destination_custody,
        }
    }

    /// Activates the original meter without borrowing this session across I/O.
    pub fn begin_original_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<OwnedNativeMountOriginalWorkScope, NativeMountError> {
        let driver = self.driver.as_ref()
            .ok_or_else(|| NativeMountError::Io(std::io::ErrorKind::NotConnected.into()))?;
        let scope = with_driver!(
            driver, session, session.begin_original_work(work), Err(NativeMountError::UnsupportedTarget),
        )?;
        Ok(OwnedNativeMountOriginalWorkScope { _driver: scope })
    }

    /// Authenticated active kernel-root identity captured by this driver during its original constructor.
    #[must_use]
    pub fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        self.driver.as_ref().and_then(|driver| {
            with_driver!(driver, session, session.native_root_identity(), None)
        })
    }

    /// Stable mount identity.
    #[must_use]
    pub const fn mount_id(&self) -> MountId {
        self.request.mount_id
    }

    /// Exact mounted destination.
    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.request.destination
    }

    /// Records a genuine failure in the enclosing measured cleanup stage.
    pub fn record_measured_cleanup_failure(&mut self) {
        self.measured_cleanup_failed = true;
    }

    /// Borrows actual cleanup identity, retained ownership, and detach evidence.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountCleanupState<'_> {
        let (driver_fence, driver_source) = self.driver.as_ref()
            .map_or((false, false), |driver| with_driver!(driver, session, session.retirement_ownership(), (false, false)));
        NativeMountCleanupState {
            mount_id: self.request.mount_id,
            destination: &self.request.destination,
            driver_retained: self.driver.is_some(),
            destination_fence_retained: self.destination_guard.is_some() || driver_fence,
            source_retained: self.retained_source.is_some() || self.startup_source_watch.is_some() || driver_source,
            namespace_detach_confirmed: self.driver.as_ref().map_or(
                self.namespace_detach_confirmed, |driver| with_driver!(driver, session, session.namespace_detach_confirmed(), false),
            ),
            measured_cleanup_failed: self.measured_cleanup_failed,
        }
    }

    #[cfg(windows)]
    pub(super) fn take_retired_namespace_outcome(
        &mut self,
    ) -> Option<projfs::ProjFsNamespaceOutcome> {
        if !self.cleanup_state().cleanup_complete() { return None; }
        self.namespace_outcome.take()
    }

    /// Moves the first retained fatal SDK callback failure and its actual completed request work.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        let retained = self.retired_callback_failure.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(failure) = retained {
            return Some(failure);
        }
        self.driver.as_ref().and_then(|driver| {
            with_driver!(driver, session, session.take_callback_failure(), None)
        })
    }

    /// Completed future SDK request work, including ordinary errno answers.
    #[must_use]
    pub fn completed_callback_work(&self) -> crate::WorkCounters {
        self.driver.as_ref().map_or(
            self.completed_future_callback_work, |driver| with_driver!(driver, session, session.completed_callback_work(), crate::WorkCounters::UNCHARGED),
        )
    }

    pub(crate) fn flush_callbacks(&self) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.flush_callbacks(&NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(cause, _)| projfs::detach_work_error(cause))?),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Some(_) => Ok(()),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(DriverSession::Unsupported) => Err(NativeMountError::UnsupportedTarget),
            None => Ok(()),
        }
    }

    /// Flushes the actual SDK callbacks without borrowing work across a wait.
    pub fn flush_callbacks_with_meter(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.flush_callbacks(work),
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

    /// Revalidates on the same original shared callback and control meter.
    pub fn revalidate_with_meter(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let driver = self.driver.as_ref()
            .ok_or_else(|| NativeMountError::Io(std::io::ErrorKind::NotConnected.into()))?;
        with_driver!(driver, session, session.revalidate(work), Err(NativeMountError::UnsupportedTarget))
    }

    /// Drops the kernel's cached entry/attributes for one mount-relative path (leading `/` optional).
    pub fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let driver = self.driver.as_ref()
            .ok_or_else(|| NativeMountError::Driver("session is stopped".to_owned()))?;
        with_driver!(driver, session, session.invalidate(path), session.invalidate(path, &NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(cause, _)| projfs::detach_work_error(cause))?), Err(NativeMountError::UnsupportedTarget))
    }

    /// Brings kernel caches in line with a changed source view.
    pub fn revalidate(&self) -> Result<(), NativeMountError> {
        let driver = self.driver.as_ref()
            .ok_or_else(|| NativeMountError::Driver("session is stopped".to_owned()))?;
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(cause, _)| {
            #[cfg(windows)]
            { projfs::detach_work_error(cause) }
            #[cfg(not(windows))]
            { NativeMountError::from(cause) }
        })?;
        with_driver!(driver, session, session.revalidate(&work), Ok(()))
    }

    /// Detaches the kernel namespace and ends the driver session; returns whether a live session was actually stopped.
    pub fn stop(&mut self) -> Result<bool, NativeMountError> {
        if self.cleanup_state().cleanup_complete() { return Ok(false); }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.measured_cleanup_failed = true;
            return Err(error.into());
        }
        self.stop_measured(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|(error, _work)| error)
    }

    /// Detaches through the existing platform driver and destination-fence machinery, admitting every actual native leaf on one original ledger.
    pub fn stop_measured(
        &mut self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.measured_cleanup_failed = true;
                return Err((error.into(), work));
            }
        };
        match self.stop_with_meter(&work) {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
            Err(error) => Err((error, work.receipt())),
        }
    }

    /// Continues the same physical cleanup engine on the original shared Arc.
    pub fn stop_with_meter(
        &mut self, work: &NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::Meter(work))
    }

    /// Retires the original namespace using its before-start accepted controls.
    pub fn stop_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::AcceptedWait(wait))
    }

    fn stop_with_context(
        &mut self, work: NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            let mut changed = false;
            if let Some(driver) = self.driver.as_mut() {
                with_driver!(&mut *driver, session, session.stop(&work), session.stop(match &work {
                    NativeSessionStopWork::Meter(meter) => meter.cancellation(),
                    NativeSessionStopWork::AcceptedWait(wait) => wait,
                }), Err(NativeMountError::UnsupportedTarget))?;
                self.namespace_detach_confirmed = with_driver!(&*driver, session, session.namespace_detach_confirmed(), false);
                // Copy settled callback totals and faults before dropping the driver context.
                self.completed_future_callback_work = with_driver!(&*driver, session, session.completed_callback_work(), crate::WorkCounters::UNCHARGED);
                *self.retired_callback_failure.get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    with_driver!(&*driver, session, session.take_callback_failure(), None);
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
                        NativeSessionStopWork::Meter(meter) => owner.stop(&mut adapter::NativeDemandCarrier::Native(meter)).map_err(NativeMountError::from),
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
                    NativeSessionStopWork::Meter(meter) => guard.release(&mut &**meter)?,
                    NativeSessionStopWork::AcceptedWait(wait) => guard.release_cleanup(Some(wait))?,
                }
                drop(self.destination_guard.take());
                changed = true;
            }
            Ok(changed)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }
}

enum NativeSessionStopWork<'a> {
    Meter(&'a NativeMaterializationWork),
    AcceptedWait(&'a crate::CancellationToken),
}

/// Probes the target's one canonical native namespace mechanism.
#[must_use]
pub fn probe_native_mount() -> NativeMountCapabilities {
    #[cfg(target_os = "linux")]
    let (kind, available, reason) = (
        Some(NativeMountKind::LinuxFuse),
        std::fs::OpenOptions::new().read(true).write(true).open("/dev/fuse").is_ok(),
        "/dev/fuse is not usable",
    );
    #[cfg(target_os = "macos")]
    let (kind, available, reason) = (
        Some(NativeMountKind::MacOsNfs), Path::new("/sbin/mount_nfs").is_file(),
        "the built-in macOS NFS mount utility is unavailable",
    );
    #[cfg(windows)]
    let (kind, available, reason) = (
        Some(NativeMountKind::WindowsProjFs),
        Path::new(&std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into()))
            .join("System32").join("ProjectedFSLib.dll").is_file(),
        "ProjectedFSLib.dll is unavailable",
    );
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    let (kind, available, reason) = (None, false, "unsupported target");
    NativeMountCapabilities {
        kind,
        unavailable_reason: (!available).then_some(reason),
    }
}

/// Starts a real native projection over the supplied canonical checkout.
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
    let storage = match PreparedNativeMountStart::new(&work) {
        Ok(storage) => storage,
        Err(cause) => return Err(NativeMountError::StartupAdmissionFailed { cause, source }),
    };
    let initialized = initialize_native_mount_services().and_then(|()| {
        acyclic_native_runtime::initialize_native_io_services().map_err(Into::into)
    });
    if let Err(error) = initialized {
        let owner = NativeMountSession::new(request, Some(source), None);
        return Err(NativeMountError::StartupFailed(storage.failed(error, owner)));
    }
    start_native_mount_prepared_with_meter(request, source, allow_nonempty, &work, storage, None)
}

/// Finite projection of the same native constructor and actual work prefix.
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
    let prepared = match prepare_native_mount_start(&work) {
        Ok(prepared) => prepared,
        Err(cause) => return Err((NativeMountError::StartupAdmissionFailed { cause, source }, work.receipt())),
    };
    match start_native_mount_prepared_with_meter(
        request, source, false, &work, prepared, None,
    ) {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

fn start_native_mount_prepared_with_meter(
    request: NativeMountRequest, source: Arc<dyn MountFilesystem>, allow_nonempty: bool,
    work: &NativeMaterializationWork, storage: PreparedNativeMountStart,
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
    let result = lineage_admission.and_then(|()| start_driver_into(
        &request, source, work, &mut driver, &mut retirement, |packet| {
            let _services = adapter::CallbackRuntime::existing()?;
            work.state.lock().check()?;
            MountDestinationGuard::acquire_with_work(
                &request.destination, allow_nonempty, work, &mut packet.destination_guard, false,
            )
        },
    ));
    // A prebirth rejection leaves its packet here. An accepted loop owns it
    // even on failure; the retained driver signals that same owner to retire.
    let (destination_guard, retained_source, startup_source_watch) = match retirement {
        Some(packet) => (packet.destination_guard, packet.retained_source, packet.startup_source_watch),
        None => (None, None, None),
    };
    let mut owner = NativeMountSession::new(request, retained_source, destination_custody);
    owner.driver = driver;
    owner.destination_guard = destination_guard;
    owner.startup_source_watch = startup_source_watch;
    #[cfg(windows)]
    { owner.namespace_binding = namespace_binding; }
    match result {
        Ok(()) => {
            drop(storage);
            Ok(owner)
        },
        Err(error) => {
            Err(NativeMountError::StartupFailed(storage.failed(error, owner)))
        },
    }
}

/// Starts a native projection mounted directly over an existing, non-empty directory â€” its contents are shadowed for the mount's duration and reappear once the returned session stops.
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
pub fn reclaim_native_mount_destination_fence(destination: &Path) -> Result<(), NativeMountError> {
    let canonical = destination
        .canonicalize()
        .map_err(|_| NativeMountError::InvalidDestination)?;
    MountDestinationGuard::reclaim_canonical(&canonical)
}

/// Reclaims every unlocked native-mount destination fence in one parent.
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

/// Recovers one implementation-owned disposable native destination after its provider process dies.
pub fn recover_native_mount_destination(destination: &Path) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.recover() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed { error: Box::new(error), owner }),
    }
}

/// Reclaims a Windows provider's dead mount path without deleting authored residue.
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
    let cancellation = crate::CancellationToken::new();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    guard.release(&mut work)?;
    Ok(preserved)
}

/// Detaches and unfences one crash-left native destination while retaining its underlying directory and authored host residue for an explicit caller decision.
pub fn detach_native_mount_destination_after_crash(
    destination: &Path,
) -> Result<(), NativeMountError> {
    let mut owner = NativeMountRecoveryOwner::acquire(destination)?;
    match owner.detach() {
        Ok(_) => Ok(()),
        Err(error) => Err(NativeMountError::RecoveryFailed { error: Box::new(error), owner }),
    }
}

const NATIVE_FENCE_PREFIX: &str = ".acyclic-fs-mount-";
const NATIVE_FENCE_SUFFIX: &str = ".lock";
const NATIVE_FENCE_NAME_BYTES: usize = NATIVE_FENCE_PREFIX.len() + 64 + NATIVE_FENCE_SUFFIX.len();
const NATIVE_REGISTRY_NAME: &str = ".acyclic-fs-mount-registry.lock";

#[derive(Debug)]
struct MountDestinationGuard {
    file: Option<File>,
    lock_path: PathBuf,
    // Retain the physical parent and fixed leaf so replacing an alias cannot redirect cleanup.
    parent_directory: Option<cap_std::fs::Dir>,
    fence_name: Option<[u8; NATIVE_FENCE_NAME_BYTES]>,
    registry_file: Option<File>,
    cleanup_name: Option<crate::native_host::NativePreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>>,
    birth_cleanup: Option<crate::NativeWorkReservation>,
}

/// The accepted fence path has no allocator or new cleanup-reservation ingress.
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
        destination: &Path, allow_nonempty: bool, native: &NativeMaterializationWork,
        guard: &mut Option<Self>,
        recovery: bool,
    ) -> Result<(), NativeMountError>
    {
        use crate::native_host::NativeHostWork as _;
        let work = &mut &*native;
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
        let (cleanup_name, birth_cleanup) = {
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
            (Some(prepared), Some(credit))
        };
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
        let admitted = Self { file: Some(file), lock_path: PathBuf::new(),
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

    /// Administrative LastDrop closes only the original held fence.
    fn close_administrative_fence(&mut self, original: &mut crate::NativeWorkReservation) {
        if self.file.is_some() {
            original.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(self.file.take());
        }
    }

    fn release_cleanup(
        &mut self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        if self.file.is_none() { return Ok(()); }
        let invalid = || NativeMountError::Materialization(crate::NativeTreeMaterializationError::InvalidLayout);
        let directory = self.parent_directory.as_ref().ok_or_else(invalid)?;
        let registry = self.registry_file.as_ref().ok_or_else(invalid)?;
        let name = self.cleanup_name.as_ref().ok_or_else(invalid)?;
        let credit = self.birth_cleanup.as_mut().ok_or_else(invalid)?;
        let loan = credit.loan(wait);
        let mut work = NativeFenceReleaseWork { loan: &loan, pending: None };
        // A contended registry is a real pending control. Do not spend an
        // unbounded sequence of new lock attempts after cancellation.
        lock_mount_file_with_work(registry, false, &mut work)?;
        match crate::native_host::remove_prepared_ascii_leaf_with_work(directory, name, &mut work) {
            Ok(()) => {}
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.file.take();
        Ok(())
    }

    fn release<W: crate::native_host::NativeHostWork>(
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
        let lock_name = match self.fence_name.as_ref() {
            Some(name) => Path::new(std::str::from_utf8(name).expect("literal prefix, hex and suffix are UTF-8")),
            None => Path::new(self.lock_path.file_name().ok_or(NativeMountError::InvalidDestination)?),
        };
        let registry = crate::native_host::open_leaf_with_work(
            directory, Path::new(NATIVE_REGISTRY_NAME),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        match crate::native_host::remove_leaf_with_work(directory, lock_name, work) {
            Ok(()) => {}
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        self.file.take();
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

impl Drop for MountDestinationGuard {
    fn drop(&mut self) {
        // A finite fence is retired only by its original invocation. Dropping
        // it must not invent an unbounded cleanup engine after denied work.
        if self.parent_directory.is_none() && self.file.is_some() {
            let cancellation = crate::CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
            let _ = self.release(&mut work);
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

fn start_driver_into<F>(
    request: &NativeMountRequest, source: Arc<dyn MountFilesystem>, work: &NativeMaterializationWork,
    out: &mut Option<DriverSession>, retirement: &mut Option<NativeMountRetirement>, prepare: F,
) -> Result<(), NativeMountError>
where F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError> {
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        #[cfg(target_os = "linux")]
        let (start, wrap) = (fuse::FuseSession::start_into, DriverSession::Fuse);
        #[cfg(target_os = "macos")]
        let (start, wrap) = (darwin_mount::DarwinMountSession::start_into, DriverSession::DarwinMount);
        #[cfg(windows)]
        let (start, wrap) = (projfs::ProjFsSession::start, DriverSession::ProjFs);
        let mut driver = None;
        let result = start(request, source, work, &mut driver, retirement, prepare);
        *out = driver.map(wrap);
        result
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (&request, &source, &work, &out, &retirement, &prepare);
        Err(NativeMountError::UnsupportedTarget.into())
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
        ByteRange, CancellationToken, Fs, VolumeId, WatchBatch, WatchChange, WorkBudget,
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
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let cancellation = CancellationToken::new();
        let checkout = runtime.block_on(async {
            let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
            private_checkout(&volume, &cancellation).await
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

    pub(super) async fn memory_volume(
        id: VolumeId, config: VolumeConfig, cancellation: &CancellationToken,
    ) -> Result<crate::Volume<impl crate::AsyncAuthorityStore + use<>, impl crate::AsyncObjectStore + use<>>, Box<dyn std::error::Error>> {
        Ok(Fs::memory().create_volume_with_id(id, config, WorkBudget::UNBOUNDED, cancellation).await?.value)
    }

    pub(super) async fn private_checkout<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
        volume: &crate::Volume<A, O>, cancellation: &CancellationToken,
    ) -> Result<crate::Checkout<A, O>, Box<dyn std::error::Error>> {
        Ok(volume.checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED, cancellation,
        ).await?.value)
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
        let state = owner.cleanup_state();
        assert!(!state.destination_fence_retained && !state.controls_pending);
        let thread = owner.worker.take().expect("same original worker");
        drop(owner);
        await_recovery_worker(thread)?;
        assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
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
        let state = recovery.cleanup_state();
        assert!(!state.destination_fence_retained && !state.controls_pending);
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
                if let Some(status) = child.try_wait(None)? {
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
            if let Some(status) = child.try_wait(None)? {
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
        let meter = NativeMaterializationWork::from_meter(Arc::clone(&original));
        let mut guard = None;
        MountDestinationGuard::acquire_with_work(&destination, false, &meter, &mut guard, false)?;
        let mut guard = guard.expect("actual fence was admitted");
        assert!(Arc::ptr_eq(meter.native_read_meter(), &original));
        assert!(guard.parent_directory.is_some());
        assert!(guard.file.is_some());
        let mut competing_guard = None;
        assert!(matches!(MountDestinationGuard::acquire_with_work(&destination, false, &meter, &mut competing_guard, false),
            Err(NativeMountError::DestinationBusy)));
        assert!(competing_guard.is_none(), "a competing lock never becomes an owned cleanup target");
        {
            let mut work = original.lock();
            work.budget.backend_write_operations = work.work.backend_write_operations;
        }
        let before = original.lock().work;
        let error = guard.release(&mut &meter).expect_err("original write allowance denies cleanup");
        assert!(matches!(error, NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert!(guard.file.is_some());
        assert_eq!(original.lock().work.backend_write_operations, before.backend_write_operations);
        original.lock().budget.backend_write_operations = u64::MAX;
        guard.release(&mut &meter)?;
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
        MountDestinationGuard::acquire_with_work(
            &destination, false, &work, &mut guard, false)?;
        let mut guard = guard.expect("actual fence was admitted");
        let before = work.receipt();
        cancelled.cancel();
        let refused_wait = crate::CancellationToken::new();
        refused_wait.cancel();
        assert!(guard.release_cleanup(Some(&refused_wait)).is_err());
        assert!(guard.file.is_some(), "denied wait preserves the actual fence");
        assert_eq!(work.receipt(), before, "no accepted operation starts on a denied wait");
        guard.release_cleanup(Some(&crate::CancellationToken::new()))?;
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
        let error = guard.release(&mut work).expect_err("native file deletion cannot remove a directory");
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
        let error = guard.release(&mut work).expect_err("original cancellation must win before native work");
        assert!(matches!(error, NativeMountError::Demand(crate::DemandError::Cancelled)));
        assert_eq!(work.work, crate::WorkCounters::default());
        assert!(guard.file.is_some());
        assert!(guard.lock_path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn capture_host_hard_links_preserves_shared_file_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let config = VolumeConfig { limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) };
        let cancellation = CancellationToken::new();
        let volume = memory_volume(VolumeId::from_bytes([92; 16]), config, &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
        let host = tempfile::tempdir()?;
        std::fs::write(host.path().join("a"), b"shared")?;
        std::fs::hard_link(host.path().join("a"), host.path().join("b"))?;
        let path = |name: &str| portable_path(limits, &[name.as_bytes()]);
        let options = CaptureOptions {
            expected_root_identity: capture_root_identity(host.path())?,
            source_root: host.path().to_path_buf(),
            maximum_paths: 2,
            maximum_extent_spans: 16,
        };
        let paths = [path("a")?, path("b")?];
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        crate::native_capture::capture_paths_batched(
            &mut checkout,
            &paths,
            &options,
            1,
            &cancellation,
            None,
            &original,
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
        for name in ["z", "0"] {
            std::fs::hard_link(host.path().join("a"), host.path().join(name))?;
            let additional = path(name)?;
            capture_paths(
                &mut checkout, &[paths[0].clone(), additional.clone()], &options,
                WorkBudget::UNBOUNDED, &cancellation,
            ).await?;
            let additional_id = checkout
                .lookup_no_follow(&additional, WorkBudget::UNBOUNDED, &cancellation)
                .await?.value.record.ok_or("additional link missing")?.file_id;
            assert_eq!(additional_id, first.file_id);
        }
        std::fs::hard_link(host.path().join("a"), host.path().join("c"))?;
        let third = path("c")?;
        let watch = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: 1,
                first_sequence: 1,
                next_sequence: 2,
                changes: vec![WatchChange::Created(third.clone())],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 1, ..options.clone() },
            &crate::CapturePolicy::default(),
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
            &CaptureOptions { maximum_paths: 1, ..options.clone() },
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
                epoch: 1,
                first_sequence: 2,
                next_sequence: 3,
                changes: vec![WatchChange::Created(existing.clone())],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 1, ..options },
            &crate::CapturePolicy::default(),
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
        let volume = memory_volume(VolumeId::from_bytes([94; 16]), VolumeConfig::portable(Lifecycle::Ephemeral), &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
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
            &crate::CapturePolicy::default(),
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
        let config = VolumeConfig { limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) };
        let cancellation = CancellationToken::new();
        let volume = memory_volume(VolumeId::from_bytes([91; 16]), config, &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
        let file = portable_path(limits, &[b"sparse.bin"])?;
        checkout
            .create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (Bytes::from_static(b"head")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        checkout
            .change_content(file.clone(), crate::ContentChange::Resize { logical_bytes: 1024 * 1024 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        checkout
            .change_content(file.clone(), crate::ContentChange::Write { offset: 1024 * 1024 - 4, bytes: (Bytes::from_static(b"tail")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)
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

        let capture_options = CaptureOptions {
            expected_root_identity: capture_root_identity(&destination)?,
            source_root: destination.clone(),
            maximum_paths: 1,
            maximum_extent_spans: 16,
        };
        let sparse_capture = capture_paths(
            &mut checkout,
            std::slice::from_ref(&file),
            &capture_options,
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
            &capture_options,
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
        let renamed = portable_path(limits, &[b"renamed.bin"])?;
        mutate_as_a_user_would(|| std::fs::rename(&host_file, destination.join("renamed.bin")))
            .map_err(|error| std::io::Error::other(format!("host rename: {error}")))?;
        let watch = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: 7,
                first_sequence: 11,
                next_sequence: 12,
                changes: vec![WatchChange::Renamed {
                    from: file.clone(),
                    to: renamed.clone(),
                }],
                original: Default::default(),
            },
            &capture_options,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(watch.value.next_sequence, 12);
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

        let moved = portable_path(limits, &[b"moved.bin"])?;
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
                epoch: 7,
                first_sequence: 12,
                next_sequence: 14,
                changes: vec![
                    WatchChange::Renamed {
                        from: renamed.clone(),
                        to: moved.clone(),
                    },
                    WatchChange::Created(renamed.clone()),
                ],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 2, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
                epoch: 7,
                first_sequence: 14,
                next_sequence: 15,
                changes: vec![WatchChange::Created(renamed.clone())],
                original: Default::default(),
            },
            &capture_options,
            &crate::CapturePolicy::default(),
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
                epoch: 7,
                first_sequence: 15,
                next_sequence: 17,
                changes: vec![
                    WatchChange::Created(renamed.clone()),
                    WatchChange::Modified(renamed.clone()),
                ],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 2, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
            &CaptureOptions { maximum_paths: 16, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
            &CaptureOptions { maximum_paths: 16, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
                epoch: 8,
                first_sequence: 0,
                next_sequence: 1,
                changes: vec![WatchChange::Renamed {
                    from: missing,
                    to: arrived.clone(),
                }],
                original: Default::default(),
            },
            &capture_options,
            &crate::CapturePolicy::default(),
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
                epoch: 8,
                first_sequence: 1,
                next_sequence: 3,
                changes: vec![
                    WatchChange::Created(temporary_name.clone()),
                    WatchChange::Renamed {
                        from: temporary_name,
                        to: arrived.clone(),
                    },
                ],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 2, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
                epoch: 8,
                first_sequence: 3,
                next_sequence: 5,
                changes: vec![
                    WatchChange::Created(created_and_modified.clone()),
                    WatchChange::Modified(created_and_modified.clone()),
                ],
                original: Default::default(),
            },
            &CaptureOptions { maximum_paths: 2, ..capture_options.clone() },
            &crate::CapturePolicy::default(),
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
        let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
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
            &crate::CapturePolicy::default(),
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
                epoch: 1,
                first_sequence: 0,
                next_sequence: 1,
                changes: vec![WatchChange::Created(parent.clone())],
                original: Default::default(),
            },
            &options,
            &crate::CapturePolicy::default(),
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
                epoch: 1,
                first_sequence: 1,
                next_sequence: 3,
                changes: vec![
                    WatchChange::Removed(parent.clone()),
                    WatchChange::Removed(child.clone()),
                ],
                original: Default::default(),
            },
            &options,
            &crate::CapturePolicy::default(),
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

    // Stale creation hints must preserve existing directories and their children.
    #[tokio::test]
    async fn stale_created_hints_for_existing_directory_capture_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let limits = config.limits;
        let cancellation = CancellationToken::new();
        let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
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
            &crate::CapturePolicy::default(),
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
                epoch: 1,
                first_sequence: 0,
                next_sequence: 6,
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
            &crate::CapturePolicy::default(),
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
                        length: (expected.len() as u64),
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
        let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
        let mut checkout = private_checkout(&volume, &cancellation).await?;
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
                epoch: 1,
                first_sequence: 0,
                next_sequence: 2,
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
            &crate::CapturePolicy::default(),
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
        let config = VolumeConfig { profile: FilesystemProfile::Posix, limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) };
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
        let mut source_checkout = private_checkout(&source, &cancellation).await?;
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
            .create(crate::AuthoredMutation::CreateEmptySpecial { path: fifo.clone(), kind: crate::kernel::FileKind::Fifo, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        source_checkout
            .create(crate::AuthoredMutation::CreateEmptySpecial { path: socket.clone(), kind: crate::kernel::FileKind::Socket, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
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
        let mut captured_checkout = private_checkout(&captured, &cancellation).await?;
        let receipt = capture_baseline(
            &mut captured_checkout,
            &CaptureOptions {
                expected_root_identity: capture_root_identity(&destination)?,
                source_root: destination,
                maximum_paths: 2,
                maximum_extent_spans: 16,
            },
            &crate::CapturePolicy::default(),
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
