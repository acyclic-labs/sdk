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
#[cfg(any(target_os = "linux", target_os = "macos"))]
use crate::kernel::MetadataField;
use crate::{FileId, MountId, VolumeId};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::time::SystemTime;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};
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
    CheckoutMountSource, SharedCheckout, SharedCheckoutGuard, SharedCheckoutState,
    initialize_native_mount_services,
};

mod view_gate;
pub(crate) use view_gate::ViewGateReadAdmissions;

mod view_ledger;
pub(crate) use view_ledger::ViewObserverAdmissions;
pub use view_ledger::{ViewObserver, ViewOrigin, ViewStamp};

mod lazy;
pub use lazy::LazyMountSource;

mod customer;
pub use customer::{
    LazyMount, Mount, MountLifecycleError, MountOptions, MountPublication, NativeWorkingSet,
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
};

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
    match error {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) => libc::EOPNOTSUPP,
        MountSourceError::Engine(_) | MountSourceError::Work(_) => libc::EIO,
        MountSourceError::Stale | MountSourceError::Cancelled => libc::ESTALE,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
fn source_error(error: MountSourceError) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
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
/// bytes.
pub type ContentSink<'a> = dyn FnMut(u64, Bytes) -> Result<(), MountSourceError> + 'a;

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
    /// Reads at most `maximum_bytes`, clipping to EOF in the same coherent view.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`Self::read_range`].
    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError>;
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
    /// The content pin a [`MountFilesystem::lookup_pinned`] of this entry
    /// issues in the same view, so a projection may answer that lookup from
    /// the listing.
    pub pin: Option<MountContentPin>,
}

/// One independently resumable directory-handle page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountDirectoryPage {
    /// Ordered entries after the supplied opaque cursor.
    pub entries: Vec<MountDirectoryEntry>,
    /// Opaque continuation owned by this open directory handle.
    pub next_cursor: Option<Vec<u8>>,
}

impl MountDirectoryPage {
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
/// Components contain raw Unix name bytes on Linux FUSE/macOS NFS and little-endian
/// UTF-16 code units on `ProjFS`. The checkout adapter converts them into the
/// volume's declared logical-name representation without lossy text routing.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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

/// Errors returned by the canonical checkout callback bridge.
#[derive(Clone, Debug, Error, strum::IntoStaticStr)]
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
    /// An explicitly admitted native operation was cancelled.
    #[error("native operation was cancelled")]
    Cancelled,
    /// Canonical publication exceeded its exact work allowance.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
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
    pub(crate) work: &'a mut crate::demand::DemandWork,
}

/// Physical ownership of known native-driver payloads admitted by one original
/// lifecycle ledger. Clones are aliases, not new allocations or authorities.
#[derive(Clone)]
pub(crate) struct NativeMountAllocationCustody {
    live: Arc<std::sync::atomic::AtomicU64>,
}

struct NativeMountAllocationAdmission {
    live: std::sync::Weak<std::sync::atomic::AtomicU64>,
    adopted: u64,
}

/// Original invocation's weak retirement proofs; fresh invocations do not
/// acquire permission to release borrowed driver allocations.
#[derive(Default)]
pub(crate) struct NativeMountAllocationAdmissions {
    first: Option<NativeMountAllocationAdmission>,
    more: Vec<NativeMountAllocationAdmission>,
}

impl NativeMountAllocationAdmissions {
    pub(crate) fn freed_bytes(&self) -> u64 {
        self.first.iter().chain(self.more.iter()).try_fold(0_u64, |freed, entry| {
            let actual = entry.live.upgrade().map_or_else(|| {
                // An expired strong owner still has this original weak
                // allocation. Reconciliation drops it before releasing metadata.
                if entry.adopted == 0 { 0 }
                else { std::mem::size_of::<std::sync::atomic::AtomicU64>() as u64 }
            }, |live| live.load(std::sync::atomic::Ordering::Acquire));
            freed.checked_add(entry.adopted.checked_sub(actual)?)
        }).expect("native physical retirement cannot exceed original admitted ownership")
    }

    pub(crate) fn reconcile(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            let actual = match entry.live.upgrade() {
                Some(live) => live.load(std::sync::atomic::Ordering::Acquire),
                None => {
                    // This ledger owns the sole weak registration. Free the
                    // actual allocation before releasing its metadata claim.
                    drop(std::mem::take(&mut entry.live));
                    0
                }
            };
            let retired = entry.adopted.checked_sub(actual).ok_or(crate::WorkError::Overflow)?;
            allocations.release(retired).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            entry.adopted = actual;
        }
        Ok(())
    }

    fn reserve_slot(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        counters: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<(), NativeMountError> {
        if self.first.is_none() || self.more.len() < self.more.capacity() { return Ok(()); }
        let width = std::mem::size_of::<NativeMountAllocationAdmission>();
        let capacity = self.more.capacity().max(1).checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?;
        let bytes = capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        let old_bytes = self.more.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: self.more.len().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        counters.admit(&copied, &budget)?;
        allocations.claim_bytes(bytes, 1, counters, budget)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        let mut vector = Vec::new();
        if let Err(error) = vector.try_reserve_exact(capacity) {
            allocations.release(bytes).map_err(|failure| failure.into_work_or(crate::WorkError::Overflow))?;
            return Err(NativeMountError::Allocation(error));
        }
        vector.append(&mut self.more);
        self.more = vector;
        allocations.release(old_bytes).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        counters.charge(&copied, &budget)?;
        Ok(())
    }
}

impl NativeMountAllocationCustody {
    pub(crate) fn new(work: &mut crate::demand::DemandWork) -> Result<Self, NativeMountError> {
        work.check()?;
        let (admissions, allocations, counters, budget) = work.native_mount_admission_parts()?;
        admissions.reserve_slot(allocations, counters, budget)?;
        let copied = crate::WorkCounters {
            bytes_copied: if admissions.first.is_some() {
                std::mem::size_of::<NativeMountAllocationAdmission>() as u64
            } else { 0 },
            ..crate::WorkCounters::UNCHARGED
        };
        counters.admit(&copied, &budget)?;
        let bytes = std::mem::size_of::<std::sync::atomic::AtomicU64>() as u64;
        allocations.claim_bytes(bytes, 1, counters, budget)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        let live = Arc::new(std::sync::atomic::AtomicU64::new(bytes));
        let admission = NativeMountAllocationAdmission { live: Arc::downgrade(&live), adopted: bytes };
        if admissions.first.is_none() { admissions.first = Some(admission); }
        else { admissions.more.push(admission); }
        counters.charge(&copied, &budget)?;
        Ok(Self { live })
    }

    /// Admits a concrete allocation before entering its actual leaf.
    pub(crate) fn claim(
        &self, bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<(), NativeMountError> {
        self.retain(bytes, work, true)
    }

    /// Transfers a producer's already-live known claim without adopting or
    /// allocating it again. Call only after moving that exact owned payload.
    pub(crate) fn retain_admitted(
        &self, bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<(), NativeMountError> {
        self.retain(bytes, work, false)
    }

    fn retain(
        &self, bytes: u64, work: &mut crate::demand::DemandWork, allocate: bool,
    ) -> Result<(), NativeMountError> {
        work.check()?;
        let (admissions, allocations, counters, budget) = work.native_mount_admission_parts()?;
        let entry = admissions.first.iter_mut().chain(admissions.more.iter_mut())
            .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
            .ok_or(crate::DemandError::InvalidRequest)?;
        let adopted = entry.adopted.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        self.live.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if allocate {
            allocations.claim_bytes(bytes, u64::from(bytes != 0), counters, budget)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        }
        // Concurrent retirement can only decrease the admitted value.
        self.live.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        entry.adopted = adopted;
        Ok(())
    }

    /// Marks only physically dropped payloads. The original ledger, if still
    /// retained, performs the release during its next reconciliation.
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::WorkError> {
        let metadata = std::mem::size_of::<std::sync::atomic::AtomicU64>() as u64;
        self.live.fetch_update(
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
            |live| live.checked_sub(bytes).filter(|live| *live >= metadata),
        ).map(|_| ()).map_err(|_| crate::WorkError::Overflow)
    }
}

impl NativeMountWork<'_> {
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
    pub fn remaining(&self) -> Result<crate::WorkBudget, crate::WorkError> {
        self.work.work.remaining(self.work.budget)
    }
}

/// Synchronous callback surface implemented by an embedded checkout adapter.
///
/// Native kernels invoke callbacks on foreign threads, so this intentionally
/// exposes a blocking boundary. Implementations may enter an async runtime but
/// must enforce finite work and timeout limits internally.
pub trait MountFilesystem: Send + Sync + 'static {
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
        piece: u32, sink: &mut ContentSink<'_>, work: &mut NativeMountWork<'_>,
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
    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError>;

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
    ) -> Result<Bytes, MountSourceError>;

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
    /// Original source failure, without allocating a replacement diagnostic.
    #[error("native mount source failed: {0}")]
    Source(#[source] MountSourceError),
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
    /// The driver has actual successful namespace-detach evidence.
    /// This does not imply callbacks, pending controls, or fence cleanup ended.
    pub namespace_detach_confirmed: bool,
    /// The last measured cleanup failed; retained custody requires explicit retry.
    pub measured_cleanup_failed: bool,
    /// Both the driver and its destination fence were actually released.
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
}

struct NativeRecoveryState {
    destination: PathBuf,
    fence_identity: crate::NativeRootIdentity,
    #[cfg(windows)]
    destination_identity: Option<crate::NativeRootIdentity>,
    guard: Option<MountDestinationGuard>,
    control: NativeRecoveryControl,
    driver_step_complete: bool,
    destination_removed: bool,
    measured_cleanup_failed: bool,
}

/// Original recovery control and destination fence, acquired once before action.
///
/// Retain this owner on failure and retry it without acquiring another lease.
/// No registry, journal, generated mount ID, or independent unmount engine is
/// created. Unfinished Drop preserves the complete original heap owner until
/// process exit rather than issuing unbudgeted I/O or publishing a reusable root.
pub struct NativeMountRecoveryOwner {
    state: Option<Box<NativeRecoveryState>>,
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
    /// Its original fence identity and known owned path/state are retained.
    /// Initialize native I/O services before a measured macOS action.
    ///
    /// # Errors
    ///
    /// Rejects a live owner or preserves the original acquisition failure.
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let guard = MountDestinationGuard::acquire_for_recovery(destination)?;
        let cancellation = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let destination = crate::native_host::clone_path_measured(destination, &mut work)?;
        let (fence_identity, _) = crate::native_host::file_identity_and_length_with_work(
            guard.file.as_ref().ok_or(NativeMountError::InvalidDestination)?, &mut work,
        )?;
        #[cfg(windows)]
        let destination_identity = match crate::native_host::root_path_identity_measured(&destination, &mut work) {
            Ok(identity) => Some(identity),
            Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let control = NativeRecoveryControl::default();
        #[cfg(windows)]
        let control = {
            let mut control = control;
            if let Some(identity) = destination_identity { control.bind_root_identity(identity); }
            control
        };
        work.allocate(std::mem::size_of::<NativeRecoveryState>() as u64)?;
        Ok(Self { state: Some(Box::new(NativeRecoveryState {
            destination, fence_identity,
            #[cfg(windows)]
            destination_identity,
            guard: Some(guard), control, driver_step_complete: false,
            destination_removed: false, measured_cleanup_failed: false,
        })) })
    }

    /// Borrows the real original identity, evidence, and retained resources.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountRecoveryCleanupState<'_> {
        let state = self.state.as_ref().expect("live recovery owner");
        NativeMountRecoveryCleanupState {
            destination: &state.destination,
            fence_identity: state.fence_identity,
            destination_fence_retained: state.guard.is_some(),
            controls_pending: state.control.controls_pending(),
            namespace_detach_confirmed: state.control.namespace_detach_confirmed(),
            destination_removed: state.destination_removed,
            measured_cleanup_failed: state.measured_cleanup_failed,
            cleanup_complete: state.guard.is_none() && !state.control.controls_pending(),
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
        if self.state.as_ref().expect("live recovery owner").guard.is_none() { return Ok(false); }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.state.as_mut().expect("live recovery owner").measured_cleanup_failed = true;
            return Err(error.into());
        }
        self.measured_action(remove_destination, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Recovers and removes only an empty disposable destination on finite work.
    ///
    /// # Errors
    ///
    /// Preserves actual work and original control/fence custody. Admission never
    /// claims to abort or roll back an already-entered cooperative native call.
    pub fn recover_measured(
        &mut self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, NativeMountError> {
        self.measured_action(true, budget, cancellation)
    }

    /// Detaches on finite work without removing the destination.
    ///
    /// # Errors
    ///
    /// Retains all actual pending controls and the same original fence.
    pub fn detach_measured(
        &mut self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, NativeMountError> {
        self.measured_action(false, budget, cancellation)
    }

    fn measured_action(
        &mut self, remove_destination: bool, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, NativeMountError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        match self.finish_with_work(remove_destination, &mut work) {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.work }),
            Err(error) => Err(crate::OperationFailure::new(error, work.work)),
        }
    }

    fn finish_with_work(
        &mut self, remove_destination: bool, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, NativeMountError> {
        let state = self.state.as_mut().expect("live recovery owner");
        let result = (|| {
            if state.guard.is_none() { return Ok(false); }
            if !state.driver_step_complete {
                recover_driver_with_work(&mut state.control, &state.destination, remove_destination, work)?;
                if state.control.controls_pending() { return Err(NativeMountError::ControlPending); }
                state.driver_step_complete = true;
            }
            if remove_destination && !state.destination_removed {
                #[cfg(windows)]
                projfs::remove_empty_recovery_destination_measured(&state.destination, state.destination_identity, work)?;
                #[cfg(not(windows))]
                remove_empty_recovery_destination(&state.destination, work)?;
                state.destination_removed = true;
            }
            state.guard.as_mut().expect("original retained fence").release_measured(work)?;
            state.guard.take();
            Ok(true)
        })();
        state.measured_cleanup_failed = result.is_err();
        result
    }
}

impl Drop for NativeMountRecoveryOwner {
    fn drop(&mut self) {
        if self.state.as_ref().is_some_and(|state| state.guard.is_some() || state.control.controls_pending()) {
            // Preserve the complete original heap owner, not lost inline handle
            // values or a newly synthesized recovery capability.
            if let Some(state) = self.state.take() { std::mem::forget(state); }
        }
    }
}

fn recover_driver_with_work(
    control: &mut NativeRecoveryControl, destination: &Path,
    disposable: bool, work: &mut crate::demand::DemandWork,
) -> Result<(), NativeMountError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    { let _ = disposable; control.recover_measured(destination, work) }
    #[cfg(target_os = "windows")]
    { control.recover_measured(destination, disposable, work) }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    { let _ = (control, destination, disposable, work); Err(NativeMountError::UnsupportedTarget) }
}

#[cfg(not(windows))]
fn remove_empty_recovery_destination(
    destination: &Path, work: &mut crate::demand::DemandWork,
) -> Result<(), NativeMountError> {
    let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
    let name = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    let result = crate::native_host::open_root_directory_with_work(parent, work)
        .and_then(|directory| crate::native_host::remove_empty_directory_held_with_work(
            &directory, Path::new(name), work,
        ));
    match result {
        Ok(()) => Ok(()),
        Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Process-owned mounted namespace. Ordinary Drop attempts one stop; after a
/// measured failure it preserves live custody without an unbudgeted retry.
pub struct NativeMountSession {
    mount_id: MountId,
    destination: PathBuf,
    driver: Option<DriverSession>,
    destination_guard: Option<MountDestinationGuard>,
    preserve_on_drop: bool,
    measured_cleanup_failed: bool,
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
        NativeMountCleanupState {
            mount_id: self.mount_id,
            destination: &self.destination,
            driver_retained: self.driver.is_some(),
            destination_fence_retained: self.destination_guard.is_some(),
            namespace_detach_confirmed: self.driver.as_ref()
                .is_none_or(driver_namespace_detach_confirmed),
            measured_cleanup_failed: self.measured_cleanup_failed,
            cleanup_complete: self.driver.is_none() && self.destination_guard.is_none(),
        }
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
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.flush_callbacks_measured(work.work),
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
        match self.driver.as_ref() {
            #[cfg(target_os = "linux")]
            Some(DriverSession::Fuse(session)) => session.revalidate_measured(work.work),
            #[cfg(target_os = "macos")]
            Some(DriverSession::DarwinMount(session)) => session.revalidate_measured(work.work),
            #[cfg(target_os = "windows")]
            Some(DriverSession::ProjFs(session)) => session.revalidate_measured(work.work),
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
        if self.driver.is_none() && self.destination_guard.is_none() {
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
            .map_err(|failure| failure.error)
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, NativeMountError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        match self.stop_with_work(&mut NativeMountWork { work: &mut work }) {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.work }),
            Err(error) => Err(crate::OperationFailure::new(error, work.work)),
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
        let result = stop_owned_session_with_work(
            &mut self.driver,
            &mut self.destination_guard,
            work.work,
            stop_driver_measured,
            MountDestinationGuard::release_measured,
        );
        self.preserve_on_drop = result.is_err();
        self.measured_cleanup_failed = result.is_err();
        result
    }
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
    if stop_owned_session(driver, destination_guard, stop, release).is_err() {
        retain_owned_session(driver, destination_guard);
    }
}

fn retain_owned_session<D, G>(driver: &mut Option<D>, destination_guard: &mut Option<G>) {
    // Final loss of the Rust facade cannot make an unresolved kernel namespace
    // reusable. Preserve the actual original owners until process exit, exactly
    // as ordinary failed Drop does; explicit retry requires retaining the facade.
    if let Some(active) = driver.take() {
        std::mem::forget(active);
    }
    if let Some(guard) = destination_guard.take() {
        std::mem::forget(guard);
    }
}

impl Drop for NativeMountSession {
    fn drop(&mut self) {
        if !self.preserve_on_drop {
            let _ = self.stop();
        }
        if self.preserve_on_drop {
            retain_owned_session(&mut self.driver, &mut self.destination_guard);
        }
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
        preserve_on_drop: false,
        measured_cleanup_failed: false,
    })
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
            start_owned_session_over_existing(&request.destination, || {
                start_driver(&request, source)
            })?;
        Ok(NativeMountSession {
            mount_id: request.mount_id,
            destination: request.destination,
            driver: Some(driver),
            destination_guard: Some(destination_guard),
            preserve_on_drop: false,
            measured_cleanup_failed: false,
        })
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


fn start_owned_session<D>(
    destination: &Path,
    start: impl FnOnce() -> Result<D, DriverStartFailure>,
) -> Result<(D, MountDestinationGuard), NativeMountError> {
    let destination_guard = admit_destination(destination)?;
    finish_started_session(destination_guard, start())
}

fn finish_started_session<D>(
    destination_guard: MountDestinationGuard,
    result: Result<D, DriverStartFailure>,
) -> Result<(D, MountDestinationGuard), NativeMountError> {
    match result {
        Ok(driver) => Ok((driver, destination_guard)),
        Err(failure) => {
            if failure.preserve_destination_fence {
                // Startup left an unknown kernel namespace behind. Keep the
                // process-owned lock live until process exit rather than
                // publishing the destination as reusable.
                std::mem::forget(destination_guard);
            }
            Err(failure.error)
        }
    }
}

/// Same as [`start_owned_session`], but admits an existing non-empty
/// destination (see [`admit_destination_over_existing`]).
#[cfg(not(windows))]
fn start_owned_session_over_existing<D>(
    destination: &Path,
    start: impl FnOnce() -> Result<D, DriverStartFailure>,
) -> Result<(D, MountDestinationGuard), NativeMountError> {
    let destination_guard = admit_destination_over_existing(destination)?;
    finish_started_session(destination_guard, start())
}

fn admit_destination(destination: &Path) -> Result<MountDestinationGuard, NativeMountError> {
    let guard = MountDestinationGuard::acquire(destination)?;
    validate_destination(destination)?;
    Ok(guard)
}

/// Same process-fence as [`admit_destination`], but admits an existing
/// non-empty directory instead of requiring emptiness.
#[cfg(not(windows))]
fn admit_destination_over_existing(
    destination: &Path,
) -> Result<MountDestinationGuard, NativeMountError> {
    let guard = MountDestinationGuard::acquire(destination)?;
    validate_directory(destination)?;
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
}

impl MountDestinationGuard {
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    fn acquire_with_work(
        destination: &Path, allow_nonempty: bool, work: &mut crate::demand::DemandWork,
    ) -> Result<Self, NativeMountError> {
        let directory = crate::native_host::open_root_directory_with_work(destination, work)?;
        if !allow_nonempty {
            let (mut reader, allocation) = crate::native_host::read_dir_held_with_work(&directory, work)?;
            let result = reader.next_with_work(work);
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
        let canonical = crate::native_host::canonical_directory_path_held_with_work(directory, work)?;
        #[cfg(windows)]
        let canonical = crate::native_host::canonical_directory_wide_with_work(&directory, work)?;
        #[cfg(unix)]
        let canonical_bytes = canonical.as_os_str().len() as u64;
        #[cfg(windows)]
        let canonical_bytes = canonical.len().checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let hashed = crate::WorkCounters {
            bytes_hashed: canonical_bytes.checked_add(b"acyclic-fs/native-mount-destination/v1\0".len() as u64)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        };
        let result = (|| {
            work.check()?;
            work.work.admit(&hashed, &work.budget)?;
            let mut identity = blake3::Hasher::new();
            identity.update(b"acyclic-fs/native-mount-destination/v1\0");
            #[cfg(unix)]
            update_path_identity(&mut identity, &canonical);
            #[cfg(windows)]
            for unit in &canonical { identity.update(&unit.to_le_bytes()); }
            work.record(hashed)?;
            let hex = identity.finalize().to_hex();
            let mut name = [0u8; NATIVE_FENCE_NAME_BYTES];
            name[..NATIVE_FENCE_PREFIX.len()].copy_from_slice(NATIVE_FENCE_PREFIX.as_bytes());
            name[NATIVE_FENCE_PREFIX.len()..NATIVE_FENCE_PREFIX.len() + 64].copy_from_slice(hex.as_bytes());
            name[NATIVE_FENCE_PREFIX.len() + 64..].copy_from_slice(NATIVE_FENCE_SUFFIX.as_bytes());
            Ok::<_, NativeMountError>(name)
        })();
        #[cfg(unix)]
        let canonical_allocation = canonical.capacity() as u64;
        #[cfg(windows)]
        let canonical_allocation = canonical.capacity().checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        drop(canonical);
        work.release(canonical_allocation)?;
        let name = result?;
        let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
        let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        let parent_directory = crate::native_host::open_root_directory_with_work(parent, work)?;
        let registry = crate::native_host::open_leaf_with_work(
            &parent_directory, Path::new(NATIVE_REGISTRY_NAME),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        let file = crate::native_host::open_leaf_with_work(
            &parent_directory, Path::new(std::str::from_utf8(&name).expect("literal prefix, hex and suffix are UTF-8")),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        )?;
        lock_mount_file_with_work(&file, false, work)?;
        drop(registry);
        Ok(Self { file: Some(file), lock_path: PathBuf::new(), registry_path: PathBuf::new(),
            parent_directory: Some(parent_directory), fence_name: Some(name) })
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
                Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        })?;
        drop(registry);
        Ok(())
    }
}

fn lock_mount_file_with_work(
    file: &File, blocking: bool, work: &mut crate::demand::DemandWork,
) -> Result<(), NativeMountError> {
    loop {
        let delta = crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED };
        work.check()?;
        work.work.admit(&delta, &work.budget)?;
        let result = fs2::FileExt::try_lock_exclusive(file);
        work.record(delta)?;
        match result {
            Ok(()) => return Ok(()),
            Err(error) if lock_is_contended(&error) && blocking => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) if lock_is_contended(&error) => return Err(NativeMountError::DestinationBusy),
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
    validate_directory(destination)?;
    let mut entries =
        std::fs::read_dir(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if entries.next().is_some() {
        return Err(NativeMountError::InvalidDestination);
    }
    Ok(())
}

/// The directory half of [`validate_destination`], without the
/// empty-directory requirement — shared with
/// [`mount_native_over_existing`]'s admission path.
fn validate_directory(destination: &Path) -> Result<(), NativeMountError> {
    let metadata =
        std::fs::symlink_metadata(destination).map_err(|_| NativeMountError::InvalidDestination)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, DriverStartFailure> {
    fuse::FuseSession::start(request, source)
        .map(DriverSession::Fuse)
        .map_err(Into::into)
}

#[cfg(target_os = "macos")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, DriverStartFailure> {
    darwin_mount::DarwinMountSession::start(request, source).map(DriverSession::DarwinMount)
}

#[cfg(target_os = "windows")]
fn start_driver(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, DriverStartFailure> {
    projfs::ProjFsSession::start(request, source).map(DriverSession::ProjFs)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_driver(
    _request: &NativeMountRequest,
    _source: Arc<dyn MountFilesystem>,
) -> Result<DriverSession, DriverStartFailure> {
    Err(NativeMountError::UnsupportedTarget.into())
}

fn stop_driver_measured(
    driver: &mut DriverSession,
    work: &mut crate::demand::DemandWork,
) -> Result<(), NativeMountError> {
    match driver {
        #[cfg(target_os = "linux")]
        DriverSession::Fuse(session) => session.stop_measured(work),
        #[cfg(target_os = "macos")]
        DriverSession::DarwinMount(session) => session.stop_measured(work),
        #[cfg(target_os = "windows")]
        DriverSession::ProjFs(session) => session.stop_measured(work),
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        DriverSession::Unsupported => {
            let _ = work;
            Err(NativeMountError::UnsupportedTarget)
        }
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
    fn failed_driver_start_releases_destination_ownership() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        std::fs::create_dir(&destination)?;

        let failed = start_owned_session::<()>(&destination, || {
            Err(NativeMountError::Driver("injected startup failure".to_owned()).into())
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
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("mount");
        let release = temporary.path().join("release");
        std::fs::create_dir(&destination)?;
        let mut crashed = MountDestinationGuard::acquire(&destination)?;
        let crash_left_path = crashed.lock_path.clone();
        crashed.file.take();
        std::mem::forget(crashed);

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
