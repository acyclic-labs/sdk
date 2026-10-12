use super::*;

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
pub type ContentSink<'a> =
    dyn FnMut(u64, acyclic_stream::OwnedBytes) -> Result<(), MountSourceError> + 'a;

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
    fn lookup_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError>;
    /// Reads an exact owned range admitted on the original invocation.
    fn read_range_with_meter(
        &self,
        offset: u64,
        length: u32,
        work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads up to the original bound without losing the producer's capacity.
    fn read_up_to_with_meter(
        &self,
        offset: u64,
        maximum_bytes: u32,
        work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Finds the actual sparse boundary through the original source engine.
    fn seek_with_meter(
        &self,
        offset: u64,
        target: MountSeekTarget,
        work: &NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError>;
    /// Consumes the caller's exact already-admitted owned payload.
    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Changes length through the same operative source invocation.
    fn resize_with_meter(
        &self,
        logical_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Applies the actual sparse allocation operation.
    fn allocate_range_with_meter(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Mutates exact metadata and optional size through the same engine.
    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Reads an owned named attribute without erasing capacity.
    fn read_attribute(
        &self,
        name: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned native attribute page.
    fn list_attributes_with_meter(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted attribute value.
    fn write_attribute(
        &self,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Removes the actual named attribute through the same engine.
    fn remove_attribute(
        &self,
        name: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Settles this actual handle's queued effects before durability acknowledgement.
    fn settle(&self, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Returns the owned sparse range and its original producer's exact claim.
    fn read_sparse_range(
        &self,
        offset: u64,
        length: u32,
        work: &NativeMaterializationWork,
    ) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>;
    /// Consumes the exact already-admitted sparse payload, including capacities known before each span's conversion to `Bytes`.
    fn write_sparse_range(
        &self,
        offset: u64,
        range: (MountSparseRange, crate::NativeAllocation),
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;

    /// Returns current identity, size, and metadata for `fstat`-style calls.
    fn lookup(&self) -> Result<MountLookup, MountSourceError>;
    /// Reads one exact bounded logical range.
    fn read_range(
        &self,
        offset: u64,
        length: u32,
    ) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
    /// Reads at most `maximum_bytes`, clipping to EOF in the same coherent view.
    fn read_up_to(
        &self,
        offset: u64,
        maximum_bytes: u32,
    ) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
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
    pub(super) entries: Vec<MountDirectoryEntry>,
    /// Opaque continuation owned by this open directory handle.
    pub(super) next_cursor: Option<Vec<u8>>,
    pub(super) ownership: crate::demand::SourcePayloadOwnership,
}

impl MountDirectoryPage {
    /// Retains a page and any original allocation custody.
    #[must_use]
    pub fn new(
        entries: Vec<MountDirectoryEntry>,
        next_cursor: Option<Vec<u8>>,
        allocation: Option<crate::NativeAllocation>,
    ) -> Self {
        let ownership = allocation.map_or_else(
            crate::demand::SourcePayloadOwnership::default,
            crate::demand::SourcePayloadOwnership::from_native_allocation,
        );
        Self {
            entries,
            next_cursor,
            ownership,
        }
    }

    /// Borrows entries without detaching their physical capacity custody.
    #[must_use]
    pub fn entries(&self) -> &[MountDirectoryEntry] {
        &self.entries
    }

    /// Borrows this page's opaque continuation.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&[u8]> {
        self.next_cursor.as_deref()
    }

    /// Internal physical projections transfer data and its exact custody together, splitting that custody only for actual escaping payloads.
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<MountDirectoryEntry>,
        Option<Vec<u8>>,
        crate::demand::SourcePayloadOwnership,
    ) {
        (self.entries, self.next_cursor, self.ownership)
    }

    /// Known owned capacities of this exact page, excluding borrowed metadata and opaque allocator overhead.
    pub fn logical_owned_bytes(&self) -> Result<u64, crate::WorkError> {
        let entries = self
            .entries
            .capacity()
            .checked_mul(std::mem::size_of::<MountDirectoryEntry>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.entries
            .iter()
            .try_fold(entries, |bytes, entry| {
                bytes
                    .checked_add(entry.name.capacity() as u64)
                    .ok_or(crate::WorkError::Overflow)
            })?
            .checked_add(
                self.next_cursor
                    .as_ref()
                    .map_or(0, |cursor| cursor.capacity() as u64),
            )
            .ok_or(crate::WorkError::Overflow)
    }
}

impl std::fmt::Debug for MountDirectoryPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountDirectoryPage")
            .field("entries", &self.entries)
            .field("next_cursor", &self.next_cursor)
            .finish()
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
        let slots = self
            .names
            .capacity()
            .checked_mul(std::mem::size_of::<Vec<u8>>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.names
            .iter()
            .try_fold(slots, |bytes, name| {
                bytes
                    .checked_add(name.capacity() as u64)
                    .ok_or(crate::WorkError::Overflow)
            })?
            .checked_add(
                self.next_cursor
                    .as_ref()
                    .map_or(0, |cursor| cursor.capacity() as u64),
            )
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
    pub(super) components: Vec<Vec<u8>>,
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
    Invalid(std::borrow::Cow<'static, str>),
    /// Operation is not exactly observable or representable on this platform.
    #[error("filesystem operation is unsupported: {0}")]
    Unsupported(std::borrow::Cow<'static, str>),
    /// Canonical engine operation failed.
    #[error("filesystem engine failure: {0}")]
    Engine(std::borrow::Cow<'static, str>),
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
    SelectedPublication(crate::LivePublicationObservation),
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
    #[error(
        "filesystem engine failure: source demand failed: native watcher startup failed: {cause}"
    )]
    NativeWatchStartupFailed {
        /// Original typed cause, moved without another allocation.
        #[source]
        cause: crate::NativeTreeMaterializationError,
        /// Actual same root watcher to retain through measured terminal cleanup.
        watch: crate::watch::NativeWatch,
    },
    /// Same complete root watcher and already-retained source startup cause.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error(
        "filesystem engine failure: source demand failed: native source watcher startup failed: {cause}"
    )]
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
    #[error("{0}")]
    NativeWorkspaceCause(#[source] crate::WorkspaceError, Option<crate::NativeAllocation>),
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
    /// Returns the underlying source failure.
    pub fn semantic_error(&self) -> &Self {
        let mut error = self;
        loop {
            match error {
                Self::Retained(cause) => error = cause.semantic_error(),
                Self::MutationFinalization { primary, .. } if primary.is_some() => {
                    error = primary
                        .as_ref()
                        .as_ref()
                        .expect("a completed mutation has its original primary")
                }
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
            Self::NativeWorkspaceCause(_, allocation) => Some(allocation),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MountSourceError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error, None)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn native_materialization_errno(error: &crate::NativeTreeMaterializationError) -> i32 {
    match error {
        crate::NativeTreeMaterializationError::Io(error) => {
            error.raw_os_error().unwrap_or(libc::EIO)
        }
        crate::NativeTreeMaterializationError::Cancelled => libc::ESTALE,
        crate::NativeTreeMaterializationError::AllocationFailed(_) => libc::ENOMEM,
        crate::NativeTreeMaterializationError::PayloadTooLarge { .. } => libc::EFBIG,
        _ => libc::EIO,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn native_workspace_errno(error: &crate::WorkspaceError) -> i32 {
    use crate::WorkspaceError;
    match error {
        WorkspaceError::Cancelled(_)
        | WorkspaceError::Filesystem(crate::FsError::Cancelled(_))
        | WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
            libc::ECANCELED
        }
        WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) => {
            native_materialization_errno(error)
        }
        WorkspaceError::NotFound | WorkspaceError::Filesystem(crate::FsError::NotFound) => {
            libc::ENOENT
        }
        WorkspaceError::NotDirectory | WorkspaceError::Filesystem(crate::FsError::NotDirectory) => {
            libc::ENOTDIR
        }
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => libc::EIO,
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed)
        | WorkspaceError::Filesystem(crate::FsError::HandleAllocationFailed) => libc::ENOMEM,
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NamespacePath(_) => libc::EINVAL,
        WorkspaceError::ReadLimitExceeded => libc::EFBIG,
        WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity
        | WorkspaceError::StaleTarget
        | WorkspaceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        _ => libc::EIO,
    }
}

pub(crate) fn native_workspace_failure_is_fatal(error: &crate::WorkspaceError) -> bool {
    if let crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) = error {
        return native_materialization_failure_is_fatal(error);
    }
    matches!(
        crate::workspace::workspace_bounded_error(error),
        Some(
            crate::WorkspaceError::Work(_)
                | crate::WorkspaceError::Cancelled(_)
                | crate::WorkspaceError::Filesystem(crate::FsError::RootReadWorkMerge { .. })
        )
    ) || matches!(
        error,
        crate::WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed)
            | crate::WorkspaceError::Filesystem(crate::FsError::HandleAllocationFailed)
    )
}

pub(crate) fn native_materialization_failure_is_fatal(
    error: &crate::NativeTreeMaterializationError,
) -> bool {
    matches!(
        error,
        crate::NativeTreeMaterializationError::Work(_)
            | crate::NativeTreeMaterializationError::Cancelled
            | crate::NativeTreeMaterializationError::AllocationFailed(_)
            | crate::NativeTreeMaterializationError::IndeterminatePreparation
            | crate::NativeTreeMaterializationError::ExternalMutation
            | crate::NativeTreeMaterializationError::AllocationChanged { .. }
    )
}

impl From<NativeMountStartupAdmissionError> for MountSourceError {
    fn from(error: NativeMountStartupAdmissionError) -> Self {
        match error {
            NativeMountStartupAdmissionError::Materialization(
                crate::NativeTreeMaterializationError::Work(error),
            ) => Self::Work(error),
            NativeMountStartupAdmissionError::Materialization(error) => Self::NativeWork(error),
            NativeMountStartupAdmissionError::Demand(error) => lazy::mount_demand_error(error),
            NativeMountStartupAdmissionError::ServicesCold => Self::ServicesNotInitialized,
            NativeMountStartupAdmissionError::ServiceUnavailable(error) => {
                Self::ServiceUnavailable(error)
            }
        }
    }
}
