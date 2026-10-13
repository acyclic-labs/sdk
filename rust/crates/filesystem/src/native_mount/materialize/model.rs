use super::*;

/// Exact hard bounds and destination for one materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeOptions {
    /// Existing empty directory owned by the caller.
    pub destination: PathBuf,
    /// Original host allocation, when attachment follows a durable preparation.
    pub expected_root_identity: Option<crate::NativeRootIdentity>,
    /// Maximum directory entries fetched per authenticated page.
    pub maximum_directory_entries: u32,
    /// Maximum sparse spans fetched per authenticated range plan.
    pub maximum_extent_spans: u32,
    /// Maximum bytes read or zero-filled in one allocation.
    pub transfer_bytes: u64,
}

/// Both outputs preserve canonical metadata in the SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializeMode {
    DurableOutput,
    ReconstructibleView,
}

pub(super) fn host_metadata(metadata: FileMetadata) -> (FileMetadata, Option<crate::ObjectId>) {
    #[cfg(unix)]
    let mut metadata = metadata;
    #[cfg(unix)]
    {
        metadata.changed_ns = crate::kernel::MetadataField::Unavailable;
    }
    #[cfg(target_os = "linux")]
    {
        metadata.created_ns = crate::kernel::MetadataField::Unavailable;
        if metadata.posix_flags == crate::kernel::MetadataField::Value(0) {
            metadata.posix_flags = crate::kernel::MetadataField::Unavailable;
        }
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let named_attributes = match metadata.named_attributes {
        crate::kernel::MetadataField::Value(root) => {
            metadata.named_attributes = crate::kernel::MetadataField::Unavailable;
            Some(root)
        }
        _ => None,
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let named_attributes = None;
    (metadata, named_attributes)
}

impl MaterializeOptions {
    /// Creates the canonical bounded native materialization policy.
    #[must_use]
    pub fn native(destination: impl Into<PathBuf>) -> Self {
        Self {
            destination: destination.into(),
            expected_root_identity: None,
            maximum_directory_entries: 1_024,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Exact result and host-side movement facts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaterializationReceipt {
    /// Materialized regular-file or hard-link bindings.
    pub files: u64,
    /// Materialized directory bindings, excluding the supplied root.
    pub directories: u64,
    /// Materialized symbolic-link bindings.
    pub symbolic_links: u64,
    /// Materialized FIFO, socket, character-device, or block-device bindings.
    pub special_files: u64,
    /// Logical regular-file bytes represented at the destination.
    pub logical_file_bytes: u64,
    /// Bytes physically written for content and allocated-zero spans.
    pub written_bytes: u64,
}

/// How a materialized host path replaces its current destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathReplacement {
    /// Exchange an existing entry atomically on the same volume.
    Atomic,
    /// Route ordinary renames through a live kernel mount.
    LiveMount,
}

/// Exact result of restoring one authenticated path into a host tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathRestore {
    /// The authenticated entry was materialized and published.
    Restored,
    /// The authenticated path was absent and the host entry was removed.
    Removed,
}

/// Fail-closed explicit materialization errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum MaterializeError {
    /// Destination is absent, non-directory, a symlink, or non-empty.
    #[error("materialization destination must be an existing empty real directory")]
    InvalidDestination,
    /// The destination path now names another physical allocation.
    #[error(
        "materialization destination allocation changed: expected {expected:?}, actual {actual:?}"
    )]
    AllocationChanged {
        /// Original witnessed allocation.
        expected: crate::NativeRootIdentity,
        /// Allocation actually opened for the attempted materialization.
        actual: crate::NativeRootIdentity,
    },
    /// A canonical name cannot be represented exactly on this host.
    #[error("canonical name cannot be represented exactly on this host")]
    UnrepresentableName,
    /// The requested authenticated path is absent from the checkout.
    #[error("requested path is absent from the checkout")]
    MissingPath,
    /// Host path is empty, absolute, escapes the root, or cannot be encoded.
    #[error("requested host path is not a valid relative path")]
    InvalidPath,
    /// This host adapter cannot recreate the authenticated kind exactly.
    #[error("file kind {0:?} cannot be materialized exactly on this host")]
    UnsupportedKind(FileKind),
    /// This host adapter cannot reproduce an authenticated metadata field.
    #[error("metadata field {0} cannot be materialized exactly on this host")]
    UnsupportedMetadata(&'static str),
    /// Supplied operation bounds are zero or exceed native addressability.
    #[error("materialization options are invalid")]
    InvalidOptions,
    /// Selected paths overlap, so pathwise staging would be order-dependent.
    #[error("materialization paths must be unique and non-overlapping")]
    OverlappingPaths,
    /// Original native accounting or custody failed without diagnostic copying.
    #[error(transparent)]
    NativeWork(crate::NativeTreeMaterializationError),
    /// Original SDK source failed without copying its diagnostic payload.
    #[error(transparent)]
    Source(crate::FsError),
    /// A fixed engine invariant failed without allocating diagnostic text.
    #[error("filesystem engine failed: {0}")]
    Engine(&'static str),
    /// The original host/demand cause remains typed and unboxed.
    #[error(transparent)]
    Demand(crate::DemandError),
    /// Replacement and its rollback both failed; the owned backup is retained.
    #[error(
        "replacement failed: {primary}; displaced entry remains in the owned backup after rollback failed: {rollback}"
    )]
    RestoreRollback {
        /// Primary actual replacement failure.
        #[source]
        primary: crate::DemandError,
        /// Actual attempted rollback failure.
        rollback: crate::DemandError,
    },
    /// Primary materialization and actual cleanup both failed.
    #[error("materialization failed: {primary}; cleanup also failed: {cleanup}")]
    RestoreCleanup {
        /// Original primary cause in its pre-admitted physical slot.
        #[source]
        primary: Box<MaterializeError>,
        /// Actual cleanup cause, without diagnostic copying.
        cleanup: crate::DemandError,
        /// Allocation released after both error payloads.
        allocation: crate::NativeAllocation,
    },
    /// Host filesystem operation failed.
    #[error("host filesystem operation failed: {0}")]
    Io(#[source] std::io::Error),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// The original caller cancellation stopped materialization.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

impl From<crate::FsError> for MaterializeError {
    fn from(error: crate::FsError) -> Self {
        match error {
            crate::FsError::NativeKernel(error) => MaterializeError::from(error),
            crate::FsError::Work(error)
            | crate::FsError::Authority(crate::AuthorityStoreError::Work(error))
            | crate::FsError::Object(crate::ObjectStoreError::Work(error))
            | crate::FsError::Path(crate::kernel::PathLookupError::Work(error))
            | crate::FsError::Path(crate::kernel::PathLookupError::FileTable(
                crate::kernel::FileRecordReadError::Work(error)
                | crate::kernel::FileRecordReadError::Storage(crate::ObjectStoreError::Work(error)),
            ))
            | crate::FsError::Path(crate::kernel::PathLookupError::Tree(
                crate::kernel::TreeReadError::Work(error)
                | crate::kernel::TreeReadError::Storage(crate::ObjectStoreError::Work(error)),
            )) => Self::Work(error),
            crate::FsError::Cancelled(error) => Self::Cancelled(error),
            crate::FsError::Authority(crate::AuthorityStoreError::Cancelled)
            | crate::FsError::Object(crate::ObjectStoreError::Cancelled)
            | crate::FsError::Path(crate::kernel::PathLookupError::FileTable(
                crate::kernel::FileRecordReadError::Storage(crate::ObjectStoreError::Cancelled),
            ))
            | crate::FsError::Path(crate::kernel::PathLookupError::Tree(
                crate::kernel::TreeReadError::Storage(crate::ObjectStoreError::Cancelled),
            )) => Self::Cancelled(crate::CancellationError),
            error => Self::Source(error),
        }
    }
}

impl From<std::io::Error> for MaterializeError {
    fn from(error: std::io::Error) -> Self {
        if let Some(work) = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<WorkError>())
        {
            MaterializeError::Work(*work)
        } else if let Some(demand) = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<DemandError>())
        {
            match demand {
                DemandError::Work(work) => MaterializeError::Work(*work),
                DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
                _ => MaterializeError::Io(error),
            }
        } else {
            MaterializeError::Io(error)
        }
    }
}

impl From<crate::NativeTreeMaterializationError> for MaterializeError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self {
        match error {
            crate::NativeTreeMaterializationError::Work(error) => MaterializeError::Work(error),
            crate::NativeTreeMaterializationError::Cancelled => {
                MaterializeError::Cancelled(crate::CancellationError)
            }
            crate::NativeTreeMaterializationError::Io(error) => MaterializeError::Io(error),
            error => MaterializeError::NativeWork(error),
        }
    }
}

impl From<DemandError> for MaterializeError {
    fn from(error: DemandError) -> Self {
        match error {
            DemandError::Work(error) => MaterializeError::Work(error),
            DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
            DemandError::Io(error) => MaterializeError::Io(error),
            #[cfg(target_os = "linux")]
            DemandError::NativeMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
            #[cfg(target_os = "macos")]
            DemandError::NativeMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
            #[cfg(windows)]
            DemandError::NativeMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
            error => MaterializeError::Demand(error),
        }
    }
}
