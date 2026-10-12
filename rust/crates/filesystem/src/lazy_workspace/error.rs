use super::*;

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Original workspace failure.
    #[error(transparent)]
    WorkspaceCause(#[from] WorkspaceError),
    /// Original filesystem failure.
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    /// Original path validation failure.
    #[error(transparent)]
    NamespacePath(#[from] crate::kernel::NamespacePathError),
    /// Demand provider operation failed.
    #[error("source demand failed: {0}")]
    Demand(#[from] DemandError),
    /// Companion state failed.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state changed too frequently to complete a bounded retry.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// Provider identity or invalidation epoch no longer matches the binding.
    #[error("lazy source binding is stale")]
    StaleSource,
    /// Requested path is absent after applying authored tombstones.
    #[error("path is absent")]
    NotFound,
    /// Requested subtree root is not a directory.
    #[error("path is not a directory")]
    NotDirectory,
    /// Requested operation requires a regular file.
    #[error("path is not a regular file")]
    NotRegularFile,
    /// Requested read exceeds the explicit bound.
    #[error("file exceeds the requested read bound")]
    TooLarge,
    /// A directory page must request at least one entry.
    #[error("directory page bound must be positive")]
    InvalidPageBound,
    /// Directory view changed after the supplied cursor was issued.
    #[error("lazy directory cursor is stale")]
    StaleCursor,
    /// The path no longer names the object the caller resolved.
    #[error("lazy filesystem object identity is stale")]
    StaleIdentity,
    /// The source fact cannot be represented by this workspace profile.
    #[error("source node cannot be represented by the authored workspace")]
    UnsupportedNode,
    /// Removing the last promoted binding would resurrect stale source bytes
    #[error("source hard-link aliases must be promoted before removing the final authored binding")]
    UnresolvedHardLinks,
    /// Exact traversal was cancelled before every path was captured.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// Original projection failure and failed producer rollback, both retained.
    #[error("{primary}; source rollback failed: {cleanup}")]
    SourcePageCleanup {
        /// Original consumer failure.
        #[source]
        primary: Box<Self>,
        /// Original rollback failure.
        cleanup: DemandError,
        /// Producer state still requiring cleanup.
        publication: Option<Box<crate::demand::SourceDirectoryPublication>>,
        /// Diagnostic allocation retires after both causes and producer state.
        #[cfg(not(target_arch = "wasm32"))]
        allocation: Option<crate::NativeAllocation>,
    },
    /// Original logical cut cannot be authenticated.
    #[error("original snapshot is unavailable")]
    OriginalSnapshotUnavailable,
    /// Original native denial.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] crate::NativeTreeMaterializationError),
    /// Native construction failed after this work prefix.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native construction failed: {cause}")]
    NativeConstruction {
        /// Original constructor failure.
        #[source]
        cause: crate::NativeTreeMaterializationError,
        /// Completed work.
        work: WorkCounters,
    },
    /// Payload belongs to another invocation.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native ledger mismatch")]
    NativeLedgerMismatch,
    /// Original creation failure with retained diagnostic storage.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeWorkspace(#[source] WorkspaceError, Option<crate::NativeAllocation>),
    /// Original IO error with its retained allocation.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordIo(#[source] std::io::Error, Option<crate::NativeAllocation>),
    /// Original codec error with its retained allocation.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordJson(#[source] serde_json::Error, Option<crate::NativeAllocation>),
    /// Record lock timed out.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record lock timed out: {}", .0.display())]
    NativeRecordLockTimeout(std::path::PathBuf, Option<crate::NativeAllocation>),
    /// Record namespace has another owner.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record ownership conflict: {}", .0.display())]
    NativeRecordOwnershipConflict(std::path::PathBuf, Option<crate::NativeAllocation>),
    /// Nonterminal materialization cannot be discarded.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization is in progress")]
    NativeRecordMaterializationInProgress,
    /// Stored record does not match its address.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record integrity failure")]
    NativeRecordIntegrity,
    /// Context transaction is incompatible.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("context transaction is incompatible: {0}")]
    NativeRecordContextTransaction(String, Option<crate::NativeAllocation>),
    /// Intact log frame is undecodable.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("corrupt record log: {}", .0.display())]
    NativeRecordCorrupt(std::path::PathBuf, Option<crate::NativeAllocation>),
}

impl LazyWorkspaceError {
    /// Original semantic cause.
    pub fn semantic_error(&self) -> &Self {
        match self {
            Self::SourcePageCleanup { primary, .. } => primary.semantic_error(),
            error => error,
        }
    }

    /// Completed standalone work retained by this failure.
    pub fn spent_work(&self) -> WorkCounters {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeConstruction { work, .. } => *work,
            _ => WorkCounters::UNCHARGED,
        }
    }

    /// Transfers the original work prefix to the standalone result.
    pub fn into_measured_failure(self) -> crate::OperationFailure<Self> {
        let work = self.spent_work();
        crate::OperationFailure::new(self, work)
    }
}
