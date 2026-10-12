use super::*;
/// Typed failures from a source dependency.
#[derive(Debug, Error)]
pub enum DemandError {
    /// The reference is for another source or an invalidated epoch.
    #[error("source reference is stale")]
    StaleSource,
    /// The node changed while a request was being answered.
    #[error("source node changed during the request")]
    StaleVersion,
    /// A directory cursor is expired or belongs to another query.
    #[error("source directory cursor is expired or mismatched")]
    StaleCursor,
    /// Another request is currently advancing this cursor.
    #[error("source directory cursor is in use")]
    CursorBusy,
    /// Source root disappeared or was replaced.
    #[error("source root is unavailable")]
    SourceUnavailable,
    /// Exact requested path is absent.
    #[error("source path is absent")]
    Absent,
    /// Request requires a regular file.
    #[error("source node is not a regular file")]
    NotRegularFile,
    /// Request requires a directory.
    #[error("source node is not a directory")]
    NotDirectory,
    /// Bounds or path are invalid.
    #[error("source request is invalid")]
    InvalidRequest,
    /// Request was cancelled.
    #[error("source request was cancelled")]
    Cancelled,
    /// The bounded native worker could not complete the request.
    #[error("native source worker failed")]
    WorkerUnavailable,
    /// This provider cannot operate against the original native ledger.
    #[error("source does not support original native work custody")]
    NativeWorkUnavailable,
    /// An original leaf panicked; no new work may use its poisoned state.
    #[error("original native work state is poisoned")]
    OriginalWorkPoisoned,
    /// Explicit native preparation allowance was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A real owned-buffer reservation failed without discarding its cause.
    #[error("source buffer allocation failed: {0}")]
    Allocation(#[source] std::collections::TryReserveError),
    /// A started native watcher remains owned until measured cleanup succeeds.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("{0}")]
    SourceWatchStartup(#[source] std::sync::Arc<crate::source_watch::SourceWatchStartupFailure>),
    /// Native or remote source failed.
    #[error("source failed: {0}")]
    Io(std::io::Error),
    /// Demand-driven native observation could not be admitted exactly.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error(transparent)]
    Observation(#[from] crate::watch::NativeWatchError),
    /// Canonical mounted-source delivery failed with its original typed cause.
    #[cfg(feature = "native-mount")]
    #[error(transparent)]
    Mount(#[from] crate::native_mount::MountSourceError),
    /// A held native metadata mutation failed with its typed cause.
    #[cfg(all(
        any(target_os = "linux", target_os = "macos", windows),
        any(feature = "native-mount", test)
    ))]
    #[error(transparent)]
    NativeMetadata(#[from] crate::native_host::NativeMetadataError),
}

impl From<std::io::Error> for DemandError {
    fn from(error: std::io::Error) -> Self {
        // A file that gave way to a rename or delete changed under the read.
        #[cfg(all(windows, feature = "native-watch"))]
        if error
            .get_ref()
            .is_some_and(|inner| inner.is::<crate::native_host::Yielded>())
        {
            return Self::StaleVersion;
        }
        Self::Io(error)
    }
}

impl From<NamespacePathError> for DemandError {
    fn from(error: NamespacePathError) -> Self {
        match error {
            NamespacePathError::Work(error) => Self::Work(error),
            NamespacePathError::Cancelled => Self::Cancelled,
            _ => Self::InvalidRequest,
        }
    }
}

/// Demand result with source-work counters on both success and failure.
pub type DemandResult<T> = Result<OperationReceipt<T>, OperationFailure<DemandError>>;

/// One change a source reports having happened to it outside every view of
/// it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceChange {
    /// This name may bind another entry, or none, and so may everything
    /// reached through it.
    Name(NamespacePath),
    /// The entry this name binds may have changed in place: its content or
    /// attributes. The name, and everything reached through it, still binds
    /// what it did.
    Entry(NamespacePath),
    /// The node with this [`SourceNode::file_identity`] may have changed,
    /// under every name bound to it.
    Node([u8; 32]),
    /// Changes may have gone unreported: anything may have changed.
    Everything,
    /// Changes may not all have been reported yet, though none is known to
    /// have been lost: every fact read before is verified against the
    /// source at its next use, and kept where it still holds.
    Unconfirmed,
}

/// Receives the changes one watched source reports.
pub trait SourceChangeSink: Send + Sync {
    /// One batch of changes, in the order the host reported them. Called on
    /// the source's notification thread, which holds no lock a source
    /// request waits on; the sink must not wait on a later notification.
    fn source_changed(&self, changes: &[SourceChange]);
}

/// Live observation of every change made to one source outside every view
/// of it. Dropping it stops the notifications.
pub trait SourceWatch: Send + Sync {
    /// Returns once every change that completed before the call has reached
    /// the sink.
    ///
    /// # Errors
    ///
    /// Fails only when the host cannot be read; the watch is then inexact.
    fn fence(&self) -> Result<(), DemandError>;

    /// Whether every change still reaches the sink. False once the host
    /// stopped reporting changes exactly, after the sink last received
    /// [`SourceChange::Everything`]; from then on a view reads the source
    /// afresh.
    fn is_exact(&self) -> bool;
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(super) fn measured<T>(
    operation: impl FnOnce(&mut WorkCounters) -> Result<T, DemandError>,
) -> DemandResult<T> {
    let mut work = WorkCounters::default();
    operation(&mut work)
        .map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Provider contract for exact lookup, bounded pages, ranges, and metadata.
/// Implementations must validate the reference and version for each request.
/// Async methods do not by themselves guarantee kernel completion I/O; each
/// provider must document its execution mechanism separately.
#[async_trait::async_trait]
pub trait DemandSource: Send + Sync {
    /// Current opaque source reference.
    fn reference(&self) -> SourceReference;

    /// Resolve only the requested path. `None` is known absence.
    async fn lookup(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<SourceNode>>;

    /// Continue one directory enumeration by at most `maximum_entries`.
    async fn list_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        cancellation: &CancellationToken,
    ) -> DemandResult<SourceDirectoryPage>;

    /// Read at most `length` bytes at `offset`, using exact version evidence.
    async fn read_range(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes>;

    /// Opens one regular file at exactly `expected` for repeated range reads.
    async fn open_file(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Box<dyn DemandFile>>;

    /// Reads one symbolic link's exact opaque target without following it.
    async fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes>;

    /// Starts reporting every change made to this source outside every view
    /// of it to `sink`, when the host reports such changes exactly: `None`
    /// when it cannot, and every view of the source then reads it afresh.
    /// The answer holds for the source, not per request.
    ///
    /// # Errors
    ///
    /// Fails when the host refuses a watch it would otherwise provide.
    fn watch(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        let _ = sink;
        Ok(None)
    }

    /// Read metadata without reading a file body.
    async fn read_metadata(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: Option<SourceVersion>,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<SourceNode>> {
        let answer = self.lookup(source, path, cancellation).await?;
        if let Some(expected) = expected {
            match answer.value {
                Some(node) if node.version == expected => {}
                _ => {
                    return Err(OperationFailure::new(
                        DemandError::StaleVersion,
                        answer.work,
                    ));
                }
            }
        }
        Ok(answer)
    }
}

/// A regular source file opened at one exact version.
///
/// Reads block the calling thread like a native file read, so they belong on
/// threads that may block, such as native mount callback threads. Every read
/// proves that the opened version is still current before returning bytes.
pub trait DemandFile: Send + Sync {
    /// Reads at most `length` bytes at `offset`; a range past the end of the
    /// file is shortened, never padded.
    ///
    /// # Errors
    ///
    /// Fails with the same typed evidence as [`DemandSource::read_range`]
    /// once the file, its path, or its source is no longer the opened one.
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes>;
}

/// Synchronous admission hook for directories reached by a native lazy source.
///
/// Implementations normally install one non-recursive native notification
/// subscription. The hook runs before the corresponding filesystem read, so a
/// change cannot race between first observation and watcher admission. It must
/// inspect only the named directory and its ancestors; recursive enumeration
/// would violate the lazy-source contract.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub trait DemandDirectoryObserver: Send + Sync {
    /// Admits one exact directory for observation.
    ///
    /// # Errors
    ///
    /// Fails closed when the directory cannot be observed exactly.
    fn observe_directory(&self, directory: &NamespacePath) -> Result<(), DemandError>;
}
