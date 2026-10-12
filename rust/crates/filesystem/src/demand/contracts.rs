use super::*;
use crate::NativeMaterializationWork;
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

impl From<crate::FsError> for DemandError {
    fn from(error: crate::FsError) -> Self {
        match error {
            crate::FsError::Work(error) => DemandError::Work(error),
            crate::FsError::Cancelled(_) => DemandError::Cancelled,
            crate::FsError::EncodedAllocationFailed(error) => DemandError::Allocation(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::FsError::NativeKernel(error) => match error {
                crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
                crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
                crate::NativeTreeMaterializationError::OriginalWorkPoisoned => {
                    DemandError::OriginalWorkPoisoned
                }
                crate::NativeTreeMaterializationError::AllocationFailed(error) => {
                    DemandError::Allocation(error)
                }
                _ => DemandError::InvalidRequest,
            },
            _ => DemandError::InvalidRequest,
        }
    }
}

/// Demand result with source-work counters on both success and failure.
pub type DemandResult<T> = Result<OperationReceipt<T>, OperationFailure<DemandError>>;

/// One change a source reports having happened to it outside every view of
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceChange {
    /// This name may bind another entry, or none, and so may everything
    Name(NamespacePath),
    /// The entry this name binds may have changed in place: its content or
    Entry(NamespacePath),
    /// The node with this [`SourceNode::file_identity`] may have changed,
    Node([u8; 32]),
    /// Changes may have gone unreported: anything may have changed.
    Everything,
    /// Changes may not all have been reported yet, though none is known to
    Unconfirmed,
}

/// Receives the changes one watched source reports.
pub trait SourceChangeSink: Send + Sync {
    /// SDK payload retained by this sink, excluding caller-owned data.
    fn logical_owned_bytes(&self) -> u64 {
        0
    }

    /// Delivers one ordered batch on the caller's accounting context.
    fn source_changed(
        &self,
        changes: &[SourceChange],
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
}

/// Live observation of every change made to one source outside every view
pub trait SourceWatch: Send + Sync {
    /// Delivers preceding changes within the accounting allowance.
    fn fence(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError>;

    /// Quiesces this subscription within the accounting allowance.
    fn stop(&self, accounting: &mut crate::kernel::KernelAccounting<'_>)
    -> Result<(), DemandError>;

    /// Retires this owned subscription against its original birth-paid cleanup.
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Completes only the already-accepted native physical retirement plan,
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    /// True only after this provider has observed physical watch quiescence.
    fn is_stopped(&self) -> bool {
        false
    }

    /// SDK logical payload retained by this returned owner. Existing borrowed
    fn logical_owned_bytes(&self) -> u64;

    /// Whether every change still reaches the sink. False once the host
    fn is_exact(&self) -> bool;
}

/// Provider contract for exact lookup, bounded pages, ranges, and metadata.
pub trait DemandSource: Send + Sync {
    /// Current opaque source reference.
    fn reference(&self) -> SourceReference;

    /// Resolve only the requested path. `None` is known absence.
    fn lookup<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<Option<SourceNode>, DemandError>> + Send;

    /// Continue one directory enumeration by at most `maximum_entries`.
    fn list_page<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<SourceDirectoryPage, DemandError>> + Send;

    /// Read at most `length` bytes at `offset`, using exact version evidence.
    fn read_range<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<crate::kernel::OwnedSourceChunk, DemandError>> + Send;

    /// Opens one regular file at exactly `expected` for repeated range reads.
    fn open_file<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<Box<dyn DemandFile>, DemandError>> + Send;

    /// Reads one symbolic link's exact opaque target without following it.
    fn read_link<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<crate::kernel::OwnedSourceChunk, DemandError>> + Send;

    /// Starts reporting every change made to this source outside every view
    fn watch<W: crate::facade::FsWork + Send>(
        &self,
        _sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let accounting = work.accounting(cancellation);
        let result = (|| {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                crate::native_host::NativeHostWork::check(&NativeMaterializationWork::clone(
                    original,
                ))?;
                return Err(DemandError::NativeWorkUnavailable);
            }
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            Ok(None)
        })();
        work.finish(result, &accounting)
    }

    /// Read metadata without reading a file body.
    fn read_metadata<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: Option<SourceVersion>,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<Option<SourceNode>, DemandError>> + Send {
        async move {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    let mut node = self.lookup(source, path, original, cancellation).await?;
                    crate::native_host::NativeHostWork::check(&NativeMaterializationWork::clone(
                        original,
                    ))?;
                    if let Some(expected) = expected
                        && node.as_ref().map(|node| node.version) != Some(expected)
                    {
                        return Err(DemandError::StaleVersion);
                    }
                    if let Some(node) = &mut node
                        && node.kind == SourceNodeKind::RegularFile
                    {
                        node.link_count = None;
                    }
                    return Ok(node);
                }
                let budget = accounting.remaining().map_err(DemandError::from)?;
                let answer = self.lookup(source, path, budget, cancellation).await;
                let node = accept_source_result(answer, &mut accounting)?;
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                if let Some(expected) = expected {
                    accounting
                        .charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })
                        .map_err(DemandError::from)?;
                    if node.as_ref().map(|node| node.version) != Some(expected) {
                        return Err(DemandError::StaleVersion);
                    }
                }
                Ok(node)
            }
            .await;
            work.finish(result, &accounting)
        }
    }
}

/// A regular source file opened at one exact version.
pub trait DemandFile: Send + Sync {
    /// SDK-owned payload retained by this opened file, including its box;
    fn logical_owned_bytes(&self) -> u64;

    /// Reads a bounded range at the file's pinned version.
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError>;
}

/// Synchronous admission hook for directories reached by a native lazy source.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub trait DemandDirectoryObserver: Send + Sync {
    /// Admits one exact directory on the caller's accounting context.
    fn observe_directory(
        &self,
        directory: &NamespacePath,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
}

pub(crate) fn accept_source_result<T>(
    answer: DemandResult<T>,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<T, DemandError> {
    let (result, completed) = match answer {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err(failure) => (Err(failure.error), *failure.work),
    };
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(DemandError::NativeWorkUnavailable)?;
    *work = work.with_backend(completed, allocations.live_bytes())?;
    work.verify(budget)?;
    result
}
