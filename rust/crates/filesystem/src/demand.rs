//! Bounded, versioned access to unresolved filesystem sources.
//!
//! A source reference identifies a provider capability. It never contains a
//! host path or provider credential. Resolving one request does not publish a
//! workspace generation or make the result part of recorded history.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opaque identity and invalidation epoch of an attached source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SourceReference {
    /// Provider-scoped opaque identity; no local path is encoded here.
    pub identity: [u8; 16],
    /// Incremented when source knowledge is invalidated.
    pub epoch: u64,
}

/// Evidence for the exact node observed by a source request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SourceVersion(pub [u8; 32]);

/// The kind of an observed source node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceNodeKind {
    /// Directory, whose children remain unresolved until requested.
    Directory,
    /// Regular file, whose content ranges remain unresolved until requested.
    RegularFile,
    /// Symbolic link, never followed by the source provider.
    SymbolicLink,
    /// POSIX named pipe.
    Fifo,
    /// POSIX local socket node.
    Socket,
    /// Native character device.
    CharacterDevice,
    /// Native block device.
    BlockDevice,
    /// Another native node kind which cannot be projected losslessly.
    Unsupported,
}

/// Scalar host metadata. `None` means the provider did not make that fact
/// available; it does not mean a numeric zero.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceMetadata {
    /// POSIX permission and kind bits, when available.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner, when available.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group, when available.
    pub posix_gid: Option<u32>,
    /// POSIX inode flags, when available.
    pub posix_flags: Option<u64>,
    /// Windows file attributes, when available.
    pub windows_attributes: Option<u32>,
    /// Creation/birth time in signed Unix nanoseconds.
    pub created_ns: Option<i128>,
    /// Last content modification time in signed Unix nanoseconds.
    pub modified_ns: Option<i128>,
    /// Last access time in signed Unix nanoseconds.
    pub accessed_ns: Option<i128>,
    /// Last metadata-change time in signed Unix nanoseconds.
    pub changed_ns: Option<i128>,
}

/// Metadata returned by an exact path lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceNode {
    /// Source node kind.
    pub kind: SourceNodeKind,
    /// Stable path-independent source identity. Hard-link aliases share it.
    pub file_identity: [u8; 32],
    /// Exact number of source namespace bindings when the provider exposes it.
    pub link_count: Option<u64>,
    /// Native device identity for character and block devices.
    pub device: Option<(u32, u32)>,
    /// Logical length for a regular file; absent for other kinds.
    pub logical_bytes: Option<u64>,
    /// Version that must match a later read of this node.
    pub version: SourceVersion,
    /// Explicitly available scalar metadata.
    pub metadata: SourceMetadata,
}

/// One directory entry in native enumeration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryEntry {
    /// Lossless host component encoding.
    pub name: crate::kernel::LogicalName,
    /// Entry kind, observed without reading its body.
    pub kind: SourceNodeKind,
}

/// Cursor bound to one directory and one source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    source: SourceReference,
    directory: NamespacePath,
    directory_version: SourceVersion,
    token: u64,
}

/// One bounded directory page. Native pages retain native enumeration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryPage {
    /// At most the requested number of entries.
    pub entries: Vec<SourceDirectoryEntry>,
    /// Opaque continuation bound to this source and directory version.
    pub next: Option<SourceCursor>,
    /// Directory metadata evidence for this page.
    pub version: SourceVersion,
}

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
    /// Native or remote source failed.
    #[error("source failed: {0}")]
    Io(#[from] std::io::Error),
    /// Demand-driven native observation could not be admitted exactly.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error(transparent)]
    Observation(#[from] crate::watch::NativeWatchError),
}

impl From<NamespacePathError> for DemandError {
    fn from(_: NamespacePathError) -> Self {
        Self::InvalidRequest
    }
}

/// Demand result with source-work counters on both success and failure.
pub type DemandResult<T> = Result<OperationReceipt<T>, OperationFailure<DemandError>>;

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn measured<T>(
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

    /// Reads one symbolic link's exact opaque target without following it.
    async fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes>;

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

/// A composable source view that hides exact subtrees before they enter lazy
/// observation state. This is the SDK boundary used for host-private entries
/// such as a physical checkout's `.git` directory.
#[derive(Clone)]
pub struct FilteredDemandSource<D> {
    inner: D,
    excluded: std::sync::Arc<Vec<NamespacePath>>,
}

impl<D> FilteredDemandSource<D> {
    /// Wraps a source with canonical excluded subtree roots.
    #[must_use]
    pub fn new(inner: D, excluded: Vec<NamespacePath>) -> Self {
        Self {
            inner,
            excluded: std::sync::Arc::new(excluded),
        }
    }

    /// Returns the wrapped source capability.
    #[must_use]
    pub const fn inner(&self) -> &D {
        &self.inner
    }

    fn excludes(&self, path: &NamespacePath) -> bool {
        self.excluded.iter().any(|root| path.is_within(root))
    }

    fn excludes_child(&self, directory: &NamespacePath, name: &crate::kernel::LogicalName) -> bool {
        self.excluded.iter().any(|root| {
            root.components().len() == directory.components().len().saturating_add(1)
                && root.components().starts_with(directory.components())
                && root.components().last() == Some(name)
        })
    }
}

#[async_trait::async_trait]
impl<D: DemandSource> DemandSource for FilteredDemandSource<D> {
    fn reference(&self) -> SourceReference {
        self.inner.reference()
    }

    async fn lookup(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<SourceNode>> {
        if self.excludes(path) {
            return Ok(OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            });
        }
        self.inner.lookup(source, path, cancellation).await
    }

    async fn list_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        cancellation: &CancellationToken,
    ) -> DemandResult<SourceDirectoryPage> {
        if self.excludes(directory) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        let mut page = self
            .inner
            .list_page(source, directory, cursor, maximum_entries, cancellation)
            .await?;
        page.value
            .entries
            .retain(|entry| !self.excludes_child(directory, &entry.name));
        Ok(page)
    }

    async fn read_range(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        if self.excludes(path) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        self.inner
            .read_range(source, path, expected, offset, length, cancellation)
            .await
    }

    async fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        if self.excludes(path) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        self.inner
            .read_link(source, path, expected, cancellation)
            .await
    }
}

/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native {
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::native_host::HostRoot;
    #[cfg(unix)]
    use cap_std::fs::FileTypeExt as _;
    use cap_std::fs::{Metadata, MetadataExt, ReadDir};
    use std::collections::{BTreeMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Semaphore;

    const MAXIMUM_NATIVE_REQUESTS: usize = 64;

    struct DirectoryState {
        directory: NamespacePath,
        version: SourceVersion,
        entries: ReadDir,
        pending: Option<cap_std::fs::DirEntry>,
        replay: VecDeque<SourceDirectoryEntry>,
    }

    struct CursorState {
        directory: Option<DirectoryState>,
    }

    pub(super) struct CursorLease<'a> {
        source: &'a NativeDemandSource,
        reference: SourceReference,
        token: u64,
        state: Option<DirectoryState>,
        restore: bool,
    }

    impl CursorLease<'_> {
        fn state(&mut self) -> Result<&mut DirectoryState, DemandError> {
            self.state.as_mut().ok_or(DemandError::StaleCursor)
        }

        fn take(&mut self) -> Result<DirectoryState, DemandError> {
            self.state.take().ok_or(DemandError::StaleCursor)
        }

        fn discard(&mut self) {
            self.state.take();
            if self.restore {
                if let Ok(mut active) = self.source.inner.cursors.lock() {
                    active.remove(&self.token);
                }
                self.restore = false;
            }
        }

        fn abandon(&mut self) {
            self.discard();
        }

        fn commit(&mut self) -> Result<(), DemandError> {
            let mut active = self
                .source
                .inner
                .cursors
                .lock()
                .map_err(|_| DemandError::StaleCursor)?;
            if self.source.reference() != self.reference {
                return Err(DemandError::StaleSource);
            }
            if self.restore {
                let Some(slot) = active.get_mut(&self.token) else {
                    return Err(DemandError::StaleCursor);
                };
                slot.directory = Some(self.take()?);
                self.restore = false;
            } else {
                if active.len() >= 1024
                    && let Some(oldest) = active.keys().next().copied()
                {
                    active.remove(&oldest);
                }
                active.insert(
                    self.token,
                    CursorState {
                        directory: Some(self.take()?),
                    },
                );
            }
            Ok(())
        }
    }

    impl Drop for CursorLease<'_> {
        fn drop(&mut self) {
            if !self.restore {
                return;
            }
            if let Ok(mut active) = self.source.inner.cursors.lock()
                && self.source.reference() == self.reference
                && let Some(slot) = active.get_mut(&self.token)
            {
                if let Some(state) = self.state.take() {
                    slot.directory = Some(state);
                } else {
                    active.remove(&self.token);
                }
            }
        }
    }

    /// One native root. Opening it does not enumerate any descendants.
    /// Filesystem calls currently run on Tokio blocking workers on every host.
    #[derive(Clone)]
    pub struct NativeDemandSource {
        inner: Arc<NativeDemandInner>,
    }

    struct NativeDemandInner {
        path: PathBuf,
        root: HostRoot,
        identity: [u8; 16],
        epoch: AtomicU64,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        cursors: Mutex<BTreeMap<u64, CursorState>>,
        next_cursor: AtomicU64,
        requests: Arc<Semaphore>,
        observer: Option<Arc<dyn DemandDirectoryObserver>>,
    }

    struct CancelWorkerOnDrop(CancellationToken);

    impl Drop for CancelWorkerOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    impl NativeDemandSource {
        /// Returns the stable identity of the already-open native root.
        #[must_use]
        pub fn root_identity(&self) -> crate::NativeRootIdentity {
            self.inner.root.identity()
        }

        #[cfg(test)]
        pub(super) async fn occupy_all_requests(
            &self,
        ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
            self.inner.requests.clone().acquire_many_owned(64).await
        }

        #[cfg(test)]
        pub(super) async fn wait_for_cancelled_worker(
            &self,
            started: tokio::sync::oneshot::Sender<()>,
            stopped: tokio::sync::oneshot::Sender<()>,
        ) {
            let cancellation = CancellationToken::new();
            let _ = self
                .run_blocking(&cancellation, move |_, worker_cancellation| {
                    let _ = started.send(());
                    while !worker_cancellation.is_cancelled() {
                        std::thread::yield_now();
                    }
                    let _ = stopped.send(());
                    Ok(OperationReceipt {
                        value: (),
                        work: WorkCounters::default(),
                    })
                })
                .await;
        }

        #[cfg(test)]
        pub(super) async fn validate_after_work_for_test(
            &self,
            work: WorkCounters,
            cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            self.run_blocking_after_work(work, cancellation, |_, _| {
                unreachable!("validation must not start")
            })
            .await
        }

        #[cfg(test)]
        pub(super) fn cancel_during_page(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            cursor: SourceCursor,
            cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            measured(|work| {
                let metadata = self.metadata(directory)?.ok_or(DemandError::Absent)?;
                let mut lease =
                    self.cursor_lease(source, directory, version(&metadata), Some(cursor))?;
                let mut visited = 0;
                let result = self.page_entries(lease.state()?, 4096, cancellation, work, || {
                    visited += 1;
                    if visited == 2 {
                        cancellation.cancel();
                    }
                });
                match result {
                    Err(DemandError::Cancelled) => {
                        drop(lease);
                        Ok(())
                    }
                    Err(error) => Err(error),
                    Ok(_) => Err(DemandError::InvalidRequest),
                }
            })
        }

        /// Opens and identifies only the root directory off the async executor.
        pub async fn open(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
        ) -> Result<Self, DemandError> {
            Self::open_configured(
                path,
                profile,
                limits,
                SourceReference {
                    identity: *uuid::Uuid::new_v4().as_bytes(),
                    epoch: 0,
                },
                None,
            )
            .await
        }

        /// Opens a lazy native source whose demanded directories are admitted
        /// to an external observer before any corresponding filesystem read.
        ///
        /// # Errors
        ///
        /// Returns the same bounded native admission failures as [`Self::open`].
        pub async fn open_with_observer(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            observer: Arc<dyn DemandDirectoryObserver>,
        ) -> Result<Self, DemandError> {
            Self::open_with_reference_and_observer(
                path,
                profile,
                limits,
                SourceReference {
                    identity: *uuid::Uuid::new_v4().as_bytes(),
                    epoch: 0,
                },
                observer,
            )
            .await
        }

        /// Reopens only the root directory using a durable provider identity.
        ///
        /// Adapters persist this opaque reference beside the lazy workspace;
        /// reopening never enumerates descendants or reads file contents.
        pub async fn open_with_reference(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
        ) -> Result<Self, DemandError> {
            Self::open_configured(path, profile, limits, reference, None).await
        }

        /// Reopens a durably identified lazy source with demand-driven
        /// directory observation.
        ///
        /// # Errors
        ///
        /// Returns the same bounded native admission failures as
        /// [`Self::open_with_reference`].
        pub async fn open_with_reference_and_observer(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Arc<dyn DemandDirectoryObserver>,
        ) -> Result<Self, DemandError> {
            Self::open_configured(path, profile, limits, reference, Some(observer)).await
        }

        async fn open_configured(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
        ) -> Result<Self, DemandError> {
            let path = path.as_ref().to_path_buf();
            tokio::task::spawn_blocking(move || {
                Self::open_blocking(path, profile, limits, reference, observer)
            })
            .await
            .map_err(|_| DemandError::WorkerUnavailable)?
        }

        fn open_blocking(
            path: PathBuf,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
        ) -> Result<Self, DemandError> {
            let path = if path.is_absolute() {
                path
            } else {
                std::env::current_dir()?.join(path)
            };
            let root = HostRoot::open(&path)?;
            Ok(Self {
                inner: Arc::new(NativeDemandInner {
                    path,
                    root,
                    identity: reference.identity,
                    epoch: AtomicU64::new(reference.epoch),
                    profile,
                    limits,
                    cursors: Mutex::new(BTreeMap::new()),
                    next_cursor: AtomicU64::new(0),
                    requests: Arc::new(Semaphore::new(MAXIMUM_NATIVE_REQUESTS)),
                    observer,
                }),
            })
        }

        fn observe_directory(&self, directory: &NamespacePath) -> Result<(), DemandError> {
            self.inner
                .observer
                .as_ref()
                .map_or(Ok(()), |observer| observer.observe_directory(directory))
        }

        fn observe_parent(&self, path: &NamespacePath) -> Result<(), DemandError> {
            let parent = path.parent().unwrap_or_else(|| path.clone());
            self.observe_directory(&parent)
        }

        async fn run_blocking<T: Send + 'static>(
            &self,
            cancellation: &CancellationToken,
            job: impl FnOnce(Self, CancellationToken) -> DemandResult<T> + Send + 'static,
        ) -> DemandResult<T> {
            if cancellation.is_cancelled() {
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            let permit = tokio::select! {
                acquired = self.inner.requests.clone().acquire_owned() =>
                    acquired.map_err(|_| OperationFailure::before_work(DemandError::WorkerUnavailable))?,
                () = cancellation.cancelled() =>
                    return Err(OperationFailure::before_work(DemandError::Cancelled)),
            };
            if cancellation.is_cancelled() {
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            let source = self.clone();
            let cancel_on_drop = CancelWorkerOnDrop(CancellationToken::new());
            let worker_cancellation = cancel_on_drop.0.clone();
            let mut worker = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                job(source, worker_cancellation)
            });
            let result = tokio::select! {
                result = &mut worker => result,
                () = cancellation.cancelled() => {
                    cancel_on_drop.0.cancel();
                    worker.await
                },
            };
            result.map_err(|_| OperationFailure::before_work(DemandError::WorkerUnavailable))?
        }

        async fn run_blocking_after_work<T: Send + 'static>(
            &self,
            work: WorkCounters,
            cancellation: &CancellationToken,
            job: impl FnOnce(Self, CancellationToken) -> DemandResult<T> + Send + 'static,
        ) -> DemandResult<T> {
            if cancellation.is_cancelled() {
                return Err(OperationFailure::new(DemandError::Cancelled, work));
            }
            let permit = tokio::select! {
                acquired = self.inner.requests.clone().acquire_owned() =>
                    acquired.map_err(|_| OperationFailure::new(DemandError::WorkerUnavailable, work))?,
                () = cancellation.cancelled() =>
                    return Err(OperationFailure::new(DemandError::Cancelled, work)),
            };
            let source = self.clone();
            let cancel_on_drop = CancelWorkerOnDrop(CancellationToken::new());
            let worker_cancellation = cancel_on_drop.0.clone();
            let mut worker = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                job(source, worker_cancellation)
            });
            let result = tokio::select! {
                result = &mut worker => result,
                () = cancellation.cancelled() => {
                    cancel_on_drop.0.cancel();
                    worker.await
                },
            };
            result.map_err(|_| OperationFailure::new(DemandError::WorkerUnavailable, work))?
        }

        async fn validate_read_after_work(
            &self,
            relative: PathBuf,
            validation_file: std::fs::File,
            source: SourceReference,
            expected: SourceVersion,
            work: WorkCounters,
            cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            self.run_blocking_after_work(
                work,
                cancellation,
                move |provider, worker_cancellation| {
                    let after = cap_std::fs::File::from_std(validation_file)
                        .metadata()
                        .map_err(|error| OperationFailure::new(error.into(), work))?;
                    if version(&after) != expected
                        || provider
                            .inner
                            .root
                            .symlink_metadata(&relative)
                            .as_ref()
                            .map(version)
                            .ok()
                            != Some(expected)
                    {
                        return Err(OperationFailure::new(DemandError::StaleVersion, work));
                    }
                    provider
                        .check(source, &worker_cancellation)
                        .map_err(|error| OperationFailure::new(error, work))?;
                    Ok(OperationReceipt { value: (), work })
                },
            )
            .await
        }

        /// Invalidates prior source references and directory cursors without
        /// reading any descendant. Watch overflow and broad hints use this.
        pub fn invalidate(&self) -> SourceReference {
            let epoch = self.inner.epoch.fetch_add(1, Ordering::AcqRel) + 1;
            if let Ok(mut cursors) = self.inner.cursors.lock() {
                cursors.clear();
            }
            SourceReference {
                identity: self.inner.identity,
                epoch,
            }
        }

        fn check(
            &self,
            source: SourceReference,
            cancellation: &CancellationToken,
        ) -> Result<(), DemandError> {
            if source != self.reference() {
                return Err(DemandError::StaleSource);
            }
            if cancellation.is_cancelled() {
                return Err(DemandError::Cancelled);
            }
            let current =
                HostRoot::open(&self.inner.path).map_err(|_| DemandError::SourceUnavailable)?;
            if current.identity() != self.inner.root.identity() {
                return Err(DemandError::SourceUnavailable);
            }
            Ok(())
        }

        fn relative(&self, path: &NamespacePath) -> Result<PathBuf, DemandError> {
            let limits = self.inner.limits;
            if path.depth() > usize::from(limits.maximum_path_depth)
                || path.encoded_bytes() > limits.maximum_path_bytes
                || path.components().iter().any(|component| {
                    component.as_bytes().len()
                        > usize::try_from(limits.maximum_component_bytes).unwrap_or(usize::MAX)
                })
            {
                return Err(DemandError::InvalidRequest);
            }
            crate::native_capture::namespace_to_host_path(path)
                .map_err(|_| DemandError::InvalidRequest)
        }

        fn metadata(&self, path: &NamespacePath) -> Result<Option<Metadata>, DemandError> {
            match self.inner.root.symlink_metadata(&self.relative(path)?) {
                Ok(metadata) => Ok(Some(metadata)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
            }
        }

        fn node(metadata: &Metadata) -> SourceNode {
            let kind = node_kind(metadata.file_type());
            SourceNode {
                kind,
                file_identity: file_identity(metadata),
                link_count: link_count(metadata),
                device: device_identity(metadata, kind),
                logical_bytes: (kind == SourceNodeKind::RegularFile
                    || cfg!(unix) && kind == SourceNodeKind::SymbolicLink)
                    .then_some(metadata.len()),
                version: version(metadata),
                metadata: source_metadata(metadata),
            }
        }

        pub(super) fn cursor_lease(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            observed: SourceVersion,
            cursor: Option<SourceCursor>,
        ) -> Result<CursorLease<'_>, DemandError> {
            let restore = cursor.is_some();
            let (token, state) = if let Some(cursor) = cursor {
                if cursor.source != source
                    || cursor.directory != *directory
                    || cursor.directory_version != observed
                {
                    return Err(DemandError::StaleCursor);
                }
                let mut active = self
                    .inner
                    .cursors
                    .lock()
                    .map_err(|_| DemandError::StaleCursor)?;
                let slot = active
                    .get_mut(&cursor.token)
                    .ok_or(DemandError::StaleCursor)?;
                let state = slot.directory.take().ok_or(DemandError::CursorBusy)?;
                (cursor.token, state)
            } else {
                let token = self
                    .inner
                    .next_cursor
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(1)
                    })
                    .map_err(|_| DemandError::StaleCursor)?
                    + 1;
                (token, self.open_directory_state(directory, observed)?)
            };
            Ok(CursorLease {
                source: self,
                reference: source,
                token,
                state: Some(state),
                restore,
            })
        }

        fn open_directory_state(
            &self,
            directory: &NamespacePath,
            observed: SourceVersion,
        ) -> Result<DirectoryState, DemandError> {
            Ok(DirectoryState {
                directory: directory.clone(),
                version: observed,
                entries: self.inner.root.read_dir(&self.relative(directory)?)?,
                pending: None,
                replay: VecDeque::new(),
            })
        }

        fn next_entry(
            &self,
            entry: &cap_std::fs::DirEntry,
        ) -> Result<SourceDirectoryEntry, DemandError> {
            let (encoding, bytes) = crate::native_name::host_name_bytes(
                &entry.file_name(),
                self.inner.profile,
                self.inner.limits.maximum_component_bytes,
            )
            .map_err(|_| DemandError::InvalidRequest)?;
            let name = crate::kernel::LogicalName::new(
                encoding,
                bytes,
                self.inner.limits.maximum_component_bytes,
            )
            .map_err(|_| DemandError::InvalidRequest)?;
            let kind = node_kind(entry.file_type()?);
            Ok(SourceDirectoryEntry { name, kind })
        }

        fn page_entries(
            &self,
            state: &mut DirectoryState,
            maximum_entries: u32,
            cancellation: &CancellationToken,
            work: &mut WorkCounters,
            mut on_entry: impl FnMut(),
        ) -> Result<Vec<SourceDirectoryEntry>, DemandError> {
            let mut entries = Vec::with_capacity(maximum_entries as usize);
            let result = (|| {
                while entries.len() < maximum_entries as usize {
                    if cancellation.is_cancelled() {
                        return Err(DemandError::Cancelled);
                    }
                    if let Some(entry) = state.replay.pop_front() {
                        entries.push(entry);
                        continue;
                    }
                    let next = if let Some(pending) = state.pending.take() {
                        Some(Ok(pending))
                    } else {
                        let next = state.entries.next();
                        if next.is_some() {
                            work.source_entries_visited += 1;
                        }
                        next
                    };
                    let Some(entry) = next else { break };
                    let entry = entry?;
                    match self.next_entry(&entry) {
                        Ok(converted) => entries.push(converted),
                        Err(error) => {
                            state.pending = Some(entry);
                            return Err(error);
                        }
                    }
                    on_entry();
                }
                if state.pending.is_none()
                    && let Some(next) = state.entries.next()
                {
                    state.pending = Some(next?);
                    work.source_entries_visited += 1;
                }
                Ok(())
            })();
            if result.is_err() {
                entries.extend(state.replay.drain(..));
                state.replay = std::mem::take(&mut entries).into();
            }
            result.map(|()| entries)
        }
    }

    fn node_kind(file_type: cap_std::fs::FileType) -> SourceNodeKind {
        if file_type.is_dir() {
            SourceNodeKind::Directory
        } else if file_type.is_file() {
            SourceNodeKind::RegularFile
        } else if file_type.is_symlink() {
            SourceNodeKind::SymbolicLink
        } else {
            special_node_kind(&file_type)
        }
    }

    #[cfg(unix)]
    fn special_node_kind(file_type: &cap_std::fs::FileType) -> SourceNodeKind {
        if file_type.is_fifo() {
            SourceNodeKind::Fifo
        } else if file_type.is_socket() {
            SourceNodeKind::Socket
        } else if file_type.is_char_device() {
            SourceNodeKind::CharacterDevice
        } else if file_type.is_block_device() {
            SourceNodeKind::BlockDevice
        } else {
            SourceNodeKind::Unsupported
        }
    }

    #[cfg(not(unix))]
    fn special_node_kind(_file_type: &cap_std::fs::FileType) -> SourceNodeKind {
        SourceNodeKind::Unsupported
    }

    #[async_trait::async_trait]
    impl DemandSource for NativeDemandSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.inner.identity,
                epoch: self.inner.epoch.load(Ordering::Acquire),
            }
        }

        async fn lookup(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            let path = path.clone();
            self.run_blocking(cancellation, move |provider, request_cancellation| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
                    provider.observe_parent(&path)?;
                    work.source_path_components = path.depth() as u64;
                    let node = provider.metadata(&path)?.as_ref().map(Self::node);
                    provider.check(source, &request_cancellation)?;
                    Ok(node)
                })
            })
            .await
        }

        async fn list_page(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            cancellation: &CancellationToken,
        ) -> DemandResult<SourceDirectoryPage> {
            let directory = directory.clone();
            self.run_blocking(cancellation, move |provider, request_cancellation| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
                    provider.observe_directory(&directory)?;
                    work.source_path_components = directory.depth() as u64;
                    if maximum_entries == 0 || maximum_entries > 4096 {
                        return Err(DemandError::InvalidRequest);
                    }
                    let metadata = provider.metadata(&directory)?.ok_or(DemandError::Absent)?;
                    if !metadata.is_dir() {
                        return Err(DemandError::NotDirectory);
                    }
                    let observed = version(&metadata);
                    let mut lease = provider.cursor_lease(source, &directory, observed, cursor)?;
                    if lease.state()?.directory != directory || lease.state()?.version != observed {
                        lease.discard();
                        return Err(DemandError::StaleCursor);
                    }
                    let entries = provider.page_entries(
                        lease.state()?,
                        maximum_entries,
                        &request_cancellation,
                        work,
                        || {},
                    );
                    if entries.is_err() && !matches!(entries, Err(DemandError::Cancelled)) {
                        lease.abandon();
                    }
                    let entries = entries?;
                    if let Err(error) = provider.check(source, &request_cancellation) {
                        if matches!(error, DemandError::Cancelled) {
                            lease.state()?.replay.extend(entries);
                        } else {
                            lease.abandon();
                        }
                        return Err(error);
                    }
                    let current = provider.metadata(&directory);
                    if current.is_err() {
                        lease.abandon();
                    }
                    if current?.as_ref().map(version) != Some(observed) {
                        lease.discard();
                        return Err(DemandError::StaleVersion);
                    }
                    let next =
                        if lease.state()?.pending.is_some() || !lease.state()?.replay.is_empty() {
                            let token = lease.token;
                            lease.commit()?;
                            Some(SourceCursor {
                                source,
                                directory: directory.clone(),
                                directory_version: observed,
                                token,
                            })
                        } else {
                            lease.discard();
                            None
                        };
                    work.items_returned = entries.len() as u64;
                    Ok(SourceDirectoryPage {
                        entries,
                        next,
                        version: observed,
                    })
                })
            })
            .await
        }

        async fn read_range(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            if cancellation.is_cancelled() {
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            let length = usize::try_from(length)
                .map_err(|_| OperationFailure::before_work(DemandError::InvalidRequest))?;
            if length > 1024 * 1024 || offset.checked_add(length as u64).is_none() {
                return Err(OperationFailure::before_work(DemandError::InvalidRequest));
            }
            let mut work = WorkCounters {
                source_path_components: path.depth() as u64,
                ..WorkCounters::default()
            };
            let path = path.clone();
            let prepared = self
                .run_blocking(cancellation, move |provider, worker_cancellation| {
                    provider
                        .check(source, &worker_cancellation)
                        .map_err(|error| OperationFailure::new(error, work))?;
                    provider
                        .observe_parent(&path)
                        .map_err(|error| OperationFailure::new(error, work))?;
                    let relative = provider
                        .relative(&path)
                        .map_err(|error| OperationFailure::new(error, work))?;
                    let file = match provider.inner.root.open_file(&relative) {
                        Ok(file) => file,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Err(OperationFailure::new(DemandError::Absent, work));
                        }
                        Err(error) => {
                            return Err(OperationFailure::new(error.into(), work));
                        }
                    };
                    let before = file
                        .metadata()
                        .map_err(|error| OperationFailure::new(error.into(), work))?;
                    if !before.is_file() {
                        return Err(OperationFailure::new(DemandError::NotRegularFile, work));
                    }
                    if version(&before) != expected {
                        return Err(OperationFailure::new(DemandError::StaleVersion, work));
                    }
                    let available = before.len().saturating_sub(offset);
                    let length = length.min(usize::try_from(available).unwrap_or(usize::MAX));
                    Ok(OperationReceipt {
                        value: (relative, file.into_std(), length),
                        work,
                    })
                })
                .await?
                .value;
            let (relative, file, length) = prepared;
            let validation_file = file
                .try_clone()
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            let mut read = acyclic_native_runtime::read_batch_async(
                file,
                vec![acyclic_native_runtime::OwnedRead { offset, length }],
            );
            let mut cancelled = false;
            let result = tokio::select! {
                result = &mut read => result,
                () = cancellation.cancelled() => {
                    cancelled = true;
                    read.await
                }
            }
            .map_err(|error| OperationFailure::new(error.into(), work))?;
            let bytes = result
                .into_iter()
                .next()
                .ok_or_else(|| OperationFailure::new(DemandError::SourceUnavailable, work))?;
            work.source_bytes_read = bytes.len() as u64;
            if cancelled {
                return Err(OperationFailure::new(DemandError::Cancelled, work));
            }
            self.validate_read_after_work(
                relative,
                validation_file,
                source,
                expected,
                work,
                cancellation,
            )
            .await?;
            work.output_bytes = bytes.len() as u64;
            Ok(OperationReceipt { value: bytes, work })
        }

        async fn read_link(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            let path = path.clone();
            self.run_blocking(cancellation, move |provider, request_cancellation| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
                    provider.observe_parent(&path)?;
                    work.source_path_components = path.depth() as u64;
                    let before = provider.metadata(&path)?.ok_or(DemandError::Absent)?;
                    if !before.file_type().is_symlink() {
                        return Err(DemandError::InvalidRequest);
                    }
                    if version(&before) != expected {
                        return Err(DemandError::StaleVersion);
                    }
                    let target = provider.inner.root.read_link(&provider.relative(&path)?)?;
                    let bytes = os_string_bytes(target.as_os_str());
                    work.source_bytes_read = bytes.len() as u64;
                    work.output_bytes = bytes.len() as u64;
                    let after = provider.metadata(&path)?.ok_or(DemandError::Absent)?;
                    if version(&after) != expected {
                        return Err(DemandError::StaleVersion);
                    }
                    provider.check(source, &request_cancellation)?;
                    Ok(Bytes::from(bytes))
                })
            })
            .await
        }
    }

    #[cfg(unix)]
    fn os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt as _;
        value.as_bytes().to_vec()
    }

    #[cfg(windows)]
    fn os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
        use std::os::windows::ffi::OsStrExt as _;
        value.encode_wide().flat_map(u16::to_le_bytes).collect()
    }

    fn version(metadata: &Metadata) -> SourceVersion {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-version-v1\0");
        hash.update(&metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            hash.update(&metadata.dev().to_le_bytes());
            hash.update(&metadata.ino().to_le_bytes());
            hash.update(&metadata.mtime().to_le_bytes());
            hash.update(&metadata.mtime_nsec().to_le_bytes());
            hash.update(&metadata.ctime().to_le_bytes());
            hash.update(&metadata.ctime_nsec().to_le_bytes());
        }
        #[cfg(windows)]
        {
            hash.update(&metadata.file_attributes().to_le_bytes());
            hash.update(&metadata.last_write_time().to_le_bytes());
            hash.update(&metadata.creation_time().to_le_bytes());
            hash.update(
                &cap_primitives::fs::_WindowsByHandle::volume_serial_number(metadata)
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
            hash.update(
                &cap_primitives::fs::_WindowsByHandle::file_index(metadata)
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
        }
        SourceVersion(*hash.finalize().as_bytes())
    }

    fn file_identity(metadata: &Metadata) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-v1\0");
        #[cfg(unix)]
        {
            hash.update(&metadata.dev().to_le_bytes());
            hash.update(&metadata.ino().to_le_bytes());
        }
        #[cfg(windows)]
        {
            hash.update(
                &cap_primitives::fs::_WindowsByHandle::volume_serial_number(metadata)
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
            hash.update(
                &cap_primitives::fs::_WindowsByHandle::file_index(metadata)
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
        }
        *hash.finalize().as_bytes()
    }

    fn link_count(metadata: &Metadata) -> Option<u64> {
        #[cfg(unix)]
        {
            Some(metadata.nlink())
        }
        #[cfg(windows)]
        {
            cap_primitives::fs::_WindowsByHandle::number_of_links(metadata).map(u64::from)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = metadata;
            None
        }
    }

    #[allow(
        clippy::useless_conversion,
        reason = "dev_t and major/minor result widths differ across Unix targets"
    )]
    fn device_identity(metadata: &Metadata, kind: SourceNodeKind) -> Option<(u32, u32)> {
        #[cfg(unix)]
        {
            if !matches!(
                kind,
                SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice
            ) {
                return None;
            }
            let raw: libc::dev_t = metadata.rdev().try_into().ok()?;
            Some((
                libc::major(raw).try_into().ok()?,
                libc::minor(raw).try_into().ok()?,
            ))
        }
        #[cfg(not(unix))]
        {
            let _ = (metadata, kind);
            None
        }
    }

    fn source_metadata(metadata: &Metadata) -> SourceMetadata {
        let mut result = SourceMetadata {
            created_ns: metadata.created().ok().and_then(system_time_nanos),
            modified_ns: metadata.modified().ok().and_then(system_time_nanos),
            accessed_ns: metadata.accessed().ok().and_then(system_time_nanos),
            ..SourceMetadata::default()
        };
        #[cfg(unix)]
        {
            result.posix_mode = Some(metadata.mode());
            result.posix_uid = Some(metadata.uid());
            result.posix_gid = Some(metadata.gid());
            result.changed_ns = Some(
                i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()),
            );
        }
        #[cfg(windows)]
        {
            result.windows_attributes = Some(metadata.file_attributes());
        }
        result
    }

    fn system_time_nanos(time: cap_std::time::SystemTime) -> Option<i128> {
        match time.into_std().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => Some(
                i128::from(duration.as_secs()) * 1_000_000_000
                    + i128::from(duration.subsec_nanos()),
            ),
            Err(error) => Some(
                -(i128::from(error.duration().as_secs()) * 1_000_000_000
                    + i128::from(error.duration().subsec_nanos())),
            ),
        }
    }
}

#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests {
    use super::native::NativeDemandSource;
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingObserver {
        directories: Mutex<Vec<NamespacePath>>,
    }

    impl DemandDirectoryObserver for RecordingObserver {
        fn observe_directory(&self, directory: &NamespacePath) -> Result<(), DemandError> {
            self.directories
                .lock()
                .map_err(|_| DemandError::SourceUnavailable)?
                .push(directory.clone());
            Ok(())
        }
    }

    fn path(value: &str) -> Result<NamespacePath, Box<dyn Error>> {
        let portable = crate::path::PortablePath::parse(value, VolumeLimits::default())?;
        Ok(NamespacePath::from_portable(
            &portable,
            VolumeLimits::default(),
        )?)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_source_reports_symbolic_link_target_bytes() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::os::unix::fs::symlink("target", root.path().join("link"))?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Posix,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source
            .lookup(reference, &path("/link")?, &cancellation)
            .await?
            .value
            .ok_or("symbolic link was absent")?;

        assert_eq!(node.kind, SourceNodeKind::SymbolicLink);
        assert_eq!(node.logical_bytes, Some(6));
        assert_eq!(
            source
                .read_link(reference, &path("/link")?, node.version, &cancellation)
                .await?
                .value
                .as_ref(),
            b"target"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_source_observes_only_the_demanded_directory_before_reading()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("nested"))?;
        std::fs::write(root.path().join("nested/file"), b"content")?;
        let observer = Arc::new(RecordingObserver::default());
        let source = NativeDemandSource::open_with_observer(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            observer.clone(),
        )
        .await?;
        let reference = source.reference();
        source
            .lookup(reference, &path("/nested/file")?, &CancellationToken::new())
            .await?;
        assert_eq!(
            observer
                .directories
                .lock()
                .map_err(|_| "observer state poisoned")?
                .as_slice(),
            &[path("/nested")?]
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_provider_reads_do_not_visit_unrelated_content() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("forbidden"))?;
        std::fs::write(
            root.path().join("forbidden").join("unrelated"),
            vec![7; 8 * 1024 * 1024],
        )?;
        let mut body = vec![b'0'; 1024 * 1024];
        body[3..7].copy_from_slice(b"3456");
        std::fs::write(root.path().join("seen"), body)?;

        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let provider: &dyn DemandSource = &source;
        let reference = provider.reference();
        let cancellation = CancellationToken::new();
        let node = provider
            .lookup(reference, &path("/seen")?, &cancellation)
            .await?;
        assert_eq!(node.work.source_path_components, 1);
        assert_eq!(node.work.source_entries_visited, 0);
        assert_eq!(node.work.source_bytes_read, 0);
        let version = node.value.ok_or("seen path was absent")?.version;

        let bytes = provider
            .read_range(reference, &path("/seen")?, version, 3, 4, &cancellation)
            .await?;
        assert_eq!(bytes.value.as_ref(), b"3456");
        assert_eq!(bytes.work.source_bytes_read, 4);
        assert_eq!(bytes.work.source_entries_visited, 0);

        let first = provider
            .list_page(reference, &path("/")?, None, 1, &cancellation)
            .await?;
        assert_eq!(first.value.entries.len(), 1);
        assert!(first.work.source_entries_visited <= 2);
        let continuation = first.value.next.ok_or("second root entry missing")?;
        let second = provider
            .list_page(reference, &path("/")?, Some(continuation), 1, &cancellation)
            .await?;
        assert_eq!(second.value.entries.len(), 1);
        assert!(second.work.source_entries_visited <= 1);
        Ok(())
    }

    #[tokio::test]
    async fn native_requests_wait_for_capacity_and_cancel_before_work() -> Result<(), Box<dyn Error>>
    {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let permits = source.occupy_all_requests().await?;
        let cancellation = CancellationToken::new();
        let root_path = path("/")?;
        let query = source.lookup(source.reference(), &root_path, &cancellation);
        tokio::pin!(query);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut query)
                .await
                .is_err()
        );
        cancellation.cancel();
        let Err(failure) =
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut query).await?
        else {
            return Err("cancelled request entered the worker".into());
        };
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
        drop(permits);
        Ok(())
    }

    #[tokio::test]
    async fn post_read_validation_cancels_with_exact_prior_work() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let permits = source.occupy_all_requests().await?;
        let cancellation = CancellationToken::new();
        let work = WorkCounters {
            source_path_components: 2,
            source_bytes_read: 17,
            ..WorkCounters::default()
        };
        let validation = source.validate_after_work_for_test(work, &cancellation);
        tokio::pin!(validation);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut validation)
                .await
                .is_err()
        );
        cancellation.cancel();
        let failure = tokio::time::timeout(std::time::Duration::from_secs(1), &mut validation)
            .await?
            .err()
            .ok_or("cancelled validation unexpectedly succeeded")?;
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, work);
        drop(permits);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_request_cancels_admitted_worker() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            source
                .wait_for_cancelled_worker(started_tx, stopped_tx)
                .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx).await??;
        request.abort();
        let _ = request.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), stopped_rx).await??;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_directory_page_retries_without_skipping_entries()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "c", "d", "e", "f"] {
            std::fs::write(root.path().join(name), b"x")?;
        }
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let directory = path("/")?;
        let live = CancellationToken::new();
        let first = source
            .list_page(reference, &directory, None, 1, &live)
            .await?;
        let cursor = first.value.next.ok_or("expected continuation")?;
        let interrupted = CancellationToken::new();
        source.cancel_during_page(reference, &directory, cursor.clone(), &interrupted)?;
        let mut found = first.value.entries;
        let mut next = Some(cursor);
        while let Some(cursor) = next {
            let page = source
                .list_page(reference, &directory, Some(cursor), 2, &live)
                .await?;
            found.extend(page.value.entries);
            next = page.value.next;
        }
        let mut names: Vec<_> = found
            .into_iter()
            .map(|entry| entry.name.as_bytes().to_vec())
            .collect();
        names.sort();
        assert_eq!(names.len(), 6);
        assert_eq!(names.first().map(Vec::as_slice), Some(&b"a"[..]));
        assert_eq!(names.last().map(Vec::as_slice), Some(&b"f"[..]));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_use_reports_busy_without_losing_cursor() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "c"] {
            std::fs::write(root.path().join(name), b"x")?;
        }
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let directory = path("/")?;
        let cancellation = CancellationToken::new();
        let first = source
            .list_page(reference, &directory, None, 1, &cancellation)
            .await?;
        let cursor = first.value.next.ok_or("expected continuation")?;
        let lease = source.cursor_lease(
            reference,
            &directory,
            first.value.version,
            Some(cursor.clone()),
        )?;
        assert!(matches!(
            source.cursor_lease(
                reference,
                &directory,
                first.value.version,
                Some(cursor.clone())
            ),
            Err(DemandError::CursorBusy)
        ));
        drop(lease);
        let second = source
            .list_page(reference, &directory, Some(cursor), 1, &cancellation)
            .await?;
        assert_eq!(second.value.entries.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn version_and_cursor_evidence_fail_closed() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("directory"))?;
        std::fs::write(root.path().join("file"), b"old")?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source
            .lookup(reference, &path("/file")?, &cancellation)
            .await?
            .value
            .ok_or("file absent")?;
        std::fs::write(root.path().join("file"), b"newer contents")?;
        let Err(stale) = source
            .read_range(
                reference,
                &path("/file")?,
                node.version,
                0,
                3,
                &cancellation,
            )
            .await
        else {
            return Err("stale source content was accepted".into());
        };
        assert!(matches!(stale.error, DemandError::StaleVersion));
        assert_eq!(stale.work.source_path_components, 1);
        assert_eq!(stale.work.source_bytes_read, 0);
        assert!(matches!(
            source
                .read_metadata(reference, &path("/file")?, Some(node.version), &cancellation)
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleVersion)
        ));

        let first = source
            .list_page(reference, &path("/")?, None, 1, &cancellation)
            .await?;
        let cursor = first.value.next.ok_or("second root entry missing")?;
        assert!(matches!(
            source
                .list_page(
                    reference,
                    &path("/directory")?,
                    Some(cursor),
                    1,
                    &cancellation
                )
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleCursor)
        ));
        source.invalidate();
        assert!(matches!(
            source
                .lookup(reference, &path("/file")?, &cancellation)
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleSource)
        ));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn native_lookup_preserves_non_utf8_posix_names() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};
        use std::os::unix::ffi::OsStringExt;

        let raw = vec![b'n', 0xff];
        let name = LogicalName::new(
            NameEncoding::PosixBytes,
            raw.clone(),
            VolumeLimits::default().maximum_component_bytes,
        )?;
        assert_lossless_name(
            FilesystemProfile::Posix,
            std::ffi::OsString::from_vec(raw),
            name,
        )
        .await
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_lookup_preserves_utf16_windows_names() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let raw = "snowman-☃"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let name = LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            raw,
            VolumeLimits::default().maximum_component_bytes,
        )?;
        assert_lossless_name(
            FilesystemProfile::Windows,
            std::ffi::OsString::from("snowman-☃"),
            name,
        )
        .await
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_lookup_accepts_windows_names_longer_than_127_code_units()
    -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let host_name = "x".repeat(200);
        let limits = crate::model::VolumeConfig::native(crate::model::Lifecycle::Durable).limits;
        let raw = host_name
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let name = LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            raw,
            limits.maximum_component_bytes,
        )?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join(&host_name), b"data")?;
        let source =
            NativeDemandSource::open(root.path(), FilesystemProfile::Windows, limits).await?;
        let exact = NamespacePath::new(vec![name], limits)?;
        assert!(
            source
                .lookup(source.reference(), &exact, &CancellationToken::new())
                .await?
                .value
                .is_some()
        );
        Ok(())
    }

    #[cfg(any(target_os = "linux", windows))]
    async fn assert_lossless_name(
        profile: FilesystemProfile,
        host_name: std::ffi::OsString,
        name: crate::kernel::LogicalName,
    ) -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join(host_name), b"data")?;
        let source =
            NativeDemandSource::open(root.path(), profile, VolumeLimits::default()).await?;
        let exact = NamespacePath::new(vec![name], VolumeLimits::default())?;
        let node = source
            .lookup(source.reference(), &exact, &CancellationToken::new())
            .await?
            .value
            .ok_or("lossless file absent")?;
        let answer = source
            .read_range(
                source.reference(),
                &exact,
                node.version,
                0,
                4,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(answer.value.as_ref(), b"data");
        Ok(())
    }
}
