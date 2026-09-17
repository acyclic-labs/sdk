//! Bounded, versioned access to unresolved filesystem sources.
//!
//! A source reference identifies a provider capability. It never contains a
//! host path or provider credential. Resolving one request does not publish a
//! workspace generation or make the result part of recorded history.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
use bytes::Bytes;
use thiserror::Error;

/// Opaque identity and invalidation epoch of an attached source.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SourceReference {
    /// Provider-scoped opaque identity; no local path is encoded here.
    pub identity: [u8; 16],
    /// Incremented when source knowledge is invalidated.
    pub epoch: u64,
}

/// Evidence for the exact node observed by a source request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SourceVersion(pub [u8; 32]);

/// The kind of an observed source node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceNodeKind {
    /// Directory, whose children remain unresolved until requested.
    Directory,
    /// Regular file, whose content ranges remain unresolved until requested.
    RegularFile,
    /// Symbolic link, never followed by the source provider.
    SymbolicLink,
    /// Another native node kind.
    Other,
}

/// Scalar host metadata. `None` means the provider did not make that fact
/// available; it does not mean a numeric zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceMetadata {
    /// POSIX permission and kind bits, when available.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner, when available.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group, when available.
    pub posix_gid: Option<u32>,
    /// Windows file attributes, when available.
    pub windows_attributes: Option<u32>,
    /// Last content modification time in signed Unix nanoseconds.
    pub modified_ns: Option<i128>,
}

/// Metadata returned by an exact path lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceNode {
    /// Source node kind.
    pub kind: SourceNodeKind,
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
}

impl From<NamespacePathError> for DemandError {
    fn from(_: NamespacePathError) -> Self {
        Self::InvalidRequest
    }
}

/// Demand result with source-work counters on both success and failure.
pub type DemandResult<T> = Result<OperationReceipt<T>, OperationFailure<DemandError>>;

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

/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native {
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::native_host::HostRoot;
    use cap_std::fs::{Metadata, MetadataExt, ReadDir};
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    struct DirectoryState {
        directory: NamespacePath,
        version: SourceVersion,
        entries: ReadDir,
        pending: Option<cap_std::fs::DirEntry>,
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
        cursors: Mutex<BTreeMap<u64, DirectoryState>>,
        next_cursor: AtomicU64,
    }

    impl NativeDemandSource {
        /// Opens and identifies only the root directory off the async executor.
        pub async fn open(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
        ) -> Result<Self, DemandError> {
            let path = path.as_ref().to_path_buf();
            tokio::task::spawn_blocking(move || Self::open_blocking(path, profile, limits))
                .await
                .map_err(|_| DemandError::WorkerUnavailable)?
        }

        fn open_blocking(
            path: PathBuf,
            profile: FilesystemProfile,
            limits: VolumeLimits,
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
                    identity: *uuid::Uuid::new_v4().as_bytes(),
                    epoch: AtomicU64::new(0),
                    profile,
                    limits,
                    cursors: Mutex::new(BTreeMap::new()),
                    next_cursor: AtomicU64::new(0),
                }),
            })
        }

        async fn run_blocking<T: Send + 'static>(
            &self,
            cancellation: &CancellationToken,
            job: impl FnOnce(Self) -> DemandResult<T> + Send + 'static,
        ) -> DemandResult<T> {
            if cancellation.is_cancelled() {
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            let source = self.clone();
            tokio::task::spawn_blocking(move || job(source))
                .await
                .map_err(|_| OperationFailure::before_work(DemandError::WorkerUnavailable))?
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
            crate::native_capture::relative_host_path(path).map_err(|_| DemandError::InvalidRequest)
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
                logical_bytes: (kind == SourceNodeKind::RegularFile).then_some(metadata.len()),
                version: version(metadata),
                metadata: source_metadata(metadata),
            }
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
        ) -> Result<Vec<SourceDirectoryEntry>, DemandError> {
            let mut entries = Vec::with_capacity(maximum_entries as usize);
            while entries.len() < maximum_entries as usize {
                if cancellation.is_cancelled() {
                    return Err(DemandError::Cancelled);
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
                entries.push(self.next_entry(&entry?)?);
            }
            if state.pending.is_none()
                && let Some(next) = state.entries.next()
            {
                state.pending = Some(next?);
                work.source_entries_visited += 1;
            }
            Ok(entries)
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
            SourceNodeKind::Other
        }
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
            let request_cancellation = cancellation.clone();
            self.run_blocking(cancellation, move |provider| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
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
            let request_cancellation = cancellation.clone();
            self.run_blocking(cancellation, move |provider| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
                    work.source_path_components = directory.depth() as u64;
                    if maximum_entries == 0 || maximum_entries > 4096 {
                        return Err(DemandError::InvalidRequest);
                    }
                    let metadata = provider.metadata(&directory)?.ok_or(DemandError::Absent)?;
                    if !metadata.is_dir() {
                        return Err(DemandError::NotDirectory);
                    }
                    let observed = version(&metadata);
                    let (token, mut state) = if let Some(cursor) = cursor {
                        if cursor.source != source
                            || cursor.directory != directory
                            || cursor.directory_version != observed
                        {
                            return Err(DemandError::StaleCursor);
                        }
                        let state = provider
                            .inner
                            .cursors
                            .lock()
                            .map_err(|_| DemandError::StaleCursor)?
                            .remove(&cursor.token)
                            .ok_or(DemandError::StaleCursor)?;
                        (cursor.token, state)
                    } else {
                        let token = provider
                            .inner
                            .next_cursor
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                                current.checked_add(1)
                            })
                            .map_err(|_| DemandError::StaleCursor)?
                            + 1;
                        (
                            token,
                            DirectoryState {
                                directory: directory.clone(),
                                version: observed,
                                entries: provider
                                    .inner
                                    .root
                                    .read_dir(&provider.relative(&directory)?)?,
                                pending: None,
                            },
                        )
                    };
                    if state.directory != directory || state.version != observed {
                        return Err(DemandError::StaleCursor);
                    }
                    let entries = provider.page_entries(
                        &mut state,
                        maximum_entries,
                        &request_cancellation,
                        work,
                    )?;
                    provider.check(source, &request_cancellation)?;
                    if provider.metadata(&directory)?.as_ref().map(version) != Some(observed) {
                        return Err(DemandError::StaleVersion);
                    }
                    let next = if state.pending.is_some() {
                        let mut active = provider
                            .inner
                            .cursors
                            .lock()
                            .map_err(|_| DemandError::StaleCursor)?;
                        if source != provider.reference() {
                            return Err(DemandError::StaleSource);
                        }
                        if active.len() >= 1024
                            && let Some(oldest) = active.keys().next().copied()
                        {
                            active.remove(&oldest);
                        }
                        active.insert(token, state);
                        Some(SourceCursor {
                            source,
                            directory: directory.clone(),
                            directory_version: observed,
                            token,
                        })
                    } else {
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
            let path = path.clone();
            let request_cancellation = cancellation.clone();
            self.run_blocking(cancellation, move |provider| {
                measured(|work| {
                    provider.check(source, &request_cancellation)?;
                    let length =
                        usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?;
                    if length > 1024 * 1024 || offset.checked_add(length as u64).is_none() {
                        return Err(DemandError::InvalidRequest);
                    }
                    let relative = provider.relative(&path)?;
                    work.source_path_components = path.depth() as u64;
                    let file = match provider.inner.root.open_file(&relative) {
                        Ok(file) => file,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Err(DemandError::Absent);
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let before = file.metadata()?;
                    if !before.is_file() {
                        return Err(DemandError::NotRegularFile);
                    }
                    if version(&before) != expected {
                        return Err(DemandError::StaleVersion);
                    }
                    let available = before.len().saturating_sub(offset);
                    let length = length.min(usize::try_from(available).unwrap_or(usize::MAX));
                    let file = file.into_std();
                    let mut bytes = vec![0; length];
                    let mut count = 0;
                    while count < length {
                        if request_cancellation.is_cancelled() {
                            return Err(DemandError::Cancelled);
                        }
                        let remaining =
                            bytes.get_mut(count..).ok_or(DemandError::InvalidRequest)?;
                        let read = read_at(&file, remaining, offset + count as u64)?;
                        if read == 0 {
                            break;
                        }
                        count += read;
                        work.source_bytes_read += read as u64;
                    }
                    bytes.truncate(count);
                    if request_cancellation.is_cancelled() {
                        return Err(DemandError::Cancelled);
                    }
                    if version(&cap_std::fs::File::from_std(file).metadata()?) != expected
                        || provider
                            .inner
                            .root
                            .symlink_metadata(&relative)
                            .as_ref()
                            .map(version)
                            .ok()
                            != Some(expected)
                    {
                        return Err(DemandError::StaleVersion);
                    }
                    provider.check(source, &request_cancellation)?;
                    work.output_bytes = count as u64;
                    Ok(Bytes::from(bytes))
                })
            })
            .await
        }
    }

    #[cfg(unix)]
    fn read_at(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        file.read_at(bytes, offset)
    }

    #[cfg(windows)]
    fn read_at(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        file.seek_read(bytes, offset)
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

    fn source_metadata(metadata: &Metadata) -> SourceMetadata {
        let mut result = SourceMetadata {
            modified_ns: metadata.modified().ok().and_then(|time| {
                let duration = time.into_std().duration_since(std::time::UNIX_EPOCH).ok()?;
                Some(
                    i128::from(duration.as_secs()) * 1_000_000_000
                        + i128::from(duration.subsec_nanos()),
                )
            }),
            ..SourceMetadata::default()
        };
        #[cfg(unix)]
        {
            result.posix_mode = Some(metadata.mode());
            result.posix_uid = Some(metadata.uid());
            result.posix_gid = Some(metadata.gid());
        }
        #[cfg(windows)]
        {
            result.windows_attributes = Some(metadata.file_attributes());
        }
        result
    }
}

#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests {
    use super::native::NativeDemandSource;
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use std::error::Error;

    fn path(value: &str) -> Result<NamespacePath, Box<dyn Error>> {
        let portable = crate::path::PortablePath::parse(value, VolumeLimits::default())?;
        Ok(NamespacePath::from_portable(
            &portable,
            VolumeLimits::default(),
        )?)
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

    #[cfg(unix)]
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
