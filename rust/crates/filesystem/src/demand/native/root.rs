struct DirectoryState {
    directory: NamespacePath,
    version: SourceVersion,
    entries: HostStatReader,
    pending: Option<HostListedEntry>,
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
    /// Whether a test let this source be watched, so tests cover the path
    /// taken by roots the host cannot watch.
    #[cfg(test)]
    watchable: bool,
}

struct NativeDemandInner {
    path: PathBuf,
    root: HostRoot,
    /// Whether host I/O beneath the root may block a native callback
    /// thread: only on a local filesystem, which cannot stall it past
    /// the callback's timeout.
    inline: bool,
    identity: [u8; 16],
    epoch: AtomicU64,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    cursors: Mutex<BTreeMap<u64, CursorState>>,
    next_cursor: AtomicU64,
    requests: Arc<Semaphore>,
    observer: Option<Arc<dyn DemandDirectoryObserver>>,
    /// Live change watches: on Linux, inotify admits directories to them
    /// one by one; on Windows, each holds the root open (see
    /// [`Self::root_named_while_watched`]).
    #[cfg(any(target_os = "linux", windows))]
    watches: std::sync::RwLock<Vec<std::sync::Weak<crate::source_watch::NativeSourceWatch>>>,
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
        tokio::task::spawn_blocking(crate::obs::in_context(
            crate::obs::caller_context(),
            move || Self::open_blocking(path, profile, limits, reference, observer),
        ))
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
                inline: root.is_local(),
                root,
                identity: reference.identity,
                epoch: AtomicU64::new(reference.epoch),
                profile,
                limits,
                cursors: Mutex::new(BTreeMap::new()),
                next_cursor: AtomicU64::new(0),
                requests: Arc::new(Semaphore::new(MAXIMUM_NATIVE_REQUESTS)),
                observer,
                #[cfg(any(target_os = "linux", windows))]
                watches: std::sync::RwLock::new(Vec::new()),
            }),
            #[cfg(test)]
            watchable: true,
        })
    }

    /// This source, reporting no changes as a root the host cannot watch
    /// would. The live tests that run both ways mount through FUSE or NFS.
    #[cfg(all(test, unix, feature = "native-mount"))]
    pub(crate) fn unwatched(self) -> Self {
        Self {
            watchable: false,
            ..self
        }
    }

    /// This source as if its root were not local, so tests cover the
    /// path host I/O beneath a remote root takes.
    #[cfg(test)]
    pub(crate) fn remote(mut self) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.inline = false;
        }
        self
    }

    fn observe_directory(&self, directory: &NamespacePath) -> Result<(), DemandError> {
        self.admit_to_watches(directory)?;
        self.inner
            .observer
            .as_ref()
            .map_or(Ok(()), |observer| observer.observe_directory(directory))
    }

    /// Watches `directory`, and every directory on the way to it, before
    /// anything beneath it is read; returns whether a watch began
    /// watching `directory` itself only now, after which a fact about it
    /// read before must be read again.
    #[cfg(target_os = "linux")]
    fn admit_to_watches(&self, directory: &NamespacePath) -> Result<bool, DemandError> {
        let live = self
            .inner
            .watches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect::<Vec<_>>();
        if live.is_empty() {
            return Ok(false);
        }
        let relative = self.relative(directory)?;
        // Every watch admits it, not just the first that does.
        let mut added = false;
        for watch in &live {
            added |= watch.admit(&relative);
        }
        Ok(added)
    }

    /// One watch covers the whole root on every other host.
    #[cfg(not(target_os = "linux"))]
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the Linux variant admits directories to inotify watches and can fail; this one keeps its signature"
    )]
    fn admit_to_watches(&self, _directory: &NamespacePath) -> Result<bool, DemandError> {
        Ok(false)
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
        // A native callback thread blocks only its own request: run a
        // local job there rather than hop to a pooled worker and back.
        // A remote one stays on a worker, where the callback's timeout
        // still bounds it.
        if self.inner.inline && acyclic_native_runtime::inline_blocking_allowed() {
            return job(self.clone(), cancellation.clone());
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
        let mut worker = tokio::task::spawn_blocking(crate::obs::in_context(
            crate::obs::caller_context(),
            move || {
                let _permit = permit;
                job(source, worker_cancellation)
            },
        ));
        let result = tokio::select! {
            result = &mut worker => result,
            () = cancellation.cancelled() => {
                cancel_on_drop.0.cancel();
                worker.await
            },
        };
        result.map_err(|_| OperationFailure::before_work(DemandError::WorkerUnavailable))?
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
        if self.root_named_while_watched() {
            return Ok(());
        }
        // The held root answers for the source only while its path still
        // names it. Comparing identities never reopens the root.
        if crate::NativeRootIdentity::of_root_path(&self.inner.path).ok()
            != Some(self.inner.root.identity())
        {
            return Err(DemandError::SourceUnavailable);
        }
        Ok(())
    }

    /// Whether a live watch proves the root's path still names the held
    /// root. A Windows watch holds the root open without sharing its
    /// deletion, so while it lives neither the root nor any ancestor can
    /// be renamed or removed.
    #[cfg(windows)]
    fn root_named_while_watched(&self) -> bool {
        self.inner
            .watches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|watch| watch.strong_count() != 0)
    }

    /// Watches elsewhere report a moved root rather than prevent it.
    #[cfg(not(windows))]
    #[allow(
        clippy::unused_self,
        reason = "the Windows variant reads the live watch set; this one keeps its signature"
    )]
    const fn root_named_while_watched(&self) -> bool {
        false
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
        crate::native_capture::namespace_to_host_path(path).map_err(|_| DemandError::InvalidRequest)
    }

    fn metadata(&self, path: &NamespacePath) -> Result<Option<HostStat>, DemandError> {
        match self.inner.root.stat(&self.relative(path)?) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn node(metadata: &HostStat) -> SourceNode {
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
            entries: self.inner.root.read_dir_stats(&self.relative(directory)?)?,
            pending: None,
            replay: VecDeque::new(),
        })
    }

    /// Converts one enumerated entry into the node a lookup reports,
    /// stat'ing the name only where enumeration could not report it.
    /// `None` means the entry vanished after enumeration.
    fn next_entry(
        &self,
        directory: &Path,
        entry: &HostListedEntry,
    ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
        let file_name = &entry.name;
        let (encoding, bytes) = crate::native_name::host_name_bytes(
            file_name,
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
        let node = match &entry.stat {
            Some(stat) => Self::node(stat),
            None => match self.inner.root.stat(&directory.join(file_name)) {
                Ok(stat) => Self::node(&stat),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            },
        };
        Ok(Some(SourceDirectoryEntry { name, node }))
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
            let directory = self.relative(&state.directory)?;
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
                match self.next_entry(&directory, &entry) {
                    Ok(converted) => entries.extend(converted),
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
