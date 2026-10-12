struct DirectoryState {
    directory: NamespacePath,
    version: SourceVersion,
    entries: HostStatReader,
    ownership: SourcePayloadOwnership,
    pending: Option<HostListedEntry>,
    replay: VecDeque<SourceDirectoryEntry>,
}

pub(super) struct CursorLease {
    source: NativeDemandSource,
    reference: SourceReference,
    token: u64,
    state: Option<DirectoryState>,
    restore: bool,
}

impl CursorLease {
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
            *slot = Some(self.take()?);
            self.restore = false;
        } else {
            if active.len() >= 1024
                && let Some(oldest) = active.keys().next().copied()
            {
                active.remove(&oldest);
            }
            active.insert(self.token, Some(self.take()?));
        }
        Ok(())
    }
}

impl Drop for CursorLease {
    fn drop(&mut self) {
        if !self.restore {
            return;
        }
        if let Ok(mut active) = self.source.inner.cursors.lock()
            && self.source.reference() == self.reference
            && let Some(slot) = active.get_mut(&self.token)
        {
            if let Some(state) = self.state.take() {
                *slot = Some(state);
            } else {
                active.remove(&self.token);
            }
        }
    }
}

/// One native root. Opening it does not enumerate any descendants.
#[derive(Clone)]
pub struct NativeDemandSource {
    inner: Arc<NativeDemandInner>,
    /// Whether a test let this source be watched, so tests cover the path
    #[cfg(test)]
    watchable: bool,
}

struct NativeDemandInner {
    path: PathBuf,
    root: HostRoot,
    /// Whether host I/O beneath the root may block a native callback
    inline: bool,
    identity: [u8; 16],
    epoch: AtomicU64,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    cursors: Mutex<BTreeMap<u64, Option<DirectoryState>>>,
    next_cursor: AtomicU64,
    requests: Arc<Semaphore>,
    observer: Option<Arc<dyn DemandDirectoryObserver>>,
    /// Live change watches: on Linux, inotify admits directories to them
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
        let path = NamespacePath::new(Vec::new(), self.inner.limits).expect("root path");
        let cancellation = CancellationToken::new();
        let _ = self
            .request(
                &path,
                WorkBudget::UNBOUNDED,
                &cancellation,
                move |_, _, context| {
                    let _ = started.send(());
                    while context.check().is_ok() {
                        std::thread::yield_now();
                    }
                    let _ = stopped.send(());
                    Ok(())
                },
            )
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

    /// Opens only the root, with its optional durable identity and observer.
    pub async fn open(
        path: impl AsRef<Path>,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        reference: Option<SourceReference>,
        observer: Option<Arc<dyn DemandDirectoryObserver>>,
    ) -> Result<Self, DemandError> {
        let path = path.as_ref().to_path_buf();
        let reference = reference.unwrap_or_else(|| SourceReference {
            identity: *uuid::Uuid::new_v4().as_bytes(),
            epoch: 0,
        });
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
    #[cfg(all(test, unix, feature = "native-mount"))]
    pub(crate) fn unwatched(self) -> Self {
        Self {
            watchable: false,
            ..self
        }
    }

    /// This source as if its root were not local, so tests cover the
    #[cfg(test)]
    pub(crate) fn remote(mut self) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.inline = false;
        }
        self
    }

    fn observe_directory(
        &self,
        directory: &NamespacePath,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        self.admit_to_watches(directory, context)?;
        self.inner.observer.as_ref().map_or(Ok(()), |observer| {
            if let Some(original) = context.native_source_work() {
                return observer.observe_directory_with_native_work(directory, original);
            }
            let cancellation = context.with_original_leaf(|work| work.cancellation().clone());
            let answer = observer.observe_directory(directory, context.remaining()?, &cancellation);
            let (result, completed) = match answer {
                Ok(receipt) => (Ok(()), receipt.work),
                Err(failure) => (Err(failure.error), *failure.work),
            };
            context.merge(completed)?;
            result
        })
    }

    /// Watches `directory`, and every directory on the way to it, before
    fn admit_to_watches(
        &self,
        directory: &NamespacePath,
        context: &mut CallbackWork,
    ) -> Result<bool, DemandError> {
        context.check()?;
        #[cfg(target_os = "linux")]
        {
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
            let (relative, allocation) = self.relative(directory, context)?;
            let mut added = false;
            for watch in &live {
                added |= watch.admit_with_work(&relative, context)?;
            }
            drop(relative);
            context.release(allocation)?;
            Ok(added)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = directory;
            Ok(false)
        }
    }

    fn observe_parent(
        &self,
        path: &NamespacePath,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        let (parent, allocation) = account_source(context, |accounting, cancellation| {
            path.prefix_with_accounting(
                path.depth().saturating_sub(1),
                self.inner.limits,
                accounting,
                cancellation,
            )
        })?;
        let result = self.observe_directory(&parent, context);
        let bytes = parent.retained_bytes();
        drop(parent);
        account_source(context, |accounting, _| {
            crate::kernel::retire_allocation(allocation, bytes, accounting)
        })?;
        result
    }

    /// Invalidates prior source references and directory cursors without
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
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        context.check()?;
        if source != self.reference() {
            return Err(DemandError::StaleSource);
        }
        if self.root_named_while_watched() {
            return Ok(());
        }
        context.started_io(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if crate::NativeRootIdentity::of_root_path(&self.inner.path).ok()
            != Some(self.inner.root.identity())
        {
            return Err(DemandError::SourceUnavailable);
        }
        Ok(())
    }

    /// Whether a live watch proves the root's path still names the held
    fn root_named_while_watched(&self) -> bool {
        #[cfg(windows)]
        {
            self.inner
                .watches
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|watch| watch.strong_count() != 0)
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    fn relative(
        &self,
        path: &NamespacePath,
        context: &mut CallbackWork,
    ) -> Result<(DemandPath, u64), DemandError> {
        NamespacePath::validate_component_lengths(
            path.components().iter().map(|name| name.as_bytes().len()),
            self.inner.limits,
        )?;
        #[cfg(unix)]
        {
            crate::native_capture::namespace_to_host_path_with_work(path, context)
        }
        #[cfg(windows)]
        {
            crate::native_capture::namespace_host_units_with_work(
                path,
                context,
                u16::from(b'\\'),
                crate::native_capture::namespace_component_wide,
            )
        }
    }
    fn metadata(
        &self,
        path: &NamespacePath,
        context: &mut CallbackWork,
    ) -> Result<Option<HostStat>, DemandError> {
        let (relative, allocation) = self.relative(path, context)?;
        #[cfg(unix)]
        let result = self.inner.root.stat(&relative, context);
        #[cfg(windows)]
        let result = self.inner.root.stat_wide(&relative, context);
        drop(relative);
        context.release(allocation)?;
        match result {
            Ok(stat) => Ok(Some(stat)),
            Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn node(metadata: &HostStat) -> SourceNode {
        let kind = node_kind(metadata.file_type());
        SourceNode {
            kind,
            file_identity: file_identity(metadata),
            link_count: Some(metadata.nlink()),
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
        context: &mut CallbackWork,
    ) -> Result<CursorLease, DemandError> {
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
            let state = slot.take().ok_or(DemandError::CursorBusy)?;
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
            (
                token,
                self.open_directory_state(directory, observed, context)?,
            )
        };
        Ok(CursorLease {
            source: self.clone(),
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
        context: &mut CallbackWork,
    ) -> Result<DirectoryState, DemandError> {
        let (relative, allocation) = self.relative(directory, context)?;
        #[cfg(unix)]
        let result = self.inner.root.read_dir_stats(&relative, context);
        #[cfg(windows)]
        let result = self.inner.root.read_dir_stats_wide(&relative, context);
        drop(relative);
        context.release(allocation)?;
        let (entries, allocation) = result?;
        let ownership = SourcePayloadOwnership::new(context)?;
        ownership.retain(allocation, context)?;
        Ok(DirectoryState {
            directory: directory.clone(),
            version: observed,
            entries,
            ownership,
            pending: None,
            replay: VecDeque::new(),
        })
    }
    fn next_entry(
        &self,
        directory: &DemandPath,
        entry: &HostListedEntry,
        context: &mut CallbackWork,
    ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
        #[cfg(unix)]
        let ((encoding, bytes), allocation) = crate::native_name::host_name_bytes_with_source_work(
            &entry.name,
            self.inner.profile,
            self.inner.limits.maximum_component_bytes,
            context,
        )?;
        #[cfg(windows)]
        let ((encoding, bytes), allocation) =
            crate::native_name::host_wide_name_bytes_with_source_work(
                &entry.name,
                self.inner.profile,
                self.inner.limits.maximum_component_bytes,
                context,
            )?;
        let _ = allocation;
        let name = crate::kernel::LogicalName::new(
            encoding,
            bytes,
            self.inner.limits.maximum_component_bytes,
        )
        .map_err(|_| DemandError::InvalidRequest)?;
        let node = match &entry.stat {
            Some(stat) => Self::node(stat),
            None => {
                #[cfg(unix)]
                let stat = self.inner.root.stat(&directory.join(&entry.name), context);
                #[cfg(windows)]
                let stat = self
                    .inner
                    .root
                    .open_dir_held_wide(directory, context)
                    .and_then(|(parent, _)| {
                        crate::native_host::stat_held_name_wide(
                            &parent,
                            &entry.name,
                            self.inner.root.identity().device as u32,
                            context,
                        )
                    });
                match stat {
                    Ok(stat) => Self::node(&stat),
                    Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        Ok(Some(SourceDirectoryEntry { name, node }))
    }
    fn page_entries(
        &self,
        state: &mut DirectoryState,
        maximum: u32,
        context: &mut CallbackWork,
        mut on_entry: impl FnMut(),
    ) -> Result<Vec<SourceDirectoryEntry>, DemandError> {
        let allocation =
            context.claim(maximum as u64 * std::mem::size_of::<SourceDirectoryEntry>() as u64)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(maximum as usize)
            .map_err(DemandError::Allocation)?;
        state.ownership.retain(allocation, context)?;
        let (relative, allocation) = self.relative(&state.directory, context)?;
        let result = (|| {
            while entries.len() < maximum as usize {
                context.check()?;
                if let Some(entry) = state.replay.pop_front() {
                    entries.push(entry);
                    continue;
                }
                let next = match state.pending.take() {
                    Some(entry) => Some(entry),
                    None => self.read_entry(&mut state.entries, context)?,
                };
                let Some(entry) = next else { break };
                match self.next_entry(&relative, &entry, context) {
                    Ok(converted) => entries.extend(converted),
                    Err(error) => {
                        state.pending = Some(entry);
                        return Err(error);
                    }
                }
                on_entry();
            }
            if state.pending.is_none() {
                state.pending = self.read_entry(&mut state.entries, context)?;
            }
            Ok(())
        })();
        drop(relative);
        context.release(allocation)?;
        if result.is_err() {
            entries.extend(state.replay.drain(..));
            state.replay = std::mem::take(&mut entries).into();
        }
        result.map(|()| entries)
    }
    fn read_entry(
        &self,
        reader: &mut HostStatReader,
        context: &mut CallbackWork,
    ) -> Result<Option<HostListedEntry>, DemandError> {
        #[cfg(unix)]
        let next = reader.next(context)?;
        #[cfg(windows)]
        let next = reader.next_wide(context)?;
        match next {
            None => Ok(None),
            Some((entry, _allocation)) => {
                context.record(WorkCounters {
                    source_entries_visited: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                Ok(Some(entry))
            }
        }
    }
}
