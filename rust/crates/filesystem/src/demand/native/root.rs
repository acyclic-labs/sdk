/// One native root. Opening it does not enumerate any descendants.
#[derive(Clone)]
pub struct NativeDemandSource {
    inner: Arc<NativeDemandInner>,
    excluded: Arc<ExcludedPaths>,
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
    ownership: SourcePayloadOwnership,
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

    /// Opens only the root, with its optional durable identity and observer.
    pub async fn open<W: FsWork + Send>(
        path: std::borrow::Cow<'_, Path>,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        reference: Option<SourceReference>,
        observer: Option<Arc<dyn DemandDirectoryObserver>>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut context = callback_work(&mut accounting)?;
            let input = match &path {
                std::borrow::Cow::Owned(path) => path.capacity() as u64,
                _ => 0,
            };
            let mut claimed_input = 0;
            let result = (|| {
                context.claim_allocation(input, 0, WorkCounters::UNCHARGED)?;
                claimed_input = input;
                let controls = crate::source_watch::arc_control_bytes::<NativeDemandInner>()
                    + crate::source_watch::arc_control_bytes::<Semaphore>()
                    + crate::source_watch::arc_control_bytes::<ExcludedPaths>();
                let ownership = SourcePayloadOwnership::new(&mut context)?;
                let allocation = context.claim(controls)?;
                ownership.retain(allocation, &mut context)?;
                #[cfg(unix)]
                let (path, allocation) = {
                    let path =
                        crate::native_host::normalize_root_path(path.as_ref(), &mut context)?;
                    let allocation = path.capacity() as u64;
                    (path, allocation)
                };
                #[cfg(windows)]
                let (path, allocation) =
                    crate::native_host::clone_path(path.as_ref(), &mut context)?;
                ownership.retain(allocation, &mut context)?;
                Ok((path, ownership))
            })();
            drop(path);
            let released = context.release(claimed_input);
            let result = result.and_then(|value| released.map(|()| value));
            let result = match result {
                Ok((path, ownership)) => tokio::task::spawn_blocking(crate::obs::in_context(
                    crate::obs::caller_context(),
                    move || {
                        let result = (|| {
                            let root = HostRoot::open(&path, &mut context)?;
                            let inline = crate::native_host::filesystem_is_local(
                                &root.directory,
                                &mut context,
                            )?;
                            let reference = reference.unwrap_or_else(|| SourceReference {
                                identity: *uuid::Uuid::new_v4().as_bytes(),
                                epoch: 0,
                            });
                            context.check()?;
                            Ok(Self {
                                excluded: Arc::new(ExcludedPaths {
                                    paths: Vec::new(),
                                    ownership: SourcePayloadOwnership::default(),
                                }),
                                inner: Arc::new(NativeDemandInner {
                                    path,
                                    root,
                                    inline,
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
                                    ownership,
                                }),
                                #[cfg(test)]
                                watchable: true,
                            })
                        })();
                        (result, context)
                    },
                ))
                .await
                .map_err(|_| DemandError::WorkerUnavailable)?,
                Err(error) => (Err(error), context),
            };
            finish_context(result.0, result.1, &mut accounting)
        }
        .await;
        work.finish(result, &accounting)
    }

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
            account_source(context, |accounting, _| {
                observer.observe_directory(directory, accounting)
            })
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
            let watches = self
                .inner
                .watches
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !watches.iter().any(|watch| watch.strong_count() != 0) {
                return Ok(false);
            }
            let (relative, allocation) = self.relative(directory, context)?;
            let mut added = false;
            for watch in watches.iter().filter_map(std::sync::Weak::upgrade) {
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
            .map_err(DemandError::from)
        })?;
        let result = self.observe_directory(&parent, context);
        let bytes = parent.retained_bytes();
        drop(parent);
        account_source(context, |accounting, _| {
            crate::kernel::retire_allocation(allocation, bytes, accounting)
                .map_err(DemandError::from)
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
        match crate::native_host::root_path_identity(&self.inner.path, context) {
            Ok(identity) if identity == self.inner.root.identity() => {}
            Err(DemandError::Work(error)) => return Err(DemandError::Work(error)),
            Err(DemandError::Cancelled) => return Err(DemandError::Cancelled),
            _ => return Err(DemandError::SourceUnavailable),
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
        let result = self.inner.root.stat(&relative, context);
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
}
