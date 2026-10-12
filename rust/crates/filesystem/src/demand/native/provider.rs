#[async_trait::async_trait]
impl DemandSource for NativeDemandSource {
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: self.inner.identity,
            epoch: self.inner.epoch.load(Ordering::Acquire),
        }
    }

    /// Watches a root on a local file system, whose changes all pass
    /// through this kernel and so are all reported; a network or
    /// user-space file system changes out of its sight.
    fn watch(
        &self,
        sink: Arc<dyn SourceChangeSink>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        if !self.inner.inline {
            return Ok(None);
        }
        #[cfg(test)]
        if !self.watchable {
            return Ok(None);
        }
        let changes = Arc::new(NativeChanges {
            inner: Arc::downgrade(&self.inner),
            sink,
        });
        let watch = Arc::new(crate::source_watch::NativeSourceWatch::start(
            &self.inner.root,
            changes,
        )?);
        #[cfg(any(target_os = "linux", windows))]
        {
            let mut watches = self
                .inner
                .watches
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            watches.retain(|watch| watch.strong_count() != 0);
            watches.push(Arc::downgrade(&watch));
        }
        Ok(Some(Box::new(NativeWatch(watch))))
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
                let mut node = provider.metadata(&path)?.as_ref().map(Self::node);
                // A directory's own facts (its listing's times and link
                // count) are reported only once it is watched itself.
                if node.is_some_and(|node| node.kind == SourceNodeKind::Directory)
                    && provider.admit_to_watches(&path)?
                {
                    node = provider.metadata(&path)?.as_ref().map(Self::node);
                }
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
                let next = if lease.state()?.pending.is_some() || !lease.state()?.replay.is_empty()
                {
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
        let range = ReadRange::new(offset, length).map_err(OperationFailure::before_work)?;
        let path = path.clone();
        self.run_blocking(cancellation, move |provider, request_cancellation| {
            measured(|work| {
                work.source_path_components = path.depth() as u64;
                NativeDemandFile::open(provider, source, &path, expected, &request_cancellation)?
                    .read(range, &request_cancellation, work)
            })
        })
        .await
    }

    async fn open_file(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Box<dyn DemandFile>> {
        let path = path.clone();
        self.run_blocking(cancellation, move |provider, request_cancellation| {
            measured(|work| {
                work.source_path_components = path.depth() as u64;
                let file = NativeDemandFile::open(
                    provider,
                    source,
                    &path,
                    expected,
                    &request_cancellation,
                )?;
                Ok(Box::new(file) as Box<dyn DemandFile>)
            })
        })
        .await
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
