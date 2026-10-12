use super::*;

pub(in crate::native_mount::lazy) struct LazyOpenFile<A, O, D, S> {
    pub(in crate::native_mount::lazy) lazy: Arc<LazyWorkspace<A, O, D, S>>,
    pub(in crate::native_mount::lazy) authored: Arc<CheckoutMountSource<A, O>>,
    pub(in crate::native_mount::lazy) runtime: Arc<CallbackRuntime>,
    pub(in crate::native_mount::lazy) path: String,
    pub(in crate::native_mount::lazy) mount_path: MountPath,
    pub(in crate::native_mount::lazy) expected_source: FileId,
    /// The source file, held open and version-proven since the mount opened
    pub(in crate::native_mount::lazy) source_file: Box<dyn DemandFile>,
    pub(in crate::native_mount::lazy) source_node: SourceNode,
    pub(in crate::native_mount::lazy) source_generation: u64,
    pub(in crate::native_mount::lazy) source_view: Arc<SourceViewGate>,
    pub(in crate::native_mount::lazy) detached: DetachedIdentities<A, O>,
    pub(in crate::native_mount::lazy) open_sources: OpenIdentityHandles,
    pub(in crate::native_mount::lazy) promoted: Mutex<Option<Arc<dyn MountOpenFile>>>,
    pub(in crate::native_mount::lazy) unstaged: UnstagedIdentities,
}

impl<A, O, D, S> Drop for LazyOpenFile<A, O, D, S> {
    fn drop(&mut self) {
        untrack_open_handle(
            &self.open_sources,
            self.expected_source,
            self as *const Self as *const (),
        );
    }
}

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn current_authored(&self) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError> {
        if let Some(file) = self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .get(&self.expected_source)
            .filter(|entry| entry.ready)
            .map(|entry| Arc::clone(&entry.file))
        {
            return Ok(Some(file));
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = promoted.as_ref() {
            return Ok(Some(Arc::clone(file)));
        }
        let revision = self.authored.shared_checkout().revision();
        if self.unstaged.checked_at(self.expected_source, revision) {
            return Ok(None);
        }
        let file = self.authored.attached_file_by_id(self.expected_source)?;
        if let Some(file) = file {
            *promoted = Some(Arc::clone(&file));
            return Ok(Some(file));
        }
        self.unstaged.remember(self.expected_source, revision);
        Ok(None)
    }

pub(in crate::native_mount::lazy) fn read_source(&self, offset: u64, length: u64) -> Result<Bytes, MountSourceError> {
        self.source_file
            .read_range(offset, length, &crate::CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_error(failure.error.into()))
    }

pub(in crate::native_mount::lazy) fn source_lease(&self) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move {
                source_view
                    .read_for_callback(owner, Some(self.source_generation))
                    .await
            }
        })
    }

pub(in crate::native_mount::lazy) fn with_authored<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(Some(self.source_generation)).await }
        })?;
        if let Some(file) = self.current_authored()? {
            return operation(file.as_ref());
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if promoted.is_none() {
            self.runtime.wait(|| async {
                stage_mount_promotion(
                    &self.lazy,
                    &self.authored,
                    &self.path,
                    &self.mount_path,
                    self.expected_source,
                )
                .await
            })?;
            *promoted = Some(self.authored.open_file(&self.mount_path)?);
        }
        let file = promoted.as_ref().ok_or(MountSourceError::Stale)?;
        operation(file.as_ref())
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// A handle names one file, whose facts change with it as a native
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.lookup();
        }
        let current = self.runtime.wait(|| async {
            self.lazy
                .source_lookup(self.lazy.source_reference(), &self.path)
                .await
                .map_err(lazy_error)
        })?;
        let node = current
            .filter(|node| node.file_identity == self.source_node.file_identity)
            .unwrap_or(self.source_node);
        Ok(mount_lookup(
            LazyLookup::Source(node),
            self.lazy.source_file_id(&node),
        ))
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_range(offset, length);
        }
        self.read_source(offset, u64::from(length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_up_to(offset, maximum_bytes);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid("source is not regular".to_owned()))?
            .saturating_sub(offset)
            .min(u64::from(maximum_bytes));
        if length == 0 {
            return Ok(Bytes::new());
        }
        self.read_source(offset, length)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.seek(offset, target);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid("seek requires a regular file".to_owned()))?;
        if offset >= length {
            return Ok(None);
        }
        Ok(Some(match target {
            MountSeekTarget::Data => offset,
            MountSeekTarget::Hole => length,
        }))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        let _lease = self.source_lease()?;
        // An unpromoted source file carries no named attributes.
        match self.current_authored()? {
            Some(file) => file.read_attribute(name),
            None => Ok(None),
        }
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _lease = self.source_lease()?;
        match self.current_authored()? {
            Some(file) => file.list_attributes(cursor, maximum_entries),
            None => Ok(MountAttributePage {
                names: Vec::new(),
                next_cursor: None,
            }),
        }
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.remove_attribute(name))
    }
}
