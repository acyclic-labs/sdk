use super::*;

pub(in crate::native_mount::lazy) struct ViewBoundOpenFile<A, O> {
    pub(in crate::native_mount::lazy) inner: Arc<dyn MountOpenFile>,
    pub(in crate::native_mount::lazy) runtime: Arc<CallbackRuntime>,
    pub(in crate::native_mount::lazy) source_view: Arc<SourceViewGate>,
    pub(in crate::native_mount::lazy) detached: DetachedIdentities<A, O>,
    pub(in crate::native_mount::lazy) open_sources: OpenIdentityHandles,
    pub(in crate::native_mount::lazy) file_id: Option<FileId>,
    pub(in crate::native_mount::lazy) generation: u64,
}

impl<A, O> Drop for ViewBoundOpenFile<A, O> {
    fn drop(&mut self) {
        if let Some(file_id) = self.file_id {
            untrack_open_handle(
                &self.open_sources,
                file_id,
                self as *const Self as *const (),
            );
        }
    }
}

impl<A, O> ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
pub(in crate::native_mount::lazy) fn current_file(&self) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let Some(file_id) = self.file_id else {
            return Ok(Arc::clone(&self.inner));
        };
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(detached
            .get(&file_id)
            .filter(|entry| entry.ready)
            .map_or_else(
                || Arc::clone(&self.inner),
                |entry| Arc::clone(&entry.file) as Arc<dyn MountOpenFile>,
            ))
    }

pub(in crate::native_mount::lazy) fn with_view<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.read_for_callback(owner, Some(generation)).await }
        })?;
        operation(self.current_file()?.as_ref())
    }

pub(in crate::native_mount::lazy) fn with_mutation<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.write_stable(Some(generation)).await }
        })?;
        operation(self.current_file()?.as_ref())
    }
}

impl<A, O> MountOpenFile for ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.with_view(MountOpenFile::lookup)
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_range(offset, length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_up_to(offset, maximum_bytes))
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.with_view(|file| file.seek(offset, target))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_range(offset, bytes))
    }

    fn settle(&self) -> Result<(), MountSourceError> {
        self.with_mutation(MountOpenFile::settle)
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.with_view(|file| file.read_attribute(name))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_view(|file| file.list_attributes(cursor, maximum_entries))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.remove_attribute(name))
    }
}
