use super::*;

pub(in crate::native_mount::lazy) struct ViewBoundOpenFile<A, O> {
    pub(in crate::native_mount::lazy) inner: Arc<dyn MountOpenFile>,
    pub(in crate::native_mount::lazy) runtime: Arc<CallbackRuntime>,
    pub(in crate::native_mount::lazy) source_view: Arc<SourceViewGate>,
    pub(in crate::native_mount::lazy) source_lifetime: crate::CancellationToken,
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
    fn lifetime(&self) -> &CancellationToken { &self.source_lifetime }
fn current_file(
        &self,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            let Some(file_id) = self.file_id else { return Ok(Arc::clone(&self.inner)); };
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            Ok(match detached_position(&detached, file_id, Some(work))? {
                Ok(position) if detached[position].1.ready => Arc::clone(&detached[position].1.file) as Arc<dyn MountOpenFile>,
                _ => Arc::clone(&self.inner),
            })
        })
    }

fn with_view<T>(
        &self, mut carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let owner = SourceViewGate::callback_owner();
                    let caller = carrier.cancellation();
                    let _lease = self.source_view.read_for_callback_with_carrier(
                        owner, Some(self.generation), carrier, &caller, Some(&self.source_lifetime),
                    ).await?;
                    operation(self.current_file(carrier)?.as_ref())
                })
            })
    }

fn with_mutation<T>(
        &self, mut carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let caller = carrier.cancellation();
                    let _lease = self.source_view.write_stable_with_carrier(
                        Some(self.generation), carrier, &caller, Some(&self.source_lifetime),
                    ).await?;
                    operation(self.current_file(carrier)?.as_ref())
                })
            })
    }
}

impl<A, O> MountOpenFile for ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
fn lookup_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.lookup_with_meter(work))
    }

fn read_range_with_meter(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.read_range_with_meter(offset, length, work))
    }

fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.read_up_to_with_meter(offset, maximum_bytes, work))
    }

fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork)
        -> Result<Option<u64>, MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.seek_with_meter(offset, target, work))
    }

fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.write_range_with_meter(offset, bytes, work))
    }

fn resize_with_meter(&self, logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.resize_with_meter(logical_bytes, work))
    }

fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.allocate_range_with_meter(offset, length, operation, work))
    }

fn set_attributes(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.set_attributes(metadata, logical_bytes, work))
    }

fn read_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.read_attribute(name, work))
    }

fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.list_attributes_with_meter(cursor, maximum_entries, work))
    }

fn write_attribute(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.write_attribute(name, value, mode, work))
    }

fn remove_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.remove_attribute(name, work))
    }

fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.settle(work))
    }

fn read_sparse_range(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<(crate::native_mount::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        self.with_view(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.read_sparse_range(offset, length, work))
    }

fn write_sparse_range(&self, offset: u64, range: (crate::native_mount::MountSparseRange, crate::NativeAllocation),
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(crate::native_mount::adapter::NativeDemandCarrier::Native(work), |file| file.write_sparse_range(offset, range, work))
    }
    fn lookup(&self) -> Result<MountLookup, MountSourceError> { adapter::boundary(self.lifetime(), |work| self.lookup_with_meter(work)) }
    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> { adapter::boundary(self.lifetime(), |work| self.read_range_with_meter(offset, length, work).map(|packet| packet.bytes)) }
    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> { adapter::boundary(self.lifetime(), |work| self.read_up_to_with_meter(offset, maximum_bytes, work).map(|packet| packet.bytes)) }
    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> { adapter::boundary(self.lifetime(), |work| self.seek_with_meter(offset, target, work)) }
    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> { adapter::boundary(self.lifetime(), |work| self.write_range_with_meter(offset, crate::kernel::OwnedSourceChunk::claimed(bytes.into(), 0), work)) }
    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> { adapter::boundary(self.lifetime(), |work| self.resize_with_meter(logical_bytes, work)) }
    fn allocate_range(&self, offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError> { adapter::boundary(self.lifetime(), |work| self.allocate_range_with_meter(offset, length, operation, work)) }
    fn list_attributes(&self, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError> { adapter::boundary(self.lifetime(), |work| self.list_attributes_with_meter(cursor, maximum_entries, work).map(|(page, _)| page)) }
}
