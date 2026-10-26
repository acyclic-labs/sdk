use super::*;

impl<A, O, D, S> Drop for LazyOpenFile<A, O, D, S> {
    fn drop(&mut self) {
        untrack_open_handle(
            &self.open_sources,
            self.expected_source,
            self as *const Self as *const (),
        );
    }
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

fn untrack_open_handle(handles: &OpenIdentityHandles, file_id: FileId, ptr: *const ()) {
    let Ok(mut open) = handles.lock() else {
        return;
    };
    if let Some(entries) = open.get_mut(&file_id) {
        entries.retain(|entry| entry.as_ptr() as *const () != ptr);
        if entries.is_empty() {
            open.remove(&file_id);
        }
    }
}

impl<A, O> ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn current_file(&self) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
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

    // Both convenience and explicit-work calls use one original ledger.
    // Only the public convenience entry creates it; wrappers never reset it.
    fn with_view<T>(
        &self,
        work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let owner = ViewGate::callback_owner()?;
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let lease =
            with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || {
                    self.source_view.read_for_callback_with_carrier(
                        owner,
                        Some(self.generation),
                        carrier,
                        work.cancellation(),
                        Some(&self.cancellation),
                    )
                })
            })?;
        let result = operation(self.current_file()?.as_ref());
        drop(lease);
        result
    }

    fn with_mutation<T>(
        &self,
        work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let lease =
            with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || {
                    self.source_view.write_stable_with_carrier(
                        Some(self.generation),
                        carrier,
                        work.cancellation(),
                        Some(&self.cancellation),
                    )
                })
            })?
            .into_mutation_phase();
        let result = operation(self.current_file()?.as_ref());
        drop(lease);
        result
    }
}

impl<A, O> MountOpenFile for ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    adapter::ordinary_file_callbacks!(self, &self.cancellation);

    fn clone_source(
        &self,
        checkout: &(dyn std::any::Any + Send + Sync),
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileId, MountSourceError> {
        self.with_view(work, |file| file.clone_source(checkout, work))
    }

    fn clone_range_from(
        &self,
        source: &dyn MountOpenFile,
        source_offset: u64,
        offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| {
            file.clone_range_from(source, source_offset, offset, length, work)
        })
    }

    fn lookup_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.with_view(work, |file| file.lookup_with_meter(work))
    }
    fn read_range_with_meter(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(work, |file| {
            file.read_range_with_meter(offset, length, work)
        })
    }
    fn read_up_to_with_meter(
        &self,
        offset: u64,
        maximum_bytes: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(work, |file| {
            file.read_up_to_with_meter(offset, maximum_bytes, work)
        })
    }
    fn seek_with_meter(
        &self,
        offset: u64,
        target: MountSeekTarget,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        self.with_view(work, |file| file.seek_with_meter(offset, target, work))
    }
    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| {
            file.write_range_with_meter(offset, bytes, work)
        })
    }
    fn append_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| {
            file.append_range_with_meter(offset, bytes, work)
        })
    }

    fn resize_with_meter(
        &self,
        logical_bytes: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| file.resize_with_meter(logical_bytes, work))
    }
    fn allocate_range_with_meter(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| {
            file.allocate_range_with_meter(offset, length, operation, work)
        })
    }
    fn list_attributes_with_meter(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.with_view(work, |file| {
            file.list_attributes_with_meter(cursor, maximum_entries, work)
        })
    }
    fn read_sparse_range(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<
        (
            crate::native_mount::MountSparseRange,
            crate::NativeAllocation,
        ),
        MountSourceError,
    > {
        self.with_view(work, |file| file.read_sparse_range(offset, length, work))
    }
    fn write_sparse_range(
        &self,
        offset: u64,
        range: (
            crate::native_mount::MountSparseRange,
            crate::NativeAllocation,
        ),
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| file.write_sparse_range(offset, range, work))
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| file.settle(work))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| {
            file.set_attributes(metadata, logical_bytes, work)
        })
    }

    fn read_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.with_view(work, |file| file.read_attribute(name, work))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| file.write_attribute(name, value, mode, work))
    }

    fn remove_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(work, |file| file.remove_attribute(name, work))
    }
}
