use super::*;

pub(in crate::native_mount::lazy) struct LazyOpenFile<A, O, D, S> {
    pub(in crate::native_mount::lazy) lazy: Arc<LazyWorkspace<A, O, D, S>>,
    pub(in crate::native_mount::lazy) authored: Arc<CheckoutMountSource<A, O>>,
    pub(in crate::native_mount::lazy) runtime: Arc<CallbackRuntime>,
    pub(in crate::native_mount::lazy) path: String,
    pub(in crate::native_mount::lazy) mount_path: MountPath,
    pub(in crate::native_mount::lazy) expected_source: FileId,
    pub(in crate::native_mount::lazy) source_file: Box<dyn DemandFile>,
    pub(in crate::native_mount::lazy) source_node: SourceNode,
    pub(in crate::native_mount::lazy) source_generation: u64,
    pub(in crate::native_mount::lazy) source_view: Arc<SourceViewGate>,
    pub(in crate::native_mount::lazy) detached: DetachedIdentities<A, O>,
    pub(in crate::native_mount::lazy) open_sources: OpenIdentityHandles,
    pub(in crate::native_mount::lazy) _input_allocation: Option<crate::NativeAllocation>,
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
    fn lifetime(&self) -> &CancellationToken { self.authored.cancellation() }
async fn current_authored_with_carrier(
        &self, owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<(Arc<dyn MountOpenFile>, u64)>, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        {
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            if let Ok(position) = carrier.with_work(|work| detached_position(&detached, self.expected_source, Some(work)))? {
                if detached[position].1.ready {
                    return Ok(Some((Arc::clone(&detached[position].1.file) as Arc<dyn MountOpenFile>, 0)));
                }
            }
        }
        self.authored.attached_file_by_id(self.expected_source, owner, carrier).await
    }

fn with_view_with_meter<T>(
        &self, work: &crate::NativeMaterializationWork, refresh: bool,
        operation: impl FnOnce(Option<&dyn MountOpenFile>, SourceNode,
            &mut crate::native_mount::adapter::NativeDemandCarrier<'_>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self.source_view.read_for_callback_with_carrier(
                    owner, Some(self.source_generation), carrier, &caller,
                    Some(self.authored.cancellation()),
                ).await?;
                let file = self.current_authored_with_carrier(owner, carrier).await?;
                let node = if file.is_none() && refresh {
                    let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                    let result = self.lazy.source_lookup_measured(
                        self.lazy.source_reference(), &self.path, budget, &caller).await;
                    carrier.with_work(|work| mount_accept(work, result, lazy_error))?
                        .filter(|node| node.file_identity == self.source_node.file_identity)
                        .unwrap_or(self.source_node)
                } else { self.source_node };
                let bytes = file.as_ref().map_or(0, |(_, bytes)| *bytes);
                let result = operation(file.as_ref().map(|(file, _)| file.as_ref()), node, carrier);
                drop(file);
                carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)?;
                result
            })
        })
    }

fn with_authored_with_meter<T>(
        &self, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self.source_view.write_stable_with_carrier(
                    Some(self.source_generation), carrier, &caller, Some(self.authored.cancellation()),
                ).await?;
                let file = match self.current_authored_with_carrier(owner, carrier).await? {
                    Some(file) => file,
                    None => {
                        stage_mount_promotion_with_carrier(&self.lazy, &self.authored, &self.path,
                            &self.mount_path, self.expected_source, carrier).await?;
                        self.authored.attached_file_by_id(
                            self.expected_source, owner, carrier).await?.ok_or(MountSourceError::Stale)?
                    }
                };
                let result = operation(file.0.as_ref());
                let bytes = file.1;
                drop(file);
                carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)?;
                result
            })
        })
    }

fn read_source_with_carrier(
        &self,
        offset: u64,
        length: u64,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return crate::demand::read_file(&*self.source_file, offset, length, &original, original.cancellation()).map_err(mount_demand_error);
        }
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let caller = carrier.cancellation();
        let read = crate::demand::read_file(&*self.source_file, offset, length, budget, &caller);
        let output = carrier.with_work(|work| mount_accept(work, read, mount_demand_error))?;
        let retained = output.logical_owned_bytes;
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(output);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        Ok(output)
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
fn lookup_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.with_view_with_meter(work, true, |file, node, carrier| {
            if let Some(file) = file { return file.lookup_with_meter(work); }
            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
            let caller = carrier.cancellation();
            let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, &caller);
            let result = self.lazy.source_file_id(&node.file_identity, &mut accounting);
            let spent = accounting.spent();
            let result = result.map(|value| crate::OperationReceipt { value, work: spent })
                .map_err(|error| crate::OperationFailure::new(error, spent));
            let id = carrier.with_work(|work| mount_accept(work, result, lazy_error))?;
            Ok(mount_lookup(&LazyLookup::Source(node), id))
        })
    }

fn read_range_with_meter(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, carrier| match file {
            Some(file) => file.read_range_with_meter(offset, length, work),
            None => self.read_source_with_carrier(offset, u64::from(length), carrier),
        })
    }

fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.read_up_to_with_meter(offset, maximum_bytes, work); }
            let length = match node.logical_bytes {
                Some(length) => length.saturating_sub(offset).min(u64::from(maximum_bytes)),
                None => return Err(mount_content_invalid_with_carrier(carrier, "source is not regular")?),
            };
            if length == 0 {
                return carrier.empty_source_chunk();
            }
            self.read_source_with_carrier(offset, length, carrier)
        })
    }

fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork)
        -> Result<Option<u64>, MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.seek_with_meter(offset, target, work); }
            carrier.with_work(|work| mount_items(work, 1))?;
            let length = match node.logical_bytes {
                Some(length) => length,
                None => return Err(mount_content_invalid_with_carrier(carrier, "seek requires a regular file")?),
            };
            Ok((offset < length).then_some(match target { MountSeekTarget::Data => offset, MountSeekTarget::Hole => length }))
        })
    }

fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_range_with_meter(offset, bytes, work))
    }

fn resize_with_meter(&self, logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.resize_with_meter(logical_bytes, work))
    }

fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.allocate_range_with_meter(offset, length, operation, work))
    }

fn set_attributes(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.set_attributes(metadata, logical_bytes, work))
    }

fn read_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.read_attribute(name, work),
            None => Ok(None),
        })
    }

fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.list_attributes_with_meter(cursor, maximum_entries, work),
            None => Ok((MountAttributePage { names: Vec::new(), next_cursor: None },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)),
        })
    }

fn write_attribute(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_attribute(name, value, mode, work))
    }

fn remove_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.remove_attribute(name, work))
    }

fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.settle(work),
            None => Ok(()),
        })
    }

fn read_sparse_range(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<(crate::native_mount::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.read_sparse_range(offset, length, work); }
            let original = work.clone();
            let logical_bytes = u64::from(length);
            if offset.checked_add(logical_bytes).is_none() {
                return Err(MountSourceError::NativeText {
                    kind: crate::native_mount::NativeSourceTextKind::Invalid,
                    text: "sparse range overflows the file address space",
                });
            }
            let source_length = match node.logical_bytes {
                Some(length) => length,
                None => return Err(MountSourceError::NativeText {
                    kind: crate::native_mount::NativeSourceTextKind::Invalid, text: "seek requires a regular file",
                }),
            };
            let data_length = source_length.saturating_sub(offset).min(logical_bytes);
            if data_length == 0 {
                return Ok((crate::native_mount::MountSparseRange { logical_bytes, spans: Vec::new() },
                    original.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?));
            }
            let output = self.read_source_with_carrier(offset, data_length, carrier)?;
            if output.bytes.len() as u64 != data_length {
                return Err(MountSourceError::Stale);
            }
            if !output.is_intrinsic() {
                return Err(MountSourceError::NativeUnsupported);
            }
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let counters = crate::WorkCounters { items_returned: 1, ..crate::WorkCounters::UNCHARGED };
            original.admit(counters).map_err(MountSourceError::NativeWork)?;
            let span_bytes = std::mem::size_of::<crate::native_mount::MountSparseSpan>() as u64;
            let mut packet = (
                Vec::<crate::native_mount::MountSparseSpan>::new(),
                original.claim(span_bytes, 1, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?,
            );
            packet.0.try_reserve_exact(1).map_err(MountSourceError::Allocation)?;
            packet.1.reconcile_capacity(
                (packet.0.capacity() * std::mem::size_of::<crate::native_mount::MountSparseSpan>()) as u64,
            ).map_err(MountSourceError::NativeWork)?;
            let (bytes, _, _) = output.into_owned_parts();
            packet.0.push(crate::native_mount::MountSparseSpan {
                relative_offset: 0, bytes,
            });
            original.record_admitted(counters).map_err(MountSourceError::NativeWork)?;
            Ok((crate::native_mount::MountSparseRange { logical_bytes, spans: packet.0 }, packet.1))
        })
    }

fn write_sparse_range(&self, offset: u64, range: (crate::native_mount::MountSparseRange, crate::NativeAllocation),
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_sparse_range(offset, range, work))
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
