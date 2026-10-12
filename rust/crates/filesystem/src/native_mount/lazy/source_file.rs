use super::*;

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    async fn current_authored(
        &self,
        owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<(Arc<dyn MountOpenFile>, u64)>, MountSourceError> {
        carrier
            .with_work(|work| work.check())
            .map_err(mount_demand_error)?;
        {
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            carrier.with_work(|work| mount_items(work, 1))?;
            if let Some(entry) = detached
                .get(&self.expected_source)
                .filter(|entry| entry.ready)
            {
                return Ok(Some((Arc::clone(&entry.file) as Arc<dyn MountOpenFile>, 0)));
            }
        }
        self.authored
            .attached_file_by_id(self.expected_source, owner, carrier)
            .await
    }

    fn observe<T>(
        &self,
        work: &crate::NativeMaterializationWork,
        refresh: bool,
        operation: impl FnOnce(
            Option<&dyn MountOpenFile>,
            SourceNode,
            &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
        ) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = ViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self
                    .source_view
                    .read_for_callback_with_carrier(
                        owner,
                        Some(self.source_generation),
                        carrier,
                        &caller,
                        Some(self.authored.cancellation()),
                    )
                    .await?;
                let file = self.current_authored(owner, carrier).await?;
                let node = if file.is_none() && refresh {
                    self.lazy
                        .source_lookup(
                            self.lazy.source_reference(),
                            &self.path,
                            &mut crate::kernel::KernelAccounting::native(work, &caller),
                        )
                        .await
                        .map_err(lazy_error)?
                        .filter(|node| node.file_identity == self.source_node.file_identity)
                        .unwrap_or(self.source_node)
                } else {
                    self.source_node
                };
                let bytes = file.as_ref().map_or(0, |(_, bytes)| *bytes);
                let result = operation(file.as_ref().map(|(file, _)| file.as_ref()), node, carrier);
                drop(file);
                carrier
                    .with_work(|work| work.release(bytes))
                    .map_err(mount_demand_error)?;
                result
            })
        })
    }

    fn mutate<T>(
        &self,
        work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = ViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self
                    .source_view
                    .write_stable_with_carrier(
                        Some(self.source_generation),
                        carrier,
                        &caller,
                        Some(self.authored.cancellation()),
                    )
                    .await?
                    .into_mutation_phase();
                let file = match self.current_authored(owner, carrier).await? {
                    Some(file) => file,
                    None => {
                        stage_mount_promotion(
                            &self.lazy,
                            &self.authored,
                            &self.path,
                            self.expected_source,
                            carrier,
                        )
                        .await?;
                        self.authored
                            .attached_file_by_id(self.expected_source, owner, carrier)
                            .await?
                            .ok_or(MountSourceError::Stale)?
                    }
                };
                let result = operation(file.0.as_ref());
                let bytes = file.1;
                drop(file);
                carrier
                    .with_work(|work| work.release(bytes))
                    .map_err(mount_demand_error)?;
                result
            })
        })
    }

    fn read_source(
        &self,
        offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        crate::demand::read_file(
            &*self.source_file,
            offset,
            length,
            work,
            work.cancellation(),
        )
        .map_err(mount_demand_error)
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn lookup_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.observe(work, true, |file, node, _| {
            if let Some(file) = file {
                return file.lookup_with_meter(work);
            }
            Ok(mount_lookup(
                LazyLookup::Source {
                    node,
                    source: self.lazy.source_reference(),
                },
                self.expected_source,
            ))
        })
    }

    fn read_range_with_meter(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.observe(work, false, |file, _, _| match file {
            Some(file) => file.read_range_with_meter(offset, length, work),
            None => self.read_source(offset, u64::from(length), work),
        })
    }

    fn read_up_to_with_meter(
        &self,
        offset: u64,
        maximum_bytes: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.observe(work, false, |file, node, carrier| {
            if let Some(file) = file {
                return file.read_up_to_with_meter(offset, maximum_bytes, work);
            }
            let length = match node.logical_bytes {
                Some(length) => length.saturating_sub(offset).min(u64::from(maximum_bytes)),
                None => {
                    return Err(MountSourceError::Invalid("source is not regular".into()));
                }
            };
            if length == 0 {
                return carrier.empty_source_chunk();
            }
            self.read_source(offset, length, work)
        })
    }

    fn seek_with_meter(
        &self,
        offset: u64,
        target: MountSeekTarget,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        self.observe(work, false, |file, node, carrier| {
            if let Some(file) = file {
                return file.seek_with_meter(offset, target, work);
            }
            carrier.with_work(|work| mount_items(work, 1))?;
            let length = match node.logical_bytes {
                Some(length) => length,
                None => {
                    return Err(MountSourceError::Invalid(
                        "seek requires a regular file".into(),
                    ));
                }
            };
            Ok((offset < length).then_some(match target {
                MountSeekTarget::Data => offset,
                MountSeekTarget::Hole => length,
            }))
        })
    }

    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| {
            file.write_range_with_meter(offset, bytes, work)
        })
    }

    fn resize_with_meter(
        &self,
        logical_bytes: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| file.resize_with_meter(logical_bytes, work))
    }

    fn allocate_range_with_meter(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| {
            file.allocate_range_with_meter(offset, length, operation, work)
        })
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| {
            file.set_attributes(metadata, logical_bytes, work)
        })
    }

    fn read_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.observe(work, false, |file, _, _| match file {
            Some(file) => file.read_attribute(name, work),
            None => Ok(None),
        })
    }

    fn list_attributes_with_meter(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.observe(work, false, |file, _, _| match file {
            Some(file) => file.list_attributes_with_meter(cursor, maximum_entries, work),
            None => Ok((
                MountAttributePage {
                    names: Vec::new(),
                    next_cursor: None,
                },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork)?,
            )),
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| file.write_attribute(name, value, mode, work))
    }

    fn remove_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.mutate(work, |file| file.remove_attribute(name, work))
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.observe(work, false, |file, _, _| match file {
            Some(file) => file.settle(work),
            None => Ok(()),
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
        self.observe(work, false, |file, node, carrier| {
            if let Some(file) = file {
                return file.read_sparse_range(offset, length, work);
            }
            let logical_bytes = u64::from(length);
            if offset.checked_add(logical_bytes).is_none() {
                return Err(MountSourceError::Invalid(
                    "sparse range overflows the file address space".into(),
                ));
            }
            let source_length = match node.logical_bytes {
                Some(length) => length,
                None => {
                    return Err(MountSourceError::Invalid(
                        "seek requires a regular file".into(),
                    ));
                }
            };
            let data_length = source_length.saturating_sub(offset).min(logical_bytes);
            if data_length == 0 {
                return Ok((
                    crate::native_mount::MountSparseRange {
                        logical_bytes,
                        spans: Vec::new(),
                    },
                    work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                        .map_err(MountSourceError::NativeWork)?,
                ));
            }
            let output = self.read_source(offset, data_length, work)?;
            if output.bytes.len() as u64 != data_length {
                return Err(MountSourceError::Stale);
            }
            if !output.is_intrinsic() {
                return Err(MountSourceError::NativeUnsupported);
            }
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
            let counters = crate::WorkCounters {
                items_returned: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(counters).map_err(MountSourceError::NativeWork)?;

            let mut scratch_carrier = adapter::NativeDemandCarrier::Native(work);
            let mut scratch = adapter::InputBuffers::new(&mut scratch_carrier)?;
            let mut spans = scratch.vec(1)?;
            let (bytes, _, _) = output.into_owned_parts();
            spans.push(crate::native_mount::MountSparseSpan {
                relative_offset: 0,
                bytes,
            });
            work.record_admitted(counters)
                .map_err(MountSourceError::NativeWork)?;
            Ok((
                crate::native_mount::MountSparseRange {
                    logical_bytes,
                    spans,
                },
                scratch
                    .retain()
                    .ok_or(MountSourceError::NativeUnsupported)?,
            ))
        })
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
        self.mutate(work, |file| file.write_sparse_range(offset, range, work))
    }
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.lookup_with_meter(work)
        })
    }
    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.read_range_with_meter(offset, length, work)
                .map(|packet| packet.bytes)
        })
    }
    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.read_up_to_with_meter(offset, maximum_bytes, work)
                .map(|packet| packet.bytes)
        })
    }
    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.seek_with_meter(offset, target, work)
        })
    }
    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.write_range_with_meter(
                offset,
                crate::kernel::OwnedSourceChunk::claimed(bytes.into(), 0),
                work,
            )
        })
    }
    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.resize_with_meter(logical_bytes, work)
        })
    }
    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.allocate_range_with_meter(offset, length, operation, work)
        })
    }
    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        adapter::boundary(self.authored.cancellation(), |work| {
            self.list_attributes_with_meter(cursor, maximum_entries, work)
                .map(|(page, _)| page)
        })
    }
}
