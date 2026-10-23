struct DetachedMountState<A, O> {
    file: DetachedFile<A, O>,
    metadata: FileMetadata,
    mutation_epoch: u64,
}

impl<A, O> DetachedMountState<A, O> {
    async fn change_content(
        &mut self,
        change: ContentChange<std::convert::Infallible>,
        ledger: &Arc<ViewLedger>,
        clock: &super::clock::Clock,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut metadata = self.metadata;
        let stamped = metadata
            .stamp_content_change(content_change_time(clock)?)
            .then_some(metadata);
        self.file
            .change_content(change, stamped, work, work.cancellation())
            .await?;
        self.metadata = metadata;
        self.record_change(ledger, &mut NativeDemandCarrier::Native(work))
    }

    fn record_change(
        &mut self,
        ledger: &Arc<ViewLedger>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.mutation_epoch = self
            .mutation_epoch
            .checked_add(1)
            .ok_or(MountSourceError::Stale)?;
        record_view_change(ledger, &ViewChange::Node(self.file.file_id()), carrier)
    }
}

pub(super) struct CheckoutDetachedFile<A, O> {
    _native_allocation: crate::NativeAllocation,
    state: tokio::sync::Mutex<DetachedMountState<A, O>>,
    ledger: Arc<ViewLedger>,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
}

impl<A, O> CheckoutDetachedFile<A, O> {
    pub(super) async fn read_bytes(
        &self,
        offset: u64,
        length: u32,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        let state = await_lock(
            self.state.lock(),
            carrier,
            &cancellation,
            Some(&self.cancellation),
        )
        .await?;
        let range = ByteRange {
            offset,
            length: u64::from(length),
        };
        let original = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let read = state
            .file
            .read_range(range, &original, &cancellation)
            .await?;
        let capacity = read.logical_owned_bytes();
        let chunk = crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, capacity);
        carrier.check(&cancellation, Some(&self.cancellation))?;
        Ok(chunk)
    }

    async fn change_attribute(
        &self,
        name: &[u8],
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = work.cancellation();
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let name = inputs.mutation_attribute_name(name, self.profile, self.limits)?;
        let mut state = await_lock(
            self.state.lock(),
            inputs.carrier,
            cancellation,
            Some(&self.cancellation),
        )
        .await?;
        state.metadata = state
            .file
            .change_named_attribute(&name, value, work, cancellation)
            .await?;
        state.record_change(&self.ledger, inputs.carrier)
    }

    pub(super) async fn snapshot(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = await_lock(
            self.state.lock(),
            carrier,
            cancellation,
            Some(&self.cancellation),
        )
        .await?;
        let metadata = if let Some(original) = carrier.original_native_work() {
            state.file.read_metadata(&original, cancellation).await?
        } else {
            state.metadata
        };
        carrier.check(cancellation, Some(&self.cancellation))?;
        Ok((state.file.record(), metadata, state.mutation_epoch))
    }
}

impl<A, O> CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn change_content(
        &self,
        change: ContentChange<std::convert::Infallible>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content_at(change, false, work)
    }

    fn change_content_at(
        &self,
        change: ContentChange<std::convert::Infallible>,
        append_only: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut state = await_lock(
                self.state.lock(),
                &mut NativeDemandCarrier::Native(work),
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
            if append_only
                && !matches!(&change, ContentChange::Write { offset, .. } if *offset == state.file.logical_bytes())
            {
                return Err(MountSourceError::AccessDenied);
            }
            state
                .change_content(change, &self.ledger, &self.runtime.clock, work)
                .await
        })
    }
}

impl<A, O> MountOpenFile for CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn clone_source(
        &self,
        _checkout: &(dyn std::any::Any + Send + Sync),
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileId, MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        Err(MountSourceError::NativeUnsupported)
    }

    fn clone_range_from(
        &self,
        _source: &dyn MountOpenFile,
        _source_offset: u64,
        _offset: u64,
        _length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        Err(MountSourceError::NativeUnsupported)
    }

    fn lookup_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let (record, metadata, _) = self
                .snapshot(&mut NativeDemandCarrier::Native(work), work.cancellation())
                .await?;
            let mut node = mount_node(record);
            node.link_count = 0;
            Ok(MountLookup { node, metadata })
        })
    }

    fn read_up_to_with_meter(
        &self,
        offset: u64,
        maximum_bytes: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        if u64::from(maximum_bytes) > self.limits.maximum_read_bytes {
            return Err(MountSourceError::from(FsError::FileRead(
                crate::kernel::FileRangeReadError::InvalidRange,
            )));
        }
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = work.cancellation();
            let state = await_lock(
                self.state.lock(),
                &mut carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
            let length = state
                .file
                .logical_bytes()
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                carrier.check(cancellation, Some(&self.cancellation))?;
                return Ok(crate::kernel::OwnedSourceChunk::intrinsic(
                    OwnedBytes::new(),
                    0,
                ));
            }
            let read = state
                .file
                .read_range(ByteRange { offset, length }, work, cancellation)
                .await?;
            let capacity = read.logical_owned_bytes();
            carrier.check(cancellation, Some(&self.cancellation))?;
            Ok(crate::kernel::OwnedSourceChunk::intrinsic(
                read.bytes, capacity,
            ))
        })
    }

    fn seek_with_meter(
        &self,
        offset: u64,
        target: MountSeekTarget,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let state = await_lock(
                self.state.lock(),
                &mut carrier,
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
            let position = state
                .file
                .seek(
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    work,
                    work.cancellation(),
                )
                .await?;
            carrier.check(work.cancellation(), Some(&self.cancellation))?;
            Ok(position)
        })
    }

    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content(
            ContentChange::Write {
                offset,
                bytes: bytes.bytes,
            },
            work,
        )
    }

    fn append_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content_at(
            ContentChange::Write {
                offset,
                bytes: bytes.bytes,
            },
            true,
            work,
        )
    }

    fn resize_with_meter(
        &self,
        logical_bytes: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Resize { logical_bytes }, work)
    }

    fn allocate_range_with_meter(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content(
            range_allocation(ByteRange { offset, length }, operation),
            work,
        )
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            drop(
                await_lock(
                    self.state.lock(),
                    &mut carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?,
            );
            Ok(())
        })
    }

    fn read_sparse_range(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let state = await_lock(
                self.state.lock(),
                &mut NativeDemandCarrier::Native(work),
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
            sparse::read_sparse(&state.file, offset, length, work, &self.cancellation).await
        })
    }

    fn write_sparse_range(
        &self,
        offset: u64,
        range: (MountSparseRange, crate::NativeAllocation),
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        sparse::validate_sparse_allocation(&range.1, work)?;
        invoke(&self.runtime, work, || async {
            let mut state = await_lock(
                self.state.lock(),
                &mut NativeDemandCarrier::Native(work),
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
            let changes =
                sparse::sparse_changes(offset, &range.0, state.file.logical_bytes(), work)?;
            for change in changes {
                state
                    .change_content(change, &self.ledger, &self.runtime.clock, work)
                    .await?;
            }
            Ok(())
        })
    }

    fn read_range_with_meter(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        invoke(&self.runtime, work, || {
            self.read_bytes(offset, length, &mut carrier)
        })
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        boundary(&self.cancellation, |work| self.lookup_with_meter(work))
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.read_range_with_meter(offset, length, work)
        })
        .map(|chunk| chunk.bytes)
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.read_up_to_with_meter(offset, maximum_bytes, work)
        })
        .map(|chunk| chunk.bytes)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.seek_with_meter(offset, target, work)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.change_content(
                ContentChange::Write {
                    offset,
                    bytes: bytes.into(),
                },
                work,
            )
        })
    }

    fn append_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.change_content_at(
                ContentChange::Write {
                    offset,
                    bytes: bytes.into(),
                },
                true,
                work,
            )
        })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.resize_with_meter(logical_bytes, work)
        })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.allocate_range_with_meter(offset, length, operation, work)
        })
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut state = await_lock(
                self.state.lock(),
                &mut carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
            require_attribute_root(metadata, state.metadata)?;
            state
                .file
                .set_attributes(metadata, logical_bytes, work, cancellation)
                .await?;
            state.metadata = metadata;
            state.record_change(&self.ledger, &mut carrier)
        })
    }

    fn read_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let name = inputs.attribute_name(name, self.profile, self.limits)?;
            let state = await_lock(
                self.state.lock(),
                inputs.carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
            let value = state
                .file
                .read_named_attribute(&name, work, cancellation)
                .await?;
            inputs
                .carrier
                .check(cancellation, Some(&self.cancellation))?;
            Ok(value)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.list_attributes_with_meter(cursor, maximum_entries, work)
        })
        .map(|(page, _)| page)
    }

    fn list_attributes_with_meter(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let cursor = cursor
                .map(|bytes| inputs.attribute_name(bytes, self.profile, self.limits))
                .transpose()?;
            let state = await_lock(
                self.state.lock(),
                inputs.carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(work, cancellation);
            let listing = state
                .file
                .list_named_attributes_with_accounting(
                    cursor.as_ref(),
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let page = inputs.attribute_page(&listing.0)?;
            drop(listing);
            drop(state);
            let bytes = cursor
                .as_ref()
                .map_or(0, |name| name.retained_bytes() as u64);
            drop(cursor);
            inputs.release(bytes)?;
            inputs
                .carrier
                .check(cancellation, Some(&self.cancellation))?;
            Ok((
                page,
                inputs.retain().ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?,
            ))
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        invoke(&self.runtime, work, || {
            self.change_attribute(name, Some((value.bytes.as_ref(), mode)), work)
        })
    }

    fn remove_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || {
            self.change_attribute(name, None, work)
        })
    }
}
