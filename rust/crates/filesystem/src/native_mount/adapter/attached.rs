struct CheckoutAttachedFile<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    file_id: FileId,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    _native_allocation: Option<crate::NativeAllocation>,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
}

impl<A, O> CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn observer(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Checkout<A, O>, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let observation = self
            .checkout
            .observe(
                ViewGate::callback_owner(),
                &mut carrier,
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
        let checkout = observation
            .observer(work, work.cancellation())
            .map_err(fs_error)?;
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        Ok(checkout)
    }

    async fn change_attribute(
        &self,
        name: &[u8],
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let cancellation = work.cancellation();
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let name = inputs.attribute_name(name, self.profile, self.limits)?;
        let (mut candidate, epoch, stamp) = self
            .checkout
            .candidate(None, inputs.carrier, cancellation, Some(&self.cancellation))
            .await?;
        candidate
            .checkout
            .change_named_attribute_by_id(self.file_id, &name, value, work, cancellation)
            .await
            .map_err(fs_error)?;
        let publication = self
            .checkout
            .install_change(
                candidate,
                epoch,
                stamp,
                ViewChange::Node(self.file_id),
                work,
                &self.cancellation,
            )
            .await?;
        if publication == MountPublication::PerMutation {
            self.checkout
                .publish(
                    crate::PublicationPermit::Unrestricted,
                    work,
                    cancellation,
                    Some(&self.cancellation),
                    self.native_callback_guard.clone(),
                )
                .await?;
        }
        Ok(())
    }

    fn change_content(
        &self,
        change: ContentChange<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let (mut candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    &mut NativeDemandCarrier::Native(work),
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            candidate
                .checkout
                .change_content(
                    self.file_id,
                    change,
                    ContentTimes::Stamp(content_change_time()?),
                    work,
                    cancellation,
                )
                .await
                .map_err(fs_error)?;
            let publication = self
                .checkout
                .install_change(
                    candidate,
                    epoch,
                    stamp,
                    ViewChange::Node(self.file_id),
                    work,
                    &self.cancellation,
                )
                .await?;
            if publication == MountPublication::PerMutation {
                self.checkout
                    .publish(
                        crate::PublicationPermit::Unrestricted,
                        work,
                        cancellation,
                        Some(&self.cancellation),
                        self.native_callback_guard.clone(),
                    )
                    .await?;
            }
            Ok(())
        })
    }
}

impl<A, O> MountOpenFile for CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let record = checkout
                .read_file_record_by_id(self.file_id, work, work.cancellation())
                .await
                .map_err(fs_error)?;
            let metadata = checkout
                .read_metadata_by_id(self.file_id, work, work.cancellation())
                .await
                .map_err(fs_error)?;
            NativeDemandCarrier::Native(work)
                .check(work.cancellation(), Some(&self.cancellation))?;
            Ok(MountLookup {
                node: mount_node(record),
                metadata,
            })
        })
    }

    fn read_range_with_meter(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let read = checkout
                .read_file_range_by_id(
                    self.file_id,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(fs_error)?;
            let capacity = read.logical_owned_bytes();
            NativeDemandCarrier::Native(work).finish_read(
                crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, capacity),
                work.cancellation(),
                Some(&self.cancellation),
            )
        })
    }

    fn read_up_to_with_meter(
        &self,
        offset: u64,
        maximum_bytes: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let read = checkout
                .read_file_up_to_by_id(
                    self.file_id,
                    offset,
                    maximum_bytes,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(fs_error)?;
            let capacity = read.logical_owned_bytes();
            NativeDemandCarrier::Native(work).finish_read(
                crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, capacity),
                work.cancellation(),
                Some(&self.cancellation),
            )
        })
    }

    fn seek_with_meter(
        &self,
        offset: u64,
        target: MountSeekTarget,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let position = checkout
                .seek_file_extent_by_id(
                    self.file_id,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(fs_error)?;
            NativeDemandCarrier::Native(work)
                .check(work.cancellation(), Some(&self.cancellation))?;
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
            let checkout = await_lock(
                self.checkout.lock(),
                &mut NativeDemandCarrier::Native(work),
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
            checkout.ensure_publication_resolved()
        })
    }

    fn read_sparse_range(
        &self,
        offset: u64,
        length: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let record = checkout
                .read_file_record_by_id(self.file_id, work, work.cancellation())
                .await
                .map_err(fs_error)?;
            let file = checkout
                .detached_from_record(record, work, work.cancellation())
                .map_err(fs_error)?;
            sparse::read_sparse(&file, offset, length, work, &self.cancellation).await
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
            let cancellation = work.cancellation();
            let (mut candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    &mut NativeDemandCarrier::Native(work),
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let record = candidate
                .checkout
                .read_file_record_by_id(self.file_id, work, cancellation)
                .await
                .map_err(fs_error)?;
            let changes =
                sparse::sparse_changes(offset, &range.0, mount_node(record).logical_bytes, work)?;
            let times = ContentTimes::Stamp(content_change_time()?);
            for change in changes {
                candidate
                    .checkout
                    .change_content(self.file_id, change, times, work, cancellation)
                    .await
                    .map_err(fs_error)?;
            }
            let publication = self
                .checkout
                .install_change(
                    candidate,
                    epoch,
                    stamp,
                    ViewChange::Node(self.file_id),
                    work,
                    &self.cancellation,
                )
                .await?;
            if publication == MountPublication::PerMutation {
                self.checkout
                    .publish(
                        crate::PublicationPermit::Unrestricted,
                        work,
                        cancellation,
                        Some(&self.cancellation),
                        self.native_callback_guard.clone(),
                    )
                    .await?;
            }
            Ok(())
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
            let (mut candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    &mut NativeDemandCarrier::Native(work),
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            candidate
                .checkout
                .set_attributes_by_id(self.file_id, metadata, logical_bytes, work, cancellation)
                .await
                .map_err(fs_error)?;
            let publication = self
                .checkout
                .install_change(
                    candidate,
                    epoch,
                    stamp,
                    ViewChange::Node(self.file_id),
                    work,
                    &self.cancellation,
                )
                .await?;
            if publication == MountPublication::PerMutation {
                self.checkout
                    .publish(
                        crate::PublicationPermit::Unrestricted,
                        work,
                        cancellation,
                        Some(&self.cancellation),
                        self.native_callback_guard.clone(),
                    )
                    .await?;
            }
            Ok(())
        })
    }

    fn read_attribute(
        &self,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let name = inputs.attribute_name(name, self.profile, self.limits)?;
            let observation = self
                .checkout
                .observe(
                    owner,
                    inputs.carrier,
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let mut checkout = inputs.observer(&observation, cancellation)?;
            let value = checkout
                .read_named_attribute_by_id(self.file_id, &name, work, cancellation)
                .await
                .map_err(fs_error)?;
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
            let mut checkout = self.observer(work).await?;
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(work, cancellation);
            let listing = checkout
                .list_named_attributes_by_id_with_accounting(
                    self.file_id,
                    cursor.as_ref(),
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(fs_error)?;
            let page = inputs.attribute_page(&listing.0)?;
            drop(listing);
            drop(checkout);
            let bytes = cursor
                .as_ref()
                .map_or(0, |name| name.retained_bytes() as u64);
            drop(cursor);
            inputs.release(bytes)?;
            inputs
                .carrier
                .check(cancellation, Some(&self.cancellation))?;
            Ok((page, inputs.retain().expect("native attribute page")))
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
