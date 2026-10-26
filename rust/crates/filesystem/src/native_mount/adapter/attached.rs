struct CheckoutAttachedFile<A: 'static, O: 'static> {
    checkout: Arc<SharedCheckout<A, O>>,
    file_id: FileId,
    deadline: Option<u64>,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    _native_allocation: crate::NativeAllocation,
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
                ViewGate::callback_owner()?,
                &mut carrier,
                work.cancellation(),
                Some(&self.cancellation),
            )
            .await?;
        let checkout = observation.observer(
            &mut crate::kernel::allocation::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )?;
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
        let name = inputs.mutation_attribute_name(name, self.profile, self.limits)?;
        let mut candidate = self
            .checkout
            .file_candidate(
                self.file_id,
                inputs.carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
        candidate.deadline = self.deadline;
        candidate
            .checkout
            .change_named_attribute(self.file_id, &name, value, work, cancellation)
            .await?;
        self.checkout
            .install_change(
                candidate,
                ViewChange::Node(self.file_id),
                work,
                &self.cancellation,
                self.native_callback_guard.clone(),
            )
            .await?;
        Ok(())
    }

    fn change_content(
        &self,
        change: ContentChange<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.change_content_at(change, false, work)
    }

    fn change_content_at(
        &self,
        change: ContentChange<FileId>,
        append_only: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || {
            self.checkout.change_file_content(
                self.file_id,
                change,
                append_only,
                ContentChangeContext {
                    work,
                    deadline: self.deadline,
                    lifetime: &self.cancellation,
                    clock: &self.runtime.clock,
                    guard: self.native_callback_guard.clone(),
                },
            )
        })
    }
}

impl<A, O> MountOpenFile for CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn clone_source(
        &self,
        checkout: &(dyn std::any::Any + Send + Sync),
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileId, MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let checkout = checkout
            .downcast_ref::<Arc<SharedCheckout<A, O>>>()
            .ok_or(MountSourceError::NativeUnsupported)?;
        if checkout.volume_id != self.checkout.volume_id {
            return Err(MountSourceError::AccessDenied);
        }
        if !Arc::ptr_eq(checkout, &self.checkout) {
            return Err(MountSourceError::NativeUnsupported);
        }
        Ok(self.file_id)
    }

    fn clone_range_from(
        &self,
        source: &dyn MountOpenFile,
        source_offset: u64,
        offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let source = source.clone_source(&self.checkout, work)?;
        self.change_content(
            ContentChange::CloneFrom {
                source,
                source_offset,
                offset,
                length,
            },
            work,
        )
    }

    fn lookup_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut checkout = self.observer(work).await?;
            let record = checkout
                .read_file_record_by_id(self.file_id, work, work.cancellation())
                .await?;
            let metadata = checkout
                .read_metadata(self.file_id, work, work.cancellation())
                .await?;
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
                .read_file_range(
                    self.file_id,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    work,
                    work.cancellation(),
                )
                .await?;
            let capacity = read.logical_owned_bytes();
            NativeDemandCarrier::Native(work)
                .check(work.cancellation(), Some(&self.cancellation))?;
            Ok(crate::kernel::OwnedSourceChunk::intrinsic(
                read.bytes, capacity,
            ))
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
                .read_file_up_to(
                    self.file_id,
                    offset,
                    maximum_bytes,
                    work,
                    work.cancellation(),
                )
                .await?;
            let capacity = read.logical_owned_bytes();
            NativeDemandCarrier::Native(work)
                .check(work.cancellation(), Some(&self.cancellation))?;
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
            let mut checkout = self.observer(work).await?;
            let position = checkout
                .seek_file_extent(
                    self.file_id,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    work,
                    work.cancellation(),
                )
                .await?;
            NativeDemandCarrier::Native(work)
                .check(work.cancellation(), Some(&self.cancellation))?;
            Ok(position)
        })
    }

    checkout_file_callbacks!();

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
                .await?;
            let file = checkout.detached_from_record(record, &work, work.cancellation())?;
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
            let mut candidate = self
                .checkout
                .file_candidate(
                    self.file_id,
                    &mut NativeDemandCarrier::Native(work),
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            candidate.deadline = self.deadline;
            let record = candidate
                .checkout
                .read_file_record_by_id(self.file_id, work, cancellation)
                .await?;
            let changes =
                sparse::sparse_changes(offset, &range.0, mount_node(record).logical_bytes, work)?;
            let times = ContentTimes::Stamp(content_change_time(&self.runtime.clock)?);
            for change in changes {
                candidate
                    .checkout
                    .change_content(self.file_id, change, times, work, cancellation)
                    .await?;
            }
            self.checkout
                .install_change(
                    candidate,
                    ViewChange::Node(self.file_id),
                    work,
                    &self.cancellation,
                    self.native_callback_guard.clone(),
                )
                .await?;
            Ok(())
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
            let mut candidate = self
                .checkout
                .file_candidate(
                    self.file_id,
                    &mut NativeDemandCarrier::Native(work),
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let current = candidate
                .checkout
                .read_metadata(self.file_id, work, cancellation)
                .await?;
            require_attribute_root(metadata, current)?;
            candidate.deadline = self.deadline;
            candidate
                .checkout
                .set_attributes(self.file_id, metadata, logical_bytes, work, cancellation)
                .await?;
            self.checkout
                .install_change(
                    candidate,
                    ViewChange::Node(self.file_id),
                    work,
                    &self.cancellation,
                    self.native_callback_guard.clone(),
                )
                .await?;
            Ok(())
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
            let mut checkout = self.observer(work).await?;
            let value = checkout
                .read_named_attribute(self.file_id, &name, work, cancellation)
                .await?;
            inputs
                .carrier
                .check(cancellation, Some(&self.cancellation))?;
            Ok(value)
        })
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
                .list_named_attributes_with_accounting(
                    self.file_id,
                    cursor.as_ref(),
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await?;
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
            Ok((
                page,
                inputs.retain().ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?,
            ))
        })
    }
}
