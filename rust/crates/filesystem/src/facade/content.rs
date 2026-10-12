use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Changes content by namespace path or stable file identity.
    pub async fn change_content<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        change: ContentChange<F>,
        times: ContentTimes,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            let input = (file, change);
            let bytes = content_input_path_bytes(&input)?;
            let allocation = accounting.claim_owned(bytes, 0)?;
            let compiled = self.compile_content_with_accounting(
                crate::kernel::Owned::new(input, allocation, bytes),
                times, &mut accounting, cancellation,
            ).await?;
            self.mutate_owned_with_accounting(compiled, &mut accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Input custody covers paths; opaque source custody stays with its caller
    pub(crate) async fn compile_content_with_accounting<F: ContentFile>(
        &mut self,
        input: crate::kernel::Owned<(F, ContentChange<F>)>,
        times: ContentTimes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Mutation>>, FsError> {
        let mut input = crate::kernel::Owned::new(Some(input.value), input.allocation, input.bytes);
        let result = async {
            accounting.check()?;
            let mut operations = crate::kernel::Owned::allocate(
                1 + usize::from(matches!(times, ContentTimes::Stamp(_))), accounting,
            )?;
            let lowered = async {
                let (file, change) = input.value.take().expect("content input is consumed once");
                let operation = match change {
                    ContentChange::Write { bytes, .. } if bytes.is_empty() => ContentOperation::ValidateRegular,
                    ContentChange::Write { offset, bytes } => {
                        let content = self.stage_authored_blob_with_accounting(bytes, accounting, cancellation).await?;
                        ContentOperation::Write { offset, length: content.logical_bytes, content: content.root }
                    }
                    ContentChange::Resize { logical_bytes } => ContentOperation::Resize { logical_bytes },
                    ContentChange::ZeroRange { range, allocated, extend } => ContentOperation::ZeroRange { range, allocated, extend },
                    ContentChange::Preallocate { range, keep_size } => ContentOperation::Preallocate { range, keep_size },
                    ContentChange::CloneFrom { source, source_offset, offset, length } => ContentOperation::CloneFrom { source, source_offset, offset, length },
                };
                let mut stamped_metadata = None;
                if let ContentTimes::Stamp(at_ns) = times {
                    let mut metadata = match file.address() {
                        ContentAddress::Path(path) => self.read_metadata_with_accounting(path, accounting, cancellation).await?,
                        ContentAddress::File(file_id) => self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?,
                    };
                    if metadata.stamp_content_change(at_ns) {
                        stamped_metadata = Some(self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?);
                    }
                }
                if let Some(metadata) = stamped_metadata {
                    let cloned = file.clone_content_with_accounting(accounting, cancellation)?;
                    let crate::kernel::Owned {
                        value: cloned, allocation: mut cloned_allocation, bytes: mut cloned_bytes,
                    } = cloned;
                    let mutation = cloned.mutation(operation);
                    // A clone operation also moves its original source path.
                    let source_bytes = mutation_path_owned_bytes(&mutation)
                        .checked_sub(cloned_bytes).ok_or(WorkError::Overflow)?;
                    let mut source_allocation = crate::kernel::split_allocation(&mut input.allocation, source_bytes)?;
                    input.bytes = input.bytes.checked_sub(source_bytes).ok_or(WorkError::Overflow)?;
                    if let Err(error) = cloned_allocation.absorb(&mut source_allocation) {
                        drop(mutation);
                        crate::kernel::retire_allocation(source_allocation, source_bytes, accounting)?;
                        crate::kernel::retire_allocation(cloned_allocation, cloned_bytes, accounting)?;
                        return Err(error);
                    }
                    cloned_bytes = cloned_bytes.checked_add(source_bytes).ok_or(WorkError::Overflow)?;
                    let pushed = push_authored_operation(
                        &mut operations, &mut cloned_allocation, &mut cloned_bytes, accounting, mutation,
                    );
                    crate::kernel::retire_allocation(cloned_allocation, cloned_bytes, accounting)?;
                    pushed?;
                    push_authored_operation(
                        &mut operations, &mut input.allocation, &mut input.bytes, accounting,
                        file.mutation(ContentOperation::SetMetadata { metadata }),
                    )?;
                } else {
                    push_authored_operation(
                        &mut operations, &mut input.allocation, &mut input.bytes, accounting,
                        file.mutation(operation),
                    )?;
                }
                Ok::<_, FsError>(())
            }.await;
            if let Err(error) = lowered {
                operations.retire(accounting)?;
                return Err(error);
            }
            Ok(operations)
        }.await;
        input.retire(accounting)?;
        result
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {

    /// Applies one content change and, when `metadata` is given, replaces
    pub async fn change_content<W: FsWork>(
        &mut self,
        change: ContentChange<std::convert::Infallible>,
        metadata: Option<FileMetadata>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            accounting.check()?;
            let mutation = match change {
                ContentChange::Write { bytes, .. } if bytes.is_empty() => None,
                ContentChange::Write { offset, bytes } => {
                    if bytes.len() as u64 > self.volume.config.limits.maximum_read_bytes {
                        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                    }
                    let mut source = std::io::Cursor::new(bytes.as_ref());
                    let content = stage_content_for_volume_with_accounting(
                        &self.volume, &mut source, (bytes.len() as u64).max(1),
                        accounting, cancellation,
                    ).await;
                    drop(source);
                    drop(bytes);
                    let content = content?;
                    Some(RegularMutation::Write {
                        offset, length: content.logical_bytes, content: content.root, content_offset: 0,
                    })
                }
                ContentChange::Resize { logical_bytes } => Some(RegularMutation::Resize { logical_bytes }),
                ContentChange::ZeroRange { range, allocated, extend } => Some(RegularMutation::ZeroRange {
                    offset: range.offset, length: range.length, allocated, extend,
                }),
                ContentChange::Preallocate { range, keep_size } => Some(RegularMutation::Preallocate {
                    offset: range.offset, length: range.length, keep_size,
                }),
                ContentChange::CloneFrom { source, .. } => match source {},
            };
            let payload = match mutation {
                Some(mutation) => crate::kernel::apply_regular_mutation_with_accounting(
                    &self.volume.fs.inner.objects, self.record.payload, mutation,
                    self.volume.config, accounting, cancellation,
                ).await?.payload,
                None => self.record.payload,
            };
            let metadata_id = match metadata {
                Some(metadata) => {
                    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
                    self.volume.fs.put_encoded_with_accounting(
                        ObjectKind::Metadata, encoded, accounting, cancellation,
                    ).await?
                }
                None => self.record.metadata,
            };
            accounting.check()?;
            self.record.payload = payload;
            self.record.metadata = metadata_id;
            Ok(())
        }.await;
        budget.finish(result, &accounting)
    }
}

