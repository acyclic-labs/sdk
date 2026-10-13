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
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            let input = (file, change);
            let bytes = content_input_path_bytes(&input)?;
            let allocation = accounting.claim_owned(bytes, 0)?;
            let compiled = self
                .compile_content_with_accounting(
                    Owned::new(input, allocation, bytes),
                    times,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            self.mutate_owned_with_accounting(compiled, &mut accounting, cancellation)
                .await
        }
        .await;
        budget.finish(result, &accounting)
    }

    async fn content_file_id<F: ContentFile>(
        &mut self,
        file: &F,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        match file.address() {
            ContentAddress::File(id) => Ok(id),
            ContentAddress::Path(path) => {
                let (lookup, allocation) = self
                    .lookup_no_follow_with_accounting(&path, accounting, cancellation, true)
                    .await?;
                drop(allocation);
                Ok(lookup.record.ok_or(FsError::NotFound)?.file_id)
            }
        }
    }

    pub(crate) async fn compile_content_with_accounting<F: ContentFile>(
        &mut self,
        input: Owned<(F, ContentChange<F>)>,
        times: ContentTimes,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<Vec<Mutation>>, FsError> {
        let mut input = Owned::new(Some(input.value), input.allocation, input.bytes);
        let result = async {
            accounting.check()?;
            let mut operations = Owned::<Vec<Mutation>>::allocate(
                1 + usize::from(matches!(times, ContentTimes::Stamp(_))),
                accounting,
            )?;
            let lowered = async {
                let (file, change) = input.value.take().expect("content input is consumed once");
                let file_id = self
                    .content_file_id(&file, accounting, cancellation)
                    .await?;
                let mutation = match change {
                    ContentChange::CloneFrom {
                        source,
                        source_offset,
                        offset,
                        length,
                    } => {
                        let source_file_id = self
                            .content_file_id(&source, accounting, cancellation)
                            .await?;
                        Mutation::CloneFileRange {
                            source_file_id,
                            source_offset,
                            destination_file_id: file_id,
                            destination_offset: offset,
                            length,
                        }
                    }
                    change => Mutation::File {
                        file_id,
                        mutation: match change {
                            ContentChange::Write { bytes, .. } if bytes.is_empty() => {
                                FileMutation::ValidateRegular
                            }
                            ContentChange::Write { offset, bytes } => {
                                let content = self
                                    .stage_authored_blob_with_accounting(
                                        bytes,
                                        accounting,
                                        cancellation,
                                    )
                                    .await?;
                                FileMutation::Write {
                                    offset,
                                    length: content.logical_bytes,
                                    content: content.root,
                                    content_offset: 0,
                                }
                            }
                            ContentChange::Resize { logical_bytes } => {
                                FileMutation::Resize { logical_bytes }
                            }
                            ContentChange::ZeroRange {
                                range,
                                allocated,
                                extend,
                            } => FileMutation::ZeroRange {
                                offset: range.offset,
                                length: range.length,
                                allocated,
                                extend,
                            },
                            ContentChange::Preallocate { range, keep_size } => {
                                FileMutation::Preallocate {
                                    offset: range.offset,
                                    length: range.length,
                                    keep_size,
                                }
                            }
                            ContentChange::CloneFrom { .. } => unreachable!("clone handled above"),
                        },
                    },
                };
                push_authored_operation(
                    &mut operations,
                    &mut input.allocation,
                    &mut input.bytes,
                    accounting,
                    mutation,
                )?;
                if let ContentTimes::Stamp(at_ns) = times {
                    let mut metadata = self
                        .read_metadata_by_id_with_accounting(file_id, accounting, cancellation)
                        .await?;
                    if metadata.stamp_content_change(at_ns) {
                        let metadata = self
                            .stage_metadata_with_accounting(metadata, accounting, cancellation)
                            .await?;
                        push_authored_operation(
                            &mut operations,
                            &mut input.allocation,
                            &mut input.bytes,
                            accounting,
                            Mutation::File {
                                file_id,
                                mutation: FileMutation::SetMetadata { metadata },
                            },
                        )?;
                    }
                }
                Ok::<_, FsError>(())
            }
            .await;
            if let Err(error) = lowered {
                operations.retire(accounting)?;
                return Err(error);
            }
            Ok(operations)
        }
        .await;
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
                        &self.volume,
                        &mut source,
                        (bytes.len() as u64).max(1),
                        accounting,
                        cancellation,
                    )
                    .await;
                    drop(source);
                    drop(bytes);
                    let content = content?;
                    Some(RegularMutation::Write {
                        offset,
                        length: content.logical_bytes,
                        content: content.root,
                        content_offset: 0,
                    })
                }
                ContentChange::Resize { logical_bytes } => {
                    Some(RegularMutation::Resize { logical_bytes })
                }
                ContentChange::ZeroRange {
                    range,
                    allocated,
                    extend,
                } => Some(RegularMutation::ZeroRange {
                    offset: range.offset,
                    length: range.length,
                    allocated,
                    extend,
                }),
                ContentChange::Preallocate { range, keep_size } => {
                    Some(RegularMutation::Preallocate {
                        offset: range.offset,
                        length: range.length,
                        keep_size,
                    })
                }
                ContentChange::CloneFrom { source, .. } => match source {},
            };
            let payload = match mutation {
                Some(mutation) => {
                    crate::kernel::apply_regular_mutation_with_accounting(
                        &self.volume.fs.inner.objects,
                        self.record.payload,
                        mutation,
                        self.volume.config,
                        accounting,
                        cancellation,
                    )
                    .await?
                    .payload
                }
                None => self.record.payload,
            };
            let metadata_id = match metadata {
                Some(metadata) => {
                    let encoded = crate::kernel::codec::encode_owned(
                        accounting,
                        |observe, allocation_error| {
                            crate::kernel::encode_file_metadata(metadata, observe, allocation_error)
                        },
                    )?;
                    self.volume
                        .fs
                        .put_encoded_with_accounting(
                            ObjectKind::Metadata,
                            encoded,
                            accounting,
                            cancellation,
                        )
                        .await?
                }
                None => self.record.metadata,
            };
            accounting.check()?;
            self.record.payload = payload;
            self.record.metadata = metadata_id;
            Ok(())
        }
        .await;
        budget.finish(result, &accounting)
    }
}
