use super::*;

/// Exact creation/replacement precondition for one named attribute write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamedAttributeWriteMode {
    /// Insert when absent and replace when present.
    Upsert,
    /// Require authenticated absence.
    Create,
    /// Require authenticated presence.
    Replace,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {

    /// Reads ordered named attributes, retaining original names/vector custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_named_attributes_with_native_work(
        &self, root: ObjectId, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<(AttributeName, acyclic_stream::OwnedBytes)>, NativeAllocation), FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let mut attributes = Owned::<Vec<(AttributeName, acyclic_stream::OwnedBytes)>>::allocate(0, &mut accounting)?;
        let mut total = 0_u64;
        loop {
            let after = attributes.value.last().map(|(name, _)| name);
            let (value, allocation) = list_attributes_async_with_accounting(
                &self.volume.fs.inner.objects, root, after, 256,
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation, None,
            ).await?;
            let mut listing = (value, allocation);
            let has_more = listing.0.has_more;
            let retired = listing.0.entries.capacity().checked_mul(size_of::<crate::kernel::AttributeEntry>())
                .map(usize_to_u64).ok_or(WorkError::Overflow)?;
            attributes.allocation.absorb(&mut listing.1)?;
            for entry in listing.0.entries.drain(..) {
                total = total.saturating_add(entry.value_bytes);
                if total > self.volume.config.limits.maximum_read_bytes {
                    return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                }
                attributes.ensure_for_push(usize::MAX, &mut accounting)?;
                let read = read_blob_range_with_accounting(
                    &self.volume.fs.inner.objects, entry.value, ByteRange { offset: 0, length: entry.value_bytes },
                    DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
                ).await?;
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: size_of::<(AttributeName, acyclic_stream::OwnedBytes)>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                attributes.value.push((entry.name, read.bytes));
                accounting.record_completed(copied)?;
            }
            drop(listing);
            accounting.release_owned(&mut attributes.allocation, retired)?;
            if !has_more { break; }
        }
        Ok((attributes.value, native_pinned_allocation(attributes.allocation)?))
    }

    /// Reads every named attribute under one metadata attribute root, in
    pub async fn read_named_attributes(
        &self,
        root: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<(AttributeName, acyclic_stream::OwnedBytes)>> {
        let mut work = WorkCounters::default();
        let mut attributes = Vec::new();
        let mut total = 0_u64;
        let mut after: Option<AttributeName> = None;
        loop {
            let listing = list_attributes(
                &self.volume.fs.inner.objects,
                root,
                after.as_ref(),
                256,
                DecodeLimits::for_volume(self.volume.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeList))?;
            work = add(work, listing.work)?;
            for entry in listing.entries {
                total = total.saturating_add(entry.value_bytes);
                if total > self.volume.config.limits.maximum_read_bytes {
                    return Err(OperationFailure::new(
                        FsError::FileRead(FileRangeReadError::InvalidRange),
                        work,
                    ));
                }
                let read = read_blob_range(
                    &self.volume.fs.inner.objects,
                    entry.value,
                    ByteRange {
                        offset: 0,
                        length: entry.value_bytes,
                    },
                    DecodeLimits::for_volume(self.volume.config),
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
                work = add(work, read.work)?;
                after = Some(entry.name.clone());
                attributes.push((entry.name, read.bytes));
            }
            if !listing.has_more {
                break;
            }
        }
        Ok(FsReceipt {
            value: attributes,
            work,
        })
    }
}

pub(super) async fn read_named_attribute_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, metadata: FileMetadata, name: &AttributeName,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<crate::kernel::OwnedSourceChunk>, FsError> {
    let MetadataField::Value(root) = metadata.named_attributes else { return Ok(None); };
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (value, allocation) = crate::kernel::lookup_attribute_async_with_accounting(
            &volume.fs.inner.objects, root, name, DecodeLimits::for_volume(volume.config),
            accounting, cancellation,
        ).await?;
        let lookup = (value, allocation);
        let entry = lookup.0.as_ref().map(|entry| (entry.value, entry.value_bytes));
        drop(lookup);
        readonly_cleanup_standalone(accounting, before, 0)?;
        let Some((value, value_bytes)) = entry else { return Ok(None); };
        if value_bytes > volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let read = crate::kernel::read_blob_range_with_accounting(
            &volume.fs.inner.objects, value, ByteRange { offset: 0, length: value_bytes },
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await?;
        let owned = read.logical_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(Some(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, owned)));
        }
        Ok(Some(crate::kernel::OwnedSourceChunk::claimed(read.bytes, owned)))
    }.await;
    let retained = result.as_ref().ok().and_then(|chunk| chunk.as_ref())
        .map_or(0, |chunk| chunk.logical_owned_bytes);
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

pub(super) async fn list_named_attributes_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, metadata: FileMetadata, after: Option<&AttributeName>, maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let MetadataField::Value(root) = metadata.named_attributes else {
        return Ok((AttributeListing {
            entries: Vec::new(), has_more: false, next_residency: None, work: accounting.spent(),
        }, accounting.claim_owned(0, 0)?));
    };
    crate::kernel::list_attributes_async_with_accounting(
        &volume.fs.inner.objects, root, after, maximum_entries, DecodeLimits::for_volume(volume.config),
        accounting, cancellation, None,
    ).await
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Reads one exact attribute while preserving its producer allocation custody.
    pub async fn read_named_attribute_by_id<W: FsWork>(
        &mut self, file_id: FileId, name: &AttributeName,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
            read_named_attribute_to_file_root_with_accounting(&self.volume, metadata, name, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of named attributes by stable file identity.
    pub async fn list_named_attributes_by_id(
        &mut self, file_id: FileId, after: Option<&AttributeName>, maximum_entries: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_by_id_with_accounting(
            file_id, after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(mut value, allocation)| {
            drop(allocation); value.work = work; FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_by_id_with_accounting(
        &mut self, file_id: FileId, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
        list_named_attributes_to_file_root_with_accounting(&self.volume, metadata, after, maximum_entries, accounting, cancellation).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute_by_id<W: FsWork>(
        &mut self, file_id: FileId, name: &AttributeName, value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            let accounting = &mut accounting;
            let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
            let metadata = Self::change_named_attribute_metadata_with_accounting(
                &self.volume, metadata, name, value, None, accounting, cancellation,
            ).await?;
            self.set_attributes_by_id_with_accounting(file_id, metadata, None, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn change_named_attribute_metadata_with_accounting(
        volume: &Volume<A, O>, mut metadata: FileMetadata, name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        maximum_value_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let root = match metadata.named_attributes {
            MetadataField::Value(root) => root,
            MetadataField::Unavailable if value.is_some() => {
                let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_attribute_page(&AttributePage::Leaf(Vec::new()), volume.config.limits.maximum_directory_page_entries, observe, allocation_error))?;
                volume.fs.put_encoded_with_accounting(ObjectKind::AttributePage, encoded, accounting, cancellation).await?
            },
            MetadataField::Unavailable => return Err(FsError::NotFound),
        };
        let (current, allocation) = crate::kernel::lookup_attribute_async_with_accounting(
            &volume.fs.inner.objects, root, name,
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await?;
        let current_bytes = current.as_ref().map_or(0, |entry| crate::foundation::usize_to_u64(entry.name.retained_bytes()));
        let current = crate::kernel::Owned::new(current, allocation, current_bytes);
        let result = async {
            match (value.map(|(_, mode)| mode), current.value.as_ref()) {
                (Some(NamedAttributeWriteMode::Create), Some(_))
                | (Some(NamedAttributeWriteMode::Replace), None) => return Err(FsError::CreationRejected),
                (None, None) => return Err(FsError::NotFound),
                _ => {}
            }
            let blob = if let Some((bytes, _)) = value {
                let source_bytes = crate::foundation::usize_to_u64(bytes.len());
                if maximum_value_bytes.is_some_and(|maximum| source_bytes > maximum) {
                    return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                }
                let mut source = std::io::Cursor::new(bytes);
                Some(stage_content_for_volume_with_accounting(
                    volume, &mut source, source_bytes.max(1), accounting, cancellation,
                ).await?)
            } else { None };
            let mut observer = crate::kernel::OriginalReadAccounting::new(accounting)?;
            let cloned = name.try_clone::<FsError>(
                DecodeLimits::for_volume(volume.config).maximum_name_bytes,
                &mut |step| observer.observe(step),
            );
            match cloned {
                Ok(name) => {
                    let (allocation, bytes) = observer.into_parts();
                    Ok((blob, crate::kernel::Owned::new(name, allocation, bytes)))
                }
                Err(error) => {
                    observer.release_failed()?;
                    Err(error)
                }
            }
        }.await;
        let (blob, name) = match result {
            Ok(value) => value,
            Err(error) => { current.retire(accounting)?; return Err(error); }
        };
        let crate::kernel::Owned { value: current, allocation: mut custody, bytes: current_bytes } = current;
        let crate::kernel::Owned { value: name, allocation: name_custody, bytes: name_bytes } = name;
        let mutation = match blob {
            Some(blob) => {
                let replacement = AttributeEntry { name, value_bytes: blob.logical_bytes, value: blob.root };
                match current {
                    Some(expected) => AttributeMutation::Replace { expected, replacement },
                    None => AttributeMutation::Insert(replacement),
                }
            }
            None => AttributeMutation::Remove { name, expected: current },
        };
        let mutation = crate::kernel::Owned::new(mutation, name_custody, name_bytes);
        let changed = crate::kernel::apply_attribute_mutations_with_accounting(
            &volume.fs.inner.objects, root, std::slice::from_ref(&mutation.value), 1,
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await;
        mutation.retire(accounting)?;
        accounting.release_owned(&mut custody, current_bytes)?;
        metadata.named_attributes = MetadataField::Value(changed?.value);
        Ok(metadata)
    }

    /// Reads the same exact named attribute, preserving its producer's actual
    pub async fn read_named_attribute<W: FsWork>(
        &mut self, path: &NamespacePath, name: &AttributeName,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = self.read_metadata_with_accounting(path, accounting, cancellation).await?;
            read_named_attribute_to_file_root_with_accounting(&self.volume, metadata, name, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of exact named-attribute identities without
    pub async fn list_named_attributes(
        &mut self, path: &NamespacePath, after: Option<&AttributeName>, maximum_entries: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_with_accounting(path, after, maximum_entries, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|(mut value, allocation)| {
            drop(allocation); value.work = work; FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_with_accounting(
        &mut self, path: &NamespacePath, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = self.read_metadata_with_accounting(path, accounting, cancellation).await?;
        list_named_attributes_to_file_root_with_accounting(&self.volume, metadata, after, maximum_entries, accounting, cancellation).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute(
        &mut self, path: NamespacePath, name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let lookup = self.lookup_no_follow_with_metadata_with_accounting(
                &path, &mut accounting, cancellation,
            ).await?.ok_or(FsError::NotFound)?;
            let metadata = Self::change_named_attribute_metadata_with_accounting(
                &self.volume, lookup.metadata, name, value,
                Some(self.volume.config.limits.maximum_read_bytes), &mut accounting, cancellation,
            ).await?;
            self.set_attributes_by_id_with_accounting(lookup.record.file_id, metadata, None, &mut accounting, cancellation).await
        }.await;
        measured(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {

    /// Reads one exact detached attribute with its producer allocation custody.
    pub async fn read_named_attribute<W: FsWork>(
        &self,
        name: &AttributeName,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, self.record, accounting, cancellation,
            ).await?;
            read_named_attribute_to_file_root_with_accounting(
                &self.volume, metadata, name, accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of detached named-attribute identities.
    pub async fn list_named_attributes(
        &self,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_with_accounting(
            after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(value, _allocation)| FsReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_with_accounting(
        &self, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = read_record_metadata_to_file_root_with_accounting(
            &self.volume, self.record, accounting, cancellation,
        ).await?;
        list_named_attributes_to_file_root_with_accounting(
            &self.volume, metadata, after, maximum_entries, accounting, cancellation,
        ).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute<W: FsWork>(
        &mut self, name: &AttributeName, value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, self.record, accounting, cancellation,
            ).await?;
            let metadata = Checkout::<A, O>::change_named_attribute_metadata_with_accounting(
                &self.volume, metadata, name, value, None, accounting, cancellation,
            ).await?;
            self.set_attributes_with_accounting(metadata, None, accounting, cancellation).await?;
            Ok(metadata)
        }.await;
        budget.finish(result, &accounting)
    }
}

