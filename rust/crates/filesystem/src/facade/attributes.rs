use super::*;
use crate::kernel::{
    OwnedSourceChunk, apply_attribute_mutations_with_accounting, decode_owned,
    encode_attribute_page, lookup_attribute_async_with_accounting,
};

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

pub(super) async fn read_attribute<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    metadata: FileMetadata,
    name: &AttributeName,
    maximum_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<OwnedSourceChunk>, FsError> {
    let MetadataField::Value(root) = metadata.named_attributes else {
        return Ok(None);
    };
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (entry, allocation) = lookup_attribute_async_with_accounting(
            &volume.fs.inner.objects,
            root,
            name,
            DecodeLimits::for_volume(volume.config),
            accounting,
            cancellation,
        )
        .await?;
        let bytes = entry
            .as_ref()
            .map_or(0, |entry| usize_to_u64(entry.name.retained_bytes()));
        let entry = Owned::new(entry, allocation, bytes);
        let value = entry
            .value
            .as_ref()
            .map(|entry| (entry.value, entry.value_bytes));
        entry.retire(accounting)?;
        readonly_cleanup_standalone(accounting, before, 0)?;
        let Some((value, length)) = value else {
            return Ok(None);
        };
        if length > maximum_bytes.min(volume.config.limits.maximum_read_bytes) {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let read = read_blob_range_with_accounting(
            &volume.fs.inner.objects,
            value,
            ByteRange { offset: 0, length },
            DecodeLimits::for_volume(volume.config),
            accounting,
            cancellation,
            true,
        )
        .await?;
        let bytes = read.logical_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(Some(OwnedSourceChunk::intrinsic(read.bytes, bytes)));
        }
        Ok(Some(OwnedSourceChunk::claimed(read.bytes, bytes)))
    }
    .await;
    let retained = result
        .as_ref()
        .ok()
        .and_then(Option::as_ref)
        .map_or(0, |chunk| chunk.logical_owned_bytes);
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

async fn list_attributes<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    metadata: FileMetadata,
    after: Option<&AttributeName>,
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(AttributeListing, KernelOwnedAllocation), FsError> {
    match metadata.named_attributes {
        MetadataField::Unavailable => Ok((
            AttributeListing {
                entries: Vec::new(),
                has_more: false,
                next_residency: None,
                work: accounting.spent(),
            },
            accounting.claim_owned(0, 0)?,
        )),
        MetadataField::Value(root) => {
            list_attributes_async_with_accounting(
                &volume.fs.inner.objects,
                root,
                after,
                maximum_entries,
                DecodeLimits::for_volume(volume.config),
                accounting,
                cancellation,
                None,
            )
            .await
        }
    }
}

pub(super) async fn change_attribute<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    mut metadata: FileMetadata,
    name: &AttributeName,
    value: Option<(&[u8], NamedAttributeWriteMode)>,
    maximum_value_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileMetadata, FsError> {
    let limits = DecodeLimits::for_volume(volume.config);
    let root = attribute_root(
        volume,
        metadata.named_attributes,
        value.map(|(_, mode)| mode),
        accounting,
        cancellation,
    )
    .await?;
    let (entry, allocation) = lookup_attribute_async_with_accounting(
        &volume.fs.inner.objects,
        root,
        name,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let bytes = entry
        .as_ref()
        .map_or(0, |entry| usize_to_u64(entry.name.retained_bytes()));
    let mut current = Owned::new(entry, allocation, bytes);
    let result = async {
        require_attribute_mode(value.map(|(_, mode)| mode), current.value.is_some())?;
        let blob = if let Some((bytes, _)) = value {
            let length = usize_to_u64(bytes.len());
            if length > maximum_value_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            Some(
                stage_content_for_volume_with_accounting(
                    volume,
                    &mut std::io::Cursor::new(bytes),
                    length.max(1),
                    accounting,
                    cancellation,
                )
                .await?,
            )
        } else {
            None
        };
        let (name, allocation) = decode_owned(accounting, |observe| {
            name.try_clone(limits.maximum_name_bytes, &mut |step| observe(step))
        })?;
        let mut name = Owned::new(name, allocation, 0);
        name.bytes = usize_to_u64(name.value.retained_bytes());
        let bytes = current
            .bytes
            .checked_add(name.bytes)
            .ok_or(WorkError::Overflow)?;
        if let Err(error) = current.allocation.absorb(&mut name.allocation) {
            name.retire(accounting)?;
            return Err(error);
        }
        current.bytes = bytes;
        let mutation = match blob {
            Some(blob) => {
                let replacement = AttributeEntry {
                    name: name.value,
                    value_bytes: blob.logical_bytes,
                    value: blob.root,
                };
                match current.value.take() {
                    Some(expected) => AttributeMutation::Replace {
                        expected,
                        replacement,
                    },
                    None => AttributeMutation::Insert(replacement),
                }
            }
            None => AttributeMutation::Remove {
                name: name.value,
                expected: current.value.take(),
            },
        };
        let changed = apply_attribute_mutations_with_accounting(
            &volume.fs.inner.objects,
            root,
            std::slice::from_ref(&mutation),
            1,
            limits,
            accounting,
            cancellation,
        )
        .await;
        drop(mutation);
        metadata.named_attributes = MetadataField::Value(changed?.value);
        Ok(metadata)
    }
    .await;
    current.retire(accounting)?;
    result
}

fn require_attribute_mode(
    mode: Option<NamedAttributeWriteMode>,
    present: bool,
) -> Result<(), FsError> {
    match (mode, present) {
        (Some(NamedAttributeWriteMode::Create), true)
        | (Some(NamedAttributeWriteMode::Replace), false) => {
            return Err(FsError::CreationRejected);
        }
        (None, false) => return Err(FsError::NotFound),
        _ => {}
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {
    /// Reads ordered named attributes with their owned names and values.
    pub async fn read_named_attributes<W: FsWork>(
        &self,
        root: ObjectId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<(AttributeName, acyclic_stream::OwnedBytes)>> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            let mut attributes =
                Owned::<Vec<(AttributeName, acyclic_stream::OwnedBytes)>>::allocate(
                    0,
                    &mut accounting,
                )?;
            let mut total = 0_u64;
            loop {
                let after = attributes.value.last().map(|(name, _)| name);
                let (listing, mut allocation) = list_attributes_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    root,
                    after,
                    256,
                    DecodeLimits::for_volume(self.volume.config),
                    &mut accounting,
                    cancellation,
                    None,
                )
                .await?;
                let mut listing = listing;
                let retired = listing
                    .entries
                    .capacity()
                    .checked_mul(size_of::<AttributeEntry>())
                    .map(usize_to_u64)
                    .ok_or(WorkError::Overflow)?;
                attributes.allocation.absorb(&mut allocation)?;
                for entry in listing.entries.drain(..) {
                    total = total.saturating_add(entry.value_bytes);
                    if total > self.volume.config.limits.maximum_read_bytes {
                        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                    }
                    attributes.ensure_for_push(usize::MAX, &mut accounting)?;
                    let read = read_blob_range_with_accounting(
                        &self.volume.fs.inner.objects,
                        entry.value,
                        ByteRange {
                            offset: 0,
                            length: entry.value_bytes,
                        },
                        DecodeLimits::for_volume(self.volume.config),
                        &mut accounting,
                        cancellation,
                        true,
                    )
                    .await?;
                    let copied = WorkCounters {
                        items_examined: 1,
                        bytes_copied: size_of::<(AttributeName, acyclic_stream::OwnedBytes)>()
                            as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    attributes.value.push((entry.name, read.bytes));
                    accounting.record_completed(copied)?;
                }
                let has_more = listing.has_more;
                drop(listing);
                accounting.release_owned(&mut attributes.allocation, retired)?;
                if !has_more {
                    break;
                }
            }
            Ok((attributes.value, attributes.allocation))
        }
        .await;
        work.finish_owned(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Reads one exact named attribute by path or stable identity.
    pub async fn read_named_attribute<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        name: &AttributeName,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let metadata = self
                .read_metadata_with_accounting(file, &mut accounting, cancellation)
                .await?;
            read_attribute(
                &self.volume,
                metadata,
                name,
                self.volume.config.limits.maximum_read_bytes,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of named attributes by path or stable identity.
    pub async fn list_named_attributes<F: ContentFile>(
        &mut self,
        file: F,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .list_named_attributes_with_accounting(
                file,
                after,
                maximum_entries,
                &mut accounting,
                cancellation,
            )
            .await;
        measured(result.map(|(value, _)| value), &accounting)
    }

    pub(crate) async fn list_named_attributes_with_accounting<F: ContentFile>(
        &mut self,
        file: F,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, KernelOwnedAllocation), FsError> {
        let metadata = self
            .read_metadata_with_accounting(file, accounting, cancellation)
            .await?;
        list_attributes(
            &self.volume,
            metadata,
            after,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await
    }

    /// Changes one exact named attribute by path or stable identity.
    pub async fn change_named_attribute<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            let (id, metadata, maximum) = match file.address() {
                ContentAddress::File(id) => (
                    id,
                    self.read_metadata_with_accounting(id, &mut accounting, cancellation)
                        .await?,
                    u64::MAX,
                ),
                ContentAddress::Path(path) => {
                    let lookup = self
                        .lookup_no_follow_with_metadata_with_accounting(
                            path,
                            &mut accounting,
                            cancellation,
                        )
                        .await?
                        .ok_or(FsError::NotFound)?;
                    (
                        lookup.record.file_id,
                        lookup.metadata,
                        self.volume.config.limits.maximum_read_bytes,
                    )
                }
            };
            let metadata = change_attribute(
                &self.volume,
                metadata,
                name,
                value,
                maximum,
                &mut accounting,
                cancellation,
            )
            .await?;
            self.set_attributes_with_accounting(id, metadata, None, &mut accounting, cancellation)
                .await
        }
        .await;
        budget.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {
    /// Reads one exact named attribute from this detached file.
    pub async fn read_named_attribute<W: FsWork>(
        &self,
        name: &AttributeName,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume,
                self.record,
                &mut accounting,
                cancellation,
            )
            .await?;
            read_attribute(
                &self.volume,
                metadata,
                name,
                self.volume.config.limits.maximum_read_bytes,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of detached named attributes.
    pub async fn list_named_attributes(
        &self,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .list_named_attributes_with_accounting(
                after,
                maximum_entries,
                &mut accounting,
                cancellation,
            )
            .await;
        measured(result.map(|(value, _)| value), &accounting)
    }

    pub(crate) async fn list_named_attributes_with_accounting(
        &self,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, KernelOwnedAllocation), FsError> {
        let metadata = read_record_metadata_to_file_root_with_accounting(
            &self.volume,
            self.record,
            accounting,
            cancellation,
        )
        .await?;
        list_attributes(
            &self.volume,
            metadata,
            after,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await
    }

    /// Changes one exact named attribute atomically in this detached file.
    pub async fn change_named_attribute<W: FsWork>(
        &mut self,
        name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume,
                self.record,
                &mut accounting,
                cancellation,
            )
            .await?;
            let metadata = change_attribute(
                &self.volume,
                metadata,
                name,
                value,
                u64::MAX,
                &mut accounting,
                cancellation,
            )
            .await?;
            self.set_attributes_with_accounting(metadata, None, &mut accounting, cancellation)
                .await?;
            Ok(metadata)
        }
        .await;
        budget.finish(result, &accounting)
    }
}

async fn attribute_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    attributes: MetadataField<ObjectId>,
    mode: Option<NamedAttributeWriteMode>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let limits = DecodeLimits::for_volume(volume.config);
    let root = match attributes {
        MetadataField::Value(root) => root,
        MetadataField::Unavailable if mode.is_none() => return Err(FsError::NotFound),
        MetadataField::Unavailable if matches!(mode, Some(NamedAttributeWriteMode::Replace)) => {
            return Err(FsError::CreationRejected);
        }
        MetadataField::Unavailable => {
            let encoded = crate::kernel::codec::encode_owned(accounting, |observe, reserve| {
                encode_attribute_page(
                    &AttributePage::Leaf(Vec::new()),
                    limits.maximum_page_items,
                    observe,
                    reserve,
                )
            })?;
            volume
                .fs
                .put_encoded_with_accounting(
                    ObjectKind::AttributePage,
                    encoded,
                    accounting,
                    cancellation,
                )
                .await?
        }
    };

    Ok(root)
}
