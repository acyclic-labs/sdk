use super::*;

/// One child returned from an authenticated directory page, resolved by the
pub struct ResolvedDirectoryEntry<A, O> {
    /// Exact canonical child name.
    pub name: LogicalName,
    /// Generation-scoped child handle with authenticated metadata.
    pub file: ResolvedFile<A, O>,
}

/// One bounded authenticated directory page with reader-scoped child handles.
pub struct ResolvedDirectoryPage<A, O> {
    /// Ordered children strictly after the supplied cursor.
    pub entries: Vec<ResolvedDirectoryEntry<A, O>>,
    /// Whether at least one additional child exists.
    pub has_more: bool,
}

/// One directory entry paired with its path-independent authenticated record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryRecordEntry {
    /// Exact canonical name.
    pub name: LogicalName,
    /// Complete child file record from the shared file-table frontier.
    pub record: FileRecord,
    /// Complete authenticated metadata referenced by `record`.
    pub metadata: FileMetadata,
}

/// One page-efficient directory listing for native projection callbacks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryRecordPage {
    /// Ordered entries strictly after the supplied cursor.
    pub entries: Vec<DirectoryRecordEntry>,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// One bounded directory binding page requested from a [`PinnedReader`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPageRequest {
    /// Exact directory namespace path.
    pub path: NamespacePath,
    /// Exclusive canonical name cursor.
    pub after: Option<LogicalName>,
    /// Maximum entries returned for this page.
    pub maximum_entries: u32,
}

/// One present no-follow path result with authenticated metadata decoded in
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathMetadataLookup {
    /// Path-independent authenticated file record.
    pub record: FileRecord,
    /// Complete metadata referenced by `record`.
    pub metadata: FileMetadata,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {


    /// Returns an exact cursor page and its original-owned names/vector claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_directory_records_with_native_work(
        &self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, NativeAllocation), FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (page, allocation) = list_directory_records_to_file_root_with_accounting(
            &self.volume, &self.root, path, after, maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }


    /// Resolves one bounded directory page into generation-scoped child
    pub async fn resolve_directory_page(
        &self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ResolvedDirectoryPage<A, O>> {
        let listing = list_directory_page(
            &self.volume,
            &self.root,
            path,
            after,
            maximum_entries,
            budget,
            cancellation,
        )
        .await?;
        let mut work = listing.work;
        let page = listing.value;
        let mut file_ids = Vec::new();
        file_ids
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        file_ids.extend(page.entries.iter().map(|entry| entry.file_id));
        let records = if file_ids.is_empty() {
            Vec::new()
        } else {
            let receipt = lookup_file_records(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                &file_ids,
                maximum_entries,
                DecodeLimits::for_volume(self.volume.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut metadata_requests = Vec::new();
        metadata_requests
            .try_reserve_exact(records.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for record in &records {
            let record = record
                .as_ref()
                .ok_or_else(|| OperationFailure::new(FsError::InvalidDirectoryRecord, work))?;
            metadata_requests.push(ObjectReadRequest {
                object_id: record.metadata,
                maximum_bytes: self.volume.config.limits.maximum_object_bytes,
            });
        }
        let metadata = if metadata_requests.is_empty() {
            Vec::new()
        } else {
            let receipt = self
                .volume
                .fs
                .inner
                .objects
                .read_many(&metadata_requests, remaining(work, budget)?, cancellation)
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for ((binding, record), metadata) in page.entries.into_iter().zip(records).zip(metadata) {
            let record = record
                .ok_or_else(|| OperationFailure::new(FsError::InvalidDirectoryRecord, work))?;
            if record.file_id != binding.file_id || record.kind != binding.kind {
                return Err(OperationFailure::new(FsError::InvalidDirectoryRecord, work));
            }
            let metadata =
                crate::kernel::decode_file_metadata(&metadata, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(()))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
            let logical_bytes = record_logical_bytes(record);
            entries.push(ResolvedDirectoryEntry {
                name: binding.name,
                file: ResolvedFile {
                    reader: self.clone(),
                    record,
                    description: FileDescription {
                        kind: record.kind,
                        logical_bytes,
                        metadata,
                    },
                },
            });
        }
        Ok(FsReceipt {
            value: ResolvedDirectoryPage {
                entries,
                has_more: page.has_more,
            },
            work,
        })
    }
}

pub(crate) async fn list_directory_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    root: &GenerationRoot,
    path: &NamespacePath,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (page, allocation) = list_directory_to_file_root_with_accounting(
            volume, root, path, after, maximum_entries, accounting, cancellation,
        ).await?;
        attach_directory_records_to_file_root_with_accounting(
            volume, root.file_table, page, allocation, maximum_entries, accounting, cancellation, None,
        ).await
    }.await;
    let retained = match &result {
        Ok((page, _)) if before.is_some() => directory_record_page_retained_bytes(page)?,
        _ => 0,
    };
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

pub(super) fn directory_record_page_retained_bytes(page: &DirectoryRecordPage) -> Result<u64, FsError> {
    let backing = page.entries.capacity().checked_mul(size_of::<DirectoryRecordEntry>())
        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
    page.entries.iter().try_fold(backing, |bytes, entry| {
        bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(FsError::Work(WorkError::Overflow))
    })
}

pub(super) async fn attach_directory_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, file_table: ObjectId, page: DirectoryPage,
    allocation: crate::kernel::allocation::KernelOwnedAllocation,
    maximum_entries: u32, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let mut source = (page, allocation);
    let count = source.0.entries.len();
    let mut ids = crate::kernel::Owned::<Vec<FileId>>::allocate(count, accounting)?;
    let mut output = crate::kernel::Owned::<Vec<DirectoryRecordEntry>>::allocate(count, accounting)?;
    let copied = WorkCounters {
        bytes_copied: count.checked_mul(size_of::<FileId>()).map(crate::foundation::usize_to_u64)
            .ok_or(WorkError::Overflow)?, ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    ids.value.extend(source.0.entries.iter().map(|entry| entry.file_id));
    accounting.record_completed(copied)?;
    source.1.absorb(&mut ids.allocation)?;
    source.1.absorb(&mut output.allocation)?;
    let mut buffers = ((source.0, ids.value, output.value), source.1);
    if count != 0 {
        let (value, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
            &volume.fs.inner.objects, file_table, &buffers.0.1, maximum_entries,
            DecodeLimits::for_volume(volume.config), accounting, cancellation, custody,
        ).await?;
        let records = (value, allocation);
        let mut requests = crate::kernel::Owned::<Vec<ObjectReadRequest>>::allocate(count, accounting)?;
        for record in &records.0 {
            let record = record.ok_or(FsError::InvalidDirectoryRecord)?;
            let copied = WorkCounters {
                bytes_copied: size_of::<ObjectReadRequest>() as u64, ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            requests.value.push(ObjectReadRequest {
                object_id: record.metadata, maximum_bytes: volume.config.limits.maximum_object_bytes,
            });
            accounting.record_completed(copied)?;
        }
        let reads = crate::kernel::read_objects_with_accounting(
            &volume.fs.inner.objects, &requests.value, accounting, cancellation,
        ).await?;
        requests.retire(accounting)?;
        if reads.value.len() != count || records.0.len() != count {
            return Err(FsError::InvalidDirectoryRecord);
        }
        let (page, _, entries) = &mut buffers.0;
        for ((binding, record), metadata) in page.entries.drain(..).zip(&records.0).zip(&reads.value) {
            let record = record.ok_or(FsError::InvalidDirectoryRecord)?;
            if record.file_id != binding.file_id || record.kind != binding.kind {
                return Err(FsError::InvalidDirectoryRecord);
            }
            let metadata = crate::kernel::decode_owned(accounting, |observe| crate::kernel::decode_file_metadata(&metadata.bytes, DecodeLimits::for_volume(volume.config), &mut |step| observe(step))).map(|(metadata, _)| metadata)?;
            let copied = WorkCounters {
                items_examined: 1, bytes_copied: size_of::<DirectoryRecordEntry>() as u64,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            entries.push(DirectoryRecordEntry { name: binding.name, record, metadata });
            accounting.record_completed(copied)?;
        }
    }
    let retired = buffers.0.0.entries.capacity().checked_mul(size_of::<crate::kernel::TreeEntry>())
        .and_then(|bytes| buffers.0.1.capacity().checked_mul(size_of::<FileId>())
            .and_then(|ids| bytes.checked_add(ids)))
        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
    let (page, file_ids, entries) = buffers.0;
    let has_more = page.has_more;
    drop(page.entries);
    drop(file_ids);
    let mut result = (DirectoryRecordPage { entries, has_more }, buffers.1);
    accounting.release_owned(&mut result.1, retired)?;
    Ok((result.0, result.1))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    /// Returns one directory page with all child records fetched in one shared
    pub async fn list_directory_records(
        &mut self,
        path: &NamespacePath,
        after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryRecordPage> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_directory_records_with_accounting(
            path, after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(value, allocation)| {
            drop(allocation);
            FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }


    pub(crate) async fn list_directory_records_with_accounting(
        &mut self, path: &NamespacePath, after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let (page, allocation) = self.list_directory_with_accounting(
                path, after, maximum_entries, accounting, cancellation,
            ).await?;
            let (value, allocation) = attach_directory_records_to_file_root_with_accounting(
                &self.volume, self.root.file_table, page, allocation, maximum_entries, accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            let page = (value, allocation);
            if self.tracks_observations() && !page.0.entries.is_empty() {
                let mut ids = crate::kernel::Owned::<Vec<FileId>>::allocate(page.0.entries.len(), accounting)?;
                let copied = WorkCounters {
                    bytes_copied: page.0.entries.len().checked_mul(size_of::<FileId>())
                        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                ids.value.extend(page.0.entries.iter().map(|entry| entry.record.file_id));
                accounting.record_completed(copied)?;
                self.observe_base_metadata_ids_with_accounting(&ids.value, accounting, cancellation).await?;
                ids.retire(accounting)?;
            }
            Ok((page.0, page.1))
        }.await;
        let retained = match &result {
            Ok((page, _)) if before.is_some() => directory_record_page_retained_bytes(page)?,
            _ => 0,
        };
        readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }


    /// Lists one directory page and child records on the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_directory_records_with_native_work(
        &mut self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, crate::NativeAllocation), FsError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
        let (page, allocation) = self.list_directory_records_with_accounting(
            path, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }
}

