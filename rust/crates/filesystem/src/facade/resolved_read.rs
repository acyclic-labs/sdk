use super::*;

/// Public facts needed to plan reads without exposing generation-bound records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDescription {
    /// Terminal entry kind.
    pub kind: FileKind,
    /// Logical content length for regular files and symbolic links.
    pub logical_bytes: u64,
    /// Canonical metadata authenticated with the same pinned generation.
    pub metadata: FileMetadata,
}

/// One file resolved and authenticated by a specific pinned reader.
pub struct ResolvedFile<A, O> {
    pub(super) reader: PinnedReader<A, O>,
    pub(super) record: FileRecord,
    pub(super) description: FileDescription,
}

impl<A, O> ResolvedFile<A, O> {

    /// Stable file identity authenticated with this handle.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.record.file_id
    }

    /// Public semantic facts authenticated with this handle.
    #[must_use]
    pub const fn description(&self) -> &FileDescription {
        &self.description
    }
}

/// One range requested from a reader-scoped resolved file.
pub struct ResolvedFileRangeReadRequest<'a, A, O> {
    /// Exact resolved file handle.
    pub file: &'a ResolvedFile<A, O>,
    /// Exact logical byte range returned.
    pub range: ByteRange,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {

    /// Reads resolved handles without another namespace traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_resolved_ranges_with_native_work(
        &self, requests: &[ResolvedFileRangeReadRequest<'_, A, O>], concurrency: usize,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<FileRangeRead>, NativeAllocation), FsError> {
        if concurrency == 0 || requests.len()
            > usize::try_from(self.volume.config.limits.maximum_paths_per_batch).unwrap_or(usize::MAX)
        { return Err(FsError::FileRead(FileRangeReadError::InvalidRange)); }
        self.validate_native_work(work)?;
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let mut reads = Owned::<Vec<FileRangeRead>>::allocate(requests.len(), &mut accounting)?;
        for request in requests {
            let read = request.file.reader.read_record_range(
                request.file.record, request.range, work, cancellation,
            ).await?;
            let copied = WorkCounters {
                items_examined: 1, bytes_copied: usize_to_u64(size_of::<FileRangeRead>()),..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            reads.value.push(read);
            accounting.record_completed(copied)?;
        }
        Ok((reads.value, native_pinned_allocation(reads.allocation)?))
    }

    /// Resolves ordered paths into native-reader handles for later range reads.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resolve_file_ranges_with_native_work(
        &self, paths: &[NamespacePath], work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<Option<ResolvedFile<A, O>>>, NativeAllocation), FsError> {
        cancellation.check()?;
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let lookup = lookup_paths_async_with_accounting(
            &self.volume.fs.inner.objects, &self.root, paths, self.volume.config, &mut accounting, cancellation, None,
        ).await?;
        let mut handles = Owned::<Vec<Option<ResolvedFile<A, O>>>>::allocate(
            lookup.value().entries.len(), &mut accounting,
        )?;
        for entry in &lookup.value().entries {
            let file = if let Some(record) = entry.record {
                let metadata = self.read_record_metadata(record, work, cancellation).await?;
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: usize_to_u64(size_of::<ResolvedFile<A, O>>()),..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                let file = ResolvedFile {
                    reader: self.clone(), record, description: FileDescription {
                        kind: record.kind, logical_bytes: record_logical_bytes(record), metadata,
                    },
                };
                accounting.record_completed(copied)?;
                Some(file)
            } else { None };
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(size_of::<Option<ResolvedFile<A, O>>>()),..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            handles.value.push(file);
            accounting.record_completed(copied)?;
        }
        Ok((handles.value, native_pinned_allocation(handles.allocation)?))
    }

    pub(super) async fn resolve_file_records(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileRecord>>> {
        let lookup = lookup_paths(
            &self.volume.fs.inner.objects,
            &self.root,
            paths,
            self.volume.config,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::Path))?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(lookup.entries.len())
            .map_err(|_| {
                OperationFailure::new(FsError::PendingMutationAllocationFailed, lookup.work)
            })?;
        for entry in lookup.entries {
            records.push(entry.record);
        }
        Ok(FsReceipt {
            value: records,
            work: lookup.work,
        })
    }

    /// Describes an ordered path batch while sharing namespace and object traversal.
    pub async fn describe_files(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileDescription>>> {
        let resolved = self.resolve_files(paths, budget, cancellation).await?;
        let descriptions = resolved
            .value
            .into_iter()
            .map(|file| file.map(|file| file.description))
            .collect();
        Ok(FsReceipt {
            value: descriptions,
            work: resolved.work,
        })
    }

    /// Resolves and describes paths once, returning reader-scoped handles for
    pub async fn resolve_files(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<ResolvedFile<A, O>>>> {
        let resolved = self
            .resolve_file_records(paths, budget, cancellation)
            .await?;
        let mut work = resolved.work;
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(resolved.value.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        requests.extend(
            resolved
                .value
                .iter()
                .flatten()
                .map(|record| ObjectReadRequest {
                    object_id: record.metadata,
                    maximum_bytes: self.volume.config.limits.maximum_object_bytes,
                }),
        );
        let reads = if requests.is_empty() {
            Vec::new()
        } else {
            let receipt = ({ let __object_args = (&requests, remaining(work, budget)?, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self
                .volume
                .fs
                .inner
                .objects.read_many(__object_args.0, &mut __object_work).await.map(|(values, _allocation)| values); __object_work.receipt(__object_result) })
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut reads = reads.into_iter();
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(resolved.value.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for record in resolved.value {
            let Some(record) = record else {
                handles.push(None);
                continue;
            };
            let bytes = reads
                .next()
                .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
            let metadata =
                crate::kernel::decode_file_metadata(&bytes, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(()))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
            let logical_bytes = record_logical_bytes(record);
            handles.push(Some(ResolvedFile {
                reader: self.clone(),
                record,
                description: FileDescription {
                    kind: record.kind,
                    logical_bytes,
                    metadata,
                },
            }));
        }
        Ok(FsReceipt {
            value: handles,
            work,
        })
    }

    /// Reads ranges from already resolved files with bounded concurrency and
    pub async fn read_resolved_ranges(
        &self,
        requests: &[ResolvedFileRangeReadRequest<'_, A, O>],
        concurrency: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<FileRangeRead>> {
        if concurrency == 0
            || requests.len()
                > usize::try_from(self.volume.config.limits.maximum_paths_per_batch)
                    .unwrap_or(usize::MAX)
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        if budget != WorkBudget::UNBOUNDED {
            let mut work = WorkCounters::default();
            let mut reads = Vec::new();
            reads
                .try_reserve_exact(requests.len())
                .map_err(|_| OperationFailure::before_work(FsError::Work(WorkError::Overflow)))?;
            for request in requests {
                let read = request
                    .file
                    .read_range(request.range, remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, read.work)?;
                reads.push(read.value);
            }
            return Ok(FsReceipt { value: reads, work });
        }
        let cancellation = cancellation.clone();
        let pending = requests
            .iter()
            .enumerate()
            .map(|(index, request)| {
                (
                    index,
                    request.file.reader.clone(),
                    request.file.record,
                    request.range,
                )
            })
            .collect::<Vec<_>>();
        let results = stream::iter(pending.into_iter().map(|(index, reader, record, range)| {
            let cancellation = cancellation.clone();
            async move {
                (
                    index,
                    reader
                        .read_record_range(record, range, budget, &cancellation)
                        .await,
                )
            }
        }))
        .buffer_unordered(concurrency.min(requests.len().max(1)))
        .collect::<Vec<_>>()
        .await;
        order_batch_results(results, budget)
    }
}

pub(super) fn order_batch_results<T>(
    mut results: Vec<(usize, FsResult<T>)>,
    budget: WorkBudget,
) -> FsResult<Vec<T>> {
    results.sort_unstable_by_key(|(index, _)| *index);
    let mut work = WorkCounters::default();
    let mut completed = Vec::new();
    completed
        .try_reserve_exact(results.len())
        .map_err(|_| OperationFailure::before_work(FsError::Work(WorkError::Overflow)))?;
    let mut first_failure = None;
    for (_, result) in results {
        match result {
            Ok(receipt) => {
                work = add(work, receipt.work)?;
                completed.push(receipt.value);
            }
            Err(failure) => {
                work = add(work, *failure.work)?;
                if first_failure.is_none() {
                    first_failure = Some(failure.error);
                }
            }
        }
    }
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    if let Some(error) = first_failure {
        return Err(OperationFailure::new(error, work));
    }
    Ok(FsReceipt {
        value: completed,
        work,
    })
}

