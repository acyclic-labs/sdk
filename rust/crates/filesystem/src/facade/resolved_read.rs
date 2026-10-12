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
    /// Resolves ordered paths into native-reader handles for later range reads.
    pub async fn resolve_files<W: FsWork>(
        &self,
        paths: &[NamespacePath],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<Option<ResolvedFile<A, O>>>> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            cancellation.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            let lookup = lookup_paths_async_with_accounting(
                &self.volume.fs.inner.objects,
                &self.root,
                paths,
                self.volume.config,
                &mut accounting,
                cancellation,
                None,
            )
            .await?;
            let mut handles = Owned::<Vec<Option<ResolvedFile<A, O>>>>::allocate(
                lookup.value().entries.len(),
                &mut accounting,
            )?;
            for entry in &lookup.value().entries {
                let file = if let Some(record) = entry.record {
                    let metadata = read_record_metadata_to_file_root_with_accounting(
                        &self.volume,
                        record,
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                    let copied = WorkCounters {
                        items_examined: 1,
                        bytes_copied: usize_to_u64(size_of::<ResolvedFile<A, O>>()),
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    let file = ResolvedFile {
                        reader: self.clone(),
                        record,
                        description: FileDescription {
                            kind: record.kind,
                            logical_bytes: record_logical_bytes(record),
                            metadata,
                        },
                    };
                    accounting.record_completed(copied)?;
                    Some(file)
                } else {
                    None
                };
                let copied = WorkCounters {
                    bytes_copied: usize_to_u64(size_of::<Option<ResolvedFile<A, O>>>()),
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                handles.value.push(file);
                accounting.record_completed(copied)?;
            }
            Ok((handles.value, handles.allocation))
        }
        .await;
        work.finish_owned(result, &accounting)
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
        Ok(OperationReceipt {
            value: descriptions,
            work: resolved.work,
        })
    }
    /// Reads ordered ranges with bounded concurrency and one cumulative work context.
    pub async fn read_resolved_ranges<W: FsWork>(
        &self,
        requests: &[ResolvedFileRangeReadRequest<'_, A, O>],
        concurrency: usize,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Vec<FileRangeRead>> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            if concurrency == 0
                || requests.len() > self.volume.config.limits.maximum_paths_per_batch as usize
            {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            if readonly_standalone_live(&accounting).is_some()
                && accounting.remaining()? == WorkBudget::UNBOUNDED
            {
                let results = stream::iter(requests.iter().enumerate().map(
                    |(index, request)| async move {
                        (
                            index,
                            request
                                .file
                                .reader
                                .read_record_range(
                                    request.file.record,
                                    request.range,
                                    WorkBudget::UNBOUNDED,
                                    cancellation,
                                )
                                .await,
                        )
                    },
                ))
                .buffer_unordered(concurrency.min(requests.len().max(1)))
                .collect::<Vec<_>>()
                .await;
                let result = order_batch_results(results, WorkBudget::UNBOUNDED);
                let work = match &result {
                    Ok(receipt) => receipt.work,
                    Err(failure) => *failure.work,
                };
                accounting.record_completed(work)?;
                let receipt = result.map_err(|failure| failure.error)?;
                let allocation = accounting.claim_owned(
                    usize_to_u64(receipt.value.capacity() * size_of::<FileRangeRead>()),
                    u64::from(!requests.is_empty()),
                )?;
                accounting.charge(WorkCounters {
                    items_examined: requests.len() as u64,
                    bytes_copied: usize_to_u64(requests.len() * size_of::<FileRangeRead>()),
                    ..WorkCounters::UNCHARGED
                })?;
                return Ok((receipt.value, allocation));
            }
            let mut reads = Owned::<Vec<FileRangeRead>>::allocate(requests.len(), &mut accounting)?;
            for request in requests {
                let read = request
                    .file
                    .reader
                    .read_range_record(
                        request.file.record,
                        request.range,
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                let copied = WorkCounters {
                    items_examined: 1,
                    bytes_copied: size_of::<FileRangeRead>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                reads.value.push(read);
                accounting.record_completed(copied)?;
            }
            Ok((reads.value, reads.allocation))
        }
        .await;
        budget.finish_owned(result, &accounting)
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
    Ok(OperationReceipt {
        value: completed,
        work,
    })
}
