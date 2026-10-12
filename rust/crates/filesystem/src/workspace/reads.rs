use super::*;

async fn read_selected<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, maximum_bytes: u64, work: W, cancellation: &CancellationToken,
) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let mut checkout = workspace.open_checkout(selector, CheckoutMode::read_only_pinned(), &mut accounting, cancellation).await?;
        let (path, allocation) = customer_path_with_accounting(path, checkout.value.volume_config(), &mut accounting, cancellation)?;
        let bytes = path.retained_bytes();
        let path = crate::kernel::Owned::new(path, allocation, bytes);
        let (lookup, allocation) = checkout.value.lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true).await?;
        drop(allocation);
        let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
        if record.kind != FileKind::Regular { return Err(WorkspaceError::NotRegularFile); }
        let length = regular_file_bytes(record)?;
        if length > maximum_bytes { return Err(WorkspaceError::ReadLimitExceeded); }
        checkout.value.read_file_range_with_accounting(&path.value, crate::ByteRange { offset: 0, length }, &mut accounting, cancellation).await
            .map(|read| read.bytes).map_err(WorkspaceError::from)
    }.await;
    work.finish(result, &accounting)
}

async fn read_range<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, offset: u64, length: u64, work: W, cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        let read = checkout.value.read_file_range_with_accounting(&path.value, crate::ByteRange { offset, length }, &mut accounting, cancellation).await?;
        let bytes = read.logical_owned_bytes();
        Ok::<_, WorkspaceError>(crate::kernel::OwnedSourceChunk::claimed(read.bytes, bytes))
    }.await;
    work.finish(result, &accounting)
}

async fn read_link<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, work: W, cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        checkout.value.read_symbolic_link_with_accounting(&path.value, &mut accounting, cancellation).await.map_err(WorkspaceError::from)
    }.await;
    work.finish(result, &accounting)
}

async fn lookup_stat<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<WorkspaceStat>, WorkspaceError> {
    let (mut checkout, path) = workspace.open_read_path(selector, path, accounting, cancellation).await?;
    checkout.value.lookup_no_follow_with_metadata_with_accounting(&path.value, accounting, cancellation).await?
        .map(|lookup| {
            let logical_bytes = match lookup.record.payload {
                FilePayload::InlineRegular(bytes) => Some(u64::try_from(bytes.as_bytes().len()).map_err(|_| crate::WorkError::Overflow)?),
                FilePayload::Regular { logical_bytes, .. } => Some(logical_bytes),
                FilePayload::SymbolicLink { target_bytes, .. } => Some(target_bytes),
                FilePayload::ReparsePoint { payload_bytes, .. } => Some(payload_bytes),
                FilePayload::Directory { .. } | FilePayload::Device { .. } | FilePayload::Empty => None,
            };
            Ok(WorkspaceStat { file_id: lookup.record.file_id, kind: lookup.record.kind, link_count: lookup.record.link_count, logical_bytes, metadata: WorkspaceMetadata::from_engine(lookup.metadata) })
        }).transpose()
}

async fn stat<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, work: W, cancellation: &CancellationToken,
) -> W::Output<WorkspaceStat, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = lookup_stat(workspace, selector, path, &mut accounting, cancellation).await.and_then(|value| value.ok_or(WorkspaceError::NotFound));
    work.finish(result, &accounting)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Reads one complete regular file within the caller's bound.
    pub async fn read<W: crate::facade::FsWork>(&self, path: &str, maximum_bytes: u64, work: W, cancellation: &CancellationToken) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(self, GenerationSelector::Head, path, maximum_bytes, work, cancellation).await
    }

    /// Reads one exact file range.
    pub async fn read_range<W: crate::facade::FsWork>(&self, path: &str, offset: u64, length: u64, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(self, GenerationSelector::Head, path, offset, length, work, cancellation).await
    }

    /// Reads complete no-follow metadata for one path.
    pub async fn stat<W: crate::facade::FsWork>(&self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(self, GenerationSelector::Head, path, work, cancellation).await
    }

    pub(crate) async fn stat_optional_measured(&self, path: &str, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = lookup_stat(self, GenerationSelector::Head, path, &mut accounting, cancellation).await;
        crate::facade::measured(result, &accounting)
    }

    pub(crate) async fn record_by_id(&self, file_id: FileId) -> Result<crate::kernel::FileRecord, WorkspaceError> {
        let mut checkout = self.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        checkout.read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(WorkspaceError::engine)
    }

    /// Reads one opaque symbolic-link target.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(&self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(self, GenerationSelector::Head, path, work, cancellation).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Reads one complete regular file from this exact generation.
    pub async fn read<W: crate::facade::FsWork>(&self, path: &str, maximum_bytes: u64, work: W, cancellation: &CancellationToken) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(&self.workspace, GenerationSelector::Exact(self.id), path, maximum_bytes, work, cancellation).await
    }

    /// Reads one exact range from this immutable generation.
    pub async fn read_range<W: crate::facade::FsWork>(&self, path: &str, offset: u64, length: u64, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(&self.workspace, GenerationSelector::Exact(self.id), path, offset, length, work, cancellation).await
    }

    /// Reads complete no-follow metadata from this immutable generation.
    pub async fn stat<W: crate::facade::FsWork>(&self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(&self.workspace, GenerationSelector::Exact(self.id), path, work, cancellation).await
    }

    /// Reads one opaque symbolic-link target from this exact generation.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(&self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(&self.workspace, GenerationSelector::Exact(self.id), path, work, cancellation).await
    }

    /// Resolves an ordered no-follow path batch, preserving absence and duplicates.
    pub async fn lookup_paths(&self, paths: &[NamespacePath], budget: WorkBudget, cancellation: &CancellationToken) -> crate::FsResult<Vec<Option<crate::kernel::FileRecord>>> {
        in_heap(move || async move {
            let checkout = self.workspace.volume.checkout(GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), budget, cancellation).await?;
            let mut work = checkout.work;
            let mut checkout = checkout.value;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget).map_err(crate::collection::handle_allocation_error)?;
                let mut accounting = KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, cancellation);
                let (lookup, allocation) = checkout.lookup_batch_no_follow_with_accounting(paths, false, &mut accounting, cancellation).await?;
                let bytes = lookup.retained_allocation_bytes;
                let lookup = crate::kernel::Owned::new(lookup, allocation, bytes);
                let mut records = crate::kernel::Owned::allocate(lookup.value.entries.len(), &mut accounting).map_err(|error| match error {
                    FsError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed) => FsError::HandleAllocationFailed,
                    error => error,
                })?;
                for entry in lookup.value.entries {
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    records.value.push(entry.record);
                }
                Ok(records.value)
            }.await;
            result.map(|value| crate::OperationReceipt { value, work }).map_err(|error| crate::OperationFailure::new(error, work))
        }).await
    }
}
