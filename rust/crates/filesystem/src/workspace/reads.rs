use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Reads one complete regular file within the caller's bound.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(
            self,
            GenerationSelector::Head,
            path,
            maximum_bytes,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one exact file range.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            work,
            cancellation,
        )
        .await
    }

    /// Reads complete no-follow metadata for one path.
    pub async fn stat<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(self, GenerationSelector::Head, path, work, cancellation).await
    }

    pub(crate) async fn stat_optional(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = lookup_stat(
            self,
            GenerationSelector::Head,
            path,
            &mut accounting,
            cancellation,
        )
        .await;
        crate::facade::measured(result, &accounting)
    }

    pub(crate) async fn record_by_id(
        &self,
        file_id: FileId,
    ) -> Result<crate::kernel::FileRecord, WorkspaceError> {
        let mut checkout = self
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        checkout
            .read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(WorkspaceError::engine)
    }

    /// Returns one authenticated bounded directory page and its result allocation.
    pub async fn list_directory<W: crate::facade::FsWork>(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
        list(
            self,
            GenerationSelector::Head,
            path,
            after,
            maximum_entries,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one opaque symbolic-link target.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(self, GenerationSelector::Head, path, work, cancellation).await
    }

    /// Plans clipped sparse spans without reading file bodies.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }
}
