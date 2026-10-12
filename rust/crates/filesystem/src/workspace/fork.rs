use super::*;

/// Exact source selection for a cheap workspace fork.
#[derive(Clone)]
pub struct ForkOptions<A, O> {
    pub(crate) generation: Generation<A, O>,
    pub(crate) idempotency_key: IdempotencyKey,
    pub(crate) paths: Option<Vec<String>>,
}

impl<A, O> ForkOptions<A, O> {
    /// Forks from one exact immutable generation.
    #[must_use]
    pub fn from_generation(generation: Generation<A, O>, idempotency_key: IdempotencyKey) -> Self {
        Self {
            generation,
            idempotency_key,
            paths: None,
        }
    }

    /// Carries only explicitly selected files or directory subtrees. Selection
    #[must_use]
    pub fn inherit_paths(mut self, paths: Vec<String>) -> Self {
        self.paths = Some(paths);
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Forks an exact generation under supplied limits and a publication seal.
    pub async fn fork<F, Fut>(&self, destination: impl AsRef<str>, options: ForkOptions<A, O>, permit: crate::PublicationPermit, budget: WorkBudget, cancellation: &CancellationToken, mut before_publication: F) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(|budget| async move {
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let bytes = options.paths.as_ref().map_or(Ok(0), |paths| workspace_owned_strings_bytes(paths)).map_err(|error| crate::OperationFailure::new(error, work))?;
            allocations.claim_bytes(bytes, 0, &mut work, WorkBudget::UNBOUNDED).map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
            if options.generation.workspace.id != self.id { return Err(crate::OperationFailure::new(WorkspaceError::ForeignGeneration, work)); }
            cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            let remaining = workspace_remaining(work, budget, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, work))?;
            let name = merge_workspace_backend(WorkspaceName::new(destination, remaining, cancellation), &mut work, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, work))?;
            work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            allocations.release(bytes).map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
            let remaining = work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            let mut guard = &mut before_publication;
            let forked = merge_workspace_backend(self.volume.fs.fork_workspace::<_, _, WorkspaceError>(name.value, &options.generation, options.idempotency_key, options.paths, permit, remaining, cancellation, Some(&mut guard)).await, &mut work, 0).map_err(|error| crate::OperationFailure::new(error, work))?;
            work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            Ok(crate::OperationReceipt { value: forked.value, work })
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }

    /// Forks on the original native invocation with its publication seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn fork_with_native_work<'original, E, F, Fut>(&self, destination: &str, options: ForkOptions<A, O>, permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork, cancellation: &CancellationToken, before_publication: &mut F) -> Result<Self, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let bytes = options.paths.as_ref().map_or(Ok(0), |paths| workspace_owned_strings_bytes(paths)).map_err(E::from)?;
        let (allocation, admitted) = accounting.record_actual_owned(bytes)?;
        let mut options = crate::kernel::Owned::new(options, allocation, bytes);
        admitted?;
        if options.value.generation.workspace.id != self.id { return Err(WorkspaceError::ForeignGeneration.into()); }
        accounting.check()?;
        let name = WorkspaceName::new(destination, original, cancellation).map_err(E::from)?;
        accounting.release_owned(&mut options.allocation, bytes)?;
        let ForkOptions { generation, idempotency_key, paths } = options.value;
        let mut guard = crate::NativeAuthorityGuard(before_publication.clone());
        self.volume.fs.fork_workspace(name, &generation, idempotency_key, paths, permit, original, cancellation, Some(&mut guard)).await
    }
}
