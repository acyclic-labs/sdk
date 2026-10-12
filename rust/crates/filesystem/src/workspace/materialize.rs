use super::*;

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Materializes selected paths with shared parent and hard-link state.
    pub async fn materialize<'a>(&self, paths: impl ExactSizeIterator<Item = &'a str> + ProviderTask, options: &crate::MaterializeOptions, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let opened = self.workspace.checkout(GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        let mut checkout = opened.value;
        let mut work = opened.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget).map_err(workspace_allocation_error)?;
            let mut accounting = KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, cancellation);
            let mut parsed = crate::kernel::Owned::allocate(paths.len(), &mut accounting)?;
            for path in paths {
                let (path, allocation) = customer_path_with_accounting(path, checkout.volume_config(), &mut accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Transaction::<A, O>::push_mutation(&mut parsed, crate::kernel::Owned::new(path, allocation, bytes), &mut accounting)?;
            }
            drop(accounting);
            let live = allocations.live_bytes();
            let materialized = merge_workspace_backend(crate::materialize_checkout_paths(&mut checkout, &parsed.value, options, workspace_remaining(work, budget, live)?, cancellation).await, &mut work, live)?;
            work.verify(budget)?;
            Ok(materialized.value)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work }).map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Replaces or removes one host path from this exact generation.
    pub async fn restore_host_path(&self, relative: &std::path::Path, replacement: crate::HostPathReplacement, options: &crate::MaterializeOptions, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError> {
        let checkout = self.workspace.checkout(GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        let mut value = checkout.value;
        let remaining = checkout.work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), checkout.work))?;
        let receipt = crate::restore_checkout_host_path(&mut value, relative, replacement, options, remaining, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(checkout.work, WorkspaceError::engine))?;
        merge_workspace_work(checkout.work, receipt, budget)
    }
}
