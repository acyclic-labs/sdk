use super::*;

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Materializes selected paths with shared parent and hard-link state.
    pub async fn materialize<'a>(
        &self,
        paths: impl ExactSizeIterator<Item = &'a str> + ProviderTask,
        options: &crate::MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let mut checkout = self
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let mut parsed = crate::kernel::Owned::allocate(paths.len(), &mut accounting)?;
            for path in paths {
                let (path, allocation) = customer_path_with_accounting(
                    path,
                    checkout.value.volume_config(),
                    &mut accounting,
                    cancellation,
                )?;
                let bytes = path.retained_bytes();
                Transaction::<A, O>::push_mutation(
                    &mut parsed,
                    crate::kernel::Owned::new(path, allocation, bytes),
                    &mut accounting,
                )?;
            }
            let materialized = crate::materialize_checkout_paths(
                &mut checkout.value,
                &parsed.value,
                options,
                accounting.remaining()?,
                cancellation,
            )
            .await;
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            let value =
                merge_workspace_backend(materialized, spent, allocations.live_bytes())?.value;
            spent.verify(budget)?;
            Ok(value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }

    /// Replaces or removes one host path from this exact generation.
    pub async fn restore_host_path(
        &self,
        relative: &std::path::Path,
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError>
    {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let mut checkout = self
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let restored = crate::restore_checkout_host_path(
                &mut checkout.value,
                relative,
                replacement,
                options,
                accounting.remaining()?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_error(WorkspaceError::engine));
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            let value = merge_workspace_backend(restored, spent, allocations.live_bytes())?.value;
            spent.verify(budget)?;
            Ok(value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }
}
