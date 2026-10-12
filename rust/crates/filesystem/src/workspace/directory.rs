use super::*;

async fn list<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, after: Option<&LogicalName>, maximum: u32, budget: WorkBudget, cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::DirectoryPage>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        let (page, allocation) = checkout.value.list_directory_with_accounting(&path.value, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum, &mut accounting, cancellation).await?;
        drop(allocation);
        Ok::<_, WorkspaceError>(page)
    }.await;
    crate::facade::measured(result, &accounting)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns one authenticated bounded directory page.
    pub async fn list_directory(&self, path: &str, after: Option<&LogicalName>, maximum_entries: u32, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::DirectoryPage>, WorkspaceError> {
        list(self, GenerationSelector::Head, path, after, maximum_entries, budget, cancellation).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Returns one directory page from this exact generation.
    pub fn list_directory<'a>(&'a self, path: &'a str, after: Option<&'a LogicalName>, maximum_entries: u32, budget: WorkBudget, cancellation: &'a CancellationToken) -> impl Future<Output = crate::MeasuredResult<crate::OperationReceipt<crate::kernel::DirectoryPage>, WorkspaceError>> + ProviderTask + 'a {
        list(&self.workspace, GenerationSelector::Exact(self.id), path, after, maximum_entries, budget, cancellation)
    }

    /// Returns an exact-generation page retaining its original native allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_after_with_native_work(&self, path: &str, after: Option<&LogicalName>, maximum: usize, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<(crate::kernel::DirectoryPage, crate::NativeAllocation), WorkspaceError> {
        let maximum = u32::try_from(maximum).map_err(|_| crate::WorkError::Overflow)?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        let (mut checkout, path) = self.workspace.open_read_path(GenerationSelector::Exact(self.id), path, &mut accounting, cancellation).await?;
        let (page, allocation) = checkout.value.list_directory_with_accounting(&path.value, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum, &mut accounting, cancellation).await?;
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else { unreachable!() };
        Ok((page, allocation))
    }
}
