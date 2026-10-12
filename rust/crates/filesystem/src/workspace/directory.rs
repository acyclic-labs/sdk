use super::*;

async fn list<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>, selector: GenerationSelector, path: &str, after: Option<&LogicalName>, maximum: u32, work: W, cancellation: &CancellationToken,
) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        let (page, allocation) = checkout.value.list_directory_with_accounting(&path.value, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum, &mut accounting, cancellation).await?;
        let allocation = workspace_allocation(allocation, &accounting)?;
        Ok::<_, WorkspaceError>((page, allocation))
    }.await;
    work.finish(result, &accounting)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns one authenticated bounded directory page and its result allocation.
    pub async fn list_directory<W: crate::facade::FsWork>(&self, path: &str, after: Option<&LogicalName>, maximum_entries: u32, work: W, cancellation: &CancellationToken) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
        list(self, GenerationSelector::Head, path, after, maximum_entries, work, cancellation).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Returns one exact-generation directory page and its result allocation.
    pub fn list_directory<'a, W: crate::facade::FsWork + 'a>(&'a self, path: &'a str, after: Option<&'a LogicalName>, maximum_entries: u32, work: W, cancellation: &'a CancellationToken) -> impl Future<Output = W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError>> + ProviderTask + 'a {
        async move { list(&self.workspace, GenerationSelector::Exact(self.id), path, after, maximum_entries, work, cancellation).await }
    }
}
