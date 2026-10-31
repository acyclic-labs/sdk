use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Transaction<A, O> {
    /// Reads a staged file in this transaction's original selected generation.
    /// The observation joins its publication dependencies; no head is reopened.
    pub async fn read<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            read_core::read_checkout(
                &mut self.checkout,
                &path.value,
                maximum_bytes,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        read_core::finish(
            &self.workspace,
            &work,
            result,
            &mut accounting,
            cancellation,
        )
        .await
    }

    /// Returns a bounded page of this unpublished candidate's directory index.
    /// Staged edits are visible, while concurrent publishers remain fenced by
    /// the original transaction. This does not publish or reopen a generation.
    pub async fn list_directory<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let (page, allocation) = self
                .checkout
                .list_directory_with_accounting(
                    &path.value,
                    after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok((page, allocation))
        }
        .await;
        read_core::finish(
            &self.workspace,
            &work,
            result,
            &mut accounting,
            cancellation,
        )
        .await
    }
}
