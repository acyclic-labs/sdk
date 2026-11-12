use super::*;
use crate::kernel::{AttributeName, OwnedSourceChunk};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Generation<A, O> {
    /// Returns an attribute's authenticated blob identity and length without
    /// reading its payload. Matching absence is represented by `None`.
    pub async fn named_attribute_value<W: crate::facade::FsWork>(
        &self,
        path: &str,
        name: &AttributeName,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<(crate::ObjectId, u64)>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut scope = accounting.allocation_scope()?;
            let result = async {
                let (mut checkout, path) = self
                    .workspace
                    .open_read_path(
                        GenerationSelector::Exact(self.id),
                        path,
                        &mut scope,
                        cancellation,
                    )
                    .await?;
                let value = checkout
                    .value
                    .named_attribute_value_with_accounting(
                        &path.value,
                        name,
                        &mut scope,
                        cancellation,
                    )
                    .await?;
                path.retire(&mut scope)?;
                checkout.retire(&mut scope)?;
                Ok(value)
            }
            .await;
            read_core::authorize(&self.workspace, result, &mut scope, cancellation).await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Reads one exact attribute at this immutable generation. The optional
    /// payload bound is enforced before fetching its value. Current workspace
    /// authority is checked before returning the selected bytes.
    pub async fn read_named_attribute<W: crate::facade::FsWork>(
        &self,
        path: &str,
        name: &AttributeName,
        maximum_bytes: Option<u64>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<OwnedSourceChunk>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut scope = accounting.allocation_scope()?;
            let result = async {
                let (mut checkout, path) = self
                    .workspace
                    .open_read_path(
                        GenerationSelector::Exact(self.id),
                        path,
                        &mut scope,
                        cancellation,
                    )
                    .await?;
                let value = checkout
                    .value
                    .read_named_attribute_with_accounting(
                        &path.value,
                        name,
                        maximum_bytes,
                        &mut scope,
                        cancellation,
                    )
                    .await?;
                path.retire(&mut scope)?;
                checkout.retire(&mut scope)?;
                Ok(value)
            }
            .await;
            let result =
                read_core::authorize(&self.workspace, result, &mut scope, cancellation).await;
            if result.is_ok() {
                scope.keep();
            }
            result
        }
        .await;
        work.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Transaction<A, O> {
    /// Changes one exact attribute in the same atomic transaction as its file.
    pub async fn change_named_attribute<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        name: &AttributeName,
        value: Option<(&[u8], crate::NamedAttributeWriteMode)>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let result = self
                .checkout
                .change_named_attribute_with_accounting(
                    &path.value,
                    name,
                    value,
                    &mut accounting,
                    cancellation,
                )
                .await;
            path.retire(&mut accounting)?;
            result.map_err(WorkspaceError::from)
        }
        .await;
        work.finish(result, &accounting)
    }
}
