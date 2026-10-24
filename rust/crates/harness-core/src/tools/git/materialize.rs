use super::*;
use crate::filesystem::CheckoutPublication;
use acyclic_fs::MaterializeOptions;

/// Fences every facade effect and optionally materializes an ordinary checkout.
pub(crate) struct RootMaterializingGitExecutor<'a, F> {
    /// Managed workspace executor.
    pub inner: &'a WorkspaceGitExecutor<'a, F>,
    /// Ordinary checkout path.
    pub root: &'a Path,
    /// Pinned physical checkout identity.
    pub source_root_identity: acyclic_fs::NativeRootIdentity,
}

impl<F, Fut> RootMaterializingGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    /// Publishes a generation transition into the checkout.
    pub async fn materialize(
        &self,
        operation_id: OperationId,
        from: LocalGeneration,
    ) -> Result<(), Error> {
        let target = self
            .inner
            .switched
            .lock()
            .map_err(|_| {
                WorkspaceGitExecutor::<F>::error("Git workspace switch lock is unavailable")
            })?
            .as_ref()
            .map_or_else(
                || self.inner.current.clone(),
                |lazy| lazy.workspace().clone(),
            );
        let to_generation = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        publish_checkout_transition(
            self.root,
            self.source_root_identity,
            self.inner.store,
            operation_id,
            from,
            to_generation,
            &|| self.validate(),
        )
        .await
    }
}

// Preparation can await storage and merge drivers. Revalidate after it,
// before handing any prepared result to a publication using the lease permit.
pub(super) async fn prepared<T, P, F, Fut>(preparation: P, before_effect: &F) -> Result<T, Error>
where
    P: Future<Output = Result<T, Error>>,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let value = preparation.await?;
    before_effect().await?;
    Ok(value)
}

#[doc(hidden)]
pub async fn publish_checkout_transition<F, Fut>(
    root: &Path,
    source_root_identity: acyclic_fs::NativeRootIdentity,
    store: &LocalCoreStateStore,
    operation_id: OperationId,
    from: LocalGeneration,
    to_generation: LocalGeneration,
    before_effect: &F,
) -> Result<(), Error>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let publication = CheckoutPublication::new(root, source_root_identity, operation_id, &work)?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    publication
        .publish(&from, &to_generation, store, &options, &work, before_effect)
        .await?;
    Ok(())
}

impl<F, Fut> GitFilesystemExecutor for RootMaterializingGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    type Error = Error;

    async fn validate(&self) -> Result<(), Self::Error> {
        self.inner.validate().await
    }

    async fn validate_merge_resolution(
        &self,
        operation: OperationId,
        action: &GitFilesystemAction,
        tree: GitTreeRef,
    ) -> Result<GitFilesystemResult, Self::Error> {
        self.inner
            .validate_merge_resolution(operation, action, tree)
            .await
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        let from = self
            .inner
            .current
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        self.validate().await?;
        let result = self.inner.execute(operation_id, action).await;
        // Conflict projection is an intentional state transition: the join
        // executor leaves the Git transition pending and returns an error so
        // the caller can present the typed conflicts. Materialize that new
        // generation before propagating the error, otherwise the checkout
        // would never expose the files the user must edit.
        let materialized = self.materialize(operation_id, from).await;
        match (result, materialized) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) | (_, Err(error)) => Err(error),
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
