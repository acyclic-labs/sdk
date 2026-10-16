use super::*;

/// Publishes git facade mutations into the ordinary checkout.
pub(crate) struct RootMaterializingGitExecutor<'a> {
    /// Managed workspace executor.
    pub inner: &'a WorkspaceGitExecutor<'a>,
    /// Ordinary checkout path.
    pub root: &'a Path,
    /// Pinned physical checkout identity.
    pub source_root_identity: acyclic_fs::NativeRootIdentity,
}

impl RootMaterializingGitExecutor<'_> {
    /// Publishes a generation transition into the checkout.
    pub(crate) async fn materialize(
        &self,
        operation_id: OperationId,
        from: LocalGeneration,
    ) -> Result<(), Error> {
        let target = self
            .inner
            .switched
            .lock()
            .map_err(|_| WorkspaceGitExecutor::error("Git workspace switch lock is unavailable"))?
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
        let to = to_generation.id();
        if from.id() == to {
            return Ok(());
        }
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        crate::filesystem::workspace_publication::publish_checkout(
            self.inner.store,
            self.root,
            self.source_root_identity,
            operation_id,
            &from,
            &to_generation,
            work,
        )
        .await?;
        crate::filesystem::workspace_publication::finish_checkout(
            self.inner.store,
            self.root,
            operation_id,
        )
        .await?;
        Ok(())
    }
}

impl GitFilesystemExecutor for RootMaterializingGitExecutor<'_> {
    type Error = Error;

    async fn validate(&self) -> Result<(), Self::Error> {
        self.inner.validate().await
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        let from = self
            .inner
            .current
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
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
