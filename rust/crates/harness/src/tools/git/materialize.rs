use super::*;

/// Publishes git facade mutations into the ordinary checkout.
pub(crate) struct RootMaterializingGitExecutor<'a> {
    /// Managed workspace executor.
    pub inner: WorkspaceGitExecutor<'a>,
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
            .map_err(display)?;
        let to = to_generation.id();
        if from.id() == to {
            return Ok(());
        }
        let operation_directory = NativeDirectory(self.root)
            .publication_directory(operation_id)
            .map_err(Error::Storage)?;
        let options = MaterializeOptions {
            destination: operation_directory.join("target"),
            expected_root_identity: None,
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let cancellation = CancellationToken::new();
        acyclic_fs::publish_native_generation_transition(
            &from,
            &to_generation,
            self.inner.store,
            acyclic_fs::NativeWorkspacePublication {
                root: self.root,
                source_root_identity: self.source_root_identity,
                operation_directory: &operation_directory,
                operation_id,
                from: from.id(),
                to,
                excluded_names: &[".git"],
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
            acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| display(cause))?,
            || std::future::ready(Ok(())),
        )
        .await
        .map_err(|(cause, _)| display(cause))?;
        self.inner
            .store
            .remove_materialization_async(operation_id)
            .await
            .map_err(display)?;
        NativeDirectory(
            &NativeDirectory(self.root)
                .publication_root()
                .map_err(Error::Storage)?,
        )
        .remove(&operation_directory)
        .map_err(display)?;
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
            .map_err(display)?;
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
