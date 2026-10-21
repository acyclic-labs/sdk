use super::*;
use crate::filesystem::NativeDirectory;
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
    before_effect().await?;
    let to = to_generation.id();
    let operation_directory = NativeDirectory(root).publication_directory(operation_id)?;
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| Error::from(cause))?;
    let journal = acyclic_fs::load_native_materialization_journal(
        store,
        operation_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .map_err(|(cause, _)| Error::from(cause))?
    .value;
    if work.root_identity(root).map_err(Error::from)? != source_root_identity {
        return Err(Error::Unauthorized(
            "checkout root changed before publication".into(),
        ));
    }
    let mut from = from;
    if let Some(journal) = &journal {
        // A git action can commit its logical generation before its native
        // publication is acknowledged. Its retry then observes the target as
        // the current head; resume the original journal's source generation.
        if from.id() == to
            && let acyclic_fs::MaterializationIntent::GenerationTransition {
                from: original,
                to: target,
            } = journal.plan.intent
            && target == to
        {
            from = from
                .workspace()
                .generation(original, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| Error::from(failure.error))?
                .value;
        }
        if completed(journal, source_root_identity, from.id(), to)? {
            // The durable journal is also the completion marker. A retry must
            // not overwrite edits made after an acknowledged publication.
            cleanup(root, &operation_directory)?;
            return Ok(());
        }
    } else if from.id() == to {
        return Ok(());
    }
    drop(journal);
    let parent = operation_directory
        .parent()
        .ok_or_else(|| Error::Storage("staging parent is absent".into()))?;
    std::fs::create_dir_all(parent)
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?;
    let actual =
        acyclic_fs::validate_native_preparation_location(root, &operation_directory, &work)
            .map_err(Error::from)?;
    if actual != source_root_identity {
        return Err(Error::Storage(
            "checkout root changed before publication".into(),
        ));
    }
    let options = MaterializeOptions {
        destination: operation_directory.join("target"),
        expected_root_identity: None,
        maximum_directory_entries: 4_096,
        maximum_extent_spans: 65_536,
        transfer_bytes: 8 * 1024 * 1024,
    };
    acyclic_fs::publish_native_generation_transition(
        &from,
        &to_generation,
        store,
        acyclic_fs::NativeWorkspacePublication {
            root,
            source_root_identity,
            operation_directory: &operation_directory,
            operation_id,
            from: from.id(),
            to,
            excluded_names: &[".git", ".system", ".volumes"],
            options: &options,
            budget: WorkBudget::UNBOUNDED,
            cancellation: &cancellation,
        },
        work,
        || async {
            before_effect().await.map_err(|error| {
                acyclic_fs::NativeWorkspacePublicationError::Native(
                    acyclic_fs::NativeTreeMaterializationError::Io(std::io::Error::other(error)),
                )
            })
        },
    )
    .await
    .map_err(|(cause, _)| Error::from(cause))?;
    cleanup(root, &operation_directory)
}

fn completed(
    journal: &acyclic_fs::MaterializationJournal,
    root: acyclic_fs::NativeRootIdentity,
    from: acyclic_fs::GenerationId,
    to: acyclic_fs::GenerationId,
) -> Result<bool, Error> {
    if journal.root_identity != Some(root.to_bytes())
        || journal.plan.intent
            != (acyclic_fs::MaterializationIntent::GenerationTransition { from, to })
    {
        return Err(Error::Storage(
            "checkout publication differs from its original generation transition".into(),
        ));
    }
    Ok(journal.phase == acyclic_fs::MaterializationPhase::Applied)
}

fn cleanup(_root: &Path, operation_directory: &Path) -> Result<(), Error> {
    if !operation_directory
        .try_exists()
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?
    {
        return Ok(());
    }
    let parent = operation_directory
        .parent()
        .ok_or_else(|| Error::Invalid("native staging directory has no parent".into()))?;
    acyclic_native_runtime::remove_child_directory(parent, operation_directory)
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))
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

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
