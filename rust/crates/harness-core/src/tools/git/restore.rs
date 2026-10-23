use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    /// The generation a restore under `operation_id` already committed. A
    /// restore's paths and expected head derive from the live workspace, so
    /// a retry after later writes would build a different transaction under
    /// the same identity; the durable operation record names the one result
    /// every retry of that operation replays.
    pub(super) async fn committed_restore(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<GitFilesystemResult>, Error> {
        let generation = self
            .current
            .operation_generation(
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| Error::from(failure.error))?;
        Ok(generation.map(|generation| GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(
                generation.workspace_id(),
                generation.id(),
            )),
            tracked_paths: None,
        }))
    }

    pub(super) async fn restore_paths(
        &self,
        operation_id: OperationId,
        tree: GitTreeRef,
        paths: &[String],
    ) -> Result<GitFilesystemResult, Error> {
        if let Some(replayed) = self.committed_restore(operation_id).await? {
            return Ok(replayed);
        }
        let source = self.generation(tree).await?;
        let current = self
            .current
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        // Every tree written over the live workspace leaves its reserved
        // paths, and the directories holding them, in place.
        let paths = git_paths_preserving_reserved(
            &current,
            Some(&source),
            &self.ignore,
            paths.iter().cloned(),
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(Error::from)?;
        if paths.is_empty() {
            // Only reserved paths, or the directories holding them, were
            // named: the workspace already matches everywhere else.
            return Ok(GitFilesystemResult::Applied {
                tree: Some(GitTreeRef::exact(self.current.id(), current.id())),
                tracked_paths: None,
            });
        }
        let outcome = self
            .current
            .restore_paths_from(
                &source,
                &paths,
                current.id(),
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                self.permit,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        committed(outcome)
    }
}
