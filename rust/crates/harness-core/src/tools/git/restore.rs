use super::*;

impl<P, O, S> WorkspaceGitExecutor<'_, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
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
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
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
        tree: &GitTreeRef,
        paths: &[String],
    ) -> Result<GitFilesystemResult, Error> {
        if let Some(replayed) = self.committed_restore(operation_id).await? {
            return Ok(replayed);
        }
        let source = self.generation(tree).await?;
        let current = self
            .current
            .head(
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        if paths.is_empty() {
            // The caller selected no paths.
            return Ok(GitFilesystemResult::Applied {
                tree: Some(GitTreeRef::exact(self.current.id(), current.id())),
                tracked_paths: None,
            });
        }
        let outcome = self
            .current
            .restore_paths_from(
                acyclic_fs::PathRestore {
                    source: &source,
                    paths,
                    if_current: current.id(),
                    idempotency_key: IdempotencyKey::from_bytes(operation_id.into_bytes()),
                },
                self.permit,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                |_| async {
                    Ok::<_, acyclic_fs_work::performance::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs_work::performance::OperationReceipt {
                            value: (),
                            work: acyclic_fs_work::performance::WorkCounters::UNCHARGED,
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
