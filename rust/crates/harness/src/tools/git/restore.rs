use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn restore_paths(
        &self,
        operation_id: OperationId,
        tree: GitTreeRef,
        paths: &[String],
    ) -> Result<GitFilesystemResult, Error> {
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
        let outcome = self
            .current
            .restore_paths_from(
                &source,
                paths,
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
