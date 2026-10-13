use super::*;

impl PluginGitExecutor<'_> {
    pub(super) async fn generation(
        &self,
        tree: GitTreeRef,
        operation: &str,
    ) -> Result<LocalGeneration, PluginGitError> {
        let reference = self.exact(tree, operation).await?;
        self.workspace(reference.workspace_id)
            .await?
            .generation(
                reference.generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| display(failure.error).into())
    }

    pub(crate) fn error(message: impl Into<String>) -> PluginGitError {
        PluginGitError(message.into())
    }

    pub(crate) fn switched_workspace(&self) -> Result<Option<LocalLazyWorkspace>, PluginGitError> {
        self.switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))
            .map(|mut workspace| workspace.take())
    }

    pub(crate) async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<LocalWorkspace, PluginGitError> {
        WorkspaceGraph::new(self.store.clone())
            .workspace(
                self.fs,
                workspace_id,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(display)
            .map_err(PluginGitError)
    }

    pub(crate) async fn exact(
        &self,
        tree: GitTreeRef,
        operation: &str,
    ) -> Result<GitGenerationRef, PluginGitError> {
        match tree {
            GitTreeRef::Exact(reference) => Ok(reference),
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let lazy = self
                    .lazy_current
                    .open_related(
                        workspace,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(display)?;
                let snapshot_bytes = snapshot.id.into_bytes();
                let destination = format!("git-exact-{}", hex::encode(&snapshot_bytes[..12]));
                let exact = lazy
                    .exactify_snapshot(
                        snapshot,
                        &destination,
                        IdempotencyKey::from_bytes(snapshot_bytes[..16].try_into().map_err(
                            |_| Self::error(format!("{operation} snapshot identity is invalid")),
                        )?),
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(display)?;
                WorkspaceGraph::new(self.store.clone())
                    .register_existing_child(
                        lazy.workspace(),
                        &self
                            .fs
                            .open_workspace(
                                &destination,
                                acyclic_fs::WorkBudget::UNBOUNDED,
                                &acyclic_fs::CancellationToken::new(),
                            )
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(display)?,
                        snapshot.authored_generation,
                    )
                    .await
                    .map_err(display)?;
                Ok(GitGenerationRef {
                    workspace_id: exact.workspace_id(),
                    generation: exact.id(),
                })
            }
        }
    }
}
