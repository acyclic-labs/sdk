use super::*;

impl PluginGitExecutor<'_> {
    pub(super) async fn generation(
        &self,
        tree: GitTreeRef,
    ) -> Result<LocalGeneration, PluginGitError> {
        match tree {
            GitTreeRef::Exact(reference) => self
                .workspace(reference.workspace_id)
                .await?
                .generation(
                    reference.generation,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| display(failure.error).into()),
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
                let mut identity = [0; 16];
                identity.copy_from_slice(&snapshot_bytes[..16]);
                let exact = lazy
                    .exactify_snapshot(
                        snapshot,
                        &destination,
                        IdempotencyKey::from_bytes(identity),
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
                                WorkBudget::UNBOUNDED,
                                &CancellationToken::new(),
                            )
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(display)?,
                        snapshot.authored_generation,
                    )
                    .await
                    .map_err(display)?;
                Ok(exact)
            }
        }
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
        if workspace_id == self.current.id() {
            return Ok(self.current.clone());
        }
        let record = WorkspaceGraph::new(self.store.clone())
            .resolve(workspace_id)
            .await
            .map_err(display)
            .map_err(PluginGitError)?;
        let workspace = self
            .fs
            .open_workspace(
                &record.workspace_name,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| Self::error(display(failure.error)))?;
        if workspace.id() != workspace_id {
            return Err(Self::error("workspace-lineage state is incompatible"));
        }
        Ok(workspace)
    }
}
