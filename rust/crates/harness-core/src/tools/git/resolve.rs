use super::*;

impl<P, O, S> WorkspaceGitExecutor<'_, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(super) async fn generation(
        &self,
        tree: GitTreeRef,
    ) -> Result<ManagedGeneration<P, O>, Error> {
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
                .map_err(|failure| Error::from(failure.error).into()),
            #[cfg(not(target_arch = "wasm32"))]
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let View::Checkout(current) = self.lazy_current else {
                    return Err(Error::Unauthorized(
                        "volume repository refers to a foreign native source".into(),
                    ));
                };
                let lazy = current
                    .inspect_related(
                        workspace,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(super::errors::inspection_error)?;
                let generation = lazy
                    .exact_generation(
                        snapshot,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(Error::from)?;
                // Retain each distinct returned value through the source
                // workspace's lifetime. Repeated inspections of unchanged
                // content share an immutable pin, never a mutable scratch view.
                generation
                    .retain_if_unpublished(
                        format!(
                            "git-exact-{}",
                            hex::encode(generation.id().to_bytes().as_slice())
                        ),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)
                    .map_err(Error::from)?;
                Ok(generation)
            }
            #[cfg(target_arch = "wasm32")]
            GitTreeRef::Lazy(_) => Err(Error::Unsupported(
                "native source snapshots require a native checkout".into(),
            )),
        }
    }

    /// Constructs a facade error.
    pub(crate) fn error(message: impl Into<String>) -> Error {
        Error::Conflict(message.into())
    }

    /// Takes the workspace selected by a branch switch.
    pub(crate) fn switched_workspace(&self) -> Result<Option<OwnedView<P, O>>, Error> {
        self.switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))
            .map(|mut workspace| workspace.take())
    }

    /// Resolves a workspace through the repository lineage.
    pub(crate) async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<ManagedWorkspace<P, O>, Error> {
        self.repository_access().workspace(workspace_id).await
    }
}
