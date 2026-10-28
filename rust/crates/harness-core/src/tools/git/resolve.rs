use super::*;

impl<F, Fut, P, O, S> WorkspaceGitExecutor<'_, F, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn generation(&self, tree: GitTreeRef) -> Result<ManagedGeneration<P, O>, Error> {
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
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let View::Checkout(current) = self.lazy_current else {
                    return Err(Error::Unauthorized(
                        "volume repository refers to a foreign native source".into(),
                    ));
                };
                let lazy = current
                    .open_related(
                        workspace,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(Error::from)?;
                let snapshot_bytes = snapshot.id.into_bytes();
                let destination = format!("git-exact-{}", hex::encode(&snapshot_bytes[..12]));
                let mut identity = [0; 16];
                identity.copy_from_slice(&snapshot_bytes[..16]);
                let (child, initial) = lazy
                    .fork_snapshot(
                        &destination,
                        snapshot,
                        IdempotencyKey::from_bytes(identity),
                        PublicationPermit::Unrestricted,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(Error::from)?;
                WorkspaceGraph::new(self.store.clone())
                    .register_existing_child_at_generation(
                        lazy.workspace(),
                        child.workspace(),
                        snapshot.authored_generation,
                        initial.authored_generation,
                    )
                    .await
                    .map_err(S::lineage_error)?;
                child
                    .exactify(
                        "/",
                        PublicationPermit::Unrestricted,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(Error::from)
            }
            #[cfg(not(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32"))))]
            GitTreeRef::Lazy(_) => Err(Error::Unsupported("native source snapshots require a native checkout".into())),
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
        if workspace_id == self.current.id() {
            self.access.require(&self.current).await?;
            return Ok(self.current.clone());
        }
        let graph = WorkspaceGraph::new(self.store.clone());
        graph
            .authorize_descendant(workspace_id, self.repository_id)
            .await
            .map_err(|error| match error {
                acyclic_fs::WorkspaceLineageError::UnauthorizedJoin => {
                    Error::Unauthorized("Git workspace is outside its repository lineage".into())
                }
                error => S::lineage_error(error),
            })?;
        let workspace = acyclic_fs::WorkspaceResolver::resolve(
            &((*self.fs).clone(), WorkspaceGraph::new(self.store.clone())),
            workspace_id,
        )
        .await
        .map_err(S::lineage_error)?;
        self.access.require(&workspace).await?;
        Ok(workspace)
    }
}
