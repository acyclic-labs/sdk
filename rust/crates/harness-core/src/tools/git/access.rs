//! Repository sources are authorized before exposing saved metadata or recovering an intent.
use super::*;

pub(crate) struct RepositoryAccess<'a, P: ?Sized + 'static, O: 'static, S> {
    pub access: &'a crate::filesystem::WorkspaceAccess,
    pub filesystem: &'a acyclic_fs::Fs<acyclic_fs::StreamAuthorityStore<P>, O>,
    pub store: &'a S,
    pub current: &'a ManagedWorkspace<P, O>,
    pub repository: acyclic_fs::WorkspaceId,
}

impl<P, O, S> RepositoryAccess<'_, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(crate) async fn request_result(
        &self,
        operation: acyclic_fs::OperationId,
    ) -> crate::Result<Option<GitCommandOutput>> {
        let state = self.state().await?;
        if state.request(operation).is_none() {
            return Ok(None);
        }
        self.require_result_access(&state).await?;
        Ok(state.request(operation).cloned())
    }

    /// Authorizes saved output without copying it or running its command.
    pub(crate) async fn require_saved_result_access(&self) -> crate::Result<()> {
        self.require_result_access(&self.state().await?).await
    }

    async fn state(&self) -> crate::Result<acyclic_fs::GitCompatState> {
        GitCompatRepository::new(self.repository, self.store.clone())
            .load()
            .await
            .map_err(super::errors::compat_error::<S>)
    }

    async fn require_result_access(&self, state: &acyclic_fs::GitCompatState) -> crate::Result<()> {
        self.authorize(state).await?;
        self.access.require(self.current).await?;
        Ok(())
    }

    async fn authorize(&self, state: &acyclic_fs::GitCompatState) -> crate::Result<()> {
        let mut workspaces = BTreeSet::new();
        workspaces.extend(state.branches.values().map(|branch| branch.workspace_id));
        workspaces.extend(state.retired_workspaces.iter().copied());
        workspaces.extend(state.commits.values().flat_map(|commit| {
            [
                commit.tree.workspace_id(),
                commit.workspace_tree.workspace_id(),
            ]
        }));
        workspaces.extend(state.stash.iter().map(|tree| tree.workspace_id()));
        if let Some(pending) = &state.pending {
            pending_workspaces(&pending.action, &mut workspaces)?;
        }
        if let Some((_, pending)) = state.checkout_reconciliation {
            workspaces.insert(pending.workspace_id);
        }
        for id in workspaces {
            self.workspace(id).await?;
        }
        Ok(())
    }

    pub(super) async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> crate::Result<ManagedWorkspace<P, O>> {
        if workspace_id == self.current.id() {
            self.access.require(self.current).await?;
            return Ok(self.current.clone());
        }
        let graph = WorkspaceGraph::new(self.store.clone());
        graph
            .authorize_descendant(workspace_id, self.repository)
            .await
            .map_err(|error| match error {
                acyclic_fs::WorkspaceLineageError::UnauthorizedJoin => {
                    Error::Unauthorized("Git workspace is outside its repository lineage".into())
                }
                error => S::lineage_error(error),
            })?;
        let workspace = acyclic_fs::WorkspaceResolver::resolve(
            &((*self.filesystem).clone(), graph),
            workspace_id,
        )
        .await
        .map_err(S::lineage_error)?;
        self.access.require(&workspace).await?;
        Ok(workspace)
    }
}

impl<F, Fut, P, O, S> WorkspaceGitExecutor<'_, F, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
    F: Fn() -> Fut + acyclic_stream::ProviderPlatform,
    Fut: Future<Output = Result<(), Error>> + acyclic_stream::ProviderTask,
{
    pub(super) fn repository_access(&self) -> RepositoryAccess<'_, P, O, S> {
        RepositoryAccess {
            access: self.access,
            filesystem: self.fs,
            store: self.store,
            current: &self.current,
            repository: self.repository_id,
        }
    }

    pub(super) async fn authorize_repository(&self) -> crate::Result<()> {
        let state = GitCompatRepository::new(self.repository_id, self.store.clone())
            .load()
            .await
            .map_err(super::errors::compat_error::<S>)?;
        self.repository_access().authorize(&state).await
    }
}

// Recovery executes the same typed action as first admission. Authenticate its
// sources even if a branch was removed after the intent was persisted.
fn pending_workspaces(
    action: &acyclic_fs::GitFilesystemAction,
    workspaces: &mut std::collections::BTreeSet<acyclic_fs::WorkspaceId>,
) -> crate::Result<()> {
    use acyclic_fs::GitFilesystemAction as Action;
    match action {
        Action::CaptureCommit {
            workspace_tree,
            head_tree,
            head_workspace_tree,
            ..
        } => {
            workspaces.insert(workspace_tree.workspace_id());
            workspaces.extend(head_tree.iter().map(|tree| tree.workspace_id()));
            workspaces.extend(head_workspace_tree.iter().map(|tree| tree.workspace_id()));
        }
        Action::ForkBranch { source_tree, .. } => {
            workspaces.insert(source_tree.workspace_id());
        }
        Action::SwitchWorkspace { workspace_id } => {
            workspaces.insert(*workspace_id);
        }
        Action::Diff { from, to, .. } => {
            workspaces.extend(from.iter().map(|tree| tree.workspace_id()));
            workspaces.insert(to.workspace_id());
        }
        Action::RestoreGeneration { tree, .. }
        | Action::RestorePaths { tree, .. }
        | Action::Grep { tree, .. }
        | Action::Clean { tree, .. }
        | Action::Archive { tree }
        | Action::CheckIgnore { tree, .. } => {
            workspaces.insert(tree.workspace_id());
        }
        Action::Join {
            target_tree,
            source_workspace,
            ..
        } => {
            workspaces.insert(target_tree.workspace_id());
            workspaces.insert(*source_workspace);
        }
        Action::ApplyCommit { base, source, .. } => {
            workspaces.extend(base.iter().chain(source).map(|tree| tree.workspace_id()));
        }
        Action::Blame { commits, .. } => {
            workspaces.extend(commits.iter().flat_map(|commit| {
                [
                    commit.tree.workspace_id(),
                    commit.workspace_tree.workspace_id(),
                ]
            }));
        }
        Action::ApplyPatch { .. } => {}
        _ => return Err(Error::FilesystemAccessConfiguration("Git facade")),
    }
    Ok(())
}
