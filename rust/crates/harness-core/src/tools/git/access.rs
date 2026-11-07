//! Repository sources are authorized before exposing saved metadata or recovering an intent.
use super::*;

pub(crate) struct RepositoryAccess<'a, O: 'static, S> {
    pub access: &'a crate::filesystem::WorkspaceAccess,
    pub filesystem: &'a acyclic_fs::Fs<acyclic_fs::StreamAuthorityStore, O>,
    pub store: &'a S,
    pub current: &'a ManagedWorkspace<O>,
    pub repository: acyclic_fs::WorkspaceId,
}

impl<'context, O, S> RepositoryAccess<'context, O, S>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(crate) fn request_result<'task>(
        &'task self,
        operation: acyclic_fs::OperationId,
    ) -> impl std::future::Future<Output = crate::Result<Option<GitCommandOutput>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
            let state = self.state().await?;
            if state.request(operation).is_none() {
                return Ok(None);
            }
            self.require_result_access(&state).await?;
            Ok(state.request(operation).cloned())
        }
    }

    /// Authorizes saved output without copying it or running its command.
    pub(crate) fn require_saved_result_access<'task>(
        &'task self,
    ) -> impl std::future::Future<Output = crate::Result<()>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move { self.require_result_access(&self.state().await?).await }
    }

    fn state<'task>(
        &'task self,
    ) -> impl std::future::Future<Output = crate::Result<acyclic_fs::GitCompatState>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
            GitCompatRepository::new(self.repository, self.store.clone())
                .load()
                .await
                .map_err(super::errors::compat_error::<S>)
        }
    }

    fn require_result_access<'task>(
        &'task self,
        state: &'task acyclic_fs::GitCompatState,
    ) -> impl std::future::Future<Output = crate::Result<()>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
            self.authorize(state).await?;
            self.access.require(self.current).await?;
            Ok(())
        }
    }

    fn authorize<'task>(
        &'task self,
        state: &'task acyclic_fs::GitCompatState,
    ) -> impl std::future::Future<Output = crate::Result<()>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
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
    }

    pub(super) fn workspace<'task>(
        &'task self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> impl std::future::Future<Output = crate::Result<ManagedWorkspace<O>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
            if workspace_id == self.current.id() {
                self.access.require(self.current).await?;
                return Ok(self.current.clone());
            }
            let graph = WorkspaceGraph::new(self.store.clone());
            graph
                .authorize_descendant(workspace_id, self.repository)
                .await
                .map_err(|error| match error {
                    acyclic_fs::WorkspaceLineageError::UnauthorizedJoin => Error::Unauthorized(
                        "Git workspace is outside its repository lineage".into(),
                    ),
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
}

impl<'context, O, S> WorkspaceGitExecutor<'context, O, S>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(super) fn repository_access(&self) -> RepositoryAccess<'_, O, S> {
        RepositoryAccess {
            access: self.access,
            filesystem: self.fs,
            store: self.store,
            current: &self.current,
            repository: self.repository_id,
        }
    }

    pub(super) fn authorize_repository<'task>(
        &'task self,
    ) -> impl std::future::Future<Output = crate::Result<()>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, O, S> {
        async move {
            let state = GitCompatRepository::new(self.repository_id, self.store.clone())
                .load()
                .await
                .map_err(super::errors::compat_error::<S>)?;
            self.repository_access().authorize(&state).await
        }
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
