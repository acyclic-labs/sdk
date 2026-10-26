//! Repository sources are authorized before exposing saved metadata or recovering an intent.
use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn authorize_repository(&self) -> crate::Result<()> {
        let Some(state) = acyclic_fs::GitCompatStore::load(self.store, self.repository_id)
            .await
            .map_err(Error::from)?
        else {
            return Ok(());
        };
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
