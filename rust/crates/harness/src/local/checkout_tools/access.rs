//! Conservative admission for adapters that expose a raw checkout.
use super::*;

impl CheckoutTools {
    pub(super) async fn require_command_access(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        ceiling: &Capabilities,
        facade: bool,
    ) -> Result<()> {
        require_mount_access(
            workspace,
            &self.workspace.mounts,
            &self.signed,
            ceiling,
            if facade {
                "local fs facade"
            } else {
                "local bash"
            },
        )
        .await?;
        if facade {
            self.require_repository_access(workspace.id(), ceiling)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn require_repository_access(
        &self,
        selected: acyclic_fs::WorkspaceId,
        ceiling: &Capabilities,
    ) -> Result<()> {
        let state = acyclic_fs::GitCompatStore::load(
            &self.workspace.store,
            self.workspace.view().workspace().id(),
        )
        .await
        .map_err(storage)?;
        let Some(state) = state else {
            return Ok(());
        };
        let mut workspaces = std::collections::BTreeSet::new();
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
        workspaces.remove(&selected);
        for id in workspaces {
            let root = self.workspace.view().workspace();
            if id == root.id() {
                // Task roots are authenticated lazy bindings, not registrations
                // in the separate branch-lineage catalog.
                require_mount_access(
                    root,
                    &self.workspace.mounts,
                    &self.signed,
                    ceiling,
                    "local fs facade",
                )
                .await?;
                continue;
            }
            let workspace = acyclic_fs::WorkspaceResolver::resolve(
                &(
                    self.workspace.filesystem.clone(),
                    acyclic_fs::WorkspaceGraph::new(self.workspace.store.clone()),
                ),
                id,
            )
            .await
            .map_err(storage)?;
            require_mount_access(
                &workspace,
                &self.workspace.mounts,
                &self.signed,
                ceiling,
                "local fs facade",
            )
            .await?;
        }
        Ok(())
    }
}

// Recovery executes the same typed action as first admission. Authenticate its
// sources even if a branch was removed after the intent was persisted.
fn pending_workspaces(
    action: &acyclic_fs::GitFilesystemAction,
    workspaces: &mut std::collections::BTreeSet<acyclic_fs::WorkspaceId>,
) -> Result<()> {
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
        _ => return Err(Error::FilesystemAccessConfiguration("local fs facade")),
    }
    Ok(())
}

pub(in crate::local) async fn require_mount_access(
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    mounts: &workspace::Mounts,
    signed: &Scope,
    ceiling: &Capabilities,
    surface: &'static str,
) -> Result<()> {
    let (_, primary) = mounts.named(None)?;
    require_volume_access(workspace, &primary.view.volume, signed, ceiling, surface).await?;
    for (_, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            require_volume_access(
                source.workspace.workspace(),
                &mount.view.volume,
                signed,
                ceiling,
                surface,
            )
            .await?;
        }
    }
    Ok(())
}

pub(super) async fn require_volume_access(
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    volume: &VolumeRef,
    signed: &Scope,
    ceiling: &Capabilities,
    surface: &'static str,
) -> Result<()> {
    crate::filesystem::require_unrestricted_volume_access(
        workspace, volume, signed, ceiling, surface,
    )
    .await
}
