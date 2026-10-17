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
        // A raw facade cannot safely recover an opaque pending filesystem action.
        if state.pending.is_some() {
            return Err(Error::FilesystemAccessConfiguration("local fs facade"));
        }
        let mut workspaces = std::collections::BTreeSet::new();
        workspaces.extend(state.branches.values().map(|branch| branch.workspace_id));
        workspaces.extend(state.commits.values().flat_map(|commit| {
            [
                commit.tree.workspace_id(),
                commit.workspace_tree.workspace_id(),
            ]
        }));
        workspaces.extend(state.stash.iter().map(|tree| tree.workspace_id()));
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
    use crate::filesystem::{PreparationIo, access};
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    for grants in [ceiling, signed.capabilities()] {
        if crate::core::filesystem_scope_label(grants, &mut None)?.is_none()
            && (!grants.contains(&volume.capability(VolumeOperation::Read, &mut None)?)
                || !grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?))
        {
            return Err(Error::FilesystemAccessConfiguration(surface));
        }
    }
    let label = crate::core::filesystem_scope_label(signed.capabilities(), &mut None)?;
    let payload = match label {
        Some(_) => access::capability_payload(label, VolumeOperation::Read, &io)?,
        None => include_bytes!("../../filesystem/access/legacy-checkout.json"),
    };
    let compiled =
        access::access_view(workspace, signed, None, payload, Some(ceiling), &io).await?;
    if compiled.0.generation_sensitive() || compiled.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted(&compiled.0, &acyclic_fs::access::Action::ALL, surface)
}
