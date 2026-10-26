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
    crate::filesystem::require_unrestricted_volume_access(
        workspace, volume, signed, ceiling, surface,
    )
    .await
}
