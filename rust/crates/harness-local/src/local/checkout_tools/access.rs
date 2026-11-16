//! Conservative admission for adapters that expose a raw checkout.
use super::*;

impl CheckoutTools {
    /// Select and authorize the same workspace while its command guard is held.
    /// The shared check admits structured filesystem grants and exact volume
    /// grants, and refuses any restrictive policy before exposing raw files.
    pub(super) async fn command_workspace(
        &self,
        state: &mut State,
        context: &ToolContext,
        surface: &'static str,
    ) -> Result<Arc<LocalWorkspace>> {
        let workspace = self
            .workspace
            .selected_workspace(state, context.task())
            .await?;
        let view = state
            .active
            .as_ref()
            .unwrap_or_else(|| self.workspace.view());
        require_mount_access(
            view.workspace(),
            &workspace.mounts,
            &self.signed,
            context.effective_scope().grants(),
            surface,
        )
        .await?;
        Ok(workspace)
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
