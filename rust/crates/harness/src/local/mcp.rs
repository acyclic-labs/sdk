use super::*;
use crate::{
    mcp::{McpClientLimits, McpInstallation},
    native_settings::{NativeSettings, SettingsConfig},
};

pub(super) async fn install(
    project: &LocalProject,
    settings: impl FnOnce(&Path) -> Result<SettingsConfig>,
    tools: ToolRegistry,
    limits: McpClientLimits,
) -> Result<McpInstallation> {
    let path = match project {
        LocalProject::Workspace(path) => path,
        LocalProject::Volume { directory, .. } => directory,
    };
    let workspace = std::fs::canonicalize(path).map_err(Error::process_io)?;
    // Selection keeps the configured spelling; process startup uses the canonical root.
    NativeSettings::load(settings(path)?)
        .await?
        .mcp
        .install(tools, &workspace, limits)
        .await
}

pub(super) fn user_settings(workspace: &Path) -> Result<SettingsConfig> {
    Ok(SettingsConfig::new(
        workspace,
        dirs::home_dir()
            .ok_or_else(|| Error::Unsupported("user settings directory is unavailable".into()))?,
    ))
}
