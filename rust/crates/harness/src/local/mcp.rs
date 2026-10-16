use super::*;
use crate::{
    mcp::{McpClientLimits, McpInstallation},
    native_settings::{NativeSettings, SettingsConfig},
};

pub(super) async fn install(
    project: &LocalProject,
    settings: Option<SettingsConfig>,
    tools: ToolRegistry,
    limits: McpClientLimits,
) -> Result<McpInstallation> {
    let path = match project {
        LocalProject::Workspace(path) => path,
        LocalProject::Volume { directory, .. } => directory,
    };
    let workspace = std::fs::canonicalize(path).map_err(Error::process_io)?;
    let settings = match settings {
        Some(settings) => settings,
        None => SettingsConfig::new(
            &workspace,
            dirs::home_dir().ok_or_else(|| {
                Error::Unsupported("user settings directory is unavailable".into())
            })?,
        ),
    };
    NativeSettings::load(settings)
        .await?
        .mcp
        .install(tools, &workspace, limits)
        .await
}
