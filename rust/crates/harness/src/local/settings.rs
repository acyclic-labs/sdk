use super::*;
use crate::native_settings::SettingsConfig;

pub(super) fn configuration(
    project: &LocalProject,
    configured: Option<SettingsConfig>,
) -> Result<Arc<SettingsConfig>> {
    let mut configured = match configured {
        Some(configured) => configured,
        None => SettingsConfig::new(
            project.root(),
            dirs::home_dir().ok_or_else(|| {
                Error::Unsupported("user settings directory is unavailable".into())
            })?,
        ),
    };
    configured.load_mcp = cfg!(any(feature = "mcp-stdio", feature = "mcp-http"));
    Ok(Arc::new(configured))
}
