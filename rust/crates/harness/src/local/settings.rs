use super::*;
use crate::native_settings::SettingsConfig;

pub(super) fn configuration(
    project: &LocalProject,
    configured: Option<SettingsConfig>,
) -> Result<Arc<SettingsConfig>> {
    let mut configured = match configured {
        Some(configured) => configured,
        None => SettingsConfig::new(project.root(), user_home()?),
    };
    configured.load_mcp = cfg!(any(feature = "mcp-stdio", feature = "mcp-http"));
    Ok(Arc::new(configured))
}

/// The user's home, which holds user settings, MCP configuration and CODEX_HOME. This is the only place the harness
/// consults it. Unit tests cannot reach it: a test that does not inject settings fails here instead of reading the
/// machine's real settings and credentials.
#[cfg(not(test))]
fn user_home() -> Result<PathBuf> {
    dirs::home_dir()
        .ok_or_else(|| Error::Unsupported("user settings directory is unavailable".into()))
}

/// Unit tests have no user home; see the production definition.
#[cfg(test)]
fn user_home() -> Result<PathBuf> {
    Err(Error::Unsupported(UNIT_TESTS_INJECT_SETTINGS.into()))
}

#[cfg(test)]
pub(super) const UNIT_TESTS_INJECT_SETTINGS: &str =
    "unit tests must inject LocalHarnessConfig::settings; the user's home is unreachable";
