//! One native settings snapshot for hooks and MCP connection declarations.

use crate::{
    Error, Result,
    mcp::config::{Dialect, ImportedServer, McpConfig},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    io::Read as _,
    path::{Path, PathBuf},
};
mod model;
mod model_selection;
mod workspace;

/// Native user locations and independent trust for repository-supplied commands.
pub struct SettingsConfig {
    workspace: PathBuf,
    home: PathBuf,
    codex_home: PathBuf,
    trust_project_hooks: bool,
    trust_project_mcp: bool,
    maximum_file_bytes: u64,
    pub(crate) load_mcp: bool,
    #[cfg(feature = "test-support")]
    _test_home: Option<tempfile::TempDir>,
}

impl SettingsConfig {
    /// Selects user settings by default; repository settings require explicit trust.
    pub fn new(workspace: impl Into<PathBuf>, home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let codex_home = codex_directory(&home, configured_codex_home().as_deref());
        Self::with_roots(workspace.into(), home, codex_home)
    }

    fn with_roots(workspace: PathBuf, home: PathBuf, codex_home: PathBuf) -> Self {
        Self {
            workspace,
            home,
            codex_home,
            trust_project_hooks: false,
            trust_project_mcp: false,
            maximum_file_bytes: 1_048_576,
            load_mcp: true,
            #[cfg(feature = "test-support")]
            _test_home: None,
        }
    }

    #[cfg(all(
        feature = "test-support",
        feature = "filesystem-local",
        any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
    ))]
    pub(crate) fn for_test(workspace: impl Into<PathBuf>) -> Result<Self> {
        let home = tempfile::tempdir().map_err(Error::process_io)?;
        let mut settings = Self::with_roots(
            workspace.into(),
            home.path().to_owned(),
            home.path().join(".codex"),
        );
        settings._test_home = Some(home);
        Ok(settings)
    }

    /// Overrides CODEX_HOME or ~/.codex for this snapshot.
    #[must_use]
    pub fn codex_home(mut self, directory: impl Into<PathBuf>) -> Self {
        self.codex_home = directory.into();
        self
    }

    /// Permits repository-supplied hooks, independently of MCP server trust.
    #[must_use]
    pub fn trust_project_hooks(mut self, trust: bool) -> Self {
        self.trust_project_hooks = trust;
        self
    }

    /// Permits repository-supplied MCP servers, including native executables.
    #[must_use]
    pub fn trust_project_mcp(mut self, trust: bool) -> Self {
        self.trust_project_mcp = trust;
        self
    }

    /// Replaces the positive per-file read bound.
    #[must_use]
    pub fn maximum_file_bytes(mut self, maximum: u64) -> Self {
        self.maximum_file_bytes = maximum;
        self
    }
}

/// Host-owned parsed configuration; connection secrets stay outside task records.
#[derive(Default)]
pub struct NativeSettings {
    /// Normalized, compiled lifecycle hooks.
    #[cfg(feature = "hooks")]
    pub hooks: crate::hooks::Hooks,
    /// Enabled MCP declarations after whole-entry scope overrides.
    pub mcp: McpConfig,
}

struct Source {
    path: PathBuf,
    hooks: bool,
    mcp: Option<Dialect>,
    local: bool,
}

struct ReadPolicy {
    toml: bool,
    hook_order: Option<usize>,
    hook_settings_order: usize,
    mcp: Option<Dialect>,
    mcp_order: usize,
    local: bool,
    order: usize,
}

impl NativeSettings {
    /// Reads each resolved file once, selecting its parser from the configured filename.
    pub async fn load(config: impl Into<std::sync::Arc<SettingsConfig>>) -> Result<Self> {
        let config = config.into();
        let load_mcp = config.load_mcp;
        Self::snapshot(config, load_mcp, true).await
    }

    #[cfg(all(feature = "hooks", feature = "filesystem-local"))]
    pub(crate) async fn load_hooks(
        config: std::sync::Arc<SettingsConfig>,
    ) -> Result<crate::hooks::Hooks> {
        Ok(Self::snapshot(config, false, true).await?.hooks)
    }

    #[cfg(all(
        feature = "filesystem-local",
        any(feature = "mcp-stdio", feature = "mcp-http")
    ))]
    pub(crate) async fn load_mcp(config: std::sync::Arc<SettingsConfig>) -> Result<McpConfig> {
        Ok(Self::snapshot(config, true, false).await?.mcp)
    }

    async fn snapshot(
        config: std::sync::Arc<SettingsConfig>,
        load_mcp: bool,
        load_hooks: bool,
    ) -> Result<Self> {
        if config.maximum_file_bytes == 0 || config.maximum_file_bytes == u64::MAX {
            return Err(Error::Invalid(
                "settings require a finite positive byte bound".into(),
            ));
        }
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        acyclic_native_runtime::run_blocking_io(
            move || Self::read(&config, load_mcp, load_hooks),
            None,
        )
        .map_err(Error::process_io)?
        .await
        .map_err(Error::process_io)?
    }

    fn read(config: &SettingsConfig, load_mcp: bool, load_hooks: bool) -> Result<Self> {
        let configured_workspace = &config.workspace;
        let workspace = std::fs::canonicalize(configured_workspace).map_err(Error::process_io)?;
        let hooks = cfg!(feature = "hooks") && load_hooks;
        let mut sources = vec![
            Source {
                path: config.home.join(".claude.json"),
                hooks: false,
                mcp: Some(Dialect::Claude),
                local: true,
            },
            Source {
                path: config.home.join(".claude/settings.json"),
                hooks,
                mcp: None,
                local: false,
            },
            Source {
                path: config.codex_home.join("hooks.json"),
                hooks,
                mcp: None,
                local: false,
            },
            Source {
                path: config.codex_home.join("config.toml"),
                hooks,
                mcp: Some(Dialect::Codex),
                local: false,
            },
        ];
        if hooks && config.trust_project_hooks {
            for name in [
                ".claude/settings.json",
                ".claude/settings.local.json",
                ".codex/hooks.json",
            ] {
                sources.push(Source {
                    path: workspace.join(name),
                    hooks: true,
                    mcp: None,
                    local: false,
                });
            }
        }
        if config.trust_project_mcp {
            sources.push(Source {
                path: workspace.join(".mcp.json"),
                hooks: false,
                mcp: Some(Dialect::Claude),
                local: false,
            });
        }
        if hooks && config.trust_project_hooks || config.trust_project_mcp {
            sources.push(Source {
                path: workspace.join(".codex/config.toml"),
                hooks: hooks && config.trust_project_hooks,
                mcp: config.trust_project_mcp.then_some(Dialect::Codex),
                local: false,
            });
        }
        if !load_mcp {
            for source in &mut sources {
                source.mcp = None;
                source.local = false;
            }
        }
        let mut snapshot = Self::default();
        let mut selected = BTreeMap::new();
        #[cfg(feature = "hooks")]
        let mut hook_documents = Vec::new();
        #[cfg(feature = "hooks")]
        let mut disable_hooks: Option<(usize, Value)> = None;
        let mut unique = BTreeMap::<PathBuf, ReadPolicy>::new();
        for (order, source) in sources
            .into_iter()
            .filter(|source| source.hooks || source.mcp.is_some())
            .enumerate()
        {
            let toml = source
                .path
                .extension()
                .is_some_and(|extension| extension == "toml");
            let path = match std::fs::canonicalize(&source.path) {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(Error::process_io(error)),
            };
            match unique.entry(path) {
                Entry::Vacant(entry) => {
                    entry.insert(ReadPolicy {
                        toml,
                        hook_order: source.hooks.then_some(order),
                        hook_settings_order: order,
                        mcp: source.mcp,
                        mcp_order: order,
                        local: source.local,
                        order,
                    });
                }
                Entry::Occupied(mut entry) => {
                    let prior = entry.get_mut();
                    if prior.toml != toml
                        || prior.mcp.is_some() && source.mcp.is_some() && prior.mcp != source.mcp
                    {
                        return Err(Error::Invalid(
                            "one settings file declares conflicting formats".into(),
                        ));
                    }
                    if source.hooks {
                        prior.hook_order.get_or_insert(order);
                        prior.hook_settings_order = order;
                    }
                    prior.mcp = prior.mcp.or(source.mcp);
                    if source.mcp.is_some() {
                        prior.mcp_order = order;
                    }
                    prior.local |= source.local;
                }
            }
        }
        let mut unique: Vec<_> = unique.into_iter().collect();
        unique.sort_unstable_by_key(|(_, policy)| policy.order);
        for (path, source) in unique {
            let mut bytes = Vec::new();
            std::fs::File::open(&path)
                .map_err(Error::process_io)?
                .take(config.maximum_file_bytes + 1)
                .read_to_end(&mut bytes)
                .map_err(Error::process_io)?;
            if bytes.len() as u64 > config.maximum_file_bytes {
                return Err(Error::Invalid(
                    "settings file exceeds its read bound".into(),
                ));
            }
            let mut document: BTreeMap<String, Value> = parse(&bytes, source.toml)?;
            #[cfg(feature = "hooks")]
            if let Some(order) = source.hook_order {
                if let Some(disabled) = document.remove("disableAllHooks")
                    && disable_hooks
                        .as_ref()
                        .is_none_or(|(prior, _)| *prior < source.hook_settings_order)
                {
                    disable_hooks = Some((source.hook_settings_order, disabled));
                }
                if let Some(hooks) = document.remove("hooks") {
                    hook_documents.push((order, hooks));
                }
            }
            if let Some(dialect) = source.mcp {
                let key = match dialect {
                    Dialect::Claude => "mcpServers",
                    Dialect::Codex => "mcp_servers",
                };
                if let Some(servers) = document.remove(key) {
                    select(&mut selected, servers, dialect, source.mcp_order)?;
                }
                if source.local
                    && let Some(projects) = document.remove("projects")
                    && let Some((_, project)) = object(projects)?.into_iter().find(|(path, _)| {
                        workspace::matches(path, configured_workspace, &workspace)
                    })
                    && let Some(servers) = object(project)?.remove("mcpServers")
                {
                    select(&mut selected, servers, Dialect::Claude, usize::MAX)?;
                }
            }
        }
        for (name, (_, dialect, imported)) in selected {
            if imported.get("enabled") == Some(&Value::Bool(false)) {
                continue;
            }
            let imported: ImportedServer = serde_json::from_value(imported)
                .map_err(|error| Error::Invalid(format!("MCP server {name}: {error}")))?;
            snapshot.mcp.insert(name, imported, dialect)?;
        }
        #[cfg(feature = "hooks")]
        {
            let disabled = disable_hooks
                .map(|(_, value)| serde_json::from_value::<bool>(value))
                .transpose()
                .map_err(|_| Error::Invalid("disableAllHooks must be a boolean".into()))?
                .unwrap_or(false);
            if !disabled {
                hook_documents.sort_unstable_by_key(|(order, _)| *order);
                for (_, hooks) in hook_documents {
                    snapshot
                        .hooks
                        .add_document(crate::hooks::imports::Document {
                            hooks: serde_json::from_value(hooks).map_err(|error| {
                                Error::Invalid(format!("settings hooks: {error}"))
                            })?,
                        })?;
                }
            }
        }
        Ok(snapshot)
    }
}

fn select(
    selected: &mut BTreeMap<String, (usize, Dialect, Value)>,
    declarations: Value,
    dialect: Dialect,
    order: usize,
) -> Result<()> {
    for (name, declaration) in object(declarations)? {
        match selected.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert((order, dialect, declaration));
            }
            Entry::Occupied(mut entry) if entry.get().0 <= order => {
                entry.insert((order, dialect, declaration));
            }
            Entry::Occupied(_) => {}
        }
    }
    Ok(())
}

fn object(value: Value) -> Result<serde_json::Map<String, Value>> {
    match value {
        Value::Object(fields) => Ok(fields),
        _ => Err(Error::Invalid(
            "selected settings section must be an object".into(),
        )),
    }
}

fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8], toml: bool) -> Result<T> {
    if toml {
        toml::from_str(
            std::str::from_utf8(bytes)
                .map_err(|_| Error::Invalid("settings must be UTF-8".into()))?,
        )
        .map_err(|error| Error::Invalid(format!("settings: {error}")))
    } else {
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(format!("settings: {error}")))
    }
}

/// The process's CODEX_HOME. Unit tests never consult it: their settings resolve under the home they inject, so no
/// test can reach a real Codex directory or its credentials.
#[cfg(not(test))]
fn configured_codex_home() -> Option<std::ffi::OsString> {
    crate::native_environment::variable("CODEX_HOME")
}

#[cfg(test)]
fn configured_codex_home() -> Option<std::ffi::OsString> {
    None
}

fn codex_directory(home: &Path, configured: Option<&std::ffi::OsStr>) -> PathBuf {
    configured
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"))
}

#[cfg(test)]
mod tests;
