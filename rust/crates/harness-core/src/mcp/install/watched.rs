//! Trusted settings edits replace the same shared catalog source.
use super::*;
use crate::{
    input_watch::InputWatch,
    native_settings::{NativeSettings, SettingsConfig},
    tool::ToolSource,
};

struct WatchedConfig {
    watch: InputWatch,
    settings: Arc<SettingsConfig>,
    workspace: std::path::PathBuf,
    limits: McpClientLimits,
    secrets: Arc<dyn crate::secrets::SecretSource>,
    state: tokio::sync::Mutex<(u64, McpConfig)>,
    tools: std::sync::Mutex<ToolRegistry>,
    removed: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl McpConfig {
    /// Install a shared catalog which follows trusted config files and MCP events.
    pub async fn install_watched(
        tools: ToolRegistry,
        settings: Arc<SettingsConfig>,
        workspace: &Path,
        limits: McpClientLimits,
        secrets: Arc<dyn crate::secrets::SecretSource>,
    ) -> Result<McpInstallation> {
        let watch = InputWatch::new(settings.mcp_paths())?;
        let revision = watch.revision();
        watch.arm()?;
        let config = NativeSettings::load_mcp(settings.clone()).await?;
        let installed = config
            .clone()
            .install(ToolRegistry::default(), workspace, limits, secrets.clone())
            .await?;
        let source = Arc::new(WatchedConfig {
            watch,
            settings,
            workspace: workspace.to_owned(),
            limits,
            secrets,
            state: tokio::sync::Mutex::new((revision, config)),
            tools: std::sync::Mutex::new(installed.tools),
            removed: std::sync::Mutex::new(Default::default()),
        });
        let mut capabilities = installed.capabilities;
        capabilities.0.insert("mcp:configured".into());
        Ok(McpInstallation {
            tools: tools.with_source(source),
            capabilities,
            diagnostics: installed.diagnostics,
        })
    }
}

impl ToolSource for WatchedConfig {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            let revision = self.watch.revision();
            if state.0 != revision {
                self.watch.arm()?;
                let config = NativeSettings::load_mcp(self.settings.clone()).await?;
                if state.1 != config {
                    let unchanged = |source: &dyn ToolSource| {
                        config.servers.iter().any(|(name, server)| {
                            state.1.servers.get(name) == Some(server)
                                && source.capability() == Some(format!("mcp:call:{name}").as_str())
                        })
                    };
                    let mut next_tools = self.snapshot()?;
                    next_tools.retain_sources(unchanged);
                    let changed = McpConfig {
                        servers: config
                            .servers
                            .iter()
                            .filter(|(name, server)| state.1.servers.get(*name) != Some(*server))
                            .map(|(name, server)| (name.clone(), server.clone()))
                            .collect(),
                    };
                    let installed = changed
                        .install(
                            ToolRegistry::default(),
                            &self.workspace,
                            self.limits,
                            self.secrets.clone(),
                        )
                        .await?;
                    let failed = |source: &dyn ToolSource| {
                        installed.diagnostics.iter().any(|failure| {
                            source.capability()
                                == Some(format!("mcp:call:{}", failure.server).as_str())
                        })
                    };
                    let mut previous = self.snapshot()?;
                    previous.retain_sources(failed);
                    next_tools.extend(previous)?;
                    next_tools.extend(installed.tools)?;
                    let next = next_tools.definitions(None)?;
                    {
                        let mut tools = self
                            .tools
                            .lock()
                            .map_err(|_| Error::Storage("MCP config lock poisoned".into()))?;
                        let mut removed = self
                            .removed
                            .lock()
                            .map_err(|_| Error::Storage("MCP config lock poisoned".into()))?;
                        for tool in tools.definitions(None)? {
                            if !next.iter().any(|new| new.name == tool.name) {
                                removed.insert(tool.name);
                            }
                        }
                        removed.retain(|name| !next.iter().any(|tool| &tool.name == name));
                        *tools = next_tools;
                    }
                    let mut admitted = config;
                    for failure in &installed.diagnostics {
                        match state.1.servers.get(&failure.server) {
                            Some(server) => {
                                admitted
                                    .servers
                                    .insert(failure.server.clone(), server.clone());
                            }
                            None => {
                                admitted.servers.remove(&failure.server);
                            }
                        }
                    }
                    state.1 = admitted;
                    if !installed.diagnostics.is_empty() {
                        // Keep failed transports pending while successful and
                        // unchanged connections retain their original owners.
                        return self.snapshot()?.refresh().await;
                    }
                }
                state.0 = revision;
            }
            let tools = self.snapshot()?;
            tools.refresh().await
        })
    }
    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(self
            .tools
            .lock()
            .map_err(|_| Error::Storage("MCP config lock poisoned".into()))?
            .clone())
    }
    fn was_removed(&self, name: &str) -> Result<bool> {
        Ok(self
            .removed
            .lock()
            .map_err(|_| Error::Storage("MCP config lock poisoned".into()))?
            .contains(name)
            || self.snapshot()?.was_removed(name)?)
    }
    fn capability(&self) -> Option<&str> {
        Some("mcp:configured")
    }
}
