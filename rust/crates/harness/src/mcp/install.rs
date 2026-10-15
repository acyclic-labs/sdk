//! Native configuration to ordinary, additive registered tools.

use super::{
    McpCatalog, McpConfig, McpDiscoveryPolicy, McpSchemaExposure, McpServer, McpToolDefinition,
    McpToolTransport,
};
use crate::{
    Capabilities, Error, Result,
    tool::{JsonToolProjection, ToolProjection, ToolRegistry},
};
use acyclic_stream::BoxProviderFuture;
use futures::future::join_all;
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};

mod native;

/// Positive startup discovery bounds, independent of task execution allowances.
#[derive(Clone, Copy)]
pub struct McpClientLimits {
    /// Maximum concurrently initialized servers.
    pub servers: u32,
    /// Maximum advertised tools from each server, before filtering.
    pub tools_per_server: u32,
    /// Maximum pages from one tools/list traversal.
    pub pages_per_server: u32,
    /// Maximum aggregate catalog bytes from each server.
    pub catalog_bytes: u32,
}

impl Default for McpClientLimits {
    fn default() -> Self {
        Self {
            servers: 32,
            tools_per_server: 128,
            pages_per_server: 16,
            catalog_bytes: 1_048_576,
        }
    }
}

/// A server that could not be installed; no connection configuration is retained.
pub struct McpServerFailure {
    /// Configured server namespace.
    pub server: String,
    /// Startup failure, without resolved connection credentials.
    pub message: String,
}

/// Host-owned tool assembly. The registry retains each installed connection.
pub struct McpInstallation {
    /// Existing tools plus successfully initialized MCP tools.
    pub tools: ToolRegistry,
    /// Grants needed to call the newly installed tools, before caller attenuation.
    pub capabilities: Capabilities,
    /// Optional server failures, in configuration order.
    pub diagnostics: Vec<McpServerFailure>,
}

struct Connected {
    transport: Arc<dyn McpToolTransport>,
    tools: Vec<McpToolDefinition>,
}

impl McpConfig {
    /// Connects configured servers and adds their tools without replacing caller tools.
    /// `workspace` must be the absolute project root. Required failures reject startup.
    pub async fn install(
        self,
        tools: ToolRegistry,
        workspace: &Path,
        limits: McpClientLimits,
    ) -> Result<McpInstallation> {
        if !workspace.is_absolute() {
            return Err(Error::Invalid("MCP workspace must be absolute".into()));
        }
        let workspace = Arc::new(workspace.to_owned());
        self.install_with(tools, limits, move |server| {
            let workspace = Arc::clone(&workspace);
            Box::pin(async move { native::connect(server, &workspace, limits).await })
        })
        .await
    }

    async fn install_with<F>(
        self,
        mut tools: ToolRegistry,
        limits: McpClientLimits,
        connect: F,
    ) -> Result<McpInstallation>
    where
        F: for<'a> Fn(&'a McpServer) -> BoxProviderFuture<'a, Result<Connected>>,
    {
        if limits.servers == 0
            || limits.tools_per_server == 0
            || limits.pages_per_server == 0
            || limits.catalog_bytes == 0
            || self.servers.len() as u64 > u64::from(limits.servers)
        {
            return Err(Error::Invalid(
                "MCP startup bounds must be positive and sufficient".into(),
            ));
        }
        let mut diagnostics = Vec::new();
        let mut capabilities = Capabilities::default();
        let results = join_all(self.servers.into_iter().map(|(name, server)| {
            let connect = &connect;
            async move {
                let result = tokio::time::timeout(
                    Duration::from_millis(u64::from(server.startup_timeout_ms)),
                    connect(&server),
                )
                .await
                .map_err(|_| Error::Process {
                    kind: "timeout",
                    message: "MCP startup deadline elapsed".into(),
                })
                .and_then(|result| result)
                .and_then(|connected| register(name.clone(), &server, connected, limits));
                (name, server.required, result)
            }
        }))
        .await;
        for (server, required, result) in results {
            match result {
                Ok((additional, grants)) => {
                    tools = tools.extend(additional)?;
                    capabilities.0.extend(grants.0);
                }
                Err(error) if required => return Err(error),
                Err(error) => diagnostics.push(McpServerFailure {
                    server,
                    message: error.to_string(),
                }),
            }
        }
        Ok(McpInstallation {
            tools,
            capabilities,
            diagnostics,
        })
    }
}

fn register(
    name: String,
    server: &McpServer,
    mut connected: Connected,
    limits: McpClientLimits,
) -> Result<(ToolRegistry, Capabilities)> {
    if connected.tools.len() as u64 > u64::from(limits.tools_per_server) {
        return Err(Error::Invalid("MCP discovery exceeds tool bound".into()));
    }
    let mut catalog = McpCatalog {
        server: name,
        revision: uuid::Uuid::new_v4().to_string(),
        schema_exposure: McpSchemaExposure::Eager,
        discovery: McpDiscoveryPolicy::Disabled,
        projection_schema: crate::tool::json_projection_schema(json!({})),
        tools: std::mem::take(&mut connected.tools),
    };
    catalog.tools.retain(|tool| {
        server
            .enabled_tools
            .as_ref()
            .is_none_or(|names| names.contains(&tool.name))
            && !server.disabled_tools.contains(&tool.name)
    });
    let mut tools = ToolRegistry::default();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    catalog.install(
        &mut tools,
        None,
        &connected.transport,
        &projection,
        limits.tools_per_server,
        limits.catalog_bytes,
    )?;
    let mut capabilities = Capabilities::new(
        catalog
            .tools
            .iter()
            .map(|tool| format!("tool:call:mcp.{}.{}", catalog.server, tool.name)),
    );
    if !catalog.tools.is_empty() {
        capabilities
            .0
            .insert(format!("mcp:call:{}", catalog.server));
    }
    Ok((tools, capabilities))
}

#[cfg(test)]
mod tests;
