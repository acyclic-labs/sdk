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

mod cancellation;
mod native;
use cancellation::CancelOnDrop;

/// Optional host-selected discovery and exchange allowances. No default ceiling.
#[derive(Clone, Copy, Default)]
pub struct McpClientLimits {
    /// Maximum concurrently initialized servers.
    pub servers: Option<u32>,
    /// Maximum advertised tools from each server, before filtering.
    pub tools_per_server: Option<u32>,
    /// Maximum pages from one tools/list traversal.
    pub pages_per_server: Option<u32>,
    /// Maximum aggregate catalog bytes from each server.
    pub catalog_bytes: Option<u32>,
    /// Maximum request or response bytes for an installed tool call.
    pub tool_bytes: Option<u32>,
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
    binding: [u8; 32],
}

impl McpConfig {
    /// Connects configured servers and adds their tools without replacing caller tools.
    /// `workspace` must be the absolute project root. Required failures reject startup.
    /// Credential references resolve only through the host's `secrets`.
    pub async fn install(
        self,
        tools: ToolRegistry,
        workspace: &Path,
        limits: McpClientLimits,
        secrets: Arc<dyn crate::secrets::SecretSource>,
    ) -> Result<McpInstallation> {
        if !workspace.is_absolute() {
            return Err(Error::Invalid("MCP workspace must be absolute".into()));
        }
        let workspace = Arc::new(workspace.to_owned());
        self.install_with(tools, limits, move |server| {
            let workspace = Arc::clone(&workspace);
            let secrets = Arc::clone(&secrets);
            Box::pin(async move { native::connect(server, &workspace, limits, &*secrets).await })
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
        if limits.servers == Some(0)
            || limits.tools_per_server == Some(0)
            || limits.pages_per_server == Some(0)
            || limits.catalog_bytes == Some(0)
            || limits.tool_bytes == Some(0)
            || limits
                .servers
                .is_some_and(|maximum| self.servers.len() as u64 > u64::from(maximum))
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
                let required = server.required;
                let result = async {
                    let deadline = super::deadline::after(
                        server.startup_timeout_ms.map(Duration::from_millis),
                    )?;
                    let connected = super::deadline::until(deadline, connect(&server))
                        .await
                        .map_err(|_| startup_timeout())??;
                    let name = name.clone();
                    register_before(deadline, move |check| {
                        register(name, &server, connected, limits, check)
                    })
                    .await
                }
                .await;
                (name, required, result)
            }
        }))
        .await;
        for (server, required, result) in results {
            match result {
                Ok((additional, grants)) => {
                    tools.extend(additional)?;
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

fn startup_timeout() -> Error {
    Error::Process(crate::ProcessError::Timeout {
        exit_code: None,
        message: "MCP startup deadline elapsed".into(),
    })
}

async fn register_before<T: Send + 'static>(
    deadline: impl Into<Option<tokio::time::Instant>>,
    register: impl FnOnce(&dyn Fn() -> Result<()>) -> Result<T> + Send + 'static,
) -> Result<T> {
    let deadline = deadline.into();
    use super::sync::{Arc, atomic::AtomicBool};
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
    let flag = Arc::clone(&guard.0);
    let mut worker =
        acyclic_native_runtime::run_blocking_io(move || cancellation::run(flag, register), None)
            .map_err(Error::process_io)?;
    match super::deadline::until(deadline, &mut worker).await {
        Ok(result) if deadline.is_none_or(|deadline| tokio::time::Instant::now() < deadline) => {
            result.map_err(Error::process_io)?
        }
        result => {
            guard.cancel();
            // Cancellation is cooperative at schema boundaries. Never return a timeout
            // while its registration still owns a connection or a worker job.
            if result.is_err() {
                let completion = worker.await.map_err(Error::process_io)?;
                drop(completion);
            } else {
                drop(result);
            }
            Err(startup_timeout())
        }
    }
}

fn register(
    name: String,
    server: &McpServer,
    mut connected: Connected,
    limits: McpClientLimits,
    check: &(impl Fn() -> Result<()> + ?Sized),
) -> Result<(ToolRegistry, Capabilities)> {
    check()?;
    if limits
        .tools_per_server
        .is_some_and(|maximum| connected.tools.len() as u64 > u64::from(maximum))
    {
        return Err(Error::Invalid("MCP discovery exceeds tool bound".into()));
    }
    let mut catalog = McpCatalog {
        server: name,
        revision: String::new(),
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
    catalog.tools.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    catalog.revision = blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
        &(
            "harness/mcp-install/v1",
            connected.binding,
            server.tool_timeout_ms,
            limits.tool_bytes,
            &catalog,
        ),
        None,
        &mut None,
    )?)
    .to_hex()
    .to_string();
    let mut tools = ToolRegistry::default();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    catalog.install_checked(
        &mut tools,
        None,
        &connected.transport,
        &projection,
        (limits.tools_per_server, limits.catalog_bytes),
        &check,
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
#[cfg(feature = "mcp-stdio")]
mod path_tests;
#[cfg(test)]
mod tests;
