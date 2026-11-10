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
mod live;
#[cfg(test)]
mod live_tests;
mod native;
mod watched;
use cancellation::CancelOnDrop;

/// Protective per-message bounds, independent of catalog size and task allowances.
#[derive(Clone, Copy)]
pub struct McpClientLimits {
    /// Maximum bytes in one discovery request or response, never the whole catalog.
    pub frame_bytes: u32,
    /// Maximum request or response bytes for an installed tool call.
    pub tool_bytes: u32,
}

impl Default for McpClientLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 1_048_576,
            tool_bytes: 1_048_576,
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
    binding: [u8; 32],
    generation: u64,
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
        if limits.frame_bytes == 0 || limits.tool_bytes == 0 {
            return Err(Error::Invalid("MCP message bounds must be positive".into()));
        }
        let mut diagnostics = Vec::new();
        let mut capabilities = Capabilities::default();
        let results = join_all(self.servers.into_iter().map(|(name, server)| {
            let connect = &connect;
            async move {
                let deadline = tokio::time::Instant::now()
                    + Duration::from_millis(u64::from(server.startup_timeout_ms));
                let result = tokio::time::timeout_at(deadline, connect(&server))
                    .await
                    .map_err(|_| startup_timeout())
                    .and_then(|result| result);
                let required = server.required;
                let result = match result {
                    Ok(connected) => {
                        let name = name.clone();
                        register_before(deadline, move |check| {
                            check()?;
                            live::LiveCatalog::new(name, server, connected, limits)
                        })
                        .await
                    }
                    Err(error) => Err(error),
                };
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
    deadline: tokio::time::Instant,
    register: impl FnOnce(&dyn Fn() -> Result<()>) -> Result<T> + Send + 'static,
) -> Result<T> {
    use super::sync::{Arc, atomic::AtomicBool};
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
    let flag = Arc::clone(&guard.0);
    let mut worker =
        acyclic_native_runtime::run_blocking_io(move || cancellation::run(flag, register), None)
            .map_err(Error::process_io)?;
    match tokio::time::timeout_at(deadline, &mut worker).await {
        Ok(result) if tokio::time::Instant::now() < deadline => {
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
    let revision = blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
        &(
            "harness/mcp-install/v1",
            connected.binding,
            server.tool_timeout_ms,
            limits.tool_bytes,
            &catalog,
        ),
        u64::MAX,
        &mut None,
    )?)
    .to_hex()
    .to_string();
    catalog.revision = format!(
        "{}{}",
        recovery_binding(connected.binding, server, limits)?,
        revision
    );
    let mut tools = ToolRegistry::default();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    catalog.install_checked(
        &mut tools,
        None,
        &connected.transport,
        &projection,
        None,
        &check,
    )?;
    let mut capabilities = Capabilities::new(
        catalog
            .tools
            .iter()
            .map(|tool| format!("tool:call:mcp.{}.{}", catalog.server, tool.name)),
    );
    capabilities
        .0
        .insert(format!("mcp:call:{}", catalog.server));
    Ok((tools, capabilities))
}

fn recovery_binding(
    binding: [u8; 32],
    server: &McpServer,
    limits: McpClientLimits,
) -> Result<String> {
    Ok(
        blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
            &(
                "harness/mcp-recovery-binding/v1",
                binding,
                server.tool_timeout_ms,
                limits.tool_bytes,
                &server.enabled_tools,
                &server.disabled_tools,
            ),
            u64::MAX,
            &mut None,
        )?)
        .to_hex()
        .to_string(),
    )
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod path_tests;
#[cfg(test)]
mod tests;
