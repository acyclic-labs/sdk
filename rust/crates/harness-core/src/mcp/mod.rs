//! MCP clients over the ordinary tool, execution and interaction owners.
pub mod callbacks;
mod catalog;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod config;
#[cfg(not(target_arch = "wasm32"))]
pub use config::{McpConfig, McpServer, McpTransport};
#[cfg(not(target_arch = "wasm32"))]
mod install;
mod layouts;
mod paging;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
pub use install::{McpClientLimits, McpInstallation, McpServerFailure};
pub use paging::tool_pages;
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod oauth;
mod protocol;
mod remote;
mod rpc;
pub mod services;
pub mod stdio;
#[cfg(not(target_arch = "wasm32"))]
use crate::sync;
pub use catalog::{McpCatalog, McpDiscoveryPolicy, McpSchemaExposure};
pub use protocol::{McpToolDefinition, McpToolTransport, PROTOCOL_VERSION};
#[cfg(test)]
mod tests;

use crate::{Error, Result, wire::ErrorCode};

fn error(work: acyclic_fs::NativeWork<'_>, code: ErrorCode, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    crate::Error::from_code(code, reason)
}
fn charge(work: acyclic_fs::NativeWork<'_>, items: u64, read: u64, copied: u64) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: items,
            source_bytes_read: read,
            bytes_copied: copied,
            ..Default::default()
        })?;
    }
    let _ = (work, items, read, copied);
    Ok(())
}

fn validate_operation_context(
    operation: crate::OperationId,
    context: Option<&crate::runtime::ToolContext>,
) -> Result<()> {
    if context.is_some_and(|context| context.operation_id() != operation) {
        return Err(Error::Unauthorized(
            "MCP task context operation mismatch".into(),
        ));
    }
    Ok(())
}
