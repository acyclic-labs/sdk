//! MCP clients over the ordinary tool, execution and interaction owners.
pub mod callbacks;
mod catalog;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
pub(crate) mod config;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
pub use config::{McpConfig, McpServer, McpTransport};
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
mod install;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
pub use install::{McpClientLimits, McpInstallation, McpServerFailure};
pub mod http;
#[cfg(all(feature = "mcp-oauth", not(target_arch = "wasm32")))]
pub mod oauth;
mod protocol;
mod remote;
mod rpc;
pub mod services;
pub mod stdio;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
use crate::sync;
pub use catalog::{McpCatalog, McpDiscoveryPolicy, McpSchemaExposure};
pub use protocol::{McpToolDefinition, McpToolTransport, PROTOCOL_VERSION};
#[cfg(test)]
mod tests;

use crate::{Error, NativeWork, Result, wire::ErrorCode};

fn error(work: NativeWork<'_>, code: ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}
fn charge(work: NativeWork<'_>, items: u64, read: u64, copied: u64) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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
