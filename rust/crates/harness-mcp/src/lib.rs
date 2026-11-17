//! MCP clients over the ordinary tool, execution and interaction owners.
pub mod callbacks;
mod catalog;
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_harness_native_settings::mcp_config as config;
#[cfg(not(target_arch = "wasm32"))]
mod deadline;
#[cfg(not(target_arch = "wasm32"))]
pub use config::{McpConfig, McpServer, McpTransport};
#[cfg(not(target_arch = "wasm32"))]
mod install;
mod paging;
#[cfg(not(target_arch = "wasm32"))]
pub use install::{McpClientLimits, McpConfigWatchExt, McpInstallation, McpServerFailure};
pub use paging::{McpDiscoveryLimits, tool_pages};
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod oauth;
mod protocol;
mod remote;
mod rpc;
pub mod services;
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod settings_validation;
pub mod stdio;
pub use catalog::{McpCatalog, McpDiscoveryPolicy, McpSchemaExposure};
pub use protocol::{McpToolDefinition, McpToolTransport, PROTOCOL_VERSION};
#[cfg(test)]
mod tests;

use acyclic_harness_core::{Error, Result, wire::ErrorCode};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sync {
    pub(crate) use acyclic_harness_core::sync::{Arc, atomic};
}

fn error(work: acyclic_fs::NativeWork<'_>, code: ErrorCode, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    acyclic_harness_core::Error::from_code(code, reason)
}
fn charge(work: acyclic_fs::NativeWork<'_>, items: u64, read: u64, copied: u64) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.charge(acyclic_fs_work::performance::WorkCounters {
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
    operation: acyclic_harness_core::OperationId,
    context: Option<&acyclic_harness_core::runtime::ToolContext>,
) -> Result<()> {
    if context.is_some_and(|context| context.operation_id() != operation) {
        return Err(Error::Unauthorized(
            "MCP task context operation mismatch".into(),
        ));
    }
    Ok(())
}

/// Decodes a pinned complete MCP process receipt from a native request.
pub trait NativeProcessRequestExt {
    /// Decodes the receipt with no process or network I/O.
    fn mcp_response(
        &self,
        result: &acyclic_harness_core::filesystem::NativeProcessResult,
    ) -> acyclic_harness_core::Result<serde_json::Value>;
}

impl NativeProcessRequestExt for acyclic_harness_core::filesystem::NativeProcessRequest {
    fn mcp_response(
        &self,
        result: &acyclic_harness_core::filesystem::NativeProcessResult,
    ) -> acyclic_harness_core::Result<serde_json::Value> {
        let exchange = self.mcp_stdio.as_ref().ok_or_else(|| {
            acyclic_harness_core::Error::Unsupported("native request has no MCP exchange".into())
        })?;
        if result.stop.is_some() {
            return Err(acyclic_harness_core::Error::Indeterminate(
                exchange.operation,
            ));
        }
        stdio::McpStdioRequestExt::decode_response(exchange, &result.stdout)
    }
}
