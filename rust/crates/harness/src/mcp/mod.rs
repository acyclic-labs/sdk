//! MCP clients over the ordinary tool, execution and interaction owners.
pub mod callbacks;
mod catalog;
pub mod http;
#[cfg(all(feature = "mcp-oauth", not(target_arch = "wasm32")))]
pub mod oauth;
mod protocol;
mod remote;
mod rpc;
pub mod services;
pub mod stdio;
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
