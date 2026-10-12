//! MCP protocol bindings and atomic catalog installation.

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

pub use catalog::{McpCatalog, McpCatalogInstallation, McpDiscoveryPolicy, McpSchemaExposure};
pub use protocol::{
    McpInitializeResult, McpToolDefinition, McpToolResult, McpToolTransport, McpToolsPage,
    PROTOCOL_VERSION,
};

#[cfg(test)]
mod tests;
