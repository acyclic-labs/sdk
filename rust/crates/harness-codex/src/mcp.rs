//! The MCP endpoint that exposes the consumer's tool registry to Codex (task A3).
//!
//! Contract, fixed by `tests/mcp.rs`:
//! - Streamable-HTTP MCP at `/mcp` on `127.0.0.1:0`, protocol `2025-06-18`,
//!   handling `initialize`, `notifications/initialized`, `tools/list`, `tools/call`.
//! - Requests without the per-turn bearer token get a 401.
//! - `tools/list` holds exactly the registry tools the scope grants
//!   (`tool:call:<name>`), with dots mapped to underscores (`acyclic.web` ↔ `acyclic_web`).
//! - `tools/call` runs the registry's executor after the grant, `authorize` and
//!   policy checks the stock loop applies. Tool errors and refusals come back as
//!   `isError` results, never as transport failures.

use acyclic_harness::{
    Error, Result,
    runtime::{RuntimeScope, ToolPolicy},
    tool::ToolRegistry,
};
use std::sync::Arc;

/// Maps a registry tool name to the MCP name Codex sees.
#[must_use]
pub fn mcp_name(tool: &str) -> String {
    tool.replace('.', "_")
}

/// A running MCP endpoint for one turn.
#[derive(Debug)]
pub struct McpEndpoint {
    url: String,
    token: String,
}

impl McpEndpoint {
    /// Starts the endpoint over `tools`, limited to what `scope` grants.
    ///
    /// # Errors
    /// When the listener cannot bind or a tool name cannot be mapped.
    pub async fn start(
        tools: ToolRegistry,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        let _ = (tools, scope, policy);
        Err(Error::Unsupported(
            "codex MCP endpoint is not built yet (A3)".into(),
        ))
    }

    /// The URL Codex is configured with.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The bearer token Codex must present.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn tool_names_map_dots_to_underscores() {
        assert_eq!(super::mcp_name("acyclic.web"), "acyclic_web");
        assert_eq!(super::mcp_name("acyclic.skills"), "acyclic_skills");
    }
}
