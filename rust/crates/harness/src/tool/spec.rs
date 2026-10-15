//! Data-owned tool declarations, bound to typed implementations by the host.

use super::{Tool, ToolDefinition, ToolExecutor, ToolProjection};
use std::sync::Arc;

/// One catalog entry. Schemas and model descriptions have a single owner.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    /// Immutable model and result contract.
    pub definition: ToolDefinition,
    /// Typed implementation selected by the composition root.
    pub executor: BuiltinTool,
    /// Effects used by approval policies and tool discovery.
    pub effects: Vec<ToolEffect>,
}

/// First-party implementation identities.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinTool {
    /// Provider-neutral web search.
    WebSearch,
}

/// Externally observable effects of a tool invocation.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// Sends the query to a configured network provider.
    Network,
}

impl ToolSpec {
    /// Binds a validated declaration without duplicating its schemas.
    pub fn bind(
        self,
        executor: Arc<dyn ToolExecutor>,
        projection: Arc<dyn ToolProjection>,
    ) -> crate::Result<Tool> {
        self.definition.validate(&mut None)?;
        Ok(Tool {
            hook_names: &[],
            definition: self.definition,
            executor,
            projection,
        })
    }
}
