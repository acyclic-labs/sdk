use crate::{OperationId, Result, runtime::ToolContext};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Initialized-session protocol revision.
pub const PROTOCOL_VERSION: &str = "2025-11-25";

/// One bounded discovery page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolsPage {
    /// Remote tool contracts.
    pub tools: Vec<McpToolDefinition>,
    /// Cursor for the next admitted discovery request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Remote tool contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolDefinition {
    /// Server-local tool name.
    pub name: String,
    /// Human-facing title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Server-provided description.
    #[serde(default)]
    pub description: String,
    /// Argument schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub input_schema: Value,
    /// Structured output schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonSchema"))]
    pub output_schema: Option<Value>,
}

/// Pinned transport for admitted operations and optional task authority.
pub trait McpToolTransport: acyclic_stream::ProviderPlatform {
    /// Installed callback policy.
    fn callback_policy(&self) -> Option<&super::callbacks::McpCallbackPolicy> {
        None
    }

    /// Calls one admitted operation with its task context when available.
    fn call<'a>(
        &'a self,
        operation: OperationId,
        name: &'a str,
        arguments: Value,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Value>>;

    /// Queries the exact prior operation without redispatching it.
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<Value>>>;
}
