use crate::{Error, OperationId, Result, runtime::ToolContext};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Initialized-session protocol revision.
pub const PROTOCOL_VERSION: &str = "2025-11-25";

/// Negotiated server initialization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    /// Negotiated protocol revision.
    pub protocol_version: String,
    /// Advertised server capabilities.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub capabilities: Value,
    /// Server implementation identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub server_info: Value,
    /// External server instructions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl McpInitializeResult {
    /// Rejects unsupported initialization contracts.
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION
            || !self.capabilities.is_object()
            || self
                .server_info
                .get("name")
                .and_then(Value::as_str)
                .is_none()
            || self
                .server_info
                .get("version")
                .and_then(Value::as_str)
                .is_none()
        {
            return Err(Error::Unsupported(
                "MCP initialization contract unsupported".into(),
            ));
        }
        Ok(())
    }
}

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

/// Ordered protocol content and structured output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "camelCase")]
pub struct McpToolResult {
    /// Ordered content blocks.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue[]"))]
    pub content: Vec<Value>,
    /// Structured output, preserving explicit null.
    #[serde(
        default,
        deserialize_with = "present_json",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub structured_content: Option<Value>,
    /// Recorded tool failure.
    #[serde(default)]
    pub is_error: bool,
    /// Protocol metadata.
    #[serde(default, rename = "_meta", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub meta: Option<Value>,
}

fn present_json<'de, D: serde::Deserializer<'de>>(
    value: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(value).map(Some)
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
    ) -> BoxProviderFuture<'a, Result<McpToolResult>>;

    /// Queries the exact prior operation without redispatching it.
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>>;
}
