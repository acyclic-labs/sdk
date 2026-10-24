//! Opt-in server requests using the ordinary execution and interaction owners.
#![allow(deprecated, reason = "Pinned MCP 2025-11-25 supports sampling.")]
use crate::{
    Error, OperationId, Result,
    context::{Context, ContextPlacement, SourceStage},
    executor::{ExecutionJournal, StockExecutor, TurnInput},
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelRole,
        NativeMediaIntent, NativeMediaPolicy,
    },
    runtime::ToolContext,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Immutable opt-in policy; bind this with the server/catalog revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpCallbackPolicy {
    /// Consumer namespace, not remote implementation name.
    pub server: String,
    /// Exact host-selected server revision.
    pub revision: String,
    /// Enable model requests under the task's ordinary model grant.
    pub sampling: bool,
    /// Enable forms under the task's ordinary interaction grant.
    pub elicitation: bool,
    /// Finite callback count per exchange, including replayed requests.
    pub maximum_requests: u32,
}
impl McpCallbackPolicy {
    /// Validates the immutable namespace, revision and finite opt-in allowance.
    pub fn validate(&self) -> Result<()> {
        crate::registry::validate_component_label(&self.server, "MCP callback server")?;
        crate::registry::validate_component_label(&self.revision, "MCP callback revision")?;
        if self.server.contains('.') {
            return Err(Error::Invalid(
                "MCP callback server namespace cannot contain a dot".into(),
            ));
        }
        if self.maximum_requests == 0 || (!self.sampling && !self.elicitation) {
            return Err(Error::Invalid("invalid MCP callback policy".into()));
        }
        Ok(())
    }
    /// Wire capabilities advertise only explicitly installed admission routes.
    pub fn capabilities(&self) -> Result<Value> {
        self.validate()?;
        let mut capabilities = serde_json::Map::new();
        if self.sampling {
            capabilities.insert("sampling".into(), json!({}));
        }
        if self.elicitation {
            capabilities.insert("elicitation".into(), json!({"form":{}}));
        }
        Ok(Value::Object(capabilities))
    }
}

/// Exact callback request retained in the execution journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpCallbackRequest {
    /// Operation enclosing the server request.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub parent: OperationId,
    /// Pinned callback admission policy.
    pub policy: McpCallbackPolicy,
    /// Complete JSON-RPC request, including opaque server ID and exact parameters.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub message: Value,
}
impl McpCallbackRequest {
    /// Validates the SDK request and derives its payload-independent journal identity.
    pub fn validate(&self) -> Result<(OperationId, rmcp::model::ServerRequest)> {
        self.policy.validate()?;
        if self.message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || self.message.get("result").is_some()
            || self.message.get("error").is_some()
        {
            return Err(Error::Invalid("invalid MCP callback envelope".into()));
        }
        let id = self
            .message
            .get("id")
            .filter(|id| id.is_string() || id.is_i64())
            .ok_or_else(|| Error::Invalid("invalid MCP callback ID".into()))?;
        let params = self
            .message
            .get("params")
            .ok_or_else(|| Error::Invalid("MCP callback parameters absent".into()))?;
        let method = self.message.get("method").and_then(Value::as_str);
        match method {
            Some("sampling/createMessage") if self.policy.sampling => {}
            Some("elicitation/create") if self.policy.elicitation => {
                if let Some(schema) = params.get("requestedSchema") {
                    crate::executor::compile_json_schema(schema, "MCP elicitation", &mut None)?;
                }
            }
            _ => {
                return Err(Error::Unsupported(
                    "MCP callback is not enabled by pinned policy".into(),
                ));
            }
        }
        let decoded: rmcp::model::JsonRpcRequest<rmcp::model::ServerRequest> =
            rmcp::model::JsonRpcRequest::deserialize(&self.message)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        if !matches!(
            (method, &decoded.request),
            (
                Some("sampling/createMessage"),
                rmcp::model::ServerRequest::CreateMessageRequest(_)
            ) | (
                Some("elicitation/create"),
                rmcp::model::ServerRequest::ElicitRequest(_)
            )
        ) {
            return Err(Error::Invalid("invalid MCP callback parameters".into()));
        }
        let digest = crate::executor::json::canonical_json_digest(
            &(
                "harness/mcp-callback/v1",
                self.parent,
                &self.policy.server,
                &self.policy.revision,
                id,
            ),
            u64::MAX,
            &mut None,
        )?;
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        Ok((OperationId::from_bytes(bytes), decoded.request))
    }
}

mod runtime;
pub use runtime::McpRuntimeCallbacks;
mod sampling;
use sampling::sampling_messages;
