//! Opt-in server requests using the ordinary execution and interaction owners.
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
use acyclic_stream::BoxProviderFuture;
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
    /// Validates a pinned request and returns its payload-independent journal identity.
    pub fn validate(&self) -> Result<OperationId> {
        self.policy.validate()?;
        let _: rmcp::model::JsonRpcRequest<Value> =
            rmcp::model::JsonRpcRequest::deserialize(&self.message)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        if self.message.get("result").is_some() || self.message.get("error").is_some() {
            return Err(Error::Invalid(
                "MCP callback mixes request and response envelopes".into(),
            ));
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
        match self.message.get("method").and_then(Value::as_str) {
            Some("sampling/createMessage") if self.policy.sampling => validate_sampling(params),
            Some("elicitation/create") if self.policy.elicitation => {
                let _: rmcp::model::ElicitRequestParams =
                    rmcp::model::ElicitRequestParams::deserialize(params)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                if let Some(schema) = params.get("requestedSchema") {
                    crate::contract::compile_json_schema(schema, "MCP elicitation", &mut None)?;
                }
                Ok(())
            }
            _ => Err(Error::Unsupported(
                "MCP callback is not enabled by pinned policy".into(),
            )),
        }?;
        let digest = crate::contract::canonical_json_digest(
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
        Ok(OperationId::from_bytes(bytes))
    }

    /// Validates the reply identity and result schema before any response POST.
    pub fn validate_reply(&self, reply: &Value) -> Result<()> {
        if reply.get("id") != self.message.get("id")
            || reply.get("method").is_some()
            || reply.get("error").is_some()
        {
            return Err(Error::Invalid(
                "MCP callback reply identity or envelope differs".into(),
            ));
        }
        let response: rmcp::model::JsonRpcResponse<Value> =
            rmcp::model::JsonRpcResponse::deserialize(reply)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        match self.message.get("method").and_then(Value::as_str) {
            Some("sampling/createMessage") => validate_sampling_result(response.result),
            Some("elicitation/create") => {
                let _: rmcp::model::ElicitResult =
                    rmcp::model::ElicitResult::deserialize(&response.result)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                if response.result.get("action").and_then(Value::as_str) == Some("accept")
                    && let Some(schema) = self.message.pointer("/params/requestedSchema")
                {
                    let content = response
                        .result
                        .get("content")
                        .ok_or_else(|| Error::Invalid("accepted MCP form has no content".into()))?;
                    crate::contract::validate_json_schema_value(
                        schema,
                        content,
                        "MCP elicitation answer",
                        &mut None,
                    )?;
                }
                Ok(())
            }
            _ => Err(Error::Unsupported(
                "MCP callback reply method is unsupported".into(),
            )),
        }
    }
}

/// Explicit host selection for ordinary sampling input and media admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSamplingPolicy {
    /// Exact host input used only when the remote transcript does not end in a user message.
    pub continuation: ModelContent,
    /// Opt-in image processing contract; absent means images are unsupported.
    pub image: Option<NativeMediaPolicy>,
    /// Opt-in audio duration/work/byte contract; absent means audio is unsupported.
    pub audio: Option<NativeMediaPolicy>,
}

mod runtime;
pub use runtime::McpRuntimeCallbacks;
mod sampling;
use sampling::{sampling_messages, validate_sampling, validate_sampling_result};
