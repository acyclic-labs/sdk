//! Opt-in server requests using the ordinary execution and interaction owners.
use crate::{Error, OperationId, Result, context::{Context, ContextPlacement, SourceStage}, executor::{ExecutionJournal, StockExecutor, TurnInput}, interaction::{Interaction, InteractionOutcome, InteractionResponse}, model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelRole, NativeMediaIntent, NativeMediaPolicy}, runtime::ToolContext};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use base64::Engine as _;

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
            return Err(Error::Invalid("MCP callback server namespace cannot contain a dot".into()));
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
        if self.sampling { capabilities.insert("sampling".into(), json!({})); }
        if self.elicitation { capabilities.insert("elicitation".into(), json!({"form":{}})); }
        Ok(Value::Object(capabilities))
    }
}

/// Exact request pin retained in the existing execution journal, never a new ledger.
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
    /// Stable identity excludes payload: changed content conflicts at journal staging.
    pub fn operation(&self) -> Result<OperationId> {
        self.policy.validate()?;
        let _: rmcp::model::JsonRpcRequest<Value> = rmcp::model::JsonRpcRequest::deserialize(&self.message)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if self.message.get("result").is_some() || self.message.get("error").is_some() {
            return Err(Error::Invalid("MCP callback mixes request and response envelopes".into()));
        }
        let id = self.message.get("id").filter(|id| id.is_string() || id.is_i64())
            .ok_or_else(|| Error::Invalid("invalid MCP callback ID".into()))?;
        let digest = crate::contract::canonical_json_digest(&("harness/mcp-callback/v1", self.parent, &self.policy.server, &self.policy.revision, id))?;
        let mut bytes = [0;16];
        for (byte, source) in bytes.iter_mut().zip(digest) { *byte = source; }
        Ok(OperationId::from_bytes(bytes))
    }

    /// Validates the exact protocol request without granting execution authority.
    pub fn validate(&self) -> Result<()> {
        self.operation()?;
        let params = self.message.get("params").ok_or_else(|| Error::Invalid("MCP callback parameters absent".into()))?;
        match self.message.get("method").and_then(Value::as_str) {
            Some("sampling/createMessage") if self.policy.sampling => validate_sampling(params),
            Some("elicitation/create") if self.policy.elicitation => {
                let _: rmcp::model::ElicitRequestParams = rmcp::model::ElicitRequestParams::deserialize(params).map_err(|error| Error::Invalid(error.to_string()))?;
                if let Some(schema) = params.get("requestedSchema") { crate::contract::compile_json_schema(schema, "MCP elicitation")?; }
                Ok(())
            }
            _ => Err(Error::Unsupported("MCP callback is not enabled by pinned policy".into())),
        }
    }

    /// Validates the reply identity and result schema before any response POST.
    pub fn validate_reply(&self, reply: &Value) -> Result<()> {
        if reply.get("id") != self.message.get("id") || reply.get("method").is_some() || reply.get("error").is_some() {
            return Err(Error::Invalid("MCP callback reply identity or envelope differs".into()));
        }
        let response: rmcp::model::JsonRpcResponse<Value> = rmcp::model::JsonRpcResponse::deserialize(reply).map_err(|error| Error::Invalid(error.to_string()))?;
        match self.message.get("method").and_then(Value::as_str) {
            Some("sampling/createMessage") => validate_sampling_result(response.result),
            Some("elicitation/create") => {
                let _: rmcp::model::ElicitResult = rmcp::model::ElicitResult::deserialize(&response.result).map_err(|error| Error::Invalid(error.to_string()))?;
                if response.result.get("action").and_then(Value::as_str) == Some("accept")
                    && let Some(schema) = self.message.pointer("/params/requestedSchema")
                {
                    let content = response.result.get("content").ok_or_else(|| Error::Invalid("accepted MCP form has no content".into()))?;
                    crate::contract::validate_json_schema_value(schema, content, "MCP elicitation answer")?;
                }
                Ok(())
            }
            _ => Err(Error::Unsupported("MCP callback reply method is unsupported".into())),
        }
    }
}

/// A host may implement this using its recorded admission owners; never invoke
/// a raw model provider or duplicate an interaction/effect journal here.
pub trait McpCallbackAdmission: acyclic_stream::ProviderPlatform {
    /// Admits/reconciles the exact request. Repeated identity must retain one result.
    fn admit<'a>(&'a self, context: ToolContext, request: &'a McpCallbackRequest) -> BoxProviderFuture<'a, Result<Value>>;
}

/// Explicit host selection for ordinary sampling input and media admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSamplingPolicy {
    /// Exact host input used only when the remote transcript does not end in a
    /// user message. This is explicit policy, not a synthesized remote message.
    pub continuation: ModelContent,
    /// Opt-in image processing contract; absent means images are unsupported.
    pub image: Option<NativeMediaPolicy>,
    /// Opt-in audio duration/work/byte contract; absent means audio is unsupported.
    pub audio: Option<NativeMediaPolicy>,
}

/// Concrete recorded model/interaction adapter. The supplied stock executor is
/// already bound to the ordinary durable host/task (or local task), model,
/// context/token budgets and grants; no MCP-specific provider is introduced.
pub struct McpRuntimeCallbacks {
    executor: StockExecutor,
    journal: Arc<dyn ExecutionJournal>,
    model_name: String,
    sampling: McpSamplingPolicy,
}
impl McpRuntimeCallbacks {
    /// Selects the existing execution owner and model display identity.
    pub fn new(executor: StockExecutor, journal: Arc<dyn ExecutionJournal>, model_name: String, sampling: McpSamplingPolicy) -> Result<Self> {
        if model_name.is_empty() { return Err(Error::Invalid("MCP sampling model name is empty".into())); }
        sampling.continuation.validate_user_input()?;
        if sampling.image.as_ref().is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Image { .. }))
            || sampling.audio.as_ref().is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Audio { .. }))
        { return Err(Error::Invalid("MCP sampling media intent differs from selected modality".into())); }
        Ok(Self { executor: executor.with_tool_registry(crate::tool::ToolRegistry::new()), journal, model_name, sampling })
    }
    async fn run(&self, context: ToolContext, request: &McpCallbackRequest) -> Result<Value> {
        crate::contract::validate_json_byte_bound(request, context.scope().limits().file_bytes)?;
        request.validate()?;
        if request.parent != context.operation_id() { return Err(Error::Unauthorized("MCP callback parent differs from admitted call".into())); }
        let operation = request.operation()?;
        let method = request.message.get("method").and_then(Value::as_str)
            .ok_or_else(|| Error::Invalid("MCP callback method absent".into()))?;
        let params = request.message.get("params").cloned().unwrap_or_else(|| json!({}));
        let result = match method {
            "sampling/createMessage" if request.policy.sampling => {
                if !context.scope().grants().contains("model:generate") { return Err(Error::Unauthorized("MCP sampling lacks model:generate".into())); }
                validate_sampling(&params)?;
                if params.get("tools").is_some_and(|tools| tools.as_array().is_none_or(|tools| !tools.is_empty())) {
                    return Err(Error::Unsupported("MCP sampling tool projection was not selected by this host".into()));
                }
                let sampling: SamplingRequest = serde_json::from_value(params)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                // Pins fields that the host-selected model does not consume too.
                self.pin(operation, request).await?;
                let (input, prior) = sampling.messages(&context, operation, &self.sampling).await?;
                let stage = Arc::new(SourceStage::new("mcp-sampling", operation.to_string(), Arc::new(Context { messages: prior, ..Context::default() }), ContextPlacement::Prepend));
                let executor = self.executor.clone().with_max_output_tokens(sampling.max_tokens)?
                    .with_context_stage(stage)?
                    .with_task_context(context.task(), operation)?;
                let events = executor.model_step(self.journal.as_ref(), &TurnInput { operation_id: operation, input, selected_context: None, max_steps: 1 }, 0, &[]).await?;
                let mut text = String::new();
                for event in events {
                    match event {
                        ModelEvent::Content { delta } => text.push_str(&delta),
                        ModelEvent::ToolCall { .. } => return Err(Error::Unsupported("MCP sampling tools require host-selected tool projection".into())),
                        ModelEvent::Reasoning { .. } | ModelEvent::Completed { .. } => {}
                    }
                }
                json!({"role":"assistant","content":{"type":"text","text":text},"model":self.model_name,"stopReason":"endTurn"})
            }
            "elicitation/create" if request.policy.elicitation => {
                if !context.scope().grants().contains("interaction:route") { return Err(Error::Unauthorized("MCP elicitation lacks interaction:route".into())); }
                let _: rmcp::model::ElicitRequestParams = rmcp::model::ElicitRequestParams::deserialize(&params)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                if params.get("mode").and_then(Value::as_str).is_some_and(|mode| mode != "form") {
                    return Err(Error::Unsupported("MCP URL elicitation requires a separately installed host interaction".into()));
                }
                let interaction = Interaction::Form {
                    title: params.get("message").and_then(Value::as_str).ok_or_else(|| Error::Invalid("MCP form message absent".into()))?.into(),
                    schema: params.get("requestedSchema").cloned().ok_or_else(|| Error::Invalid("MCP form schema absent".into()))?,
                };
                interaction.validate()?;
                self.pin(operation, request).await?;
                match context.interact(operation, interaction.clone()).await? {
                    InteractionOutcome::Answered { answer } => {
                        let bytes = self.journal.load(&answer).await?;
                        answer.descriptor().verify(&bytes)?;
                        let response: InteractionResponse = crate::contract::json_from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
                        interaction.validate_response(&response)?;
                        let InteractionResponse::Form { value } = response else { return Err(Error::Invalid("MCP form answer kind differs".into())); };
                        json!({"action":"accept","content":value})
                    }
                    InteractionOutcome::Declined | InteractionOutcome::Denied => json!({"action":"decline"}),
                    InteractionOutcome::Cancelled | InteractionOutcome::Expired => json!({"action":"cancel"}),
                    InteractionOutcome::Indeterminate { .. } => return Err(Error::Indeterminate(operation)),
                    InteractionOutcome::Approved => return Err(Error::Invalid("MCP form received approval".into())),
                }
            }
            _ => return Err(Error::Unsupported("MCP callback not enabled by pinned host policy".into())),
        };
        Ok(json!({"jsonrpc":"2.0","id":request.message.get("id"),"result":result}))
    }
    async fn pin(&self, operation: OperationId, request: &McpCallbackRequest) -> Result<()> {
        self.journal.stage(operation, "mcp:callback:request".into(), crate::contract::canonical_json_bytes(&(request, &self.model_name, &self.sampling))?, "application/json").await?;
        Ok(())
    }
}
impl McpCallbackAdmission for McpRuntimeCallbacks {
    fn admit<'a>(&'a self, context: ToolContext, request: &'a McpCallbackRequest) -> BoxProviderFuture<'a, Result<Value>> {
        Box::pin(self.run(context, request))
    }
}

#[expect(deprecated, reason = "sampling is required by the pinned MCP 2025-11-25 protocol; rmcp retains its typed contract")]
fn validate_sampling(params: &Value) -> Result<()> {
    let _: rmcp::model::CreateMessageRequestParams = rmcp::model::CreateMessageRequestParams::deserialize(params)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

#[expect(deprecated, reason = "sampling is required by the pinned MCP 2025-11-25 protocol; rmcp retains its typed contract")]
fn validate_sampling_result(value: Value) -> Result<()> {
    let result: rmcp::model::CreateMessageResult = serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    result.validate().map_err(Error::Invalid)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SamplingRequest { messages: Vec<Value>, #[serde(default)] system_prompt: Option<String>, max_tokens: u32 }
impl SamplingRequest {
    async fn messages(&self, context: &ToolContext, operation: OperationId, policy: &McpSamplingPolicy) -> Result<(ModelContent, Vec<ModelMessage>)> {
        if self.max_tokens == 0 { return Err(Error::Invalid("MCP sampling token bound is zero".into())); }
        let mut messages = Vec::new();
        // Server-supplied system prompt remains external user content.
        if let Some(prompt) = &self.system_prompt { messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text(prompt.clone()) }); }
        for (message_index, message) in self.messages.iter().enumerate() {
            let role = match message.get("role").and_then(Value::as_str) {
                Some("user") => ModelRole::User, Some("assistant") => ModelRole::Assistant,
                _ => return Err(Error::Invalid("invalid MCP sampling role".into())),
            };
            let content = message.get("content").ok_or_else(|| Error::Invalid("MCP sampling content absent".into()))?;
            let blocks = content.as_array().map_or_else(|| std::slice::from_ref(content), Vec::as_slice);
            let mut parts = Vec::new();
            for (block_index, block) in blocks.iter().enumerate() {
                let part = match block.get("type").and_then(Value::as_str) {
                    Some("text") => ModelContentPart::Text { text: block.get("text").and_then(Value::as_str).ok_or_else(|| Error::Invalid("MCP sampling text absent".into()))?.into() },
                    Some(kind @ ("image" | "audio")) => {
                        let media = if kind == "image" { &policy.image } else { &policy.audio };
                        let media = media.as_ref().ok_or_else(|| Error::Unsupported("MCP sampling media was not selected by this host".into()))?;
                        let maximum_bytes = media.maximum_bytes.min(context.scope().limits().file_bytes);
                        let encoded_bound = maximum_bytes.checked_add(2).and_then(|length| (length / 3).checked_mul(4)).ok_or_else(|| Error::Invalid("MCP media byte allowance overflows".into()))?;
                        let encoded = block.get("data").and_then(Value::as_str).ok_or_else(|| Error::Invalid("MCP media data absent".into()))?;
                        if maximum_bytes == 0 || encoded.len() as u64 > encoded_bound { return Err(Error::Invalid("MCP media exceeds selected byte allowance".into())); }
                        let bytes = base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_| Error::Invalid("MCP media base64 invalid".into()))?;
                        if bytes.len() as u64 > maximum_bytes { return Err(Error::Invalid("MCP media exceeds selected byte allowance".into())); }
                        let mime = block.get("mimeType").and_then(Value::as_str).ok_or_else(|| Error::Invalid("MCP media MIME type absent".into()))?;
                        let digest = crate::contract::canonical_json_digest(&("harness/mcp-sampling-media/v1", operation, message_index, block_index))?;
                        let mut identity = [0;16];
                        for (byte, source) in identity.iter_mut().zip(digest) { *byte = source; }
                        let path = format!("mcp/{operation}/sampling/{message_index}-{block_index}");
                        let file = context.stage_file_once(OperationId::from_bytes(identity), &path, &bytes, mime, "MCP sampling media").await?;
                        media.validate(&file, context.scope().limits())?;
                        ModelContentPart::File { file, policy: FileProjectionPolicy::Native(Box::new(media.clone())) }
                    }
                    _ => return Err(Error::Unsupported("MCP sampling content needs a host-selected projection".into())),
                };
                parts.push(part);
            }
            messages.push(ModelMessage { role, content: ModelContent::Parts(parts) });
        }
        if messages.last().is_some_and(|message| message.role == ModelRole::User) {
            let last = messages.pop().ok_or_else(|| Error::Invalid("MCP sampling input disappeared".into()))?;
            Ok((last.content, messages))
        } else {
            Ok((policy.continuation.clone(), messages))
        }
    }
}
