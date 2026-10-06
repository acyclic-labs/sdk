//! Provider-neutral immutable model values and streaming host contract.

use crate::{Error, OperationId, Result, conversation::FileRef};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable provider-owned model selection; there is no global catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    /// Provider selected by application code.
    pub provider: String,
    /// Provider-owned model name.
    pub name: String,
    /// Immutable model revision or digest.
    pub revision: String,
    /// Provider-specific options retained as typed JSON.
    pub options: Value,
}

impl Model {
    /// Creates a validated immutable model value.
    pub fn new(
        provider: impl Into<String>,
        name: impl Into<String>,
        revision: impl Into<String>,
        options: Value,
    ) -> Result<Self> {
        let value = Self {
            provider: provider.into(),
            name: name.into(),
            revision: revision.into(),
            options,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates constructor and deserialized selections.
    pub fn validate(&self) -> Result<()> {
        if self.provider.trim().is_empty()
            || self.name.trim().is_empty()
            || self.revision.trim().is_empty()
        {
            return Err(Error::Invalid(
                "model provider, name, and revision must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

/// One provider-neutral prompt item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelMessage {
    /// Semantic role (`system`, `user`, `assistant`, or `tool`).
    pub role: ModelRole,
    /// Typed, provider-neutral content; file bytes are never part of this value.
    pub content: ModelContent,
}

/// Closed provider-neutral role set; adapters may reject unsupported combinations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Host instruction.
    System,
    /// Human input.
    User,
    /// Agent or model output.
    Assistant,
    /// Result from a tool call.
    Tool,
}

impl ModelRole {
    /// Stable wire role name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// Transient model-visible content. Canonical conversation storage always retains refs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(
    clippy::large_enum_variant,
    reason = "model parts retain their public untagged content shape"
)]
pub enum ModelContent {
    /// Plain prompt text or bounded projection produced by a resolver.
    Text(String),
    /// One typed content part.
    Part(ModelContentPart),
    /// Ordered multimodal content parts.
    Parts(Vec<ModelContentPart>),
}

impl ModelContent {
    /// Applies the same byte and part ceilings to direct inputs and selected
    /// history before either can reach a provider or durable request journal.
    pub fn validate_limits(&self, limits: crate::conversation::Limits) -> Result<()> {
        limits.validate()?;
        let parts = match self {
            Self::Text(text) => {
                return if text.len() as u64 <= limits.render_bytes {
                    Ok(())
                } else {
                    Err(Error::Invalid("model text exceeds render limit".into()))
                };
            }
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) => parts.as_slice(),
        };
        if parts.len() > limits.attachments + 1 {
            return Err(Error::Invalid(
                "model content exceeds attachment limit".into(),
            ));
        }
        for part in parts {
            match part {
                ModelContentPart::Text { text } if text.len() as u64 > limits.render_bytes => {
                    return Err(Error::Invalid("model text exceeds render limit".into()));
                }
                ModelContentPart::File { file, policy } => {
                    limits.validate_file(file)?;
                    if *policy == FileProjectionPolicy::BoundedFull
                        && file.descriptor().byte_length() > limits.render_bytes
                    {
                        return Err(Error::Invalid("model file exceeds render limit".into()));
                    }
                }
                ModelContentPart::ToolCall {
                    name, arguments, ..
                } => {
                    crate::registry::validate_component_label(name, "tool name")?;
                    if crate::contract::canonical_json_bytes(arguments)?.len() as u64
                        > limits.render_bytes
                    {
                        return Err(Error::Invalid(
                            "model tool projection exceeds render limit".into(),
                        ));
                    }
                }
                ModelContentPart::ToolResult { name, value, .. } => {
                    crate::registry::validate_component_label(name, "tool name")?;
                    if crate::contract::canonical_json_bytes(value)?.len() as u64
                        > limits.render_bytes
                    {
                        return Err(Error::Invalid(
                            "model tool projection exceeds render limit".into(),
                        ));
                    }
                }
                ModelContentPart::Text { .. } => {}
            }
        }
        Ok(())
    }

    /// Rejects tool and empty content in a human turn input.
    pub fn validate_user_input(&self) -> Result<()> {
        let parts = match self {
            Self::Text(text) => {
                return if text.is_empty() {
                    Err(Error::Invalid("user input is empty".into()))
                } else {
                    Ok(())
                };
            }
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) if !parts.is_empty() && parts.len() <= 1_024 => parts.as_slice(),
            _ => return Err(Error::Invalid("user input part count is invalid".into())),
        };
        for part in parts {
            match part {
                ModelContentPart::Text { text } if !text.is_empty() => {}
                ModelContentPart::File { file, .. } => file.validate()?,
                _ => {
                    return Err(Error::Invalid(
                        "user input contains an unsupported part".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Returns every immutable file referenced by one typed input.
    #[must_use]
    pub fn file_refs(&self) -> Vec<&FileRef> {
        match self {
            Self::Part(ModelContentPart::File { file, .. }) => vec![file],
            Self::Text(_) | Self::Part(_) => Vec::new(),
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ModelContentPart::File { file, .. } => Some(file),
                    _ => None,
                })
                .collect(),
        }
    }
}

/// One provider-neutral part requiring an explicit provider projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelContentPart {
    /// Plain text.
    Text {
        /// Model-visible text.
        text: String,
    },
    /// Immutable file reference with an explicit byte-resolution policy.
    File {
        /// Immutable file identity.
        file: FileRef,
        /// Resolution and rendering policy.
        policy: FileProjectionPolicy,
    },
    /// Assistant tool request.
    ToolCall {
        /// Stable call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        arguments: Value,
    },
    /// Tool result with matching call identity.
    ToolResult {
        /// Matching call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Model-visible result projection.
        value: Value,
    },
}

/// File handling policy selected before provider dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileProjectionPolicy {
    /// Metadata-only file reference, with no byte read.
    Reference,
    /// Verified, bounded text bytes.
    BoundedFull,
    /// Verified provider-native input such as an image.
    Native,
}

/// Hard aggregate ceiling for the canonical serialized model input, independent of output bounds.
pub const MAX_MODEL_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

/// Complete immutable request to a model provider.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Selected model value.
    pub model: Model,
    /// Ordered reconstructable context.
    pub messages: Vec<ModelMessage>,
    /// Tools visible for this request.
    pub tools: Vec<crate::tool::ToolDefinition>,
    /// Maximum output tokens when constrained.
    pub max_output_tokens: Option<u32>,
}

/// Immutable, admitted input and the exact canonical bytes supplied to a provider.
#[derive(Clone, Debug)]
pub struct PreparedModelRequest {
    request: ModelRequest,
    bytes: Vec<u8>,
    manifest: ModelRequestManifest,
}

/// Versioned identity of ordered provider input; counters bound wire bytes, not heap use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequestManifest {
    /// Canonical construction version.
    pub version: u32,
    /// BLAKE3 digest of the exact supplied bytes.
    pub request_digest: [u8; 32],
    /// Serialized byte count.
    pub wire_bytes: u64,
    /// Exact model and ordered tool definitions, including pinned revisions and schemas.
    pub binding_digest: [u8; 32],
    /// Ordered role and content identities.
    pub message_digests: Vec<[u8; 32]>,
}

impl ModelRequest {
    fn validate(&self, limits: crate::conversation::Limits) -> Result<()> {
        use crate::tool::{ToolInvocation, validate_value};
        use std::collections::BTreeMap;

        limits.validate()?;
        self.model.validate()?;
        if self.messages.is_empty() || self.messages.len() > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if self.max_output_tokens.is_none_or(|bound| bound == 0) {
            return Err(Error::Invalid(
                "model output token bound is required".into(),
            ));
        }
        let mut tools = BTreeMap::new();
        for tool in &self.tools {
            tool.validate()?;
            if tools.insert(&tool.name, tool).is_some() {
                return Err(Error::Invalid("model tool definition is repeated".into()));
            }
        }
        let mut pending = BTreeMap::new();
        for message in &self.messages {
            message.content.validate_limits(limits)?;
            let parts = match &message.content {
                ModelContent::Text(_) => &[][..],
                ModelContent::Part(part) => std::slice::from_ref(part),
                ModelContent::Parts(parts) => parts.as_slice(),
            };
            for part in parts {
                match part {
                    ModelContentPart::ToolCall {
                        call_id,
                        name,
                        arguments,
                    } => {
                        ToolInvocation::validate_identity(call_id, name)?;
                        if message.role != ModelRole::Assistant || pending.contains_key(call_id) {
                            return Err(Error::Invalid(
                                "model tool call role or identity is invalid".into(),
                            ));
                        }
                        let tool = tools
                            .get(name)
                            .ok_or_else(|| Error::Invalid("model tool call is unbound".into()))?;
                        validate_value(&tool.input_schema, arguments, "model tool arguments")?;
                        pending.insert(call_id, name);
                    }
                    ModelContentPart::ToolResult {
                        call_id,
                        name,
                        value,
                    } => {
                        ToolInvocation::validate_identity(call_id, name)?;
                        if message.role != ModelRole::Tool || pending.remove(call_id) != Some(name)
                        {
                            return Err(Error::Invalid(
                                "model tool result has no matching call".into(),
                            ));
                        }
                        let tool = tools
                            .get(name)
                            .ok_or_else(|| Error::Invalid("model tool result is unbound".into()))?;
                        validate_value(&tool.output_schema, value, "model tool result")?;
                    }
                    ModelContentPart::Text { .. } | ModelContentPart::File { .. } => {
                        if message.role == ModelRole::Tool {
                            return Err(Error::Invalid(
                                "tool role requires a paired result".into(),
                            ));
                        }
                    }
                }
            }
            if message.role == ModelRole::Tool && parts.is_empty() {
                return Err(Error::Invalid("tool role requires a paired result".into()));
            }
            if !pending.is_empty() && matches!(message.role, ModelRole::System | ModelRole::User) {
                return Err(Error::Invalid("model tool exchange is interrupted".into()));
            }
        }
        if !pending.is_empty() {
            return Err(Error::Invalid("model tool exchange is incomplete".into()));
        }
        Ok(())
    }
}

impl PreparedModelRequest {
    /// Admits explicit input without retrieval, compaction or truncation.
    /// Providers are trusted to honor the token ceiling and native encoding.
    pub fn prepare(request: ModelRequest, limits: crate::conversation::Limits) -> Result<Self> {
        request.validate(limits)?;
        let bytes = crate::contract::canonical_json_bytes(&request)?;
        if bytes.len() as u64 > MAX_MODEL_REQUEST_BYTES {
            return Err(Error::Invalid(
                "model request exceeds aggregate byte limit".into(),
            ));
        }
        let manifest = ModelRequestManifest {
            version: 1,
            request_digest: *blake3::hash(&bytes).as_bytes(),
            wire_bytes: bytes.len() as u64,
            binding_digest: crate::contract::canonical_json_digest(&(
                &request.model,
                &request.tools,
            ))?,
            message_digests: request
                .messages
                .iter()
                .map(crate::contract::canonical_json_digest)
                .collect::<Result<_>>()?,
        };
        Ok(Self {
            request,
            bytes,
            manifest,
        })
    }

    /// Read-only admitted values for provider-specific encoding.
    #[must_use]
    pub fn request(&self) -> &ModelRequest {
        &self.request
    }

    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Versioned identity of these bytes and ordered content.
    #[must_use]
    pub fn manifest(&self) -> &ModelRequestManifest {
        &self.manifest
    }
}

/// Ordered event emitted by a model run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelEvent {
    /// User-visible content fragment.
    Content {
        /// Incremental content.
        delta: String,
    },
    /// Model reasoning fragment when the provider exposes one.
    Reasoning {
        /// Incremental reasoning content.
        delta: String,
    },
    /// Complete request to invoke one registered tool.
    ToolCall {
        /// Provider/model-owned call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        arguments: Value,
    },
    /// Model completed this step.
    Completed {
        /// Provider-owned usage and finish metadata.
        metadata: Value,
    },
}

/// Durable identity and observed prefix of one interrupted model attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelAttempt {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Zero-based stock-executor step.
    pub step: u32,
    /// Canonical digest of the complete model request.
    pub request_digest: [u8; 32],
    /// Durable event prefix already observed.
    pub observed: Vec<ModelEvent>,
}

/// Replaceable streaming model provider.
pub trait ModelProvider: Send + Sync {
    /// Starts one request and yields ordered model events.
    fn generate<'a>(&'a self, request: PreparedModelRequest) -> BoxStream<'a, Result<ModelEvent>>;

    /// Continues or reconciles an interrupted run without starting another model request.
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::Limits;
    use serde_json::json;

    fn request() -> Result<ModelRequest> {
        Ok(ModelRequest {
            model: Model::new("mock", "exact", "pinned", json!({}))?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("é\0🦀\r\n".into()),
            }],
            tools: vec![crate::tool::ToolDefinition {
                name: "echo".into(),
                revision: "schema-1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"string"}),
                output_schema: json!({"type":"string"}),
            }],
            max_output_tokens: Some(32),
        })
    }

    fn exchange(request: &mut ModelRequest) {
        request.messages.extend([
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    arguments: json!("é"),
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    value: json!("é"),
                }),
            },
        ]);
    }

    #[test]
    fn canonical_bytes_pin_unicode_order_schemas_and_parent_mutation() -> Result<()> {
        let mut original = request()?;
        exchange(&mut original);
        let prepared = PreparedModelRequest::prepare(original.clone(), Limits::default())?;
        assert_eq!(
            prepared.bytes(),
            include_bytes!("../../../../fixtures/harness/v2/model-request.json")
        );
        assert_eq!(
            serde_json::from_slice::<ModelRequest>(prepared.bytes())
                .map_err(|e| Error::Invalid(e.to_string()))?,
            original
        );
        assert_eq!(
            prepared.manifest().request_digest,
            *blake3::hash(prepared.bytes()).as_bytes()
        );
        assert_eq!(
            prepared.manifest().wire_bytes,
            prepared.bytes().len() as u64
        );
        original.messages[0].content = ModelContent::Text("parent changed".into());
        assert_ne!(
            PreparedModelRequest::prepare(original, Limits::default())?
                .manifest()
                .request_digest,
            prepared.manifest().request_digest
        );
        let mut changed = prepared.request().clone();
        changed.tools[0].revision = "schema-2".into();
        assert_ne!(
            PreparedModelRequest::prepare(changed, Limits::default())?
                .manifest()
                .binding_digest,
            prepared.manifest().binding_digest
        );
        Ok(())
    }

    #[test]
    fn malformed_exchanges_and_bounds_fail_closed() -> Result<()> {
        let base = request()?;
        let mut valid = base.clone();
        exchange(&mut valid);
        PreparedModelRequest::prepare(valid.clone(), Limits::default())?;
        let mut candidates = Vec::new();
        let mut changed = valid.clone();
        changed.messages.pop();
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.messages.swap(1, 2);
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.messages[1].role = ModelRole::User;
        candidates.push(changed);
        let mut changed = valid.clone();
        if let ModelContent::Part(ModelContentPart::ToolResult { value, .. }) =
            &mut changed.messages[2].content
        {
            *value = json!(42);
        }
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.tools.push(changed.tools[0].clone());
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.max_output_tokens = None;
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.max_output_tokens = Some(0);
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.model.revision.clear();
        candidates.push(changed);
        let mut changed = valid.clone();
        if let ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) =
            &mut changed.messages[1].content
        {
            *arguments = json!(false);
        }
        candidates.push(changed);
        for candidate in candidates {
            assert!(PreparedModelRequest::prepare(candidate, Limits::default()).is_err());
        }
        let mut oversized = base;
        let maximum = usize::try_from(MAX_MODEL_REQUEST_BYTES)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        oversized.model.options = json!({"large": "x".repeat(maximum)});
        assert!(PreparedModelRequest::prepare(oversized, Limits::default()).is_err());
        Ok(())
    }
}
