//! Provider-neutral immutable model values and streaming host contract.

use crate::{Error, OperationId, Result, conversation::FileRef, registry::ComponentIdentity};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable provider-owned model selection; there is no global catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    /// Provider selected by application code.
    pub provider: String,
    /// Provider-owned model name.
    pub name: String,
    /// Immutable model revision or digest.
    pub revision: String,
    /// Provider-specific model-visible options admitted by a registered policy.
    /// Credentials remain provider-owned transport state.
    pub options: Value,
}

/// Registered schema and immutable identity for model-visible options.
///
/// Transport credentials belong to the provider implementation and are never
/// represented by this value. The identity is carried into model-input
/// admission so a policy revision cannot silently drift across a fork.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOptionPolicy {
    /// Stable policy implementation and schema identity.
    pub identity: ComponentIdentity,
    /// JSON Schema for the model-visible options object.
    pub schema: Value,
}

impl ModelOptionPolicy {
    /// Registers one immutable option schema and its non-empty identity.
    pub fn new(identity: ComponentIdentity, schema: Value) -> Result<Self> {
        let policy = Self { identity, schema };
        policy.validate_registration()?;
        Ok(policy)
    }

    /// Returns the canonical digest of the registered model-visible schema.
    pub fn schema_digest(&self) -> Result<[u8; 32]> {
        self.validate_registration()?;
        crate::contract::canonical_json_digest(&self.schema)
    }

    fn validate_registration(&self) -> Result<()> {
        crate::registry::validate_component_label(&self.identity.name, "model option policy name")?;
        crate::registry::validate_component_label(
            &self.identity.version,
            "model option policy revision",
        )?;
        if self.identity.digest == [0; 32] {
            return Err(Error::Invalid("model option policy digest is empty".into()));
        }
        jsonschema::validator_for(&self.schema)
            .map_err(|error| Error::Invalid(format!("invalid model option schema: {error}")))?;
        Ok(())
    }

    /// Validates only the model-visible option value against the pinned schema.
    pub fn validate(&self, options: &Value) -> Result<()> {
        self.validate_registration()?;
        crate::tool::validate_value(&self.schema, options, "model options")
    }
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
        if value.provider.trim().is_empty()
            || value.name.trim().is_empty()
            || value.revision.trim().is_empty()
        {
            return Err(Error::Invalid(
                "model provider, name, and revision must be non-empty".into(),
            ));
        }
        Ok(value)
    }
}

/// One provider-neutral prompt item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Versioned canonical JSON string used to preserve a model text message in ref-only history.
pub const MODEL_TEXT_MEDIA_TYPE: &str = "application/vnd.acyclic.model-text.v1+json";

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

/// Complete immutable request to a model provider.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Returns the registered model-visible option policy for this provider.
    /// Provider credentials remain implementation state and are never returned.
    fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
        None
    }

    /// Validates immutable input before a new dispatch or recovered attempt.
    /// This hook must not perform I/O or mutate the request.
    fn admit(&self, request: &ModelRequest) -> Result<()> {
        if let Some(policy) = self.model_option_policy() {
            return policy.validate(&request.model.options);
        }
        let empty = request.model.options.is_null()
            || request
                .model
                .options
                .as_object()
                .is_some_and(|value| value.is_empty());
        if empty {
            Ok(())
        } else {
            Err(Error::Invalid(
                "model options require a registered provider policy".into(),
            ))
        }
    }

    /// Starts one request from the exact bytes admitted by the harness.
    ///
    /// Providers that serialize a wire request must consume
    /// [`crate::model_input::PreparedModelInput::bytes`] directly.
    fn generate<'a>(
        &'a self,
        prepared: crate::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>>;

    /// Reconciles only after verifying the original complete request.
    fn reconcile_admitted<'a>(
        &'a self,
        request: ModelRequest,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            self.admit(&request)?;
            if crate::contract::canonical_json_digest(&request)? != attempt.request_digest {
                return Err(Error::Conflict(
                    "reconciliation request digest changed".into(),
                ));
            }
            self.reconcile(attempt).await
        })
    }

    /// Continues or reconciles an interrupted run without starting another model request.
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>>;
}

#[cfg(test)]
mod wire_contract_tests {
    use super::*;
    use serde_json::json;

    fn request() -> serde_json::Value {
        json!({
            "model": {"provider": "mock", "name": "local", "revision": "1", "options": {}},
            "messages": [{"role": "user", "content": "exact input"}],
            "tools": [],
            "max_output_tokens": null
        })
    }

    #[test]
    fn model_request_rejects_discarded_transport_and_nested_fields() {
        let original = request();
        assert!(serde_json::from_value::<ModelRequest>(original.clone()).is_ok());
        for layer in ["request", "model", "message", "part"] {
            let mut altered = original.clone();
            match layer {
                "request" => altered["transport_metadata"] = json!({"secret": "hidden"}),
                "model" => altered["model"]["transport_metadata"] = json!("hidden"),
                "message" => altered["messages"][0]["ui_state"] = json!("hidden"),
                "part" => {
                    altered["messages"][0]["content"] =
                        json!({"kind": "text", "text": "exact input", "ui_state": "hidden"})
                }
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<ModelRequest>(altered).is_err(),
                "{layer}"
            );
        }
    }

    #[test]
    fn omitted_and_null_token_bounds_have_identical_canonical_bytes() -> Result<()> {
        let explicit: ModelRequest =
            serde_json::from_value(request()).map_err(|error| Error::Invalid(error.to_string()))?;
        let mut omitted = request();
        omitted
            .as_object_mut()
            .expect("request object")
            .remove("max_output_tokens");
        let omitted: ModelRequest =
            serde_json::from_value(omitted).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(explicit, omitted);
        assert_eq!(
            crate::contract::canonical_json_bytes(&explicit)?,
            crate::contract::canonical_json_bytes(&omitted)?
        );
        Ok(())
    }

    #[test]
    fn registered_model_option_policy_rejects_undeclared_fields() -> Result<()> {
        let policy = ModelOptionPolicy::new(
            crate::registry::ComponentIdentity {
                name: "mock.options".into(),
                version: "1".into(),
                digest: [7; 32],
            },
            json!({
                "type": "object",
                "properties": {"mode": {"type": "string"}},
                "additionalProperties": false,
            }),
        )?;
        assert!(policy.validate(&json!({"mode": "safe"})).is_ok());
        assert!(policy.validate(&json!({"credential": "secret"})).is_err());
        Ok(())
    }

    #[test]
    fn deserialized_model_option_policy_revalidates_identity_and_schema() -> Result<()> {
        let policy = ModelOptionPolicy::new(
            crate::registry::ComponentIdentity {
                name: "mock.options".into(),
                version: "1".into(),
                digest: [7; 32],
            },
            json!({"type": "object", "additionalProperties": false}),
        )?;
        let mut encoded =
            serde_json::to_value(&policy).map_err(|error| Error::Invalid(error.to_string()))?;
        encoded["schema"] = json!({"type": "not-a-schema-type"});
        let decoded: ModelOptionPolicy =
            serde_json::from_value(encoded).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(decoded.validate(&json!({})).is_err());
        Ok(())
    }
}
