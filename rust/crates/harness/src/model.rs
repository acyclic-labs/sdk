//! Provider-neutral immutable model values and streaming host contract.

use crate::{Error, OperationId, Result};
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
pub struct ModelMessage {
    /// Semantic role (`system`, `user`, `assistant`, or `tool`).
    pub role: String,
    /// Structured content owned by the application/provider bridge.
    pub content: Value,
}

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

/// Ordered event emitted by a model run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>>;

    /// Continues or reconciles an interrupted run without starting another model request.
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>>;
}
