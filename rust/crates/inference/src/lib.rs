//! Inference provider contract and deterministic reference model.

use acyclic_contracts::Result;
use async_trait::async_trait;

/// Model inference request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceRequest {
    /// Selected model name.
    pub model: String,
    /// Canonical input messages.
    pub messages: Vec<String>,
}

/// Model inference response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceResponse {
    /// Generated output text.
    pub output: String,
}

/// Provider interface implemented by deterministic, customer, and hosted models.
#[async_trait]
pub trait InferenceProvider: Send + Sync {
    /// Completes a model request.
    async fn complete(&self, request: InferenceRequest) -> Result<InferenceResponse>;
}

/// Deterministic provider suitable for examples and conformance tests.
#[derive(Clone, Default)]
pub struct DeterministicInference;

#[async_trait]
impl InferenceProvider for DeterministicInference {
    async fn complete(&self, request: InferenceRequest) -> Result<InferenceResponse> {
        Ok(InferenceResponse {
            output: request.messages.join("\n"),
        })
    }
}
