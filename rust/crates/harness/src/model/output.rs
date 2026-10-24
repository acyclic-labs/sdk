//! Opaque, ordered completed provider output with explicit executable calls.

use super::ReasoningOrigin;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One advertised executable call in a completed provider output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub struct ModelToolCall {
    /// Provider-owned identity.
    pub call_id: String,
    /// Registered tool name.
    pub name: String,
    /// Exact semantic arguments.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    pub arguments: Value,
}

/// Completed output retained as one ordered, provider-owned continuation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
pub struct OutputBlock {
    /// Origin identity; the same provider/model/credential ownership used for reasoning.
    pub origin: ReasoningOrigin,
    /// Safe readable projection, separate from opaque native state.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub text: Option<String>,
    /// Exact ordered native output bytes. The SDK never interprets or rewrites these.
    pub continuity: Vec<u8>,
    /// Calls advertised by that native output, in order. Must match operational events.
    pub calls: Vec<ModelToolCall>,
}

impl OutputBlock {
    pub(crate) fn validate_limits(&self, limits: crate::conversation::Limits) -> crate::Result<()> {
        limits.validate()?;
        if self.continuity.len() as u64 > limits.render_bytes
            || crate::contract::canonical_json_bytes(self)?.len() as u64 > limits.render_bytes
            || self.calls.len() > limits.tool_calls_per_step
        {
            return Err(crate::Error::Invalid(
                "model output exceeds continuation limits".into(),
            ));
        }
        for call in &self.calls {
            crate::tool::ToolInvocation::validate_identity(&call.call_id, &call.name)?;
        }
        Ok(())
    }
}
