use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Complete provider tool call, shared by stream events and replayed context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ToolCall {
    /// Harness-minted canonical call identity; provider bindings live in metadata.
    #[serde(alias = "callId")]
    pub call_id: String,
    /// Registered tool name.
    pub name: String,
    /// Schema-defined arguments.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    #[serde(deserialize_with = "crate::deserialize_json_value")]
    pub arguments: Value,
    /// Opaque provider continuation data retained for follow-ups and replay.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    #[serde(deserialize_with = "crate::deserialize_json_value")]
    pub metadata: Value,
}

impl ToolCall {
    /// Constructs a call without optional provider continuation data.
    pub fn new(call_id: String, name: String, arguments: Value) -> Self {
        Self {
            call_id,
            name,
            arguments,
            metadata: Value::Null,
        }
    }

    /// Attaches continuation data without changing the tool's arguments.
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn payload_bytes(&self) -> crate::Result<u64> {
        use super::inventory::add;
        use crate::executor::json::json_value_logical_owned_bytes;
        add(
            add(self.call_id.capacity() as u64, self.name.capacity() as u64)?,
            add(
                json_value_logical_owned_bytes(&self.arguments)?,
                json_value_logical_owned_bytes(&self.metadata)?,
            )?,
        )
    }
}
