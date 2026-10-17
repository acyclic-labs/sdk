//! Immutable provider-neutral model values and streaming contract.

mod call_id;
mod content;
mod instruction_identity;
mod instruction_reads;
mod instructions;
pub use instruction_identity::{InstructionIdentity, ProjectInstruction};
pub(crate) use instruction_reads::deduplicate_reads;
mod inventory;
pub use instructions::InstructionSource;
pub(crate) use instructions::{
    instruction_files, instruction_reservation, project_instruction, reserve_instruction_bodies,
};
mod media;
mod prefix;
mod preparation;
mod provider;
mod reasoning;
mod request;
mod usage;
pub(crate) use usage::accumulate_usage;
#[cfg(test)]
mod instruction_tests;
#[cfg(test)]
mod tests;
mod tool_call;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use crate::executor::json::json_value_logical_owned_bytes;
pub use content::{
    ModelContent, ModelContentPart, ModelDataPart, ModelMessage, ModelRole, ToolResultContent,
};
pub use media::{
    FileProjectionPolicy, ImageDetail, NativeConfigurationBinding, NativeMediaIntent,
    NativeMediaPolicy,
};
pub use prefix::ModelPrefix;
#[cfg(feature = "filesystem")]
pub(crate) use preparation::MODEL_DIAGNOSTIC_LABEL_BYTES;
pub(crate) use preparation::{
    charge_items, clone_content, clone_file, clone_messages, clone_model, clone_request,
    clone_tool_definition, clone_value, hash_bytes, vec_with_capacity,
};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use provider::NativeModelStream;
pub use provider::{ModelDispatch, ModelEvent, ModelProvider};
pub use reasoning::{ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome};
pub use request::{ModelRequest, PreparedModelRequest};
pub use tool_call::ToolCall;

use crate::store::HistoryIo;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::Invalid;
#[cfg(feature = "filesystem")]
use crate::wire::ErrorCode::Storage;
use crate::wire::ErrorCode::Unsupported;
use crate::{
    Error, OperationId, Result,
    conversation::{FileRef, Limits},
    executor::{PreparationIo, with_history_io},
};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use inventory::slots;
#[cfg(feature = "filesystem")]
use inventory::{add, overflow};
use preparation::{history_step, invalid, value_bound};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable provider-owned model selection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Model {
    /// Application-selected provider.
    pub provider: String,
    /// Provider-owned model name.
    pub name: String,
    /// Immutable revision or digest.
    pub revision: String,
    /// Provider-specific options.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    #[serde(deserialize_with = "crate::deserialize_json_value")]
    pub options: Value,
}
impl Model {
    /// Constructs a validated model selection.
    pub fn new(provider: String, name: String, revision: String, options: Value) -> Result<Self> {
        let value = Self {
            provider,
            name,
            revision,
            options,
        };
        value.validate()?;
        Ok(value)
    }
    /// Validates constructed or deserialized selections.
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
