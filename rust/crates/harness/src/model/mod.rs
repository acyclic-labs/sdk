//! Immutable provider-neutral model values and streaming contract.

mod content;
mod inventory;
mod media;
mod preparation;
mod prefix;
mod provider;
mod request;
#[cfg(test)]
mod tests;

pub use content::{ModelContent, ModelContentPart, ModelDataPart, ModelMessage, ModelRole, ToolResultContent};
pub use media::{FileProjectionPolicy, ImageDetail, NativeConfigurationBinding, NativeMediaIntent, NativeMediaPolicy};
pub use prefix::ModelPrefix;
pub use provider::{ModelDispatch, ModelEvent, ModelProvider};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use provider::NativeModelStream;
pub use request::{ModelRequest, PreparedModelRequest};
pub(crate) use preparation::{charge_items, clone_content, clone_file, clone_messages, clone_model,
    clone_request, clone_tool_definition, clone_value, hash_bytes, vec_with_capacity};
#[cfg(feature = "filesystem")]
pub(crate) use preparation::model_serde_diagnostic_label_bytes;

use crate::{Error, OperationId, Result, conversation::{FileRef, Limits}, executor::{PreparationIo, with_history_io}};
use crate::store::HistoryIo;
use crate::wire::ErrorCode::{Invalid, Storage, Unsupported};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use inventory::{add, overflow, slots};
use preparation::{history_step, invalid, value_bound};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

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
    pub options: Value,
}
impl Model {
    /// Constructs a validated model selection.
    pub fn new(provider: String, name: String, revision: String, options: Value) -> Result<Self> {
        let value = Self { provider, name, revision, options };
        value.validate()?;
        Ok(value)
    }
    /// Validates constructed or deserialized selections.
    pub fn validate(&self) -> Result<()> {
        if self.provider.trim().is_empty() || self.name.trim().is_empty() || self.revision.trim().is_empty() {
            return Err(Error::Invalid("model provider, name, and revision must be non-empty".into()));
        }
        Ok(())
    }
}
