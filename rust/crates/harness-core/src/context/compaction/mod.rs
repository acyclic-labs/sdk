//! The context window kernel: verified admission against the selected
//! model's window, with model-chosen rewrites that preserve whole dependency
//! groups. Failed or insufficient rewrites never authorize silent truncation.

use super::{Context, ContextSummary, ModelContextCapacity};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

/// Capacity admission against the selected model's context window.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct CapacityCompaction {}

impl CapacityCompaction {
    /// Only actual model capacity can require a rewrite.
    pub fn needs_compaction(
        &self,
        capacity: ModelContextCapacity,
        input_tokens: u64,
    ) -> Result<bool> {
        Ok(input_tokens > self.input_ceiling(capacity)?)
    }

    /// Input room after reserving the provider's verified maximum response.
    /// Unknown response capacity cannot authorize a smaller, guessed reserve.
    pub fn input_ceiling(&self, capacity: ModelContextCapacity) -> Result<u64> {
        capacity.validate()?;
        let reserve = capacity.output_tokens.ok_or_else(|| {
            Error::Invalid("context management requires a verified response capacity".into())
        })?;
        let ceiling = capacity
            .context_tokens
            .checked_sub(reserve)
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::Invalid("response reserve exhausts the model context window".into())
            })?;
        Ok(u64::from(ceiling))
    }
}

/// Selects capacity-bound compaction or the installed context pipeline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "config",
    rename_all = "snake_case"
)]
pub enum CompactionPolicy {
    /// Send the installed context pipeline's requests unchecked.
    Disabled,
    /// Admit each request against the selected model's window, asking the
    /// separate context manager for a rewrite when required.
    Capacity(CapacityCompaction),
}

impl CompactionPolicy {
    /// Selects context-window admission unless compaction is explicitly disabled.
    pub fn for_model(
        selected: Option<Self>,
        model: &crate::model::Model,
        provider: &dyn crate::model::ModelProvider,
    ) -> Result<Self> {
        if matches!(selected, Some(Self::Disabled)) {
            return Ok(Self::Disabled);
        }
        let capacity = provider.context_capacity(model)?;
        capacity.validate()?;
        Ok(selected.unwrap_or_default())
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::Capacity(CapacityCompaction::default())
    }
}

mod exchanges;
mod selection;
pub(crate) use selection::{compact_context, mandatory_message_count, mandatory_messages};
mod copying;
pub(crate) use copying::{clone_context, clone_context_range, clone_messages, clone_metadata};
mod allocation;
pub(crate) use allocation::reserve_vector;
use allocation::{quota_vector, retire_vector};
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod copying_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod defaults_tests;
#[cfg(test)]
mod tests;

mod projection;
pub(crate) use projection::{compact_projection, prefix_digest};
