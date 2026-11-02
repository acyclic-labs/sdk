//! The context window kernel: verified admission against the selected
//! model's window and a mechanical overflow backstop that evicts the oldest
//! whole dependency groups into one fixed marker, with no model call.

use super::{Context, ContextSummary, ModelContextCapacity, ModelTokenCount};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

/// Ordinary configurable threshold compaction; no model-family heuristic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ThresholdCompaction {
    /// Input headroom retained for each response.
    pub response_reserve_tokens: u32,
    /// Recent suffix the backstop retains alongside the irreducible set: its
    /// low-water mark, so one eviction leaves room for later steps instead of
    /// evicting, and breaking the prefix cache, on each.
    pub recent_tokens: u32,
}

impl Default for ThresholdCompaction {
    fn default() -> Self {
        Self {
            response_reserve_tokens: 16_384,
            recent_tokens: 20_000,
        }
    }
}

impl ThresholdCompaction {
    /// Reserves the selected output capacity before splitting remaining input.
    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "filesystem-local")))]
    #[doc(hidden)]
    pub fn for_capacity(capacity: ModelContextCapacity, fixed_tokens: u32) -> Result<Self> {
        capacity.validate()?;
        let output = capacity.output_reservation(None)?;
        let usable = capacity
            .context_tokens
            .checked_sub(fixed_tokens)
            .and_then(|available| available.checked_sub(output))
            .filter(|available| *available >= 2)
            .ok_or_else(|| {
                Error::Invalid("compaction budgets exceed selected model capacity".into())
            })?;
        let mut policy = Self::default();
        policy.response_reserve_tokens = output;
        policy.recent_tokens = policy.recent_tokens.min(usable / 2);
        policy.validate(capacity, None)?;
        Ok(policy)
    }

    /// Validates the defaults or replacement against the actual selected capacity.
    pub fn validate(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u32> {
        capacity.validate()?;
        let output = capacity.output_reservation(output_tokens)?;
        let required =
            u64::from(self.response_reserve_tokens.max(output)) + u64::from(self.recent_tokens);
        if self.response_reserve_tokens == 0
            || self.recent_tokens == 0
            || required >= u64::from(capacity.context_tokens)
        {
            return Err(Error::Invalid(
                "compaction budgets exceed selected model capacity".into(),
            ));
        }
        Ok(output)
    }

    /// Tests input pressure against the validated response reserve.
    pub fn needs_compaction(
        &self,
        capacity: ModelContextCapacity,
        input_tokens: u64,
        output_tokens: Option<u32>,
    ) -> Result<bool> {
        let output = self.validate(capacity, output_tokens)?;
        Ok(input_tokens
            > u64::from(capacity.context_tokens - self.response_reserve_tokens.max(output)))
    }
}

/// Selects automatic threshold compaction or the installed context pipeline.
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
    /// Admit each request against the selected model's window, evicting into
    /// the backstop's marker at overflow.
    Threshold(ThresholdCompaction),
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::Threshold(ThresholdCompaction::default())
    }
}

mod exchanges;
mod selection;
pub(crate) use selection::{compact_context, mandatory_messages};
mod copying;
pub(crate) use copying::{clone_context, clone_messages, clone_metadata};
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

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod unknown_output_tests;

mod projection;
pub(crate) use projection::{compact_projection, prefix_digest};
