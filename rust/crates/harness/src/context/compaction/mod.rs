//! Primitive threshold compaction using provider-owned token accounting.

use super::{CompactionRetention, Context, ContextSummary, ModelContextCapacity, ModelTokenCount};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

/// Ordinary configurable threshold compaction; no model-family heuristic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ThresholdCompaction {
    /// Input headroom retained for each response.
    pub response_reserve_tokens: u32,
    /// Minimum recent suffix token budget retained alongside mandatory content.
    pub recent_tokens: u32,
    /// Consumer-selected roles and media that remain verbatim.
    pub retention: CompactionRetention,
}

impl Default for ThresholdCompaction {
    fn default() -> Self {
        Self {
            response_reserve_tokens: 16_384,
            recent_tokens: 20_000,
            retention: CompactionRetention::default(),
        }
    }
}

impl ThresholdCompaction {
    /// Validates the defaults or replacement against the actual selected capacity.
    pub fn validate(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u32> {
        capacity.validate()?;
        self.retention.validate(None)?;
        let required = u64::from(self.response_reserve_tokens) + u64::from(self.recent_tokens);
        let output =
            output_tokens.unwrap_or(capacity.output_tokens.min(self.response_reserve_tokens));
        if self.response_reserve_tokens == 0
            || self.recent_tokens == 0
            || required >= u64::from(capacity.context_tokens)
            || output == 0
            || output > capacity.output_tokens
            || output > self.response_reserve_tokens
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
    ) -> Result<bool> {
        self.validate(capacity, None)?;
        Ok(input_tokens > u64::from(capacity.context_tokens - self.response_reserve_tokens))
    }
}

/// Selects automatic threshold compaction or the installed context pipeline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "config",
    rename_all = "snake_case"
)]
pub enum CompactionPolicy {
    /// Preserve the installed context pipeline without automatic summarization.
    Disabled,
    /// Perform admitted threshold compaction under the selected model's capacities.
    Threshold(ThresholdCompaction),
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::Threshold(ThresholdCompaction::default())
    }
}

mod selection;
pub(crate) use selection::compact_context;
pub(super) use selection::mandatory_flags;
use selection::{compaction_error, compaction_step};
mod copying;
pub(crate) use copying::{clone_context, clone_messages, clone_metadata};
mod allocation;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use allocation::clone_retention_native;
pub(crate) use allocation::reserve_vector;
use allocation::{quota_vector, retire_vector};
#[cfg(test)]
mod tests;
