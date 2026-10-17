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
    /// Keeps at least half the capacity after fixed overhead for input, then
    /// splits that input between recent context and mandatory content/summary.
    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "filesystem-local")))]
    pub(crate) fn for_capacity(capacity: ModelContextCapacity, fixed_tokens: u32) -> Result<Self> {
        capacity.validate()?;
        let available = capacity
            .context_tokens
            .checked_sub(fixed_tokens)
            .filter(|available| *available >= 3)
            .ok_or_else(|| {
                Error::Invalid("compaction budgets exceed selected model capacity".into())
            })?;
        let mut policy = Self::default();
        policy.response_reserve_tokens = policy
            .response_reserve_tokens
            .min(capacity.output_tokens)
            .min(available / 2);
        let usable = available - policy.response_reserve_tokens;
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

#[cfg(feature = "filesystem")]
impl CompactionPolicy {
    /// Copies the policy on the optional original preparation ledger.
    pub(crate) fn copy(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        Ok(match self {
            Self::Disabled => Self::Disabled,
            Self::Threshold(policy) => Self::Threshold(ThresholdCompaction {
                response_reserve_tokens: policy.response_reserve_tokens,
                recent_tokens: policy.recent_tokens,
                retention: policy.retention.copy(io)?,
            }),
        })
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
#[cfg(all(test, not(target_arch = "wasm32")))]
mod copying_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod defaults_tests;
#[cfg(test)]
mod tests;
