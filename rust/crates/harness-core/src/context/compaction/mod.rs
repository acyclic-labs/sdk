//! Primitive threshold compaction using provider-owned token accounting.

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
    /// Minimum recent suffix token budget retained alongside mandatory content.
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

    /// Plans retained input around the exact admitted output reservation.
    pub(crate) fn for_reservation(
        capacity: ModelContextCapacity,
        fixed_tokens: u32,
        output_tokens: u32,
    ) -> Result<Self> {
        let output_tokens = capacity.output_reservation(Some(output_tokens))?;
        Self::for_capacity(
            ModelContextCapacity {
                output_tokens: Some(output_tokens),
                ..capacity
            },
            fixed_tokens,
        )
    }

    /// Plans input headroom at the selected watermark while retaining the
    /// original output reservation. A narrower provider capacity can require
    /// preparation earlier, never later than the selected watermark.
    pub(crate) fn for_watermark(
        capacity: ModelContextCapacity,
        watermark_tokens: u32,
        output_tokens: u32,
    ) -> Result<Self> {
        let mut policy = Self::for_reservation(capacity, 0, output_tokens)?;
        let watermark = watermark_tokens.min(capacity.context_tokens - output_tokens);
        if watermark < 2 {
            return Err(Error::Invalid(
                "compaction watermark leaves no retained input capacity".into(),
            ));
        }
        policy.response_reserve_tokens = capacity.context_tokens - watermark;
        policy.recent_tokens = policy.recent_tokens.min(watermark / 2);
        policy.validate(capacity, Some(output_tokens))?;
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
pub(crate) use projection::compact_projection;
