//! The context window kernel: verified admission against the selected
//! model's window and a mechanical overflow backstop that evicts the oldest
//! whole dependency groups into one fixed marker, with no model call.

use super::{Context, ContextSummary, ModelContextCapacity};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

/// Capacity admission against the selected model, with optional caller headroom.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct CapacityCompaction {
    /// Caller-reserved response headroom; zero uses the selected output capacity.
    pub response_reserve_tokens: u32,
}

impl CapacityCompaction {
    /// Rejects selections that leave no room for history and its summary.
    #[doc(hidden)]
    pub fn for_capacity(capacity: ModelContextCapacity, fixed_tokens: u32) -> Result<Self> {
        let policy = Self::default();
        let output = policy.validate(capacity, None)?;
        if u64::from(fixed_tokens) + u64::from(output) + 2 > u64::from(capacity.context_tokens) {
            return Err(Error::Invalid(
                "compaction budgets exceed selected model capacity".into(),
            ));
        }
        Ok(policy)
    }

    /// Validates caller headroom against the actual selected output capacity.
    pub fn validate(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u32> {
        capacity.validate()?;
        let output = capacity.output_reservation(output_tokens)?;
        if self.response_reserve_tokens.max(output) >= capacity.context_tokens {
            return Err(Error::Invalid(
                "compaction budgets exceed selected model capacity".into(),
            ));
        }
        Ok(output)
    }

    /// Only actual model capacity can require a rewrite.
    pub fn needs_compaction(
        &self,
        capacity: ModelContextCapacity,
        input_tokens: u64,
        output_tokens: Option<u32>,
    ) -> Result<bool> {
        Ok(input_tokens > self.input_ceiling(capacity, output_tokens)?)
    }

    /// Selected context capacity less the response reservation.
    pub fn input_ceiling(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u64> {
        let output = self.validate(capacity, output_tokens)?;
        Ok(u64::from(
            capacity.context_tokens - self.response_reserve_tokens.max(output),
        ))
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
    /// Admit each request against the selected model's window, evicting into
    /// the backstop's marker at overflow.
    Capacity(CapacityCompaction),
}

impl CompactionPolicy {
    /// Selects automatic budgets for this model, or validates an explicit
    /// policy without changing its budgets. `None` selects automatic budgets.
    pub fn for_model(
        selected: Option<Self>,
        model: &crate::model::Model,
        provider: &dyn crate::model::ModelProvider,
    ) -> Result<Self> {
        Self::for_output(selected, model, provider, None)
    }

    pub(crate) fn for_output(
        selected: Option<Self>,
        model: &crate::model::Model,
        provider: &dyn crate::model::ModelProvider,
        output: Option<u32>,
    ) -> Result<Self> {
        if matches!(selected, Some(Self::Disabled)) {
            return Ok(Self::Disabled);
        }
        let capacity = provider.context_capacity(model)?;
        let output = output.or(provider
            .model_profile(model)?
            .and_then(|profile| profile.max_output_tokens()));
        let reservation = capacity.output_reservation(output)?;
        match selected {
            Some(Self::Capacity(policy)) => {
                policy.validate(capacity, output)?;
                Ok(Self::Capacity(policy))
            }
            _ => Ok(Self::Capacity(CapacityCompaction::for_capacity(
                ModelContextCapacity {
                    output_tokens: Some(reservation),
                    ..capacity
                },
                0,
            )?)),
        }
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
