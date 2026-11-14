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
    /// Only actual model capacity can require a rewrite. `response_tokens` is
    /// the admitted request's response evidence; see [`Self::input_ceiling`].
    pub fn needs_compaction(
        &self,
        capacity: ModelContextCapacity,
        input_tokens: u64,
        response_tokens: u64,
    ) -> Result<bool> {
        Ok(input_tokens > self.input_ceiling(capacity, response_tokens)?)
    }

    /// The most input that still leaves room for the next response. The
    /// catalog states how the provider budgets that response:
    ///
    /// - **Reserved** (`output_tokens` without `output_shares_context`): the
    ///   provider sets its published maximum response aside ahead of input,
    ///   so the ceiling is `context_tokens - output_tokens`.
    /// - **Shared** (`output_shares_context`): the response draws on whatever
    ///   window the input leaves, and its published maximum may be the whole
    ///   window, so nothing fixed is reserved. The ceiling is
    ///   `context_tokens - response_tokens`, where `response_tokens` is the
    ///   largest response this agent's admitted request retains: the largest
    ///   assistant message by the verified count's per-message weight. It is
    ///   zero before any response exists, grows only with observed responses,
    ///   and is never a constant or a request output cap.
    /// - **Unknown** (neither): refused, because any reserve would be a guess.
    ///
    /// Either way, a reserve that leaves no input room is an error.
    pub fn input_ceiling(
        &self,
        capacity: ModelContextCapacity,
        response_tokens: u64,
    ) -> Result<u64> {
        capacity.validate()?;
        let reserve = if capacity.output_shares_context {
            response_tokens
        } else {
            u64::from(capacity.output_tokens.ok_or_else(|| {
                Error::Invalid(
                    "context management requires a verified response capacity: the catalog must \
                     publish the model's reserved output_tokens or declare output_shares_context"
                        .into(),
                )
            })?)
        };
        u64::from(capacity.context_tokens)
            .checked_sub(reserve)
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::Invalid("response reserve exhausts the model context window".into())
            })
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
        let policy = selected.unwrap_or_default();
        let capacity = provider.context_capacity(model)?;
        if let Self::Capacity(compaction) = &policy {
            // An unknown response capacity refuses here, before any turn.
            compaction.input_ceiling(capacity, 0)?;
        }
        capacity.validate()?;
        Ok(policy)
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
