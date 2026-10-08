//! Primitive threshold configuration and provider-owned token accounting.

use super::{CompactionRetention, Context};
use crate::{
    Error, Result,
    model::{ModelContentPart, PreparedModelRequest},
};
use serde::{Deserialize, Serialize};

/// Actual context and output capacities of one immutable selected model.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelContextCapacity {
    /// Maximum tokens in the provider's complete input plus generated output.
    pub context_tokens: u32,
    /// Maximum generated tokens supported by the selected model.
    pub output_tokens: u32,
}

impl ModelContextCapacity {
    /// Rejects impossible provider advertisements.
    pub fn validate(self) -> Result<()> {
        if self.context_tokens == 0
            || self.output_tokens == 0
            || self.output_tokens > self.context_tokens
        {
            return Err(Error::Invalid("selected model capacity is invalid".into()));
        }
        Ok(())
    }
}

/// Provider-owned additive upper bounds for an exact prepared request.
/// Counters must include structured content, native media and provider framing.
/// The SDK supplies no tokenizer or model-name capacity catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelTokenCount {
    /// Binds every count to the exact canonical model request.
    pub request_digest: [u8; 32],
    /// Upper bound for tools, options and request framing independent of messages.
    pub fixed_tokens: u32,
    /// Ordered per-message upper bounds, including message-specific framing.
    /// The provider must make these safe for ordered subsequences of this request.
    pub message_tokens: Vec<u32>,
}

impl ModelTokenCount {
    /// Verifies request identity and dimensions, returning the full input bound.
    pub fn validate(&self, request: &PreparedModelRequest) -> Result<u64> {
        if self.request_digest != request.manifest().request_digest
            || self.message_tokens.len() != request.request().messages.len()
        {
            return Err(Error::Invalid(
                "token count does not bind the prepared request".into(),
            ));
        }
        self.message_tokens
            .iter()
            .try_fold(u64::from(self.fixed_tokens), |total, count| {
                total
                    .checked_add(u64::from(*count))
                    .ok_or_else(|| Error::Invalid("token count exceeds portable arithmetic".into()))
            })
    }
}

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
    /// Returns a finite effective output ceiling when the caller did not supply one.
    pub fn validate(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u32> {
        capacity.validate()?;
        self.retention.validate()?;
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

    /// Selects a complete-message recent suffix. An oversized message is retained
    /// whole; later mandatory/request validation may explicitly reject the budget.
    pub fn recent_start(&self, count: &ModelTokenCount) -> Result<usize> {
        if self.recent_tokens == 0 {
            return Err(Error::Invalid(
                "recent token budget must be positive".into(),
            ));
        }
        let mut recent = 0_u64;
        let mut start = count.message_tokens.len();
        while start > 0 && recent < u64::from(self.recent_tokens) {
            start -= 1;
            let tokens = count
                .message_tokens
                .get(start)
                .ok_or_else(|| Error::Invalid("recent token position is missing".into()))?;
            recent = recent
                .checked_add(u64::from(*tokens))
                .ok_or_else(|| Error::Invalid("recent token count overflows".into()))?;
        }
        Ok(start)
    }

    /// Plans against the shared mandatory-message selection used by replay.
    pub fn projection_budget(
        &self,
        context: &Context,
        count: &ModelTokenCount,
    ) -> Result<(usize, usize)> {
        let mut through = self.recent_start(count)?;
        if count.message_tokens.len() != context.messages.len() || through == 0 {
            return Err(Error::Invalid(
                "no older source fits the compaction policy".into(),
            ));
        }
        let mut pending = std::collections::BTreeMap::new();
        for (position, message) in context.messages.iter().take(through).enumerate() {
            for part in message.content.parts() {
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        pending.insert(call_id, position);
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        pending.remove(call_id);
                    }
                    _ => {}
                }
            }
        }
        if let Some(first) = pending.values().min() {
            through = *first;
        }
        if through == 0 {
            return Err(Error::Invalid(
                "tool exchange leaves no complete older compaction source".into(),
            ));
        }
        let mandatory = super::mandatory_positions(context, through, &self.retention);
        let maximum = mandatory
            .len()
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("compaction projection count overflows".into()))?;
        if maximum > context.messages.len() {
            return Err(Error::Invalid(
                "mandatory content leaves no compaction source".into(),
            ));
        }
        Ok((through, maximum))
    }
}

/// Replaceable primitive policy. Custom context transformations can disable
/// the stock threshold and own their explicit admission/projection decisions.
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
