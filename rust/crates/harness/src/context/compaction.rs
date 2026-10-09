//! Primitive threshold compaction using provider-owned token accounting.

use super::{CompactionRetention, Context, ModelContextCapacity, ModelTokenCount};
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
        self.projection_budget_with_message_limit(context, count, usize::MAX)
    }

    /// Applies a hard retained-message allowance alongside the recent token budget.
    /// The summary occupies one position. Mandatory messages and complete tool
    /// exchanges remain verbatim; an impossible allowance fails explicitly.
    pub fn projection_budget_with_message_limit(
        &self,
        context: &Context,
        count: &ModelTokenCount,
        maximum_messages: usize,
    ) -> Result<(usize, usize)> {
        let recent_messages = maximum_messages.checked_sub(1).ok_or_else(|| {
            Error::Invalid("compaction requires a summary message allowance".into())
        })?;
        let mut through = self
            .recent_start(count)?
            .max(context.messages.len().saturating_sub(recent_messages));
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
        if through == 0 && maximum_messages == usize::MAX {
            return Err(Error::Invalid(
                "tool exchange leaves no complete older compaction source".into(),
            ));
        }
        if through == 0
            || super::mandatory_positions(context, through, &self.retention).len()
                >= maximum_messages
        {
            through = bounded_source_cut(context, through, &self.retention, maximum_messages)?;
        }
        let mandatory = super::mandatory_positions(context, through, &self.retention);
        let maximum = mandatory
            .len()
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("compaction projection count overflows".into()))?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(Error::Invalid(
                "mandatory content leaves no compaction source".into(),
            ));
        }
        Ok((through, maximum))
    }
}

// Mandatory closure shrinks monotonically as the covered prefix advances.
// Enumerate complete exchange boundaries once, then search them without a
// quadratic scan over all possible message cuts. The current final message
// remains outside the summary source.
fn bounded_source_cut(
    context: &Context,
    minimum: usize,
    retention: &CompactionRetention,
    maximum_messages: usize,
) -> Result<usize> {
    let mut pending = std::collections::BTreeSet::new();
    let mut cuts = Vec::new();
    for (position, message) in context
        .messages
        .iter()
        .take(context.messages.len().saturating_sub(1))
        .enumerate()
    {
        for part in message.content.parts() {
            match part {
                ModelContentPart::ToolCall { call_id, .. } => {
                    pending.insert(call_id);
                }
                ModelContentPart::ToolResult { call_id, .. } => {
                    pending.remove(call_id);
                }
                _ => {}
            }
        }
        if pending.is_empty() && position + 1 >= minimum {
            cuts.push(position + 1);
        }
    }
    let position = cuts.partition_point(|cut| {
        super::mandatory_positions(context, *cut, retention).len() >= maximum_messages
    });
    cuts.get(position)
        .copied()
        .ok_or_else(|| Error::Invalid("mandatory content leaves no compaction source".into()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelContent, ModelMessage, ModelRole, ToolResultContent};

    #[test]
    fn count_pressure_preserves_instructions_and_complete_exchanges() -> Result<()> {
        let mut context = Context {
            messages: (0..8)
                .map(|index| ModelMessage {
                    role: if index == 0 {
                        ModelRole::System
                    } else {
                        ModelRole::User
                    },
                    content: ModelContent::Text(format!("message {index}")),
                })
                .collect(),
            current_input_index: Some(7),
            ..Context::default()
        };
        let count = ModelTokenCount {
            request_digest: [0; 32],
            fixed_tokens: 1,
            message_tokens: vec![1; 8],
        };
        let policy = ThresholdCompaction::default();
        assert_eq!(
            policy.projection_budget_with_message_limit(&context, &count, 6)?,
            (4, 6)
        );
        context.messages[3] = ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall {
                call_id: "call".into(),
                name: "tool".into(),
                arguments: serde_json::Value::Null,
            }),
        };
        context.messages[4] = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "call".into(),
                name: "tool".into(),
                content: ToolResultContent::Json {
                    value: serde_json::Value::Null,
                },
            }),
        };
        assert_eq!(
            policy.projection_budget_with_message_limit(&context, &count, 6)?,
            (5, 5)
        );
        assert!(
            policy
                .projection_budget_with_message_limit(&context, &count, 2)
                .is_err()
        );
        // Explicit token-only callers retain their original no-pressure behavior.
        assert!(policy.projection_budget(&context, &count).is_err());
        Ok(())
    }
}
