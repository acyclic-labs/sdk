use super::*;

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

    /// Plans against the shared mandatory-message selection used by replay.
    pub(crate) fn projection_budget(
        &self,
        context: &Context,
        count: &ModelTokenCount,
        maximum_messages: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(usize, usize)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let recent_messages = maximum_messages.checked_sub(1).ok_or_else(|| {
            compaction_error(
                quota.work(),
                "compaction requires a summary message allowance",
            )
        })?;
        if self.recent_tokens == 0 {
            return Err(compaction_error(
                quota.work(),
                "recent token budget must be positive",
            ));
        }
        let mut recent = 0_u64;
        let mut start = count.message_tokens.len();
        while start > 0 && recent < u64::from(self.recent_tokens) {
            compaction_step(&mut quota)?;
            start -= 1;
            recent = recent
                .checked_add(u64::from(count.message_tokens[start]))
                .ok_or_else(|| compaction_error(quota.work(), "recent token count overflows"))?;
        }
        let mut through = start.max(context.messages.len().saturating_sub(recent_messages));
        if count.message_tokens.len() != context.messages.len() || through == 0 {
            return Err(compaction_error(
                quota.work(),
                "no older source fits the compaction policy",
            ));
        }
        let parts = context
            .messages
            .iter()
            .try_fold(0_usize, |count, message| {
                count.checked_add(message.content.parts().len())
            })
            .ok_or_else(|| compaction_error(quota.work(), "context part count overflows"))?;
        let mut pending = quota_vector::<(&str, usize)>(parts, &mut quota)?;
        for (position, message) in context.messages.iter().take(through).enumerate() {
            compaction_step(&mut quota)?;
            for part in message.content.parts() {
                compaction_step(&mut quota)?;
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) {
                            prior.1 = position;
                        } else {
                            pending.push((call_id, position));
                        }
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) {
                            pending.swap_remove(prior);
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(first) = pending.iter().map(|(_, position)| *position).min() {
            through = first;
        }
        if through == 0 && maximum_messages == usize::MAX {
            return Err(compaction_error(
                quota.work(),
                "tool exchange leaves no complete older compaction source",
            ));
        }
        let mandatory_count = |covered, quota: &mut crate::wire_codec::CodecQuota<'_, '_>| {
            let (flags, pending) = mandatory_flags(context, covered, &self.retention, quota)?;
            let count = flags.iter().filter(|selected| **selected).count();
            retire_vector(pending, quota)?;
            retire_vector(flags, quota)?;
            Ok::<_, Error>(count)
        };
        if through == 0 || mandatory_count(through, &mut quota)? >= maximum_messages {
            pending.clear();
            let mut cuts =
                quota_vector::<usize>(context.messages.len().saturating_sub(1), &mut quota)?;
            for (position, message) in context
                .messages
                .iter()
                .take(context.messages.len().saturating_sub(1))
                .enumerate()
            {
                compaction_step(&mut quota)?;
                for part in message.content.parts() {
                    compaction_step(&mut quota)?;
                    match part {
                        ModelContentPart::ToolCall { call_id, .. } => {
                            if !pending.iter().any(|(id, _)| *id == call_id) {
                                pending.push((call_id, position));
                            }
                        }
                        ModelContentPart::ToolResult { call_id, .. } => {
                            if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) {
                                pending.swap_remove(prior);
                            }
                        }
                        _ => {}
                    }
                }
                if pending.is_empty() && position + 1 >= through {
                    cuts.push(position + 1);
                }
            }
            let mut lower = 0;
            let mut upper = cuts.len();
            while lower < upper {
                let middle = lower + (upper - lower) / 2;
                if mandatory_count(cuts[middle], &mut quota)? >= maximum_messages {
                    lower = middle + 1;
                } else {
                    upper = middle;
                }
            }
            through = *cuts.get(lower).ok_or_else(|| {
                compaction_error(
                    quota.work(),
                    "mandatory content leaves no compaction source",
                )
            })?;
            retire_vector(cuts, &mut quota)?;
        }
        let maximum = mandatory_count(through, &mut quota)?
            .checked_add(1)
            .ok_or_else(|| {
                compaction_error(quota.work(), "compaction projection count overflows")
            })?;
        retire_vector(pending, &mut quota)?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(compaction_error(
                quota.work(),
                "mandatory content leaves no compaction source",
            ));
        }
        Ok((through, maximum))
    }
}

/// Replaceable primitive policy.
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
