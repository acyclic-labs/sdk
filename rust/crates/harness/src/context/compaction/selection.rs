use super::*;

use super::exchanges::{Group, PendingCalls, exchange_groups, track_part};

impl ThresholdCompaction {
    /// Plans a bounded projection preserving complete tool exchanges.
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
        for tokens in count.message_tokens.iter().rev() {
            if recent >= u64::from(self.recent_tokens) {
                break;
            }
            compaction_step(&mut quota)?;
            start -= 1;
            recent = recent
                .checked_add(u64::from(*tokens))
                .ok_or_else(|| compaction_error(quota.work(), "recent token count overflows"))?;
        }
        let mut through = start.max(context.messages.len().saturating_sub(recent_messages));
        if count.message_tokens.len() != context.messages.len() || through == 0 {
            return Err(compaction_error(
                quota.work(),
                "no older source fits the compaction policy",
            ));
        }
        let (groups, pending) = exchange_groups(context, &mut quota)?;
        for group in groups.iter().rev() {
            compaction_step(&mut quota)?;
            if group.size > 0 && group.root < through && through <= group.end {
                through = group.root;
            }
        }
        if through == 0 && maximum_messages == usize::MAX {
            return Err(compaction_error(
                quota.work(),
                "tool exchange leaves no complete older compaction source",
            ));
        }
        if through == 0 || self.mandatory_count(context, through, &mut quota)? >= maximum_messages {
            through =
                self.complete_source(context, through, maximum_messages, &groups, &mut quota)?;
        }
        let maximum = self
            .mandatory_count(context, through, &mut quota)?
            .checked_add(1)
            .ok_or_else(|| {
                compaction_error(quota.work(), "compaction projection count overflows")
            })?;
        retire_vector(pending, &mut quota)?;
        retire_vector(groups, &mut quota)?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(compaction_error(
                quota.work(),
                "mandatory content leaves no compaction source",
            ));
        }
        Ok((through, maximum))
    }
    fn mandatory_count(
        &self,
        context: &Context,
        covered: usize,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<usize> {
        let (flags, pending, groups) = mandatory_flags(context, covered, &self.retention, quota)?;
        let count = flags.iter().filter(|selected| **selected).count();
        retire_vector(pending, quota)?;
        retire_vector(flags, quota)?;
        retire_vector(groups, quota)?;
        Ok(count)
    }

    fn complete_source(
        &self,
        context: &Context,
        through: usize,
        maximum_messages: usize,
        groups: &[Group],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<usize> {
        let mut cuts = quota_vector::<usize>(context.messages.len().saturating_sub(1), quota)?;
        let mut end = 0;
        for (position, group) in groups
            .iter()
            .take(context.messages.len().saturating_sub(1))
            .enumerate()
        {
            compaction_step(quota)?;
            end = end.max(groups[group.root].end);
            if end <= position && position + 1 >= through {
                cuts.push(position + 1);
            }
        }
        let mut lower = 0;
        let mut upper = cuts.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.mandatory_count(
                context,
                *cuts.get(middle).ok_or_else(|| {
                    compaction_error(quota.work(), "compaction source index is invalid")
                })?,
                quota,
            )? >= maximum_messages
            {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let through = *cuts.get(lower).ok_or_else(|| {
            compaction_error(
                quota.work(),
                "mandatory content leaves no compaction source",
            )
        })?;
        retire_vector(cuts, quota)?;
        Ok(through)
    }
}

pub(in crate::context) fn mandatory_flags<'a>(
    context: &'a Context,
    covered: usize,
    retention: &super::CompactionRetention,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<(Vec<bool>, PendingCalls<'a>, Vec<Group>)> {
    let (groups, mut pending) = exchange_groups(context, quota)?;
    let mut selected = quota_vector::<bool>(context.messages.len(), quota)?;
    for (position, message) in context.messages.iter().enumerate() {
        compaction_step(quota)?;
        selected.push(
            context.current_input_index.map(|index| index as usize) == Some(position)
                || position + 1 == context.messages.len()
                || position >= covered
                || message.content.file_parts().any(|(_, policy)| {
                    matches!(
                        policy,
                        crate::model::FileProjectionPolicy::ProjectInstructions(_)
                    )
                })
                || retention.roles.contains(&message.role)
                || (retention.native_media && message.content.contains_native_media()),
        );
    }
    // Components are flattened. First select every selected member's root,
    // then select its other members without retaining unrelated interior text.
    for (position, group) in groups.iter().enumerate() {
        compaction_step(quota)?;
        let keep = selected[position];
        selected[group.root] |= keep;
    }
    for (position, group) in groups.iter().enumerate() {
        compaction_step(quota)?;
        let keep = selected[group.root];
        selected[position] |= keep;
    }
    pending.clear();
    Ok((selected, pending, groups))
}

pub(crate) fn mandatory_messages(
    context: &Context,
    covered: usize,
    retention: &super::CompactionRetention,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<crate::model::ModelMessage>> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut messages = Vec::new();
    let made = (|| {
        let (selected, pending, groups) = mandatory_flags(context, covered, retention, &mut quota)?;
        messages = quota_vector(selected.iter().filter(|keep| **keep).count(), &mut quota)?;
        for (message, keep) in context.messages.iter().zip(&selected) {
            compaction_step(&mut quota)?;
            if *keep {
                messages.push(super::copying::clone_message(
                    message.role,
                    &message.content,
                    &mut quota,
                )?);
            }
        }
        retire_vector(pending, &mut quota)?;
        retire_vector(groups, &mut quota)?;
        retire_vector(selected, &mut quota)
    })();
    if let Err(error) = made {
        drop(messages);
        return Err(error);
    }
    quota.transfer();
    Ok(messages)
}

pub(crate) fn compact_context(
    context: &Context,
    maximum: usize,
    summary: &super::ContextSummary,
    retention: &super::CompactionRetention,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    use crate::model::{ModelContent, ModelMessage, ModelRole};
    if maximum < 2 {
        return Err(compaction_error(
            work,
            "compaction must retain the current input and summary",
        ));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Context::default();
    let made = (|| {
        let (mut selected, mut pending, groups) = mandatory_flags(
            context,
            summary.source_messages as usize,
            retention,
            &mut quota,
        )?;
        let mut count = selected.iter().filter(|value| **value).count();
        if count >= maximum {
            return Err(compaction_error(
                quota.work(),
                "mandatory context exceeds compaction message budget",
            ));
        }
        for group in groups.iter().rev() {
            compaction_step(&mut quota)?;
            let root = group.root;
            let size = groups[root].size;
            if !selected[root] && size < maximum - count {
                selected[root] = true;
                count += size;
            }
        }
        for (position, group) in groups.iter().enumerate() {
            compaction_step(&mut quota)?;
            let keep = selected[group.root];
            selected[position] |= keep;
        }
        result.messages = quota_vector::<ModelMessage>(count + 1, &mut quota)?;
        let file = if quota.work().is_some() {
            summary.output.clone_in_quota(&mut quota)?
        } else {
            summary.output.clone()
        };
        result.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::File {
                file,
                policy: crate::model::FileProjectionPolicy::BoundedFull,
            }),
        });
        for (position, (message, selected)) in context.messages.iter().zip(&selected).enumerate() {
            if !*selected {
                continue;
            }
            for part in message.content.parts() {
                compaction_step(&mut quota)?;
                let call = track_part(part, position, &mut pending);
                if matches!(part, ModelContentPart::ToolResult { .. }) && call.is_none() {
                    return Err(compaction_error(
                        quota.work(),
                        "compaction boundary splits a tool exchange",
                    ));
                }
            }
            if context.current_input_index.map(|index| index as usize) == Some(position) {
                result.current_input_index =
                    Some(u32::try_from(result.messages.len()).map_err(|_| {
                        compaction_error(quota.work(), "current input index exceeds portable count")
                    })?);
            }
            result.messages.push(super::copying::clone_message(
                message.role,
                &message.content,
                &mut quota,
            )?);
        }
        if !pending.is_empty() {
            return Err(compaction_error(
                quota.work(),
                "compaction boundary splits a tool exchange",
            ));
        }
        retire_vector(pending, &mut quota)?;
        retire_vector(groups, &mut quota)?;
        retire_vector(selected, &mut quota)?;
        let (metadata, claim) = clone_metadata(&context.metadata, quota.work())?;
        result.metadata = metadata;
        quota.adopt(claim)?;
        Ok(())
    })();
    if let Err(error) = made {
        drop(result);
        return Err(error);
    }
    let claim = if quota.work().is_some() {
        quota.retained()
    } else {
        0
    };
    quota.transfer();
    Ok((result, claim))
}

pub(super) fn compaction_step(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    if let Some(io) = quota.work().as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

pub(super) fn compaction_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}
