use super::*;

use super::exchanges::{Group, exchange_groups, mandatory_selection};

impl ThresholdCompaction {
    /// Plans a bounded projection preserving complete tool exchanges.
    pub(crate) fn projection_budget(
        &self,
        context: &Context,
        count: &ModelTokenCount,
        maximum_messages: usize,
        recent_tokens: u32,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(usize, usize)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let recent_messages = maximum_messages.checked_sub(1).ok_or_else(|| {
            compaction_error(
                quota.work(),
                "compaction requires a summary message allowance",
            )
        })?;
        if recent_tokens == 0 {
            return Err(compaction_error(
                quota.work(),
                "recent token budget must be positive",
            ));
        }
        let mut recent = 0_u64;
        let mut start = count.message_tokens.len();
        for tokens in count.message_tokens.iter().rev() {
            if recent >= u64::from(recent_tokens) {
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
        let groups = exchange_groups(context, &mut quota)?;
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
        let selected = mandatory_selection(context, context.messages.len(), &groups, &mut quota)?;
        let mut mandatory = context.messages.len();
        let mut end = 0;
        let mut cut = None;
        // At a complete boundary, each component is entirely before or after
        // the cut. Retire its size once, at its last member, unless it contains
        // irreducible content. No candidate needs another dependency walk.
        for (position, group) in groups
            .iter()
            .take(context.messages.len().saturating_sub(1))
            .enumerate()
        {
            compaction_step(&mut quota)?;
            let component = &groups[group.root];
            end = end.max(component.end);
            if component.end == position && !selected[group.root] {
                mandatory -= component.size;
            }
            if end <= position && position + 1 >= through && mandatory < maximum_messages {
                cut = Some(position + 1);
                break;
            }
        }
        let through = cut.ok_or_else(|| {
            compaction_error(
                quota.work(),
                "mandatory content leaves no compaction source",
            )
        })?;
        let maximum = mandatory.checked_add(1).ok_or_else(|| {
            compaction_error(quota.work(), "compaction projection count overflows")
        })?;
        retire_vector(groups, &mut quota)?;
        retire_vector(selected, &mut quota)?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(compaction_error(
                quota.work(),
                "mandatory content leaves no compaction source",
            ));
        }
        Ok((through, maximum))
    }
}

pub(in crate::context) fn mandatory_flags(
    context: &Context,
    covered: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<(Vec<bool>, Vec<Group>)> {
    let groups = exchange_groups(context, quota)?;
    let selected = mandatory_selection(context, covered, &groups, quota)?;
    Ok((selected, groups))
}

pub(crate) fn mandatory_messages(
    context: &Context,
    covered: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<crate::model::ModelMessage>> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut messages = Vec::new();
    let made = (|| {
        let (selected, groups) = mandatory_flags(context, covered, &mut quota)?;
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
        let (mut selected, groups) =
            mandatory_flags(context, summary.source_messages as usize, &mut quota)?;
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
            if keep && groups[group.root].end == context.messages.len() {
                return Err(compaction_error(
                    quota.work(),
                    "compaction boundary splits a tool exchange",
                ));
            }
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
