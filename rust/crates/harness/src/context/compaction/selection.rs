use super::*;

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

pub(in crate::context) fn mandatory_flags<'a>(
    context: &'a Context,
    covered: usize,
    retention: &super::CompactionRetention,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<(Vec<bool>, Vec<(&'a str, usize)>)> {
    let parts = context
        .messages
        .iter()
        .try_fold(0_usize, |count, message| {
            count.checked_add(message.content.parts().len())
        })
        .ok_or_else(|| compaction_error(quota.work(), "context part count overflows"))?;
    let mut pending = quota_vector::<(&str, usize)>(parts, quota)?;
    let mut edges = quota_vector::<(usize, usize)>(parts, quota)?;
    let mut selected = quota_vector::<bool>(context.messages.len(), quota)?;
    for (position, message) in context.messages.iter().enumerate() {
        compaction_step(quota)?;
        selected.push(
            context.current_input_index.map(|index| index as usize) == Some(position)
                || position + 1 == context.messages.len()
                || position >= covered
                || retention.roles.contains(&message.role)
                || (retention.native_media && message.content.contains_native_media()),
        );
        for part in message.content.parts() {
            compaction_step(quota)?;
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
                        let (_, call) = pending.swap_remove(prior);
                        edges.push((call, position));
                    }
                }
                _ => {}
            }
        }
    }
    loop {
        let mut changed = false;
        for &(call, response) in &edges {
            compaction_step(quota)?;
            if selected[call] || selected[response] {
                changed |= !selected[call] || !selected[response];
                selected[call] = true;
                selected[response] = true;
            }
        }
        if !changed {
            break;
        }
    }
    retire_vector(edges, quota)?;
    pending.clear();
    Ok((selected, pending))
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
        let (mut selected, mut pending) = mandatory_flags(
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
        for selected in selected.iter_mut().rev() {
            if count + 1 == maximum {
                break;
            }
            if !*selected {
                *selected = true;
                count += 1;
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
        for (position, message) in context.messages.iter().enumerate() {
            if !selected[position] {
                continue;
            }
            for part in message.content.parts() {
                compaction_step(&mut quota)?;
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        if !pending.iter().any(|(id, _)| *id == call_id) {
                            pending.push((call_id, position));
                        }
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        let prior = pending
                            .iter()
                            .position(|(id, _)| *id == call_id)
                            .ok_or_else(|| {
                                compaction_error(
                                    quota.work(),
                                    "compaction boundary splits a tool exchange",
                                )
                            })?;
                        pending.swap_remove(prior);
                    }
                    _ => {}
                }
            }
            if context.current_input_index.map(|index| index as usize) == Some(position) {
                result.current_input_index =
                    Some(u32::try_from(result.messages.len()).map_err(|_| {
                        compaction_error(quota.work(), "current input index exceeds portable count")
                    })?);
            }
            let slot = std::mem::size_of::<ModelMessage>() as u64;
            if let Some(io) = quota.work().as_deref() {
                io.admit_copy(slot)?;
            }
            let (content, claim) = crate::model::clone_content(&message.content, quota.work())?;
            quota.adopt(claim)?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.copy_bytes(slot)?;
            }
            result.messages.push(ModelMessage {
                role: message.role,
                content,
            });
        }
        if !pending.is_empty() {
            return Err(compaction_error(
                quota.work(),
                "compaction boundary splits a tool exchange",
            ));
        }
        retire_vector(pending, &mut quota)?;
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
