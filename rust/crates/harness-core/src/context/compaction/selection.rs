use super::*;

use super::exchanges::{Group, exchange_groups};

impl CapacityCompaction {
    /// Plans a bounded projection preserving complete tool exchanges.
    pub(crate) fn projection_budget(
        &self,
        context: &Context,
        maximum_messages: Option<usize>,
        requested_cut: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(usize, usize)> {
        self.try_projection_budget(context, maximum_messages, requested_cut, work)?
            .ok_or_else(|| compaction_error(work, "no complete older compaction source"))
    }

    /// No complete evictable prefix is an ordinary speculative miss. Invalid
    /// input and accounting failures remain errors, never silently suppressed.
    pub(crate) fn try_projection_budget(
        &self,
        context: &Context,
        maximum_messages: Option<usize>,
        requested_cut: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<(usize, usize)>> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut through = requested_cut;
        if let Some(maximum) = maximum_messages {
            let recent_messages = maximum.checked_sub(1).ok_or_else(|| {
                compaction_error(
                    quota.work(),
                    "compaction requires a summary message allowance",
                )
            })?;
            through = through.max(context.messages.len().saturating_sub(recent_messages));
        }
        if through == 0 || through >= context.messages.len() {
            return Ok(None);
        }
        let groups = exchange_groups(context, &mut quota)?;
        for group in groups.iter().rev() {
            compaction_step(&mut quota)?;
            if group.size > 0 && group.root < through && through <= group.end {
                through = group.root;
            }
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
            if component.retained_through == position && !selected[group.root] {
                mandatory -= component.size;
            }
            if component.end == position {
                for source in component.sources() {
                    compaction_step(&mut quota)?;
                    if groups[source].retained_through == position && !selected[source] {
                        mandatory -= groups[source].size;
                    }
                }
            }
            if end <= position
                && position + 1 >= through
                && maximum_messages.is_none_or(|maximum| mandatory < maximum)
            {
                cut = Some(position + 1);
                break;
            }
        }
        let Some(through) = cut else { return Ok(None) };
        let maximum = mandatory.checked_add(1).ok_or_else(|| {
            compaction_error(quota.work(), "compaction projection count overflows")
        })?;
        retire_vector(groups, &mut quota)?;
        retire_vector(selected, &mut quota)?;
        if maximum > context.messages.len() || maximum_messages.is_some_and(|limit| maximum > limit)
        {
            return Ok(None);
        }
        Ok(Some((through, maximum)))
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

fn mandatory_selection(
    context: &Context,
    covered: usize,
    groups: &[Group],
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<bool>> {
    let mut selected = quota_vector::<bool>(context.messages.len(), quota)?;
    for (position, message) in context.messages.iter().enumerate() {
        compaction_step(quota)?;
        let mut configuration = false;
        for part in message.content.parts() {
            compaction_step(quota)?;
            configuration |= matches!(part, ModelContentPart::Configuration { .. });
        }
        selected.push(
            context.current_input_index.map(|index| index as usize) == Some(position)
                || position + 1 == context.messages.len()
                || position >= covered
                || message.content.contains_project_instructions()
                || message.role == crate::model::ModelRole::System
                || configuration
                || message.content.contains_native_media(),
        );
    }
    retain_latest_changes(context, &mut selected, quota)?;
    // Components are flattened. First select every selected member's root,
    // then select its other members without retaining unrelated interior text.
    for (position, group) in groups.iter().enumerate() {
        compaction_step(quota)?;
        let keep = selected[position];
        selected[group.root] |= keep;
    }
    for group in groups {
        compaction_step(quota)?;
        if selected[group.root] {
            for source in groups[group.root].sources() {
                compaction_step(quota)?;
                selected[source] = true;
            }
        }
    }
    for (position, group) in groups.iter().enumerate() {
        compaction_step(quota)?;
        let keep = selected[group.root];
        selected[position] |= keep;
    }
    Ok(selected)
}

// Frozen manifests do not contain revisions appended after the epoch started.
// Retain each source's latest notice until a new manifest replaces the epoch.
fn retain_latest_changes(
    context: &Context,
    selected: &mut [bool],
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    let original = crate::wire_codec::BorrowedIndexWork::new(quota.work());
    let mut seen = acyclic_collections::FixedIndex::<(), _, 32>::default();
    for (position, message) in context.messages.iter().enumerate().rev() {
        let Some((notices, _)) = message.content.notice_parts() else {
            continue;
        };
        for notice in notices.iter().rev() {
            if notice.source != "harness" || notice.kind != crate::context::CONTEXT_CHANGED {
                continue;
            }
            let Some(key) = &notice.coalesce_key else {
                selected[position] = true;
                continue;
            };
            let digest = original
                .with(|work| crate::executor::json::canonical_json_digest(key, None, work))?;
            if seen.get(digest, &mut &original)?.is_none() {
                seen.get_or_try_insert_with(digest, &mut &original, |_| Ok(()))?;
                selected[position] = true;
            }
        }
    }
    Ok(())
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

pub(crate) fn mandatory_message_count(
    context: &Context,
    covered: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<usize> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let (selected, groups) = mandatory_flags(context, covered, &mut quota)?;
    let count = selected.iter().filter(|keep| **keep).count();
    retire_vector(groups, &mut quota)?;
    retire_vector(selected, &mut quota)?;
    Ok(count)
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
        let count = selected.iter().filter(|value| **value).count();
        if count >= maximum {
            return Err(compaction_error(
                quota.work(),
                "mandatory context exceeds compaction message budget",
            ));
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
        let mut summary = Some(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::File {
                file,
                policy: crate::model::FileProjectionPolicy::BoundedFull,
            }),
        });
        for (position, (message, selected)) in context.messages.iter().zip(&selected).enumerate() {
            // The summary follows the leading system messages, which are always
            // retained: they stay the request's header, so a compacted request
            // keeps the cached system prefix on every route.
            if message.role != ModelRole::System
                && let Some(summary) = summary.take()
            {
                result.messages.push(summary);
            }
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
        if let Some(summary) = summary {
            result.messages.push(summary);
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
    #[cfg(not(target_arch = "wasm32"))]
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
