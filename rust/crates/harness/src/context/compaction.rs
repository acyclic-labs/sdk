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
        &self, context: &Context, count: &ModelTokenCount, maximum_messages: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(usize, usize)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let recent_messages = maximum_messages.checked_sub(1)
            .ok_or_else(|| compaction_error(quota.work(), "compaction requires a summary message allowance"))?;
        if self.recent_tokens == 0 {
            return Err(compaction_error(quota.work(), "recent token budget must be positive"));
        }
        let mut recent = 0_u64;
        let mut start = count.message_tokens.len();
        while start > 0 && recent < u64::from(self.recent_tokens) {
            compaction_step(&mut quota)?;
            start -= 1;
            recent = recent.checked_add(u64::from(count.message_tokens[start]))
                .ok_or_else(|| compaction_error(quota.work(), "recent token count overflows"))?;
        }
        let mut through = start.max(context.messages.len().saturating_sub(recent_messages));
        if count.message_tokens.len() != context.messages.len() || through == 0 {
            return Err(compaction_error(quota.work(), "no older source fits the compaction policy"));
        }
        let parts = context.messages.iter().try_fold(0_usize, |count, message|
            count.checked_add(message.content.parts().len()))
            .ok_or_else(|| compaction_error(quota.work(), "context part count overflows"))?;
        let mut pending = quota_vector::<(&str, usize)>(parts, &mut quota)?;
        for (position, message) in context.messages.iter().take(through).enumerate() {
            compaction_step(&mut quota)?;
            for part in message.content.parts() {
                compaction_step(&mut quota)?;
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) { prior.1 = position; }
                        else { pending.push((call_id, position)); }
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) { pending.swap_remove(prior); }
                    }
                    _ => {}
                }
            }
        }
        if let Some(first) = pending.iter().map(|(_, position)| *position).min() { through = first; }
        if through == 0 && maximum_messages == usize::MAX {
            return Err(compaction_error(quota.work(), "tool exchange leaves no complete older compaction source"));
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
            let mut cuts = quota_vector::<usize>(context.messages.len().saturating_sub(1), &mut quota)?;
            for (position, message) in context.messages.iter().take(context.messages.len().saturating_sub(1)).enumerate() {
                compaction_step(&mut quota)?;
                for part in message.content.parts() {
                    compaction_step(&mut quota)?;
                    match part {
                        ModelContentPart::ToolCall { call_id, .. } => {
                            if !pending.iter().any(|(id, _)| *id == call_id) { pending.push((call_id, position)); }
                        }
                        ModelContentPart::ToolResult { call_id, .. } => {
                            if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) { pending.swap_remove(prior); }
                        }
                        _ => {}
                    }
                }
                if pending.is_empty() && position + 1 >= through { cuts.push(position + 1); }
            }
            let mut lower = 0;
            let mut upper = cuts.len();
            while lower < upper {
                let middle = lower + (upper - lower) / 2;
                if mandatory_count(cuts[middle], &mut quota)? >= maximum_messages { lower = middle + 1; }
                else { upper = middle; }
            }
            through = *cuts.get(lower).ok_or_else(|| compaction_error(quota.work(),
                "mandatory content leaves no compaction source"))?;
            retire_vector(cuts, &mut quota)?;
        }
        let maximum = mandatory_count(through, &mut quota)?.checked_add(1)
            .ok_or_else(|| compaction_error(quota.work(), "compaction projection count overflows"))?;
        retire_vector(pending, &mut quota)?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(compaction_error(quota.work(), "mandatory content leaves no compaction source"));
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

pub(crate) fn clone_context(
    context: &Context, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    let (mut copy, claim) = clone_context_prefix(context, context.messages.len(), work)?;
    copy.current_input_index = context.current_input_index;
    Ok((copy, claim))
}

pub(crate) fn clone_context_prefix(
    context: &Context, through: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    let source = context.messages.get(..through)
        .ok_or_else(|| Error::Invalid("summary extent exceeds source projection".into()))?;
    if work.is_none() {
        return Ok((Context { messages: source.to_vec(), metadata: context.metadata.clone(),
            current_input_index: context.current_input_index.filter(|index| (*index as usize) < through) }, 0));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Context::default();
    let copied = (|| {
        let (messages, claim) = clone_messages(source, quota.work())?;
        result.messages = messages;
        quota.adopt(claim)?;
        let (metadata, claim) = clone_metadata(&context.metadata, quota.work())?;
        result.metadata = metadata;
        quota.adopt(claim)?;
        result.current_input_index = context.current_input_index.filter(|index| (*index as usize) < through);
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_metadata(
    metadata: &std::collections::BTreeMap<String, crate::conversation::FileRef>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(std::collections::BTreeMap<String, crate::conversation::FileRef>, u64)> {
    if work.is_none() { return Ok((metadata.clone(), 0)); }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = std::collections::BTreeMap::new();
    let copied = (|| {
        for (key, file) in metadata {
            let key = quota.work().as_deref_mut().expect("observed metadata").clone_text(key)?;
            quota.adopt(key.capacity() as u64)?;
            let file = file.clone_in_quota(&mut quota)?;
            let bytes = std::mem::size_of::<(String, crate::conversation::FileRef)>() as u64;
            let io = quota.work().as_deref_mut().expect("observed metadata");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            result.insert(key, file);
        }
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_messages(
    messages: &[crate::model::ModelMessage],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Vec<crate::model::ModelMessage>, u64)> {
    clone_base_messages(messages, None, &[], work)
}

pub(crate) fn clone_base_messages(
    first: &[crate::model::ModelMessage], user: Option<&crate::model::ModelContent>,
    second: &[crate::model::ModelMessage],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Vec<crate::model::ModelMessage>, u64)> {
    use crate::model::{ModelMessage, ModelRole};
    if work.is_none() {
        let mut result = first.to_vec();
        if let Some(content) = user { result.push(ModelMessage { role: ModelRole::User, content: content.clone() }); }
        result.extend_from_slice(second);
        return Ok((result, 0));
    }
    let count = first.len().checked_add(second.len()).and_then(|n| n.checked_add(usize::from(user.is_some())))
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))?;
    let bytes = count.checked_mul(std::mem::size_of::<ModelMessage>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))? as u64;
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Vec::new();
    let copied = (|| {
        let io = quota.work().as_deref_mut().expect("observed context");
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
        result.try_reserve_exact(count).map_err(|cause|
            quota.work().as_deref().expect("observed context").allocation_failure(cause))?;
        let actual = (result.capacity() * std::mem::size_of::<ModelMessage>()) as u64;
        if actual > bytes {
            let io = quota.work().as_deref_mut().expect("observed context");
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        for (role, content) in first.iter().map(|m| (m.role, &m.content))
            .chain(user.into_iter().map(|content| (ModelRole::User, content)))
            .chain(second.iter().map(|m| (m.role, &m.content))) {
            let slot = std::mem::size_of::<ModelMessage>() as u64;
            quota.work().as_deref().expect("observed context").admit_copy(slot)?;
            let (content, claim) = crate::model::clone_content(content, quota.work())?;
            quota.adopt(claim)?;
            quota.work().as_deref_mut().expect("observed context").copy_bytes(slot)?;
            result.push(ModelMessage { role, content });
        }
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

fn quota_vector<T>(capacity: usize, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<Vec<T>> {
    let mut values = Vec::new();
    reserve_vector_with_work(&mut values, capacity, quota.work())?;
    quota.adopt((values.capacity() * std::mem::size_of::<T>()) as u64)?;
    Ok(values)
}

fn retire_vector<T>(values: Vec<T>, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    let bytes = (values.capacity() * std::mem::size_of::<T>()) as u64;
    drop(values);
    quota.relinquish(bytes)
}

pub(super) fn mandatory_flags<'a>(
    context: &'a Context, covered: usize, retention: &super::CompactionRetention,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<(Vec<bool>, Vec<(&'a str, usize)>)> {
    let parts = context.messages.iter().try_fold(0_usize, |count, message|
        count.checked_add(message.content.parts().len()))
        .ok_or_else(|| compaction_error(quota.work(), "context part count overflows"))?;
    let mut pending = quota_vector::<(&str, usize)>(parts, quota)?;
    let mut edges = quota_vector::<(usize, usize)>(parts, quota)?;
    let mut selected = quota_vector::<bool>(context.messages.len(), quota)?;
    for (position, message) in context.messages.iter().enumerate() {
        compaction_step(quota)?;
        selected.push(context.current_input_index.map(|index| index as usize) == Some(position)
            || position + 1 == context.messages.len()
            || position >= covered
            || retention.roles.contains(&message.role)
            || (retention.native_media && message.content.contains_native_media()));
        for part in message.content.parts() {
            compaction_step(quota)?;
            match part {
                ModelContentPart::ToolCall { call_id, .. } => {
                    if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) {
                        prior.1 = position;
                    } else { pending.push((call_id, position)); }
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
        if !changed { break; }
    }
    retire_vector(edges, quota)?;
    pending.clear();
    Ok((selected, pending))
}

pub(crate) fn compact_context(
    context: &Context, maximum: usize, summary: &super::ContextSummary,
    retention: &super::CompactionRetention,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    use crate::model::{ModelContent, ModelMessage, ModelRole};
    if maximum < 2 {
        return Err(compaction_error(work, "compaction must retain the current input and summary"));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Context::default();
    let made = (|| {
        let (mut selected, mut pending) = mandatory_flags(
            context, summary.source_messages as usize, retention, &mut quota)?;
        let mut count = selected.iter().filter(|value| **value).count();
        if count >= maximum {
            return Err(compaction_error(quota.work(), "mandatory context exceeds compaction message budget"));
        }
        for selected in selected.iter_mut().rev() {
            if count + 1 == maximum { break; }
            if !*selected { *selected = true; count += 1; }
        }
        result.messages = quota_vector::<ModelMessage>(count + 1, &mut quota)?;
        let file = if quota.work().is_some() {
            summary.output.clone_in_quota(&mut quota)?
        } else { summary.output.clone() };
        result.messages.push(ModelMessage { role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::File {
                file, policy: crate::model::FileProjectionPolicy::BoundedFull,
            }) });
        for (position, message) in context.messages.iter().enumerate() {
            if !selected[position] { continue; }
            for part in message.content.parts() {
                compaction_step(&mut quota)?;
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        if !pending.iter().any(|(id, _)| *id == call_id) { pending.push((call_id, position)); }
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        let prior = pending.iter().position(|(id, _)| *id == call_id)
                            .ok_or_else(|| compaction_error(quota.work(), "compaction boundary splits a tool exchange"))?;
                        pending.swap_remove(prior);
                    }
                    _ => {}
                }
            }
            if context.current_input_index.map(|index| index as usize) == Some(position) {
                result.current_input_index = Some(u32::try_from(result.messages.len())
                    .map_err(|_| compaction_error(quota.work(), "current input index exceeds portable count"))?);
            }
            let slot = std::mem::size_of::<ModelMessage>() as u64;
            if let Some(io) = quota.work().as_deref() { io.admit_copy(slot)?; }
            let (content, claim) = crate::model::clone_content(&message.content, quota.work())?;
            quota.adopt(claim)?;
            if let Some(io) = quota.work().as_deref_mut() { io.copy_bytes(slot)?; }
            result.messages.push(ModelMessage { role: message.role, content });
        }
        if !pending.is_empty() {
            return Err(compaction_error(quota.work(), "compaction boundary splits a tool exchange"));
        }
        retire_vector(pending, &mut quota)?;
        retire_vector(selected, &mut quota)?;
        let (metadata, claim) = clone_metadata(&context.metadata, quota.work())?;
        result.metadata = metadata;
        quota.adopt(claim)?;
        Ok(())
    })();
    if let Err(error) = made { drop(result); return Err(error); }
    let claim = if quota.work().is_some() { quota.retained() } else { 0 };
    quota.transfer();
    Ok((result, claim))
}

fn compaction_step(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    if let Some(io) = quota.work().as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn compaction_error(work: &Option<&mut dyn crate::store::history::HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, reason);
    }
    let _ = work;
    Error::Invalid(reason.into())
}

pub(crate) fn reserve_vector_with_work<T>(
    values: &mut Vec<T>, capacity: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()); }
    let Some(io) = work.as_deref_mut() else {
        values.reserve(capacity.saturating_sub(values.len()));
        return Ok(());
    };
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))? as u64;
    let old = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let copied = (values.len() * std::mem::size_of::<T>()) as u64;
    io.admit_copy(copied)?;
    io.admit_allocation(bytes)?;
    let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
    quota.work().as_deref_mut().expect("observed vector").copy_bytes(copied)?;
    if let Err(cause) = values.try_reserve_exact(capacity - values.len()) {
        return Err(quota.work().as_deref().expect("observed vector").allocation_failure(cause));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let reconciled = (|| {
        if actual > bytes {
            let io = quota.work().as_deref_mut().expect("observed vector");
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        quota.work().as_deref_mut().expect("observed vector").release_owned(old)?;
        Ok(())
    })();
    if let Err(error) = reconciled {
        drop(std::mem::take(values));
        return Err(error);
    }
    quota.transfer();
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use crate::model::ModelMessage;
    use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
    use std::mem::size_of;

    fn overflow() -> Error {
        Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "context clone storage overflows" }
    }

    pub(crate) fn native_vector<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<(Vec<T>, NativeAllocation)> {
        let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64;
        let mut guard = work.retain_owned(bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if bytes != 0 {
            work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
        })?;
        guard.resize_retained(values.capacity().checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok((values, guard))
    }

    pub(crate) fn clone_text_native(text: &str, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation)> {
        work.admit(WorkCounters { bytes_copied: text.len() as u64,
            allocation_operations: u64::from(!text.is_empty()), ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut guard = work.retain_owned(text.len() as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        work.charge(WorkCounters { allocation_operations: u64::from(!text.is_empty()),
            ..WorkCounters::default() }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut copy = String::new();
        copy.try_reserve_exact(text.len()).map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
        })?;
        guard.resize_retained(copy.capacity() as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        work.charge(WorkCounters { bytes_copied: text.len() as u64, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        copy.push_str(text);
        Ok((copy, guard))
    }

    pub(crate) fn mandatory_positions_native(
        context: &Context, covered: usize, retention: &CompactionRetention, work: &NativeMaterializationWork,
    ) -> Result<(Vec<usize>, NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let (selected, pending) = mandatory_flags(context, covered, retention, &mut quota)?;
        retire_vector(pending, &mut quota)?;
        let mut positions = quota_vector(selected.iter().filter(|selected| **selected).count(), &mut quota)?;
        positions.extend(selected.iter().enumerate().filter_map(|(position, selected)| selected.then_some(position)));
        retire_vector(selected, &mut quota)?;
        quota.transfer();
        drop(quota);
        drop(observed);
        Ok((positions, io.into_custody()))
    }

    pub(crate) fn clone_messages_native(messages: &[ModelMessage], work: &NativeMaterializationWork) -> Result<(Vec<ModelMessage>, NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work).map_err(super::super::native_context_work_error)?;
        let (copy, _) = clone_messages(messages, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_context_native(context: &Context, work: &NativeMaterializationWork) -> Result<(Context, NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work).map_err(super::super::native_context_work_error)?;
        let (copy, _) = clone_context(context, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_context_prefix_native(
        context: &Context, through: usize, work: &NativeMaterializationWork,
    ) -> Result<(Context, NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work).map_err(super::super::native_context_work_error)?;
        let (copy, _) = clone_context_prefix(context, through, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_retention_native(
        retention: &CompactionRetention, work: &NativeMaterializationWork,
    ) -> Result<(CompactionRetention, NativeAllocation)> {
        let copied = retention.roles.len().checked_mul(size_of::<crate::model::ModelRole>()).ok_or_else(overflow)? as u64;
        work.admit(WorkCounters { bytes_copied: copied,
            allocation_operations: u64::from(!retention.roles.is_empty()), ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut roles = native_vector::<crate::model::ModelRole>(retention.roles.len(), work)?;
        work.charge(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::default()
        }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        roles.0.extend_from_slice(&retention.roles);
        Ok((CompactionRetention { roles: roles.0, native_media: retention.native_media }, roles.1))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::{clone_context_native, clone_context_prefix_native, clone_messages_native, clone_retention_native, clone_text_native, mandatory_positions_native};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelContent, ModelMessage, ModelRole, ToolResultContent};

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_metadata_depth_denial_precedes_output_tree_allocation() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 1024, peak_allocation_bytes: 1_048_576,
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576, items_examined: 4096,
            ..WorkBudget::default()
        };
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let mut source = serde_json::Value::Null;
        for _ in 0..257 { source = serde_json::Value::Array(vec![source]); }
        let before = work.receipt();
        assert!(super::super::context_contract_native(&source, &work).is_err());
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        let control = NativeMaterializationWork::new(budget, &cancellation).expect("control ledger");
        let source = serde_json::json!({ "source": ["bounded"] });
        let (actual, custody) = super::super::context_contract_native(&source, &control).expect("bounded actual metadata");
        assert_eq!(actual, source);
        assert!(custody.work().shares_ledger(&control));
        assert!(custody.bytes() > 0);
        drop(actual);
        drop(custody);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn observed_compaction_matches_ordinary_exchange_and_input_selection() -> Result<()> {
        use crate::conversation::{FileDescriptor, FileRef, ProviderRef, VolumeClass, VolumeOwner, VolumeRef};
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let file = FileRef::new(VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?, "context", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])))?,
            "context/summary.txt", "pinned", FileDescriptor::from_bytes(b"summary", "text/plain")?, "summary.txt")?;
        let context = Context {
            messages: vec![
                ModelMessage { role: ModelRole::User, content: ModelContent::Text("old".into()) },
                ModelMessage { role: ModelRole::Assistant, content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call".into(), name: "tool".into(), arguments: serde_json::Value::Null,
                }) },
                ModelMessage { role: ModelRole::Tool, content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call".into(), name: "tool".into(),
                    content: ToolResultContent::Json { value: serde_json::Value::Null },
                }) },
                ModelMessage { role: ModelRole::User, content: ModelContent::Text("current".into()) },
            ], current_input_index: Some(3), ..Context::default()
        };
        let summary = super::super::ContextSummary { operation_id: crate::OperationId::new(), step: 0,
            source_messages: 3, source_digest: [0; 32], output: file };
        let retention = CompactionRetention { roles: vec![], native_media: false };
        let expected = compact_context(&context, 4, &summary, &retention, &mut None)?.0;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 1024, peak_allocation_bytes: 1_048_576,
            bytes_copied: 1_048_576, items_examined: 1024,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        let (actual, claim) = compact_context(&context, 4, &summary, &retention,
            &mut Some(&mut observer))?;
        let custody = observer.into_custody();
        assert_eq!(actual, expected);
        assert_eq!(claim, custody.bytes());
        assert!(custody.work().shares_ledger(&work));
        drop(actual);
        drop(custody);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn partial_field_copy_releases_original_custody_and_cancelled_copy_is_empty() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 3,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4096,
            items_examined: 128,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let messages = vec![
            ModelMessage { role: ModelRole::User, content: ModelContent::Text("first".into()) },
            ModelMessage { role: ModelRole::User, content: ModelContent::Text("second".into()) },
        ];
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        assert!(clone_messages(&messages, &mut Some(&mut observer)).is_err());
        assert!(work.receipt().bytes_copied >= 5);
        let custody = observer.into_custody();
        assert_eq!(custody.bytes(), 0, "failed partial payload drops before its quota releases");
        drop(custody);
        cancellation.cancel();
        let before = work.receipt();
        assert!(native::clone_messages_native(&messages, &work).is_err());
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    }

    #[test]
    fn summary_prefix_preserves_ordinary_input_selection() -> Result<()> {
        let mut context = Context {
            messages: vec![
                ModelMessage { role: ModelRole::User, content: ModelContent::Text("first".into()) },
                ModelMessage { role: ModelRole::Assistant, content: ModelContent::Parts(vec![
                    ModelContentPart::Text { text: "second".into() },
                ]) },
            ],
            current_input_index: Some(1),
            ..Context::default()
        };
        let (prefix, claim) = clone_context_prefix(&context, 1, &mut None)?;
        assert_eq!(prefix.messages, context.messages[..1]);
        assert_eq!(prefix.current_input_index, None);
        assert_eq!(claim, 0);
        context.current_input_index = Some(0);
        assert_eq!(clone_context_prefix(&context, 1, &mut None)?.0.current_input_index, Some(0));
        assert!(clone_context_prefix(&context, 3, &mut None).is_err());
        Ok(())
    }

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
            policy.projection_budget(&context, &count, 6, &mut None)?,
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
            policy.projection_budget(&context, &count, 6, &mut None)?,
            (5, 5)
        );
        assert!(
            policy
                .projection_budget(&context, &count, 2, &mut None)
                .is_err()
        );
        // Explicit token-only callers retain their original no-pressure behavior.
        assert!(policy.projection_budget(&context, &count, usize::MAX, &mut None).is_err());
        Ok(())
    }
}
