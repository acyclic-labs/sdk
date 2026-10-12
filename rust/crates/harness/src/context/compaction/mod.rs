//! Primitive threshold compaction using provider-owned token accounting.

use super::{CompactionRetention, Context, ModelContextCapacity, ModelTokenCount};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

mod policy;
pub use policy::*;
mod copying;
pub(crate) use copying::{
    clone_context, clone_context_prefix, clone_messages, clone_metadata, reserve_vector,
};
use copying::{quota_vector, retire_vector};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::clone_retention_native;
#[cfg(test)]
mod tests;

pub(super) fn mandatory_flags<'a>(
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

fn compaction_step(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    if let Some(io) = quota.work().as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn compaction_error(
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
