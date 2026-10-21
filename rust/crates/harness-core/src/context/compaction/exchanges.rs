use super::{
    Context, ModelContentPart, quota_vector,
    selection::{compaction_error, compaction_step},
};
use crate::{Result, model::ModelRole};

pub(super) type PendingCalls<'a> = Vec<(&'a str, usize)>;

/// A message's dependency component. Only roots own the size and final endpoint.
#[derive(Clone, Copy)]
pub(super) struct Group {
    pub root: usize,
    pub end: usize,
    pub size: usize,
}

/// Native continuity, its assistant calls and their results share a component.
/// Plain text between them remains independently selectable.
pub(super) fn exchange_groups<'a>(
    context: &'a Context,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<(Vec<Group>, PendingCalls<'a>)> {
    let parts = context
        .messages
        .iter()
        .try_fold(0usize, |count, message| {
            count.checked_add(message.content.parts().len())
        })
        .ok_or_else(|| compaction_error(quota.work(), "context part count overflows"))?;
    let mut pending = quota_vector::<(&str, usize)>(parts, quota)?;
    let mut groups = quota_vector::<Group>(context.messages.len(), quota)?;
    for position in 0..context.messages.len() {
        compaction_step(quota)?;
        groups.push(Group {
            root: position,
            end: position,
            size: 1,
        });
    }
    let mut reasoning_start = None;
    for (position, message) in context.messages.iter().enumerate() {
        compaction_step(quota)?;
        if message.role != ModelRole::Assistant {
            reasoning_start = None;
        }
        for part in message.content.parts() {
            compaction_step(quota)?;
            if message.role == ModelRole::Assistant {
                match part {
                    ModelContentPart::Reasoning { block } if !block.continuity.is_empty() => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(&mut groups, first, position, quota)?;
                    }
                    ModelContentPart::ToolCall(_) => {
                        if let Some(first) = reasoning_start {
                            link(&mut groups, first, position, quota)?;
                        }
                    }
                    _ => {}
                }
            }
            if let Some(first) = track_part(part, position, &mut pending) {
                link(&mut groups, first, position, quota)?;
            }
        }
    }
    for &(_, position) in &pending {
        let root = root(&mut groups, position, quota)?;
        // An unfinished exchange has no valid cut after its first message.
        groups[root].end = context.messages.len();
    }
    for position in 0..groups.len() {
        root(&mut groups, position, quota)?;
    }
    Ok((groups, pending))
}

fn root(
    groups: &mut [Group],
    position: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<usize> {
    compaction_step(quota)?;
    let mut root = position;
    while groups[root].root != root {
        compaction_step(quota)?;
        root = groups[root].root;
    }
    let mut next = position;
    while groups[next].root != root {
        compaction_step(quota)?;
        let parent = groups[next].root;
        groups[next].root = root;
        next = parent;
    }
    groups[position].root = root;
    Ok(root)
}

fn link(
    groups: &mut [Group],
    first: usize,
    last: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    let a = root(groups, first, quota)?;
    let b = root(groups, last, quota)?;
    if a != b {
        // Minimum roots let selection propagate dependencies in two linear passes.
        let (first, last) = (a.min(b), a.max(b));
        groups[last].root = first;
        groups[first].end = groups[first].end.max(groups[last].end);
        groups[first].size += groups[last].size;
        groups[last].size = 0;
    }
    Ok(())
}

pub(super) fn track_part<'a>(
    part: &'a ModelContentPart,
    position: usize,
    pending: &mut PendingCalls<'a>,
) -> Option<usize> {
    match part {
        ModelContentPart::ToolCall(crate::model::ToolCall { call_id, .. }) => {
            if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) {
                prior.1 = position;
            } else {
                pending.push((call_id, position));
            }
            None
        }
        ModelContentPart::ToolResult { call_id, .. } => pending
            .iter()
            .position(|(id, _)| *id == call_id)
            .map(|index| pending.swap_remove(index).1),
        _ => None,
    }
}
