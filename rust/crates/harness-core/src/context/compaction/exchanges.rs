use super::{
    Context, ModelContentPart, quota_vector,
    selection::{compaction_error, compaction_step},
};
use crate::{Result, model::ModelRole};
use acyclic_collections::{CompiledPrefixError, CompiledPrefixIndex, IndexReadWork};
use std::cell::Cell;

/// A message's dependency component. Only roots own the size and final endpoint.
/// An endpoint at the source length marks an incomplete exchange.
#[derive(Clone, Copy)]
pub(super) struct Group {
    pub root: usize,
    pub end: usize,
    pub size: usize,
}

/// Native bytes on any assistant part, its assistant calls and their results
/// share a component.
/// Plain text between them remains independently selectable.
pub(super) fn exchange_groups(
    context: &Context,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<Group>> {
    let calls = context.messages.iter().try_fold(0usize, |count, message| {
        compaction_step(quota)?;
        message
            .content
            .parts()
            .iter()
            .try_fold(count, |count, part| {
                compaction_step(quota)?;
                if matches!(part, ModelContentPart::ToolCall(_)) {
                    count.checked_add(1).ok_or_else(|| {
                        compaction_error(quota.work(), "context call count overflows")
                    })
                } else {
                    Ok(count)
                }
            })
    })?;
    let mut pending = quota_vector::<(&str, Cell<Option<usize>>)>(calls, quota)?;
    for message in &context.messages {
        compaction_step(quota)?;
        for part in message.content.parts() {
            compaction_step(quota)?;
            if let ModelContentPart::ToolCall(call) = part {
                pending.push((&call.call_id, Cell::new(None)));
            }
        }
    }
    let pending_bytes =
        (pending.capacity() * std::mem::size_of::<(&str, Cell<Option<usize>>)>()) as u64;
    let mut groups = quota_vector::<Group>(context.messages.len(), quota)?;
    for position in 0..context.messages.len() {
        compaction_step(quota)?;
        groups.push(Group {
            root: position,
            end: position,
            size: 1,
        });
    }
    let original = crate::wire_codec::BorrowedIndexWork::new(quota.work());
    let mut work = &original;
    let pending = match CompiledPrefixIndex::from_repeated_entries(
        pending,
        |entry| entry.0.as_bytes(),
        &mut work,
    ) {
        Ok(index) => index,
        Err(CompiledPrefixError::Work(error)) => return Err(error),
        Err(CompiledPrefixError::Allocation(cause)) => {
            return original.with(|work| {
                Err(match work.as_deref_mut() {
                    Some(io) => io.allocation_failure(cause),
                    None => crate::Error::Invalid(cause.to_string()),
                })
            });
        }
        Err(CompiledPrefixError::Capacity | CompiledPrefixError::Order(_)) => {
            return original.with(|work| {
                Err(compaction_error(
                    work,
                    "tool dependency index cannot be represented",
                ))
            });
        }
    };
    let mut reasoning_start = None;
    for (position, message) in context.messages.iter().enumerate() {
        work.examine(1)?;
        if message.role != ModelRole::Assistant {
            reasoning_start = None;
        }
        for part in message.content.parts() {
            work.examine(1)?;
            if message.role == ModelRole::Assistant {
                match part {
                    ModelContentPart::Reasoning { block } if !block.continuity.is_empty() => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(&mut groups, first, position, &mut work)?;
                    }
                    ModelContentPart::Text {
                        native: Some(_), ..
                    } => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(&mut groups, first, position, &mut work)?;
                    }
                    ModelContentPart::ToolCall(call) => {
                        let first = match call.native {
                            Some(_) => Some(*reasoning_start.get_or_insert(position)),
                            None => reasoning_start,
                        };
                        if let Some(first) = first {
                            link(&mut groups, first, position, &mut work)?;
                        }
                    }
                    _ => {}
                }
            }
            match part {
                ModelContentPart::ToolCall(call) => {
                    let entry = pending
                        .matches(call.call_id.as_bytes(), |_| false, &mut work)?
                        .exact
                        .map(|matched| matched.entry);
                    let Some((_, slot)) = entry else {
                        return original.with(|work| {
                            Err(compaction_error(
                                work,
                                "tool call is absent from its dependency index",
                            ))
                        });
                    };
                    if let Some(first) = slot.get() {
                        // Reusing an outstanding id cannot make its earlier
                        // call independently evictable from the same result.
                        link(&mut groups, first, position, &mut work)?;
                    } else {
                        slot.set(Some(position));
                    }
                }
                ModelContentPart::ToolResult { call_id, .. } => {
                    match pending
                        .matches(call_id.as_bytes(), |_| false, &mut work)?
                        .exact
                        .and_then(|matched| matched.entry.1.take())
                    {
                        Some(first) => {
                            link(&mut groups, first, position, &mut work)?;
                        }
                        None => {
                            let root = root(&mut groups, position, &mut work)?;
                            groups[root].end = context.messages.len();
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for (_, slot) in pending.entries() {
        work.examine(1)?;
        let Some(position) = slot.get() else { continue };
        let root = root(&mut groups, position, &mut work)?;
        // An unfinished exchange has no valid cut after its first message.
        groups[root].end = context.messages.len();
    }
    for position in 0..groups.len() {
        root(&mut groups, position, &mut work)?;
    }
    drop(pending);
    drop(original);
    quota.relinquish(pending_bytes)?;
    Ok(groups)
}

fn root(
    groups: &mut [Group],
    position: usize,
    work: &mut impl IndexReadWork<Error = crate::Error>,
) -> Result<usize> {
    work.examine(1)?;
    let mut root = position;
    while groups[root].root != root {
        work.examine(1)?;
        root = groups[root].root;
    }
    let mut next = position;
    while groups[next].root != root {
        work.examine(1)?;
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
    work: &mut impl IndexReadWork<Error = crate::Error>,
) -> Result<()> {
    let a = root(groups, first, work)?;
    let b = root(groups, last, work)?;
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
