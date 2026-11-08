//! Complete dependency groups over already admitted, borrowed message semantics.
use acyclic_collections::{CompiledPrefixError, CompiledPrefixIndex, IndexReadWork, IndexWork};
use std::cell::Cell;

/// Dependency semantics of one admitted content part, without parsing its bytes.
#[derive(Clone, Copy)]
pub enum Part<'a> {
    /// Independently selectable content.
    Other,
    /// Provider-native assistant continuity.
    Native,
    /// A tool invocation and whether it carries native continuity.
    Call {
        /// Original borrowed call identity.
        id: &'a str,
        /// Whether this call starts native continuity.
        native: bool,
    },
    /// A tool result consuming the outstanding invocation with this identity.
    Result {
        /// Original borrowed call identity.
        id: &'a str,
    },
}

/// A stable, allocation-free semantic view of an admitted message.
pub trait Message {
    /// Whether native assistant continuity can cross this message.
    fn assistant(&self) -> bool;
    /// Original ordered content parts. Repeated passes must return the same view.
    fn parts(&self) -> impl Iterator<Item = Part<'_>>;
}

/// A message's dependency component. Only roots own the size and final endpoint.
/// An endpoint at the source length marks an incomplete exchange.
#[derive(Clone, Copy)]
pub struct Group {
    /// Earliest message in this component.
    pub root: usize,
    /// Last member, or source length for an incomplete exchange.
    pub end: usize,
    /// Component size at the root; zero at other members.
    pub size: usize,
}

/// Native bytes on any assistant part, its assistant calls and their results
/// share a component.
/// Plain text between them remains independently selectable.
///
/// The caller supplies an empty output vector with capacity for every admitted
/// message, retaining its funding. Other allocations borrow `work` until this
/// call returns. Every input visit, key comparison and component traversal is
/// charged before use; refusal leaves unpublished partial output in `groups`.
/// The semantic projection must remain stable across passes. Work is bounded
/// by this finite input and its keys, not by conversation history. Per-ID lookup
/// is O(ID bytes); component traversal is charged, not claimed constant-time.
/// There are no waits, I/O, string copies or per-message allocations.
pub fn exchange_groups<M: Message, W: IndexWork>(
    messages: &[M],
    groups: &mut Vec<Group>,
    work: &mut W,
) -> Result<(), CompiledPrefixError<W::Error>> {
    use CompiledPrefixError::{Allocation, Capacity, Work};
    work.check().map_err(Work)?;
    if !groups.is_empty() || groups.capacity() < messages.len() {
        return Err(Capacity);
    }
    let mut calls = 0usize;
    for message in messages {
        work.charge_items(1).map_err(Work)?;
        for part in message.parts() {
            work.charge_items(1).map_err(Work)?;
            if matches!(part, Part::Call { .. }) {
                calls = calls.checked_add(1).ok_or(Capacity)?;
            }
        }
    }
    let bytes = calls
        .checked_mul(std::mem::size_of::<(&str, Cell<Option<usize>>)>())
        .ok_or(Capacity)?;
    // Declared before backing: every exit destroys the original vector and
    // compiled table before releasing their original funding.
    let _pending_allocation = if bytes == 0 {
        None
    } else {
        Some(work.claim(bytes as u64).map_err(Work)?)
    };
    let mut pending = Vec::new();
    pending.try_reserve_exact(calls).map_err(Allocation)?;
    for message in messages {
        work.charge_items(1).map_err(Work)?;
        for part in message.parts() {
            work.charge_items(1).map_err(Work)?;
            if let Part::Call { id, .. } = part {
                pending.push((id, Cell::new(None)));
            }
        }
    }
    for position in 0..messages.len() {
        work.charge_items(1).map_err(Work)?;
        groups.push(Group {
            root: position,
            end: position,
            size: 1,
        });
    }
    let pending =
        CompiledPrefixIndex::from_repeated_entries(pending, |entry| entry.0.as_bytes(), work)?;
    let mut reasoning_start = None;
    for (position, message) in messages.iter().enumerate() {
        work.examine(1).map_err(Work)?;
        if !message.assistant() {
            reasoning_start = None;
        }
        for part in message.parts() {
            work.examine(1).map_err(Work)?;
            if message.assistant() {
                match part {
                    Part::Native => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(groups, first, position, work).map_err(Work)?;
                    }
                    Part::Call { native, .. } => {
                        let first = if native {
                            Some(*reasoning_start.get_or_insert(position))
                        } else {
                            reasoning_start
                        };
                        if let Some(first) = first {
                            link(groups, first, position, work).map_err(Work)?;
                        }
                    }
                    _ => {}
                }
            }
            match part {
                Part::Call { id, .. } => {
                    let entry = pending
                        .matches(id.as_bytes(), |_| false, work)
                        .map_err(Work)?
                        .exact
                        .map(|matched| matched.entry);
                    let Some((_, slot)) = entry else {
                        return Err(Capacity);
                    };
                    if let Some(first) = slot.get() {
                        // Reusing an outstanding id cannot make its earlier
                        // call independently evictable from the same result.
                        link(groups, first, position, work).map_err(Work)?;
                    } else {
                        slot.set(Some(position));
                    }
                }
                Part::Result { id } => {
                    match pending
                        .matches(id.as_bytes(), |_| false, work)
                        .map_err(Work)?
                        .exact
                        .and_then(|matched| matched.entry.1.take())
                    {
                        Some(first) => {
                            link(groups, first, position, work).map_err(Work)?;
                        }
                        None => {
                            let root = root(groups, position, work).map_err(Work)?;
                            groups[root].end = messages.len();
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for (_, slot) in pending.entries() {
        work.examine(1).map_err(Work)?;
        let Some(position) = slot.get() else { continue };
        let root = root(groups, position, work).map_err(Work)?;
        // An unfinished exchange has no valid cut after its first message.
        groups[root].end = messages.len();
    }
    for position in 0..groups.len() {
        root(groups, position, work).map_err(Work)?;
    }
    Ok(())
}

fn root<E>(
    groups: &mut [Group],
    position: usize,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<usize, E> {
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

fn link<E>(
    groups: &mut [Group],
    first: usize,
    last: usize,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<(), E> {
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
