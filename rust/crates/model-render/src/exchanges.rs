//! Complete dependency groups over already admitted, borrowed message semantics.
use acyclic_collections::{
    CompiledPrefixError, CompiledPrefixIndex, IndexReadWork, IndexWork, PrefixKey,
};
use std::cell::Cell;

/// Dependency semantics of one admitted content part, without parsing its bytes.
#[derive(Clone, Copy)]
pub enum Part<'a> {
    /// Independently selectable content.
    Other,
    /// Provider-native assistant continuity.
    Native,
    /// One fragment of the declaration environment used by later calls.
    Declarations {
        /// Native provider header; false selects dynamic program declarations.
        native: bool,
        /// First fragment of a new table.
        start: bool,
        /// Last fragment, making this table active.
        complete: bool,
    },
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
    /// Original declaration component required if this exchange is retained.
    pub source: Option<usize>,
    /// Original native-header component required by this exchange.
    pub native_source: Option<usize>,
    /// Last dependent exchange endpoint; bounds when this component can retire.
    pub retained_through: usize,
}

impl Group {
    /// Independent declaration dependencies, without counting a shared group twice.
    pub fn sources(self) -> impl Iterator<Item = usize> {
        self.source.into_iter().chain(
            self.native_source
                .filter(|source| Some(*source) != self.source),
        )
    }
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
            source: None,
            native_source: None,
            retained_through: position,
        });
    }
    let pending =
        CompiledPrefixIndex::from_repeated_entries(pending, |entry| entry.0.as_bytes(), work)?;
    let (declaration_start, native_declaration_start) =
        visit_messages(messages, groups, &pending, work)?;
    for (_, slot) in pending.entries() {
        work.examine(1).map_err(Work)?;
        let Some(position) = slot.get() else { continue };
        let root = root(groups, position, work)?;
        // An unfinished exchange has no valid cut after its first message.
        group_mut(groups, root)?.end = messages.len();
    }
    for start in [declaration_start, native_declaration_start]
        .into_iter()
        .flatten()
    {
        let root = root(groups, start, work)?;
        group_mut(groups, root)?.end = messages.len();
    }
    finalize_groups(groups, work)?;
    Ok(())
}

fn finalize_groups<E>(
    groups: &mut [Group],
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<(), CompiledPrefixError<E>> {
    use CompiledPrefixError::Work;
    for position in 0..groups.len() {
        root(groups, position, work)?;
    }
    for position in 0..groups.len() {
        work.examine(1).map_err(Work)?;
        for native in [false, true] {
            work.examine(1).map_err(Work)?;
            if let Some(source) = if native {
                group_mut(groups, position)?.native_source
            } else {
                group_mut(groups, position)?.source
            } {
                let source = root(groups, source, work)?;
                let owner = group_mut(groups, position)?.root;
                let prior = if native {
                    group_mut(groups, owner)?.native_source
                } else {
                    group_mut(groups, owner)?.source
                };
                let prior = prior
                    .map(|prior| group_mut(groups, prior).map(|group| group.root))
                    .transpose()?;
                if source >= owner || prior.is_some_and(|prior| prior != source) {
                    return Err(CompiledPrefixError::Invariant);
                }
                if native {
                    group_mut(groups, owner)?.native_source = Some(source);
                } else {
                    group_mut(groups, owner)?.source = Some(source);
                }
            }
        }
        group_mut(groups, position)?.retained_through = group_mut(groups, position)?.end;
    }
    for position in 0..groups.len() {
        work.examine(1).map_err(Work)?;
        if group_mut(groups, position)?.size > 0 {
            for source in group_mut(groups, position)?.sources() {
                work.examine(1).map_err(Work)?;
                if group_mut(groups, source)?.sources().next().is_some() {
                    return Err(CompiledPrefixError::Invariant);
                }
                group_mut(groups, source)?.retained_through = group_mut(groups, source)?
                    .retained_through
                    .max(group_mut(groups, position)?.end);
            }
        }
    }
    Ok(())
}

type PendingCall<'a> = (&'a str, Cell<Option<usize>>);

fn visit_messages<'a, M: Message, W: IndexWork, K: PrefixKey<PendingCall<'a>>>(
    messages: &[M],
    groups: &mut [Group],
    pending: &CompiledPrefixIndex<PendingCall<'a>, W::Allocation, K>,
    work: &mut W,
) -> Result<(Option<usize>, Option<usize>), CompiledPrefixError<W::Error>> {
    use CompiledPrefixError::{Capacity, Work};
    let mut reasoning_start = None;
    let mut declaration_start = None;
    let mut declarations = None;
    let mut native_declaration_start = None;
    let mut native_declarations = None;
    for (position, message) in messages.iter().enumerate() {
        work.examine(1).map_err(Work)?;
        if !message.assistant() {
            reasoning_start = None;
        }
        for part in message.parts() {
            work.examine(1).map_err(Work)?;
            if message.assistant() {
                link_native_part(groups, position, part, &mut reasoning_start, work)?;
            }
            match part {
                Part::Declarations {
                    native,
                    start,
                    complete,
                } => {
                    let (declaration_start, declarations) = if native {
                        (&mut native_declaration_start, &mut native_declarations)
                    } else {
                        (&mut declaration_start, &mut declarations)
                    };
                    if start {
                        *declaration_start = Some(position);
                    }
                    let first = (*declaration_start).ok_or(CompiledPrefixError::Invariant)?;
                    link(groups, first, position, work)?;
                    if complete {
                        *declarations = Some(first);
                        *declaration_start = None;
                    }
                }
                Part::Call { id, .. } => {
                    group_mut(groups, position)?.source = declarations;
                    group_mut(groups, position)?.native_source = native_declarations;
                    let entry = pending
                        .matches(id.as_bytes(), |_| false, work)?
                        .exact
                        .map(|matched| matched.entry);
                    let Some((_, slot)) = entry else {
                        return Err(Capacity);
                    };
                    if let Some(first) = slot.get() {
                        // Reusing an outstanding id cannot make its earlier
                        // call independently evictable from the same result.
                        link(groups, first, position, work)?;
                    } else {
                        slot.set(Some(position));
                    }
                }
                Part::Result { id } => {
                    if let Some(first) = pending
                        .matches(id.as_bytes(), |_| false, work)?
                        .exact
                        .and_then(|matched| matched.entry.1.take())
                    {
                        link(groups, first, position, work)?;
                    } else {
                        let root = root(groups, position, work)?;
                        group_mut(groups, root)?.end = messages.len();
                    }
                }
                _ => {}
            }
        }
    }
    Ok((declaration_start, native_declaration_start))
}

fn link_native_part<E>(
    groups: &mut [Group],
    position: usize,
    part: Part<'_>,
    reasoning_start: &mut Option<usize>,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<(), CompiledPrefixError<E>> {
    match part {
        Part::Native => {
            let first = *reasoning_start.get_or_insert(position);
            link(groups, first, position, work)?;
        }
        Part::Call { native, .. } => {
            let first = if native {
                Some(*reasoning_start.get_or_insert(position))
            } else {
                *reasoning_start
            };
            if let Some(first) = first {
                link(groups, first, position, work)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn root<E>(
    groups: &mut [Group],
    position: usize,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<usize, CompiledPrefixError<E>> {
    use CompiledPrefixError::Work;
    work.examine(1).map_err(Work)?;
    let mut root = position;
    while group_mut(groups, root)?.root != root {
        work.examine(1).map_err(Work)?;
        root = group_mut(groups, root)?.root;
    }
    let mut next = position;
    while group_mut(groups, next)?.root != root {
        work.examine(1).map_err(Work)?;
        let parent = group_mut(groups, next)?.root;
        group_mut(groups, next)?.root = root;
        next = parent;
    }
    group_mut(groups, position)?.root = root;
    Ok(root)
}

fn link<E>(
    groups: &mut [Group],
    first: usize,
    last: usize,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<(), CompiledPrefixError<E>> {
    let a = root(groups, first, work)?;
    let b = root(groups, last, work)?;
    if a != b {
        // Minimum roots let selection propagate dependencies in two linear passes.
        let (first, last) = (a.min(b), a.max(b));
        group_mut(groups, last)?.root = first;
        group_mut(groups, first)?.end = group_mut(groups, first)?
            .end
            .max(group_mut(groups, last)?.end);
        let size = group_mut(groups, last)?.size;
        group_mut(groups, first)?.size = group_mut(groups, first)?
            .size
            .checked_add(size)
            .ok_or(CompiledPrefixError::Capacity)?;
        group_mut(groups, last)?.size = 0;
    }
    Ok(())
}

fn group_mut<E>(
    groups: &mut [Group],
    position: usize,
) -> Result<&mut Group, CompiledPrefixError<E>> {
    groups
        .get_mut(position)
        .ok_or(CompiledPrefixError::Invariant)
}
