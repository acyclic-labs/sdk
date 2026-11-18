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
    fn source_slot(&mut self, native: bool) -> &mut Option<usize> {
        if native {
            &mut self.native_source
        } else {
            &mut self.source
        }
    }

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
                match part {
                    Part::Native => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(groups, first, position, work)?;
                    }
                    Part::Call { native, .. } => {
                        let first = if native {
                            Some(*reasoning_start.get_or_insert(position))
                        } else {
                            reasoning_start
                        };
                        if let Some(first) = first {
                            link(groups, first, position, work)?;
                        }
                    }
                    _ => {}
                }
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
                    let group = group_mut(groups, position)?;
                    group.source = declarations;
                    group.native_source = native_declarations;
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
    finish_groups(groups, work)
}

fn finish_groups<E>(
    groups: &mut [Group],
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<(), CompiledPrefixError<E>> {
    use CompiledPrefixError::{Invariant, Work};
    for position in 0..groups.len() {
        root(groups, position, work)?;
    }
    for position in 0..groups.len() {
        work.examine(1).map_err(Work)?;
        for native in [false, true] {
            work.examine(1).map_err(Work)?;
            let mut entry = group(groups, position)?;
            if let Some(source) = *entry.source_slot(native) {
                let source = root(groups, source, work)?;
                let owner = entry.root;
                let mut owner_entry = group(groups, owner)?;
                if source >= owner {
                    return Err(Invariant);
                }
                if let Some(prior) = *owner_entry.source_slot(native)
                    && group(groups, prior)?.root != source
                {
                    return Err(Invariant);
                }
                *group_mut(groups, owner)?.source_slot(native) = Some(source);
            }
        }
        let entry = group_mut(groups, position)?;
        entry.retained_through = entry.end;
    }
    for position in 0..groups.len() {
        work.examine(1).map_err(Work)?;
        let entry = group(groups, position)?;
        if entry.size > 0 {
            for source in entry.sources() {
                work.examine(1).map_err(Work)?;
                let source = group_mut(groups, source)?;
                if source.sources().next().is_some() {
                    return Err(Invariant);
                }
                source.retained_through = source.retained_through.max(entry.end);
            }
        }
    }
    Ok(())
}

fn group<E>(groups: &[Group], position: usize) -> Result<Group, CompiledPrefixError<E>> {
    groups
        .get(position)
        .copied()
        .ok_or(CompiledPrefixError::Invariant)
}

fn group_mut<E>(
    groups: &mut [Group],
    position: usize,
) -> Result<&mut Group, CompiledPrefixError<E>> {
    groups
        .get_mut(position)
        .ok_or(CompiledPrefixError::Invariant)
}

fn root<E>(
    groups: &mut [Group],
    position: usize,
    work: &mut impl IndexReadWork<Error = E>,
) -> Result<usize, CompiledPrefixError<E>> {
    use CompiledPrefixError::Work;
    work.examine(1).map_err(Work)?;
    let mut root = position;
    loop {
        let parent = group(groups, root)?.root;
        if parent == root {
            break;
        }
        work.examine(1).map_err(Work)?;
        root = parent;
    }
    let mut next = position;
    while group(groups, next)?.root != root {
        work.examine(1).map_err(Work)?;
        let entry = group_mut(groups, next)?;
        let parent = entry.root;
        entry.root = root;
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
        let [first_entry, last_entry] = groups
            .get_disjoint_mut([first, last])
            .map_err(|_| CompiledPrefixError::Invariant)?;
        last_entry.root = first;
        first_entry.end = first_entry.end.max(last_entry.end);
        first_entry.size += last_entry.size;
        last_entry.size = 0;
    }
    Ok(())
}
