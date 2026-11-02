//! Dependency selection for a caller's bounded, admitted context projection.
//! Payloads remain borrowed; the caller funds and cancels every traversal.

#[derive(Clone, Copy)]
enum Pending<'a> {
    Leaf {
        id: &'a str,
        position: Option<usize>,
    },
    Branch {
        bit: usize,
        children: [usize; 2],
    },
}

/// A dependency-bearing fact, independent of provider JSON and harness storage.
#[derive(Clone, Copy)]
pub enum DependencyPart<'a> {
    /// Canonical assistant continuity.
    Continuity,
    /// Provider-native assistant text.
    Native,
    /// Assistant call and its original identity.
    Call {
        /// Borrowed call identity.
        id: &'a str,
        /// The call retains provider continuity.
        native: bool,
    },
    /// Result paired with a call.
    Result {
        /// Borrowed call identity.
        id: &'a str,
    },
    /// A part with no exchange dependency.
    Other,
}

/// A bounded context projection; never an unbounded history reader.
pub trait DependencySource {
    /// Number of messages in this projection.
    fn len(&self) -> usize;
    /// Whether the projection is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Checked total part count used to fund outstanding call identities.
    fn part_count(&self) -> Option<usize>;
    /// Whether one message was emitted by the assistant.
    fn assistant(&self, position: usize) -> bool;
    /// Host instructions, current input, configuration or native media.
    fn irreducible(&self, position: usize) -> bool;
    /// Borrow semantic parts at one valid message position.
    fn parts(&self, position: usize) -> impl Iterator<Item = DependencyPart<'_>>;
}

/// Original caller custody for allocations, work and cancellation.
pub trait DependencyBudget {
    /// Typed cancellation, capacity or malformed-input refusal.
    type Error;
    /// Charge one traversal step and observe cancellation.
    ///
    /// # Errors
    /// Returns the caller's cancellation or exhausted-work refusal.
    fn step(&mut self) -> Result<(), Self::Error>;
    /// Allocate a funded vector before retaining its elements.
    ///
    /// # Errors
    /// Returns the caller's allocation or capacity refusal.
    fn vector<T>(&mut self, capacity: usize) -> Result<Vec<T>, Self::Error>;
    /// Drop a vector and release its original custody.
    ///
    /// # Errors
    /// Returns the caller's custody-accounting refusal.
    fn retire<T>(&mut self, values: Vec<T>) -> Result<(), Self::Error>;
    /// Construct a typed invalid-input error.
    fn invalid(&mut self, reason: &'static str) -> Self::Error;
}

/// A message's dependency component. Only roots own the size and final endpoint.
/// An endpoint at the source length marks an incomplete exchange.
#[derive(Clone, Copy)]
pub struct Group {
    /// Earliest message in this component.
    pub root: usize,
    /// Last message, or the source length for an incomplete exchange.
    pub end: usize,
    /// Component size, held only by its root.
    pub size: usize,
}

/// Native bytes on any assistant part, its assistant calls and their results
/// share a component.
/// Plain text between them remains independently selectable.
///
/// # Errors
/// Returns the original budget refusal without waiting or retrying.
pub fn exchange_groups<S: DependencySource, B: DependencyBudget>(
    context: &S,
    quota: &mut B,
) -> Result<Vec<Group>, B::Error> {
    let parts = context
        .part_count()
        .ok_or_else(|| quota.invalid("context part count overflows"))?;
    let capacity = parts
        .checked_mul(2)
        .ok_or_else(|| quota.invalid("context dependency index overflows"))?;
    let mut pending = quota.vector::<Pending<'_>>(capacity)?;
    let mut groups = quota.vector::<Group>(context.len())?;
    for position in 0..context.len() {
        quota.step()?;
        groups.push(Group {
            root: position,
            end: position,
            size: 1,
        });
    }
    let mut reasoning_start = None;
    for position in 0..context.len() {
        let assistant = context.assistant(position);
        quota.step()?;
        if !assistant {
            reasoning_start = None;
        }
        for part in context.parts(position) {
            quota.step()?;
            if assistant {
                match part {
                    DependencyPart::Continuity => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(&mut groups, first, position, quota)?;
                    }
                    DependencyPart::Native => {
                        let first = *reasoning_start.get_or_insert(position);
                        link(&mut groups, first, position, quota)?;
                    }
                    DependencyPart::Call { native, .. } => {
                        let first = if native {
                            Some(*reasoning_start.get_or_insert(position))
                        } else {
                            reasoning_start
                        };
                        if let Some(first) = first {
                            link(&mut groups, first, position, quota)?;
                        }
                    }
                    _ => {}
                }
            }
            match part {
                DependencyPart::Call { id, .. } => {
                    let slot = pending_slot(&mut pending, id, quota)?;
                    if let Pending::Leaf {
                        position: Some(first),
                        ..
                    } = pending[slot]
                    {
                        // Reused outstanding identities remain one component.
                        link(&mut groups, first, position, quota)?;
                    } else {
                        pending[slot] = Pending::Leaf {
                            id,
                            position: Some(position),
                        };
                    }
                }
                DependencyPart::Result { id } => {
                    let slot = pending_slot(&mut pending, id, quota)?;
                    if let Pending::Leaf {
                        position: Some(first),
                        ..
                    } = pending[slot]
                    {
                        pending[slot] = Pending::Leaf { id, position: None };
                        link(&mut groups, first, position, quota)?;
                    } else {
                        let root = root(&mut groups, position, quota)?;
                        groups[root].end = context.len();
                    }
                }
                _ => {}
            }
        }
    }
    for entry in &pending {
        quota.step()?;
        if let Pending::Leaf {
            position: Some(position),
            ..
        } = *entry
        {
            let root = root(&mut groups, position, quota)?;
            // An unfinished exchange has no valid cut after its first message.
            groups[root].end = context.len();
        }
    }
    for position in 0..groups.len() {
        root(&mut groups, position, quota)?;
    }
    quota.retire(pending)?;
    Ok(groups)
}

// A compressed binary trie keeps lookup dependent only on identity bytes,
// never the number of outstanding calls. Each distinct key adds one leaf and
// at most one branch; two nodes per admitted part fund the entire index.
fn pending_slot<'a, B: DependencyBudget>(
    pending: &mut Vec<Pending<'a>>,
    id: &'a str,
    quota: &mut B,
) -> Result<usize, B::Error> {
    let mut leaf = 0;
    if !pending.is_empty() {
        while let Pending::Branch { bit, children } = pending[leaf] {
            quota.step()?;
            leaf = children[key_bit(id, bit)];
        }
        let Pending::Leaf { id: existing, .. } = pending[leaf] else {
            return Err(quota.invalid("dependency index did not reach a leaf"));
        };
        let Some(bit) = differing_bit(existing, id, quota)? else {
            return Ok(leaf);
        };
        if pending.capacity().saturating_sub(pending.len()) < 2 {
            return Err(quota.invalid("dependency identities exceed the admitted part count"));
        }
        // Locate the first less-specific branch. Root stays at position zero.
        let mut current = 0;
        let mut parent = None;
        while let Pending::Branch {
            bit: prior,
            children,
        } = pending[current]
        {
            quota.step()?;
            if prior >= bit {
                break;
            }
            let side = key_bit(id, prior);
            parent = Some((current, side));
            current = children[side];
        }
        let inserted = pending.len();
        pending.push(Pending::Leaf { id, position: None });
        let side = key_bit(id, bit);
        let mut children = [current; 2];
        children[side] = inserted;
        if let Some((parent, side)) = parent {
            let branch = pending.len();
            pending.push(Pending::Branch { bit, children });
            if let Pending::Branch { children, .. } = &mut pending[parent] {
                children[side] = branch;
            }
        } else {
            children[1 - side] = pending.len();
            pending.push(pending[0]);
            pending[0] = Pending::Branch { bit, children };
        }
        Ok(inserted)
    } else if pending.capacity() > 0 {
        pending.push(Pending::Leaf { id, position: None });
        Ok(0)
    } else {
        Err(quota.invalid("dependency identities exceed the admitted part count"))
    }
}

// Each octet starts with a one bit; the end of a key has a zero bit, so a
// prefix and the same prefix followed by a zero octet remain distinct.
fn key_bit(id: &str, bit: usize) -> usize {
    id.as_bytes().get(bit / 9).map_or(0, |byte| {
        if bit.is_multiple_of(9) {
            1
        } else {
            usize::from((byte >> (8 - bit % 9)) & 1)
        }
    })
}

fn differing_bit<B: DependencyBudget>(
    left: &str,
    right: &str,
    quota: &mut B,
) -> Result<Option<usize>, B::Error> {
    for position in 0..=left.len().min(right.len()) {
        quota.step()?;
        let bit = match (
            left.as_bytes().get(position),
            right.as_bytes().get(position),
        ) {
            (Some(left), Some(right)) if left != right => {
                1 + (left ^ right).leading_zeros() as usize
            }
            (Some(_), None) | (None, Some(_)) => 0,
            (None, None) => return Ok(None),
            _ => continue,
        };
        return position
            .checked_mul(9)
            .and_then(|base| base.checked_add(bit))
            .map(Some)
            .ok_or_else(|| quota.invalid("context dependency identity overflows"));
    }
    Ok(None)
}

fn root<B: DependencyBudget>(
    groups: &mut [Group],
    position: usize,
    quota: &mut B,
) -> Result<usize, B::Error> {
    quota.step()?;
    let mut root = position;
    while groups[root].root != root {
        quota.step()?;
        root = groups[root].root;
    }
    let mut next = position;
    while groups[next].root != root {
        quota.step()?;
        let parent = groups[next].root;
        groups[next].root = root;
        next = parent;
    }
    groups[position].root = root;
    Ok(root)
}

fn link<B: DependencyBudget>(
    groups: &mut [Group],
    first: usize,
    last: usize,
    quota: &mut B,
) -> Result<(), B::Error> {
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

/// Selects the irreducible set and closes it over whole exchange components.
///
/// # Errors
/// Returns the original budget refusal without waiting or retrying.
pub fn mandatory_flags<S: DependencySource, B: DependencyBudget>(
    context: &S,
    covered: usize,
    groups: &[Group],
    quota: &mut B,
) -> Result<Vec<bool>, B::Error> {
    if groups.len() != context.len() {
        return Err(quota.invalid("dependency groups differ from projection"));
    }
    let mut selected = quota.vector::<bool>(context.len())?;
    for position in 0..context.len() {
        quota.step()?;
        for _ in context.parts(position) {
            quota.step()?;
        }
        selected.push(
            context.irreducible(position) || position + 1 == context.len() || position >= covered,
        );
    }
    for (position, group) in groups.iter().enumerate() {
        quota.step()?;
        if group.root > position || groups[group.root].root != group.root {
            return Err(quota.invalid("dependency group root is not flattened"));
        }
        let keep = selected[position];
        selected[group.root] |= keep;
    }
    for (position, group) in groups.iter().enumerate() {
        quota.step()?;
        let keep = selected[group.root];
        selected[position] |= keep;
    }
    Ok(selected)
}
