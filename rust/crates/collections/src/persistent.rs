//! Immutable prefix views over the shared fixed-width branch coordinates.
use super::{Bit, IndexReadWork, IndexWork, arc_allocation_bytes, difference, high};
use std::sync::Arc;

/// A caller-funded immutable ordered index. Cloning or taking a prefix shares
/// the original nodes without allocation. Insertion copies one bounded path;
/// predecessor lookup makes at most eight branch decisions per key byte,
/// inspecting each branch's child minimum before visiting one leaf.
/// Neither operation depends on fork depth or the retained population.
pub type PersistentIndex<V, A, const N: usize = 8> = Tree<V, A, N, std::convert::Infallible>;

pub(crate) type QualifiedIndex<V, A, const N: usize> = Tree<V, A, N, u64>;

/// Worst-case original funding for one immutable insertion, independent of
/// population. Actual claims and examinations still occur on `IndexWork`.
#[derive(Clone, Copy, Debug)]
pub struct IndexInsertionBound {
    /// Sum of physical node backing claims, including copied path nodes.
    pub allocation_bytes: u64,
    /// Sum of examined items, including fixed-width key comparisons.
    pub examined_items: u64,
}

/// Shared immutable tree representation; public compositions select the cut type.
pub struct Tree<V, A, const N: usize, C> {
    root: Option<Node<V, A, N, C>>,
    before: Option<[u8; N]>,
    value_before: Option<C>,
}

/// One borrowed value and the exact inherited exclusive cut under which it
/// was found. A range value must respect this cut even after a later insertion.
pub struct PersistentPosition<'a, V, const N: usize> {
    /// The original retained key selected by the lookup.
    pub key: &'a [u8; N],
    /// The original immutable value, borrowed without a copy.
    pub value: &'a V,
    /// The effective exclusive prefix cut on the path to this value.
    pub before: Option<[u8; N]>,
}

pub(crate) struct Position<'a, V, const N: usize, C> {
    pub(crate) key: &'a [u8; N],
    pub(crate) value: &'a V,
    pub(crate) before: Option<[u8; N]>,
    pub(crate) value_before: Option<C>,
}

impl<'a, V, const N: usize> Position<'a, V, N, std::convert::Infallible> {
    fn unqualified(self) -> PersistentPosition<'a, V, N> {
        PersistentPosition {
            key: self.key,
            value: self.value,
            before: self.before,
        }
    }
}

impl<V, A, const N: usize> PersistentIndex<V, A, N> {
    /// Borrows the greatest retained key at or below `key`. Checks the original
    /// admission even on an empty view. It allocates and copies no values.
    pub fn predecessor<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> Result<Option<PersistentPosition<'_, V, N>>, W::Error> {
        self.predecessor_position(key, work)
            .map(|position| position.map(Position::unqualified))
    }

    /// Borrows the least retained key at or above `key`, without allocating.
    /// The bounded search descends one path and at most one minimum path.
    pub fn successor<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> Result<Option<PersistentPosition<'_, V, N>>, W::Error> {
        self.successor_position(key, work)
            .map(|position| position.map(Position::unqualified))
    }
}

struct Node<V, A, const N: usize, C>(Option<Arc<Body<V, A, N, C>>>);

struct Body<V, A, const N: usize, C> {
    minimum: [u8; N],
    depth: usize,
    kind: Kind<V, A, N, C>,
    // Values and child references retire before their original node funding.
    _allocation: A,
}

enum Kind<V, A, const N: usize, C> {
    Leaf(V),
    Branch {
        bit: Bit,
        low: Tree<V, A, N, C>,
        high: Tree<V, A, N, C>,
    },
}

impl<V, A, const N: usize, C> Node<V, A, N, C> {
    fn body(&self) -> Option<&Body<V, A, N, C>> {
        self.0.as_deref()
    }
}

impl<V, A, const N: usize, C> Clone for Node<V, A, N, C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V, A, const N: usize, C> Drop for Node<V, A, N, C> {
    fn drop(&mut self) {
        // All references are these private handles; there are no weak handles.
        // into_inner elects exactly one last owner even on concurrent drops,
        // and releases the physical Arc backing before returning its body.
        // Its token therefore refunds after both backing and value retirement.
        let Some(body) = self.0.take().and_then(Arc::into_inner) else {
            return;
        };
        if matches!(body.kind, Kind::Leaf(_)) {
            return;
        }
        // At most one sibling per strictly increasing branch coordinate.
        // Physical retirement needs no allocator or fallible admission, even
        // for wide adversarial keys or concurrently released shared roots.
        let mut pending = [const { [const { None }; 8] }; N];
        let mut depth = 0;
        let mut current = Some(body);
        loop {
            if let Some(mut body) = current.take() {
                if let Kind::Branch { low, high, .. } = &mut body.kind {
                    if let Some(upper) = high.root.take() {
                        pending[depth / 8][depth % 8] = Some(upper);
                        depth += 1;
                    }
                    current = low
                        .root
                        .take()
                        .and_then(|mut node| node.0.take())
                        .and_then(Arc::into_inner);
                }
                drop(body);
            }
            if current.is_none() {
                if depth == 0 {
                    break;
                }
                depth -= 1;
                current = pending[depth / 8][depth % 8]
                    .take()
                    .and_then(|mut node: Node<V, A, N, C>| node.0.take())
                    .and_then(Arc::into_inner);
            }
        }
    }
}

impl<V, A, const N: usize, C: Copy> Clone for Tree<V, A, N, C> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            before: self.before,
            value_before: self.value_before,
        }
    }
}

impl<V, A, const N: usize, C> Default for Tree<V, A, N, C> {
    fn default() -> Self {
        Self {
            root: None,
            before: None,
            value_before: None,
        }
    }
}

impl<V, A, const N: usize, C: Copy + Ord> Tree<V, A, N, C> {
    /// Computes the original funding required to admit an insertion before its
    /// key arrives. Returns `None` if the selected type's bound overflows.
    /// No allocation, admission or tree traversal is performed.
    pub fn insertion_bound(&self, insertions: u64) -> Option<IndexInsertionBound> {
        Self::insertion_bound_at_depth(self.depth() as u64, insertions)
    }

    pub(crate) fn insertion_bound_at_depth(
        depth: u64,
        insertions: u64,
    ) -> Option<IndexInsertionBound> {
        let width = u64::try_from(N).ok()?;
        let depth = width.checked_mul(8)?.min(depth.checked_add(insertions)?);
        Some(IndexInsertionBound {
            // One new leaf, at most one branch per bit, and the differing branch.
            allocation_bytes: depth
                .checked_add(2)?
                .checked_mul(insertions)?
                .checked_mul(u64::try_from(arc_allocation_bytes::<Body<V, A, N, C>>()).ok()?)?,
            // The locating pass and rebuilding pass, including child visibility
            // and complete minimum-key comparisons, each have bounded depth.
            examined_items: depth
                .checked_add(3)?
                .checked_mul(width.checked_mul(4)?.checked_add(8)?)?
                .checked_mul(insertions)?,
        })
    }

    pub(crate) fn depth(&self) -> usize {
        self.body().map_or(0, |body| body.depth)
    }

    /// Pins the original keys strictly before this exclusive cut. Repeated
    /// prefixing keeps the earlier cut, without a chain of historical views.
    pub fn prefix(&self, before: [u8; N]) -> Self {
        let before = self.before.map_or(before, |prior| prior.min(before));
        Self {
            root: self.root.clone(),
            before: Some(before),
            value_before: self.value_before,
        }
    }

    // A keyed sequence composition applies this bound to the value's inner
    // index. It never filters the outer key space or copies an inherited tree.
    pub(crate) fn values_before(&self, before: C) -> Self {
        Self {
            root: self.root.clone(),
            before: self.before,
            value_before: Some(self.value_before.map_or(before, |prior| prior.min(before))),
        }
    }

    fn body(&self) -> Option<&Body<V, A, N, C>> {
        self.root.as_ref().and_then(Node::body)
    }

    fn visible_body<W: IndexReadWork>(
        &self,
        work: &mut W,
    ) -> Result<Option<&Body<V, A, N, C>>, W::Error> {
        if self.root.is_none() {
            return Ok(None);
        }
        work.examine(1 + N as u64)?;
        Ok(self
            .body()
            .filter(|body| self.before.is_none_or(|before| body.minimum < before)))
    }

    fn child(&self, child: &Self) -> Self {
        let child = self
            .before
            .map_or_else(|| child.clone(), |before| child.prefix(before));
        match self.value_before {
            Some(before) => child.values_before(before),
            None => child,
        }
    }

    fn node<W: IndexWork<Allocation = A>>(
        minimum: [u8; N],
        kind: Kind<V, A, N, C>,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let allocation = work.claim(arc_allocation_bytes::<Body<V, A, N, C>>() as u64)?;
        let depth = match &kind {
            Kind::Leaf(_) => 0,
            Kind::Branch { low, high, .. } => 1 + low.depth().max(high.depth()),
        };
        Ok(Self {
            root: Some(Node(Some(Arc::new(Body {
                minimum,
                depth,
                kind,
                _allocation: allocation,
            })))),
            before: None,
            value_before: None,
        })
    }

    fn branch<W: IndexWork<Allocation = A>>(
        bit: Bit,
        low: Self,
        high: Self,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        match (low.visible_body(work)?, high.visible_body(work)?) {
            (None, _) => Ok(high),
            (_, None) => Ok(low),
            (Some(low_body), Some(_)) => {
                Self::node(low_body.minimum, Kind::Branch { bit, low, high }, work)
            }
        }
    }

    pub(crate) fn predecessor_position<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> Result<Option<Position<'_, V, N, C>>, W::Error> {
        work.validate()?;
        let mut cursor = self;
        let mut before = self.before;
        let mut value_before = self.value_before;
        loop {
            let Some(body) = cursor.visible_body(work)? else {
                return Ok(None);
            };
            work.examine(2 * N as u64)?;
            if body.minimum > key || before.is_some_and(|before| body.minimum >= before) {
                return Ok(None);
            }
            match &body.kind {
                Kind::Leaf(value) => {
                    return Ok(Some(Position {
                        key: &body.minimum,
                        value,
                        before,
                        value_before,
                    }));
                }
                Kind::Branch { low, high, .. } => {
                    let upper = high.visible_body(work)?;
                    work.examine(3 * N as u64)?;
                    cursor = if upper.is_some_and(|upper| {
                        upper.minimum <= key && before.is_none_or(|before| upper.minimum < before)
                    }) {
                        high
                    } else {
                        low
                    };
                    if let Some(cut) = cursor.before {
                        before = Some(before.map_or(cut, |prior| prior.min(cut)));
                    }
                    if let Some(cut) = cursor.value_before {
                        value_before = Some(value_before.map_or(cut, |prior| prior.min(cut)));
                    }
                }
            }
        }
    }

    fn successor_position<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> Result<Option<Position<'_, V, N, C>>, W::Error> {
        work.validate()?;
        let mut cursor = self;
        let mut before = None;
        let mut value_before = None;
        let mut candidate = None;
        loop {
            if let Some(cut) = cursor.before {
                before = Some(before.map_or(cut, |prior: [u8; N]| prior.min(cut)));
            }
            if let Some(cut) = cursor.value_before {
                value_before = Some(value_before.map_or(cut, |prior: C| prior.min(cut)));
            }
            let body = cursor.visible_body(work)?;
            work.examine(2 * N as u64)?;
            match body.filter(|body| before.is_none_or(|cut| key < cut && body.minimum < cut)) {
                Some(Body {
                    minimum,
                    kind: Kind::Leaf(value),
                    ..
                }) if *minimum >= key => {
                    return Ok(Some(Position {
                        key: minimum,
                        value,
                        before,
                        value_before,
                    }));
                }
                Some(Body {
                    kind: Kind::Branch { low, high, .. },
                    ..
                }) => {
                    let upper = high.visible_body(work)?;
                    work.examine(2 * N as u64)?;
                    if let Some(upper) =
                        upper.filter(|upper| before.is_none_or(|cut| upper.minimum < cut))
                    {
                        if upper.minimum <= key {
                            cursor = high;
                        } else {
                            candidate = Some((high, before, value_before));
                            cursor = low;
                        }
                    } else {
                        cursor = low;
                    }
                    continue;
                }
                _ => {}
            }
            match candidate.take() {
                Some((next, cut, value_cut)) => {
                    cursor = next;
                    before = cut;
                    value_before = value_cut;
                }
                None => return Ok(None),
            }
        }
    }

    /// Returns a new root containing `key` and the supplied value. The old root
    /// and every clone remain unchanged on success, refusal or cancellation.
    /// An insertion outside a prefix cannot reveal its discarded suffix.
    /// Each allocated node retains the original issuing owner's funding.
    pub fn insert<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        value: V,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        work.check()?;
        let mut cursor = self.visible_body(work)?;
        let mut differing = None;
        while let Some(body) = cursor {
            match &body.kind {
                Kind::Leaf(_) => {
                    work.charge_items(N as u64)?;
                    differing = difference(&body.minimum, &key);
                    break;
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    // The original uncut shape locates the differing bit. Cuts
                    // propagate to retained children when the new path is built.
                    work.charge_items(1)?;
                    cursor = if high(&key, *bit) { upper } else { low }.body();
                }
            }
        }
        let leaf = Self::node(key, Kind::Leaf(value), work)?;
        let mut view = self.clone();
        let mut result = Self::default();
        let mut destination = &mut result;
        // One original sibling depth per possible branch, with no allocation.
        let mut depths = [[0_usize; 8]; N];
        let mut depth = 0;
        loop {
            let Some(body) = view.visible_body(work)? else {
                *destination = leaf;
                break;
            };
            match &body.kind {
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } if differing.is_none_or(|difference| *bit < difference) => {
                    work.charge_items(2 * N as u64)?;
                    let bit = *bit;
                    let minimum = body.minimum.min(key);
                    let low = view.child(low);
                    let upper = view.child(upper);
                    let selected_high = high(&key, bit);
                    let (selected, sibling) = if selected_high {
                        (upper, low)
                    } else {
                        (low, upper)
                    };
                    if sibling.visible_body(work)?.is_none() {
                        view = selected;
                        continue;
                    }
                    depths[depth / 8][depth % 8] = sibling.depth();
                    depth += 1;
                    let (low, high) = if selected_high {
                        (sibling, Self::default())
                    } else {
                        (Self::default(), sibling)
                    };
                    *destination = Self::node(minimum, Kind::Branch { bit, low, high }, work)?;
                    let body = Arc::get_mut(destination.root.as_mut().unwrap().0.as_mut().unwrap())
                        .unwrap_or_else(|| unreachable!("the unpublished path has one owner"));
                    let Kind::Branch { low, high, .. } = &mut body.kind else {
                        unreachable!("the path just constructed a branch")
                    };
                    destination = if selected_high { high } else { low };
                    view = selected;
                }
                _ => {
                    *destination = match differing {
                        None => leaf,
                        Some(bit) if high(&key, bit) => Self::branch(bit, view, leaf, work)?,
                        Some(bit) => Self::branch(bit, leaf, view, work)?,
                    };
                    break;
                }
            }
        }
        let mut height = destination.depth();
        for position in (0..depth).rev() {
            work.charge_items(1)?;
            height = 1 + height.max(depths[position / 8][position % 8]);
            depths[position / 8][position % 8] = height;
        }
        let mut cursor = &mut result;
        for position in 0..depth {
            work.charge_items(1)?;
            let body = Arc::get_mut(cursor.root.as_mut().unwrap().0.as_mut().unwrap())
                .unwrap_or_else(|| unreachable!("the unpublished path has one owner"));
            body.depth = depths[position / 8][position % 8];
            let Kind::Branch {
                bit,
                low,
                high: upper,
            } = &mut body.kind
            else {
                unreachable!("the copied path contains a branch")
            };
            cursor = if high(&key, *bit) { upper } else { low };
        }
        Ok(result)
    }
}
