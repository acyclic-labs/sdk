//! Immutable prefix views over the shared fixed-width branch coordinates.
use super::{Bit, IndexReadWork, IndexWork, arc_allocation_bytes, difference, high};
use std::sync::Arc;

/// A caller-funded immutable ordered index. Cloning or taking a prefix shares
/// the original nodes without allocation. Insertion copies one bounded path;
/// predecessor lookup makes at most eight branch decisions per key byte,
/// inspecting each branch's child minimum before visiting one leaf.
/// Neither operation depends on fork depth or the retained population.
pub type PersistentIndex<V, A, const N: usize = 8> =
    Tree<V, A, N, std::convert::Infallible, [u8; N]>;

// Keyed compositions bound sequences, never the key space, so their key
// index carries no key cut and its views and nodes stay independent of width.
pub(crate) type QualifiedIndex<V, A, const N: usize> = Tree<V, A, N, u64, std::convert::Infallible>;

/// An exclusive key cut a composition may carry; `Infallible` carries none.
pub trait KeyCut<const N: usize>: Copy + Ord {
    /// The cut's key; keys at or above it are hidden.
    fn key(&self) -> &[u8; N];
}

impl<const N: usize> KeyCut<N> for [u8; N] {
    fn key(&self) -> &[u8; N] {
        self
    }
}

impl<const N: usize> KeyCut<N> for std::convert::Infallible {
    fn key(&self) -> &[u8; N] {
        match *self {}
    }
}

/// Worst-case original node funding and insertion work, bounded by key width.
/// Actual claims and examinations still occur on `IndexWork`.
#[derive(Clone, Copy, Debug)]
pub struct IndexInsertionBound {
    /// Sum of physical node backing claims, including copied path nodes.
    pub allocation_bytes: u64,
    /// Sum of examined items, including fixed-width key comparisons.
    pub examined_items: u64,
}

/// One shared exclusive cut and the original funding that keeps it alive.
pub(crate) struct Cut<K, A> {
    key: K,
    _allocation: A,
}

/// Shared immutable tree representation; public compositions select the cut types.
pub struct Tree<V, A, const N: usize, C, K> {
    root: Option<Node<V, A, N, C, K>>,
    // The cut is shared by every view and child that inherits it, so a view's
    // size never depends on the key width. Its funding retires with the last
    // view that shares it.
    before: Option<Arc<Cut<K, A>>>,
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

pub(crate) struct Position<'a, V, A, const N: usize, C, K> {
    pub(crate) key: &'a [u8; N],
    pub(crate) value: &'a V,
    pub(crate) before: Option<Arc<Cut<K, A>>>,
    pub(crate) value_before: Option<C>,
}

type PositionResult<'a, V, A, const N: usize, C, K, E> =
    Result<Option<Position<'a, V, A, N, C, K>>, E>;

impl<'a, V, A, const N: usize> Position<'a, V, A, N, std::convert::Infallible, [u8; N]> {
    fn unqualified(self) -> PersistentPosition<'a, V, N> {
        PersistentPosition {
            key: self.key,
            value: self.value,
            before: self.before.map(|cut| cut.key),
        }
    }
}

impl<V, A, const N: usize> PersistentIndex<V, A, N> {
    /// Pins the original keys strictly before this exclusive cut. Repeated
    /// prefixing keeps the earlier cut, without a chain of historical views.
    pub fn prefix<W: IndexWork<Allocation = A>>(
        &self,
        before: [u8; N],
        work: &mut W,
    ) -> Result<Self, W::Error> {
        self.cut(before, work)
    }

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

struct Node<V, A, const N: usize, C, K>(Option<Arc<Body<V, A, N, C, K>>>);

type VisibleBody<'a, V, A, const N: usize, C, K, E> = Result<Option<&'a Body<V, A, N, C, K>>, E>;

struct Body<V, A, const N: usize, C, K> {
    minimum: [u8; N],
    depth: usize,
    kind: Kind<V, A, N, C, K>,
    // Values and child references retire before their original node funding.
    _allocation: A,
}

enum Kind<V, A, const N: usize, C, K> {
    Leaf(V),
    Branch {
        bit: Bit,
        low: Tree<V, A, N, C, K>,
        high: Tree<V, A, N, C, K>,
    },
}

impl<V, A, const N: usize, C, K> Node<V, A, N, C, K> {
    fn body(&self) -> Option<&Body<V, A, N, C, K>> {
        self.0.as_deref()
    }
}

impl<V, A, const N: usize, C, K> Clone for Node<V, A, N, C, K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V, A, const N: usize, C, K> Drop for Node<V, A, N, C, K> {
    fn drop(&mut self) {
        // Retirement keeps constant stack and needs no allocator or fallible
        // admission, whatever the key width or the tree's height. A uniquely
        // held branch whose high child must wait is threaded onto `pending`
        // through its own low slot; its low child is retired next. A pending
        // branch's high child resumes once the low subtree is gone.
        let mut current = self.0.take();
        let mut pending: Option<Arc<Body<V, A, N, C, K>>> = None;
        loop {
            let Some(mut node) = current.take() else {
                let Some(mut branch) = pending.take() else {
                    break;
                };
                let Some(Body {
                    kind: Kind::Branch { low, high, .. },
                    ..
                }) = Arc::get_mut(&mut branch)
                else {
                    unreachable!("a pending branch is uniquely held")
                };
                pending = take_root(low);
                current = take_root(high);
                retire(branch);
                continue;
            };
            let step = Arc::get_mut(&mut node).map(|body| match &mut body.kind {
                Kind::Branch { low, high, .. } => {
                    let next = take_root(low);
                    let waits = high.root.is_some();
                    if waits {
                        low.root = pending.take().map(|link| Node(Some(link)));
                    }
                    (next, waits)
                }
                Kind::Leaf(_) => (None, false),
            });
            match step {
                Some((next, true)) => {
                    pending = Some(node);
                    current = next;
                }
                Some((next, false)) => {
                    retire(node);
                    current = next;
                }
                // Another handle shared this node. into_inner elects exactly
                // one last owner even on concurrent drops. Only when that
                // other handle was released after the uniqueness check does
                // this handle retire the body, through one nested drop of its
                // children; each such frame consumes one concurrent release.
                None => drop(Arc::into_inner(node)),
            }
        }
    }
}

fn take_root<V, A, const N: usize, C, K>(
    tree: &mut Tree<V, A, N, C, K>,
) -> Option<Arc<Body<V, A, N, C, K>>> {
    tree.root.take().and_then(|mut node| node.0.take())
}

/// Releases the physical Arc backing before the body, so the original funding
/// refunds after both backing and value retirement.
fn retire<V, A, const N: usize, C, K>(node: Arc<Body<V, A, N, C, K>>) {
    drop(Arc::into_inner(node));
}

impl<V, A, const N: usize, C, K> Drop for Tree<V, A, N, C, K> {
    fn drop(&mut self) {
        // The last view frees the shared cut's Arc allocation before its
        // original funding retires, so the physical counter never sees an
        // allocation outliving its credit.
        if let Some(before) = self.before.take()
            && let Some(cut) = Arc::into_inner(before)
        {
            drop(cut);
        }
    }
}

impl<V, A, const N: usize, C: Copy, K> Clone for Tree<V, A, N, C, K> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            before: self.before.clone(),
            value_before: self.value_before,
        }
    }
}

impl<V, A, const N: usize, C, K> Default for Tree<V, A, N, C, K> {
    fn default() -> Self {
        Self {
            root: None,
            before: None,
            value_before: None,
        }
    }
}

impl<V, A, const N: usize, C: Copy + Ord, K: KeyCut<N>> Tree<V, A, N, C, K> {
    /// Computes original funding for `insertions` before their keys arrive.
    /// Uses the pinned root's depth plus possible new branches, capped by key
    /// width. Returns `None` on overflow; allocates and traverses nothing.
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
                .checked_mul(u64::try_from(arc_allocation_bytes::<Body<V, A, N, C, K>>()).ok()?)?,
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

    fn cut<W: IndexWork<Allocation = A>>(&self, before: K, work: &mut W) -> Result<Self, W::Error> {
        if self.root.is_none() {
            return Ok(self.clone());
        }
        match &self.before {
            Some(prior) if prior.key <= before => Ok(self.clone()),
            _ => {
                let allocation = work.claim(arc_allocation_bytes::<Cut<K, A>>() as u64)?;
                Ok(Self {
                    root: self.root.clone(),
                    before: Some(Arc::new(Cut {
                        key: before,
                        _allocation: allocation,
                    })),
                    value_before: self.value_before,
                })
            }
        }
    }

    fn cut_shared(&self, before: &Arc<Cut<K, A>>) -> Self {
        match &self.before {
            Some(prior) if prior.key <= before.key => self.clone(),
            _ => Self {
                root: self.root.clone(),
                before: Some(Arc::clone(before)),
                value_before: self.value_before,
            },
        }
    }

    // A keyed sequence composition applies this bound to the value's inner
    // index. It never filters the outer key space or copies an inherited tree.
    pub(crate) fn values_before(&self, before: C) -> Self {
        Self {
            root: self.root.clone(),
            before: self.before.clone(),
            value_before: Some(self.value_before.map_or(before, |prior| prior.min(before))),
        }
    }

    fn body(&self) -> Option<&Body<V, A, N, C, K>> {
        self.root.as_ref().and_then(Node::body)
    }

    // Only the newly allocated insertion path is mutated. Its nodes are
    // present, uniquely owned branches; inherited nodes never enter here.
    fn unpublished_branch(&mut self) -> (&mut usize, &mut Bit, &mut Self, &mut Self) {
        let Some(Node(Some(root))) = &mut self.root else {
            unreachable!("the unpublished insertion path retains its node")
        };
        let Some(Body {
            depth,
            kind: Kind::Branch { bit, low, high },
            ..
        }) = Arc::get_mut(root)
        else {
            unreachable!("the unpublished insertion path owns one branch")
        };
        (depth, bit, low, high)
    }

    fn visible_body<W: IndexReadWork>(
        &self,
        work: &mut W,
    ) -> VisibleBody<'_, V, A, N, C, K, W::Error> {
        if self.root.is_none() {
            return Ok(None);
        }
        work.examine(1 + self.before.as_ref().map_or(0, |_| N as u64))?;
        Ok(self.body().filter(|body| {
            self.before
                .as_ref()
                .is_none_or(|before| body.minimum < *before.key.key())
        }))
    }

    fn child(&self, child: &Self) -> Self {
        let child = self
            .before
            .as_ref()
            .map_or_else(|| child.clone(), |before| child.cut_shared(before));
        match self.value_before {
            Some(before) => child.values_before(before),
            None => child,
        }
    }

    // Never inlined: the body is assembled in this frame before it moves to
    // the heap, so callers' frames stay independent of the key width. Without
    // `minimum`, a branch takes its low child's minimum.
    #[inline(never)]
    fn node<W: IndexWork<Allocation = A>>(
        minimum: Option<&[u8; N]>,
        kind: Kind<V, A, N, C, K>,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let allocation = work.claim(arc_allocation_bytes::<Body<V, A, N, C, K>>() as u64)?;
        let (depth, inherited) = match &kind {
            Kind::Leaf(_) => (0, None),
            Kind::Branch { low, high, .. } => (
                1 + low.depth().max(high.depth()),
                low.body().map(|body| &body.minimum),
            ),
        };
        let Some(minimum) = minimum.or(inherited) else {
            unreachable!("a leaf names its key and a joined branch has a low child")
        };
        Ok(Self {
            root: Some(Node(Some(Arc::new(Body {
                minimum: *minimum,
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
            (Some(_), Some(_)) => Self::node(None, Kind::Branch { bit, low, high }, work),
        }
    }

    pub(crate) fn exact_position<W: IndexReadWork>(
        &self,
        key: &[u8; N],
        work: &mut W,
    ) -> PositionResult<'_, V, A, N, C, K, W::Error> {
        work.validate()?;
        let mut cursor = self;
        let mut before: Option<Arc<Cut<K, A>>> = None;
        let mut value_before = None;
        loop {
            if let Some(cut) = &cursor.before {
                work.examine(N as u64)?;
                if key >= cut.key.key() {
                    return Ok(None);
                }
                if before.is_some() {
                    work.examine(N as u64)?;
                }
                before = Some(match before {
                    Some(prior) if prior.key <= cut.key => prior,
                    _ => Arc::clone(cut),
                });
            }
            if let Some(cut) = cursor.value_before {
                value_before = Some(value_before.map_or(cut, |prior: C| prior.min(cut)));
            }
            let Some(body) = cursor.body() else {
                return Ok(None);
            };
            work.examine(1)?;
            match &body.kind {
                Kind::Leaf(value) => {
                    work.examine(N as u64)?;
                    return Ok((body.minimum == *key).then_some(Position {
                        key: &body.minimum,
                        value,
                        before,
                        value_before,
                    }));
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(key, *bit) { upper } else { low };
                }
            }
        }
    }

    pub(crate) fn predecessor_position<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> PositionResult<'_, V, A, N, C, K, W::Error> {
        work.validate()?;
        let mut cursor = self;
        let mut before = self.before.clone();
        let mut value_before = self.value_before;
        loop {
            let Some(body) = cursor.visible_body(work)? else {
                return Ok(None);
            };
            work.examine(2 * N as u64)?;
            if body.minimum > key
                || before
                    .as_ref()
                    .is_some_and(|before| body.minimum >= *before.key.key())
            {
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
                        upper.minimum <= key
                            && before
                                .as_ref()
                                .is_none_or(|before| upper.minimum < *before.key.key())
                    }) {
                        high
                    } else {
                        low
                    };
                    if let Some(cut) = &cursor.before {
                        before = Some(match before {
                            Some(prior) if prior.key <= cut.key => prior,
                            _ => Arc::clone(cut),
                        });
                    }
                    if let Some(cut) = cursor.value_before {
                        value_before = Some(value_before.map_or(cut, |prior| prior.min(cut)));
                    }
                }
            }
        }
    }

    pub(crate) fn successor_position<W: IndexReadWork>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> PositionResult<'_, V, A, N, C, K, W::Error> {
        work.validate()?;
        let mut cursor = self;
        let mut before: Option<Arc<Cut<K, A>>> = None;
        let mut value_before = None;
        let mut candidate = None;
        loop {
            if let Some(cut) = &cursor.before {
                before = Some(match before {
                    Some(prior) if prior.key <= cut.key => prior,
                    _ => Arc::clone(cut),
                });
            }
            if let Some(cut) = cursor.value_before {
                value_before = Some(value_before.map_or(cut, |prior: C| prior.min(cut)));
            }
            let body = cursor.visible_body(work)?;
            work.examine(2 * N as u64)?;
            match body.filter(|body| {
                before
                    .as_ref()
                    .is_none_or(|cut| key < *cut.key.key() && body.minimum < *cut.key.key())
            }) {
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
                    if let Some(upper) = upper.filter(|upper| {
                        before
                            .as_ref()
                            .is_none_or(|cut| upper.minimum < *cut.key.key())
                    }) {
                        if upper.minimum <= key {
                            cursor = high;
                        } else {
                            candidate = Some((high, before.clone(), value_before));
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
        key: &[u8; N],
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
                    differing = difference(&body.minimum, key);
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
                    cursor = if high(key, *bit) { upper } else { low }.body();
                }
            }
        }
        let leaf = Self::node(Some(key), Kind::Leaf(value), work)?;
        let mut view = self.clone();
        let mut result = Self::default();
        let mut destination = &mut result;
        // Each copied branch is created holding one plus its sibling's depth;
        // the on-path child is still empty. Heights are completed below.
        let mut copied = 0_usize;
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
                    let minimum = if body.minimum <= *key {
                        &body.minimum
                    } else {
                        key
                    };
                    let low = view.child(low);
                    let upper = view.child(upper);
                    let selected_high = high(key, bit);
                    let (selected, sibling) = if selected_high {
                        (upper, low)
                    } else {
                        (low, upper)
                    };
                    if sibling.visible_body(work)?.is_none() {
                        view = selected;
                        continue;
                    }
                    copied += 1;
                    let (low, high) = if selected_high {
                        (sibling, Self::default())
                    } else {
                        (Self::default(), sibling)
                    };
                    *destination =
                        Self::node(Some(minimum), Kind::Branch { bit, low, high }, work)?;
                    let (_, _, low, high) = destination.unpublished_branch();
                    destination = if selected_high { high } else { low };
                    view = selected;
                }
                _ => {
                    *destination = match differing {
                        None => leaf,
                        Some(bit) if high(key, bit) => Self::branch(bit, view, leaf, work)?,
                        Some(bit) => Self::branch(bit, leaf, view, work)?,
                    };
                    break;
                }
            }
        }
        // Heights complete bottom-up without per-branch scratch: the copied
        // path is uniquely owned, so its on-path links are reversed on the way
        // down and restored on the way up. Stack use is independent of the key
        // width and of the tree's height, and nothing is allocated.
        // Both passes are charged before either moves a link, so a refusal
        // never observes a partly reversed path.
        work.charge_items(2 * copied as u64)?;
        let mut above = Self::default();
        let mut cursor = result;
        for _ in 0..copied {
            let (_, bit, low, upper) = cursor.unpublished_branch();
            let child = std::mem::replace(if high(key, *bit) { upper } else { low }, above);
            above = std::mem::replace(&mut cursor, child);
        }
        let mut height = cursor.depth();
        for _ in 0..copied {
            let (depth, bit, low, upper) = above.unpublished_branch();
            // A copied branch holds one plus its sibling's depth.
            height = (*depth).max(1 + height);
            *depth = height;
            let parent = std::mem::replace(if high(key, *bit) { upper } else { low }, cursor);
            cursor = std::mem::replace(&mut above, parent);
        }
        Ok(cursor)
    }
}
