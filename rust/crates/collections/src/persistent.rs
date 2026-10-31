//! Immutable prefix views over the shared fixed-width branch coordinates.
use super::{Bit, IndexWork, arc_allocation_bytes, difference, high};
use std::sync::Arc;

/// A caller-funded immutable ordered index. Cloning or taking a prefix shares
/// the original nodes without allocation. Insertion copies one bounded path;
/// predecessor lookup makes at most eight branch decisions per key byte,
/// inspecting each branch's child minimum before visiting one leaf.
/// Neither operation depends on fork depth or the retained population.
pub struct PersistentIndex<V, A, const N: usize = 8> {
    root: Option<Node<V, A, N>>,
    before: Option<[u8; N]>,
}

struct Node<V, A, const N: usize>(Option<Arc<Body<V, A, N>>>);

struct Body<V, A, const N: usize> {
    minimum: [u8; N],
    kind: Kind<V, A, N>,
    // Values and child references retire before their original node funding.
    _allocation: A,
}

enum Kind<V, A, const N: usize> {
    Leaf(V),
    Branch {
        bit: Bit,
        low: PersistentIndex<V, A, N>,
        high: PersistentIndex<V, A, N>,
    },
}

impl<V, A, const N: usize> Node<V, A, N> {
    fn body(&self) -> &Body<V, A, N> {
        self.0
            .as_deref()
            .expect("a live node owns its original body")
    }
}

impl<V, A, const N: usize> Clone for Node<V, A, N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V, A, const N: usize> Drop for Node<V, A, N> {
    fn drop(&mut self) {
        // All references are these private handles; there are no weak handles.
        // into_inner elects exactly one last owner even on concurrent drops,
        // and releases the physical Arc backing before returning its body.
        // Its token therefore refunds after both backing and value retirement.
        if let Some(body) = self.0.take().and_then(Arc::into_inner) {
            drop(body);
        }
    }
}

impl<V, A, const N: usize> Clone for PersistentIndex<V, A, N> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            before: self.before,
        }
    }
}

impl<V, A, const N: usize> Default for PersistentIndex<V, A, N> {
    fn default() -> Self {
        Self {
            root: None,
            before: None,
        }
    }
}

impl<V, A, const N: usize> PersistentIndex<V, A, N> {
    /// Pins the original keys strictly before this exclusive cut. Repeated
    /// prefixing keeps the earlier cut, without a chain of historical views.
    pub fn prefix(&self, before: [u8; N]) -> Self {
        let before = self.before.map_or(before, |prior| prior.min(before));
        Self {
            root: self.root.clone(),
            before: Some(before),
        }
    }

    fn body(&self) -> Option<&Body<V, A, N>> {
        self.root.as_ref().map(Node::body)
    }

    fn visible_body<W: IndexWork<Allocation = A>>(
        &self,
        work: &mut W,
    ) -> Result<Option<&Body<V, A, N>>, W::Error> {
        if self.root.is_none() {
            return Ok(None);
        }
        work.charge_items(1 + N as u64)?;
        Ok(self
            .body()
            .filter(|body| self.before.is_none_or(|before| body.minimum < before)))
    }

    fn child(&self, child: &Self) -> Self {
        self.before
            .map_or_else(|| child.clone(), |before| child.prefix(before))
    }

    fn node<W: IndexWork<Allocation = A>>(
        minimum: [u8; N],
        kind: Kind<V, A, N>,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let allocation = work.claim(arc_allocation_bytes::<Body<V, A, N>>() as u64)?;
        Ok(Self {
            root: Some(Node(Some(Arc::new(Body {
                minimum,
                kind,
                _allocation: allocation,
            })))),
            before: None,
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

    /// Borrows the greatest retained key at or below `key`. Checks the original
    /// admission even on an empty view. It allocates and copies no values.
    pub fn predecessor<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        work: &mut W,
    ) -> Result<Option<(&[u8; N], &V)>, W::Error> {
        work.check()?;
        let mut cursor = self;
        let mut before = self.before;
        loop {
            let Some(body) = cursor.visible_body(work)? else {
                return Ok(None);
            };
            work.charge_items(2 * N as u64)?;
            if body.minimum > key || before.is_some_and(|before| body.minimum >= before) {
                return Ok(None);
            }
            match &body.kind {
                Kind::Leaf(value) => return Ok(Some((&body.minimum, value))),
                Kind::Branch { low, high, .. } => {
                    let upper = high.visible_body(work)?;
                    work.charge_items(3 * N as u64)?;
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
                }
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
        self.insert_path(key, differing, leaf, work)
    }

    fn insert_path<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        differing: Option<Bit>,
        leaf: Self,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let Some(body) = self.visible_body(work)? else {
            return Ok(leaf);
        };
        match &body.kind {
            Kind::Branch {
                bit,
                low,
                high: upper,
            } if differing.is_none_or(|difference| *bit < difference) => {
                work.charge_items(2 * N as u64)?;
                let low = self.child(low);
                let upper = self.child(upper);
                if high(&key, *bit) {
                    Self::branch(
                        *bit,
                        low,
                        upper.insert_path(key, differing, leaf, work)?,
                        work,
                    )
                } else {
                    Self::branch(
                        *bit,
                        low.insert_path(key, differing, leaf, work)?,
                        upper,
                        work,
                    )
                }
            }
            _ => {
                let Some(bit) = differing else {
                    return Ok(leaf);
                };
                if high(&key, bit) {
                    Self::branch(bit, self.clone(), leaf, work)
                } else {
                    Self::branch(bit, leaf, self.clone(), work)
                }
            }
        }
    }
}
