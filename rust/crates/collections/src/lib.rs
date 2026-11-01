//! Caller-funded indexes and immutable page mechanics shared by primitives.
//!
//! Branch bits strictly increase along every path. Lookup therefore examines
//! at most eight branches per key byte and one leaf, independent of entry count.
//! Key widths share insertion, accounting, lookup and constant-space retirement.

#[cfg(test)]
acyclic_test_support::counting_allocator!();

mod arc;
pub mod page;
pub use arc::arc_allocation_bytes;

mod recent;
pub use recent::RecentIndex;

mod prefix;
pub use prefix::{ExtractedEntries, ExtractedPrefix, PrefixTooLong};

mod persistent;
pub use persistent::{IndexInsertionBound, PersistentIndex, PersistentPosition};

mod keyed;
pub use keyed::{BinaryKeyError, KeyedIndex, binary_key};

/// Original admission for an allocation-free index read. The same issuing
/// owner checks cancellation and accounts for every examined fixed-width key.
pub trait IndexReadWork {
    /// The issuing owner's typed refusal or cancellation.
    type Error;
    /// Checks the original admission even for an empty index.
    fn validate(&mut self) -> Result<(), Self::Error>;
    /// Accounts examined nodes and fixed-width key comparisons before use.
    fn examine(&mut self, items: u64) -> Result<(), Self::Error>;
}

impl<W: IndexWork> IndexReadWork for W {
    type Error = W::Error;

    fn validate(&mut self) -> Result<(), Self::Error> {
        self.check()
    }

    fn examine(&mut self, items: u64) -> Result<(), Self::Error> {
        self.charge_items(items)
    }
}

/// Original caller admission and allocation custody for an index operation.
///
/// Implementations borrow the issuing frame; they must not copy its remaining
/// allowance or replay a private counter after the operation. A successful
/// claim precedes one physical node allocation and returns its retained token.
pub trait IndexWork {
    /// The original typed admission or cancellation error.
    type Error;
    /// Original retained node funding, released on drop without new admission.
    type Allocation;
    /// Checks original cancellation and validity without charging work.
    fn check(&mut self) -> Result<(), Self::Error>;
    /// Admits and records this work on the original cumulative owner.
    fn charge_items(&mut self, items: u64) -> Result<(), Self::Error>;
    /// Admits one node allocation of exactly these bytes before allocation.
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error>;
}

type Link<V, A, const N: usize> = Option<Node<V, A, N>>;

enum Kind<V, A, const N: usize> {
    Leaf {
        key: [u8; N],
        value: V,
    },
    Branch {
        bit: Bit,
        low: Link<V, A, N>,
        high: Link<V, A, N>,
    },
}

struct Node<V, A, const N: usize> {
    kind: Box<Kind<V, A, N>>,
    // The complete backing, including children and values, retires before funding.
    _allocation: A,
}

/// A fixed-width ordered index with caller-funded nodes.
///
/// Branch coordinates strictly increase: one traversal visits at most 8N
/// branches and one leaf. Keys are ordered by their unsigned big-endian bytes.
/// Nodes retain original funding until after their complete backing retires.
pub struct FixedIndex<V, A, const N: usize = 16> {
    root: Link<V, A, N>,
}

/// An extracted leaf retaining its original funding through value destruction.
/// Removing an entry performs no allocation; its box is retired on extraction.
pub struct Entry<V, A, const N: usize> {
    key: [u8; N],
    value: V,
    _allocation: A,
}

impl<V, A, const N: usize> Entry<V, A, N> {
    fn from_leaf(node: Node<V, A, N>) -> Self {
        let Node { kind, _allocation } = node;
        let Kind::Leaf { key, value } = *kind else {
            unreachable!("extraction returns an original leaf")
        };
        Self {
            key,
            value,
            _allocation,
        }
    }

    /// The original fixed-width key.
    pub fn key(&self) -> &[u8; N] {
        &self.key
    }
    /// The retained value, still covered by its original allocation token.
    pub fn value(&self) -> &V {
        &self.value
    }
    /// Transfers value and original token together to the caller.
    /// The caller must destroy physical values before releasing their funding.
    pub fn into_parts(self) -> ([u8; N], V, A) {
        (self.key, self.value, self._allocation)
    }
}

impl<V, A, const N: usize> Default for FixedIndex<V, A, N> {
    fn default() -> Self {
        Self { root: None }
    }
}

// Byte and intra-byte coordinates avoid truncating keys wider than 256 bits.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Bit {
    byte: usize,
    offset: u32,
}

fn high<const N: usize>(key: &[u8; N], bit: Bit) -> bool {
    key.get(bit.byte)
        .unwrap_or_else(|| unreachable!("a branch coordinate comes from this key width"))
        & (0x80 >> bit.offset)
        != 0
}

fn difference<const N: usize>(a: &[u8; N], b: &[u8; N]) -> Option<Bit> {
    a.iter().zip(b).enumerate().find_map(|(byte, (a, b))| {
        let xor = a ^ b;
        (xor != 0).then(|| Bit {
            byte,
            offset: xor.leading_zeros(),
        })
    })
}

// An unlinked admission can be retired through the original accounting
// context. In particular, standalone ledgers also release failed admissions.
struct Funding<A> {
    leaf: A,
    branch: Option<A>,
}

impl<A> Funding<A> {
    fn admit<V, W: IndexWork<Allocation = A>, const N: usize>(
        branch: bool,
        accounting: &mut W,
    ) -> Result<Self, W::Error> {
        let bytes = std::mem::size_of::<Kind<V, A, N>>() as u64;
        let leaf = accounting.claim(bytes)?;
        let branch = if branch {
            match accounting.claim(bytes) {
                Ok(funding) => Some(funding),
                Err(error) => {
                    drop(leaf);
                    return Err(error);
                }
            }
        } else {
            None
        };
        Ok(Self { leaf, branch })
    }
}

impl<V, A, const N: usize> FixedIndex<V, A, N> {
    /// Bounds node backing and examined items for unknown-key insertions.
    ///
    /// This allocation-free bound is independent of the current root, so it
    /// remains valid when other serialized mutations precede the insertions.
    /// Each insertion claims at most one leaf and one branch. Three paths
    /// examine at most 8N branches each, plus complete key comparisons.
    /// The value constructor's own work and backing are excluded. Actual node
    /// claims and examinations still use the original `IndexWork`; this bound
    /// neither issues admission nor replaces its cancellation check.
    pub fn insertion_bound(insertions: u64) -> Option<IndexInsertionBound> {
        let node_bytes = u64::try_from(std::mem::size_of::<Kind<V, A, N>>()).ok()?;
        let width = u64::try_from(N).ok()?;
        Some(IndexInsertionBound {
            allocation_bytes: node_bytes.checked_mul(2)?.checked_mul(insertions)?,
            examined_items: width
                .checked_mul(26)?
                .checked_add(3)?
                .checked_mul(insertions)?,
        })
    }

    /// Reads one exact key without allocation. Checks admission even when empty.
    pub fn get<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        accounting: &mut W,
    ) -> Result<Option<&V>, W::Error> {
        accounting.check()?;
        let mut cursor = self.root.as_ref();
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match node.kind.as_ref() {
                Kind::Leaf {
                    key: observed,
                    value,
                } => {
                    accounting.charge_items(N as u64)?;
                    return Ok((*observed == key).then_some(value));
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&key, *bit) { upper } else { low }.as_ref();
                }
            }
        }
        Ok(None)
    }

    /// Borrows the first unsigned-byte-ordered entry without allocation.
    /// Original admission is checked even when the index is empty.
    pub fn first<W: IndexWork<Allocation = A>>(
        &self,
        accounting: &mut W,
    ) -> Result<Option<(&[u8; N], &V)>, W::Error> {
        accounting.check()?;
        let mut cursor = self.root.as_ref();
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match node.kind.as_ref() {
                Kind::Leaf { key, value } => return Ok(Some((key, value))),
                Kind::Branch { low, .. } => cursor = low.as_ref(),
            }
        }
        Ok(None)
    }

    /// Mutably borrows an existing exact value without constructing or funding
    /// a registration. Refusal leaves the value and its original custody intact.
    pub fn get_mut<W: IndexWork<Allocation = A>>(
        &mut self,
        key: [u8; N],
        accounting: &mut W,
    ) -> Result<Option<&mut V>, W::Error> {
        accounting.check()?;
        self.existing_mut(key, |items| accounting.charge_items(items))
    }

    /// Mutably borrows an existing value for its exact retained owner.
    ///
    /// The caller serializes the original owner's lifetime and supplies its
    /// retained observation sink. This path cannot admit new work or funding,
    /// check cancellation, allocate, insert, or transfer node custody. It lets
    /// wake and retirement finish after their original admission is cancelled.
    /// The owner predicate runs only after the complete key matches and before
    /// exposing the mutable value. A replacement owner is never returned.
    /// At most 8N branches, one leaf and N key bytes are observed.
    pub fn get_retained_mut(
        &mut self,
        key: [u8; N],
        exact_owner: impl FnOnce(&V) -> bool,
        mut visit: impl FnMut(u64),
    ) -> Option<&mut V> {
        let result = self.existing_mut(key, |items| {
            visit(items);
            Ok::<(), std::convert::Infallible>(())
        });
        match result {
            Ok(value) => value.filter(|value| exact_owner(value)),
            Err(impossible) => match impossible {},
        }
    }

    fn existing_mut<E>(
        &mut self,
        key: [u8; N],
        mut visit: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&mut V>, E> {
        let mut cursor = self.root.as_mut();
        while let Some(node) = cursor {
            visit(1)?;
            match node.kind.as_mut() {
                Kind::Leaf {
                    key: observed,
                    value,
                } => {
                    visit(N as u64)?;
                    return Ok((*observed == key).then_some(value));
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&key, *bit) { upper } else { low }.as_mut();
                }
            }
        }
        Ok(None)
    }

    /// Borrows the least key strictly greater than `after` without allocating.
    /// The cursor is the complete original key, never an ordinal or cache hint.
    /// At most three bounded-depth paths and N comparison bytes are visited;
    /// work does not depend on the number of keys before or after the cursor.
    pub fn seek_after<W: IndexWork<Allocation = A>>(
        &self,
        after: [u8; N],
        accounting: &mut W,
    ) -> Result<Option<(&[u8; N], &V)>, W::Error> {
        let (differing, _) = self.differing_bit(after, accounting)?;
        let Some(differing) = differing else {
            return Ok(None);
        };
        let mut cursor = self.root.as_ref();
        let mut successor = None;
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match node.kind.as_ref() {
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } if differing.is_none_or(|difference| *bit < difference) => {
                    cursor = if high(&after, *bit) {
                        upper.as_ref()
                    } else {
                        successor = upper.as_ref();
                        low.as_ref()
                    };
                }
                _ => {
                    // At the first omitted differing bit this whole subtree
                    // is on the observed leaf's side. No descendant can cross
                    // it. An exact match instead uses the saved upper subtree.
                    if differing.is_some_and(|bit| !high(&after, bit)) {
                        successor = Some(node);
                    }
                    break;
                }
            }
        }
        while let Some(node) = successor {
            accounting.charge_items(1)?;
            match node.kind.as_ref() {
                Kind::Leaf { key, value } => return Ok(Some((key, value))),
                Kind::Branch { low, .. } => successor = low.as_ref(),
            }
        }
        Ok(None)
    }

    /// Returns the existing value or constructs it after both node admissions.
    /// Every fallible charge precedes link publication; refusal leaves the index
    /// unchanged and retires all temporary tokens. Existing keys never construct.
    pub fn get_or_try_insert_with<W: IndexWork<Allocation = A>>(
        &mut self,
        key: [u8; N],
        accounting: &mut W,
        create: impl FnOnce(&mut W) -> Result<V, W::Error>,
    ) -> Result<&mut V, W::Error> {
        let (differing, depth) = self.differing_bit(key, accounting)?;
        if differing == Some(None) {
            accounting.charge_items(depth)?;
            return Ok(self.value_mut(key));
        }
        let (link, return_depth) = match differing.flatten() {
            Some(bit) => {
                let (link, depth) = self.insertion_link(key, bit, accounting)?;
                (link, depth + 1)
            }
            None => (&mut self.root, 1),
        };
        // Pay the final lookup before publishing the new link, so no budget
        // failure can turn a successful insertion into a rejected operation.
        accounting.charge_items(return_depth + N as u64)?;
        let funding = Funding::admit::<V, W, N>(differing.flatten().is_some(), accounting)?;
        let value = match create(accounting) {
            Ok(value) => value,
            Err(error) => {
                drop(funding);
                return Err(error);
            }
        };
        let Funding {
            leaf: leaf_funding,
            branch: branch_funding,
        } = funding;
        let leaf = Node {
            kind: Box::new(Kind::Leaf { key, value }),
            _allocation: leaf_funding,
        };
        let replacement = match (differing.flatten(), branch_funding) {
            (Some(bit), Some(allocation)) => {
                let mut branch = Node {
                    kind: Box::new(Kind::Branch {
                        bit,
                        low: None,
                        high: None,
                    }),
                    _allocation: allocation,
                };
                let Kind::Branch {
                    low, high: upper, ..
                } = branch.kind.as_mut()
                else {
                    unreachable!("new branch has branch kind")
                };
                if high(&key, bit) {
                    *upper = Some(leaf);
                    *low = link.take();
                } else {
                    *low = Some(leaf);
                    *upper = link.take();
                }
                branch
            }
            (None, None) => leaf,
            _ => unreachable!("nonempty table funds exactly one branch"),
        };
        *link = Some(replacement);
        Ok(self.value_mut(key))
    }

    fn differing_bit<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        accounting: &mut W,
    ) -> Result<(Option<Option<Bit>>, u64), W::Error> {
        accounting.check()?;
        let mut cursor = self.root.as_ref();
        let mut depth = 0;
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            depth += 1;
            match node.kind.as_ref() {
                Kind::Leaf { key: observed, .. } => {
                    let bytes = N as u64;
                    accounting.charge_items(bytes)?;
                    return Ok((Some(difference(&key, observed)), depth + bytes));
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&key, *bit) { upper } else { low }.as_ref();
                }
            }
        }
        Ok((None, depth))
    }

    fn insertion_link<W: IndexWork<Allocation = A>>(
        &mut self,
        key: [u8; N],
        before: Bit,
        accounting: &mut W,
    ) -> Result<(&mut Link<V, A, N>, u64), W::Error> {
        let mut cursor = &mut self.root;
        let mut depth = 0;
        loop {
            accounting.charge_items(1)?;
            depth += 1;
            let descend = matches!(cursor.as_ref().map(|node| node.kind.as_ref()),
                Some(Kind::Branch { bit, .. }) if *bit < before);
            if !descend {
                return Ok((cursor, depth));
            }
            match cursor.as_mut().map(|node| node.kind.as_mut()) {
                Some(Kind::Branch {
                    bit,
                    low,
                    high: upper,
                }) => {
                    cursor = if high(&key, *bit) { upper } else { low };
                }
                _ => unreachable!("descent selects an existing branch"),
            }
        }
    }

    fn value_mut(&mut self, key: [u8; N]) -> &mut V {
        let mut cursor = self.root.as_mut();
        while let Some(node) = cursor {
            match node.kind.as_mut() {
                Kind::Leaf {
                    key: observed,
                    value,
                } if *observed == key => return value,
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&key, *bit) { upper } else { low }.as_mut();
                }
                Kind::Leaf { .. } => break,
            }
        }
        unreachable!("lookup follows a leaf already found or inserted")
    }
    /// Extracts an exact matching owner and its original node funding.
    /// This allocation-free cleanup cannot refuse admission after cancellation.
    /// The predicate prevents an old owner from removing its replacement.
    /// Two traversals visit at most 16N branches, independent of population.
    pub fn remove_if(
        &mut self,
        key: [u8; N],
        matches: impl FnOnce(&V) -> bool,
    ) -> Option<Entry<V, A, N>> {
        let mut cursor = self.root.as_ref();
        let mut parent = None;
        loop {
            match cursor.map(|node| node.kind.as_ref()) {
                Some(Kind::Branch {
                    bit,
                    low,
                    high: upper,
                }) => {
                    parent = Some(*bit);
                    cursor = if high(&key, *bit) { upper } else { low }.as_ref();
                }
                Some(Kind::Leaf {
                    key: observed,
                    value,
                }) if *observed == key => {
                    if !matches(value) {
                        return None;
                    }
                    break;
                }
                _ => return None,
            }
        }
        let mut extracted = self
            .extract_at(parent, |bit| high(&key, bit), |_| {})
            .into_index();
        let retired = extracted
            .root
            .take()
            .unwrap_or_else(|| unreachable!("verified registration"));
        Some(Entry::from_leaf(retired))
    }

    /// Retires one exact owner, destroying its value before its original token.
    /// Like extraction and whole-index retirement, this cannot refuse admission.
    pub fn retire_if(&mut self, key: [u8; N], matches: impl FnOnce(&V) -> bool) -> bool {
        self.remove_if(key, matches).is_some()
    }
}

impl<V, A, const N: usize> Drop for FixedIndex<V, A, N> {
    fn drop(&mut self) {
        while let Some(leaf) = take_first_leaf(&mut self.root, &mut |_| {}) {
            drop(leaf);
        }
    }
}

// The same constant-space retirement cursor serves whole-index Drop and owned
// prefix iteration. Rotations preserve leaf order; no index lookup may observe
// this cursor after its first rotation. Each rotation exposes one branch for
// retirement, and every leaf and branch retires once. No key/value copy, stack,
// recursion or allocation is needed when the caller only destroys the index.
fn take_first_leaf<V, A, const N: usize>(
    cursor: &mut Link<V, A, N>,
    visit: &mut impl FnMut(u64),
) -> Option<Node<V, A, N>> {
    while let Some(mut node) = cursor.take() {
        visit(1);
        match node.kind.as_mut() {
            Kind::Leaf { .. } => return Some(node),
            Kind::Branch { low, high, .. } => match low.take() {
                None => *cursor = high.take(),
                Some(mut lower) => {
                    visit(1);
                    match lower.kind.as_mut() {
                        Kind::Leaf { .. } => {
                            *cursor = Some(node);
                            return Some(lower);
                        }
                        Kind::Branch { high: middle, .. } => {
                            *low = middle.take();
                            *middle = Some(node);
                            *cursor = Some(lower);
                        }
                    }
                }
            },
        }
    }
    None
}

#[cfg(test)]
mod tests;
