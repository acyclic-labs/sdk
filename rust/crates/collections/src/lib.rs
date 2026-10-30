//! An admitted, fixed-width index shared by primitive consumers.
//!
//! Branch bits strictly increase along every path. Lookup therefore examines
//! at most eight branches per key byte and one leaf, independent of entry count.
//! Key widths share insertion, accounting, lookup and constant-space retirement.

mod recent;
pub use recent::RecentIndex;

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

type Link<V, A, const N: usize> = Option<Box<Node<V, A, N>>>;

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
    kind: Kind<V, A, N>,
    // Children and values must be destroyed before their funding.
    _allocation: A,
}

/// A fixed-width ordered index with caller-funded nodes.
///
/// Branch coordinates strictly increase: one traversal visits at most 8N
/// branches and one leaf. Keys are ordered by their unsigned big-endian bytes.
/// Nodes retain the original allocation token until after their values drop.
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
        let bytes = std::mem::size_of::<Node<V, A, N>>() as u64;
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
    /// Reads one exact key without allocation. Checks admission even when empty.
    pub fn get<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        accounting: &mut W,
    ) -> Result<Option<&V>, W::Error> {
        accounting.check()?;
        let mut cursor = self.root.as_deref();
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match &node.kind {
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
                    cursor = if high(&key, *bit) { upper } else { low }.as_deref();
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
        let mut cursor = self.root.as_deref();
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match &node.kind {
                Kind::Leaf { key, value } => return Ok(Some((key, value))),
                Kind::Branch { low, .. } => cursor = low.as_deref(),
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
        let leaf = Box::new(Node {
            kind: Kind::Leaf { key, value },
            _allocation: leaf_funding,
        });
        let replacement = match (differing.flatten(), branch_funding) {
            (Some(bit), Some(allocation)) => {
                let mut branch = Box::new(Node {
                    kind: Kind::Branch {
                        bit,
                        low: None,
                        high: None,
                    },
                    _allocation: allocation,
                });
                let Kind::Branch {
                    low, high: upper, ..
                } = &mut branch.kind
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
        let mut cursor = self.root.as_deref();
        let mut depth = 0;
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            depth += 1;
            match &node.kind {
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
                    cursor = if high(&key, *bit) { upper } else { low }.as_deref();
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
            let descend = matches!(cursor.as_deref(), Some(Node {
                kind: Kind::Branch { bit, .. }, ..
            }) if *bit < before);
            if !descend {
                return Ok((cursor, depth));
            }
            match cursor.as_deref_mut() {
                Some(Node {
                    kind:
                        Kind::Branch {
                            bit,
                            low,
                            high: upper,
                        },
                    ..
                }) => {
                    cursor = if high(&key, *bit) { upper } else { low };
                }
                _ => unreachable!("descent selects an existing branch"),
            }
        }
    }

    fn value_mut(&mut self, key: [u8; N]) -> &mut V {
        let mut cursor = self.root.as_deref_mut();
        while let Some(node) = cursor {
            match &mut node.kind {
                Kind::Leaf {
                    key: observed,
                    value,
                } if *observed == key => return value,
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&key, *bit) { upper } else { low }.as_deref_mut();
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
        let mut cursor = self.root.as_deref();
        let mut parent = None;
        loop {
            match cursor.map(|node| &node.kind) {
                Some(Kind::Branch {
                    bit,
                    low,
                    high: upper,
                }) => {
                    parent = Some(*bit);
                    cursor = if high(&key, *bit) { upper } else { low }.as_deref();
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
        let mut link = &mut self.root;
        if let Some(parent) = parent {
            loop {
                match link.as_deref() {
                    Some(Node {
                        kind: Kind::Branch { bit, .. },
                        ..
                    }) if *bit == parent => break,
                    _ => {}
                }
                match link.as_deref_mut() {
                    Some(Node {
                        kind:
                            Kind::Branch {
                                bit,
                                low,
                                high: upper,
                            },
                        ..
                    }) => {
                        link = if high(&key, *bit) { upper } else { low };
                    }
                    _ => unreachable!("retirement follows the verified parent"),
                }
            }
        }
        let mut retired = link
            .take()
            .unwrap_or_else(|| unreachable!("verified registration"));
        if let Kind::Branch {
            bit,
            low,
            high: upper,
        } = &mut retired.kind
        {
            let (selected, sibling) = if high(&key, *bit) {
                (upper, low)
            } else {
                (low, upper)
            };
            *link = sibling.take();
            let leaf = selected
                .take()
                .unwrap_or_else(|| unreachable!("verified leaf"));
            drop(retired);
            retired = leaf;
        }
        let Node { kind, _allocation } = *retired;
        let Kind::Leaf { key, value } = kind else {
            unreachable!("extraction detaches exactly the verified leaf")
        };
        Some(Entry {
            key,
            value,
            _allocation,
        })
    }

    /// Retires one exact owner, destroying its value before its original token.
    /// Like extraction and whole-index retirement, this cannot refuse admission.
    pub fn retire_if(&mut self, key: [u8; N], matches: impl FnOnce(&V) -> bool) -> bool {
        self.remove_if(key, matches).is_some()
    }
}

impl<V, A, const N: usize> Drop for FixedIndex<V, A, N> {
    fn drop(&mut self) {
        // Rotate low branches onto the cursor, then detach their high link
        // before retirement. Each rotation exposes one branch for retirement;
        // each leaf and branch is dropped once. No stack, recursion or allocation
        // depends on the key width or the number of retained identities.
        let mut cursor = self.root.take();
        while let Some(mut node) = cursor {
            match &mut node.kind {
                Kind::Leaf { .. } => cursor = None,
                Kind::Branch { low, high, .. } => match low.take() {
                    None => cursor = high.take(),
                    Some(mut lower) => match &mut lower.kind {
                        Kind::Leaf { .. } => {
                            drop(lower);
                            cursor = Some(node);
                        }
                        Kind::Branch { high: middle, .. } => {
                            *low = middle.take();
                            *middle = Some(node);
                            cursor = Some(lower);
                        }
                    },
                },
            }
        }
    }
}

#[cfg(test)]
mod tests;
