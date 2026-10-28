//! An admitted, fixed-width index for persistent kernel ownership.
//!
//! Branch bits strictly increase along every path. Lookup therefore examines
//! at most eight branches per key byte and one leaf, independent of entry count.
//! Key widths share insertion, accounting, lookup and constant-space retirement.
use crate::FsError;
use crate::kernel::KernelAccounting;
use crate::kernel::allocation::retained::RetainedAllocation;

mod recent;
pub(crate) use recent::RecentIndex;

type Link<V, const N: usize> = Option<Box<Node<V, N>>>;

enum Kind<V, const N: usize> {
    Leaf {
        key: [u8; N],
        value: V,
    },
    Branch {
        bit: Bit,
        low: Link<V, N>,
        high: Link<V, N>,
    },
}

struct Node<V, const N: usize> {
    kind: Kind<V, N>,
    // Children and values must be destroyed before their funding.
    _allocation: RetainedAllocation,
}

pub(crate) struct FixedIndex<V, const N: usize = 16> {
    root: Link<V, N>,
}

impl<V, const N: usize> Default for FixedIndex<V, N> {
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
struct Funding {
    leaf: RetainedAllocation,
    branch: Option<RetainedAllocation>,
}

impl Funding {
    fn admit<V, const N: usize>(
        branch: bool,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let bytes = std::mem::size_of::<Node<V, N>>() as u64;
        let leaf = RetainedAllocation::claim(bytes, 1, accounting)?;
        let branch = if branch {
            match RetainedAllocation::claim(bytes, 1, accounting) {
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

impl<V, const N: usize> FixedIndex<V, N> {
    pub(crate) fn get(
        &self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&V>, FsError> {
        accounting.check()?;
        let mut cursor = self.root.as_deref();
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            match &node.kind {
                Kind::Leaf {
                    key: observed,
                    value,
                } => {
                    accounting.charge_items(crate::foundation::usize_to_u64(N))?;
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

    pub(crate) fn get_or_try_insert_with(
        &mut self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
        create: impl FnOnce(&mut KernelAccounting<'_>) -> Result<V, FsError>,
    ) -> Result<&mut V, FsError> {
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
        accounting.charge_items(return_depth + crate::foundation::usize_to_u64(N))?;
        let funding = Funding::admit::<V, N>(differing.flatten().is_some(), accounting)?;
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

    fn differing_bit(
        &self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Option<Option<Bit>>, u64), FsError> {
        let mut cursor = self.root.as_deref();
        let mut depth = 0;
        while let Some(node) = cursor {
            accounting.charge_items(1)?;
            depth += 1;
            match &node.kind {
                Kind::Leaf { key: observed, .. } => {
                    let bytes = crate::foundation::usize_to_u64(N);
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

    fn insertion_link(
        &mut self,
        key: [u8; N],
        before: Bit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(&mut Link<V, N>, u64), FsError> {
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
}

impl<V, const N: usize> Drop for FixedIndex<V, N> {
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
