//! Allocation-free radix selection over an owner's original node backing.

/// A branch coordinate in an original byte and its ordered bit positions.
/// Fixed keys use eight positions per byte; prefix-free binary keys use a
/// continuation position followed by eight data positions. Coordinates increase
/// strictly along each path, independently of the entries' physical backing.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BranchBit {
    pub(crate) byte: usize,
    pub(crate) offset: usize,
}

/// Borrowed interpretation of an original key's branch coordinates. `None`
/// marks a coordinate beyond this key, where selection must stop. Implementors
/// expose a finite, strictly ordered set of coordinates without allocating.
pub trait RadixKey {
    /// The selected side, or the end of this requested key's coordinates.
    fn branch(&self, bit: BranchBit) -> Option<bool>;
}

impl<const N: usize> RadixKey for [u8; N] {
    fn branch(&self, bit: BranchBit) -> Option<bool> {
        self.get(bit.byte)
            .filter(|_| bit.offset < 8)
            .map(|byte| byte & (0x80 >> bit.offset) != 0)
    }
}

/// Prefix-free view of arbitrary binary bytes, including empty keys and trailing
/// zeroes. Each byte has a leading continuation bit; the length ends with a zero
/// continuation bit. No encoded bytes, padding, hashing or second owner exist.
#[derive(Clone, Copy)]
pub struct BinaryKey<'a>(pub &'a [u8]);

impl RadixKey for BinaryKey<'_> {
    fn branch(&self, bit: BranchBit) -> Option<bool> {
        if bit.byte == self.0.len() && bit.offset == 0 {
            return Some(false);
        }
        self.0.get(bit.byte).and_then(|byte| match bit.offset {
            0 => Some(true),
            1..=8 => Some(byte & (0x80 >> (bit.offset - 1)) != 0),
            _ => None,
        })
    }
}

impl BinaryKey<'_> {
    /// At most 9L+1 branches, one selected node and L bytes plus the complete
    /// length comparison. Overflow refuses admission before publication.
    pub fn lookup_examinations(self) -> Option<u64> {
        u64::try_from(self.0.len())
            .ok()?
            .checked_mul(10)?
            .checked_add(3)
    }

    /// Two bounded descents, one complete comparison and one original identity
    /// observation. The bound holds after arbitrary intervening insertions.
    pub fn insertion_examinations(self) -> Option<u64> {
        self.0.len().checked_mul(9)?.checked_add(1)?;
        u64::try_from(self.0.len())
            .ok()?
            .checked_mul(19)?
            .checked_add(6)
    }

    /// First differing original coordinate, or exact equality. Admission covers
    /// every compared byte and the complete lengths before either is inspected.
    pub fn difference<E>(
        self,
        other: Self,
        mut visit: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<BranchBit>, E> {
        let common = self.0.len().min(other.0.len());
        // A borrowed slice's length fits isize, leaving room for the length
        // observation even when its full traversal bound cannot be admitted.
        visit(common as u64 + 1)?;
        for (byte, (a, b)) in self.0.iter().zip(other.0).enumerate() {
            let xor = a ^ b;
            if xor != 0 {
                return Ok(Some(BranchBit {
                    byte,
                    offset: xor.leading_zeros() as usize + 1,
                }));
            }
        }
        Ok((self.0.len() != other.0.len()).then_some(BranchBit {
            byte: common,
            offset: 0,
        }))
    }
}

impl BranchBit {
    /// Expands a compact coordinate in a prefix-free binary key.
    pub const fn from_binary_bit(bit: usize) -> Self {
        Self {
            byte: bit / 9,
            offset: bit % 9,
        }
    }

    /// Packs a prefix-free binary coordinate, refusing representation overflow.
    pub fn binary_bit_index(self) -> Option<usize> {
        self.byte.checked_mul(9)?.checked_add(self.offset)
    }

    /// Expands a backing's compact bit position without a key-width truncation.
    pub const fn from_bit(bit: usize) -> Self {
        Self {
            byte: bit / 8,
            offset: bit % 8,
        }
    }

    /// Encodes a coordinate for compact original backing, when representable.
    /// Wide byte coordinates remain valid even if this packed form overflows.
    pub fn bit_index(self) -> Option<usize> {
        self.byte.checked_mul(8)?.checked_add(self.offset)
    }

    /// Selects the branch side without copying or encoding the original key.
    pub fn high<K: AsRef<[u8]> + ?Sized>(self, key: &K) -> bool {
        key.as_ref()
            .get(self.byte)
            .unwrap_or_else(|| unreachable!("a branch coordinate comes from this key width"))
            & (0x80 >> self.offset)
            != 0
    }

    /// Finds the first differing bit, preserving every original key byte.
    /// The caller accounts for at most N byte comparisons before calling.
    pub fn difference<const N: usize>(a: &[u8; N], b: &[u8; N]) -> Option<Self> {
        a.iter().zip(b).enumerate().find_map(|(byte, (a, b))| {
            let xor = a ^ b;
            (xor != 0).then(|| Self {
                byte,
                offset: xor.leading_zeros() as usize,
            })
        })
    }
}

/// A borrowed cursor into the original owner's radix backing. A cursor may be
/// an exclusive boxed link or an inventory position; it never owns new storage.
///
/// Implementations must expose the same key interpretation on each step, with
/// strictly increasing branch coordinates and nonempty children. `child` moves
/// to exactly the selected original child, without allocating or searching.
pub trait RadixCursor: Sized {
    /// Whether this link is empty. This does not examine a physical node.
    fn is_empty(&self) -> bool;
    /// The current branch coordinate, or None for a leaf or empty link.
    fn branch_bit(&self) -> Option<BranchBit>;
    /// Moves the original cursor to the selected child of its current branch.
    fn child(self, high: bool) -> Self;
}

/// Complete examinations for two insertion descents, a full key comparison
/// and the local selected return. An absent key leaves one branch bit unused
/// on both descents. No backing allocation or value-constructor work is included.
pub const fn insertion_examinations(width: u64) -> Option<u64> {
    match width.checked_mul(17) {
        Some(paths_and_key) => paths_and_key.checked_add(2),
        None => None,
    }
}

/// Selects one leaf, or the subtree at the first coordinate at/after `before`.
/// Every physical node is admitted before inspection, including the selected
/// leaf/subtree. No key comparison, allocation, owner check or wait is hidden
/// here. The caller retains its original admission and compares the complete
/// selected leaf key before returning an exact match.
///
/// The returned depth counts these same visits. At most 8N branches and one
/// leaf are visited for a fixed key. A binary key admits at most 9L+1 branches
/// and a selected leaf or boundary representative. Stack space is constant;
/// no descent beyond the requested key depends on other keys' lengths.
pub fn select<C: RadixCursor, E, K: RadixKey + ?Sized>(
    mut cursor: C,
    key: &K,
    before: Option<BranchBit>,
    mut visit: impl FnMut(u64) -> Result<(), E>,
) -> Result<(C, u64), E> {
    let mut depth = 0;
    while !cursor.is_empty() {
        visit(1)?;
        depth += 1;
        let Some(bit) = cursor
            .branch_bit()
            .filter(|bit| before.is_none_or(|end| *bit < end))
        else {
            break;
        };
        let Some(high) = key.branch(bit) else {
            break;
        };
        cursor = cursor.child(high);
    }
    Ok((cursor, depth))
}
