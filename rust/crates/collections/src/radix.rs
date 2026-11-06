//! Allocation-free radix selection over an owner's original node backing.

/// A branch coordinate in a fixed-width unsigned-byte key. Coordinates increase
/// strictly along each path; a width of N bytes therefore admits at most 8N
/// branches, regardless of the number of entries or their physical backing.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BranchBit {
    pub(crate) byte: usize,
    pub(crate) offset: u32,
}

impl BranchBit {
    /// Expands a backing's compact bit position without a key-width truncation.
    pub const fn from_bit(bit: usize) -> Self {
        Self {
            byte: bit / 8,
            offset: (bit % 8) as u32,
        }
    }

    /// Encodes a coordinate for compact original backing, when representable.
    /// Wide byte coordinates remain valid even if this packed form overflows.
    pub fn bit_index(self) -> Option<usize> {
        self.byte.checked_mul(8)?.checked_add(self.offset as usize)
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
                offset: xor.leading_zeros(),
            })
        })
    }
}

/// A borrowed cursor into the original owner's radix backing. A cursor may be
/// an exclusive boxed link or an inventory position; it never owns new storage.
///
/// Implementations must expose the same fixed-width tree on each step, with
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
/// leaf are visited, using constant stack space and no population-dependent work.
pub fn select<C: RadixCursor, E, const N: usize>(
    mut cursor: C,
    key: &[u8; N],
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
        cursor = cursor.child(bit.high(key));
    }
    Ok((cursor, depth))
}
