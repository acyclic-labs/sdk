//! Exact keys with immutable, globally capped sequence ranges.
use super::persistent::QualifiedIndex;
use super::{IndexInsertionBound, IndexReadWork, IndexWork, PersistentIndex};

/// An original admission failure or an exact binary-key encoding refusal.
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryKeyError<E> {
    /// The original issuing owner's error, preserved without replacement.
    Work(E),
    /// The selected width must include the eight-byte complete length.
    Width(usize),
    /// The input cannot be represented completely at this selected width.
    TooLong {
        /// Actual input bytes.
        observed: usize,
        /// Caller-selected representable input bytes.
        maximum: usize,
    },
}

/// Encodes every binary byte and its complete length without hashing or
/// truncation. Zero padding followed by the big-endian length preserves binary
/// lexical order, including distinct keys with trailing zeroes. `N` is the
/// caller-selected maximum input width plus eight; encoding allocates nothing.
pub fn binary_key<const N: usize, W: IndexReadWork>(
    bytes: &[u8],
    work: &mut W,
) -> Result<[u8; N], BinaryKeyError<W::Error>> {
    work.validate().map_err(BinaryKeyError::Work)?;
    let Some(maximum) = N.checked_sub(8) else {
        return Err(BinaryKeyError::Width(N));
    };
    if bytes.len() > maximum {
        return Err(BinaryKeyError::TooLong {
            observed: bytes.len(),
            maximum,
        });
    }
    work.examine(N as u64).map_err(BinaryKeyError::Work)?;
    let mut key = [0; N];
    key[..bytes.len()].copy_from_slice(bytes);
    key[maximum..].copy_from_slice(&(bytes.len() as u64).to_be_bytes());
    Ok(key)
}

/// One shared persistent radix index maps exact complete keys to another shared
/// radix index of original sequence positions. Forking retains one exclusive
/// sequence cut in O(1); path copying carries it on unchanged outer edges.
/// Inserting into a fork can never reveal discarded records under another key.
///
/// Seek visits bounded fixed-width radix paths, independent of retained key,
/// record or fork population. Insertion copies those same bounded paths, with
/// every new physical node funded by the original `IndexWork` issuer. No value
/// clone, backend request, task, wait or second search algorithm is introduced.
pub struct KeyedIndex<V, A, const N: usize> {
    keys: QualifiedIndex<PersistentIndex<V, A>, A, N>,
}

impl<V, A, const N: usize> Default for KeyedIndex<V, A, N> {
    fn default() -> Self {
        Self {
            keys: QualifiedIndex::default(),
        }
    }
}

impl<V, A, const N: usize> Clone for KeyedIndex<V, A, N> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
        }
    }
}

impl<V, A, const N: usize> KeyedIndex<V, A, N> {
    /// Bounds original node bytes and examinations for `insertions` before
    /// their exact keys are known. Includes original outer lookups and both
    /// radix insertions; binary-key encoding is a separate allocation-free read.
    /// Returns `None` on arithmetic overflow, without allocating or traversing.
    pub fn insertion_bound(&self, insertions: u64) -> Option<IndexInsertionBound> {
        let outer = self.keys.insertion_bound(insertions)?;
        let inner = PersistentIndex::<V, A>::insertion_bound_at_depth(64, insertions)?;
        let width = u64::try_from(N).ok()?;
        let lookup = width
            .checked_mul(8)?
            .min((self.keys.depth() as u64).checked_add(insertions)?)
            .checked_add(1)?
            .checked_mul(width.checked_mul(6)?.checked_add(3)?)?
            .checked_mul(insertions)?;
        Some(IndexInsertionBound {
            allocation_bytes: outer.allocation_bytes.checked_add(inner.allocation_bytes)?,
            examined_items: outer
                .examined_items
                .checked_add(inner.examined_items)?
                .checked_add(lookup)?
                .checked_add(width.checked_mul(insertions)?)?,
        })
    }

    /// Retains all original positions strictly before this global sequence cut.
    /// Cloning and repeated capping allocate nothing and retain the earlier cut.
    pub fn cap(&self, before: u64) -> Self {
        Self {
            keys: self.keys.values_before(before),
        }
    }

    /// Borrows the first original value for this exact key in `[from, before)`.
    /// The effective inherited cut is always applied inside this operation.
    pub fn seek<W: IndexReadWork>(
        &self,
        key: [u8; N],
        from: u64,
        before: u64,
        work: &mut W,
    ) -> Result<Option<(u64, &V)>, W::Error> {
        if from >= before {
            work.validate()?;
            return Ok(None);
        }
        let Some(entry) = self.keys.predecessor_position(key, work)? else {
            return Ok(None);
        };
        work.examine(N as u64)?;
        if entry.key != &key {
            return Ok(None);
        }
        let before = entry.value_before.map_or(before, |cut| cut.min(before));
        if from >= before {
            return Ok(None);
        }
        Ok(entry
            .value
            .successor(from.to_be_bytes(), work)?
            .and_then(|entry| {
                let sequence = u64::from_be_bytes(*entry.key);
                (sequence < before).then_some((sequence, entry.value))
            }))
    }

    /// Prepares a new immutable root; all old roots and cuts remain unchanged on
    /// success, cancellation or refusal. The supplied value is moved once.
    pub fn insert<W: IndexWork<Allocation = A>>(
        &self,
        key: [u8; N],
        sequence: u64,
        value: V,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let entries = match self.keys.predecessor_position(key, work)? {
            Some(entry)
                if {
                    work.charge_items(N as u64)?;
                    entry.key == &key
                } =>
            {
                entry.value_before.map_or_else(
                    || entry.value.clone(),
                    |before| entry.value.prefix(before.to_be_bytes()),
                )
            }
            _ => PersistentIndex::default(),
        };
        let entries = entries.insert(sequence.to_be_bytes(), value, work)?;
        self.keys
            .insert(key, entries, work)
            .map(|keys| Self { keys })
    }
}
