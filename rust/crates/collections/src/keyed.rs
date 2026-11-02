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

/// Entries `(key, sequence)` with `first <= key <= last`, `sequence < before`,
/// and `sequence >= from` under the key `first`; `(first, from)` is the
/// inclusive lower cursor of an ascending read in `(key, sequence)` order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyedRange<const N: usize> {
    /// Least selected key, inclusive.
    pub first: [u8; N],
    /// Greatest selected key, inclusive.
    pub last: [u8; N],
    /// Least selected sequence under `first`, inclusive.
    pub from: u64,
    /// Global exclusive sequence cut.
    pub before: u64,
}

impl<const N: usize> KeyedRange<N> {
    /// The greatest key strictly below an exclusive `end`, if any.
    pub fn below(end: [u8; N]) -> Option<[u8; N]> {
        adjacent(end, false)
    }

    /// Sequences `[from, before)` under one exact key.
    pub fn exact(key: [u8; N], from: u64, before: u64) -> Self {
        Self {
            first: key,
            last: key,
            from,
            before,
        }
    }
}

/// One selected entry, borrowed without a copy.
#[derive(Debug, Eq, PartialEq)]
pub struct KeyedEntry<'a, V, const N: usize> {
    /// The original retained key.
    pub key: &'a [u8; N],
    /// The original sequence position.
    pub sequence: u64,
    /// The original immutable value.
    pub value: &'a V,
}

/// The fixed-width key immediately after (or before) `key`, if any.
fn adjacent<const N: usize>(mut key: [u8; N], up: bool) -> Option<[u8; N]> {
    for byte in key.iter_mut().rev() {
        let (value, carried) = if up {
            byte.overflowing_add(1)
        } else {
            byte.overflowing_sub(1)
        };
        *byte = value;
        if !carried {
            return Some(key);
        }
    }
    None
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
            // Each predecessor branch observes two visible bodies (N + 1
            // each), compares the current minimum (2N), and selects a child
            // (3N). The final leaf costs less than this same per-level bound.
            .checked_mul(width.checked_mul(7)?.checked_add(2)?)?
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
        self.entry(&KeyedRange::exact(key, from, before), false, work)
            .map(|entry| entry.map(|entry| (entry.sequence, entry.value)))
    }

    /// Borrows the nearest entry of `range` in `(key, sequence)` order: the
    /// least one, or the greatest when `descending`. Each step is one bounded
    /// radix descent of the key index and one of that key's sequence index;
    /// a key with no visible sequence in the range costs one further step.
    /// The effective inherited cut is always applied inside this operation.
    pub fn entry<W: IndexReadWork>(
        &self,
        range: &KeyedRange<N>,
        descending: bool,
        work: &mut W,
    ) -> Result<Option<KeyedEntry<'_, V, N>>, W::Error> {
        work.validate()?;
        if range.first > range.last {
            return Ok(None);
        }
        let mut key = if descending { range.last } else { range.first };
        loop {
            let position = if descending {
                self.keys.predecessor_position(key, work)?
            } else {
                self.keys.successor_position(key, work)?
            };
            let Some(position) = position else {
                return Ok(None);
            };
            work.examine(2 * N as u64)?;
            if *position.key < range.first || *position.key > range.last {
                return Ok(None);
            }
            let before = position
                .value_before
                .map_or(range.before, |cut| cut.min(range.before));
            let from = if *position.key == range.first {
                range.from
            } else {
                0
            };
            if from < before {
                let found = if descending {
                    position
                        .value
                        .predecessor((before - 1).to_be_bytes(), work)?
                } else {
                    position.value.successor(from.to_be_bytes(), work)?
                };
                if let Some(found) = found {
                    let sequence = u64::from_be_bytes(*found.key);
                    if (from..before).contains(&sequence) {
                        return Ok(Some(KeyedEntry {
                            key: position.key,
                            sequence,
                            value: found.value,
                        }));
                    }
                }
            }
            if *position.key == if descending { range.first } else { range.last } {
                return Ok(None);
            }
            match adjacent(*position.key, !descending) {
                Some(next) => key = next,
                None => return Ok(None),
            }
        }
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
