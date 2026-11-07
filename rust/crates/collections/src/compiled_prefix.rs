//! Immutable, variable-width byte prefixes compiled into one flat radix table.
use super::{IndexReadWork, IndexWork};
use std::ops::Range;

/// A stable borrowed byte view of an immutable entry. Implementations must
/// return the same bytes throughout the index's lifetime. The view's own cost
/// is the caller's responsibility, just like the prefix acceptance predicate.
pub trait PrefixKey<V> {
    /// Borrows the entry's key without allocating or copying it.
    fn bytes<'a>(&self, entry: &'a V) -> &'a [u8];
}

impl<V, F: Fn(&V) -> &[u8]> PrefixKey<V> for F {
    fn bytes<'a>(&self, entry: &'a V) -> &'a [u8] {
        self(entry)
    }
}

/// The zero-sized key view used by already sorted byte entries.
#[derive(Debug)]
pub struct EntryBytes;

impl<V: AsRef<[u8]>> PrefixKey<V> for EntryBytes {
    fn bytes<'a>(&self, entry: &'a V) -> &'a [u8] {
        entry.as_ref()
    }
}

/// A compilation or lookup refusal, before publishing any result.
#[derive(Debug)]
pub enum CompiledPrefixError<E> {
    /// The original caller's admission error.
    Work(E),
    /// Keys must be distinct; `from_sorted` also requires unsigned byte order.
    /// The position identifies an entry in the original input vector.
    Order(usize),
    /// The supplied table cannot be represented by the native address space.
    Capacity,
    /// The allocator refused the admitted node backing.
    Allocation(std::collections::TryReserveError),
    /// An internal coordinate is inconsistent with the retained key view.
    Invariant,
}

#[derive(Debug)]
struct Node {
    key: usize,
    end: usize,
    children: Range<usize>,
}

/// A matched entry and its position in the original input vector.
#[derive(Debug)]
pub struct PrefixMatch<'a, V> {
    /// The original entry, borrowed without copying its key or value.
    pub entry: &'a V,
    /// The entry's position in [`CompiledPrefixIndex::entries`].
    pub position: usize,
}

/// Exact and shortest accepted proper-prefix matches from one traversal.
#[derive(Debug)]
pub struct PrefixMatches<'a, V> {
    /// The complete byte key, if present, independently of prefix acceptance.
    pub exact: Option<PrefixMatch<'a, V>>,
    /// The shortest proper prefix accepted by the caller's boundary predicate.
    pub shortest: Option<PrefixMatch<'a, V>>,
}

/// A compiled radix table for immutable, arbitrary-length byte keys.
///
/// Keys and values stay in the caller's original vector without copying. One
/// admitted allocation holds at most two nodes per entry. Labels borrow the
/// original keys; children have distinct first bytes, hence at most 256 edges.
/// Lookup compares disjoint label bytes and at most nine edge bytes per level:
/// O(query bytes), independently of the number or lengths of unrelated keys.
/// Compilation and destruction are iterative; key depth never consumes stack.
///
/// Unlike mutable indexes this table needs no node allocation or retirement per
/// edit: compile a new immutable policy when its original owner changes it.
#[derive(Debug)]
pub struct CompiledPrefixIndex<V, A, K = EntryBytes> {
    entries: Vec<V>,
    nodes: Vec<Node>,
    key: K,
    // All entries and physical backing retire before their original funding.
    _allocation: Option<A>,
}

impl<V: AsRef<[u8]>, A> CompiledPrefixIndex<V, A> {
    /// Takes an already sorted, distinct key vector. The caller retains funding
    /// responsibility for that original vector and its values; `work` funds the
    /// one additional node table until after its physical destruction. Checks
    /// key order once at this boundary. No maximum key width is imposed.
    pub fn from_sorted<W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        work: &mut W,
    ) -> Result<Self, CompiledPrefixError<W::Error>> {
        work.check().map_err(CompiledPrefixError::Work)?;
        Self::compile(entries, EntryBytes, Some, work)
    }
}

impl<V, A> CompiledPrefixIndex<V, A> {
    /// Compiles distinct borrowed keys while retaining the exact original
    /// vector and entry order. `work` funds a temporary sort permutation and
    /// the retained node table. The permutation retires before return; keys
    /// and values are never copied. The key view must remain stable.
    pub fn from_entries<K: Fn(&V) -> &[u8], W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        key: K,
        work: &mut W,
    ) -> Result<CompiledPrefixIndex<V, A, K>, CompiledPrefixError<W::Error>> {
        use CompiledPrefixError::{Allocation, Capacity, Work};
        work.check().map_err(Work)?;
        let bytes = entries
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or(Capacity)?;
        let allocation = if bytes == 0 {
            None
        } else {
            Some(work.claim(bytes as u64).map_err(Work)?)
        };
        // Declared after its funding so backing retires first on every error.
        let mut order = Vec::new();
        order.try_reserve_exact(entries.len()).map_err(Allocation)?;
        work.charge_items(entries.len() as u64).map_err(Work)?;
        order.extend(0..entries.len());
        sort(&mut order, &mut |&left, &right| {
            let left = key.bytes(entries.get(left).ok_or(CompiledPrefixError::Invariant)?);
            let right = key.bytes(entries.get(right).ok_or(CompiledPrefixError::Invariant)?);
            work.charge_items(left.len().min(right.len()) as u64 + 1)
                .map_err(Work)?;
            Ok(left.cmp(right))
        })?;
        let index = CompiledPrefixIndex::compile(
            entries,
            key,
            |position| order.get(position).copied(),
            work,
        )?;
        drop(order);
        drop(allocation);
        Ok(index)
    }
}

// In-place heapsort permits an immediate typed refusal before each comparison.
// An infallible comparator cannot stop work safely when its admission is lost.
fn sort<T, E>(
    items: &mut [T],
    compare: &mut impl FnMut(&T, &T) -> Result<std::cmp::Ordering, CompiledPrefixError<E>>,
) -> Result<(), CompiledPrefixError<E>> {
    fn sift<T, E>(
        items: &mut [T],
        mut root: usize,
        compare: &mut impl FnMut(&T, &T) -> Result<std::cmp::Ordering, CompiledPrefixError<E>>,
    ) -> Result<(), CompiledPrefixError<E>> {
        use CompiledPrefixError::Invariant;
        while root < items.len() / 2 {
            let mut child = root * 2 + 1;
            if let Some(right) = items.get(child + 1)
                && compare(items.get(child).ok_or(Invariant)?, right)?.is_lt()
            {
                child += 1;
            }
            if !compare(
                items.get(root).ok_or(Invariant)?,
                items.get(child).ok_or(Invariant)?,
            )?
            .is_lt()
            {
                break;
            }
            items.swap(root, child);
            root = child;
        }
        Ok(())
    }
    for root in (0..items.len() / 2).rev() {
        sift(&mut *items, root, compare)?;
    }
    for end in (1..items.len()).rev() {
        items.swap(0, end);
        sift(
            items.get_mut(..end).ok_or(CompiledPrefixError::Invariant)?,
            0,
            compare,
        )?;
    }
    Ok(())
}

impl<V, A, K: PrefixKey<V>> CompiledPrefixIndex<V, A, K> {
    fn key<E>(&self, position: usize) -> Result<&[u8], CompiledPrefixError<E>> {
        self.entries
            .get(position)
            .map(|entry| self.key.bytes(entry))
            .ok_or(CompiledPrefixError::Invariant)
    }

    fn compile<W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        key: K,
        position: impl Fn(usize) -> Option<usize>,
        work: &mut W,
    ) -> Result<Self, CompiledPrefixError<W::Error>> {
        use CompiledPrefixError::{Allocation, Capacity, Invariant, Order, Work};
        for index in 1..entries.len() {
            let left = key.bytes(
                entries
                    .get(position(index - 1).ok_or(Invariant)?)
                    .ok_or(Invariant)?,
            );
            let right = key.bytes(
                entries
                    .get(position(index).ok_or(Invariant)?)
                    .ok_or(Invariant)?,
            );
            work.charge_items(left.len().min(right.len()) as u64 + 1)
                .map_err(Work)?;
            if left >= right {
                return Err(Order(position(index).ok_or(Invariant)?));
            }
        }
        let capacity = entries.len().checked_mul(2).ok_or(Capacity)?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<Node>())
            .ok_or(Capacity)?;
        let allocation = if bytes == 0 {
            None
        } else {
            Some(work.claim(bytes as u64).map_err(Work)?)
        };
        let mut result = Self {
            entries,
            nodes: Vec::new(),
            key,
            _allocation: allocation,
        };
        result
            .nodes
            .try_reserve_exact(capacity)
            .map_err(Allocation)?;
        if result.entries.is_empty() {
            return Ok(result);
        }
        result.nodes.push(Node {
            key: 0,
            end: 0,
            // Pending nodes use this range for their original entry group.
            // Processing replaces it with their contiguous child-node range.
            children: 0..result.entries.len(),
        });
        let mut cursor = 0;
        while cursor < result.nodes.len() {
            work.charge_items(1).map_err(Work)?;
            let node = result.nodes.get(cursor).ok_or(Invariant)?;
            let end = node.end;
            let before = node.children.end;
            let mut from = node.children.start;
            if result
                .key::<W::Error>(position(from).ok_or(Invariant)?)?
                .len()
                == end
            {
                from += 1;
            }
            let first_child = result.nodes.len();
            while from < before {
                let first = result.key::<W::Error>(position(from).ok_or(Invariant)?)?;
                let mut to = from + 1;
                while to < before {
                    work.charge_items(1).map_err(Work)?;
                    if result
                        .key::<W::Error>(position(to).ok_or(Invariant)?)?
                        .get(end)
                        .ok_or(Invariant)?
                        != first.get(end).ok_or(Invariant)?
                    {
                        break;
                    }
                    to += 1;
                }
                let last = result.key::<W::Error>(position(to - 1).ok_or(Invariant)?)?;
                let mut common = end;
                for (left, right) in first
                    .get(end..)
                    .ok_or(Invariant)?
                    .iter()
                    .zip(last.get(end..).ok_or(Invariant)?)
                {
                    work.charge_items(1).map_err(Work)?;
                    if left != right {
                        break;
                    }
                    common += 1;
                }
                result.nodes.push(Node {
                    key: from,
                    end: common,
                    children: from..to,
                });
                from = to;
            }
            let after = result.nodes.len();
            let node = result.nodes.get_mut(cursor).ok_or(Invariant)?;
            node.children = first_child..after;
            node.key = position(node.key).ok_or(Invariant)?;
            cursor += 1;
        }
        Ok(result)
    }

    /// Borrows entries in their original input order, without traversing nodes.
    pub fn entries(&self) -> &[V] {
        &self.entries
    }

    /// Finds both matches without allocation, a key copy or unrelated-key work.
    /// `accept` chooses valid proper-prefix boundaries (for example path
    /// components); its own cost is the caller's responsibility. It runs only
    /// until the first accepted prefix. Every node/label/edge examination is
    /// admitted by the same read owner, including a check on an empty table.
    /// Inconsistent internal coordinates or shortened key views return
    /// [`CompiledPrefixError::Invariant`], never a successful absence.
    pub fn matches<W: IndexReadWork>(
        &self,
        query: &[u8],
        mut accept: impl FnMut(&V) -> bool,
        work: &mut W,
    ) -> Result<PrefixMatches<'_, V>, CompiledPrefixError<W::Error>> {
        use CompiledPrefixError::{Invariant, Work};
        work.validate().map_err(Work)?;
        let mut result = PrefixMatches {
            exact: None,
            shortest: None,
        };
        if self.nodes.is_empty() {
            return Ok(result);
        }
        let mut cursor = 0;
        let mut from = 0;
        loop {
            work.examine(1).map_err(Work)?;
            let node = self.nodes.get(cursor).ok_or(Invariant)?;
            let entry = self.entries.get(node.key).ok_or(Invariant)?;
            let key = self.key.bytes(entry);
            let end = node.end.min(query.len());
            work.examine(end.checked_sub(from).ok_or(Invariant)? as u64)
                .map_err(Work)?;
            if query.get(from..end).ok_or(Invariant)? != key.get(from..end).ok_or(Invariant)?
                || query.len() < node.end
            {
                return Ok(result);
            }
            if key.len() == node.end {
                if query.len() == node.end {
                    result.exact = Some(PrefixMatch {
                        entry,
                        position: node.key,
                    });
                    return Ok(result);
                }
                if result.shortest.is_none() && accept(entry) {
                    result.shortest = Some(PrefixMatch {
                        entry,
                        position: node.key,
                    });
                }
            }
            let Some(&byte) = query.get(node.end) else {
                return Ok(result);
            };
            let mut children = node.children.clone();
            let child = loop {
                if children.is_empty() {
                    return Ok(result);
                }
                let middle = children.start + children.len() / 2;
                work.examine(1).map_err(Work)?;
                let observed = *self
                    .key::<W::Error>(self.nodes.get(middle).ok_or(Invariant)?.key)?
                    .get(node.end)
                    .ok_or(Invariant)?;
                match byte.cmp(&observed) {
                    std::cmp::Ordering::Less => children.end = middle,
                    std::cmp::Ordering::Greater => children.start = middle + 1,
                    std::cmp::Ordering::Equal => break middle,
                }
            };
            from = node.end;
            cursor = child;
        }
    }
}
