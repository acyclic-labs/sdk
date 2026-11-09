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

/// A compilation refusal, before publishing any index.
#[derive(Debug)]
pub enum CompiledPrefixError<E> {
    /// The original caller's admission error.
    Work(E),
    /// A distinct-key constructor found a duplicate, or `from_sorted` found
    /// keys outside unsigned byte order.
    /// The position identifies an entry in the original input vector.
    Order(usize),
    /// The supplied table cannot be represented by the native address space.
    Capacity,
    /// The allocator refused the admitted node backing.
    Allocation(std::collections::TryReserveError),
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
        Self::compile(entries, EntryBytes, |position| position, true, work)
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
        Self::from_keyed_entries(entries, key, true, work)
    }

    /// Compiles repeated borrowed keys through the same table and traversal.
    /// Every match returns the first original entry with that key; all entries
    /// retain their original positions. Prefix acceptance examines only that
    /// representative. Funding and key stability follow `from_entries`.
    pub fn from_repeated_entries<K: Fn(&V) -> &[u8], W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        key: K,
        work: &mut W,
    ) -> Result<CompiledPrefixIndex<V, A, K>, CompiledPrefixError<W::Error>> {
        Self::from_keyed_entries(entries, key, false, work)
    }

    fn from_keyed_entries<K: Fn(&V) -> &[u8], W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        key: K,
        distinct: bool,
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
            let left_key = key.bytes(entry(&entries, left));
            let right_key = key.bytes(entry(&entries, right));
            work.charge_items(left_key.len().min(right_key.len()) as u64 + 1)?;
            Ok::<_, W::Error>(left_key.cmp(right_key).then_with(|| left.cmp(&right)))
        })
        .map_err(Work)?;
        let index = CompiledPrefixIndex::compile(
            entries,
            key,
            |position| {
                *order
                    .get(position)
                    .unwrap_or_else(|| unreachable!("compilation stays within the permutation"))
            },
            distinct,
            work,
        )?;
        drop(order);
        drop(allocation);
        Ok(index)
    }
}

// Every position originates in 0..entries.len(); sorting only permutes it and
// compilation retains it unchanged in the private node table.
fn entry<V>(entries: &[V], position: usize) -> &V {
    entries
        .get(position)
        .unwrap_or_else(|| unreachable!("positions retain their original entry"))
}

// In-place heapsort permits an immediate typed refusal before each comparison.
// An infallible comparator cannot stop work safely when its admission is lost.
fn sort<T, E>(
    items: &mut [T],
    compare: &mut impl FnMut(&T, &T) -> Result<std::cmp::Ordering, E>,
) -> Result<(), E> {
    fn sift<T, E>(
        items: &mut [T],
        mut root: usize,
        compare: &mut impl FnMut(&T, &T) -> Result<std::cmp::Ordering, E>,
    ) -> Result<(), E> {
        while let Some((parent, tail)) = items.get(root..).and_then(|tail| tail.split_first()) {
            let Some((left, siblings)) = tail.get(root..).and_then(|tail| tail.split_first())
            else {
                break;
            };
            // Finding this child first proves that the coordinate fits usize.
            let mut child = root * 2 + 1;
            let selected = if let Some(right) = siblings.first()
                && compare(left, right)?.is_lt()
            {
                child += 1;
                right
            } else {
                left
            };
            if !compare(parent, selected)?.is_lt() {
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
    let mut heap = items;
    while let Some((last, rest)) = heap.split_last_mut() {
        let Some(first) = rest.first_mut() else {
            break;
        };
        std::mem::swap(first, last);
        sift(rest, 0, compare)?;
        heap = rest;
    }
    Ok(())
}

impl<V, A, K: PrefixKey<V>> CompiledPrefixIndex<V, A, K> {
    fn key(&self, position: usize) -> &[u8] {
        self.key.bytes(entry(&self.entries, position))
    }

    fn compile<W: IndexWork<Allocation = A>>(
        entries: Vec<V>,
        key: K,
        position: impl Fn(usize) -> usize,
        distinct: bool,
        work: &mut W,
    ) -> Result<Self, CompiledPrefixError<W::Error>> {
        use CompiledPrefixError::{Allocation, Capacity, Order, Work};
        for index in 1..entries.len() {
            let left = key.bytes(entry(&entries, position(index - 1)));
            let right = key.bytes(entry(&entries, position(index)));
            work.charge_items(left.len().min(right.len()) as u64 + 1)
                .map_err(Work)?;
            let order = left.cmp(right);
            if order.is_gt() || (distinct && order.is_eq()) {
                return Err(Order(position(index)));
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
        result.compile_nodes(position, work)?;
        Ok(result)
    }

    fn compile_nodes<W: IndexWork<Allocation = A>>(
        &mut self,
        position: impl Fn(usize) -> usize,
        work: &mut W,
    ) -> Result<(), CompiledPrefixError<W::Error>> {
        use CompiledPrefixError::Work;
        if self.entries.is_empty() {
            return Ok(());
        }
        self.nodes.push(Node {
            key: 0,
            end: 0,
            // Pending nodes use this range for their original entry group.
            // Processing replaces it with their contiguous child-node range.
            children: 0..self.entries.len(),
        });
        let mut cursor = 0;
        while let Some(node) = self.nodes.get(cursor) {
            work.charge_items(1).map_err(Work)?;
            let end = node.end;
            let before = node.children.end;
            let mut from = node.children.start;
            while from < before && self.key(position(from)).len() == end {
                work.charge_items(1).map_err(Work)?;
                from += 1;
            }
            let first_child = self.nodes.len();
            while from < before {
                let first = self.key(position(from));
                let Some(byte) = first.get(end) else {
                    unreachable!("terminal keys precede their nonterminal group")
                };
                let mut to = from + 1;
                while to < before {
                    work.charge_items(1).map_err(Work)?;
                    if self.key(position(to)).get(end) != Some(byte) {
                        break;
                    }
                    to += 1;
                }
                let last = self.key(position(to - 1));
                let mut common = end;
                for (left, right) in first.iter().zip(last).skip(end) {
                    work.charge_items(1).map_err(Work)?;
                    if left != right {
                        break;
                    }
                    common += 1;
                }
                self.nodes.push(Node {
                    key: from,
                    end: common,
                    children: from..to,
                });
                from = to;
            }
            let after_children = self.nodes.len();
            let Some(node) = self.nodes.get_mut(cursor) else {
                unreachable!("appending children preserves the current node")
            };
            node.children = first_child..after_children;
            node.key = position(node.key);
            cursor += 1;
        }
        Ok(())
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
    pub fn matches<W: IndexReadWork>(
        &self,
        query: &[u8],
        mut accept: impl FnMut(&V) -> bool,
        work: &mut W,
    ) -> Result<PrefixMatches<'_, V>, W::Error> {
        work.validate()?;
        let mut result = PrefixMatches {
            exact: None,
            shortest: None,
        };
        let Some(mut node) = self.nodes.first() else {
            return Ok(result);
        };
        let mut from = 0;
        loop {
            work.examine(1)?;
            let entry = entry(&self.entries, node.key);
            let key = self.key.bytes(entry);
            let end = node.end.min(query.len());
            work.examine((end - from) as u64)?;
            let Some(label) = key.get(from..end) else {
                unreachable!("the stable key contains its compiled label")
            };
            if query.get(from..end) != Some(label) || query.len() < node.end {
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
            let Some(mut children) = self.nodes.get(node.children.clone()) else {
                unreachable!("compiled children belong to the node table")
            };
            let child = loop {
                let (lower, upper) = children.split_at(children.len() / 2);
                let Some((middle, higher)) = upper.split_first() else {
                    return Ok(result);
                };
                work.examine(1)?;
                let Some(observed) = self.key(middle.key).get(node.end) else {
                    unreachable!("a child label extends its parent's label")
                };
                match byte.cmp(observed) {
                    std::cmp::Ordering::Less => children = lower,
                    std::cmp::Ordering::Greater => children = higher,
                    std::cmp::Ordering::Equal => break middle,
                }
            };
            from = node.end;
            node = child;
        }
    }
}
