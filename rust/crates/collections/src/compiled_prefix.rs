//! Immutable, variable-width byte prefixes compiled into one flat radix table.
use super::{IndexReadWork, IndexWork};
use std::ops::Range;

/// A compilation refusal, before publishing any index.
#[derive(Debug)]
pub enum CompiledPrefixError<E> {
    /// The original caller's admission error.
    Work(E),
    /// Keys must be distinct and in unsigned byte order.
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

/// Exact and shortest accepted proper-prefix matches from one traversal.
#[derive(Debug)]
pub struct PrefixMatches<'a, V> {
    /// The complete byte key, if present, independently of prefix acceptance.
    pub exact: Option<&'a V>,
    /// The shortest proper prefix accepted by the caller's boundary predicate.
    pub shortest: Option<&'a V>,
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
pub struct CompiledPrefixIndex<V, A> {
    entries: Vec<V>,
    nodes: Vec<Node>,
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
        use CompiledPrefixError::{Allocation, Capacity, Order, Work};
        work.check().map_err(Work)?;
        for (index, pair) in entries.windows(2).enumerate() {
            work.charge_items(pair[0].as_ref().len().min(pair[1].as_ref().len()) as u64 + 1)
                .map_err(Work)?;
            if pair[0].as_ref() >= pair[1].as_ref() {
                return Err(Order(index + 1));
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
            let node = &result.nodes[cursor];
            let end = node.end;
            let before = node.children.end;
            let mut from = node.key;
            if result.entries[from].as_ref().len() == end {
                from += 1;
            }
            let first_child = result.nodes.len();
            while from < before {
                let first = result.entries[from].as_ref();
                let mut to = from + 1;
                while to < before {
                    work.charge_items(1).map_err(Work)?;
                    if result.entries[to].as_ref()[end] != first[end] {
                        break;
                    }
                    to += 1;
                }
                let last = result.entries[to - 1].as_ref();
                let mut common = end;
                while common < first.len().min(last.len()) {
                    work.charge_items(1).map_err(Work)?;
                    if first[common] != last[common] {
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
            result.nodes[cursor].children = first_child..result.nodes.len();
            cursor += 1;
        }
        Ok(result)
    }

    /// Borrows the original entries in byte order, without traversing nodes.
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
        if self.nodes.is_empty() {
            return Ok(result);
        }
        let mut cursor = 0;
        let mut from = 0;
        loop {
            work.examine(1)?;
            let node = &self.nodes[cursor];
            let entry = &self.entries[node.key];
            let key = entry.as_ref();
            let end = node.end.min(query.len());
            work.examine((end - from) as u64)?;
            if query[from..end] != key[from..end] || query.len() < node.end {
                return Ok(result);
            }
            if key.len() == node.end {
                if query.len() == node.end {
                    result.exact = Some(entry);
                    return Ok(result);
                }
                if result.shortest.is_none() && accept(entry) {
                    result.shortest = Some(entry);
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
                work.examine(1)?;
                let observed = self.entries[self.nodes[middle].key].as_ref()[node.end];
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
