//! Immutable actor field pages over the shared prefix-free radix coordinates.
//! Clones retain one root. Edits copy only their key's branch frontier.
use acyclic_collections::radix::{self, BinaryKey, BranchBit, RadixCursor, RadixKey};
use std::{collections::BTreeMap, sync::Arc};

/// An immutable, canonically ordered actor field map. Cloning shares its root;
/// mutation copies only a prefix-free byte-key frontier, without hashing keys.
pub struct StateMap<V> {
    root: Option<Arc<Node<V>>>,
}

struct Node<V> {
    first: Arc<String>,
    last: Arc<String>,
    len: usize,
    kind: Kind<V>,
}

enum Kind<V> {
    Leaf(V),
    Branch {
        bit: BranchBit,
        low: Arc<Node<V>>,
        high: Arc<Node<V>>,
    },
}

struct Cursor<'a, V>(Option<&'a Node<V>>);
impl<V> RadixCursor for Cursor<'_, V> {
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    fn branch_bit(&self) -> Option<BranchBit> {
        match &self.0?.kind {
            Kind::Leaf(_) => None,
            Kind::Branch { bit, .. } => Some(*bit),
        }
    }
    fn child(self, high_side: bool) -> Self {
        match self.0.map(|node| &node.kind) {
            Some(Kind::Branch { low, high, .. }) => Self(Some(if high_side { high } else { low })),
            _ => Self(None),
        }
    }
}

fn select<'a, V>(root: Option<&'a Node<V>>, key: &str) -> Option<&'a Node<V>> {
    radix::select(Cursor(root), &BinaryKey(key.as_bytes()), None, |_| {
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap_or_else(|never| match never {})
    .0
    .0
}

impl<V> Clone for StateMap<V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}
impl<V> Default for StateMap<V> {
    fn default() -> Self {
        Self { root: None }
    }
}

impl<V> Node<V> {
    fn branch(bit: BranchBit, low: Arc<Self>, high: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            first: Arc::clone(&low.first),
            last: Arc::clone(&high.last),
            len: low.len + high.len,
            kind: Kind::Branch { bit, low, high },
        })
    }
}

impl<V> StateMap<V> {
    /// Exact number of live fields, retained in the root.
    pub fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |root| root.len)
    }
    /// Whether this root has no fields.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
    /// Read one complete key without copying a key or value.
    pub fn get(&self, key: &str) -> Option<&V> {
        let node = select(self.root.as_deref(), key)?;
        match &node.kind {
            Kind::Leaf(value) if node.first.as_str() == key => Some(value),
            _ => None,
        }
    }
    /// Borrow all fields in canonical UTF-8 byte order.
    pub fn iter(&self) -> Entries<'_, V> {
        Entries {
            pending: self.root.as_deref().into_iter().collect(),
            remaining: self.len(),
        }
    }
    /// Borrow all canonical field names.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &String> {
        self.iter().map(|(key, _)| key)
    }
    /// Borrow values in canonical field-name order.
    pub fn values(&self) -> impl ExactSizeIterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }
    /// Seek an ordered interval, pruning subtrees before its lower boundary.
    pub fn range<R: std::ops::RangeBounds<str>>(&self, bounds: R) -> Range<'_, V, R> {
        let mut entries = self.iter();
        let lower = bounds.start_bound();
        while let Some(node) = entries.pending.pop() {
            let before = match lower {
                std::ops::Bound::Unbounded => false,
                std::ops::Bound::Included(key) => node.last.as_str() < key,
                std::ops::Bound::Excluded(key) => node.last.as_str() <= key,
            };
            if before {
                entries.remaining -= node.len;
                continue;
            }
            match &node.kind {
                Kind::Leaf(_) => {
                    entries.pending.push(node);
                    break;
                }
                Kind::Branch { low, high, .. } => {
                    entries.pending.push(high);
                    entries.pending.push(low);
                }
            }
        }
        Range {
            entries,
            bounds,
            finished: false,
        }
    }
}

impl<V: Clone> StateMap<V> {
    /// Replace one field, retaining every other snapshot's original root.
    pub fn insert(&mut self, key: String, value: V) -> Option<V> {
        let previous = self.get(&key).cloned();
        self.replace(key, value);
        previous
    }
    fn replace(&mut self, key: String, value: V) {
        // Find a representative without descending beyond the queried key.
        let cursor = select(self.root.as_deref(), &key);
        let difference = cursor.and_then(|node| {
            BinaryKey(key.as_bytes())
                .difference(BinaryKey(node.first.as_bytes()), |_| {
                    Ok::<_, std::convert::Infallible>(())
                })
                .unwrap_or_else(|never| match never {})
        });
        let mut path = Vec::new();
        let mut cursor = self.root.clone();
        while let Some(node) = &cursor {
            let Kind::Branch { bit, low, high } = &node.kind else {
                break;
            };
            if difference.is_some_and(|difference| *bit >= difference) {
                break;
            }
            let high_side = BinaryKey(key.as_bytes()).branch(*bit).unwrap_or(false);
            path.push((
                *bit,
                Arc::clone(if high_side { low } else { high }),
                high_side,
            ));
            cursor = Some(Arc::clone(if high_side { high } else { low }));
        }
        let key_backing = Arc::new(key);
        let mut result = Arc::new(Node {
            first: Arc::clone(&key_backing),
            last: Arc::clone(&key_backing),
            len: 1,
            kind: Kind::Leaf(value),
        });
        if let (Some(bit), Some(old)) = (difference, cursor) {
            result = if BinaryKey(key_backing.as_bytes()).branch(bit) == Some(true) {
                Node::branch(bit, old, result)
            } else {
                Node::branch(bit, result, old)
            };
        }
        for (bit, sibling, high_side) in path.into_iter().rev() {
            result = if high_side {
                Node::branch(bit, sibling, result)
            } else {
                Node::branch(bit, result, sibling)
            };
        }
        retire(std::mem::replace(&mut self.root, Some(result)));
    }

    /// Copy the selected frontier before borrowing one field mutably.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        let value = self.get(key)?.clone();
        self.replace(super::record::copy_string(key), value);
        let mut node = self.root.as_mut()?;
        loop {
            let body = Arc::get_mut(node)
                .unwrap_or_else(|| unreachable!("the replaced key's frontier is newly owned"));
            match &mut body.kind {
                Kind::Leaf(value) => return Some(value),
                Kind::Branch { bit, low, high } => {
                    node = if BinaryKey(key.as_bytes()).branch(*bit)? {
                        high
                    } else {
                        low
                    };
                }
            }
        }
    }

    /// Remove a live field and collapse its redundant branch, without tombstones.
    pub fn remove(&mut self, key: &str) -> Option<V> {
        let previous = self.get(key)?.clone();
        let mut node = self.root.clone()?;
        let mut path = Vec::new();
        while let Kind::Branch { bit, low, high } = &node.kind {
            let high_side = BinaryKey(key.as_bytes()).branch(*bit)?;
            path.push((
                *bit,
                Arc::clone(if high_side { low } else { high }),
                high_side,
            ));
            node = Arc::clone(if high_side { high } else { low });
        }
        let mut result = None;
        for (bit, sibling, high_side) in path.into_iter().rev() {
            result = Some(match result {
                None => sibling,
                Some(selected) if high_side => Node::branch(bit, sibling, selected),
                Some(selected) => Node::branch(bit, selected, sibling),
            });
        }
        retire(std::mem::replace(&mut self.root, result));
        Some(previous)
    }
}

/// Borrowed ordered traversal of one immutable root.
pub struct Entries<'a, V> {
    pending: Vec<&'a Node<V>>,
    remaining: usize,
}
impl<'a, V> Iterator for Entries<'a, V> {
    type Item = (&'a String, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.pending.pop() {
            match &node.kind {
                Kind::Leaf(value) => {
                    self.remaining -= 1;
                    return Some((node.first.as_ref(), value));
                }
                Kind::Branch { low, high, .. } => {
                    self.pending.push(high);
                    self.pending.push(low);
                }
            }
        }
        None
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl<V> ExactSizeIterator for Entries<'_, V> {}
/// Borrowed traversal of one selected canonical key interval.
pub struct Range<'a, V, R> {
    entries: Entries<'a, V>,
    bounds: R,
    finished: bool,
}
impl<'a, V, R: std::ops::RangeBounds<str>> Iterator for Range<'a, V, R> {
    type Item = (&'a String, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let entry = self.entries.next()?;
        self.finished = match self.bounds.end_bound() {
            std::ops::Bound::Unbounded => false,
            std::ops::Bound::Included(key) => entry.0.as_str() > key,
            std::ops::Bound::Excluded(key) => entry.0.as_str() >= key,
        };
        (!self.finished).then_some(entry)
    }
}
impl<'a, V> IntoIterator for &'a StateMap<V> {
    type Item = (&'a String, &'a V);
    type IntoIter = Entries<'a, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<V: Clone> FromIterator<(String, V)> for StateMap<V> {
    fn from_iter<T: IntoIterator<Item = (String, V)>>(entries: T) -> Self {
        let mut map = Self::default();
        for (key, value) in entries {
            map.insert(key, value);
        }
        map
    }
}
impl<V: Clone> From<BTreeMap<String, V>> for StateMap<V> {
    fn from(entries: BTreeMap<String, V>) -> Self {
        entries.into_iter().collect()
    }
}
impl<V: Clone, const N: usize> From<[(String, V); N]> for StateMap<V> {
    fn from(entries: [(String, V); N]) -> Self {
        entries.into_iter().collect()
    }
}
impl<V: std::fmt::Debug> std::fmt::Debug for StateMap<V> {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.debug_map().entries(self.iter()).finish()
    }
}
impl<V: PartialEq> PartialEq for StateMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl<V: Eq> Eq for StateMap<V> {}

impl<V> std::ops::Index<&str> for StateMap<V> {
    type Output = V;
    fn index(&self, key: &str) -> &V {
        self.get(key).expect("actor state key is present")
    }
}

// Key depth is not a call-stack bound, including during rejected edits and
// retirement of a long-lived snapshot's last shared root.
fn retire<V>(root: Option<Arc<Node<V>>>) {
    let mut pending: Vec<_> = root.into_iter().collect();
    while let Some(node) = pending.pop() {
        if let Some(body) = Arc::into_inner(node) {
            if let Kind::Branch { low, high, .. } = body.kind {
                pending.push(high);
                pending.push(low);
            }
        }
    }
}
impl<V> Drop for StateMap<V> {
    fn drop(&mut self) {
        retire(self.root.take());
    }
}

#[cfg(test)]
mod tests;
