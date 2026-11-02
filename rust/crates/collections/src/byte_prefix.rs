//! Compressed, variable-width byte prefixes shared by path owners.

#[derive(Debug)]
struct Node<V> {
    prefix: Vec<u8>,
    value: Option<V>,
    children: Vec<(u8, usize)>,
}

/// A compressed byte trie with allocation-free, query-byte-bounded reads.
///
/// Child labels are distinct bytes, so binary search takes at most nine
/// comparisons. Each nonroot edge consumes at least one query byte. The flat
/// arena makes destruction independent of process stack depth. Path owners
/// supply their own component-boundary rules; this index grants no authority.
#[derive(Debug)]
pub struct BytePrefixIndex<V> {
    nodes: Vec<Node<V>>,
}

impl<V> Default for BytePrefixIndex<V> {
    fn default() -> Self {
        Self { nodes: Vec::new() }
    }
}

impl<V> BytePrefixIndex<V> {
    fn node(&self, index: usize) -> &Node<V> {
        self.nodes
            .get(index)
            .unwrap_or_else(|| unreachable!("prefix links belong to this arena"))
    }

    fn node_mut(&mut self, index: usize) -> &mut Node<V> {
        self.nodes
            .get_mut(index)
            .unwrap_or_else(|| unreachable!("prefix links belong to this arena"))
    }

    /// Moves an owned key into the index, replacing only an exact binding.
    /// Insertion may split compressed edges and grow the arena and child lists.
    pub fn insert(&mut self, mut key: Vec<u8>, value: V) -> Option<V> {
        if self.nodes.is_empty() {
            self.nodes.push(Node {
                prefix: key,
                value: Some(value),
                children: Vec::new(),
            });
            return None;
        }
        let mut remaining = key.as_slice();
        let mut index = 0;
        loop {
            let node = self.node_mut(index);
            let common = node
                .prefix
                .iter()
                .zip(remaining)
                .take_while(|(a, b)| a == b)
                .count();
            if common < node.prefix.len() {
                let suffix = Node {
                    prefix: node.prefix.split_off(common),
                    value: node.value.take(),
                    children: std::mem::take(&mut node.children),
                };
                let first = *suffix
                    .prefix
                    .first()
                    .unwrap_or_else(|| unreachable!("a split leaves a nonempty suffix"));
                let child = self.nodes.len();
                self.nodes.push(suffix);
                self.node_mut(index).children.push((first, child));
            }
            remaining = remaining
                .get(common..)
                .unwrap_or_else(|| unreachable!("common is bounded by both prefixes"));
            let Some(first) = remaining.first().copied() else {
                return self.node_mut(index).value.replace(value);
            };
            match self
                .node(index)
                .children
                .binary_search_by_key(&first, |(label, _)| *label)
            {
                Ok(slot) => {
                    index = self.node(index).children.get(slot).map_or_else(
                        || unreachable!("binary search returned an existing child"),
                        |(_, child)| *child,
                    );
                }
                Err(slot) => {
                    let consumed = key.len() - remaining.len();
                    drop(key.drain(..consumed));
                    let child = self.nodes.len();
                    self.nodes.push(Node {
                        prefix: key,
                        value: Some(value),
                        children: Vec::new(),
                    });
                    self.node_mut(index).children.insert(slot, (first, child));
                    return None;
                }
            }
        }
    }

    /// Visits bound byte prefixes in increasing length, including an exact key.
    ///
    /// `matched` receives the matched byte length and the original borrowed
    /// value. `examine` observes an upper bound on compared bytes, node visits
    /// and child-label comparisons in this same traversal. With no-op callbacks
    /// unused work is erased. At most `12 * key.len() + 1` units are examined,
    /// independent of entry count; callback work is additional. Nothing is
    /// allocated, copied, hashed or re-read from an ancestor on lookup.
    pub fn visit_prefixes<'a>(
        &'a self,
        key: &[u8],
        mut matched: impl FnMut(usize, &'a V),
        mut examine: impl FnMut(usize),
    ) {
        if self.nodes.is_empty() {
            return;
        }
        let mut remaining = key;
        let mut index = 0;
        loop {
            let node = self.node(index);
            examine(1 + remaining.len().min(node.prefix.len()));
            let Some(suffix) = remaining.strip_prefix(node.prefix.as_slice()) else {
                return;
            };
            remaining = suffix;
            if let Some(value) = node.value.as_ref() {
                matched(key.len() - remaining.len(), value);
            }
            let Some(first) = remaining.first() else {
                return;
            };
            let Ok(slot) = node.children.binary_search_by_key(first, |(label, _)| {
                examine(1);
                *label
            }) else {
                return;
            };
            index = node.children.get(slot).map_or_else(
                || unreachable!("binary search returned an existing child"),
                |(_, child)| *child,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn byte_prefixes_match_original_bindings(
            keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..128),
            query in prop::collection::vec(any::<u8>(), 0..128),
            lengths in prop::collection::vec(0usize..128, 0..64),
        ) {
            let mut index = BytePrefixIndex::default();
            let mut bindings = std::collections::BTreeMap::new();
            let prefixes = lengths.into_iter().map(|length| {
                query.iter().take(length).copied().collect::<Vec<_>>()
            });
            for (value, key) in keys.into_iter().chain(prefixes).enumerate() {
                prop_assert_eq!(index.insert(key.clone(), value), bindings.insert(key, value));
            }
            let mut observed = Vec::new();
            let mut work = 0;
            index.visit_prefixes(&query, |length, value| observed.push((length, *value)), |n| work += n);
            let expected = bindings.iter()
                .filter(|(key, _)| query.starts_with(key))
                .map(|(key, value)| (key.len(), *value))
                .collect::<Vec<_>>();
            prop_assert_eq!(observed, expected);
            prop_assert!(work <= 12 * query.len() + 1);
        }
    }
}
