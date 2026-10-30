//! Component-prefix lookup with a bound determined by the queried path bytes.

use super::PortablePath;

struct Node<V> {
    prefix: Vec<u8>,
    value: Option<V>,
    children: Vec<(u8, usize)>,
}

/// A compressed byte trie shared by canonical portable-path consumers.
///
/// Child labels are distinct bytes, so binary search takes at most nine
/// comparisons. Each nonroot edge consumes at least one query byte. Lookup
/// allocates nothing and never hashes or revisits an ancestor prefix. The flat
/// arena also makes destruction independent of the process stack depth.
pub(crate) struct PathPrefixIndex<V> {
    nodes: Vec<Node<V>>,
}

impl<V> Default for PathPrefixIndex<V> {
    fn default() -> Self {
        Self {
            nodes: vec![Node {
                prefix: vec![b'/'],
                value: None,
                children: Vec::new(),
            }],
        }
    }
}

impl<V> PathPrefixIndex<V> {
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

    /// Binds a canonical path, replacing only an exact binding.
    pub(crate) fn insert(&mut self, path: PortablePath, value: V) -> Option<V> {
        let mut key = path.0.into_bytes();
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

    pub(crate) fn longest_prefix(&self, path: &PortablePath) -> Option<&V> {
        self.lookup(path, |_| {})
    }

    fn lookup(&self, path: &PortablePath, mut visit: impl FnMut(usize)) -> Option<&V> {
        let key = path.as_str().as_bytes();
        let mut remaining = key;
        let mut selected = None;
        let mut index = 0;
        loop {
            let node = self.node(index);
            // Charge an upper bound on the compared bytes, including a failed
            // final edge. The callback is erased for ordinary lookups.
            visit(1 + remaining.len().min(node.prefix.len()));
            let Some(suffix) = remaining.strip_prefix(node.prefix.as_slice()) else {
                return selected;
            };
            remaining = suffix;
            if node.value.is_some()
                && (remaining.is_empty()
                    || remaining.first() == Some(&b'/')
                    || key.len() - remaining.len() == 1)
            {
                selected = node.value.as_ref();
            }
            let Some(first) = remaining.first() else {
                return selected;
            };
            let Ok(slot) = node.children.binary_search_by_key(first, |(label, _)| {
                visit(1);
                *label
            }) else {
                return selected;
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
    use crate::model::VolumeLimits;

    #[test]
    fn component_routes_are_bounded_by_query_bytes_after_foreign_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let parse = |path: &str| PortablePath::parse(path, limits);
        let mut paths = vec!["/".to_owned(), "/é".to_owned(), "/é/漢".to_owned()];
        let mut deep = String::new();
        for _ in 0..48 {
            deep.push_str("/a");
            paths.push(deep.clone());
        }
        let queries = [
            "/".to_owned(),
            "/é/漢/file".to_owned(),
            "/éclair".to_owned(),
            "/é/漢字".to_owned(),
            format!("{deep}/file"),
            format!("{deep}b/file"),
            "/z/absent".to_owned(),
            "/foreign/000a/file".to_owned(),
        ];
        // Opposite insertion orders force both edge splitting and insertion
        // beneath already bound prefixes through the same implementation.
        for reverse in [false, true] {
            let mut index = PathPrefixIndex::default();
            let mut admitted = paths.clone();
            let mut order = paths.iter().collect::<Vec<_>>();
            if reverse {
                order.reverse();
            }
            for path in order {
                assert_eq!(index.insert(parse(path)?, path.clone()), None);
            }
            assert_eq!(
                index.insert(parse("/é")?, "/é".to_owned()),
                Some("/é".to_owned())
            );
            for grow in [false, true] {
                if grow {
                    for suffix in 0..4096 {
                        let foreign = format!("/foreign/{suffix:04x}");
                        index.insert(parse(&foreign)?, foreign.clone());
                        admitted.push(foreign);
                    }
                }
                for query in &queries {
                    let path = parse(query)?;
                    let mut scanned = 0;
                    let expected = admitted
                        .iter()
                        .inspect(|_| scanned += 1)
                        .filter(|prefix| {
                            prefix.as_str() == "/"
                                || query == *prefix
                                || query
                                    .strip_prefix(prefix.as_str())
                                    .is_some_and(|rest| rest.starts_with('/'))
                        })
                        .max_by_key(|prefix| prefix.len());
                    let mut work = 0;
                    assert_eq!(index.lookup(&path, |bytes| work += bytes), expected);
                    assert!(work <= 12 * query.len() + 1, "{query}: {work}");
                    if grow {
                        // The full-table semantic oracle is the negative
                        // control: identical routes violate the work bound.
                        assert!(scanned > 12 * query.len() + 1);
                    }
                }
            }
        }
        Ok(())
    }
}
