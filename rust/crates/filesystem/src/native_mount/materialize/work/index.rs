//! One append-only, balanced identity index for every host and candidate kind.
//! Rotations change links, never move accumulated keys or values. Geometric
//! arena growth is the sole bulk copy and uses the existing custody allocator.
use super::*;
use crate::kernel::ordered_links::{Links, Tree};

struct Search {
    found: Option<usize>,
    parent: Option<usize>,
    side: usize,
    depth: usize,
}

struct Node<K, V> {
    key: K,
    value: V,
    links: Links,
}

pub(in crate::native_mount::materialize) struct GenerationIndex<K, V> {
    nodes: Vec<Node<K, V>>,
    root: Option<usize>,
}

impl<K, V> Default for GenerationIndex<K, V> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
        }
    }
}

impl<K: Ord, V> GenerationIndex<K, V> {
    fn node(&self, index: usize) -> Result<&Node<K, V>, MaterializeError> {
        self.nodes
            .get(index)
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout.into())
    }

    fn search(&self, key: &K, receipt: &GenerationReceipt) -> Result<Search, MaterializeError> {
        let work = receipt.custody.work();
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        accounting.check()?;
        let mut current = self.root;
        let mut parent = None;
        let mut side = 0;
        let mut depth = 0;
        while let Some(index) = current {
            let node = self.node(index)?;
            // Use the shared before-effect comparison admission, exactly once
            // for this node. The insertion position of a singleton selects
            // its left (0) or right (1) child without another comparison.
            let Err(next_side) = crate::kernel::binary_search_with_accounting(
                std::slice::from_ref(&node.key),
                &mut accounting,
                |candidate| candidate.cmp(key),
            )?
            else {
                return Ok(Search {
                    found: Some(index),
                    parent,
                    side,
                    depth,
                });
            };
            parent = current;
            side = next_side;
            current = node.links.child.get(side).copied().flatten();
            depth += 1;
        }
        Ok(Search {
            found: None,
            parent,
            side,
            depth,
        })
    }

    pub(in crate::native_mount::materialize) fn get(
        &self,
        key: &K,
        receipt: &GenerationReceipt,
    ) -> Result<Option<&V>, MaterializeError> {
        self.search(key, receipt)?
            .found
            .map(|index| self.node(index).map(|node| &node.value))
            .transpose()
    }

    pub(in crate::native_mount::materialize) fn insert(
        &mut self,
        key: K,
        value: V,
        receipt: &mut GenerationReceipt,
    ) -> Result<bool, MaterializeError> {
        let Search {
            found,
            parent,
            side,
            depth,
        } = self.search(&key, receipt)?;
        if found.is_some() {
            return Ok(false);
        }
        // AVL insertion refreshes at most the old path and performs at most
        // two rotations. Each rotation writes at most six complete link sets.
        // Reserve the complete mutation before either allocation or insertion;
        // only the actually written bytes are recorded on the original ledger.
        let maximum = depth
            .checked_add(13)
            .and_then(|count| count.checked_mul(std::mem::size_of::<Links>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Node<K, V>>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Option<usize>>()))
            .ok_or(WorkError::Overflow)?;
        let copying = receipt.custody.work().reserve_work(WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(maximum),
            ..WorkCounters::UNCHARGED
        })?;
        receipt.reserve(&mut self.nodes, 1)?;
        let index = self.nodes.len();
        self.nodes.push(Node {
            key,
            value,
            links: Links {
                parent,
                height: 1,
                count: 1,
                ..Links::default()
            },
        });
        let mut copied = std::mem::size_of::<Node<K, V>>();
        self.attach(index, parent, side, &mut copied)?;
        copying.record_completed(WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(copied),
            ..WorkCounters::UNCHARGED
        })?;
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn entries(&self) -> impl Iterator<Item = (&K, &V)> {
        self.nodes.iter().map(|node| (&node.key, &node.value))
    }

    #[cfg(test)]
    pub(super) fn valid(&self) -> bool {
        self.inspect(self.root, None, None, None, self.nodes.len())
            .is_some_and(|(_, count)| count == self.nodes.len())
    }

    #[cfg(test)]
    fn inspect(
        &self,
        index: Option<usize>,
        parent: Option<usize>,
        lower: Option<&K>,
        upper: Option<&K>,
        remaining: usize,
    ) -> Option<(usize, usize)> {
        let Some(index) = index else {
            return Some((0, 0));
        };
        let node = self.nodes.get(index)?;
        if remaining == 0
            || node.links.parent != parent
            || lower.is_some_and(|key| node.key <= *key)
            || upper.is_some_and(|key| node.key >= *key)
        {
            return None;
        }
        let [left, right] = node.links.child;
        let (a, ac) = self.inspect(left, Some(index), lower, Some(&node.key), remaining - 1)?;
        let (b, bc) = self.inspect(right, Some(index), Some(&node.key), upper, remaining - 1)?;
        let height = 1 + a.max(b);
        (a.abs_diff(b) <= 1 && height == node.links.height && node.links.count == 1 + ac + bc)
            .then_some((height, 1 + ac + bc))
    }
}

impl<K: Ord, V> Tree for GenerationIndex<K, V> {
    type Error = MaterializeError;
    fn invalid() -> Self::Error {
        crate::NativeTreeMaterializationError::InvalidLayout.into()
    }
    fn root(&self) -> Option<usize> {
        self.root
    }
    fn put_root(&mut self, root: Option<usize>, copied: &mut usize) -> Result<(), Self::Error> {
        self.root = root;
        *copied += std::mem::size_of::<Option<usize>>();
        Ok(())
    }
    fn get_links(&self, index: usize) -> Result<Links, Self::Error> {
        self.node(index).map(|node| node.links)
    }
    fn put_links(
        &mut self,
        index: usize,
        links: Links,
        copied: &mut usize,
    ) -> Result<(), Self::Error> {
        let node = self
            .nodes
            .get_mut(index)
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        node.links = links;
        *copied += std::mem::size_of::<Links>();
        Ok(())
    }
}
