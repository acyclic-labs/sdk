//! One append-only, balanced identity index for every host and candidate kind.
//! Rotations change links, never move accumulated keys or values. Geometric
//! arena growth is the sole bulk copy and uses the existing custody allocator.
use super::*;

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

#[derive(Clone, Copy, Default)]
struct Links {
    child: [Option<usize>; 2],
    parent: Option<usize>,
    height: usize,
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

    fn links(&self, index: Option<usize>) -> Result<Links, MaterializeError> {
        index.map_or(Ok(Links::default()), |index| {
            self.node(index).map(|node| node.links)
        })
    }

    fn set_links(
        &mut self,
        index: usize,
        links: Links,
        copied: &mut usize,
    ) -> Result<(), MaterializeError> {
        let node = self
            .nodes
            .get_mut(index)
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        node.links = links;
        *copied += std::mem::size_of::<Links>();
        Ok(())
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

    fn height(&self, index: Option<usize>) -> Result<usize, MaterializeError> {
        Ok(self.links(index)?.height)
    }

    fn refresh(&mut self, index: usize, copied: &mut usize) -> Result<(), MaterializeError> {
        let mut links = self.node(index)?.links;
        let [left, right] = links.child;
        links.height = 1 + self.height(left)?.max(self.height(right)?);
        self.set_links(index, links, copied)
    }

    // `side` selects the child promoted over `index`: right for a left rotation.
    fn rotate(
        &mut self,
        index: usize,
        side: usize,
        copied: &mut usize,
    ) -> Result<usize, MaterializeError> {
        let mut lower = self.node(index)?.links;
        let promoted = lower
            .child
            .get(side)
            .copied()
            .flatten()
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        let mut upper = self.node(promoted)?.links;
        let opposite = 1 - side;
        let middle = upper.child.get(opposite).copied().flatten();
        *lower
            .child
            .get_mut(side)
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)? = middle;
        if let Some(middle) = middle {
            let mut links = self.node(middle)?.links;
            links.parent = Some(index);
            self.set_links(middle, links, copied)?;
        }
        upper.parent = lower.parent;
        if let Some(parent) = lower.parent {
            let mut links = self.node(parent)?.links;
            let child = links
                .child
                .iter_mut()
                .find(|child| **child == Some(index))
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
            *child = Some(promoted);
            self.set_links(parent, links, copied)?;
        } else {
            self.root = Some(promoted);
            *copied += std::mem::size_of::<Option<usize>>();
        }
        lower.parent = Some(promoted);
        *upper
            .child
            .get_mut(opposite)
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)? = Some(index);
        self.set_links(index, lower, copied)?;
        self.set_links(promoted, upper, copied)?;
        self.refresh(index, copied)?;
        self.refresh(promoted, copied)?;
        Ok(promoted)
    }

    fn rebalance(
        &mut self,
        mut current: Option<usize>,
        copied: &mut usize,
    ) -> Result<(), MaterializeError> {
        while let Some(index) = current {
            self.refresh(index, copied)?;
            let [left, right] = self.node(index)?.links.child;
            let left_height = self.height(left)?;
            let right_height = self.height(right)?;
            let heavy = if left_height > right_height + 1 {
                Some(0)
            } else if right_height > left_height + 1 {
                Some(1)
            } else {
                None
            };
            let top = if let Some(side) = heavy {
                let child = if side == 0 { left } else { right }
                    .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
                let [a, b] = self.node(child)?.links.child;
                let inner_heavy = if side == 0 {
                    self.height(b)? > self.height(a)?
                } else {
                    self.height(a)? > self.height(b)?
                };
                if inner_heavy {
                    self.rotate(child, 1 - side, copied)?;
                }
                self.rotate(index, side, copied)?
            } else {
                index
            };
            current = self.node(top)?.links.parent;
        }
        Ok(())
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
                ..Links::default()
            },
        });
        let mut copied = std::mem::size_of::<Node<K, V>>();
        if let Some(parent) = parent {
            let mut links = self.node(parent)?.links;
            *links
                .child
                .get_mut(side)
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)? = Some(index);
            self.set_links(parent, links, &mut copied)?;
        } else {
            self.root = Some(index);
            copied += std::mem::size_of::<Option<usize>>();
        }
        self.rebalance(parent, &mut copied)?;
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
        (a.abs_diff(b) <= 1 && height == node.links.height).then_some((height, 1 + ac + bc))
    }
}
