//! Shared append-only AVL topology. Consumers own values and their original
//! allocation/copy custody; rotations only rewrite links, never move payloads.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Links {
    pub(crate) child: [Option<usize>; 2],
    pub(crate) parent: Option<usize>,
    pub(crate) height: usize,
    pub(crate) count: usize,
}

pub(crate) trait Tree: Sized {
    type Error;
    fn invalid() -> Self::Error;
    fn root(&self) -> Option<usize>;
    fn put_root(&mut self, root: Option<usize>, copied: &mut usize) -> Result<(), Self::Error>;
    fn get_links(&self, index: usize) -> Result<Links, Self::Error>;
    fn put_links(
        &mut self,
        index: usize,
        links: Links,
        copied: &mut usize,
    ) -> Result<(), Self::Error>;

    fn links(&self, index: Option<usize>) -> Result<Links, Self::Error> {
        index.map_or(Ok(Links::default()), |index| self.get_links(index))
    }

    fn refresh(&mut self, index: usize, copied: &mut usize) -> Result<(), Self::Error> {
        let mut links = self.get_links(index)?;
        let [left, right] = links.child;
        let left = self.links(left)?;
        let right = self.links(right)?;
        links.height = left
            .height
            .max(right.height)
            .checked_add(1)
            .ok_or_else(Self::invalid)?;
        links.count = left
            .count
            .checked_add(right.count)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(Self::invalid)?;
        self.put_links(index, links, copied)
    }

    // `side` selects the promoted child: right for a left rotation.
    fn rotate(
        &mut self,
        index: usize,
        side: usize,
        copied: &mut usize,
    ) -> Result<usize, Self::Error> {
        let mut lower = self.get_links(index)?;
        let promoted = lower
            .child
            .get(side)
            .copied()
            .flatten()
            .ok_or_else(Self::invalid)?;
        let mut upper = self.get_links(promoted)?;
        let opposite = 1 - side;
        let middle = upper.child[opposite];
        lower.child[side] = middle;
        if let Some(middle) = middle {
            let mut links = self.get_links(middle)?;
            links.parent = Some(index);
            self.put_links(middle, links, copied)?;
        }
        upper.parent = lower.parent;
        if let Some(parent) = lower.parent {
            let mut links = self.get_links(parent)?;
            let child = links
                .child
                .iter_mut()
                .find(|child| **child == Some(index))
                .ok_or_else(Self::invalid)?;
            *child = Some(promoted);
            self.put_links(parent, links, copied)?;
        } else {
            self.put_root(Some(promoted), copied)?;
        }
        lower.parent = Some(promoted);
        upper.child[opposite] = Some(index);
        self.put_links(index, lower, copied)?;
        self.put_links(promoted, upper, copied)?;
        self.refresh(index, copied)?;
        self.refresh(promoted, copied)?;
        Ok(promoted)
    }

    fn rebalance(
        &mut self,
        mut current: Option<usize>,
        copied: &mut usize,
    ) -> Result<(), Self::Error> {
        while let Some(index) = current {
            self.refresh(index, copied)?;
            let [left, right] = self.get_links(index)?.child;
            let left_height = self.links(left)?.height;
            let right_height = self.links(right)?.height;
            let heavy = if left_height > right_height + 1 {
                Some(0)
            } else if right_height > left_height + 1 {
                Some(1)
            } else {
                None
            };
            let top = if let Some(side) = heavy {
                let child = if side == 0 { left } else { right }.ok_or_else(Self::invalid)?;
                let [a, b] = self.get_links(child)?.child;
                let inner_heavy = if side == 0 {
                    self.links(b)?.height > self.links(a)?.height
                } else {
                    self.links(a)?.height > self.links(b)?.height
                };
                if inner_heavy {
                    self.rotate(child, 1 - side, copied)?;
                }
                self.rotate(index, side, copied)?
            } else {
                index
            };
            current = self.get_links(top)?.parent;
        }
        Ok(())
    }

    fn attach(
        &mut self,
        index: usize,
        parent: Option<usize>,
        side: usize,
        copied: &mut usize,
    ) -> Result<(), Self::Error> {
        if let Some(parent) = parent {
            let mut links = self.get_links(parent)?;
            *links.child.get_mut(side).ok_or_else(Self::invalid)? = Some(index);
            self.put_links(parent, links, copied)?;
        } else {
            self.put_root(Some(index), copied)?;
        }
        self.rebalance(parent, copied)
    }

    fn first(&self) -> Result<Option<usize>, Self::Error> {
        let mut current = self.root();
        while let Some(index) = current {
            let left = self.get_links(index)?.child[0];
            if left.is_none() {
                return Ok(current);
            }
            current = left;
        }
        Ok(None)
    }

    fn successor(&self, index: usize) -> Result<Option<usize>, Self::Error> {
        let links = self.get_links(index)?;
        if let Some(mut current) = links.child[1] {
            while let Some(left) = self.get_links(current)?.child[0] {
                current = left;
            }
            return Ok(Some(current));
        }
        let mut child = index;
        let mut parent = links.parent;
        while let Some(index) = parent {
            let links = self.get_links(index)?;
            if links.child[0] == Some(child) {
                return Ok(parent);
            }
            if links.child[1] != Some(child) {
                return Err(Self::invalid());
            }
            child = index;
            parent = links.parent;
        }
        Ok(None)
    }

    /// Select a logical rank without comparing or examining another payload.
    fn select(&self, mut rank: usize) -> Result<usize, Self::Error> {
        let mut current = self.root();
        while let Some(index) = current {
            let links = self.get_links(index)?;
            let left = self.links(links.child[0])?.count;
            match rank.cmp(&left) {
                std::cmp::Ordering::Less => current = links.child[0],
                std::cmp::Ordering::Equal => return Ok(index),
                std::cmp::Ordering::Greater => {
                    rank -= left + 1;
                    current = links.child[1];
                }
            }
        }
        Err(Self::invalid())
    }

    /// The links an insertion at this rank will change, before mutation.
    fn insertion(&self, mut rank: usize) -> Result<(Option<usize>, usize, usize), Self::Error> {
        let mut current = self.root();
        let mut parent = None;
        let mut side = 0;
        let mut depth = 0;
        while let Some(index) = current {
            let links = self.get_links(index)?;
            let left = self.links(links.child[0])?.count;
            side = usize::from(rank > left);
            if side == 1 {
                rank -= left + 1;
            }
            parent = Some(index);
            depth += 1;
            current = links.child[side];
        }
        if rank != 0 {
            return Err(Self::invalid());
        }
        Ok((parent, side, depth))
    }
}

/// Exact before-effect copy admission on the caller's original accounting.
/// A failed mutation is discarded by its owner; no partially updated topology
/// is published. Native reservations are held across each actual link write.
pub(crate) struct Metered<'a, 'operation, T> {
    pub(crate) tree: &'a mut T,
    pub(crate) accounting: &'a mut super::allocation::KernelAccounting<'operation>,
}

impl<T: Tree<Error = crate::FsError>> Tree for Metered<'_, '_, T> {
    type Error = crate::FsError;
    fn invalid() -> Self::Error {
        T::invalid()
    }
    fn root(&self) -> Option<usize> {
        self.tree.root()
    }
    fn get_links(&self, index: usize) -> Result<Links, Self::Error> {
        self.tree.get_links(index)
    }
    fn put_root(&mut self, root: Option<usize>, copied: &mut usize) -> Result<(), Self::Error> {
        self.accounting.perform(
            crate::WorkCounters {
                bytes_copied: std::mem::size_of::<Option<usize>>() as u64,
                ..crate::WorkCounters::UNCHARGED
            },
            || self.tree.put_root(root, copied),
        )?
    }
    fn put_links(
        &mut self,
        index: usize,
        links: Links,
        copied: &mut usize,
    ) -> Result<(), Self::Error> {
        self.accounting.perform(
            crate::WorkCounters {
                bytes_copied: std::mem::size_of::<Links>() as u64,
                ..crate::WorkCounters::UNCHARGED
            },
            || self.tree.put_links(index, links, copied),
        )?
    }
}
