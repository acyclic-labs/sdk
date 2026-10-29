//! Append-only payload custody with an ordered rank index. Insertion rewrites
//! logarithmically many integer links; accumulated payloads never shift.
use super::*;
use crate::kernel::ordered_links::{Links, Tree};

pub(super) struct Changes<F: Format> {
    pub(super) values: Owned<Vec<ValueChange<F::Key, F::Value>>>,
    pub(super) links: Owned<Vec<Links>>,
    root: Option<usize>,
    pub(super) maximum: usize,
    pub(super) truncated: bool,
    pub(super) ordered: bool,
}

impl<F: Format> Changes<F> {
    pub(super) fn allocate(
        maximum: usize,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let values = Owned::allocate(0, accounting)?;
        let links = match Owned::allocate(0, accounting) {
            Ok(links) => links,
            Err(error) => {
                values.retire(accounting)?;
                return Err(error);
            }
        };
        Ok(Self {
            values,
            links,
            root: None,
            maximum,
            truncated: false,
            ordered: true,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.values.value.len()
    }

    pub(super) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.links.retire(accounting)?;
        self.values.retire(accounting)
    }

    /// The terminal permutation uses only established ranks, with no new key
    /// comparisons. Height is scratch after ordering is final; subtree counts
    /// stay intact until every destination rank has been recorded.
    pub(super) fn order(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        if self.ordered {
            return Ok(());
        }
        let mut current = self.first()?;
        for rank in 0..self.len() {
            let index = current.ok_or(FsError::InvalidDiff)?;
            current = self.successor(index)?;
            let links = self
                .links
                .value
                .get_mut(index)
                .ok_or(FsError::InvalidDiff)?;
            accounting.perform(
                WorkCounters {
                    bytes_copied: std::mem::size_of::<usize>() as u64,
                    ..WorkCounters::UNCHARGED
                },
                || links.height = rank,
            )?;
        }
        for index in 0..self.len() {
            loop {
                let destination = self
                    .links
                    .value
                    .get(index)
                    .ok_or(FsError::InvalidDiff)?
                    .height;
                if destination == index {
                    break;
                }
                if destination >= self.len() {
                    return Err(FsError::InvalidDiff);
                }
                accounting.perform(
                    WorkCounters {
                        bytes_copied: (2
                            * (std::mem::size_of::<Links>()
                                + std::mem::size_of::<ValueChange<F::Key, F::Value>>()))
                            as u64,
                        ..WorkCounters::UNCHARGED
                    },
                    || {
                        self.values.value.swap(index, destination);
                        self.links.value.swap(index, destination);
                    },
                )?;
            }
        }
        Ok(())
    }

    #[cfg(all(test, feature = "memory"))]
    pub(super) fn snapshot(&self) -> Vec<ValueChange<F::Key, F::Value>> {
        (0..self.len())
            .map(|rank| {
                let index = self.select(rank).unwrap();
                self.values.value.get(index).unwrap().clone()
            })
            .collect()
    }
}

impl<F: Format> Tree for Changes<F> {
    type Error = FsError;
    fn invalid() -> Self::Error {
        FsError::InvalidDiff
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
        self.links
            .value
            .get(index)
            .copied()
            .ok_or(FsError::InvalidDiff)
    }
    fn put_links(
        &mut self,
        index: usize,
        links: Links,
        copied: &mut usize,
    ) -> Result<(), Self::Error> {
        *self
            .links
            .value
            .get_mut(index)
            .ok_or(FsError::InvalidDiff)? = links;
        *copied += std::mem::size_of::<Links>();
        Ok(())
    }
}
