//! Filesystem custody for the shared fixed-width index.
use crate::FsError;
use crate::kernel::KernelAccounting;
use crate::kernel::allocation::retained::RetainedAllocation;

pub(crate) struct FixedIndex<V, const N: usize = 16>(
    acyclic_collections::FixedIndex<V, RetainedAllocation, N>,
);

impl<V, const N: usize> Default for FixedIndex<V, N> {
    fn default() -> Self {
        Self(Default::default())
    }
}

// This borrow forwards admission to the original frame. It owns no counters.
struct IndexAccounting<'a, 'b>(&'a mut KernelAccounting<'b>);

impl acyclic_collections::IndexWork for IndexAccounting<'_, '_> {
    type Error = FsError;
    type Allocation = RetainedAllocation;

    fn check(&mut self) -> Result<(), FsError> {
        self.0.check()
    }
    fn charge_items(&mut self, items: u64) -> Result<(), FsError> {
        self.0.charge_items(items)
    }
    fn claim(&mut self, bytes: u64) -> Result<RetainedAllocation, FsError> {
        RetainedAllocation::claim(bytes, 1, self.0)
    }
}

impl<V, const N: usize> FixedIndex<V, N> {
    pub(crate) fn get(
        &self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&V>, FsError> {
        self.0.get(key, &mut IndexAccounting(accounting))
    }

    pub(crate) fn get_or_try_insert_with(
        &mut self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
        create: impl FnOnce(&mut KernelAccounting<'_>) -> Result<V, FsError>,
    ) -> Result<&mut V, FsError> {
        self.0
            .get_or_try_insert_with(key, &mut IndexAccounting(accounting), |work| create(work.0))
    }

    pub(crate) fn retire_if(&mut self, key: [u8; N], matches: impl FnOnce(&V) -> bool) -> bool {
        self.0.retire_if(key, matches)
    }
}

#[cfg(test)]
mod tests;
