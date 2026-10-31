//! Filesystem custody for the shared bounded recent-fact cache.
use super::*;

pub(crate) struct RecentIndex<V, const N: usize, const WINDOW: usize>(
    acyclic_collections::RecentIndex<V, RetainedAllocation, N, WINDOW>,
);

impl<V, const N: usize, const WINDOW: usize> Default for RecentIndex<V, N, WINDOW> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<V, const N: usize, const WINDOW: usize> RecentIndex<V, N, WINDOW> {
    pub(crate) fn complete(&self) -> bool {
        self.0.complete()
    }

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
}

#[cfg(test)]
mod tests;
