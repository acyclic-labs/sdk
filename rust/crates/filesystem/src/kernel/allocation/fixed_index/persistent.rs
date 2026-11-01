//! Immutable index preparation on the original filesystem allocation owner.
use super::*;

pub(crate) struct PersistentIndex<V, const N: usize>(
    acyclic_collections::PersistentIndex<V, RetainedAllocation, N>,
);

impl<V, const N: usize> Default for PersistentIndex<V, N> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<V, const N: usize> PersistentIndex<V, N> {
    pub(crate) fn get(
        &self,
        key: [u8; N],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&V>, FsError> {
        Ok(self
            .0
            .predecessor(key, &mut IndexAccounting(accounting))?
            .filter(|position| position.key == &key)
            .map(|position| position.value))
    }

    pub(crate) fn insert(
        &self,
        key: [u8; N],
        value: V,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        self.0
            .insert(key, value, &mut IndexAccounting(accounting))
            .map(Self)
    }
}
