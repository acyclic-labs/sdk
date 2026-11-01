//! Original index admission before acquiring a funding-reentrant owner lock.
use super::*;

/// A single synchronous traversal's prepaid work and optional insertion backing.
/// This owns original tokens, never a copied remaining caller allowance.
pub(crate) struct PreparedIndexWork {
    items: u64,
    allocation: Option<RetainedAllocation>,
    claims: u8,
}

impl PreparedIndexWork {
    pub(crate) fn read<const N: usize>(
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        // A lookup visits at most 8N branches, one leaf and N exact key bytes.
        let items = (N as u64)
            .checked_mul(9)
            .and_then(|n| n.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        accounting.charge_items(items)?;
        Ok(Self {
            items,
            allocation: None,
            claims: 0,
        })
    }

    pub(crate) fn insert<V, const N: usize>(
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let bound = acyclic_collections::FixedIndex::<V, RetainedAllocation, N>::insertion_bound(1)
            .ok_or(crate::WorkError::Overflow)?;
        accounting.charge_items(bound.examined_items)?;
        let allocation = RetainedAllocation::claim(bound.allocation_bytes, 2, accounting)?;
        Ok(Self {
            items: bound.examined_items,
            allocation: Some(allocation),
            claims: 2,
        })
    }
}

impl acyclic_collections::IndexWork for PreparedIndexWork {
    type Error = FsError;
    type Allocation = RetainedAllocation;

    fn check(&mut self) -> Result<(), FsError> {
        // Original cancellation/validity was checked by the synchronous admission.
        Ok(())
    }

    fn charge_items(&mut self, items: u64) -> Result<(), FsError> {
        self.items = self
            .items
            .checked_sub(items)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(())
    }

    fn claim(&mut self, bytes: u64) -> Result<RetainedAllocation, FsError> {
        let remaining = self
            .claims
            .checked_sub(1)
            .ok_or(crate::WorkError::Overflow)?;
        let allocation = self
            .allocation
            .as_mut()
            .ok_or(crate::WorkError::Overflow)?
            .split_off(bytes)?;
        self.claims = remaining;
        Ok(allocation)
    }
}
