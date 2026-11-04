//! Original index admission before acquiring a funding-reentrant owner lock.
use super::*;

/// A single synchronous traversal's reserved work and optional insertion backing.
/// This owns original tokens, never a copied remaining caller allowance.
pub(crate) struct PreparedIndexWork {
    items: u64,
    allocation: Option<RetainedAllocation>,
    claims: u8,
}

impl PreparedIndexWork {
    pub(crate) fn read<const N: usize, T>(
        accounting: &mut KernelAccounting<'_>,
        operation: impl FnOnce(&mut Self, &mut KernelAccounting<'_>) -> T,
    ) -> Result<T, FsError> {
        // A lookup visits at most 8N branches, one leaf and N exact key bytes.
        let items = (N as u64)
            .checked_mul(9)
            .and_then(|n| n.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        Self::run(items, None, accounting, operation)
    }

    pub(crate) fn insert<V, const N: usize, T>(
        accounting: &mut KernelAccounting<'_>,
        operation: impl FnOnce(&mut Self, &mut KernelAccounting<'_>) -> T,
    ) -> Result<T, FsError> {
        let bound = acyclic_collections::FixedIndex::<V, RetainedAllocation, N>::insertion_bound(1)
            .ok_or(crate::WorkError::Overflow)?;
        Self::run(
            bound.examined_items,
            Some(bound.allocation_bytes),
            accounting,
            operation,
        )
    }

    fn run<T>(
        items: u64,
        backing: Option<u64>,
        accounting: &mut KernelAccounting<'_>,
        operation: impl FnOnce(&mut Self, &mut KernelAccounting<'_>) -> T,
    ) -> Result<T, FsError> {
        let bound = crate::WorkCounters {
            items_examined: items,
            ..crate::WorkCounters::UNCHARGED
        };
        // Reserve before the owner lock, then settle after its guard has left
        // the synchronous closure. Native callers share the original atomic
        // credit; an ordinary caller is exclusively borrowed for this scope.
        #[cfg(not(target_arch = "wasm32"))]
        let mut credit = super::super::effects::EffectCredit::for_accounting(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut credit {
            credit.reserve(bound, false)?;
        } else {
            accounting.admit(bound)?;
        }
        #[cfg(target_arch = "wasm32")]
        accounting.admit(bound)?;
        let allocation = backing
            .map(|bytes| RetainedAllocation::claim(bytes, 2, accounting))
            .transpose()?;
        let mut work = Self {
            items,
            claims: if allocation.is_some() { 2 } else { 0 },
            allocation,
        };
        let output = operation(&mut work, accounting);
        let actual = crate::WorkCounters {
            items_examined: items - work.items,
            ..crate::WorkCounters::UNCHARGED
        };
        drop(work);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut credit {
            credit.complete(actual)?;
        } else {
            accounting.record_completed(actual)?;
        }
        #[cfg(target_arch = "wasm32")]
        accounting.record_completed(actual)?;
        Ok(output)
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
