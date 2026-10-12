use super::*;
use crate::kernel::allocation::AllocationLedger;
use crate::kernel::{KernelAccounting, KernelOwnedAllocation, Owned};

pub(crate) trait MemoryLazyNode: Serialize + Eq + Clone {
    fn owned_bytes(&self) -> u64;
    fn allocations(&self) -> u64;
    fn copy(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        accounting.check()?;
        let requested = self.owned_bytes();
        let mut guard = accounting.claim_owned(requested, self.allocations())?;
        let copied = WorkCounters {
            bytes_copied: requested,
            ..WorkCounters::UNCHARGED
        };
        if let Err(error) = accounting.admit(copied) {
            accounting.release_owned(&mut guard, requested)?;
            return Err(error.into());
        }
        let value = self.clone();
        let actual = value.owned_bytes();
        let checked = accounting
            .reconcile_owned(&mut guard, requested, actual)
            .and_then(|()| accounting.record_completed(copied));
        if let Err(error) = checked {
            drop(value);
            accounting.release_owned(&mut guard, actual)?;
            return Err(error.into());
        }
        Ok((value, guard))
    }
}

impl MemoryLazyNode for LazyOverlay {
    fn allocations(&self) -> u64 {
        u64::from(self.owned_bytes() != 0)
    }
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, .. } => path.capacity() as u64,
        }
    }
}

impl MemoryLazyNode for LazyShadow {
    fn allocations(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { record, .. } => 1 + u64::from(record.capacity() != 0),
        }
    }
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { record, .. } => {
                record.capacity() as u64 + std::mem::size_of::<crate::WorkspaceMetadata>() as u64
            }
        }
    }
}

pub(super) fn copy_bytes(
    bytes: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<u8>>, LazyWorkspaceError> {
    let mut out = Owned::allocate(bytes.len(), accounting)?;
    let work = WorkCounters {
        bytes_copied: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    if let Err(error) = accounting.admit(work) {
        out.retire(accounting)?;
        return Err(error.into());
    }
    out.value.extend_from_slice(bytes);
    if let Err(error) = accounting.record_completed(work) {
        out.retire(accounting)?;
        return Err(error.into());
    }
    Ok(out)
}

pub(crate) fn clone_lazy_node<T: MemoryLazyNode>(
    value: &T,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<T, LazyWorkspaceError> {
    let before = ledger.live_bytes();
    let result = value
        .copy(&mut KernelAccounting::borrowed_standalone(
            ledger,
            work,
            budget,
            cancellation,
        ))
        .map(|(value, _)| value);
    if result.is_err() {
        ledger
            .release(ledger.live_bytes() - before)
            .map_err(|e| e.into_work_or(crate::FsError::PendingMutationAllocationFailed))?;
    }
    result
}
