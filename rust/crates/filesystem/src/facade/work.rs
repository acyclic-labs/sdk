use super::*;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
#[cfg(not(target_arch = "wasm32"))]
use crate::{NativeAllocation, NativeMaterializationWork};
use acyclic_stream::ProviderTask;

/// Work supplied as a standalone budget or an original native invocation.
pub trait FsWork: Sync + ProviderTask {
    /// Result shape for this work source.
    type Output<T, E>;
    /// Result shape for an owned output buffer.
    type OwnedOutput<T>;
    #[doc(hidden)]
    fn finish_owned<T>(
        &self,
        result: Result<(T, KernelOwnedAllocation), FsError>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T>;
    #[doc(hidden)]
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a>;
    #[doc(hidden)]
    fn finish<T, E>(
        &self,
        result: Result<T, E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::Output<T, E>;
}

impl FsWork for WorkBudget {
    type Output<T, E> = MeasuredResult<OperationReceipt<T>, E>;
    type OwnedOutput<T> = FsResult<T>;
    fn finish_owned<T>(
        &self,
        result: Result<(T, KernelOwnedAllocation), FsError>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        measured(
            result.map(|(value, allocation)| {
                drop(allocation);
                value
            }),
            accounting,
        )
    }
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::standalone(*self, cancellation)
    }
    fn finish<T, E>(
        &self,
        result: Result<T, E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::Output<T, E> {
        measured(result, accounting)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FsWork for &NativeMaterializationWork {
    type Output<T, E> = Result<T, E>;
    type OwnedOutput<T> = Result<(T, NativeAllocation), FsError>;
    fn finish_owned<T>(
        &self,
        result: Result<(T, KernelOwnedAllocation), FsError>,
        _: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        result.and_then(|(value, allocation)| {
            Ok((
                value,
                match allocation {
                    KernelOwnedAllocation::NativeOriginal(guard) => guard,
                    KernelOwnedAllocation::Standalone => {
                        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                    }
                },
            ))
        })
    }
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::native(self, cancellation)
    }
    fn finish<T, E>(&self, result: Result<T, E>, _: &KernelAccounting<'_>) -> Self::Output<T, E> {
        result
    }
}

/// Receipt-bearing facade result.
pub type FsResult<T> = MeasuredResult<OperationReceipt<T>, FsError>;

#[inline]
pub(super) fn add(
    prior: WorkCounters,
    next: WorkCounters,
) -> Result<WorkCounters, OperationFailure<FsError>> {
    prior
        .checked_add(next)
        .map_err(|error| OperationFailure::new(error.into(), prior))
}

#[inline]
pub(super) fn remaining(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

pub(super) fn merge_simultaneous_work(
    prior: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, OperationFailure<FsError>> {
    let merged = prior
        .with_backend(nested, live_bytes)
        .map_err(|error| OperationFailure::new(error.into(), prior))?;
    merged
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), merged))?;
    Ok(merged)
}

pub(super) fn merge_simultaneous_failure(
    prior: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    error: FsError,
) -> OperationFailure<FsError> {
    match prior.with_backend(nested, live_bytes) {
        Ok(merged) => OperationFailure::new(error, merged),
        Err(overflow) => OperationFailure::new(overflow.into(), prior),
    }
}

#[inline]
pub(crate) fn measured<T, E>(
    result: Result<T, E>,
    accounting: &KernelAccounting<'_>,
) -> MeasuredResult<OperationReceipt<T>, E> {
    let work = accounting.spent();
    result
        .map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

pub(super) fn remaining_owned(
    work: WorkCounters,
    budget: WorkBudget,
    live_bytes: u64,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    let mut remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    // The ledger already admitted these live bytes within the same permit.
    remaining.peak_allocation_bytes -= live_bytes;
    Ok(remaining)
}

pub(super) fn authority_work<'a>(
    accounting: &KernelAccounting<'a>,
    cancellation: &'a CancellationToken,
) -> Result<crate::AuthorityWork<'a>, FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return Ok(crate::AuthorityWork::native(original, cancellation));
    }
    Ok(crate::AuthorityWork::new(
        accounting.remaining()?,
        cancellation,
    ))
}

pub(super) fn record_authority_work(
    accounting: &mut KernelAccounting<'_>,
    authority: &crate::AuthorityWork<'_>,
) -> Result<(), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return Ok(());
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    *work = work.with_backend(authority.spent(), allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}
