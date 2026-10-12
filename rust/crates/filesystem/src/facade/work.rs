use super::*;

/// Work supplied as a standalone budget or an original native invocation.
pub trait FsWork: Sync + ProviderTask {
    /// Result shape for this work source.
    type Output<T, E>;
    #[doc(hidden)]
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a>;
    #[doc(hidden)]
    fn finish<T, E>(&self, result: Result<T, E>, accounting: &KernelAccounting<'_>) -> Self::Output<T, E>;
}

impl FsWork for WorkBudget {
    type Output<T, E> = MeasuredResult<FsReceipt<T>, E>;
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::standalone(*self, cancellation)
    }
    fn finish<T, E>(&self, result: Result<T, E>, accounting: &KernelAccounting<'_>) -> Self::Output<T, E> {
        measured(result, accounting)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FsWork for &NativeMaterializationWork {
    type Output<T, E> = Result<T, E>;
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::native(self, cancellation)
    }
    fn finish<T, E>(&self, result: Result<T, E>, _: &KernelAccounting<'_>) -> Self::Output<T, E> { result }
}

/// Successful facade operation with exact composed work.
pub type FsReceipt<T> = OperationReceipt<T>;

/// Receipt-bearing facade result.
pub type FsResult<T> = MeasuredResult<FsReceipt<T>, FsError>;

pub(super) fn remaining_owned(
    work: WorkCounters,
    budget: WorkBudget,
    live_bytes: u64,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    let mut remaining = work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    // The ledger already admitted these live bytes within the same permit.
    remaining.peak_allocation_bytes -= live_bytes;
    Ok(remaining)
}

#[inline]
pub(super) fn add(prior: WorkCounters, next: WorkCounters) -> Result<WorkCounters, OperationFailure<FsError>> {
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

pub(super) fn merge_simultaneous_failure<E: From<WorkError>>(
    prior: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    error: E,
) -> OperationFailure<E> {
    match prior.with_backend(nested, live_bytes) {
        Ok(merged) => OperationFailure::new(error, merged),
        Err(overflow) => OperationFailure::new(overflow.into(), prior),
    }
}

#[inline]
pub(crate) fn measured<T, E>(result: Result<T, E>, accounting: &KernelAccounting<'_>) -> MeasuredResult<FsReceipt<T>, E> {
    let work = accounting.spent();
    result.map(|value| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
}

