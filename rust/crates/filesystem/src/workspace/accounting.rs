use super::*;

pub(crate) fn workspace_vec_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}

/// Actual backing transferred by an owned string-vector input.
pub(crate) fn workspace_owned_strings_bytes(values: &Vec<String>) -> Result<u64, WorkspaceError> {
    let bytes = values.capacity().checked_mul(std::mem::size_of::<String>()).ok_or(crate::WorkError::Overflow)?;
    values.iter().try_fold(u64::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?, |bytes, value| {
        bytes.checked_add(u64::try_from(value.capacity()).map_err(|_| crate::WorkError::Overflow)?)
            .ok_or_else(|| crate::WorkError::Overflow.into())
    })
}

pub(crate) fn workspace_remaining(work: crate::WorkCounters, budget: WorkBudget, live: u64) -> Result<WorkBudget, WorkspaceError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(live)
        .ok_or(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: live, maximum: remaining.peak_allocation_bytes,
        })?;
    Ok(remaining)
}

pub(super) fn merge_workspace_backend<T, E: Into<WorkspaceError>>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>, work: &mut crate::WorkCounters, live: u64,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    let backend = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    *work = work.with_backend(backend, live).map_err(|error| WorkspaceError::WorkMerge { prior: *work, backend, error })?;
    result.map_err(|failure| failure.error.into())
}

pub(super) fn workspace_allocation_error(error: crate::kernel::allocation::AllocationError) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(super) fn merge_workspace_work<T>(
    prior: crate::WorkCounters, receipt: crate::OperationReceipt<T>, budget: WorkBudget,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, WorkspaceError> {
    let work = prior.checked_add(receipt.work)
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
    work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
    Ok(crate::OperationReceipt { value: receipt.value, work })
}
