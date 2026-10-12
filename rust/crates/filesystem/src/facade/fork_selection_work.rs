use super::*;

pub(super) fn fork_selection_items(
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    cancellation.check()?;
    accounting.charge(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(())
}

pub(super) fn fork_file_table_copy<T>(
    elements: usize,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    cancellation.check()?;
    let bytes = elements
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(WorkError::Overflow)?;
    accounting.charge(WorkCounters {
        bytes_copied: u64::try_from(bytes).map_err(|_| WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(())
}

pub(super) fn fork_selection_grow<T>(
    owner: &mut Owned<Vec<T>>,
    maximum: usize,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    if owner.value.len() == owner.value.capacity() {
        fork_file_table_copy::<T>(owner.value.len(), accounting, cancellation)?;
    }
    owner.ensure_for_push(maximum, accounting)?;
    Ok(())
}

pub(super) fn fork_file_table_search<T>(
    values: &[T],
    id: FileId,
    mut key: impl FnMut(&T) -> FileId,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Result<usize, usize>, WorkspaceError> {
    let mut start = 0;
    let mut end = values.len();
    while start < end {
        fork_selection_items(accounting, cancellation)?;
        let middle = start + (end - start) / 2;
        match key(&values[middle]).cmp(&id) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            std::cmp::Ordering::Greater => end = middle,
        }
    }
    Ok(Err(start))
}

pub(super) async fn fork_selection_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    source: &crate::Generation<A, O>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Checkout<A, O>, Option<u64>), WorkspaceError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return source
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(source.id),
                CheckoutMode::read_only_pinned(),
                original,
                cancellation,
            )
            .await
            .map(|checkout| (checkout, None))
            .map_err(WorkspaceError::from);
    }
    let receipt = source
        .workspace
        .volume
        .checkout(
            GenerationSelector::Exact(source.id),
            CheckoutMode::read_only_pinned(),
            accounting.remaining()?,
            cancellation,
        )
        .await;
    let spent = match &receipt {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    let (ledger, work, budget, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
    *work = work.with_backend(spent, ledger.live_bytes())?;
    work.verify_ref(&budget)?;
    let checkout = receipt
        .map_err(|failure| WorkspaceError::from(failure.error))?
        .value;
    let bytes = checkout.logical_owned_bytes();
    let (allocation, checked) = accounting.record_actual_owned(bytes)?;
    drop(allocation);
    checked?;
    Ok((checkout, Some(bytes)))
}

pub(super) fn fork_selection_reconcile_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    retained: &mut Option<u64>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    if let Some(previous) = retained {
        let actual = checkout.logical_owned_bytes();
        let checked = if actual >= *previous {
            let (allocation, checked) = accounting.record_actual_owned(actual - *previous)?;
            drop(allocation);
            checked
        } else {
            accounting.release_owned(&mut KernelOwnedAllocation::Standalone, *previous - actual)
        };
        *previous = actual;
        checked?;
    }
    Ok(())
}
