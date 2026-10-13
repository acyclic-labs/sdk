use super::*;
use crate::kernel::codec::ReadAllocationStep;

pub(super) fn clone_page<F: Format, E: From<CanonicalDecodeError>>(
    page: &Page<F>,
    observe: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<Page<F>, E> {
    use crate::kernel::codec::ReadAllocationStep;
    match page {
        Page::Leaf(values) => {
            let mut cloned = Vec::new();
            crate::kernel::file_read::reserve_decoded_items(&mut cloned, values.len(), observe)?;
            for value in values {
                observe(ReadAllocationStep::BeforeItem)?;
                let value = F::try_clone_value(value, u32::MAX, observe)?;
                observe(ReadAllocationStep::Item)?;
                let copied = usize_to_u64(size_of::<F::Value>());
                observe(ReadAllocationStep::BeforeCopy(copied))?;
                cloned.push(value);
                observe(ReadAllocationStep::Copied(copied))?;
            }
            Ok(Page::Leaf(cloned))
        }
        Page::Internal(children) => {
            let mut cloned = Vec::new();
            crate::kernel::file_read::reserve_decoded_items(&mut cloned, children.len(), observe)?;
            for child in children {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = F::try_clone_key(&child.first, u32::MAX, observe)?;
                observe(ReadAllocationStep::Item)?;
                let copied = usize_to_u64(size_of::<Child<F::Key>>());
                observe(ReadAllocationStep::BeforeCopy(copied))?;
                cloned.push(Child {
                    first,
                    page: child.page,
                });
                observe(ReadAllocationStep::Copied(copied))?;
            }
            Ok(Page::Internal(cloned))
        }
    }
}

fn logical_page_bytes<F: Format>(shape: DecodedPageShape) -> Option<u64> {
    let container_bytes = match shape.kind {
        DecodedPageKind::Leaf => shape.items.checked_mul(size_of::<F::Value>()),
        DecodedPageKind::Internal => shape.items.checked_mul(size_of::<Child<F::Key>>()),
    }
    .map(usize_to_u64)?;
    container_bytes.checked_add(shape.nested_bytes)
}

pub(crate) fn decode_read_with_accounting<F: Format>(
    read: &ObjectRead,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return crate::kernel::file_read::decode_owned(accounting, |steps| {
            let page = F::decode(read, limits, &mut |step| steps(step))?;
            Ok(page)
        })
        .map(|(page, allocation)| {
            let logical_bytes = match &allocation {
                KernelOwnedAllocation::NativeOriginal(owned) => owned.bytes(),
                KernelOwnedAllocation::Standalone => 0,
            };
            (
                OwnedPage {
                    page: PageLease::Owned(page),
                    logical_bytes,
                    native_custody: None,
                },
                allocation,
            )
        });
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .expect("native decoding uses the original step engine");
    decode_read_with_custody::<F>(read, limits, budget, allocations, work, None, 0)
        .map(|page| (page, KernelOwnedAllocation::Standalone))
        .map_err(accounted_leaf_error)
}

pub(crate) fn clone_key<F: Format>(
    key: &F::Key,
    maximum_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(F::Key, KernelOwnedAllocation), crate::FsError> {
    clone_item(
        F::key_nested_bytes(key),
        accounting,
        data_custody,
        cached_local_bytes,
        |steps| F::try_clone_key(key, maximum_bytes, &mut |step| steps(step)),
    )
}

pub(crate) fn clone_value<F: Format>(
    value: &F::Value,
    maximum_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(F::Value, KernelOwnedAllocation), crate::FsError> {
    clone_item(
        F::value_nested_bytes(value),
        accounting,
        data_custody,
        cached_local_bytes,
        |steps| F::try_clone_value(value, maximum_bytes, &mut |step| steps(step)),
    )
}

fn clone_item<T>(
    nested: u64,
    accounting: &mut KernelAccounting<'_>,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
    clone: impl FnOnce(
        &mut dyn FnMut(ReadAllocationStep) -> Result<(), crate::FsError>,
    ) -> Result<T, crate::FsError>,
) -> Result<(T, KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return crate::kernel::file_read::decode_owned(accounting, clone);
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .expect("native cloning uses the original step engine");
    if nested != 0 {
        admit_clone(
            nested,
            budget,
            allocations,
            work,
            data_custody,
            cached_local_bytes,
        )
        .map_err(accounted_leaf_error)?;
    }
    match clone(&mut |_| Ok(())) {
        Ok(value) => Ok((value, KernelOwnedAllocation::Standalone)),
        Err(error) => {
            allocations
                .release(nested)
                .map_err(|error| accounted_leaf_error(error.into()))?;
            Err(error)
        }
    }
}

pub(super) fn claim_read_allocation(
    bytes: u64,
    operations: u64,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(), Error> {
    work.verify(budget)?;
    allocations
        .live_bytes()
        .checked_sub(cached_local_bytes)
        .ok_or(WorkError::Overflow)?;
    let external = data_custody
        .map_or(0, crate::kernel::ProofDataCustody::operation_cache_bytes)
        .checked_sub(cached_local_bytes)
        .ok_or(WorkError::Overflow)?;
    let mut allowance = budget;
    allowance.peak_allocation_bytes =
        budget
            .peak_allocation_bytes
            .checked_sub(external)
            .ok_or(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed: external,
                maximum: budget.peak_allocation_bytes,
            })?;
    let prior_peak = work.peak_allocation_bytes;
    let mut next = *work;
    next.peak_allocation_bytes = 0;
    allocations.claim_bytes(bytes, operations, &mut next, allowance)?;
    next.peak_allocation_bytes = prior_peak.max(
        allocations
            .live_bytes()
            .checked_add(external)
            .ok_or(WorkError::Overflow)?,
    );
    next.verify(budget)?;
    *work = next;
    Ok(())
}

pub(super) fn decode_read_with_custody<F: Format>(
    read: &ObjectRead,
    limits: DecodeLimits,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<OwnedPage<F>, Error> {
    let prepared = (|| -> Result<_, Error> {
        let shape = F::decode_shape(read, limits)?;
        work.charge_items(usize_to_u64(shape.items), &budget)?;
        if let Some(owner) = data_custody {
            owner.record_completed_leaf_work(WorkCounters {
                items_examined: usize_to_u64(shape.items),
                ..WorkCounters::default()
            })?;
        }
        let logical_bytes = logical_page_bytes::<F>(shape).ok_or(Error::AllocationFailed)?;
        Ok((shape, logical_bytes))
    })();
    let (shape, logical_bytes) = prepared?;
    claim_read_allocation(
        logical_bytes,
        u64::from(logical_bytes != 0),
        allocations,
        work,
        budget,
        data_custody,
        cached_local_bytes,
    )?;
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            allocation_operations: u64::from(logical_bytes != 0),
            peak_allocation_bytes: allocations
                .live_bytes()
                .checked_sub(cached_local_bytes)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::default()
        }) {
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    }
    let copied = match work.checked_add(WorkCounters {
        bytes_copied: shape.nested_bytes,
        ..WorkCounters::default()
    }) {
        Ok(copied) => copied,
        Err(error) => {
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    };
    if let Err(error) = copied.verify(budget) {
        allocations.release(logical_bytes)?;
        return Err(error.into());
    }
    *work = copied;
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            bytes_copied: shape.nested_bytes,
            ..WorkCounters::default()
        }) {
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    }
    let decoded = match F::decode(read, limits, &mut |_| Ok::<_, CanonicalDecodeError>(())) {
        Ok(decoded) => decoded,
        Err(error) => {
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    };
    Ok(OwnedPage {
        page: PageLease::Owned(decoded),
        logical_bytes,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    })
}

fn admit_clone(
    nested: u64,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(), Error> {
    let copied = WorkCounters {
        bytes_copied: nested,
        ..WorkCounters::UNCHARGED
    };
    work.admit(&copied, &budget)?;
    let prior_live = allocations.live_bytes();
    let mut next = *work;
    claim_read_allocation(
        nested,
        u64::from(nested != 0),
        allocations,
        &mut next,
        budget,
        data_custody,
        cached_local_bytes,
    )?;
    next.charge(&copied, &budget)?;
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_operation_clone_work(
            nested,
            prior_live
                .checked_sub(cached_local_bytes)
                .ok_or(WorkError::Overflow)?,
        ) {
            allocations.release(nested)?;
            return Err(error.into());
        }
    }
    *work = next;
    Ok(())
}
