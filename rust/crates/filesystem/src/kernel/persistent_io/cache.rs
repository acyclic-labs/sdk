use super::*;
use crate::kernel::allocation::arc_control_bytes;

pub(crate) async fn read_page_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    if cancellation.is_cancelled() {
        return Err(FsError::Cancelled(crate::CancellationError));
    }
    accounting.charge(PAGE_READ)?;
    let cache_key = DecodedCacheKey::new::<SharedPage<F>>(page, limits);
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work();
    #[cfg(not(target_arch = "wasm32"))]
    let standalone = original.is_none();
    #[cfg(target_arch = "wasm32")]
    let standalone = true;
    if standalone && let Some(owner) = data_custody {
        owner.record_completed_leaf_work(PAGE_READ)?;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = original {
        work.remaining()?;
    }
    if let Some((page, allocation)) =
        cached_page_with_accounting::<S, F>(store, page, limits, accounting)?
    {
        return Ok((
            OwnedPage {
                page,
                logical_bytes: 0,
            },
            allocation,
        ));
    }
    let (read, retained) =
        read_cold_object(store, page, limits, accounting, cancellation, data_custody).await?;
    let decoded = decode_read_with_accounting::<F>(&read, limits, accounting, data_custody);
    drop(read);
    if standalone {
        let mut allocation = KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, retained)?;
    }
    admit_decoded_with_accounting(store, cache_key, decoded?, accounting, data_custody)
}

pub(crate) fn cached_page_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(PageLease<F>, KernelOwnedAllocation)>, FsError> {
    accounting.check()?;
    let cache_key = DecodedCacheKey::new::<SharedPage<F>>(page, limits);
    let cached = store
        .decoded_cache_get(cache_key)
        .map_err(read_store_error)?;
    if let Some(cached) = cached {
        let page = cached
            .value
            .downcast::<SharedPage<F>>()
            .map_err(|_| read_store_error(ObjectStoreError::Corrupt))?;
        let page = PageLease::Shared {
            page,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: cached.native_custody,
        };
        let allocation = accounting.claim_owned(0, 0)?;
        return Ok(Some((page, allocation)));
    }
    Ok(None)
}

async fn read_cold_object<S: AsyncObjectStore>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(ObjectRead, u64), FsError> {
    let maximum = limits.maximum_page_object_bytes();
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let read = crate::heap_future::in_heap_claimed(original, || async {
            let mut work = crate::AuthorityWork::native(original, cancellation);
            store.read(page, maximum, &mut work).await
        })?
        .await;
        return read.map(|read| (read, 0)).map_err(read_store_error);
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(crate::ObjectStoreError::Corrupt)?;
    let prospective = *work;
    let mut remaining = prospective.remaining(budget)?;
    remaining.peak_allocation_bytes = budget
        .peak_allocation_bytes
        .checked_sub(allocations.live_bytes())
        .ok_or(WorkError::Overflow)?;
    let live = allocations.live_bytes();
    let frame_observer = |event| -> Result<u64, WorkError> {
        let Some(owner) = data_custody else {
            return Ok(0);
        };
        let standing = owner.record_heap_future_event(event)?;
        if matches!(event, crate::heap_future::HeapFutureEvent::Admitted(_)) {
            owner.record_completed_leaf_work(WorkCounters {
                peak_allocation_bytes: live,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(standing)
    };
    let read = crate::heap_future::in_heap_measured(
        move |budget| async move {
            cancellation
                .check()
                .map_err(|_| crate::ObjectFailure::before_work(ObjectStoreError::Cancelled))
                .map_err(|failure| {
                    failure.map_with_prior_work(WorkCounters::UNCHARGED, read_store_error)
                })?;
            let mut context = crate::AuthorityWork::new(budget, cancellation);
            context.read_observation =
                data_custody.map(|owner| (owner, owner.operation_cache_bytes()));
            let result = store.read(page, maximum, &mut context).await;
            context.read_receipt(result).map_err(|failure| {
                failure.map_with_prior_work(WorkCounters::UNCHARGED, read_store_error)
            })
        },
        remaining,
        cancellation,
        read_store_error(ObjectStoreError::Cancelled),
        data_custody.map(|_| &frame_observer as &(dyn Fn(_) -> Result<u64, WorkError> + Sync)),
    )
    .await;
    let nested = match &read {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    *work = prospective.with_backend(nested, live)?;
    if let Some(owner) = data_custody {
        owner.record_data_work(WorkCounters {
            peak_allocation_bytes: work.peak_allocation_bytes,
            ..WorkCounters::UNCHARGED
        })?;
    }
    let receipt = read.map_err(|failure| failure.error)?;
    work.verify(budget)?;
    let retained = retained_bytes(&receipt.value);
    claim_read_allocation(retained, 0, allocations, work, budget, data_custody, 0)?;
    if let Some(owner) = data_custody
        && let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            peak_allocation_bytes: allocations.live_bytes(),
            ..WorkCounters::UNCHARGED
        })
    {
        drop(receipt.value);
        allocations.release(retained).map_err(allocation_error)?;
        return Err(error.into());
    }
    Ok((receipt.value, retained))
}

#[allow(
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
pub(crate) fn admit_decoded_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    key: DecodedCacheKey,
    decoded: (OwnedPage<F>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), FsError> {
    if matches!(&decoded.0.page, PageLease::Shared { .. }) {
        return Ok(decoded);
    }
    if !store.supports_decoded_cache() {
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelAccounting::NativeOriginal { work, cancellation } = accounting {
            work.remaining()
                .map_err(crate::ObjectStoreError::from)
                .map_err(read_store_error)?;
            cancellation.check()?;
            // Sharing used to absorb a control guard, which also required the
            // same resource scope. Retain that invariant without a zero guard.
            if !matches!(&decoded.1, KernelOwnedAllocation::NativeOriginal(body)
                if body.work().shares_allocation_context(work))
            {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
            return Ok(decoded);
        }
        let (allocations, work, budget, _) = accounting
            .standalone_parts()
            .ok_or(crate::ObjectStoreError::Corrupt)?;
        if let Err(error) = claim_read_allocation(0, 0, allocations, work, budget, data_custody, 0)
        {
            let (page, mut allocation) = decoded;
            let bytes = page.logical_bytes;
            drop(page);
            accounting.release_owned(&mut allocation, bytes)?;
            return Err(error);
        }
        return Ok(decoded);
    }
    let empty = match accounting.claim_owned(0, 0) {
        Ok(empty) => empty,
        Err(error) => {
            let (page, mut allocation) = decoded;
            let bytes = page.logical_bytes;
            drop(page);
            accounting.release_owned(&mut allocation, bytes)?;
            return Err(error);
        }
    };
    let before = accounting.spent();
    let standalone = matches!(decoded.1, KernelOwnedAllocation::Standalone);
    let prepared = prepare_shared_page(decoded, accounting);
    let completed = (|| {
        if standalone && let Some(owner) = data_custody {
            let after = accounting.spent();
            owner.record_completed_leaf_work(WorkCounters {
                allocation_operations: after
                    .allocation_operations
                    .checked_sub(before.allocation_operations)
                    .ok_or(WorkError::Overflow)?,
                bytes_copied: after
                    .bytes_copied
                    .checked_sub(before.bytes_copied)
                    .ok_or(WorkError::Overflow)?,
                items_examined: after
                    .items_examined
                    .checked_sub(before.items_examined)
                    .ok_or(WorkError::Overflow)?,
                peak_allocation_bytes: after.peak_allocation_bytes,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok::<_, FsError>(())
    })();
    let owned = match (prepared, completed) {
        (Ok(owned), Ok(())) => owned,
        (Ok(owned), Err(error)) => {
            owned.retire(accounting)?;
            return Err(error);
        }
        (Err(error), _) => return Err(error),
    };
    admit_shared_page(store, key, owned, empty, accounting)
}

pub(super) fn admit_shared_page<S: AsyncObjectStore, F: Format>(
    store: &S,
    key: DecodedCacheKey,
    owned: crate::kernel::persistent_point::Owned<SharedPage<F>>,
    empty: KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), FsError> {
    let standalone = matches!(owned.allocation, KernelOwnedAllocation::Standalone);
    let logical_bytes = owned.value.logical_bytes();
    let local_bytes = owned.bytes;
    #[cfg(not(target_arch = "wasm32"))]
    let native_custody = match owned.allocation {
        KernelOwnedAllocation::NativeOriginal(custody) => Some(Arc::new(custody)),
        KernelOwnedAllocation::Standalone => None,
    };
    let value = DecodedCacheValue {
        value: Arc::new(owned.value),
        logical_bytes,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody,
    };
    let admitted = store.decoded_cache_admit(key, value);
    let (value, uncached) = match admitted {
        Ok(DecodedCacheAdmission::Shared(value)) => (value, false),
        Ok(DecodedCacheAdmission::Uncached(value)) => (value, true),
        Err(error) => {
            if standalone {
                accounting.release_owned(&mut KernelOwnedAllocation::Standalone, local_bytes)?;
            }
            return Err(read_store_error(error));
        }
    };
    #[cfg(not(target_arch = "wasm32"))]
    let native = value.native_custody.is_some();
    #[cfg(target_arch = "wasm32")]
    let native = false;
    let retained = if standalone && uncached && !native {
        local_bytes
    } else {
        0
    };
    if standalone && retained == 0 {
        accounting.release_owned(&mut KernelOwnedAllocation::Standalone, local_bytes)?;
    }
    let page = match value.value.downcast::<SharedPage<F>>() {
        Ok(page) => page,
        Err(value) => {
            drop(value);
            if retained != 0 {
                accounting.release_owned(&mut KernelOwnedAllocation::Standalone, retained)?;
            }
            return Err(read_store_error(ObjectStoreError::Corrupt));
        }
    };
    Ok((
        OwnedPage {
            page: PageLease::Shared {
                page,
                #[cfg(not(target_arch = "wasm32"))]
                _native_custody: value.native_custody,
            },
            logical_bytes: retained,
        },
        empty,
    ))
}

fn prepare_shared_page<F: Format>(
    decoded: (OwnedPage<F>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<super::super::persistent_point::Owned<SharedPage<F>>, FsError> {
    use super::super::persistent_point::Owned;
    let PageLease::Owned(page) = decoded.0.page else {
        unreachable!("shared leases return before admission");
    };
    let shared = SharedPage::from_owned(
        Owned::new(page, decoded.1, decoded.0.logical_bytes),
        accounting,
    )?;
    reserve_shared_controls(shared, accounting)
}

pub(super) fn reserve_shared_controls<F: Format>(
    mut shared: crate::kernel::persistent_point::Owned<SharedPage<F>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::persistent_point::Owned<SharedPage<F>>, FsError> {
    let (controls, operations) = match &shared.allocation {
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => (
            arc_control_bytes::<SharedPage<F>>()?
                .checked_add(arc_control_bytes::<crate::NativeAllocation>()?)
                .ok_or(WorkError::Overflow)?,
            2,
        ),
        KernelOwnedAllocation::Standalone => (arc_control_bytes::<SharedPage<F>>()?, 1),
    };
    let Some(bytes) = shared.bytes.checked_add(controls) else {
        shared.retire(accounting)?;
        return Err(WorkError::Overflow.into());
    };
    let mut control = match accounting.claim_owned(controls, operations) {
        Ok(control) => control,
        Err(error) => {
            shared.retire(accounting)?;
            return Err(error);
        }
    };
    if let Err(error) = shared.allocation.absorb(&mut control) {
        accounting.release_owned(&mut control, controls)?;
        shared.retire(accounting)?;
        return Err(error);
    }
    shared.bytes = bytes;
    Ok(shared)
}
