use super::*;

pub(crate) async fn read_page_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    if cancellation.is_cancelled() {
        return Err(crate::FsError::Cancelled(crate::CancellationError));
    }
    accounting.charge(PAGE_READ)?;
    let cache_key = DecodedCacheKey::new::<Page<F>>(page, limits);
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work();
    #[cfg(not(target_arch = "wasm32"))]
    let standalone = original.is_none();
    #[cfg(target_arch = "wasm32")]
    let standalone = true;
    if standalone {
        if let Some(owner) = data_custody {
            owner.record_completed_leaf_work(PAGE_READ)?;
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = original {
        work.remaining()?;
    }
    let cached = store
        .decoded_cache_get(cache_key)
        .map_err(crate::kernel::file_read::read_store_error)?;
    if let Some(cached) = cached {
        let logical_bytes = cached.logical_bytes;
        let page = cached
            .value
            .downcast::<Page<F>>()
            .map_err(|_| crate::kernel::file_read::read_store_error(ObjectStoreError::Corrupt))?;
        let source = OwnedPage {
            page: PageLease::Shared {
                page,
                logical_bytes,
            },
            logical_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: cached.native_custody,
        };
        let allocation = accounting.claim_owned(0, 0)?;
        return Ok((source, allocation));
    }
    let (read, retained) =
        read_cold_object(store, page, limits, accounting, cancellation, data_custody).await?;
    let decoded = if standalone {
        let (allocations, work, budget, _) = accounting
            .standalone_parts()
            .expect("the standalone decode branch has a standalone ledger");
        decode_read_with_custody::<F>(&read, limits, budget, allocations, work, data_custody, 0)
            .map(|page| (page, KernelOwnedAllocation::Standalone))
            .map_err(accounted_leaf_error)
    } else {
        decode_read_with_accounting::<F>(&read, limits, accounting)
    };
    drop(read);
    if standalone {
        let mut allocation = KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, retained)?;
    }
    let decoded = decoded?;
    admit_decoded_with_accounting(store, cache_key, decoded, accounting, data_custody)
}

async fn read_cold_object<S: AsyncObjectStore>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(ObjectRead, u64), crate::FsError> {
    let maximum = limits.maximum_page_object_bytes();
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let mut frame = super::batch::batch_in_heap(accounting, |_| async {
            let mut work = crate::AuthorityWork::native(original, cancellation);
            store.read(page, maximum, &mut work).await
        })?;
        let read = frame.value.as_mut().await;
        drop(frame);
        return read
            .map(|read| (read, 0))
            .map_err(crate::kernel::file_read::read_store_error);
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .expect("native object reads use the original provider callback");
    read_cold_standalone(
        store,
        page,
        maximum,
        budget,
        cancellation,
        allocations,
        work,
        data_custody,
    )
    .await
    .map_err(accounted_leaf_error)
}

async fn read_cold_standalone<S: AsyncObjectStore>(
    store: &S,
    page: ObjectId,
    maximum_bytes: u64,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(ObjectRead, u64), Error> {
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
                    failure.map_with_prior_work(WorkCounters::UNCHARGED, Error::Storage)
                })?;
            let mut context = crate::AuthorityWork::new(budget, cancellation);
            context.read_observation =
                data_custody.map(|owner| (owner, owner.operation_cache_bytes()));
            let result = store.read(page, maximum_bytes, &mut context).await;
            context.read_receipt(result).map_err(|failure| {
                failure.map_with_prior_work(WorkCounters::UNCHARGED, Error::Storage)
            })
        },
        remaining,
        cancellation,
        Error::Storage(ObjectStoreError::Cancelled),
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
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            peak_allocation_bytes: allocations.live_bytes(),
            ..WorkCounters::UNCHARGED
        }) {
            drop(receipt.value);
            allocations.release(retained)?;
            return Err(error.into());
        }
    }
    Ok((receipt.value, retained))
}

pub(crate) fn admit_decoded_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    key: DecodedCacheKey,
    decoded: (OwnedPage<F>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(OwnedPage<F>, KernelOwnedAllocation), crate::FsError> {
    if matches!(&decoded.0.page, PageLease::Shared { .. }) {
        return Ok(decoded);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        let mut source = crate::kernel::file_read::ReadOwned {
            value: decoded.0,
            custody: decoded.1,
        };
        let empty = accounting.claim_owned(0, 0)?;
        match (&source.custody, &empty) {
            (
                KernelOwnedAllocation::NativeOriginal(body),
                KernelOwnedAllocation::NativeOriginal(current),
            ) if body.shares_ledger(current) => {}
            _ => return Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
        }
        let controls = usize_to_u64(size_of::<Page<F>>())
            .checked_add(usize_to_u64(size_of::<crate::NativeAllocation>()))
            .ok_or(WorkError::Overflow)?;
        let mut control = accounting.claim_owned(controls, 2)?;
        source.custody.absorb(&mut control)?;
        let PageLease::Owned(page) = source.value.page else {
            unreachable!("shared leases return before admission");
        };
        let KernelOwnedAllocation::NativeOriginal(custody) = source.custody else {
            unreachable!("the original page custody was checked before construction");
        };
        let value = DecodedCacheValue {
            value: Arc::new(page),
            logical_bytes: source.value.logical_bytes,
            native_custody: Some(Arc::new(custody)),
        };
        let original = accounting
            .native_work()
            .expect("original page admission keeps its accounting context");
        let value = match {
            let __decoded_args = (key, value, original);
            __decoded_args
                .2
                .remaining()
                .map_err(crate::ObjectStoreError::from)
                .and_then(|_| store.decoded_cache_admit(__decoded_args.0, __decoded_args.1))
        }
        .map_err(crate::kernel::file_read::read_store_error)?
        {
            DecodedCacheAdmission::Shared(value) | DecodedCacheAdmission::Uncached(value) => value,
        };
        let page = value
            .value
            .downcast::<Page<F>>()
            .map_err(|_| crate::kernel::file_read::read_store_error(ObjectStoreError::Corrupt))?;
        return Ok((
            OwnedPage {
                page: PageLease::Shared {
                    page,
                    logical_bytes: value.logical_bytes,
                },
                logical_bytes: 0,
                native_custody: value.native_custody,
            },
            empty,
        ));
    }
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .expect("original page admission uses the native cache hook");
    admit_decoded(
        store,
        key,
        Ok(decoded.0),
        allocations,
        budget,
        work,
        data_custody,
        0,
    )
    .map(|page| (page, KernelOwnedAllocation::Standalone))
    .map_err(accounted_leaf_error)
}

pub(super) fn admit_decoded<S, F>(
    store: &S,
    key: DecodedCacheKey,
    decoded: Result<OwnedPage<F>, Error>,
    allocations: &mut AllocationLedger,
    budget: WorkBudget,
    work: &mut WorkCounters,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<OwnedPage<F>, Error>
where
    S: AsyncObjectStore,
    F: Format,
{
    let decoded = decoded?;
    let PageLease::Owned(page) = decoded.page else {
        return Err(Error::Storage(ObjectStoreError::Corrupt));
    };
    let logical_bytes = decoded.logical_bytes;
    let control_bytes = usize_to_u64(size_of::<Page<F>>());
    if let Err(error) = claim_read_allocation(
        control_bytes,
        1,
        allocations,
        work,
        budget,
        data_custody,
        cached_local_bytes,
    ) {
        drop(page);
        allocations.release(logical_bytes)?;
        return Err(error);
    }
    if let Some(owner) = data_custody {
        let local_peak = allocations
            .live_bytes()
            .checked_sub(cached_local_bytes)
            .ok_or(WorkError::Overflow)?;
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: local_peak,
            ..WorkCounters::default()
        }) {
            drop(page);
            allocations.release(control_bytes)?;
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    }
    let value = DecodedCacheValue {
        value: Arc::new(page),
        logical_bytes,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: decoded.native_custody,
    };
    match store.decoded_cache_admit(key, value) {
        Ok(DecodedCacheAdmission::Shared(value)) => {
            allocations.release(control_bytes)?;
            allocations.release(logical_bytes)?;
            Ok(OwnedPage {
                page: PageLease::Shared {
                    page: value
                        .value
                        .downcast::<Page<F>>()
                        .map_err(|_| ObjectStoreError::Corrupt)?,
                    logical_bytes: value.logical_bytes,
                },
                logical_bytes: 0,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: value.native_custody,
            })
        }
        Ok(DecodedCacheAdmission::Uncached(value)) => {
            let page = match value.value.downcast::<Page<F>>() {
                Ok(page) => page,
                Err(value) => {
                    drop(value);
                    allocations.release(control_bytes)?;
                    allocations.release(logical_bytes)?;
                    return Err(ObjectStoreError::Corrupt.into());
                }
            };
            #[cfg(not(target_arch = "wasm32"))]
            if value.native_custody.is_some() {
                allocations.release(control_bytes)?;
                allocations.release(logical_bytes)?;
                return Ok(OwnedPage {
                    page: PageLease::Shared {
                        page,
                        logical_bytes: value.logical_bytes,
                    },
                    logical_bytes: 0,
                    native_custody: value.native_custody,
                });
            }
            let page = match Arc::try_unwrap(page) {
                Ok(page) => page,
                Err(page) => {
                    drop(page);
                    allocations.release(control_bytes)?;
                    allocations.release(logical_bytes)?;
                    return Err(ObjectStoreError::Corrupt.into());
                }
            };
            allocations.release(control_bytes)?;
            Ok(OwnedPage {
                page: PageLease::Owned(page),
                logical_bytes,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: value.native_custody,
            })
        }
        Err(error) => {
            allocations.release(control_bytes)?;
            allocations.release(logical_bytes)?;
            Err(error.into())
        }
    }
}
