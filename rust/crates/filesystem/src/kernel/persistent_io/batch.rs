use super::*;

enum BatchSource {
    Cached(DecodedCacheValue),
    Cold(ObjectId),
}

struct BatchPlan {
    sources: Vec<BatchSource>,
    requests: Vec<ObjectReadRequest>,
    source_bytes: u64,
    request_bytes: u64,
    source_allocation: KernelOwnedAllocation,
    request_allocation: KernelOwnedAllocation,
}

pub(crate) struct PageBatch<'a, S: AsyncObjectStore, F: Format> {
    store: &'a S,
    sources: std::vec::IntoIter<BatchSource>,
    reads: std::vec::IntoIter<ObjectRead>,
    source_bytes: u64,
    read_container_bytes: u64,
    remaining_owned_bytes: u64,
    data_custody: Option<&'a crate::kernel::ProofDataCustody>,
    format: PhantomData<F>,
    source_allocation: KernelOwnedAllocation,
    read_allocation: KernelOwnedAllocation,
}

#[allow(
    clippy::expect_used,
    reason = "The read boundary already validates and registers the retained container and payload sizes."
)]
pub(crate) async fn read_pages_with_accounting<'a, S, F, I>(
    store: &'a S,
    pages: I,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&'a crate::kernel::ProofDataCustody>,
) -> Result<PageBatch<'a, S, F>, FsError>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    cancellation.check()?;
    let mut plan = plan_page_batch::<S, F, I>(store, pages, limits, accounting)?;
    let cold = async {
        accounting.charge(WorkCounters {
            page_reads: usize_to_u64(plan.sources.len()),
            ..WorkCounters::UNCHARGED
        })?;
        if plan.requests.is_empty() {
            return Ok(ReadOwned {
                value: Vec::new(),
                custody: KernelOwnedAllocation::Standalone,
            });
        }
        crate::heap_future::metered!(accounting, |accounting| {
            crate::kernel::file_read::read_objects_with_accounting(
                store,
                &plan.requests,
                accounting,
                cancellation,
            )
        })
    }
    .await;
    let cold = match cold {
        Ok(cold) => cold,
        Err(error) => {
            release_plan(plan, accounting)?;
            return Err(error);
        }
    };
    let (read_container_bytes, remaining_owned_bytes) = match &cold.custody {
        KernelOwnedAllocation::Standalone => {
            // The shared read boundary checked and registered both quantities.
            let read_container_bytes = usize_to_u64(
                cold.value
                    .capacity()
                    .checked_mul(size_of::<ObjectRead>())
                    .expect("registered read container"),
            );
            let total = crate::kernel::file_read::retained_object_reads_bytes(
                &cold.value,
                cold.value.capacity(),
            )
            .expect("registered read retention");
            let remaining_owned_bytes = total
                .checked_sub(read_container_bytes)
                .expect("read retention includes container");
            (read_container_bytes, remaining_owned_bytes)
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(owned) => (owned.bytes(), 0),
    };
    if cold.value.len() != plan.requests.len() {
        drop(cold.value);
        release_read_allocation(
            read_container_bytes
                .checked_add(remaining_owned_bytes)
                .expect("registered read retention"),
            cold.custody,
            accounting,
        )?;
        release_plan(plan, accounting)?;
        return Err(invalid_batch_result());
    }
    drop(std::mem::take(&mut plan.requests));
    release_read_allocation(plan.request_bytes, plan.request_allocation, accounting)?;
    Ok(PageBatch {
        store,
        sources: plan.sources.into_iter(),
        reads: cold.value.into_iter(),
        source_bytes: plan.source_bytes,
        read_container_bytes,
        remaining_owned_bytes,
        data_custody,
        format: PhantomData,
        source_allocation: plan.source_allocation,
        read_allocation: cold.custody,
    })
}

fn plan_page_batch<S, F, I>(
    store: &S,
    pages: I,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<BatchPlan, FsError>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    let count = pages.len();
    if count == 0 {
        return Err(invalid_batch_result());
    }
    accounting.check()?;
    let (sources, source_bytes, source_allocation) =
        batch_container::<BatchSource>(count, accounting)?;
    let (requests, request_bytes, request_allocation) =
        match batch_container::<ObjectReadRequest>(count, accounting) {
            Ok(container) => container,
            Err(error) => {
                drop(sources);
                release_read_allocation(source_bytes, source_allocation, accounting)?;
                return Err(error);
            }
        };
    let mut plan = BatchPlan {
        sources,
        requests,
        source_bytes,
        request_bytes,
        source_allocation,
        request_allocation,
    };
    let result = (|| {
        accounting.admit(WorkCounters {
            page_reads: usize_to_u64(count),
            ..WorkCounters::UNCHARGED
        })?;
        for object_id in pages {
            let key = DecodedCacheKey::new::<SharedPage<F>>(object_id, limits);
            accounting.check()?;
            if let Some(value) = store.decoded_cache_get(key).map_err(read_store_error)? {
                plan.sources.push(BatchSource::Cached(value));
            } else {
                plan.sources.push(BatchSource::Cold(object_id));
                plan.requests.push(ObjectReadRequest {
                    object_id,
                    maximum_bytes: limits.maximum_page_object_bytes(),
                });
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(plan),
        Err(error) => {
            release_plan(plan, accounting)?;
            Err(error)
        }
    }
}

fn release_plan(plan: BatchPlan, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    drop(plan.sources);
    drop(plan.requests);
    release_read_allocation(plan.source_bytes, plan.source_allocation, accounting)?;
    release_read_allocation(plan.request_bytes, plan.request_allocation, accounting)
}

fn batch_container<T>(
    count: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<T>, u64, KernelOwnedAllocation), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let reconcile_capacity = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let reconcile_capacity = false;
    let mut bytes = count
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .ok_or(WorkError::Overflow)?;
    let mut allocation = accounting.claim_owned(bytes, u64::from(count != 0))?;
    // Physical backing drops before its custody.
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        drop(values);
        release_read_allocation(bytes, allocation, accounting)?;
        return Err(CanonicalDecodeError::AllocationFailed.into());
    }
    if reconcile_capacity {
        let actual = values
            .capacity()
            .checked_mul(size_of::<T>())
            .map(usize_to_u64)
            .ok_or(WorkError::Overflow)?;
        if let Err(error) = accounting.reconcile_owned(&mut allocation, bytes, actual) {
            drop(values);
            release_read_allocation(actual, allocation, accounting)?;
            return Err(error);
        }
        bytes = actual;
    }
    Ok((values, bytes, allocation))
}

impl<S: AsyncObjectStore, F: Format> PageBatch<'_, S, F> {
    pub(crate) fn next_with_accounting(
        &mut self,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(OwnedPage<F>, KernelOwnedAllocation), FsError> {
        let decoded = (|| {
            cancellation.check()?;
            accounting.check()?;
            let source = self.sources.next().ok_or_else(invalid_batch_result)?;
            let object_id = match source {
                BatchSource::Cached(value) => {
                    let page = OwnedPage {
                        page: PageLease::Shared {
                            page: value
                                .value
                                .downcast::<SharedPage<F>>()
                                .map_err(|_| read_store_error(ObjectStoreError::Corrupt))?,
                            #[cfg(not(target_arch = "wasm32"))]
                            _native_custody: value.native_custody,
                        },
                        logical_bytes: 0,
                    };
                    let allocation = accounting.claim_owned(0, 0)?;
                    return Ok((page, allocation));
                }
                BatchSource::Cold(object_id) => object_id,
            };
            let read = self.reads.next().ok_or_else(invalid_batch_result)?;
            let retained = match self.read_allocation {
                KernelOwnedAllocation::Standalone => retained_bytes(&read),
                #[cfg(not(target_arch = "wasm32"))]
                KernelOwnedAllocation::NativeOriginal(_) => 0,
            };
            self.remaining_owned_bytes = self
                .remaining_owned_bytes
                .checked_sub(retained)
                .ok_or_else(invalid_batch_result)?;
            let decoded = decode_read_with_accounting::<F>(&read, limits, accounting, None);
            drop(read);
            if retained != 0 {
                release_read_allocation(retained, KernelOwnedAllocation::Standalone, accounting)?;
            }
            admit_decoded_with_accounting(
                self.store,
                DecodedCacheKey::new::<SharedPage<F>>(object_id, limits),
                decoded?,
                accounting,
                self.data_custody,
            )
        })();
        if decoded.is_err() {
            self.release_pending_with_accounting(accounting)?;
        }
        decoded
    }

    pub(crate) fn finish_with_accounting(
        mut self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let valid = self.reads.len() == 0 && self.sources.len() == 0;
        self.release_pending_with_accounting(accounting)?;
        cancellation.check()?;
        accounting.check()?;
        if valid {
            Ok(())
        } else {
            Err(invalid_batch_result())
        }
    }

    pub(crate) fn discard_with_accounting(
        mut self,
        accounting: &mut KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.release_pending_with_accounting(accounting)
    }

    fn release_pending_with_accounting(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let allocations = match accounting {
            KernelAccounting::Standalone { allocations, .. } => Some(allocations),
            KernelAccounting::BorrowedStandalone { allocations, .. } => Some(&mut **allocations),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => None,
        };
        self.reads = Vec::new().into_iter();
        self.sources = Vec::new().into_iter();
        let (source_bytes, read_bytes) = {
            let source_allocation = std::mem::replace(
                &mut self.source_allocation,
                KernelOwnedAllocation::Standalone,
            );
            let read_allocation =
                std::mem::replace(&mut self.read_allocation, KernelOwnedAllocation::Standalone);
            let source_bytes = if matches!(source_allocation, KernelOwnedAllocation::Standalone) {
                self.source_bytes
            } else {
                0
            };
            let read_bytes = if matches!(read_allocation, KernelOwnedAllocation::Standalone) {
                self.read_container_bytes
                    .checked_add(self.remaining_owned_bytes)
                    .ok_or(CanonicalDecodeError::AllocationFailed)?
            } else {
                0
            };
            (source_bytes, read_bytes)
        };
        let bytes = source_bytes
            .checked_add(read_bytes)
            .ok_or(CanonicalDecodeError::AllocationFailed)?;
        if bytes != 0 {
            allocations
                .ok_or(AllocationError::ReleaseInvariant)
                .map_err(allocation_error)?
                .release(bytes)
                .map_err(allocation_error)?;
        }
        self.source_bytes = 0;
        self.read_container_bytes = 0;
        self.remaining_owned_bytes = 0;
        Ok(())
    }
}
