//! One authenticated, bounded page-read boundary for persistent tree kernels.

use super::allocation::{AllocationError, AllocationLedger};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::persistent_btree::{Child, Format, Page};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::heap_future::in_heap;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

pub(crate) struct OwnedPage<F: Format> {
    pub(crate) page: PageLease<F>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<Arc<crate::NativeAllocation>>,
    pub(crate) logical_bytes: u64,
}

pub(crate) enum PageLease<F: Format> {
    Owned(Page<F>),
    Shared {
        page: Arc<Page<F>>,
        logical_bytes: u64,
    },
}

impl<F: Format> OwnedPage<F> {
    pub(crate) fn into_owned_with_accounting(
        self,
        source: super::allocation::KernelOwnedAllocation,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<((Page<F>, u64), super::allocation::KernelOwnedAllocation), crate::FsError> {
        let input = super::file_read::ReadOwned { value: self, custody: source };
        match input.value.page {
            PageLease::Owned(page) => Ok(((page, input.value.logical_bytes), input.custody)),
            PageLease::Shared { page, logical_bytes } => {
                #[cfg(not(target_arch = "wasm32"))]
                if accounting.native_work().is_some() {
                    let cloned = super::file_read::decode_owned(accounting, |steps| {
                        clone_page::<F, crate::FsError>(
                            &page, &mut |step| steps(step),
                        )
                    })?;
                    let bytes = match &cloned.1 {
                        super::allocation::KernelOwnedAllocation::NativeOriginal(owned) => owned.bytes(),
                        super::allocation::KernelOwnedAllocation::Standalone => 0,
                    };
                    return Ok(((cloned.0, bytes), cloned.1));
                }
                let (allocations, work, budget, _) = accounting.standalone_parts()
                    .expect("the native page clone uses the original step engine");
                allocations.claim_bytes(logical_bytes, u64::from(logical_bytes != 0), work, budget)
                    .map_err(|error| accounted_leaf_error(Error::Allocation(error)))?;
                charge_copy(logical_bytes, &budget, allocations, work).map_err(accounted_leaf_error)?;
                Ok((((*page).clone(), logical_bytes), super::allocation::KernelOwnedAllocation::Standalone))
            }
        }
    }
}

fn clone_page<F: Format, E: From<CanonicalDecodeError>>(
    page: &Page<F>,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<Page<F>, E> {
    use super::codec::ReadAllocationStep;
    match page {
        Page::Leaf(values) => {
            let mut cloned = Vec::new();
            super::file_read::reserve_decoded_items(&mut cloned, values.len(), observe)?;
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
            super::file_read::reserve_decoded_items(&mut cloned, children.len(), observe)?;
            for child in children {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = F::try_clone_key(&child.first, u32::MAX, observe)?;
                observe(ReadAllocationStep::Item)?;
                let copied = usize_to_u64(size_of::<Child<F::Key>>());
                observe(ReadAllocationStep::BeforeCopy(copied))?;
                cloned.push(Child { first, page: child.page });
                observe(ReadAllocationStep::Copied(copied))?;
            }
            Ok(Page::Internal(cloned))
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("persistent page allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Allocation(#[from] AllocationError),
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
}

pub(crate) async fn read_page_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S, page: ObjectId, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(OwnedPage<F>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    if cancellation.is_cancelled() { return Err(crate::FsError::Cancelled(crate::CancellationError)); }
    accounting.charge(PAGE_READ)?;
    let cache_key = DecodedCacheKey::new::<Page<F>>(page, limits);
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work();
    #[cfg(not(target_arch = "wasm32"))]
    let standalone = original.is_none();
    #[cfg(target_arch = "wasm32")]
    let standalone = true;
    if standalone {
        if let Some(owner) = data_custody { owner.record_completed_leaf_work(PAGE_READ)?; }
    }
    #[cfg(not(target_arch = "wasm32"))]
    let cached = match original {
        Some(work) => store.decoded_cache_get_with_native_work(cache_key, work),
        None => store.decoded_cache_get(cache_key),
    }.map_err(page_store_error)?;
    #[cfg(target_arch = "wasm32")]
    let cached = store.decoded_cache_get(cache_key).map_err(page_store_error)?;
    if let Some(cached) = cached {
        let logical_bytes = cached.logical_bytes;
        let page = cached.value.downcast::<Page<F>>().map_err(|_| page_store_error(ObjectStoreError::Corrupt))?;
        let source = OwnedPage {
            page: PageLease::Shared { page, logical_bytes }, logical_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: cached.native_custody,
        };
        let allocation = accounting.claim_owned(0, 0)?;
        return Ok((source, allocation));
    }
    let (read, retained) = read_cold_object(store, page, limits, accounting, cancellation, data_custody).await?;
    let decoded = if standalone {
        let (allocations, work, budget, _) = accounting.standalone_parts()
            .expect("the standalone decode branch has a standalone ledger");
        decode_read_with_custody::<F>(&read, limits, budget, allocations, work, data_custody, 0)
            .map(|page| (page, super::allocation::KernelOwnedAllocation::Standalone))
            .map_err(accounted_leaf_error)
    } else {
        decode_read_with_accounting::<F>(&read, limits, accounting)
    };
    drop(read);
    if standalone {
        let mut allocation = super::allocation::KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, retained)?;
    }
    let decoded = decoded?;
    admit_decoded_with_accounting(store, cache_key, decoded, accounting, data_custody)
}



fn page_store_error(error: ObjectStoreError) -> crate::FsError {
    #[cfg(not(target_arch = "wasm32"))]
    if let ObjectStoreError::Native(error) = error { return error.into(); }
    error.into()
}

async fn read_cold_object<S: AsyncObjectStore>(
    store: &S, page: ObjectId, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(ObjectRead, u64), crate::FsError> {
    let maximum = limits.maximum_page_object_bytes();
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let mut frame = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            &mut frame,
            |frame, bytes| {
                **frame = Some(original.claim(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED)?);
                Ok::<_, crate::NativeTreeMaterializationError>(())
            },
            |_| store.read_with_native_work(page, maximum, original, cancellation),
        )?;
        let read = future.as_mut().await;
        drop(future);
        drop(frame);
        return read.map(|read| (read, 0)).map_err(page_store_error);
    }
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .expect("native object reads use the original provider callback");
    read_cold_standalone(store, page, maximum, budget, cancellation, allocations, work, data_custody)
        .await.map_err(accounted_leaf_error)
}

async fn read_cold_standalone<S: AsyncObjectStore>(
    store: &S, page: ObjectId, maximum_bytes: u64, budget: WorkBudget,
    cancellation: &CancellationToken, allocations: &mut AllocationLedger, work: &mut WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(ObjectRead, u64), Error> {
    let prospective = *work;
    let mut remaining = prospective.remaining(budget)?;
    remaining.peak_allocation_bytes = budget.peak_allocation_bytes
        .checked_sub(allocations.live_bytes()).ok_or(WorkError::Overflow)?;
    let live = allocations.live_bytes();
    let frame_observer = |event| -> Result<u64, WorkError> {
        let Some(owner) = data_custody else { return Ok(0); };
        let standing = owner.record_heap_future_event(event)?;
        if matches!(event, crate::heap_future::HeapFutureEvent::Admitted(_)) {
            owner.record_completed_leaf_work(WorkCounters { peak_allocation_bytes: live, ..WorkCounters::UNCHARGED })?;
        }
        Ok(standing)
    };
    let read = crate::heap_future::in_heap_measured(
        move |budget| async move {
            AsyncObjectStore::read_with_custody(
                store, page, maximum_bytes, budget, cancellation,
                data_custody,
            ).await
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, Error::Storage))
        },
        remaining, cancellation, Error::Storage(ObjectStoreError::Cancelled),
        data_custody.map(|_| &frame_observer as &(dyn Fn(_) -> Result<u64, WorkError> + Sync)),
    ).await;
    let nested = match &read { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    *work = prospective.with_backend(nested, live)?;
    if let Some(owner) = data_custody {
        owner.record_data_work(WorkCounters { peak_allocation_bytes: work.peak_allocation_bytes, ..WorkCounters::UNCHARGED })?;
    }
    let receipt = read.map_err(|failure| failure.error)?;
    work.verify(budget)?;
    let retained = retained_bytes(&receipt.value);
    claim_read_allocation(retained, 0, allocations, work, budget, data_custody, 0)?;
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            peak_allocation_bytes: allocations.live_bytes(), ..WorkCounters::UNCHARGED
        }) {
            drop(receipt.value);
            allocations.release(retained)?;
            return Err(error.into());
        }
    }
    Ok((receipt.value, retained))
}

pub(crate) fn admit_decoded_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S, key: DecodedCacheKey,
    decoded: (OwnedPage<F>, super::allocation::KernelOwnedAllocation),
    accounting: &mut super::allocation::KernelAccounting<'_>,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(OwnedPage<F>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    if matches!(&decoded.0.page, PageLease::Shared { .. }) { return Ok(decoded); }
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        let mut source = super::file_read::ReadOwned { value: decoded.0, custody: decoded.1 };
        let empty = accounting.claim_owned(0, 0)?;
        match (&source.custody, &empty) {
            (super::allocation::KernelOwnedAllocation::NativeOriginal(body),
             super::allocation::KernelOwnedAllocation::NativeOriginal(current)) if body.shares_ledger(current) => {}
            _ => return Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
        }
        let controls = usize_to_u64(size_of::<Page<F>>())
            .checked_add(usize_to_u64(size_of::<crate::NativeAllocation>()))
            .ok_or(WorkError::Overflow)?;
        let mut control = accounting.claim_owned(controls, 2)?;
        source.custody.absorb(&mut control)?;
        let PageLease::Owned(page) = source.value.page else { unreachable!("shared leases return before admission"); };
        let super::allocation::KernelOwnedAllocation::NativeOriginal(custody) = source.custody
            else { unreachable!("the original page custody was checked before construction"); };
        let value = DecodedCacheValue {
            value: Arc::new(page), logical_bytes: source.value.logical_bytes,
            native_custody: Some(Arc::new(custody)),
        };
        let original = accounting.native_work().expect("original page admission keeps its accounting context");
        let value = match store.decoded_cache_admit_with_native_work(key, value, original).map_err(page_store_error)? {
            DecodedCacheAdmission::Shared(value) | DecodedCacheAdmission::Uncached(value) => value,
        };
        let page = value.value.downcast::<Page<F>>().map_err(|_| page_store_error(ObjectStoreError::Corrupt))?;
        return Ok((OwnedPage {
            page: PageLease::Shared { page, logical_bytes: value.logical_bytes },
            logical_bytes: 0, native_custody: value.native_custody,
        }, empty));
    }
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .expect("original page admission uses the native cache hook");
    admit_decoded(store, key, Ok(decoded.0), allocations, budget, work, data_custody, 0)
        .map(|page| (page, super::allocation::KernelOwnedAllocation::Standalone))
        .map_err(accounted_leaf_error)
}

pub(super) fn admit_decoded<S, F>(
    store: &S,
    key: DecodedCacheKey,
    decoded: Result<OwnedPage<F>, Error>,
    allocations: &mut AllocationLedger,
    budget: WorkBudget,
    work: &mut WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
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
        control_bytes, 1, allocations, work, budget, data_custody, cached_local_bytes,
    ) {
        drop(page);
        allocations.release(logical_bytes)?;
        return Err(error);
    }
    if let Some(owner) = data_custody {
        let local_peak = allocations.live_bytes().checked_sub(cached_local_bytes)
            .ok_or(WorkError::Overflow)?;
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            allocation_operations: 1, peak_allocation_bytes: local_peak, ..WorkCounters::default()
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
                    page: PageLease::Shared { page, logical_bytes: value.logical_bytes },
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
                page: PageLease::Owned(page), logical_bytes,
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

enum BatchSource {
    Cached(DecodedCacheValue),
    Cold(ObjectId),
}

struct BatchPlan {
    sources: Vec<BatchSource>,
    cold_requests: Vec<ObjectReadRequest>,
    source_bytes: u64,
    cold_request_bytes: u64,
    prospective: WorkCounters,
    remaining: WorkBudget,
    // Payload fields precede their original-ledger guards on every drop path.
    source_allocation: super::allocation::KernelOwnedAllocation,
    request_allocation: super::allocation::KernelOwnedAllocation,
}

struct ColdReadContext<'a, 'accounting> {
    prospective: WorkCounters,
    remaining: WorkBudget,
    cancellation: &'a CancellationToken,
    accounting: &'a mut super::allocation::KernelAccounting<'accounting>,
}

struct ColdReads {
    reads: Vec<ObjectRead>,
    container_bytes: u64,
    owned_bytes: u64,
    allocation: super::allocation::KernelOwnedAllocation,
}

struct BatchFuture<F: Future> {
    future: std::pin::Pin<Box<F>>,
    _allocation: super::allocation::KernelOwnedAllocation,
}

pub(crate) struct PageBatch<'a, S: AsyncObjectStore, F: Format> {
    store: &'a S,
    sources: std::vec::IntoIter<BatchSource>,
    reads: std::vec::IntoIter<ObjectRead>,
    source_bytes: u64,
    read_container_bytes: u64,
    remaining_owned_bytes: u64,
    released: bool,
    data_custody: Option<&'a super::ProofDataCustody>,
    format: PhantomData<F>,
    source_allocation: super::allocation::KernelOwnedAllocation,
    read_allocation: super::allocation::KernelOwnedAllocation,
}

pub(crate) async fn read_pages_with_accounting<'a, S, F, I>(
    store: &'a S,
    pages: I,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&'a super::ProofDataCustody>,
) -> Result<PageBatch<'a, S, F>, crate::FsError>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    cancellation.check()?;
    let mut plan = plan_page_batch::<S, F, I>(store, pages, limits, accounting)?;
    #[cfg(not(target_arch = "wasm32"))]
    let native = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let native = false;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        if let Err(error) = original.charge(WorkCounters {
            page_reads: usize_to_u64(plan.sources.len()),
            ..WorkCounters::UNCHARGED
        }) {
            release_batch_plan(plan, accounting)?;
            return Err(error.into());
        }
    }
    // Keep the existing single read_many frontier and cold-only heap frame.
    let cold = if plan.cold_requests.is_empty() {
        if let Some((_, work, _, _)) = accounting.standalone_parts() {
            *work = plan.prospective;
        }
        Ok(ColdReads {
            reads: Vec::new(),
            container_bytes: 0,
            owned_bytes: 0,
            allocation: super::allocation::KernelOwnedAllocation::Standalone,
        })
    } else {
        let context = ColdReadContext {
            prospective: plan.prospective,
            remaining: plan.remaining,
            cancellation,
            accounting,
        };
        match batch_in_heap(context, |context| {
            read_cold_pages(store, &plan.cold_requests, context)
        }) {
            Ok(mut frame) => {
                let result = frame.future.as_mut().await;
                drop(frame);
                result
            }
            Err(error) => Err(error),
        }
    };
    let mut cold = match cold {
        Ok(cold) => cold,
        Err(error) => {
            release_batch_plan(plan, accounting)?;
            return Err(error);
        }
    };
    drop(std::mem::take(&mut plan.cold_requests));
    let request_allocation = std::mem::replace(
        &mut plan.request_allocation,
        super::allocation::KernelOwnedAllocation::Standalone,
    );
    if let Err(error) = release_batch_allocation(
        request_allocation, plan.cold_request_bytes, accounting,
    ) {
        drop(cold);
        plan.cold_request_bytes = 0;
        release_batch_plan(plan, accounting)?;
        return Err(error);
    }
    plan.cold_request_bytes = 0;
    if !native && matches!(cold.allocation, super::allocation::KernelOwnedAllocation::Standalone) {
        let retained = accounting.with_allocations(|accounting| {
            let (allocations, work, budget, _) = accounting.standalone_parts()
                .ok_or_else(|| accounted_leaf_error(Error::Allocation(AllocationError::ReleaseInvariant)))?;
            let (container_bytes, owned_bytes, retained) =
                batch_retained_bytes(&cold.reads, 0).map_err(accounted_leaf_error)?;
            allocations.claim_bytes(retained, 0, work, budget)
                .map_err(|error| accounted_leaf_error(error.into()))?;
            Ok((container_bytes, owned_bytes))
        });
        match retained {
            Ok((container_bytes, owned_bytes)) => {
                cold.container_bytes = container_bytes;
                cold.owned_bytes = owned_bytes;
            }
            Err(error) => {
                drop(cold);
                release_batch_plan(plan, accounting)?;
                return Err(error);
            }
        }
    }
    Ok(PageBatch {
        store,
        sources: plan.sources.into_iter(),
        reads: cold.reads.into_iter(),
        source_bytes: plan.source_bytes,
        read_container_bytes: cold.container_bytes,
        remaining_owned_bytes: cold.owned_bytes,
        released: false,
        data_custody,
        format: PhantomData,
        source_allocation: plan.source_allocation,
        read_allocation: cold.allocation,
    })
}

fn plan_page_batch<S, F, I>(
    store: &S,
    pages: I,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<BatchPlan, crate::FsError>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    let count = pages.len();
    if count == 0 {
        return Err(accounted_leaf_error(invalid_batch_result()));
    }
    accounting.check()?;
    let (sources, source_bytes, source_allocation) = batch_container::<BatchSource>(count, accounting)?;
    let (cold_requests, cold_request_bytes, request_allocation) =
        match batch_container::<ObjectReadRequest>(count, accounting) {
            Ok(container) => container,
            Err(error) => {
                drop(sources);
                release_batch_allocation(source_allocation, source_bytes, accounting)?;
                return Err(error);
            }
        };
    let mut plan = BatchPlan {
        sources, cold_requests, source_bytes, cold_request_bytes,
        prospective: WorkCounters::UNCHARGED,
        remaining: WorkBudget::default(),
        source_allocation, request_allocation,
    };
    let planned = (|| {
        let delta = WorkCounters {
            page_reads: usize_to_u64(count),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        let (prospective, remaining) = if let Some((allocations, work, budget, _)) =
            accounting.standalone_parts()
        {
            let prospective = work.checked_add(delta)?;
            prospective.verify(budget)?;
            let mut remaining = prospective.remaining(budget)?;
            remaining.peak_allocation_bytes = budget.peak_allocation_bytes
                .checked_sub(allocations.live_bytes()).ok_or(WorkError::Overflow)?;
            (prospective, remaining)
        } else {
            (accounting.spent().checked_add(delta)?, accounting.remaining()?)
        };
        plan.prospective = prospective;
        plan.remaining = remaining;
        for object_id in pages {
            let key = DecodedCacheKey::new::<Page<F>>(object_id, limits);
            #[cfg(not(target_arch = "wasm32"))]
            let cached = match accounting.native_work() {
                Some(original) => store.decoded_cache_get_with_native_work(key, original),
                None => store.decoded_cache_get(key),
            };
            #[cfg(target_arch = "wasm32")]
            let cached = store.decoded_cache_get(key);
            if let Some(value) = cached.map_err(batch_storage_error)? {
                plan.sources.push(BatchSource::Cached(value));
            } else {
                plan.sources.push(BatchSource::Cold(object_id));
                plan.cold_requests.push(ObjectReadRequest {
                    object_id,
                    maximum_bytes: limits.maximum_page_object_bytes(),
                });
            }
        }
        Ok(())
    })();
    match planned {
        Ok(()) => Ok(plan),
        Err(error) => {
            release_batch_plan(plan, accounting)?;
            Err(error)
        }
    }
}

fn batch_container<T>(
    count: usize,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(Vec<T>, u64, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let reconcile_capacity = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let reconcile_capacity = false;
    let mut bytes = count.checked_mul(size_of::<T>())
        .map(usize_to_u64).ok_or(WorkError::Overflow)?;
    let mut allocation = accounting.claim_owned(bytes, u64::from(count != 0))?;
    // Reserve, inspect and drop physical backing outside every original-root
    // mutex. A late reconciliation failure keeps its actual guard until drop.
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        drop(values);
        release_batch_allocation(allocation, bytes, accounting)?;
        return Err(accounted_leaf_error(Error::AllocationFailed));
    }
    if reconcile_capacity {
        let actual = values.capacity().checked_mul(size_of::<T>())
            .map(usize_to_u64).ok_or(WorkError::Overflow)?;
        if let Err(error) = accounting.reconcile_owned(&mut allocation, bytes, actual) {
            drop(values);
            release_batch_allocation(allocation, actual, accounting)?;
            return Err(error);
        }
        bytes = actual;
    }
    Ok((values, bytes, allocation))
}

fn batch_in_heap<'a, 'accounting, F: Future>(
    context: ColdReadContext<'a, 'accounting>,
    create: impl FnOnce(ColdReadContext<'a, 'accounting>) -> F,
) -> Result<BatchFuture<F>, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let allocation = if context.accounting.native_work().is_some() {
        let bytes = usize_to_u64(size_of::<F>());
        context.accounting.claim_owned(bytes, u64::from(bytes != 0))?
    } else {
        super::allocation::KernelOwnedAllocation::Standalone
    };
    #[cfg(target_arch = "wasm32")]
    let allocation = super::allocation::KernelOwnedAllocation::Standalone;
    Ok(BatchFuture { future: in_heap(|| create(context)), _allocation: allocation })
}

async fn read_cold_pages<S: AsyncObjectStore>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: ColdReadContext<'_, '_>,
) -> Result<ColdReads, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = context.accounting.native_work() {
        let batch = store.read_many_with_native_work(requests, original, context.cancellation)
            .await.map_err(batch_storage_error)?;
        if batch.0.len() != requests.len() {
            drop(batch);
            return Err(accounted_leaf_error(invalid_batch_result()));
        }
        let (reads, allocation) = batch;
        let container_bytes = allocation.bytes();
        return Ok(ColdReads {
            reads, container_bytes, owned_bytes: 0,
            allocation: super::allocation::KernelOwnedAllocation::NativeOriginal(allocation),
        });
    }
    let result = store.read_many(requests, context.remaining, context.cancellation).await;
    // A failed ordinary backend receipt is retained even if context.cancellation was
    // raised during its await; cleanup never needs another admission.
    let (allocations, work, budget) = match context.accounting {
        super::allocation::KernelAccounting::Standalone { allocations, work, budget, .. } => {
            (allocations, work, *budget)
        }
        super::allocation::KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => {
            (&mut **allocations, &mut **work, *budget)
        }
        #[cfg(not(target_arch = "wasm32"))]
        super::allocation::KernelAccounting::NativeOriginal { .. } => {
            unreachable!("the native cold frontier returned through its original hook")
        }
    };
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(failure) => {
            *work = context.prospective.with_backend(*failure.work, allocations.live_bytes())?;
            return Err(accounted_leaf_error(Error::Storage(failure.error)));
        }
    };
    *work = context.prospective.with_backend(receipt.work, allocations.live_bytes())?;
    work.verify(budget)?;
    if receipt.value.len() != requests.len() {
        return Err(accounted_leaf_error(invalid_batch_result()));
    }
    Ok(ColdReads {
        reads: receipt.value, container_bytes: 0, owned_bytes: 0,
        allocation: super::allocation::KernelOwnedAllocation::Standalone,
    })
}

fn release_batch_plan(
    plan: BatchPlan,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    drop(plan.sources);
    drop(plan.cold_requests);
    release_batch_allocation(plan.source_allocation, plan.source_bytes, accounting)?;
    release_batch_allocation(plan.request_allocation, plan.cold_request_bytes, accounting)
}

fn release_batch_allocation(
    allocation: super::allocation::KernelOwnedAllocation,
    bytes: u64,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    match allocation {
        super::allocation::KernelOwnedAllocation::Standalone => {
            if bytes == 0 { return Ok(()); }
            let allocations = match accounting {
                super::allocation::KernelAccounting::Standalone { allocations, .. } => allocations,
                super::allocation::KernelAccounting::BorrowedStandalone { allocations, .. } => &mut **allocations,
                #[cfg(not(target_arch = "wasm32"))]
                super::allocation::KernelAccounting::NativeOriginal { .. } => {
                    return Err(accounted_leaf_error(Error::Allocation(AllocationError::ReleaseInvariant)));
                }
            };
            allocations.release(bytes).map_err(|error| accounted_leaf_error(error.into()))
        }
        #[cfg(not(target_arch = "wasm32"))]
        super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
            drop(allocation);
            Ok(())
        }
    }
}

fn batch_retained_bytes(reads: &[ObjectRead], source_bytes: u64) -> Result<(u64, u64, u64), Error> {
    let result_container_bytes = reads
        .len()
        .checked_mul(size_of::<ObjectRead>())
        .map(usize_to_u64)
        .ok_or(Error::AllocationFailed)?;
    let container_bytes = source_bytes
        .checked_add(result_container_bytes)
        .ok_or(Error::AllocationFailed)?;
    let remaining_owned_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total
            .checked_add(retained_bytes(read))
            .ok_or(Error::AllocationFailed)
    })?;
    let newly_retained = result_container_bytes
        .checked_add(remaining_owned_bytes)
        .ok_or(Error::AllocationFailed)?;
    Ok((container_bytes, remaining_owned_bytes, newly_retained))
}

impl<S: AsyncObjectStore, F: Format> PageBatch<'_, S, F> {
    pub(crate) fn next_with_accounting(
        &mut self,
        limits: DecodeLimits,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(OwnedPage<F>, super::allocation::KernelOwnedAllocation), crate::FsError> {
        let decoded = (|| {
            cancellation.check()?;
            accounting.check()?;
            let source = self.sources.next()
                .ok_or_else(|| accounted_leaf_error(invalid_batch_result()))?;
            let object_id = match source {
                BatchSource::Cached(value) => {
                    let page = OwnedPage {
                        page: PageLease::Shared {
                            page: value.value.downcast::<Page<F>>()
                                .map_err(|_| ObjectStoreError::Corrupt)?,
                            logical_bytes: value.logical_bytes,
                        },
                        logical_bytes: 0,
                        #[cfg(not(target_arch = "wasm32"))]
                        native_custody: value.native_custody,
                    };
                    let allocation = accounting.claim_owned(0, 0)?;
                    return Ok((page, allocation));
                }
                BatchSource::Cold(object_id) => object_id,
            };
            let read = self.reads.next()
                .ok_or_else(|| accounted_leaf_error(invalid_batch_result()))?;
            let retained = match self.read_allocation {
                super::allocation::KernelOwnedAllocation::Standalone => retained_bytes(&read),
                #[cfg(not(target_arch = "wasm32"))]
                super::allocation::KernelOwnedAllocation::NativeOriginal(_) => 0,
            };
            self.remaining_owned_bytes = self.remaining_owned_bytes.checked_sub(retained)
                .ok_or_else(|| accounted_leaf_error(invalid_batch_result()))?;
            let decoded = decode_read_with_accounting::<F>(&read, limits, accounting);
            drop(read);
            release_batch_allocation(
                super::allocation::KernelOwnedAllocation::Standalone, retained, accounting,
            )?;
            admit_decoded_with_accounting(
                self.store, DecodedCacheKey::new::<Page<F>>(object_id, limits),
                decoded?, accounting, self.data_custody,
            )
        })();
        if decoded.is_err() {
            self.release_pending_with_accounting(accounting)?;
        }
        decoded
    }

    pub(crate) fn finish_with_accounting(
        mut self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        let valid = self.reads.len() == 0 && self.sources.len() == 0;
        self.release_pending_with_accounting(accounting)?;
        cancellation.check()?;
        accounting.check()?;
        if valid { Ok(()) } else { Err(accounted_leaf_error(invalid_batch_result())) }
    }

    pub(crate) fn discard_with_accounting(
        mut self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        self.release_pending_with_accounting(accounting)
    }

    fn release_pending_with_accounting(
        &mut self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let allocations = match accounting {
            super::allocation::KernelAccounting::Standalone { allocations, .. } => Some(allocations),
            super::allocation::KernelAccounting::BorrowedStandalone { allocations, .. } => Some(&mut **allocations),
            #[cfg(not(target_arch = "wasm32"))]
            super::allocation::KernelAccounting::NativeOriginal { .. } => None,
        };
        self.release_pending(allocations).map_err(accounted_leaf_error)
    }

    fn release_pending(&mut self, allocations: Option<&mut AllocationLedger>) -> Result<(), Error> {
        if self.released { return Ok(()); }
        self.reads = Vec::new().into_iter();
        self.sources = Vec::new().into_iter();
        let source_allocation = std::mem::replace(
            &mut self.source_allocation, super::allocation::KernelOwnedAllocation::Standalone,
        );
        let read_allocation = std::mem::replace(
            &mut self.read_allocation, super::allocation::KernelOwnedAllocation::Standalone,
        );
        let source_bytes = if matches!(source_allocation, super::allocation::KernelOwnedAllocation::Standalone) {
            self.source_bytes
        } else { 0 };
        let read_bytes = if matches!(read_allocation, super::allocation::KernelOwnedAllocation::Standalone) {
            self.read_container_bytes.checked_add(self.remaining_owned_bytes)
                .ok_or(Error::AllocationFailed)?
        } else { 0 };
        drop(source_allocation);
        drop(read_allocation);
        let bytes = source_bytes.checked_add(read_bytes).ok_or(Error::AllocationFailed)?;
        if bytes != 0 {
            allocations.ok_or(AllocationError::ReleaseInvariant)?.release(bytes)?;
        }
        self.source_bytes = 0;
        self.read_container_bytes = 0;
        self.remaining_owned_bytes = 0;
        self.released = true;
        Ok(())
    }
}

fn batch_storage_error(error: ObjectStoreError) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => error.into(),
    }
}

fn retained_bytes(read: &ObjectRead) -> u64 {
    match read.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    }
}

fn invalid_batch_result() -> Error {
    Error::Storage(ObjectStoreError::Rejected(
        "object store returned an invalid page batch".to_owned(),
    ))
}

fn logical_page_bytes<F: Format>(shape: DecodedPageShape) -> Option<u64> {
    let container_bytes = match shape.kind {
        DecodedPageKind::Leaf => shape.items.checked_mul(size_of::<F::Value>()),
        DecodedPageKind::Internal => shape.items.checked_mul(size_of::<Child<F::Key>>()),
    }
    .map(usize_to_u64)?;
    container_bytes.checked_add(shape.nested_bytes)
}

fn decode_read<F: Format>(
    read: &ObjectRead,
    limits: DecodeLimits,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<OwnedPage<F>, Error> {
    decode_read_with_custody::<F>(read, limits, budget, allocations, work, None, 0)
}

pub(crate) fn decode_read_with_accounting<F: Format>(
    read: &ObjectRead,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(OwnedPage<F>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::file_read::decode_owned(accounting, |steps| {
            let page = F::decode(read, limits, &mut |step| steps(step))?;
            Ok(page)
        }).map(|(page, allocation)| {
            let logical_bytes = match &allocation {
                super::allocation::KernelOwnedAllocation::NativeOriginal(owned) => owned.bytes(),
                super::allocation::KernelOwnedAllocation::Standalone => 0,
            };
            (OwnedPage { page: PageLease::Owned(page), logical_bytes, native_custody: None }, allocation)
        });
    }
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .expect("native decoding uses the original step engine");
    decode_read::<F>(read, limits, budget, allocations, work)
        .map(|page| (page, super::allocation::KernelOwnedAllocation::Standalone))
        .map_err(accounted_leaf_error)
}

pub(crate) fn clone_key_with_accounting<F: Format>(
    key: &F::Key,
    maximum_bytes: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(F::Key, super::allocation::KernelOwnedAllocation), crate::FsError> {
    clone_key_with_accounting_and_custody::<F>(key, maximum_bytes, accounting, None, 0)
}

pub(crate) fn clone_value_with_accounting<F: Format>(
    value: &F::Value,
    maximum_bytes: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(F::Value, super::allocation::KernelOwnedAllocation), crate::FsError> {
    clone_value_with_accounting_and_custody::<F>(value, maximum_bytes, accounting, None, 0)
}

pub(crate) fn clone_key_with_accounting_and_custody<F: Format>(
    key: &F::Key,
    maximum_bytes: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(F::Key, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::file_read::decode_owned(accounting, |steps| F::try_clone_key(key, maximum_bytes, &mut |step| steps(step)));
    }
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .expect("native key cloning uses the original step engine");
    clone_key::<F>(key, maximum_bytes, budget, allocations, work, data_custody, cached_local_bytes)
        .map(|key| (key, super::allocation::KernelOwnedAllocation::Standalone))
        .map_err(accounted_leaf_error)
}

pub(crate) fn clone_value_with_accounting_and_custody<F: Format>(
    value: &F::Value,
    maximum_bytes: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(F::Value, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::file_read::decode_owned(accounting, |steps| F::try_clone_value(value, maximum_bytes, &mut |step| steps(step)));
    }
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .expect("native value cloning uses the original step engine");
    clone_value::<F>(value, maximum_bytes, budget, allocations, work, data_custody, cached_local_bytes)
        .map(|value| (value, super::allocation::KernelOwnedAllocation::Standalone))
        .map_err(accounted_leaf_error)
}

fn accounted_leaf_error(error: Error) -> crate::FsError {
    match error {
        Error::AllocationFailed => CanonicalDecodeError::AllocationFailed.into(),
        Error::Allocation(error) => error.into_work_or(CanonicalDecodeError::AllocationFailed.into()),
        Error::Storage(error) => error.into(),
        Error::Decode(error) => error.into(),
        Error::Work(error) => error.into(),
    }
}

fn claim_read_allocation(
    bytes: u64,
    operations: u64,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(), Error> {
    work.verify(budget)?;
    allocations.live_bytes().checked_sub(cached_local_bytes).ok_or(WorkError::Overflow)?;
    let external = data_custody.map_or(0, super::ProofDataCustody::operation_cache_bytes)
        .checked_sub(cached_local_bytes).ok_or(WorkError::Overflow)?;
    let mut allowance = budget;
    allowance.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(external)
        .ok_or(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: external,
            maximum: budget.peak_allocation_bytes,
        })?;
    let prior_peak = work.peak_allocation_bytes;
    let mut next = *work;
    next.peak_allocation_bytes = 0;
    allocations.claim_bytes(bytes, operations, &mut next, allowance)?;
    next.peak_allocation_bytes = prior_peak.max(
        allocations.live_bytes().checked_add(external).ok_or(WorkError::Overflow)?,
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
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<OwnedPage<F>, Error> {
    let prepared = (|| -> Result<_, Error> {
        let shape = F::decode_shape(read, limits)?;
        work.charge_items(usize_to_u64(shape.items), &budget)?;
        if let Some(owner) = data_custody {
            owner.record_completed_leaf_work(WorkCounters {
                items_examined: usize_to_u64(shape.items), ..WorkCounters::default()
            })?;
        }
        let logical_bytes = logical_page_bytes::<F>(shape).ok_or(Error::AllocationFailed)?;
        Ok((shape, logical_bytes))
    })();
    let (shape, logical_bytes) = prepared?;
    claim_read_allocation(logical_bytes, u64::from(logical_bytes != 0), allocations, work, budget, data_custody, cached_local_bytes)?;
    if let Some(owner) = data_custody {
        if let Err(error) = owner.record_completed_leaf_work(WorkCounters {
            allocation_operations: u64::from(logical_bytes != 0),
            peak_allocation_bytes: allocations.live_bytes().checked_sub(cached_local_bytes)
                .ok_or(WorkError::Overflow)?, ..WorkCounters::default()
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
            bytes_copied: shape.nested_bytes, ..WorkCounters::default()
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

pub(crate) fn clone_key<F: Format>(
    key: &F::Key,
    maximum_bytes: u32,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<F::Key, Error> {
    let nested = F::key_nested_bytes(key);
    if nested == 0 { return F::try_clone_key(key, maximum_bytes, &mut |_| Ok::<_, CanonicalDecodeError>(())).map_err(Error::Decode); }
    admit_clone(nested, budget, allocations, work, data_custody, cached_local_bytes)?;
    match F::try_clone_key(key, maximum_bytes, &mut |_| Ok::<_, CanonicalDecodeError>(())) {
        Ok(cloned) => Ok(cloned),
        Err(error) => {
            allocations.release(nested)?;
            Err(error.into())
        }
    }
}

pub(crate) fn clone_value<F: Format>(
    value: &F::Value,
    maximum_bytes: u32,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<F::Value, Error> {
    let nested = F::value_nested_bytes(value);
    if nested == 0 { return F::try_clone_value(value, maximum_bytes, &mut |_| Ok::<_, CanonicalDecodeError>(())).map_err(Error::Decode); }
    admit_clone(nested, budget, allocations, work, data_custody, cached_local_bytes)?;
    match F::try_clone_value(value, maximum_bytes, &mut |_| Ok::<_, CanonicalDecodeError>(())) {
        Ok(cloned) => Ok(cloned),
        Err(error) => {
            allocations.release(nested)?;
            Err(error.into())
        }
    }
}

fn admit_clone(
    nested: u64,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    cached_local_bytes: u64,
) -> Result<(), Error> {
    let copied = WorkCounters { bytes_copied: nested, ..WorkCounters::default() };
    work.admit(&copied, &budget)?;
    let external = match data_custody {
        Some(owner) => owner.operation_cache_bytes().checked_sub(cached_local_bytes).ok_or(WorkError::Overflow)?,
        None => 0,
    };
    let prior_live = allocations.live_bytes();
    let peak = prior_live.checked_add(nested).and_then(|bytes| bytes.checked_add(external))
        .ok_or(WorkError::Overflow)?;
    if peak > budget.peak_allocation_bytes {
        return Err(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: peak, maximum: budget.peak_allocation_bytes,
        }.into());
    }
    let mut next = *work;
    let earlier_peak = next.peak_allocation_bytes;
    next.peak_allocation_bytes = 0;
    let mut admission = budget;
    admission.peak_allocation_bytes = admission.peak_allocation_bytes.checked_sub(external).ok_or(WorkError::Overflow)?;
    allocations.claim_bytes(nested, u64::from(nested != 0), &mut next, admission)?;
    next.peak_allocation_bytes = earlier_peak.max(peak);
    next.charge(&copied, &budget)?;
    if let Some(owner) = data_custody {
        let uncached_live = prior_live.checked_sub(cached_local_bytes).ok_or(WorkError::Overflow)?;
        if let Err(error) = owner.record_operation_clone_work(nested, uncached_live) {
            allocations.release(nested)?;
            return Err(error.into());
        }
    }
    *work = next;
    Ok(())
}

/// One authenticated page read, as charged before any backend work.
const PAGE_READ: WorkCounters = WorkCounters {
    page_reads: 1,
    ..WorkCounters::UNCHARGED
};

fn charge_copy(
    copied: u64,
    budget: &WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(), Error> {
    match work.charge(
        &WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::default()
        },
        budget,
    ) {
        Ok(()) => Ok(()),
        Err(WorkError::Overflow) => Err(WorkError::Overflow.into()),
        Err(error) => {
            allocations.release(copied)?;
            Err(error.into())
        }
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/persistent_io.rs"]
mod tests;
