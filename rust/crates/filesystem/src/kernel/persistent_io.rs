//! One authenticated, bounded page-read boundary for persistent tree kernels.

use super::allocation::{AllocationError, AllocationLedger};
use super::codec::DecodedPageKind;
use super::persistent_btree::{Child, Format, Page};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
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
    pub(crate) fn into_owned(
        self,
        budget: WorkBudget,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
    ) -> Result<(Page<F>, u64), Error> {
        match self.page {
            PageLease::Owned(page) => Ok((page, self.logical_bytes)),
            PageLease::Shared {
                page,
                logical_bytes,
            } => {
                allocations.claim_bytes(
                    logical_bytes,
                    u64::from(logical_bytes != 0),
                    work,
                    budget,
                )?;
                let copied = work.checked_add(WorkCounters {
                    bytes_copied: logical_bytes,
                    ..WorkCounters::default()
                })?;
                if let Err(error) = copied.verify(budget) {
                    allocations.release(logical_bytes)?;
                    return Err(error.into());
                }
                *work = copied;
                Ok(((*page).clone(), logical_bytes))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ReadMode {
    Shared,
    Mutable,
}

struct ReadContext<'a> {
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    allocations: &'a mut AllocationLedger,
    work: &'a mut WorkCounters,
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

pub(crate) async fn read_page<S, F>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<OwnedPage<F>, Error>
where
    S: AsyncObjectStore,
    F: Format,
{
    let mut context = ReadContext {
        limits,
        budget,
        cancellation,
        allocations,
        work,
    };
    read_page_mode::<S, F>(store, page, &mut context, ReadMode::Shared).await
}

pub(crate) async fn read_page_mutable<S, F>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<OwnedPage<F>, Error>
where
    S: AsyncObjectStore,
    F: Format,
{
    let mut context = ReadContext {
        limits,
        budget,
        cancellation,
        allocations,
        work,
    };
    read_page_mode::<S, F>(store, page, &mut context, ReadMode::Mutable).await
}

async fn read_page_mode<S, F>(
    store: &S,
    page: ObjectId,
    context: &mut ReadContext<'_>,
    mode: ReadMode,
) -> Result<OwnedPage<F>, Error>
where
    S: AsyncObjectStore,
    F: Format,
{
    let prospective = context.work.checked_add(WorkCounters {
        page_reads: 1,
        ..WorkCounters::default()
    })?;
    prospective.verify(context.budget)?;
    let cache_key = DecodedCacheKey::new::<Page<F>>(page, context.limits);
    if let Some(cached) = store.decoded_cache_get(cache_key)? {
        *context.work = prospective;
        let logical_bytes = cached.logical_bytes;
        let page = cached
            .value
            .downcast::<Page<F>>()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        return match mode {
            ReadMode::Shared => Ok(OwnedPage {
                page: PageLease::Shared {
                    page,
                    logical_bytes,
                },
                logical_bytes: 0,
            }),
            ReadMode::Mutable => {
                context.allocations.claim_bytes(
                    logical_bytes,
                    u64::from(logical_bytes != 0),
                    context.work,
                    context.budget,
                )?;
                let copied = context.work.checked_add(WorkCounters {
                    bytes_copied: logical_bytes,
                    ..WorkCounters::default()
                })?;
                if let Err(error) = copied.verify(context.budget) {
                    context.allocations.release(logical_bytes)?;
                    return Err(error.into());
                }
                *context.work = copied;
                Ok(OwnedPage {
                    page: PageLease::Owned((*page).clone()),
                    logical_bytes,
                })
            }
        };
    }
    let mut remaining = prospective.remaining(context.budget)?;
    remaining.peak_allocation_bytes = context
        .budget
        .peak_allocation_bytes
        .checked_sub(context.allocations.live_bytes())
        .ok_or(WorkError::Overflow)?;
    let receipt = match AsyncObjectStore::read(
        store,
        page,
        context.limits.maximum_page_object_bytes(),
        remaining,
        context.cancellation,
    )
    .await
    {
        Ok(receipt) => receipt,
        Err(failure) => {
            *context.work =
                merge_backend_work(prospective, *failure.work, context.allocations.live_bytes())?;
            return Err(Error::Storage(failure.error));
        }
    };
    *context.work =
        merge_backend_work(prospective, receipt.work, context.allocations.live_bytes())?;
    context.work.verify(context.budget)?;

    let retained_bytes = retained_bytes(&receipt.value);
    context
        .allocations
        .claim_bytes(retained_bytes, 0, context.work, context.budget)?;
    let decoded = decode_read::<F>(
        &receipt.value,
        context.limits,
        context.budget,
        context.allocations,
        context.work,
    );
    drop(receipt.value);
    context.allocations.release(retained_bytes)?;
    match mode {
        ReadMode::Mutable => decoded,
        ReadMode::Shared => admit_decoded(store, cache_key, decoded, context.allocations),
    }
}

fn admit_decoded<S, F>(
    store: &S,
    key: DecodedCacheKey,
    decoded: Result<OwnedPage<F>, Error>,
    allocations: &mut AllocationLedger,
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
    let value = DecodedCacheValue {
        value: Arc::new(page),
        logical_bytes,
    };
    match store.decoded_cache_admit(key, value) {
        Ok(DecodedCacheAdmission::Shared(value)) => {
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
            })
        }
        Ok(DecodedCacheAdmission::Uncached(value)) => {
            let page = value
                .value
                .downcast::<Page<F>>()
                .map_err(|_| ObjectStoreError::Corrupt)?;
            Ok(OwnedPage {
                page: PageLease::Owned(
                    Arc::try_unwrap(page).map_err(|_| ObjectStoreError::Corrupt)?,
                ),
                logical_bytes,
            })
        }
        Err(error) => {
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
}

struct ColdReadContext<'a> {
    prospective: WorkCounters,
    remaining: WorkBudget,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    allocations: &'a AllocationLedger,
    work: &'a mut WorkCounters,
}

pub(crate) struct PageBatch<'a, S: AsyncObjectStore, F: Format> {
    store: &'a S,
    sources: std::vec::IntoIter<BatchSource>,
    reads: std::vec::IntoIter<ObjectRead>,
    container_bytes: u64,
    remaining_owned_bytes: u64,
    released: bool,
    format: PhantomData<F>,
}

pub(crate) async fn read_pages<'a, S, F, I>(
    store: &'a S,
    pages: I,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<PageBatch<'a, S, F>, Error>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    let plan = plan_page_batch::<S, F, I>(
        store,
        pages,
        limits,
        budget,
        cancellation,
        allocations,
        work,
    )?;
    let BatchPlan {
        sources,
        cold_requests,
        source_bytes,
        cold_request_bytes,
        prospective,
        remaining,
    } = plan;
    let reads = match read_cold_pages(
        store,
        &cold_requests,
        ColdReadContext {
            prospective,
            remaining,
            budget,
            cancellation,
            allocations,
            work,
        },
    )
    .await
    {
        Ok(reads) => reads,
        Err(error) => {
            allocations.release(source_bytes)?;
            allocations.release(cold_request_bytes)?;
            return Err(error);
        }
    };
    allocations.release(cold_request_bytes)?;
    let (container_bytes, remaining_owned_bytes, newly_retained) =
        match batch_retained_bytes(&reads, source_bytes) {
            Ok(retained) => retained,
            Err(error) => {
                allocations.release(source_bytes)?;
                return Err(error);
            }
        };
    if let Err(error) = allocations.claim_bytes(newly_retained, 0, work, budget) {
        allocations.release(source_bytes)?;
        return Err(error.into());
    }
    Ok(PageBatch {
        store,
        sources: sources.into_iter(),
        reads: reads.into_iter(),
        container_bytes,
        remaining_owned_bytes,
        released: false,
        format: PhantomData,
    })
}

fn plan_page_batch<S, F, I>(
    store: &S,
    pages: I,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<BatchPlan, Error>
where
    S: AsyncObjectStore,
    F: Format,
    I: ExactSizeIterator<Item = ObjectId>,
{
    let count = pages.len();
    if count == 0 {
        return Err(invalid_batch_result());
    }
    cancellation
        .check()
        .map_err(|_| Error::Storage(ObjectStoreError::Cancelled))?;
    let source_bytes = allocations.claim_elements::<BatchSource>(count, work, budget)?;
    let cold_request_bytes =
        match allocations.claim_elements::<ObjectReadRequest>(count, work, budget) {
            Ok(bytes) => bytes,
            Err(error) => {
                allocations.release(source_bytes)?;
                return Err(error.into());
            }
        };
    let planned = (|| {
        let mut sources = Vec::new();
        let mut cold_requests = Vec::new();
        sources
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        cold_requests
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        let prospective = work.checked_add(WorkCounters {
            page_reads: u64::try_from(count).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })?;
        prospective.verify(budget)?;
        for object_id in pages {
            let key = DecodedCacheKey::new::<Page<F>>(object_id, limits);
            if let Some(value) = store.decoded_cache_get(key)? {
                sources.push(BatchSource::Cached(value));
            } else {
                sources.push(BatchSource::Cold(object_id));
                cold_requests.push(ObjectReadRequest {
                    object_id,
                    maximum_bytes: limits.maximum_page_object_bytes(),
                });
            }
        }
        let mut remaining = prospective.remaining(budget)?;
        remaining.peak_allocation_bytes = budget
            .peak_allocation_bytes
            .checked_sub(allocations.live_bytes())
            .ok_or(WorkError::Overflow)?;
        Ok(BatchPlan {
            sources,
            cold_requests,
            source_bytes,
            cold_request_bytes,
            prospective,
            remaining,
        })
    })();
    if planned.is_err() {
        allocations.release(source_bytes)?;
        allocations.release(cold_request_bytes)?;
    }
    planned
}

async fn read_cold_pages<S: AsyncObjectStore>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: ColdReadContext<'_>,
) -> Result<Vec<ObjectRead>, Error> {
    if requests.is_empty() {
        *context.work = context.prospective;
        return Ok(Vec::new());
    }
    let receipt = match store
        .read_many(requests, context.remaining, context.cancellation)
        .await
    {
        Ok(receipt) => receipt,
        Err(failure) => {
            *context.work = merge_backend_work(
                context.prospective,
                *failure.work,
                context.allocations.live_bytes(),
            )?;
            return Err(Error::Storage(failure.error));
        }
    };
    *context.work = merge_backend_work(
        context.prospective,
        receipt.work,
        context.allocations.live_bytes(),
    )?;
    context.work.verify(context.budget)?;
    if receipt.value.len() != requests.len() {
        return Err(invalid_batch_result());
    }
    Ok(receipt.value)
}

fn batch_retained_bytes(reads: &[ObjectRead], source_bytes: u64) -> Result<(u64, u64, u64), Error> {
    let result_container_bytes = reads
        .len()
        .checked_mul(size_of::<ObjectRead>())
        .map(crate::foundation::usize_to_u64)
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
    pub(crate) fn next(
        &mut self,
        limits: DecodeLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        allocations: &mut AllocationLedger,
        work: &mut WorkCounters,
    ) -> Result<OwnedPage<F>, Error> {
        if cancellation.is_cancelled() {
            self.release_pending(allocations)?;
            return Err(Error::Storage(ObjectStoreError::Cancelled));
        }
        let Some(source) = self.sources.next() else {
            self.release_pending(allocations)?;
            return Err(invalid_batch_result());
        };
        let object_id = match source {
            BatchSource::Cached(value) => {
                return Ok(OwnedPage {
                    page: PageLease::Shared {
                        page: value
                            .value
                            .downcast::<Page<F>>()
                            .map_err(|_| ObjectStoreError::Corrupt)?,
                        logical_bytes: value.logical_bytes,
                    },
                    logical_bytes: 0,
                });
            }
            BatchSource::Cold(object_id) => object_id,
        };
        let Some(read) = self.reads.next() else {
            self.release_pending(allocations)?;
            return Err(invalid_batch_result());
        };
        let retained = retained_bytes(&read);
        let Some(remaining_owned_bytes) = self.remaining_owned_bytes.checked_sub(retained) else {
            self.release_pending(allocations)?;
            return Err(invalid_batch_result());
        };
        self.remaining_owned_bytes = remaining_owned_bytes;
        let decoded = decode_read::<F>(&read, limits, budget, allocations, work);
        drop(read);
        allocations.release(retained)?;
        match admit_decoded(
            self.store,
            DecodedCacheKey::new::<Page<F>>(object_id, limits),
            decoded,
            allocations,
        ) {
            Ok(page) => Ok(page),
            Err(error) => {
                self.release_pending(allocations)?;
                Err(error)
            }
        }
    }

    pub(crate) fn finish(mut self, allocations: &mut AllocationLedger) -> Result<(), Error> {
        if self.reads.len() != 0 || self.sources.len() != 0 {
            self.release_pending(allocations)?;
            return Err(invalid_batch_result());
        }
        self.release_pending(allocations)
    }

    pub(crate) fn discard(mut self, allocations: &mut AllocationLedger) -> Result<(), Error> {
        self.release_pending(allocations)
    }

    fn release_pending(&mut self, allocations: &mut AllocationLedger) -> Result<(), Error> {
        if self.released {
            return Ok(());
        }
        self.reads = Vec::new().into_iter();
        self.sources = Vec::new().into_iter();
        allocations.release(
            self.container_bytes
                .checked_add(self.remaining_owned_bytes)
                .ok_or(Error::AllocationFailed)?,
        )?;
        self.container_bytes = 0;
        self.remaining_owned_bytes = 0;
        self.released = true;
        Ok(())
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

fn decode_read<F: Format>(
    read: &ObjectRead,
    limits: DecodeLimits,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<OwnedPage<F>, Error> {
    let prepared = (|| -> Result<_, Error> {
        let shape = F::decode_shape(read, limits)?;
        charge_items(work, u64::try_from(shape.items).unwrap_or(u64::MAX), budget)?;
        let container_bytes = match shape.kind {
            DecodedPageKind::Leaf => shape.items.checked_mul(size_of::<F::Value>()),
            DecodedPageKind::Internal => shape.items.checked_mul(size_of::<Child<F::Key>>()),
        }
        .map(crate::foundation::usize_to_u64)
        .ok_or(Error::AllocationFailed)?;
        let logical_bytes = container_bytes
            .checked_add(shape.nested_bytes)
            .ok_or(Error::AllocationFailed)?;
        Ok((shape, logical_bytes))
    })();
    let (shape, logical_bytes) = prepared?;
    if let Err(error) =
        allocations.claim_bytes(logical_bytes, u64::from(logical_bytes != 0), work, budget)
    {
        return Err(error.into());
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
    let decoded = match F::decode(read, limits) {
        Ok(decoded) => decoded,
        Err(error) => {
            allocations.release(logical_bytes)?;
            return Err(error.into());
        }
    };
    Ok(OwnedPage {
        page: PageLease::Owned(decoded),
        logical_bytes,
    })
}

pub(crate) fn clone_key<F: Format>(
    key: &F::Key,
    maximum_bytes: u32,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<F::Key, Error> {
    let nested = F::key_nested_bytes(key);
    admit_clone(nested, budget, allocations, work)?;
    match F::try_clone_key(key, maximum_bytes) {
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
) -> Result<F::Value, Error> {
    let nested = F::value_nested_bytes(value);
    admit_clone(nested, budget, allocations, work)?;
    match F::try_clone_value(value, maximum_bytes) {
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
) -> Result<(), Error> {
    allocations.claim_bytes(nested, u64::from(nested != 0), work, budget)?;
    let copied = match work.checked_add(WorkCounters {
        bytes_copied: nested,
        ..WorkCounters::default()
    }) {
        Ok(copied) => copied,
        Err(error) => {
            allocations.release(nested)?;
            return Err(error.into());
        }
    };
    if let Err(error) = copied.verify(budget) {
        allocations.release(nested)?;
        return Err(error.into());
    }
    *work = copied;
    Ok(())
}

fn charge_items(work: &mut WorkCounters, count: u64, budget: WorkBudget) -> Result<(), WorkError> {
    let prospective = work.checked_add(WorkCounters {
        items_examined: count,
        ..WorkCounters::default()
    })?;
    prospective.verify(budget)?;
    *work = prospective;
    Ok(())
}

pub(crate) fn merge_backend_work(
    prior: WorkCounters,
    mut backend: WorkCounters,
    live_bytes: u64,
) -> Result<WorkCounters, WorkError> {
    let simultaneous_peak = live_bytes
        .checked_add(backend.peak_allocation_bytes)
        .ok_or(WorkError::Overflow)?;
    backend.peak_allocation_bytes = 0;
    let mut merged = prior.checked_add(backend)?;
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    Ok(merged)
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/persistent_io.rs"]
mod tests;
