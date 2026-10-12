use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::memory::MemoryObjectStore;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult,
    ObjectStoreError,
};
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicBool, Ordering};

/// Deterministic conformance backend that returns independently owned read
/// buffers. The production memory backend intentionally shares immutable
/// byte buffers; this backend proves kernels correctly admit the more expensive
/// ownership contract required of remote and native adapters.
#[derive(Default)]
pub(crate) struct OwnedReadObjectStore {
    pub(crate) inner: MemoryObjectStore,
    pub(crate) reject_decoded_admission: AtomicBool,
}

impl OwnedReadObjectStore {
    fn own_read(
        receipt: &crate::OperationReceipt<ObjectRead>,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        let logical_bytes = u64::try_from(receipt.value.bytes.len()).map_err(|_| {
            ObjectFailure::new(
                ObjectStoreError::Work(crate::WorkError::Overflow),
                receipt.work,
            )
        })?;
        let mut work = receipt
            .work
            .checked_add(WorkCounters {
                bytes_copied: logical_bytes,
                allocation_operations: u64::from(logical_bytes != 0),
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
        work.peak_allocation_bytes = work
            .peak_allocation_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| {
                ObjectFailure::new(
                    ObjectStoreError::Work(crate::WorkError::Overflow),
                    receipt.work,
                )
            })?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
        Ok(crate::OperationReceipt {
            value: ObjectRead {
                bytes: OwnedBytes::copy_from_slice(&receipt.value.bytes),
                retention: ObjectReadRetention::Owned { logical_bytes },
            },
            work,
        })
    }
}

impl AsyncObjectStore for OwnedReadObjectStore {
    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        if self.reject_decoded_admission.load(Ordering::Relaxed) {
            Err(ObjectStoreError::Corrupt)
        } else {
            Ok(DecodedCacheAdmission::Uncached(value))
        }
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put(&self.inner, object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            Self::own_read(
                &{
                    let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                    let __object_result = AsyncObjectStore::read(
                        &self.inner,
                        object_id,
                        maximum_bytes,
                        &mut __object_work,
                    )
                    .await;
                    __object_work.receipt(__object_result)
                }?,
                budget,
            )
        }
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        crate::async_storage::read_many_sequential(self, requests, context).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, object_id, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}
