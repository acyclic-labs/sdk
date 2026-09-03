use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::memory::MemoryObjectStore;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStore, ObjectStoreError,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

/// Deterministic conformance backend that returns independently owned read
/// buffers. The production memory backend intentionally shares immutable
/// `Bytes`; this backend proves kernels correctly admit the more expensive
/// ownership contract required of remote and native adapters.
#[derive(Default)]
pub(crate) struct OwnedReadObjectStore {
    pub(crate) inner: MemoryObjectStore,
    pub(crate) reject_decoded_admission: AtomicBool,
}

impl OwnedReadObjectStore {
    fn own_read(
        receipt: &ObjectReceipt<ObjectRead>,
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
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes: Bytes::copy_from_slice(&receipt.value.bytes),
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
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        Self::own_read(
            &ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)?,
            budget,
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}
