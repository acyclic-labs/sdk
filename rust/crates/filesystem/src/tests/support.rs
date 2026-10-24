use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::memory::MemoryObjectStore;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult,
    ObjectStoreError,
};
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicBool, Ordering};

/// Conformance backend with independently owned read buffers.
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
    ) -> Result<(), ObjectStoreError> {
        ordinary(context)?;
        self.inner.put(object_id, bytes, context).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        ordinary(context)?;
        let budget = context.read_budget()?;
        let mut backend = crate::AuthorityWork::new(budget, context.cancellation);
        let result = self
            .inner
            .read(object_id, maximum_bytes, &mut backend)
            .await;
        let owned = backend
            .receipt(result)
            .and_then(|receipt| Self::own_read(&receipt, budget));
        context.accept(owned)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        ordinary(context)?;
        crate::async_storage::read_many_sequential(self, requests, context).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        ordinary(context)?;
        self.inner.contains(object_id, context).await
    }
}

fn ordinary(context: &crate::AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if context.is_native() {
        return Err(ObjectStoreError::NativeUnavailable);
    }
    context
        .cancellation
        .check()
        .map_err(|_| ObjectStoreError::Cancelled)
}
