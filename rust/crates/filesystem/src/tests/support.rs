use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::memory::MemoryObjectStore;
use crate::performance::WorkCounters;
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
};
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicBool, Ordering};

/// Conformance backend with independently owned read buffers.
#[derive(Default)]
pub(crate) struct OwnedReadObjectStore {
    pub(crate) inner: MemoryObjectStore,
    pub(crate) reject_decoded_admission: AtomicBool,
}

impl AsyncObjectStore for OwnedReadObjectStore {
    fn decoded_cache_lookup(
        &self,
        key: DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError> {
        Ok(crate::async_storage::DecodedCacheLookup::Miss(
            crate::async_storage::DecodedCacheMiss::new(key, 0, self),
        ))
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
        let read = self.inner.read(object_id, maximum_bytes, context).await?;
        let logical_bytes = read.bytes.len() as u64;
        let mut accounting = context.read_accounting()?;
        let mut scope = accounting
            .allocation_scope()
            .map_err(crate::async_storage::object_error)?;
        let allocation = scope
            .claim_owned(logical_bytes, u64::from(logical_bytes != 0))
            .map_err(crate::async_storage::object_error)?;
        let bytes = scope
            .perform(
                WorkCounters {
                    bytes_copied: logical_bytes,
                    ..WorkCounters::UNCHARGED
                },
                || OwnedBytes::copy_from_slice(&read.bytes),
            )
            .map_err(crate::async_storage::object_error)?;
        scope
            .keep_bytes(logical_bytes)
            .map_err(crate::async_storage::object_error)?;
        drop(allocation);
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned { logical_bytes },
        })
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

/// All physical-cut adapters share the same dedicated supervisors and bound.
pub(crate) fn finish_physical_cuts<F: FnOnce() -> usize + Send>(jobs: Vec<F>) -> Vec<usize> {
    acyclic_test_support::physical_cuts::run(jobs, acyclic_native_runtime::InlineBlocking::enter)
}

#[path = "support/objects.rs"]
mod objects;
pub(crate) use objects::{
    put_attribute_page, put_blob_page, put_extent_page, put_file_metadata, put_file_table_page,
    put_generation_root, put_object, put_tree_page,
};

impl crate::async_storage::DecodedCacheOrigin for OwnedReadObjectStore {
    fn admit(
        &self,
        _key: DecodedCacheKey,
        _epoch: u64,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        if self.reject_decoded_admission.load(Ordering::Relaxed) {
            Err(ObjectStoreError::Corrupt)
        } else {
            Ok(DecodedCacheAdmission::Uncached(value))
        }
    }
}

#[path = "support/paused_objects.rs"]
mod paused_objects;
pub(crate) use paused_objects::PausedStore;
