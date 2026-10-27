//! Count the immutable-object seam without replacing the caller's work ledger,
//! decoded cache, native allocations, publication hold or collection owner.
use crate::{
    AsyncObjectStore, AuthorityAllocation, AuthorityId, AuthorityWork, Collection, DurableCommit,
    ObjectId, ObjectRead, ObjectReadRequest, ObjectStoreError, PublicationHold, PublicationScope,
    async_storage::{DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue},
    storage::{HashedObject, ObjectWrite},
};
use acyclic_stream::{
    OwnedBytes,
    testing::{Counted, RequestClass},
};
use std::sync::Arc;

impl<P: AsyncObjectStore> AsyncObjectStore for Counted<P> {
    fn supports_decoded_cache(&self) -> bool {
        self.inner().supports_decoded_cache()
    }

    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.inner().decoded_cache_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.inner().decoded_cache_admit(key, value)
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().put(id, bytes, work).await
    }

    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().put_hashed(object, work).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().put_many(writes, work).await
    }

    async fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().put_many_hashed(objects, work).await
    }

    async fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<PublicationHold, ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().flush_before_publish(scope, work).await
    }

    async fn historical_generation_evidence(
        &self,
        authority: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner()
            .historical_generation_evidence(authority, record, maximum_bytes, maximum_objects, work)
            .await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().read(id, maximum_bytes, work).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().read_many(requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.record(RequestClass::Head);
        self.inner().contains(id, work).await
    }

    fn collection(&self) -> Option<&Arc<Collection>> {
        self.inner().collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner().collection_sweeps()
    }
}
