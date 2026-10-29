//! Count the immutable-object seam without replacing the caller's work ledger,
//! decoded cache, native allocations, publication hold or collection owner.
use crate::{
    AsyncObjectStore, AuthorityAllocation, AuthorityId, AuthorityWork, Collection, DurableCommit,
    ObjectId, ObjectRead, ObjectReadRequest, ObjectStoreError, PublicationHold, PublicationScope,
    async_storage::{DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue},
    storage::{HashedObject, ObjectWrite},
};
use acyclic_stream::{
    OwnedBytes, ProviderTask,
    testing::{Counted, RequestClass},
};
use std::{future::Future, sync::Arc};

impl<P: AsyncObjectStore> AsyncObjectStore for Counted<P> {
    fn requires_child_first_publication(&self) -> bool {
        self.inner().requires_child_first_publication()
    }

    fn maximum_reference_bytes(&self) -> usize {
        self.inner().maximum_reference_bytes()
    }

    fn prepare_hashed(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<HashedObject, ObjectStoreError>> + ProviderTask {
        self.inner().prepare_hashed(object, work)
    }

    fn supports_decoded_cache(&self) -> bool {
        self.inner().supports_decoded_cache()
    }

    async fn put_stored(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().put_stored(id, bytes, context).await
    }
    async fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().read_many_stored(requests, context).await
    }
    async fn read_stored(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, ObjectStoreError> {
        self.record(RequestClass::Data);
        self.inner().read_stored(id, maximum, context).await
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

    fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().put(id, bytes, work)
    }

    fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().put_hashed(object, work)
    }

    fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().put_many(writes, work)
    }

    fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().put_many_hashed(objects, work)
    }

    fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<PublicationHold, ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().flush_before_publish(scope, work)
    }

    fn historical_generation_evidence(
        &self,
        authority: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError>,
    > + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().historical_generation_evidence(
            authority,
            record,
            maximum_bytes,
            maximum_objects,
            work,
        )
    }

    fn read(
        &self,
        id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().read(id, maximum_bytes, work)
    }

    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError>>
    + ProviderTask {
        self.record(RequestClass::Data);
        self.inner().read_many(requests, work)
    }

    fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<bool, ObjectStoreError>> + ProviderTask {
        self.record(RequestClass::Head);
        self.inner().contains(id, work)
    }

    fn collection(&self) -> Option<&Arc<Collection>> {
        self.inner().collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner().collection_sweeps()
    }
}
