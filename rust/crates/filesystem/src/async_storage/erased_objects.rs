//! The object-safe form of [`AsyncObjectStore`].
//!
//! Layers above the filesystem hold a [`SharedObjectStore`], so no backend type
//! is threaded through them and no backend future is nested in theirs. Every
//! [`AsyncObjectStore`] is a [`DynObjectStore`], and `dyn DynObjectStore`
//! forwards back to [`AsyncObjectStore`]. Each asynchronous call through the
//! erased form costs one boxed future.
use crate::async_storage::{AsyncObjectStore, AuthorityWork, PublicationScope};
use crate::storage::{HashedObject, ObjectId, ObjectReadRequest, ObjectWrite};
use crate::{AuthorityAllocation, ObjectRead, ObjectStoreError, PublicationHold};
use acyclic_stream::{BoxProviderFuture, OwnedBytes, ProviderPlatform, ProviderTask};
use std::future::Future;

type Erased<'a, T> = BoxProviderFuture<'a, Result<T, ObjectStoreError>>;
type Reads = (Vec<ObjectRead>, AuthorityAllocation);
type Evidence = Option<crate::kernel::HistoricalGenerationEvidence>;

/// An [`AsyncObjectStore`] behind a trait object; see the module docs.
pub trait DynObjectStore: ProviderPlatform {
    /// See [`AsyncObjectStore::requires_child_first_publication`].
    fn dyn_requires_child_first_publication(&self) -> bool;
    /// See [`AsyncObjectStore::maximum_reference_bytes`].
    fn dyn_maximum_reference_bytes(&self) -> usize;
    /// See [`AsyncObjectStore::prepare_hashed`].
    fn dyn_prepare_hashed<'a>(
        &'a self,
        object: HashedObject,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, HashedObject>;
    /// See [`AsyncObjectStore::supports_decoded_cache`].
    fn dyn_supports_decoded_cache(&self) -> bool;
    /// See [`AsyncObjectStore::decoded_cache_lookup`].
    fn dyn_decoded_cache_lookup(
        &self,
        key: crate::async_storage::DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError>;
    /// See [`AsyncObjectStore::put`].
    fn dyn_put<'a>(
        &'a self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()>;
    /// See [`AsyncObjectStore::put_stored`].
    fn dyn_put_stored<'a>(
        &'a self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()>;
    /// See [`AsyncObjectStore::put_hashed`].
    fn dyn_put_hashed<'a>(
        &'a self,
        object: HashedObject,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()>;
    /// See [`AsyncObjectStore::put_many`].
    fn dyn_put_many<'a>(
        &'a self,
        writes: &'a [ObjectWrite],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()>;
    /// See [`AsyncObjectStore::put_many_hashed`].
    fn dyn_put_many_hashed<'a>(
        &'a self,
        objects: &'a [HashedObject],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()>;
    /// See [`AsyncObjectStore::flush_before_publish`].
    fn dyn_flush_before_publish<'a>(
        &'a self,
        scope: PublicationScope<'a>,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, PublicationHold>;
    /// See [`AsyncObjectStore::historical_generation_evidence`].
    fn dyn_historical_generation_evidence<'a>(
        &'a self,
        authority_id: crate::AuthorityId,
        record: &'a crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Evidence>;
    /// See [`AsyncObjectStore::read`].
    fn dyn_read<'a>(
        &'a self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ObjectRead>;
    /// See [`AsyncObjectStore::read_stored`].
    fn dyn_read_stored<'a>(
        &'a self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ObjectRead>;
    /// See [`AsyncObjectStore::read_many`].
    fn dyn_read_many<'a>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Reads>;
    /// See [`AsyncObjectStore::read_many_stored`].
    fn dyn_read_many_stored<'a>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Reads>;
    /// See [`AsyncObjectStore::contains`].
    fn dyn_contains<'a>(
        &'a self,
        object_id: ObjectId,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, bool>;
    /// See [`AsyncObjectStore::collection`].
    fn dyn_collection(&self) -> Option<&std::sync::Arc<crate::Collection>>;
    /// See [`AsyncObjectStore::collection_sweeps`].
    fn dyn_collection_sweeps(&self) -> u64;
}

impl<T: AsyncObjectStore> DynObjectStore for T {
    fn dyn_requires_child_first_publication(&self) -> bool {
        self.requires_child_first_publication()
    }
    fn dyn_maximum_reference_bytes(&self) -> usize {
        self.maximum_reference_bytes()
    }
    fn dyn_prepare_hashed<'a>(
        &'a self,
        object: HashedObject,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, HashedObject> {
        Box::pin(self.prepare_hashed(object, context))
    }
    fn dyn_supports_decoded_cache(&self) -> bool {
        self.supports_decoded_cache()
    }
    fn dyn_decoded_cache_lookup(
        &self,
        key: crate::async_storage::DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError> {
        self.decoded_cache_lookup(key)
    }
    fn dyn_put<'a>(
        &'a self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()> {
        Box::pin(self.put(object_id, bytes, context))
    }
    fn dyn_put_stored<'a>(
        &'a self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()> {
        Box::pin(self.put_stored(object_id, bytes, context))
    }
    fn dyn_put_hashed<'a>(
        &'a self,
        object: HashedObject,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()> {
        Box::pin(self.put_hashed(object, context))
    }
    fn dyn_put_many<'a>(
        &'a self,
        writes: &'a [ObjectWrite],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()> {
        Box::pin(self.put_many(writes, context))
    }
    fn dyn_put_many_hashed<'a>(
        &'a self,
        objects: &'a [HashedObject],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ()> {
        Box::pin(self.put_many_hashed(objects, context))
    }
    fn dyn_flush_before_publish<'a>(
        &'a self,
        scope: PublicationScope<'a>,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, PublicationHold> {
        Box::pin(self.flush_before_publish(scope, context))
    }
    fn dyn_historical_generation_evidence<'a>(
        &'a self,
        authority_id: crate::AuthorityId,
        record: &'a crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Evidence> {
        Box::pin(self.historical_generation_evidence(
            authority_id,
            record,
            maximum_bytes,
            maximum_objects,
            context,
        ))
    }
    fn dyn_read<'a>(
        &'a self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ObjectRead> {
        Box::pin(self.read(object_id, maximum_bytes, context))
    }
    fn dyn_read_stored<'a>(
        &'a self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, ObjectRead> {
        Box::pin(self.read_stored(object_id, maximum_bytes, context))
    }
    fn dyn_read_many<'a>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Reads> {
        Box::pin(self.read_many(requests, context))
    }
    fn dyn_read_many_stored<'a>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, Reads> {
        Box::pin(self.read_many_stored(requests, context))
    }
    fn dyn_contains<'a>(
        &'a self,
        object_id: ObjectId,
        context: &'a mut AuthorityWork<'_>,
    ) -> Erased<'a, bool> {
        Box::pin(self.contains(object_id, context))
    }
    fn dyn_collection(&self) -> Option<&std::sync::Arc<crate::Collection>> {
        self.collection()
    }
    fn dyn_collection_sweeps(&self) -> u64 {
        self.collection_sweeps()
    }
}

// Every method forwards, defaults included, so an erased store keeps its
// backend's own behaviour rather than the contract's defaults. The async
// blocks name the call's two borrows, which one boxed lifetime cannot.
impl AsyncObjectStore for dyn DynObjectStore + '_ {
    fn requires_child_first_publication(&self) -> bool {
        self.dyn_requires_child_first_publication()
    }
    fn maximum_reference_bytes(&self) -> usize {
        self.dyn_maximum_reference_bytes()
    }
    fn prepare_hashed(
        &self,
        object: HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<HashedObject, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_prepare_hashed(object, context).await }
    }
    fn supports_decoded_cache(&self) -> bool {
        self.dyn_supports_decoded_cache()
    }
    fn decoded_cache_lookup(
        &self,
        key: crate::async_storage::DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError> {
        self.dyn_decoded_cache_lookup(key)
    }
    fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        async move { self.dyn_put(object_id, bytes, context).await }
    }
    fn put_stored(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        async move { self.dyn_put_stored(object_id, bytes, context).await }
    }
    fn put_hashed(
        &self,
        object: HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        async move { self.dyn_put_hashed(object, context).await }
    }
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        async move { self.dyn_put_many(writes, context).await }
    }
    fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + ProviderTask {
        async move { self.dyn_put_many_hashed(objects, context).await }
    }
    fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<PublicationHold, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_flush_before_publish(scope, context).await }
    }
    fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Evidence, ObjectStoreError>> + ProviderTask {
        async move {
            self.dyn_historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                context,
            )
            .await
        }
    }
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_read(object_id, maximum_bytes, context).await }
    }
    fn read_stored(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, ObjectStoreError>> + ProviderTask {
        async move {
            self.dyn_read_stored(object_id, maximum_bytes, context)
                .await
        }
    }
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Reads, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_read_many(requests, context).await }
    }
    fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Reads, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_read_many_stored(requests, context).await }
    }
    fn contains(
        &self,
        object_id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<bool, ObjectStoreError>> + ProviderTask {
        async move { self.dyn_contains(object_id, context).await }
    }
    fn collection(&self) -> Option<&std::sync::Arc<crate::Collection>> {
        self.dyn_collection()
    }
    fn collection_sweeps(&self) -> u64 {
        self.dyn_collection_sweeps()
    }
}

/// A shared, type-erased object backend.
pub type SharedObjectStore = std::sync::Arc<dyn DynObjectStore>;

/// Erases one object backend into a [`SharedObjectStore`].
pub fn shared_objects<O: AsyncObjectStore + 'static>(objects: O) -> SharedObjectStore {
    std::sync::Arc::new(objects)
}
