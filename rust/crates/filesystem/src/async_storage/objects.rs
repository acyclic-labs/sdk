use super::*;

/// The objects one authority record makes reachable, which must be durable before the record is appended.
#[derive(Clone, Copy, Debug)]
pub enum PublicationScope<'a> {
    /// The complete authenticated closure of one published generation.
    Closure {
        /// Every object the record reaches.
        objects: &'a [ObjectId],
        /// The store's [`AsyncObjectStore::collection_sweeps`] before the closure was proven.
        proven_at: u64,
    },
    /// Every object admitted so far, for a record whose closure was not enumerated.
    Everything,
}

contract! {
/// Nonblocking immutable-object contract suitable for `IndexedDB` and remote I/O.
pub trait AsyncObjectStore: ProviderPlatform {
    /// Borrows an immutable decoded cache entry.
    fn decoded_cache_get(
        &self,
        _key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        Ok(None)
    }

    /// Offers one authenticated decoded representation to a disposable cache.
    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        Ok(DecodedCacheAdmission::Uncached(value))
    }

    /// Writes one immutable object.
    fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask;
    /// Writes an object whose digest is already authenticated.
    fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask {
        async move {
            context.object_fallback()?;
            let (id, bytes) = object.into_parts();
            self.put(id, bytes, context).await
        }
    }
    /// Writes an atomic batch when the backend supports it.
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask {
        async move {
            context.object_fallback()?;
            let _ = writes;
            Err(crate::ObjectStoreError::Rejected(
                "object backend has no batch-write primitive".to_owned(),
            ))
        }
    }
    /// Writes a batch whose digests are already authenticated.
    fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask {
        async move {
            context.object_fallback()?;
            let writes = objects
                .iter()
                .map(|object| ObjectWrite {
                    object_id: object.object_id(),
                    bytes: object.bytes().clone(),
                })
                .collect::<Vec<_>>();
            self.put_many(&writes, context).await
        }
    }
    /// Makes the publication scope durable and retains its hold.
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<crate::PublicationHold, crate::ObjectStoreError>> + ProviderTask
    {
        async move {
            context.object_fallback()?;
            Ok(crate::PublicationHold::none())
        }
    }

    /// Reads bounded evidence for one historical generation.
    fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<
            Option<crate::kernel::HistoricalGenerationEvidence>,
            crate::ObjectStoreError,
        >,
    > + ProviderTask {
        async move {
            context.object_fallback()?;
            context
                .cancellation
                .check()
                .map_err(|_| crate::ObjectStoreError::Cancelled)?;
            Ok(None)
        }
    }
    /// Reads one object up to its byte limit.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<crate::ObjectRead, crate::ObjectStoreError>> + ProviderTask;
    /// Reads a batch and retains its native vector allocation.
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<
            (Vec<crate::ObjectRead>, crate::AuthorityAllocation),
            crate::ObjectStoreError,
        >,
    > + ProviderTask;
    /// Tests whether an object exists.
    fn contains(
        &self,
        object_id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<bool, crate::ObjectStoreError>> + ProviderTask;
    /// The collection this store runs while it stays open, if it collects.
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        None
    }
    /// The count a proof records before it starts, for [`PublicationScope::Closure`].
    fn collection_sweeps(&self) -> u64 {
        self.collection()
            .map_or(0, |collection| collection.sweeps())
    }
}

}
