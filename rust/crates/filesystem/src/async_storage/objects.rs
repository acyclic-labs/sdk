use super::*;

pub(crate) async fn prepare_object<S: AsyncObjectStore>(
    store: &S,
    object: crate::HashedObject,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<crate::HashedObject, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return store
            .prepare_hashed(object, &mut AuthorityWork::native(original, cancellation))
            .await
            .map_err(Into::into);
    }
    let mut work = AuthorityWork::new(accounting.remaining()?, cancellation);
    let result = store.prepare_hashed(object, &mut work).await;
    crate::kernel::complete_standalone_backend(accounting, work.spent(), 0)?;
    result.map_err(Into::into)
}

/// The objects one authority record makes reachable, which must be durable before the record is appended.
#[derive(Clone, Copy, Debug)]
pub enum PublicationScope<'a> {
    /// The authenticated closure of one published generation.
    Closure {
        /// Every object the record reaches, or, when its proof reused the
        /// published parent's proof from the same collection epoch, every
        /// object it reaches that the parent's closure did not list.
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
    /// Whether publication requires every referenced child to be stored first.
    fn requires_child_first_publication(&self) -> bool { false }
    /// Largest reference emitted by preparation, used to pack bounded pages.
    fn maximum_reference_bytes(&self) -> usize { 33 }
    /// Prepares the physical association before a parent encodes this reference.
    /// Returned bytes retain their original native work custody through batching.
    fn prepare_hashed(
        &self,
        object: crate::HashedObject,
        _context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<crate::HashedObject, crate::ObjectStoreError>> + ProviderTask {
        async move { Ok(object) }
    }
    /// Whether decoded-cache hooks may retain or reject a decoded page.
    ///
    /// Return false only when both hooks have their default no-cache behavior.
    /// The default preserves existing providers' cache and admission hooks.
    fn supports_decoded_cache(&self) -> bool {
        true
    }

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
    /// Imports the exact immutable representation named by the full reference.
    /// Plain stores use their ordinary write boundary. Sealed stores must
    /// authenticate the association and preserve the original CAS bytes.
    fn put_stored(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask {
        async move {
            if object_id.storage.is_some() {
                return Err(crate::ObjectStoreError::DigestMismatch);
            }
            self.put(object_id, bytes, context).await
        }
    }
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
            #[cfg(not(target_arch = "wasm32"))]
            if context.is_native() {
                if objects.is_empty() { return Err(crate::ObjectStoreError::EmptyWriteBatch); }
                for object in objects { self.put_hashed(object.clone(), context).await?; }
                return Ok(());
            }
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
    /// Reads the exact CAS representation named by the full reference.
    fn read_stored(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<crate::ObjectRead, crate::ObjectStoreError>> + ProviderTask {
        async move {
            if object_id.storage.is_some() { return Err(crate::ObjectStoreError::DigestMismatch); }
            self.read(object_id, maximum_bytes, context).await
        }
    }
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
    /// Exports exact stored bytes in request order, retaining the batch custody.
    /// Limits refer to plaintext; sealed providers admit their seal overhead.
    fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError>> + ProviderTask {
        async move {
            if requests.iter().any(|request| request.object_id.storage.is_some()) {
                return Err(crate::ObjectStoreError::DigestMismatch);
            }
            self.read_many(requests, context).await
        }
    }
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
