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
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
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
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
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
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
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
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            Ok(crate::PublicationHold::none())
        }
    }
    /// Prepares the generation proof before publication.
    fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::ObjectStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            context
                .cancellation
                .check()
                .map_err(|_| crate::ObjectStoreError::Cancelled)?;
            Ok(())
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
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
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
    /// Reads through the same provider operation, recording its actual completed SDK prefix before a caller can decode, fail, or unwind afterward.
    #[doc(hidden)]
    fn read_with_custody(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        observation: Option<&crate::kernel::ProofDataCustody>,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask {
        async move {
            cancellation.check().map_err(|_| {
                crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Cancelled,
                )
            })?;
            let standing = observation.map_or(0, |observation| observation.operation_cache_bytes());
            let mut remaining = budget;
            remaining.peak_allocation_bytes = budget
                .peak_allocation_bytes
                .checked_sub(standing)
                .ok_or_else(|| {
                    crate::storage::ObjectFailure::before_work(
                        crate::storage::ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                            counter: "peak_allocation_bytes",
                            observed: standing,
                            maximum: budget.peak_allocation_bytes,
                        }),
                    )
                })?;
            let result = {
                let mut __object_work = crate::AuthorityWork::new(remaining, cancellation);
                let __object_result = self
                    .read(object_id, maximum_bytes, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            };
            let result = match observation {
                Some(observation) => observe_read(observation, result),
                None => result,
            };
            if standing == 0 {
                return result;
            }
            match result {
                Ok(mut receipt) => {
                    receipt.work = WorkCounters::default()
                        .with_backend(receipt.work, standing)
                        .map_err(|error| {
                            crate::storage::ObjectFailure::new(error.into(), receipt.work)
                        })?;
                    receipt.work.verify(budget).map_err(|error| {
                        crate::storage::ObjectFailure::new(error.into(), receipt.work)
                    })?;
                    Ok(receipt)
                }
                Err(failure) => match WorkCounters::default().with_backend(*failure.work, standing)
                {
                    Ok(work) => Err(crate::storage::ObjectFailure::new(failure.error, work)),
                    Err(error) => Err(crate::storage::ObjectFailure::new(
                        error.into(),
                        *failure.work,
                    )),
                },
            }
        }
    }
}

impl<T: AsyncObjectStore + ?Sized> AsyncObjectStore for Arc<T> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        (**self).put(object_id, bytes, context).await
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        (**self).put_hashed(object, context).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        (**self).put_many(writes, context).await
    }
    async fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        (**self).put_many_hashed(objects, context).await
    }
    async fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, crate::ObjectStoreError> {
        (**self).flush_before_publish(_scope, context).await
    }
    async fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        (**self)
            .prepare_generation_publication(_request, _proof, _append, _scope, context)
            .await
    }
    async fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, crate::ObjectStoreError> {
        (**self)
            .historical_generation_evidence(
                _authority_id,
                _record,
                _maximum_bytes,
                _maximum_objects,
                context,
            )
            .await
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        (**self).read(object_id, maximum_bytes, context).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        (**self).read_many(requests, context).await
    }
    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        (**self).contains(object_id, context).await
    }
    fn decoded_cache_get(
        &self,
        _key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_get(_key)
    }

    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_admit(_key, value)
    }

    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        (**self).collection()
    }
    fn collection_sweeps(&self) -> u64 {
        (**self).collection_sweeps()
    }
    fn read_with_custody(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        observation: Option<&crate::kernel::ProofDataCustody>,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask {
        (**self).read_with_custody(object_id, maximum_bytes, budget, cancellation, observation)
    }
}
