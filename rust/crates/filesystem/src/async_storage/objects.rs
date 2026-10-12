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
    /// Returns one exact decoded immutable representation when resident.
    fn decoded_cache_get(
        &self,
        _key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        Ok(None)
    }

    /// Borrows the same immutable cache entry under the original native context.
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_get_with_native_work(
        &self,
        key: DecodedCacheKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        work.remaining()?;
        self.decoded_cache_get(key)
    }

    /// Offers one authenticated decoded representation to a disposable cache.
    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        Ok(DecodedCacheAdmission::Uncached(value))
    }

    /// Admits the same already guarded value without resetting native custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_admit_with_native_work(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        work.remaining()?;
        self.decoded_cache_admit(key, value)
    }

    /// Asynchronously admits one verified immutable object.
    fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask;

    /// Admits bytes using the caller's original native work and intrinsic custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits one object whose identity its construction already proved.
    fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        let (object_id, bytes) = object.into_parts();
        self.put(object_id, bytes, budget, cancellation)
    }

    /// Reuses the constructor's digest proof and physical byte owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self,
        _object: crate::storage::HashedObject,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of verified immutable objects.
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        let _ = (writes, budget, cancellation);
        async {
            Err(crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Rejected(
                    "object backend has no batch-write primitive".to_owned(),
                ),
            ))
        }
    }

    /// Uses one provider batch primitive without resetting the native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self,
        _writes: &[ObjectWrite],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of objects whose identities their construction already proved.
    fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        let writes = objects
            .iter()
            .map(|object| ObjectWrite {
                object_id: object.object_id(),
                bytes: object.bytes().clone(),
            })
            .collect::<Vec<_>>();
        async move { self.put_many(&writes, budget, cancellation).await }
    }

    /// Makes every admitted object in `scope` crash-durable before an authority record may reference it, and keeps it from collection until the returned hold drops, which the caller does after writing the record.
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<crate::PublicationHold>> + ProviderTask {
        async {
            Ok(crate::OperationReceipt {
                value: crate::PublicationHold::none(),
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Makes the closure durable under the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self,
        _scope: PublicationScope<'_>,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<crate::PublicationHold, crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }
    /// Prepares the original canonical append after its closure has been flushed.
    fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        async move {
            cancellation.check().map_err(|_| {
                crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Cancelled,
                )
            })?;
            Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Reads original protected proof evidence for exactly this settled record.
    fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>>> + ProviderTask {
        async move {
            cancellation.check().map_err(|_| {
                crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Cancelled,
                )
            })?;
            Ok(crate::OperationReceipt {
                value: None,
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Prepares those same original values under the caller's native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// The collection this store runs while it stays open, if it collects.
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        None
    }

    /// The count a proof records before it starts, for [`PublicationScope::Closure`].
    fn collection_sweeps(&self) -> u64 {
        self.collection()
            .map_or(0, |collection| collection.sweeps())
    }

    /// Asynchronously reads one complete bounded object.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask;

    /// Reads using the caller's original native budget, cancellation and journal.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ObjectRead, crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
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
            cancellation.check().map_err(|_| crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Cancelled,
            ))?;
            let standing = observation.map_or(0, |observation| observation.operation_cache_bytes());
            let mut remaining = budget;
            remaining.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(standing)
                .ok_or_else(|| crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes", observed: standing,
                        maximum: budget.peak_allocation_bytes,
                    }),
                ))?;
            let result = self.read(object_id, maximum_bytes, remaining, cancellation).await;
            let result = match observation {
                Some(observation) => observe_read(observation, result),
                None => result,
            };
            if standing == 0 {
                return result;
            }
            match result {
                Ok(mut receipt) => {
                    receipt.work = WorkCounters::default().with_backend(receipt.work, standing)
                        .map_err(|error| crate::storage::ObjectFailure::new(error.into(), receipt.work))?;
                    receipt.work.verify(budget)
                        .map_err(|error| crate::storage::ObjectFailure::new(error.into(), receipt.work))?;
                    Ok(receipt)
                }
                Err(failure) => match WorkCounters::default().with_backend(*failure.work, standing) {
                    Ok(work) => Err(crate::storage::ObjectFailure::new(failure.error, work)),
                    Err(error) => Err(crate::storage::ObjectFailure::new(error.into(), *failure.work)),
                },
            }
        }
    }

    /// Asynchronously reads an ordered batch of complete bounded objects.
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>> + ProviderTask;

    /// Reads the same actual backend frontier against the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(
        &self,
        _requests: &[ObjectReadRequest],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously probes one exact object identity.
    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>> + ProviderTask;
}



impl<T: AsyncObjectStore + ?Sized> AsyncObjectStore for Arc<T> {
    fn decoded_cache_get(
        &self,
        _key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_get(_key)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_get_with_native_work(
        &self,
        key: DecodedCacheKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_get_with_native_work(key, work)
    }
    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_admit(_key, value)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_admit_with_native_work(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        (**self).decoded_cache_admit_with_native_work(key, value, work)
    }
    fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        (**self).put(object_id, bytes, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).put_with_native_work(_object_id, _bytes, _work, _cancellation)
    }
    fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        (**self).put_hashed(object, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self,
        _object: crate::storage::HashedObject,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).put_hashed_with_native_work(_object, _work, _cancellation)
    }
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        (**self).put_many(writes, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self,
        _writes: &[ObjectWrite],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).put_many_with_native_work(_writes, _work, _cancellation)
    }
    fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        (**self).put_many_hashed(objects, budget, cancellation)
    }
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<crate::PublicationHold>> + ProviderTask {
        (**self).flush_before_publish(_scope, _budget, _cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self,
        _scope: PublicationScope<'_>,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<crate::PublicationHold, crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).flush_before_publish_with_native_work(_scope, _work, _cancellation)
    }
    fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        (**self).prepare_generation_publication(_request, _proof, _append, _scope, _budget, cancellation)
    }
    fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>>> + ProviderTask {
        (**self).historical_generation_evidence(_authority_id, _record, _maximum_bytes, _maximum_objects, _budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).prepare_generation_publication_with_native_work(_request, _proof, _append, _scope, _original, _cancellation)
    }
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        (**self).collection()
    }
    fn collection_sweeps(&self) -> u64 {
        (**self).collection_sweeps()
    }
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask {
        (**self).read(object_id, maximum_bytes, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ObjectRead, crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).read_with_native_work(_object_id, _maximum_bytes, _work, _cancellation)
    }
    #[doc(hidden)]
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
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>> + ProviderTask {
        (**self).read_many(requests, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(
        &self,
        _requests: &[ObjectReadRequest],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError>> + ProviderTask {
        (**self).read_many_with_native_work(_requests, _work, _cancellation)
    }
    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>> + ProviderTask {
        (**self).contains(object_id, budget, cancellation)
    }
}

