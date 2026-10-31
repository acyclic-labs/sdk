use super::*;

impl<S: AsyncObjectStore> AsyncObjectStore for StagedObjects<S> {
    fn requires_child_first_publication(&self) -> bool {
        self.inner.requires_child_first_publication()
    }
    fn maximum_reference_bytes(&self) -> usize {
        self.inner.maximum_reference_bytes()
    }
    async fn prepare_hashed(
        &self,
        object: crate::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::HashedObject, ObjectStoreError> {
        self.inner.prepare_hashed(object, context).await
    }

    async fn put_stored(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        if id.storage.is_none() {
            return self.put(id, bytes, work).await;
        }
        let unswept = self.collection.unswept().await;
        self.collection.writing(&unswept, [id]);
        self.inner.put_stored(id, bytes, work).await
    }
    async fn read_stored(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if id.storage.is_none() {
            return self.read(id, maximum, work).await;
        }
        self.inner.read_stored(id, maximum, work).await
    }
    async fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let unstaged = {
            let index = self.index();
            index.objects.is_empty()
                || requests
                    .iter()
                    .all(|request| !index.objects.contains_key(&request.object_id))
        };
        if unstaged {
            return self.inner.read_many_stored(requests, work).await;
        }
        crate::async_storage::read_many_stored_sequential(self, requests, work).await
    }

    fn decoded_cache_lookup(
        &self,
        key: DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError> {
        use crate::async_storage::{DecodedCacheLookup, DecodedCacheMiss};
        let index = self.index();
        Ok(match index.objects.get(&key.object_id) {
            Some(Staged::Resident {
                decoded,
                incarnation,
                ..
            }) => match decoded.get().filter(|(cached, _)| *cached == key) {
                Some((_, value)) => DecodedCacheLookup::Hit(value.clone()),
                None => DecodedCacheLookup::Miss(DecodedCacheMiss::new(key, *incarnation, self)),
            },
            Some(Staged::Spilled { .. }) => {
                DecodedCacheLookup::Miss(DecodedCacheMiss::uncached(key))
            }
            None => {
                drop(index);
                return self.inner.decoded_cache_lookup(key);
            }
        })
    }
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let hashed = byte_length(&bytes);
        work.accounting()
            .charge(WorkCounters {
                bytes_hashed: hashed,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        let object =
            HashedObject::verify(object_id, bytes).ok_or(ObjectStoreError::DigestMismatch)?;
        self.put_hashed(object, work).await
    }
    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let native = work.original;
        if object.object_id().storage.is_some() || object.length() > MAXIMUM_DRAIN_BYTES {
            let future = async {
                let unswept = self.collection.unswept().await;
                self.collection.writing(&unswept, [object.object_id()]);
                self.inner.put_hashed(object, work).await
            };
            let allocation = frame_allocation(native, &future)?;
            let mut future = Box::pin(future);
            let result = future.as_mut().await;
            drop(future);
            drop(allocation);
            return result;
        }
        let overflowing = {
            let (object_id, bytes) = object.into_parts();
            let mut index = self.index_mut();
            index.admit_resident(object_id, bytes)? && index.resident_bytes > MAXIMUM_RESIDENT_BYTES
        };
        if !overflowing {
            return Ok(());
        }
        let future = self.relieve_window(work);
        let allocation = frame_allocation(native, &future)?;
        let mut future = Box::pin(future);
        let result = future.as_mut().await;
        drop(future);
        drop(allocation);
        result
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        if writes.is_empty() {
            return Err(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            ));
        }
        for write in writes {
            self.put(write.object_id, write.bytes.clone(), work).await?;
        }
        Ok(())
    }
    async fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        if objects.is_empty() {
            return Err(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            ));
        }
        for object in objects {
            self.put_hashed(object.clone(), work).await?;
        }
        Ok(())
    }
    async fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let native = work.original;
        let future = self.flush(scope, work);
        let allocation = frame_allocation(native, &future)?;
        let mut future = Box::pin(future);
        let result = future.as_mut().await;
        drop(future);
        drop(allocation);
        result
    }
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        Some(&self.collection)
    }
    async fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        if work.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner
            .historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                work,
            )
            .await
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        match self.lookup(object_id) {
            Lookup::Resident(bytes) => return resident_read(bytes, maximum_bytes, work),
            // Only spill offsets need protection against reuse by a drain.
            // Durable immutable reads must remain independent of a concurrent
            // publication, including synchronous native callbacks.
            Lookup::Absent => {
                let native = work.original;
                let future = self.inner.read(object_id, maximum_bytes, work);
                let allocation = frame_allocation(native, &future)?;
                let mut future = Box::pin(future);
                let result = future.as_mut().await;
                drop(future);
                drop(allocation);
                return result;
            }
            Lookup::Spilled { .. } => {}
        }
        let native = work.original;
        let future = self.read_private(object_id, maximum_bytes, work);
        let allocation = frame_allocation(native, &future)?;
        let mut future = Box::pin(future);
        let result = future.as_mut().await;
        drop(future);
        drop(allocation);
        result
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, work).await
    }
    async fn contains(
        &self,
        object_id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        if work.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        work.cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        if self.index().objects.contains_key(&object_id) {
            return Ok(true);
        }
        self.inner.contains(object_id, work).await
    }
}

impl<S: AsyncObjectStore> crate::async_storage::DecodedCacheOrigin for StagedObjects<S> {
    fn admit(
        &self,
        key: DecodedCacheKey,
        epoch: u64,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        let index = self.index();
        Ok(match index.objects.get(&key.object_id) {
            Some(Staged::Resident {
                decoded,
                incarnation,
                ..
            }) if *incarnation == epoch => {
                let (cached, shared) = decoded.get_or_init(|| (key, value.clone()));
                if *cached == key {
                    DecodedCacheAdmission::Shared(shared.clone())
                } else {
                    DecodedCacheAdmission::Uncached(value)
                }
            }
            _ => DecodedCacheAdmission::Uncached(value),
        })
    }
}
