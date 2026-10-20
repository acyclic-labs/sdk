use super::*;

impl<S: AsyncObjectStore> AsyncObjectStore for StagedObjects<S> {
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        match self.index().objects.get(&key.object_id) {
            Some(Staged::Resident { decoded, .. }) => Ok(decoded
                .get()
                .filter(|(cached, _)| *cached == key)
                .map(|(_, value)| value.clone())),
            Some(Staged::Spilled { .. }) => Ok(None),
            None => self.inner.decoded_cache_get(key),
        }
    }
    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        match self.index().objects.get(&key.object_id) {
            Some(Staged::Resident { decoded, .. }) => {
                let (cached, shared) = decoded.get_or_init(|| (key, value.clone()));
                Ok(if *cached == key {
                    DecodedCacheAdmission::Shared(shared.clone())
                } else {
                    DecodedCacheAdmission::Uncached(value)
                })
            }
            Some(Staged::Spilled { .. }) => Ok(DecodedCacheAdmission::Uncached(value)),
            None => self.inner.decoded_cache_admit(key, value),
        }
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
        let hashed = byte_length(&bytes)
            .checked_add(if work.is_native() {
                crate::OBJECT_DIGEST_ENVELOPE_BYTES
            } else {
                0
            })
            .ok_or(crate::WorkError::Overflow)?;
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
        if object.length() > MAXIMUM_DRAIN_BYTES {
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
            index.admit_resident(object_id, bytes) && index.resident_bytes > MAXIMUM_RESIDENT_BYTES
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
        if work.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
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
            Lookup::Absent => return self.inner.read(object_id, maximum_bytes, work).await,
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
