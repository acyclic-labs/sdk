use super::*;

impl<S: AsyncObjectStore> AsyncObjectStore for CachedObjectStore<S> {
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
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_stored(id, bytes, context).await
    }
    async fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.inner.read_many_stored(requests, context).await
    }

    async fn read_stored(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, ObjectStoreError> {
        self.inner.read_stored(id, maximum, context).await
    }

    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.decoded_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.decoded_admit(key, value)
    }

    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        self.inner.collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner.collection_sweeps()
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner
            .historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                context,
            )
            .await
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let retained = bytes.clone();
        let epoch = self.shared.fill_epoch();
        self.inner.put(object_id, bytes, context).await?;
        self.insert(object_id, retained, epoch)?;
        Ok(())
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let object_id = object.object_id();
        let retained = object.bytes().clone();
        let epoch = self.shared.fill_epoch();
        self.inner.put_hashed(object, context).await?;
        self.insert(object_id, retained, epoch)?;
        Ok(())
    }

    async fn flush_before_publish(
        &self,
        scope: crate::async_storage::PublicationScope<'_>,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        self.inner.flush_before_publish(scope, context).await
    }

    async fn put_many_hashed(
        &self,
        objects: &[crate::HashedObject],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let epoch = self.shared.fill_epoch();
        self.inner.put_many_hashed(objects, context).await?;
        for object in objects {
            self.insert(object.object_id(), object.bytes().clone(), epoch)?;
        }
        Ok(())
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let epoch = self.shared.fill_epoch();
        self.inner.put_many(writes, context).await?;
        for write in writes {
            self.insert(write.object_id, write.bytes.clone(), epoch)?;
        }
        Ok(())
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let mut work = crate::AuthorityWork::new(context.read_budget()?, context.cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        {
            work.original = context.original;
        }
        let result = async {
            loop {
                work.accounting()
                    .charge(cache_probe_work())
                    .map_err(crate::async_storage::object_error)?;
                match self.read_plan(
                    object_id,
                    #[cfg(not(target_arch = "wasm32"))]
                    work.original,
                )? {
                    ReadPlan::Hit(bytes) => return shared_read(bytes, maximum_bytes),
                    ReadPlan::Leader(flight) => {
                        return self
                            .load_leader(object_id, maximum_bytes, flight, &mut work)
                            .await;
                    }
                    ReadPlan::Follower(flight) => {
                        let result = wait_for_flight(
                            &flight,
                            work.cancellation,
                            #[cfg(not(target_arch = "wasm32"))]
                            work.original,
                        )
                        .await?;
                        self.record_coalesced()?;
                        if let Some(bytes) = result {
                            return shared_read(bytes, maximum_bytes);
                        }
                        self.remove_flight(object_id, &flight)?;
                    }
                }
            }
        }
        .await;
        context.accept(work.receipt(result))
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let mut work = crate::AuthorityWork::new(
            context.work.remaining(context.budget)?,
            context.cancellation,
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            work.original = context.original;
        }
        let result = async {
            let mut prepared = self.prepare_batch(requests, &mut work)?;
            let leaders = self.load_batch_leaders(&prepared, &mut work).await?;
            let mut values = Owned::allocate(requests.len(), &mut work.accounting())
                .map_err(crate::async_storage::object_error)?;
            for (request, slot) in requests
                .iter()
                .zip(std::mem::take(&mut prepared.slots.value))
            {
                work.accounting()
                    .check()
                    .map_err(crate::async_storage::object_error)?;
                let read = self
                    .resolve_batch_slot(request, slot, &leaders.0, &mut work)
                    .await?;
                values.value.push(read);
                work.accounting()
                    .record_completed(WorkCounters {
                        items_returned: 1,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(crate::async_storage::object_error)?;
            }
            let allocation = match values.allocation {
                KernelOwnedAllocation::Standalone => None,
                #[cfg(not(target_arch = "wasm32"))]
                KernelOwnedAllocation::NativeOriginal(allocation) => Some(allocation),
            };
            Ok((values.value, allocation))
        }
        .await;
        context.accept(work.receipt(result))
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        context
            .accounting()
            .charge(cache_probe_work())
            .map_err(crate::async_storage::object_error)?;
        if self.cached_contains(object_id)? {
            return Ok(true);
        }
        self.inner.contains(object_id, context).await
    }
}
