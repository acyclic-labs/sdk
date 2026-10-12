use super::*;

fn accounting<'a>(
    context: &'a crate::AuthorityWork<'_>,
) -> Result<KernelAccounting<'a>, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = context.original {
        return Ok(KernelAccounting::native(original, context.cancellation));
    }
    Ok(KernelAccounting::standalone(
        context.work.remaining(context.budget)?,
        context.cancellation,
    ))
}

fn finish<T>(
    context: &mut crate::AuthorityWork<'_>,
    result: Result<T, crate::FsError>,
    spent: WorkCounters,
) -> Result<T, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if context.original.is_some() {
        return result.map_err(crate::async_storage::object_error);
    }
    context.work = context.work.checked_add(spent)?;
    result.map_err(crate::async_storage::object_error)
}

impl<P: ObjectsProvider> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let mut work = accounting(context)?;
        let result = self.put_accounted(id, bytes, false, &mut work).await;
        let spent = work.spent();
        drop(work);
        finish(context, result, spent)
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let mut work = accounting(context)?;
        let (id, bytes) = object.into_parts();
        let result = self.put_accounted(id, bytes, true, &mut work).await;
        let spent = work.spent();
        drop(work);
        finish(context, result, spent)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let mut work = accounting(context)?;
        let result = self.put_many_accounted(writes, &mut work).await;
        let spent = work.spent();
        drop(work);
        finish(context, result, spent)
    }

    async fn flush_before_publish(
        &self,
        _scope: crate::PublicationScope<'_>,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            context
                .cancellation
                .check()
                .map_err(|_| ObjectStoreError::Cancelled)?;
            original.admit(WorkCounters::UNCHARGED)?;
        }
        Ok(crate::PublicationHold::none())
    }

    async fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        Ok(())
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let mut work = accounting(context)?;
        let result = self
            .read_accounted(id, maximum, &mut work)
            .await
            .map(|(value, _allocation)| value);
        let spent = work.spent();
        drop(work);
        let read = finish(context, result, spent)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            if matches!(read.retention, ObjectReadRetention::Owned { .. }) {
                return crate::async_storage::owned_read(read, original);
            }
        }
        Ok(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let mut work = accounting(context)?;
        let result = self.read_many_accounted(requests, &mut work).await;
        let spent = work.spent();
        drop(work);
        let (reads, allocation) = finish(context, result, spent)?;
        match allocation {
            KernelOwnedAllocation::Standalone => Ok((reads, None)),
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                crate::async_storage::native_read_batch(reads, allocation)
                    .map(|(reads, allocation)| (reads, Some(allocation)))
            }
        }
    }

    async fn contains(
        &self,
        id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        let maximum = context.work.remaining(context.budget)?.object_bytes_read;
        match self.read(id, maximum, context).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
