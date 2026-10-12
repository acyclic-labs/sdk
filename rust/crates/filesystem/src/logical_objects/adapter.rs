use super::*;

impl<P: ObjectsProvider> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_accounted(object_id, bytes, false, &mut accounting).await {
            Ok(()) => Ok(crate::OperationReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(crate::async_storage::object_error(error), accounting.spent())),
        }
    }

    async fn put_hashed(
        &self, object: crate::storage::HashedObject, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let (object_id, bytes) = object.into_parts();
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_accounted(object_id, bytes, true, &mut accounting).await {
            Ok(()) => Ok(crate::OperationReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(crate::async_storage::object_error(error), accounting.spent())),
        }
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_many_accounted(writes, &mut accounting).await {
            Ok(()) => Ok(crate::OperationReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(crate::async_storage::object_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_accounted(object_id, bytes, false, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(crate::async_storage::object_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        self.put_accounted(object_id, bytes, true, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(crate::async_storage::object_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite], work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_many_accounted(writes, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(crate::async_storage::object_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, _scope: crate::async_storage::PublicationScope<'_>, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(crate::PublicationHold::none())
    }

    /// Like the ordinary PR237 preparation default, this local provider supplies no hosted or historical custody.
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof, _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(())
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.read_accounted(object_id, maximum_bytes, &mut accounting).await {
            Ok((value, _custody)) => Ok(crate::OperationReceipt { value, work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(crate::async_storage::object_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::storage::ObjectRead, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (read, _custody) = self.read_accounted(object_id, maximum_bytes, &mut accounting)
            .await.map_err(crate::async_storage::object_error)?;
        match read.retention {
            ObjectReadRetention::Shared => Ok(read),
            ObjectReadRetention::Owned { .. } => crate::async_storage::owned_read(read, work),
        }
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.read_many_accounted(requests, &mut accounting).await {
            Ok((value, _custody)) => Ok(crate::OperationReceipt { value, work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(crate::async_storage::object_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self,
        requests: &[ObjectReadRequest],
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<crate::storage::ObjectRead>, crate::materializer::NativeAllocation), ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (reads, custody) = self.read_many_accounted(requests, &mut accounting)
            .await.map_err(crate::async_storage::object_error)?;
        match custody {
            KernelOwnedAllocation::NativeOriginal(custody) =>
                crate::async_storage::native_read_batch(reads, custody),
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
    }

    async fn contains(
        &self, object_id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        match self.read(object_id, budget.object_bytes_read, budget, cancellation).await {
            Ok(receipt) => Ok(crate::OperationReceipt { value: true, work: receipt.work }),
            Err(failure) if matches!(failure.error, ObjectStoreError::Missing) =>
                Ok(crate::OperationReceipt { value: false, work: *failure.work }),
            Err(failure) => Err(failure),
        }
    }

}
