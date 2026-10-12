use super::*;

impl<P: ObjectsProvider> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                self.put_accounted(
                    object_id,
                    bytes,
                    false,
                    &mut KernelAccounting::native(work, cancellation),
                )
                .await
                .map_err(crate::async_storage::object_error)
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            match self
                .put_accounted(object_id, bytes, false, &mut accounting)
                .await
            {
                Ok(()) => Ok(crate::OperationReceipt {
                    value: (),
                    work: accounting.spent(),
                }),
                Err(error) => Err(OperationFailure::new(
                    crate::async_storage::object_error(error),
                    accounting.spent(),
                )),
            }
        }
        .await;
        context.accept(result)
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                let (object_id, bytes) = object.into_parts();
                self.put_accounted(
                    object_id,
                    bytes,
                    true,
                    &mut KernelAccounting::native(work, cancellation),
                )
                .await
                .map_err(crate::async_storage::object_error)
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let (object_id, bytes) = object.into_parts();
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            match self
                .put_accounted(object_id, bytes, true, &mut accounting)
                .await
            {
                Ok(()) => Ok(crate::OperationReceipt {
                    value: (),
                    work: accounting.spent(),
                }),
                Err(error) => Err(OperationFailure::new(
                    crate::async_storage::object_error(error),
                    accounting.spent(),
                )),
            }
        }
        .await;
        context.accept(result)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                self.put_many_accounted(writes, &mut KernelAccounting::native(work, cancellation))
                    .await
                    .map_err(crate::async_storage::object_error)
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            match self.put_many_accounted(writes, &mut accounting).await {
                Ok(()) => Ok(crate::OperationReceipt {
                    value: (),
                    work: accounting.spent(),
                }),
                Err(error) => Err(OperationFailure::new(
                    crate::async_storage::object_error(error),
                    accounting.spent(),
                )),
            }
        }
        .await;
        context.accept(result)
    }

    async fn flush_before_publish(
        &self,
        _scope: crate::async_storage::PublicationScope<'_>,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                cancellation
                    .check()
                    .map_err(|_| ObjectStoreError::Cancelled)?;
                work.admit(WorkCounters::UNCHARGED)?;
                Ok(crate::PublicationHold::none())
            }
            .await;
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
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                cancellation
                    .check()
                    .map_err(|_| ObjectStoreError::Cancelled)?;
                original.admit(WorkCounters::UNCHARGED)?;
                Ok(())
            }
            .await;
        }
        context
            .cancellation
            .check()
            .map_err(|_| crate::ObjectStoreError::Cancelled)?;
        Ok(())
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                let mut accounting = KernelAccounting::native(work, cancellation);
                let (read, _custody) = self
                    .read_accounted(object_id, maximum_bytes, &mut accounting)
                    .await
                    .map_err(crate::async_storage::object_error)?;
                match read.retention {
                    ObjectReadRetention::Shared => Ok(read),
                    ObjectReadRetention::Owned { .. } => {
                        crate::async_storage::owned_read(read, work)
                    }
                }
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            match self
                .read_accounted(object_id, maximum_bytes, &mut accounting)
                .await
            {
                Ok((value, _custody)) => Ok(crate::OperationReceipt {
                    value,
                    work: accounting.spent(),
                }),
                Err(error) => Err(OperationFailure::new(
                    crate::async_storage::object_error(error),
                    accounting.spent(),
                )),
            }
        }
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                let mut accounting = KernelAccounting::native(work, cancellation);
                let (reads, custody) = self
                    .read_many_accounted(requests, &mut accounting)
                    .await
                    .map_err(crate::async_storage::object_error)?;
                match custody {
                    KernelOwnedAllocation::NativeOriginal(custody) => {
                        crate::async_storage::native_read_batch(reads, custody)
                    }
                    KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
                }
            }
            .await
            .map(|(values, allocation)| (values, Some(allocation)));
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            match self.read_many_accounted(requests, &mut accounting).await {
                Ok((value, _custody)) => Ok(crate::OperationReceipt {
                    value,
                    work: accounting.spent(),
                }),
                Err(error) => Err(OperationFailure::new(
                    crate::async_storage::object_error(error),
                    accounting.spent(),
                )),
            }
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            match {
                let authority_input_1 = budget.object_bytes_read;

                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .read(object_id, authority_input_1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            } {
                Ok(receipt) => Ok(crate::OperationReceipt {
                    value: true,
                    work: receipt.work,
                }),
                Err(failure) if matches!(failure.error, ObjectStoreError::Missing) => {
                    Ok(crate::OperationReceipt {
                        value: false,
                        work: *failure.work,
                    })
                }
                Err(failure) => Err(failure),
            }
        }
        .await;
        context.accept(result)
    }
}
