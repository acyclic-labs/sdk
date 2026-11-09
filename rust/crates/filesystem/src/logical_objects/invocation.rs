//! Provider calls borrow the same accounting that admitted their requests.
use super::*;

/// Native reads admit their actual backing at the original physical producer.
/// Ordinary providers need bounded scratch before dispatch instead.
pub(super) fn read_capacity(work: &mut KernelAccounting<'_>) -> Result<u64, crate::FsError> {
    let remaining = work.remaining()?;
    #[cfg(not(target_arch = "wasm32"))]
    if work.native_work().is_some() {
        return Ok(remaining.object_bytes_read);
    }
    Ok(remaining
        .object_bytes_read
        .min(remaining.peak_allocation_bytes))
}

pub(super) fn read_custody(
    bytes: u64,
    work: &mut KernelAccounting<'_>,
) -> Result<KernelOwnedAllocation, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.native_work().is_some() {
        return Ok(KernelOwnedAllocation::Standalone);
    }
    work.claim_owned(bytes, 0)
}

/// Consumes a completed batch while its original result storage remains funded.
pub(super) trait BatchFinish<T>: acyclic_stream::ProviderTask {
    type Output;
    fn finish(
        self,
        results: impl ExactSizeIterator<Item = Result<T, ObjectStoreError>>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Self::Output, crate::FsError>;
}

/// One original-accounting boundary for logical immutable operations.
/// Implementations retain admitted request and output custody through their
/// physical producer; a withdrawn caller owns delivery, not physical lifetime.
pub(super) trait Provider: acyclic_stream::ProviderPlatform {
    fn get(
        &self,
        request: crate::kernel::Owned<GetRequest>,
        work: &mut KernelAccounting<'_>,
    ) -> impl std::future::Future<Output = Result<Result<Object, ObjectStoreError>, crate::FsError>>
    + acyclic_stream::ProviderTask;
    fn put(
        &self,
        request: crate::kernel::Owned<PutRequest>,
        work: &mut KernelAccounting<'_>,
    ) -> impl std::future::Future<Output = Result<Put, crate::FsError>> + acyclic_stream::ProviderTask;
    fn get_batch<F: BatchFinish<Object>>(
        &self,
        request: crate::kernel::Owned<Vec<GetRequest>>,
        work: &mut KernelAccounting<'_>,
        finish: F,
    ) -> impl std::future::Future<Output = Result<F::Output, crate::FsError>>
    + acyclic_stream::ProviderTask;
    fn put_batch<F: BatchFinish<Put>>(
        &self,
        request: crate::kernel::Owned<Vec<PutRequest>>,
        work: &mut KernelAccounting<'_>,
        finish: F,
    ) -> impl std::future::Future<Output = Result<F::Output, crate::FsError>>
    + acyclic_stream::ProviderTask;
    fn retain(
        &self,
        request: RetentionRequest,
        work: &mut KernelAccounting<'_>,
    ) -> impl std::future::Future<Output = Result<(), crate::FsError>> + acyclic_stream::ProviderTask;
    fn contains(
        &self,
        digest: acyclic_objects::Digest,
        work: &mut KernelAccounting<'_>,
    ) -> impl std::future::Future<Output = Result<bool, crate::FsError>> + acyclic_stream::ProviderTask;
}

impl<P: Blobs + ?Sized> Provider for P {
    // Admission/handoff failures stop before outcome accounting. A completed read's
    // outcome stays separate so the caller records any payload carried by an error.
    async fn get(
        &self,
        request: crate::kernel::Owned<GetRequest>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Result<Object, ObjectStoreError>, crate::FsError> {
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let future = self
                    .get_native(packet(request, original)?, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?;
                work.charge(WorkCounters {
                    object_probes: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                Ok(future.await.map_err(|error| native_error(error, original)))
            }
            _ => {
                work.charge(WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let (digest, range, maximum) = request.value;
                let result = Blobs::get(self, digest, range, maximum)
                    .await
                    .map_err(service_error);
                release(work, request.allocation, request.bytes)?;
                Ok(result)
            }
        }
    }

    async fn put(
        &self,
        request: crate::kernel::Owned<PutRequest>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Put, crate::FsError> {
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let receipt = self
                    .put_native(packet(request, original)?, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
                Ok(receipt.put().clone())
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let (body, references) = request.value;
                let result = self.put_sealed(body, references).await;
                release(work, request.allocation, request.bytes)?;
                result.map_err(|error| ObjectStoreError::LogicalObjects(error).into())
            }
        }
    }

    async fn get_batch<F: BatchFinish<Object>>(
        &self,
        request: crate::kernel::Owned<Vec<GetRequest>>,
        work: &mut KernelAccounting<'_>,
        finish: F,
    ) -> Result<F::Output, crate::FsError> {
        let count = request.value.len() as u64;
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let future = self
                    .get_batch_native(packet(request, original)?, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?;
                work.charge(WorkCounters {
                    object_probes: count,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = future
                    .await
                    .map_err(|error| native_error(error, original))?;
                finish.finish(
                    results.map(|value| value.map_err(|error| native_error(error, original))),
                    work,
                )
            }
            _ => {
                work.charge(WorkCounters {
                    object_probes: count,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = Blobs::get_batch(self, request.value).await;
                release(work, request.allocation, request.bytes)?;
                let bytes = (results.capacity() * size_of::<Result<Object, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(bytes)?;
                let result = finish.finish(
                    results
                        .into_iter()
                        .map(|value| value.map_err(service_error)),
                    work,
                );
                release(work, controls, bytes)?;
                let value = result?;
                late?;
                Ok(value)
            }
        }
    }

    async fn put_batch<F: BatchFinish<Put>>(
        &self,
        request: crate::kernel::Owned<Vec<PutRequest>>,
        work: &mut KernelAccounting<'_>,
        finish: F,
    ) -> Result<F::Output, crate::FsError> {
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let results = self
                    .put_batch_native(packet(request, original)?, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
                finish.finish(
                    results.map(|value| {
                        value
                            .map(|receipt| receipt.put().clone())
                            .map_err(|error| native_error(error, original))
                    }),
                    work,
                )
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.put_batch_sealed(request.value).await;
                release(work, request.allocation, request.bytes)?;
                let bytes = (results.capacity() * size_of::<Result<Put, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(bytes)?;
                let result = finish.finish(
                    results
                        .into_iter()
                        .map(|value| value.map_err(ObjectStoreError::LogicalObjects)),
                    work,
                );
                release(work, controls, bytes)?;
                let value = result?;
                late?;
                Ok(value)
            }
        }
    }

    async fn retain(
        &self,
        request: RetentionRequest,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let (root, mut references) = request;
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let request = acyclic_objects::ObjectsNativeRequest::new(
                    (root.value, references.value),
                    (root.allocation, references.allocation),
                    &original.runtime_meter(),
                )
                .map_err(|error| native_error(error, original))?;
                self.retain_native(request, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let retained =
                    Blobs::retain(self, &root.value, std::mem::take(&mut references.value)).await;
                drop(root);
                drop(references);
                retained.map_err(ObjectStoreError::LogicalObjects)?;
            }
        }
        Ok(())
    }

    async fn contains(
        &self,
        digest: acyclic_objects::Digest,
        work: &mut KernelAccounting<'_>,
    ) -> Result<bool, crate::FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelAccounting::NativeOriginal { work: original, .. } = work {
            let original = *original;
            let request = crate::kernel::Owned::new(digest, guard(work)?, 0);
            let future = self
                .contains_native(packet(request, original)?, original.runtime_meter())
                .map_err(|error| native_error(error, original))?;
            work.charge(WorkCounters {
                object_probes: 1,
                ..WorkCounters::UNCHARGED
            })?;
            return future
                .await
                .map_err(|error| native_error(error, original).into());
        }
        work.charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        match self.head(digest).await {
            Ok(_) => Ok(true),
            Err(error) if error.code == wire::ErrorCode::NotFound => Ok(false),
            Err(error) => Err(ObjectStoreError::LogicalObjects(error).into()),
        }
    }
}
