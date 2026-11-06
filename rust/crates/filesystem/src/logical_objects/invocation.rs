//! Provider calls borrow the same accounting that admitted their requests.
use super::*;

// Admission/handoff failures stop before outcome accounting. A completed read's
// outcome stays separate so the caller records any payload carried by an error.
pub(super) async fn get<P: Blobs + ?Sized>(
    provider: &P,
    request: crate::kernel::Owned<GetRequest>,
    work: &mut KernelAccounting<'_>,
) -> Result<Result<Object, ObjectStoreError>, crate::FsError> {
    match work {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work: original, .. } => {
            let original = *original;
            let future = provider
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
            let result = provider
                .get(digest, range, maximum)
                .await
                .map_err(service_error);
            release(work, request.allocation, request.bytes)?;
            Ok(result)
        }
    }
}

pub(super) async fn put<P: Blobs + ?Sized>(
    provider: &P,
    request: crate::kernel::Owned<PutRequest>,
    work: &mut KernelAccounting<'_>,
) -> Result<Put, crate::FsError> {
    match work {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work: original, .. } => {
            let original = *original;
            let receipt = provider
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
            let result = provider.put_sealed(body, references).await;
            release(work, request.allocation, request.bytes)?;
            result.map_err(|error| ObjectStoreError::LogicalObjects(error).into())
        }
    }
}

pub(super) async fn retain<P: Blobs + ?Sized>(
    provider: &P,
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
            provider
                .retain_native(request, original.runtime_meter())
                .map_err(|error| native_error(error, original))?
                .await
                .map_err(|error| native_error(error, original))?;
        }
        _ => {
            work.charge(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let retained = provider
                .retain(&root.value, std::mem::take(&mut references.value))
                .await;
            drop(root);
            drop(references);
            retained.map_err(ObjectStoreError::LogicalObjects)?;
        }
    }
    Ok(())
}

pub(super) async fn contains<P: Blobs + ?Sized>(
    provider: &P,
    digest: acyclic_objects::Digest,
    work: &mut KernelAccounting<'_>,
) -> Result<bool, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let KernelAccounting::NativeOriginal { work: original, .. } = work {
        let original = *original;
        let request = crate::kernel::Owned::new(digest, guard(work)?, 0);
        let future = provider
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
    match provider.head(digest).await {
        Ok(_) => Ok(true),
        Err(error) if error.code == wire::ErrorCode::NotFound => Ok(false),
        Err(error) => Err(ObjectStoreError::LogicalObjects(error).into()),
    }
}
