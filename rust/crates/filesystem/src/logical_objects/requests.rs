use super::*;
pub(super) type GetRequest = (wire::GetObjectRequest, u64);
pub(super) type PutRequest = (wire::PutObjectHeader, OwnedBytes);

pub(super) fn read_request(
    bucket: &wire::BucketRef,
    id: ObjectId,
    maximum: u64,
    work: &mut KernelAccounting<'_>,
) -> Result<(GetRequest, KernelOwnedAllocation), crate::FsError> {
    let family = id.kind.canonical_tag();
    let key_bytes = 7 + family.ilog10() as usize + 1 + 64;
    let bytes = bucket.name.len() as u64 + key_bytes as u64;
    let copy = WorkCounters {
        bytes_copied: bytes,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copy)?;
    let mut allocation = work.claim_owned(bytes, 2)?;
    let mut key = String::new();
    key.try_reserve_exact(key_bytes)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    let mut digest = [0; 64];
    hex::encode_to_slice(id.digest.as_bytes(), &mut digest)
        .map_err(|_| ObjectStoreError::Corrupt)?;
    write!(
        &mut key,
        "fs/v1/{family}/{}",
        std::str::from_utf8(&digest).map_err(|_| ObjectStoreError::Corrupt)?
    )
    .map_err(|_| ObjectStoreError::Corrupt)?;
    let bucket = bucket.clone();
    work.record_completed(copy)?;
    work.reconcile_owned(
        &mut allocation,
        bytes,
        key.capacity() as u64 + bucket.name.capacity() as u64,
    )?;
    Ok((
        (
            wire::GetObjectRequest {
                bucket: Some(bucket),
                object_key: key,
                ..Default::default()
            },
            maximum,
        ),
        allocation,
    ))
}
pub(super) fn put_request(
    bucket: &wire::BucketRef,
    id: ObjectId,
    body: OwnedBytes,
    work: &mut KernelAccounting<'_>,
) -> Result<(PutRequest, KernelOwnedAllocation), crate::FsError> {
    let ((request, _), mut allocation) = read_request(bucket, id, 0, work)?;
    let content_type = "application/vnd.acyclic.fs-object-v1";
    let copy = WorkCounters {
        bytes_copied: content_type.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copy)?;
    let mut metadata = work.claim_owned(content_type.len() as u64, 1)?;
    let content_type = content_type.to_owned();
    work.record_completed(copy)?;
    work.reconcile_owned(
        &mut metadata,
        content_type.len() as u64,
        content_type.capacity() as u64,
    )?;
    allocation.absorb(&mut metadata)?;
    Ok((
        (
            wire::PutObjectHeader {
                bucket: request.bucket,
                object_key: request.object_key,
                metadata: Some(wire::ObjectMetadata {
                    content_type,
                    ..Default::default()
                }),
                preconditions: Some(wire::Preconditions {
                    condition: Some(wire::preconditions::Condition::IfAbsent(true)),
                }),
                mutation: None,
            },
            body,
        ),
        allocation,
    ))
}
pub(super) fn vector<T>(
    length: usize,
    work: &mut KernelAccounting<'_>,
) -> Result<(Vec<T>, KernelOwnedAllocation), crate::FsError> {
    let bytes = length
        .checked_mul(size_of::<T>())
        .ok_or(WorkError::Overflow)? as u64;
    let mut allocation = work.claim_owned(bytes, 1)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    work.reconcile_owned(
        &mut allocation,
        bytes,
        (values.capacity() * size_of::<T>()) as u64,
    )?;
    Ok((values, allocation))
}
pub(super) fn release(
    work: &mut KernelAccounting<'_>,
    mut allocation: KernelOwnedAllocation,
    bytes: u64,
) -> Result<(), crate::FsError> {
    work.release_owned(&mut allocation, bytes)
}
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn packet<T>(
    data: T,
    allocation: KernelOwnedAllocation,
    work: &crate::NativeMaterializationWork,
) -> Result<acyclic_objects::v1::ObjectsNativeRequest<T>, crate::FsError> {
    match allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            acyclic_objects::v1::ObjectsNativeRequest::new(data, allocation, &work.runtime_meter())
                .map_err(|error| native_error(error).into())
        }
        _ => Err(ObjectStoreError::Corrupt.into()),
    }
}
pub(super) fn info_bytes(info: &wire::ObjectInfo) -> Result<u64, crate::FsError> {
    let mut bytes = info.etag.capacity() as u64;
    if let Some(metadata) = &info.metadata {
        for value in [
            &metadata.content_type,
            &metadata.content_encoding,
            &metadata.cache_control,
            &metadata.content_disposition,
            &metadata.content_language,
        ] {
            bytes = bytes
                .checked_add(value.capacity() as u64)
                .ok_or(WorkError::Overflow)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes
                .checked_add(size_of::<(String, String)>() as u64)
                .and_then(|bytes| bytes.checked_add(key.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(value.capacity() as u64))
                .ok_or(WorkError::Overflow)?;
        }
    }
    Ok(bytes)
}
pub(super) fn record_write(
    work: &mut KernelAccounting<'_>,
    bytes: u64,
) -> Result<(), crate::FsError> {
    let delta = WorkCounters {
        object_bytes_written: bytes,
        ..WorkCounters::UNCHARGED
    };
    match work {
        KernelAccounting::Standalone { work, .. } => {
            *work = work.checked_add(delta)?;
            Ok(())
        }
        KernelAccounting::BorrowedStandalone { work, .. } => {
            **work = work.checked_add(delta)?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            work.record_admitted(delta).map_err(Into::into)
        }
    }
}
