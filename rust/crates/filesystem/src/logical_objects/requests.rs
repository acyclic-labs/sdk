use super::*;

pub(super) type PutRequest = (wire::PutObjectHeader, OwnedBytes);

pub(super) type GetRequest = (wire::GetObjectRequest, u64);

pub(super) fn put_request_bytes(request: &PutRequest) -> u64 {
    request.0.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity() as u64)
        + request.0.object_key.capacity() as u64
        + request.0.metadata.as_ref().map_or(0, |metadata| metadata.content_type.capacity() as u64)
}

pub(super) fn put_request(
    bucket: &wire::BucketRef, object_id: ObjectId, bytes: OwnedBytes,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(PutRequest, KernelOwnedAllocation), crate::FsError> {
    let mut request_custody;
    let ((request, _), custody) = read_request(bucket, object_id, 0, accounting)?;
    request_custody = custody;
    let content_type = "application/vnd.acyclic.fs-object-v1";
    let copy = WorkCounters { bytes_copied: content_type.len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(copy)?;
    let mut metadata_custody = accounting.claim_owned(content_type.len() as u64, 1)?;
    let mut owned_type = String::new();
    owned_type.try_reserve_exact(content_type.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    accounting.reconcile_owned(&mut metadata_custody, content_type.len() as u64, owned_type.capacity() as u64)?;
    owned_type.push_str(content_type);
    accounting.record_completed(copy)?;
    request_custody.absorb(&mut metadata_custody)?;
    Ok(((wire::PutObjectHeader {
        bucket: request.bucket, object_key: request.object_key,
        metadata: Some(wire::ObjectMetadata { content_type: owned_type, ..Default::default() }),
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfAbsent(true)),
        }),
        mutation: None,
    }, bytes), request_custody))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_request<T>(
    data: T, custody: KernelOwnedAllocation, work: &crate::NativeMaterializationWork,
) -> Result<acyclic_objects::v1::ObjectsNativeRequest<T>, crate::FsError> {
    match (data, custody) {
        (data, KernelOwnedAllocation::NativeOriginal(custody)) if custody.work().shares_allocation_context(work) =>
            acyclic_objects::v1::ObjectsNativeRequest::new(data, custody, &work.runtime_meter())
                .map_err(|error| native_objects_error(error).into()),
        (data, custody) => {
            drop(data);
            drop(custody);
            Err(ObjectStoreError::Corrupt.into())
        }
    }
}

pub(super) fn release_read_custody(
    accounting: &mut KernelAccounting<'_>, mut custody: KernelOwnedAllocation, bytes: u64,
) -> Result<(), crate::FsError> {
    accounting.release_owned(&mut custody, bytes)?;
    drop(custody);
    Ok(())
}

pub(super) fn read_request(
    bucket: &wire::BucketRef, object_id: ObjectId, maximum_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(GetRequest, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    let bucket_bytes = bucket.name.len() as u64;
    let bucket_copy = WorkCounters { bytes_copied: bucket_bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(bucket_copy)?;
    let mut custody = accounting.claim_owned(bucket_bytes, u64::from(bucket_bytes != 0))?;
    let bucket = bucket.clone();
    let copied = accounting.record_completed(bucket_copy);
    let reconciled = accounting.reconcile_owned(
        &mut custody, bucket_bytes, bucket.name.capacity() as u64);
    copied?;
    reconciled?;
    let family = object_id.kind.canonical_tag();
    let key_bytes = "fs/v1/".len() + family.ilog10() as usize + 1 + "/".len() + 64;
    let key_copy = WorkCounters { bytes_copied: key_bytes as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(key_copy)?;
    let mut key_custody = accounting.claim_owned(key_bytes as u64, 1)?;
    let mut key = String::new();
    key.try_reserve_exact(key_bytes).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    accounting.reconcile_owned(&mut key_custody, key_bytes as u64, key.capacity() as u64)?;
    let mut digest = [0_u8; 64];
    hex::encode_to_slice(object_id.digest.as_bytes(), &mut digest).map_err(|_| ObjectStoreError::Corrupt)?;
    let digest = std::str::from_utf8(&digest).map_err(|_| ObjectStoreError::Corrupt)?;
    write!(&mut key, "fs/v1/{family}/{digest}")
        .map_err(|_| ObjectStoreError::Corrupt)?;
    accounting.record_completed(key_copy)?;
    custody.absorb(&mut key_custody)?;
    Ok(((wire::GetObjectRequest {
        bucket: Some(bucket), object_key: key, ..Default::default()
    }, maximum_bytes), custody))
}
