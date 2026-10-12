use super::*;

pub(super) fn service_read_error(error: Error) -> ObjectStoreError {
    match error.code {
        wire::ErrorCode::NotFound => ObjectStoreError::Missing,
        _ => ObjectStoreError::LogicalObjects(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_objects_error(error: acyclic_objects::v1::NativeObjectsError) -> ObjectStoreError {
    use acyclic_objects::v1::NativeObjectsCause;
    match &error.cause {
        NativeObjectsCause::Service(service) if !error.has_payload() => service_read_error(*service),
        NativeObjectsCause::Unsupported if !error.has_payload() => ObjectStoreError::NativeUnavailable,
        _ => ObjectStoreError::NativeObjects(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_completed_read_bytes(error: &acyclic_objects::v1::NativeObjectsError) -> Result<u64, WorkError> {
    use acyclic_objects::v1::NativeObjectsCause;
    if !error.has_payload() {
        return match &error.cause {
            NativeObjectsCause::SharedFailure(failure) => native_completed_read_bytes(failure.error()),
            _ => Ok(0),
        };
    }
    [
        error.object.as_ref().map_or(0, |value| value.body().len() as u64),
        error.secondary_object.as_ref().map_or(0, |value| value.body().len() as u64),
        error.transport_chunk.as_ref().map_or(0, |bytes| bytes.len() as u64),
    ].into_iter().try_fold(0_u64, |total, bytes| total.checked_add(bytes).ok_or(WorkError::Overflow))
}

pub(super) fn read_retention(bytes: &OwnedBytes, _accounting: &KernelAccounting<'_>) -> ObjectReadRetention {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = _accounting.native_work() {
        if bytes.matches_meter(&work.runtime_meter()) {
            if let Some(capacity) = bytes.retained_capacity() {
                return ObjectReadRetention::Owned { logical_bytes: capacity as u64 };
            }
        }
        return ObjectReadRetention::Shared;
    }
    ObjectReadRetention::Owned { logical_bytes: bytes.len() as u64 }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_error_code(error: &acyclic_objects::v1::NativeObjectsError) -> Option<wire::ErrorCode> {
    use acyclic_objects::v1::NativeObjectsCause;
    match &error.cause {
        NativeObjectsCause::Service(error) => Some(error.code),
        NativeObjectsCause::SharedFailure(error) => native_error_code(error.error()),
        _ => None,
    }
}

pub(super) fn is_precondition(error: &ObjectStoreError) -> bool {
    match error {
        ObjectStoreError::LogicalObjects(error) => error.code == wire::ErrorCode::PreconditionFailed,
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::NativeObjects(error) => native_error_code(error) == Some(wire::ErrorCode::PreconditionFailed),
        _ => false,
    }
}

pub(super) fn read_header_bytes(header: &wire::GetObjectHeader) -> Result<u64, crate::FsError> {
    let Some(info) = &header.object else { return Ok(0) };
    let mut bytes = info.etag.len() as u64;
    if let Some(metadata) = &info.metadata {
        for value in [&metadata.content_type, &metadata.content_encoding,
            &metadata.cache_control, &metadata.content_disposition, &metadata.content_language] {
            bytes = bytes.checked_add(value.len() as u64).ok_or(WorkError::Overflow)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes.checked_add(size_of::<(String, String)>() as u64)
                .and_then(|bytes| bytes.checked_add(key.len() as u64))
                .and_then(|bytes| bytes.checked_add(value.len() as u64))
                .ok_or(WorkError::Overflow)?;
        }
    }
    Ok(bytes)
}
