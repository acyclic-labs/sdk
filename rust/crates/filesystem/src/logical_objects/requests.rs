use super::*;
pub(super) type GetRequest = (acyclic_objects::Digest, ByteRange, u64);
pub(super) type PutRequest = (acyclic_stream::SealedBytes, Vec<acyclic_objects::Digest>);

/// Original custody for a request whose data owns no heap storage.
pub(super) fn guard(
    work: &mut KernelAccounting<'_>,
) -> Result<KernelOwnedAllocation, crate::FsError> {
    work.claim_owned(0, 0)
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
) -> Result<acyclic_objects::ObjectsNativeRequest<T>, crate::FsError> {
    match allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            acyclic_objects::ObjectsNativeRequest::new(data, allocation, &work.runtime_meter())
                .map_err(|error| native_error(error).into())
        }
        _ => Err(ObjectStoreError::Corrupt.into()),
    }
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
