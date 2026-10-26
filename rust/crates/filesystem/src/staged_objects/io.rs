use super::*;

pub(super) fn staged_vector<T>(
    capacity: usize,
    native: Option<&NativeMaterializationWork>,
) -> Result<(Option<NativeAllocation>, Vec<T>), ObjectStoreError> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let mut control = native
        .map(|work| {
            if bytes == 0 {
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)
            } else {
                work.claim(
                    bytes as u64,
                    u64::from((bytes as u64) != 0),
                    crate::WorkCounters::UNCHARGED,
                )
            }
        })
        .transpose()?;
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|error| {
        ObjectStoreError::Native(NativeTreeMaterializationError::AllocationFailed(error))
    })?;
    if let Some(control) = &mut control {
        let actual = values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)?;
        control.reconcile_capacity(actual as u64)?;
    }
    Ok((control, values))
}

pub(super) fn staged_owned_bytes(
    bytes: Vec<u8>,
    allocation: NativeAllocation,
    native: &NativeMaterializationWork,
) -> Result<OwnedBytes, ObjectStoreError> {
    OwnedBytes::from_vec_with_guard(bytes, allocation, native.runtime_meter()).map_err(|failure| {
        match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                native.runtime_error(error).into()
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)
            }
        }
    })
}

pub(super) async fn staged_native_read(
    file: &NativeFile,
    offset: u64,
    length: usize,
    native: &NativeMaterializationWork,
) -> Result<OwnedBytes, ObjectStoreError> {
    if length == 0 {
        native
            .runtime_meter()
            .check()
            .map_err(|error| native.runtime_error(error))?;
        return Ok(OwnedBytes::new());
    }
    let (allocation, mut bytes) = staged_vector::<u8>(length, Some(native))?;
    bytes.resize(bytes.capacity(), 0);
    let prepared = acyclic_stream::PreparedOwnedBytes::new(
        bytes,
        allocation.expect("native read buffer is funded"),
        &native.runtime_meter(),
    )
    .map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
            native.runtime_error(error).into()
        }
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
            ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)
        }
    })?;
    let (prepared, filled) = file
        .read_prepared_bounded_async(offset, prepared, length, false, native.runtime_meter())
        .map_err(|error| native.runtime_error(error))?
        .await
        .map_err(|error| native.runtime_error(error))?;
    prepared
        .freeze(filled)
        .map_err(|_| ObjectStoreError::Corrupt)
}

pub(super) async fn read_spilled(
    spill: &NativeFile,
    spilled: &[(ObjectId, u64, u64)],
) -> Result<Vec<HashedObject>, ObjectStoreError> {
    let reads = spilled
        .iter()
        .map(|&(_, offset, length)| {
            usize::try_from(length)
                .map(|length| OwnedRead { offset, length })
                .map_err(|_| ObjectStoreError::Corrupt)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = spill.read_batch_async(reads).await?;
    if bytes.len() != spilled.len() {
        return Err(ObjectStoreError::Corrupt);
    }
    spilled
        .iter()
        .zip(bytes)
        .map(|(&(object_id, _, _), bytes)| {
            HashedObject::verify(object_id, bytes.into()).ok_or(ObjectStoreError::Corrupt)
        })
        .collect()
}
