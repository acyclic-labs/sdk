use super::*;
use acyclic_stream::OwnedBytes;

pub(super) fn source_bytes(
    chunk: crate::kernel::OwnedSourceChunk,
    work: &crate::NativeMaterializationWork,
    use_bytes: impl FnOnce(&[u8]) -> Result<c_int, NativeMountError>,
) -> Result<c_int, NativeMountError> {
    let (bytes, capacity, intrinsic) = chunk.into_owned_parts();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| use_bytes(&bytes)));
    drop(bytes);
    if !intrinsic {
        work.state.lock().release(capacity)?;
    }
    match result {
        Ok(result) => result,
        Err(panic) => Err(NativeMountError::DriverPanicked {
            operation: "Darwin byte reply",
            panic: Mutex::new(panic),
        }),
    }
}

pub(super) fn copy_bytes(
    work: &crate::NativeMaterializationWork,
    bytes: &[u8],
) -> Result<crate::kernel::OwnedSourceChunk, NativeMountError> {
    work.admit(copy_work(bytes.len() as u64))?;
    if bytes.is_empty() {
        return Ok(crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::new(),
            0,
        ));
    }
    let (value, allocation) = work.copy_bytes(bytes)?;
    let retained = allocation.bytes();
    let bytes = OwnedBytes::from_vec_with_guard(value, allocation, work.runtime_meter()).map_err(
        |failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                NativeMountError::from(work.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                unreachable!("initialized vector")
            }
        },
    )?;
    Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, retained))
}
