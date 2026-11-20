//! Owned callback input, admitted in full before a source can observe it.
use crate::NativeMaterializationWork;
use crate::kernel::OwnedSourceChunk;
use crate::ledger::NativeResourceCredit::Copy;
use crate::native_mount::NativeMountError;
use acyclic_stream::OwnedBytes;

pub(in crate::native_mount) fn source_chunk(
    bytes: &[u8],
    work: &NativeMaterializationWork,
) -> Result<OwnedSourceChunk, NativeMountError> {
    work.admit(Copy.delta(bytes.len() as u64))?;
    if bytes.is_empty() {
        return Ok(OwnedSourceChunk::intrinsic(OwnedBytes::new(), 0));
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
    Ok(OwnedSourceChunk::intrinsic(bytes, retained))
}
