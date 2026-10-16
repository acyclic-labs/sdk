//! Ordinary files have no upload receipt. Authenticate their descriptor with
//! bounded streaming reads; never allocate or slice a whole-file buffer.
use super::*;
use sha2::{Digest as _, Sha256};

pub(super) async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    selected: &acyclic_fs::Generation<A, O>,
    reference: &FileRef,
    path: &str,
    io: &PreparationIo,
) -> Result<()> {
    let stat = io.sdk(selected.stat(path, io, io.cancellation()).await)?;
    let length = reference.descriptor().byte_length();
    if stat.kind != acyclic_fs::kernel::FileKind::Regular || stat.logical_bytes != Some(length) {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "range file length differs from its descriptor",
        ));
    }
    let mut digest = Sha256::new();
    let mut offset = 0;
    while offset < length {
        let hold;
        let chunk = io.sdk(
            selected
                .read_range(
                    path,
                    offset,
                    (length - offset).min(64 * 1024),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        if io.original_native_work().is_none() {
            io.retain_owned(chunk.bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        hold = io.guard_bytes(&chunk.bytes)?;
        if chunk.bytes.len() as u64 != (length - offset).min(64 * 1024) {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "range authentication did not make exact progress",
            ));
        }
        io.charge(WorkCounters {
            bytes_hashed: chunk.bytes.len() as u64,
            ..Default::default()
        })?;
        digest.update(chunk.bytes.as_ref());
        offset += chunk.bytes.len() as u64;
        drop(chunk);
        drop(hold);
    }
    if digest.finalize().as_slice() != reference.descriptor().sha256() {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "range file content differs from its descriptor",
        ));
    }
    Ok(())
}
