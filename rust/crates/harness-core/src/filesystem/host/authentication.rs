//! Authenticate retained file descriptors with bounded streaming reads.
//! Metadata recovery, publication and ordinary ranges share one verifier.
use super::*;
use sha2::{Digest as _, Sha256};

pub(in crate::filesystem) fn authenticate_input<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    selected: &'a acyclic_fs::Generation<A, O>,
    reference: &'a FileRef,
    bytes: &'a [u8],
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
    crate::conversation::BoundedContentFuture::new(io, || async move {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        authenticate(selected, reference, &path, io, Some(bytes)).await
    })
}

pub(super) async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    selected: &acyclic_fs::Generation<A, O>,
    reference: &FileRef,
    path: &str,
    io: &PreparationIo,
    mut expected: Option<&[u8]>,
) -> Result<()> {
    let stat = io.sdk(selected.stat(path, io, io.cancellation()).await)?;
    let length = reference.descriptor().byte_length();
    if stat.kind != acyclic_fs::kernel::FileKind::Regular || stat.logical_bytes != Some(length) {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "file length differs from its descriptor",
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
                "file authentication did not make exact progress",
            ));
        }
        io.charge(WorkCounters {
            bytes_hashed: chunk.bytes.len() as u64,
            ..Default::default()
        })?;
        digest.update(chunk.bytes.as_ref());
        if let Some(remaining) = &mut expected {
            let (prefix, suffix) =
                remaining
                    .split_at_checked(chunk.bytes.len())
                    .ok_or_else(|| {
                        stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Conflict,
                            "file content differs from the requested bytes",
                        )
                    })?;
            if prefix != chunk.bytes.as_ref() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "file content differs from the requested bytes",
                ));
            }
            *remaining = suffix;
        }
        offset += chunk.bytes.len() as u64;
        drop(chunk);
        drop(hold);
    }
    if digest.finalize().as_slice() != reference.descriptor().sha256()
        || expected.is_some_and(|remaining| !remaining.is_empty())
    {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "file content differs from its descriptor",
        ));
    }
    Ok(())
}
