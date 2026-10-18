//! Streaming resolution of a line-window read. One pass over bounded chunks
//! hashes and classifies the whole file, so the returned reference names the
//! exact version, while only the requested lines are retained.
use super::*;
use crate::conversation::LineWindow;
use crate::store::history::HistoryIo as _;
use sha2::{Digest as _, Sha256};

mod lines;
use lines::LineScan;

/// Bytes read per range request; the only transient buffer besides the window.
const CHUNK_BYTES: u64 = 64 * 1024;

/// Identity of the whole streamed file.
pub(super) struct Scanned {
    pub(super) sha256: [u8; 32],
    pub(super) length: u64,
    pub(super) media_type: &'static str,
}

/// Reads `path` from `generation` in chunks, retaining only `window`'s lines.
pub(super) async fn scan<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum_bytes: u64,
    window: LineWindow,
    io: &PreparationIo,
) -> Result<(acyclic_stream::OwnedBytes, Scanned)> {
    io.remaining()?;
    let stat = io.sdk(generation.stat(path, io, io.cancellation()).await)?;
    if stat.kind != FileKind::Regular {
        return io.sdk((
            Err::<(acyclic_stream::OwnedBytes, Scanned), _>(
                acyclic_fs::WorkspaceError::NotRegularFile,
            ),
            Ok(()),
        ));
    }
    let length = stat.logical_bytes.ok_or_else(|| {
        stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Storage,
            "file length is missing",
        )
    })?;
    if length > maximum_bytes {
        return io.sdk((
            Err::<(acyclic_stream::OwnedBytes, Scanned), _>(
                acyclic_fs::WorkspaceError::ReadLimitExceeded,
            ),
            Ok(()),
        ));
    }
    let mut lines = LineScan::new(window.offset, window.limit);
    let mut selected = Retained::default();
    let mut sha = Sha256::new();
    let mut offset = 0;
    while offset < length {
        let size = CHUNK_BYTES.min(length - offset);
        io.remaining()?;
        let chunk = io.sdk(
            generation
                .read_range(path, offset, size, io, io.cancellation())
                .await,
        )?;
        let bytes = chunk.bytes.as_slice();
        if bytes.len() as u64 != size {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "file range is shorter than its length",
            ));
        }
        let mut observer = io;
        observer.check_cancellation()?;
        observer.hashed_bytes(size)?;
        sha.update(bytes);
        offset += size;
        if let Some(range) = lines.update(bytes) {
            selected.append(&bytes[range], length - offset, io)?;
        }
    }
    let invalid = |reason| stock_boundary(Some(io), crate::wire::ErrorCode::Invalid, reason);
    let (selects, media_type) = lines.finish();
    let Some(media_type) = media_type else {
        return Err(invalid("file read requires UTF-8 content"));
    };
    if !selects {
        return Err(invalid("read offset is past the end of the file"));
    }
    Ok((
        selected.bytes.into(),
        Scanned {
            sha256: sha.finalize().into(),
            length,
            media_type,
        },
    ))
}

/// The window's bytes, grown only after its storage is claimed on the caller's
/// ledger. Every byte held is claimed, and a denial leaves nothing allocated
/// or copied.
#[derive(Default)]
struct Retained {
    bytes: Vec<u8>,
}

impl Retained {
    /// Appends `chunk`; `remaining` bounds what any later chunk can add, so
    /// amortized growth never claims more than the file could still supply.
    fn append(&mut self, chunk: &[u8], remaining: u64, io: &PreparationIo) -> Result<()> {
        #[cfg(test)]
        if tests::ALLOCATE_BEFORE_ADMISSION.with(std::cell::Cell::get) {
            self.bytes
                .try_reserve_exact(chunk.len())
                .map_err(Error::Allocation)?;
        }
        let mut observer = io;
        observer.admit_copy(chunk.len() as u64)?;
        let needed = self.bytes.len() + chunk.len();
        if needed > self.bytes.capacity() {
            let ceiling = usize::try_from(remaining)
                .ok()
                .and_then(|remaining| needed.checked_add(remaining))
                .unwrap_or(usize::MAX);
            let capacity = needed.max(self.bytes.capacity().saturating_mul(2).min(ceiling));
            let claimed = (capacity - self.bytes.capacity()) as u64;
            observer.retain_owned(claimed)?;
            if let Err(error) = observer.allocation_attempt(claimed).and_then(|()| {
                self.bytes
                    .try_reserve_exact(capacity - self.bytes.len())
                    .map_err(Error::Allocation)
            }) {
                observer.release_owned(claimed)?;
                return Err(error);
            }
            // An allocator may round up; claim whatever it actually holds.
            let excess = self.bytes.capacity().saturating_sub(capacity) as u64;
            if excess != 0 {
                observer.retain_owned(excess)?;
            }
        }
        observer.copy_bytes(chunk.len() as u64)?;
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
