//! Whole-file off-marker recognition over the original bounded range producer.

/// Incremental form of the whole-file off marker. A possible prefix is never
/// proof that a file is disabled: the caller must reach its selected EOF.
#[doc(hidden)]
#[derive(Default)]
pub struct OffMarker {
    matched: u8,
    rejected: bool,
}

const OFF_MARKER: &[u8] = b"off: true";

pub(crate) struct MarkerChunk<H> {
    // Field order keeps the physical bytes alive inside their original custody.
    pub bytes: acyclic_stream::OwnedBytes,
    pub _hold: H,
}

impl OffMarker {
    /// Consumes a chunk and reports whether more bytes could still form a marker.
    pub fn push(&mut self, bytes: &[u8]) -> bool {
        self.consume(bytes);
        !self.rejected
    }

    fn consume(&mut self, bytes: &[u8]) -> usize {
        if self.rejected {
            return 0;
        }
        for (index, &byte) in bytes.iter().enumerate() {
            let matched = usize::from(self.matched);
            if (matched == 0 || matched == OFF_MARKER.len()) && byte.is_ascii_whitespace() {
                continue;
            }
            if OFF_MARKER.get(matched) != Some(&byte) {
                self.rejected = true;
                return index + 1;
            }
            self.matched += 1;
        }
        bytes.len()
    }

    /// Checks the selected EOF, after every preceding byte has been consumed.
    #[must_use]
    pub fn finish(&self) -> bool {
        !self.rejected && usize::from(self.matched) == OFF_MARKER.len()
    }
}

pub(crate) trait MarkerWork {
    fn remaining(&self) -> crate::Result<u64>;
    fn charge(&self, items_examined: u64) -> crate::Result<()>;
}

impl MarkerWork for Option<&crate::conversation::ContentIo> {
    fn remaining(&self) -> crate::Result<u64> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = self {
            return Ok(io.remaining()?.items_examined);
        }
        Ok(u64::MAX)
    }

    fn charge(&self, items_examined: u64) -> crate::Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = self {
            io.charged(acyclic_fs::WorkCounters {
                items_examined,
                ..Default::default()
            })?;
        }
        let _ = items_examined;
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl MarkerWork for &acyclic_fs::NativeMaterializationWork {
    fn remaining(&self) -> crate::Result<u64> {
        Ok(acyclic_fs::NativeMaterializationWork::remaining(self)?.items_examined)
    }
    fn charge(&self, items_examined: u64) -> crate::Result<()> {
        acyclic_fs::NativeMaterializationWork::charge(
            self,
            acyclic_fs::WorkCounters {
                items_examined,
                ..Default::default()
            },
        )?;
        Ok(())
    }
}

fn consume_off(
    marker: &mut OffMarker,
    bytes: &[u8],
    work: &impl MarkerWork,
) -> crate::Result<bool> {
    let admitted = work.remaining()?.min(bytes.len() as u64) as usize;
    let examined = marker.consume(&bytes[..admitted]);
    work.charge(examined as u64)?;
    if !marker.rejected && admitted < bytes.len() {
        // The original ledger supplies the typed budget refusal before another
        // byte is inspected. Item credit cannot grow during an invocation.
        work.charge(1)?;
        return Err(crate::Error::Storage(
            "marker work allowance changed unexpectedly".into(),
        ));
    }
    Ok(!marker.rejected)
}

/// Continues a possible marker through bounded ranges at the caller's original
/// selection. The adapter returns each chunk's custody with its bytes, so both
/// are dropped before the next read. No body is accumulated or reread.
pub(crate) async fn off_from_prefix<F, Fut, H>(
    prefix: &[u8],
    length: u64,
    work: impl MarkerWork,
    mut read: F,
) -> crate::Result<bool>
where
    F: FnMut(acyclic_fs::ByteRange) -> Fut,
    Fut: std::future::Future<Output = crate::Result<MarkerChunk<H>>>,
{
    let mut offset = prefix.len() as u64;
    if offset > length {
        return Err(crate::Error::Storage(
            "marker prefix exceeds selected file".into(),
        ));
    }
    let mut marker = OffMarker::default();
    if !consume_off(&mut marker, prefix, &work)? {
        return Ok(false);
    }
    let chunk_size = offset.max(1);
    while offset < length {
        let range = acyclic_fs::ByteRange {
            offset,
            length: chunk_size.min(length - offset),
        };
        let chunk = read(range).await?;
        if chunk.bytes.len() as u64 != range.length {
            return Err(crate::Error::Storage(
                "marker range did not reach selected boundary".into(),
            ));
        }
        if !consume_off(&mut marker, &chunk.bytes, &work)? {
            return Ok(false);
        }
        offset += range.length;
    }
    Ok(marker.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn marker_work_charges_only_inspected_bytes_and_refuses_before_unfunded_work()
    -> crate::Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                items_examined: 3,
                allocation_operations: 2,
                peak_allocation_bytes: 4096,
                ..Default::default()
            },
            &cancellation,
        )
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let mut marker = OffMarker::default();
        let ordinary = [b'x'; 16_384];
        assert!(!consume_off(&mut marker, &ordinary, &&work)?);
        assert_eq!(work.remaining()?.items_examined, 2);

        let mut marker = OffMarker::default();
        assert!(matches!(
            consume_off(&mut marker, b"   off: true", &&work),
            Err(crate::Error::NativeWork { .. })
        ));
        assert_eq!(
            marker.matched, 0,
            "no byte beyond the admitted whitespace was inspected"
        );

        cancellation.cancel();
        assert!(matches!(
            consume_off(&mut marker, b"off: true", &&work),
            Err(crate::Error::NativeWork { .. })
        ));
        assert_eq!(marker.matched, 0);
        Ok(())
    }

    #[tokio::test]
    async fn off_marker_continuations_preserve_custody_and_require_progress() -> crate::Result<()> {
        use acyclic_stream::OwnedBytes;
        use std::cell::Cell;

        struct Chunk<'a>(&'a Cell<bool>);
        impl Drop for Chunk<'_> {
            fn drop(&mut self) {
                assert!(self.0.replace(false));
            }
        }
        let bytes = OwnedBytes::from(format!("{}off: true\n", " ".repeat(130)));
        let live = Cell::new(false);
        let next = Cell::new(32u64);
        let off = off_from_prefix(&bytes[..32], bytes.len() as u64, None, |range| {
            assert!(
                !live.replace(true),
                "previous chunk custody must be released"
            );
            assert_eq!(
                range.offset,
                next.get(),
                "ranges must never overlap or skip"
            );
            assert!(range.length <= 32);
            next.set(range.offset + range.length);
            std::future::ready(Ok(MarkerChunk {
                bytes: bytes.slice(range.offset as usize..next.get() as usize),
                _hold: Chunk(&live),
            }))
        })
        .await?;
        assert!(off);
        assert_eq!(next.get(), bytes.len() as u64);
        assert!(!live.get());

        // A dropped producer cannot be mistaken for EOF or retried.
        let failed = off_from_prefix(&bytes[..32], bytes.len() as u64, None, |_| {
            std::future::ready(Err::<MarkerChunk<()>, _>(crate::Error::NotFound(
                "selected source is gone".into(),
            )))
        })
        .await;
        assert_eq!(failed.unwrap_err().code(), crate::wire::ErrorCode::NotFound);
        // A successful empty response before the selected EOF must terminate,
        // rather than repeat the same offset forever.
        let empty = off_from_prefix(&bytes[..32], bytes.len() as u64, None, |_| {
            std::future::ready(Ok(MarkerChunk {
                bytes: OwnedBytes::default(),
                _hold: (),
            }))
        })
        .await;
        assert_eq!(empty.unwrap_err().code(), crate::wire::ErrorCode::Storage);
        Ok(())
    }
}
