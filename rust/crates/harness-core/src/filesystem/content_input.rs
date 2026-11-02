use super::*;
use acyclic_stream::OwnedBytes;

// Slice callers need an owned mutation payload. Encoded journal bytes already
// carry that ownership and can share their backing for the awaited publication.
pub(super) trait ContentInput: AsRef<[u8]> + Sync {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes>;
    fn publication_input(&self) -> PublicationInput<'_>;
}

impl ContentInput for [u8] {
    fn publication_input(&self) -> PublicationInput<'_> {
        PublicationInput::Bytes(self)
    }
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        bounded_io::clone_bytes(self, io)
    }
}

impl ContentInput for OwnedBytes {
    fn publication_input(&self) -> PublicationInput<'_> {
        PublicationInput::Owned(self)
    }
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        if let Some(io) = io {
            io.check_cancellation()?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                if !self.matches_meter(&work.runtime_meter()) {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "native content belongs to another allocation ledger",
                    ));
                }
                return Ok(self.clone());
            }
        }
        // Ordinary Bytes can allocate a sharing header on their first clone.
        // Retain the existing funded copy outside original native custody.
        bounded_io::clone_bytes(self.as_ref(), io)
    }
}

pub(super) enum PublicationInput<'a> {
    Bytes(&'a [u8]),
    Owned(&'a OwnedBytes),
    Stream(&'a mut dyn crate::conversation::ContentSource),
}

pub(super) struct ContentPublication<'a> {
    pub volume: &'a VolumeRef,
    pub grant: &'a ContentGrant,
    pub path: &'a str,
    pub media_type: &'a str,
    pub display_name: &'a str,
    pub maximum_bytes: u64,
    pub idempotency_key: &'a IdempotencyKey,
    pub internal_class: Option<InternalContentClass>,
    pub expected_generation: Option<&'a GenerationRef>,
    pub ceiling: Option<&'a crate::Capabilities>,
    pub io: Option<&'a PreparationIo>,
}

impl PublicationInput<'_> {
    pub(super) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Owned(bytes) => Some(bytes.as_ref()),
            Self::Stream(_) => None,
        }
    }

    pub(super) fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        match self {
            Self::Bytes(bytes) => bytes.mutation_bytes(io),
            Self::Owned(bytes) => bytes.mutation_bytes(io),
            Self::Stream(_) => Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "stream content requires staged chunks; recovery: publish through stage_stream_once",
            )),
        }
    }
}

/// Hash exactly the bytes consumed by the existing blob builder. No second
/// content pass or retained full-file buffer is required.
pub(super) struct HashedSource<'a> {
    pub source: &'a mut dyn crate::conversation::ContentSource,
    pub io: &'a PreparationIo,
    pub hash: sha2::Sha256,
    pub length: u64,
}

impl HashedSource<'_> {
    pub(super) async fn read(
        &mut self,
        destination: &mut [u8],
        maximum: u64,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::io::Result<usize> {
        use futures::{FutureExt, io::AsyncReadExt};
        use sha2::Digest;
        let maximum = destination
            .len()
            .min(usize::try_from(maximum).unwrap_or(usize::MAX));
        if maximum == 0 && !destination.is_empty() {
            return Err(std::io::Error::other(
                "content source read budget exhausted; recovery: supply sufficient source-byte credit",
            ));
        }
        let read = self.source.read(&mut destination[..maximum]).fuse();
        let cancelled = cancellation.cancelled().fuse();
        futures::pin_mut!(read, cancelled);
        let count = futures::select_biased! {
            _ = cancelled => return Err(std::io::Error::new(std::io::ErrorKind::Interrupted,
                "content source cancelled; recovery: start a new publication with a live cancellation token")),
            result = read => result?,
        };
        if count > maximum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "content source returned an invalid count; recovery: fix ContentSource::poll_read to respect its destination",
            ));
        }
        self.length = self.length.checked_add(count as u64).ok_or_else(|| {
            std::io::Error::other(
                "content source length overflow; recovery: supply a representable finite source",
            )
        })?;
        self.io
            .charge(acyclic_fs::WorkCounters {
                bytes_hashed: count as u64,
                ..Default::default()
            })
            .map_err(std::io::Error::other)?;
        self.hash.update(&destination[..count]);
        Ok(count)
    }
}

impl acyclic_fs::kernel::AsyncBlobSource for HashedSource<'_> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> std::io::Result<usize> {
        self.read(destination, budget.source_bytes_read, cancellation)
            .await
    }
}

#[cfg(test)]
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn owned_content_staging_shares_only_original_custody() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&work)?;
        let encoded = io.encode(&"q".repeat(128 * 1024), u64::MAX, false)?;
        let original = encoded.as_ptr();
        let copied = work.receipt().bytes_copied;
        let mut shared = None;
        let physical = acyclic_test_support::measure(|| {
            shared = Some(encoded.mutation_bytes(Some(&io)));
        });
        let shared = shared.expect("staging ran")?;
        assert_eq!(work.receipt().bytes_copied, copied);
        assert_eq!(physical.count_total, 0);
        assert_eq!(shared.as_ptr(), original);
        drop(encoded);
        assert!(shared.matches_meter(&work.runtime_meter()));
        assert_eq!(shared.len(), 128 * 1024 + 2);
        assert!(work.live_allocation_bytes() > baseline);

        let foreign = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let foreign_io = PreparationIo::from_native_work(&foreign)?;
        assert_eq!(
            shared.mutation_bytes(Some(&foreign_io)).unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized
        );
        let plain = OwnedBytes::from(vec![1, 2, 3]);
        assert_eq!(
            plain.mutation_bytes(Some(&io)).unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized
        );

        let borrowed = shared.as_ref().mutation_bytes(Some(&io))?;
        assert_eq!(borrowed.as_ref(), shared.as_ref());
        assert_ne!(borrowed.as_ptr(), shared.as_ptr());
        assert!(borrowed.matches_meter(&work.runtime_meter()));
        assert_eq!(work.receipt().bytes_copied, copied + shared.len() as u64);
        drop(borrowed);
        cancellation.cancel();
        assert_eq!(
            shared.mutation_bytes(Some(&io)).unwrap_err().code(),
            crate::wire::ErrorCode::Cancelled
        );
        drop(shared);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
