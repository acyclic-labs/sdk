use super::*;
use acyclic_fs::kernel::AsyncBlobSource as _;
use acyclic_stream::OwnedBytes;

// Slice callers need an owned mutation payload. Encoded journal bytes already
// carry that ownership and can share their backing for the awaited publication.
pub(super) trait ContentInput: AsRef<[u8]> + Sync {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes>;
}

impl ContentInput for [u8] {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        bounded_io::clone_bytes(self, io)
    }
}

impl ContentInput for OwnedBytes {
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

pub(super) enum PublicationInput<'a, B: ContentInput + ?Sized> {
    Bytes(&'a B),
    Source(std::pin::Pin<&'a mut dyn crate::conversation::ContentSource>),
}

impl<B: ContentInput + ?Sized> PublicationInput<'_, B> {
    pub(super) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(input) => Some(input.as_ref()),
            Self::Source(_) => None,
        }
    }
}

/// Hashes each accepted source interval once while forwarding its original
/// ownership. This projection owns no payload buffer or publication identity.
pub(super) struct DescribedSource<'a> {
    source: crate::conversation::ContentSourceAdapter<'a>,
    io: &'a PreparationIo,
    digest: sha2::Sha256,
    length: u64,
}

impl<'a> DescribedSource<'a> {
    pub(super) fn new(
        source: std::pin::Pin<&'a mut dyn crate::conversation::ContentSource>,
        io: &'a PreparationIo,
    ) -> Self {
        use sha2::Digest as _;
        Self {
            source: crate::conversation::ContentSourceAdapter::new(source),
            io,
            digest: sha2::Sha256::new(),
            length: 0,
        }
    }

    fn accept(&mut self, bytes: &[u8]) -> std::result::Result<(), acyclic_fs::FsError> {
        use sha2::Digest as _;
        if bytes.is_empty() {
            return Ok(());
        }
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(acyclic_fs::WorkError::Overflow)?;
        let work = WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        self.io.preflight(work)?;
        self.digest.update(bytes);
        self.io.charge(work)?;
        self.length = length;
        Ok(())
    }

    pub(super) fn descriptor(self, media_type: &str) -> Result<FileDescriptor> {
        use sha2::Digest as _;
        FileDescriptor::new(
            self.digest.finalize().into(),
            self.length,
            stock_string_copy(media_type, Some(self.io))?,
        )
    }
}

impl acyclic_fs::kernel::AsyncBlobSource for DescribedSource<'_> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> std::io::Result<usize> {
        let read = self.source.read(destination, budget, cancellation).await?;
        let bytes = destination
            .get(..read)
            .ok_or_else(|| std::io::Error::other("source exceeded its read destination"))?;
        self.accept(bytes).map_err(std::io::Error::other)?;
        Ok(read)
    }

    async fn read_owned(
        &mut self,
        maximum: usize,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::io::Result<Option<acyclic_fs::kernel::OwnedSourceChunk>> {
        let chunk = self
            .source
            .read_owned(maximum, budget, cancellation)
            .await?;
        if let Some(chunk) = &chunk {
            self.accept(&chunk.bytes).map_err(std::io::Error::other)?;
        }
        Ok(chunk)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a acyclic_fs::NativeMaterializationWork,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> std::result::Result<usize, acyclic_fs::FsError> {
        let read = self
            .source
            .read_with_native_work(destination, original, cancellation)
            .await?;
        let bytes = destination
            .get(..read)
            .ok_or(acyclic_fs::kernel::BlobBuildError::TooLarge)?;
        self.accept(bytes)?;
        Ok(read)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_owned_with_native_work(
        &mut self,
        maximum: usize,
        original: &acyclic_fs::NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<Option<acyclic_fs::kernel::OwnedSourceChunk>, acyclic_fs::FsError>
    {
        let chunk = self
            .source
            .read_owned_with_native_work(maximum, original, cancellation)
            .await?;
        if let Some(chunk) = &chunk {
            self.accept(&chunk.bytes)?;
        }
        Ok(chunk)
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        self.source.take_work()
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
