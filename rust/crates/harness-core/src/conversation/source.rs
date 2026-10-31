//! Object-safe streaming input over the filesystem's original source contract.
use acyclic_fs::CancellationToken;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::kernel::BlobBuildError;
use acyclic_fs::kernel::{AsyncBlobSource, OwnedSourceChunk};
use acyclic_fs::performance::{WorkBudget, WorkCounters};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::{FsError, NativeMaterializationWork};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// A borrowed producer whose wake and terminal state belong to the original
/// operation. `Pending` must register the original producer's wake. Losing that
/// producer is an error; only its explicit successful terminal state permits
/// EOF. An unavailable owned read returns `None`, which requests a buffered read
/// and does not mean EOF.
///
/// The poll interface is object safe without allocating a future for each
/// chunk. Native methods receive the same original work and cancellation; they
/// must never fall back to an ordinary read or replace the source operation.
pub trait ContentSource: acyclic_stream::ProviderTask {
    /// Reads into caller storage, returning zero only at successful EOF.
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        destination: &mut [u8],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Poll<std::io::Result<usize>>;

    /// Transfers the producer's original chunk and retained-capacity identity.
    fn poll_read_owned(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _maximum: usize,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> Poll<std::io::Result<Option<OwnedSourceChunk>>> {
        Poll::Ready(Ok(None))
    }

    /// Buffered read under the original native invocation. Missing support is
    /// a typed refusal, never an ordinary-read fallback or fabricated EOF.
    #[cfg(not(target_arch = "wasm32"))]
    fn poll_read_with_native_work(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _destination: &mut [u8],
        _original: &NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Poll<Result<usize, FsError>> {
        Poll::Ready(Err(BlobBuildError::NativeSourceUnavailable.into()))
    }

    /// Transfers owned bytes under the original native allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn poll_read_owned_with_native_work(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _maximum: usize,
        _original: &NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Poll<Result<Option<OwnedSourceChunk>, FsError>> {
        Poll::Ready(Ok(None))
    }

    /// Takes the original producer's accumulated work once, without recounting
    /// its reads or ownership transfer in this projection.
    fn take_work(self: Pin<&mut Self>) -> Option<WorkCounters> {
        None
    }
}

/// The sole object-source projection into filesystem streaming staging. It
/// borrows the producer and forwards its exact chunks, work, errors and EOF;
/// it owns no buffer, task, publication or recovery identity.
pub struct ContentSourceAdapter<'a> {
    source: Pin<&'a mut dyn ContentSource>,
}

impl<'a> ContentSourceAdapter<'a> {
    /// Retains the original borrowed producer through the awaited operation.
    pub fn new(source: Pin<&'a mut dyn ContentSource>) -> Self {
        Self { source }
    }
}

impl AsyncBlobSource for ContentSourceAdapter<'_> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        std::future::poll_fn(|context| {
            self.source
                .as_mut()
                .poll_read(context, destination, budget, cancellation)
        })
        .await
    }

    async fn read_owned(
        &mut self,
        maximum: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> std::io::Result<Option<OwnedSourceChunk>> {
        std::future::poll_fn(|context| {
            self.source
                .as_mut()
                .poll_read_owned(context, maximum, budget, cancellation)
        })
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        std::future::poll_fn(|context| {
            self.source.as_mut().poll_read_with_native_work(
                context,
                destination,
                original,
                cancellation,
            )
        })
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_owned_with_native_work(
        &mut self,
        maximum: usize,
        original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<OwnedSourceChunk>, FsError> {
        std::future::poll_fn(|context| {
            self.source.as_mut().poll_read_owned_with_native_work(
                context,
                maximum,
                original,
                cancellation,
            )
        })
        .await
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        self.source.as_mut().take_work()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "source_tests.rs"]
mod tests;
