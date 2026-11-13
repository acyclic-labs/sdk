//! Object-safe custody of the original provider-bound recording.
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use super::Outcome;
use super::{Recording, SpanId};
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use crate::StreamError;
use crate::{ProviderPlatform, StreamProvider};

trait Terminal: ProviderPlatform {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        self: Box<Self>,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError>;
}

impl<P: StreamProvider + ?Sized> Terminal for Recording<P> {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        self: Box<Self>,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        (*self).finish_submitted(bytes, result)
    }
}

/// The same admitted recording across an object-safe host boundary. Erasure
/// allocates only for a subscribed span and retains its original producer.
/// Dropping without a terminal leaves the original start explicitly unfinished.
pub struct OwnedRecording {
    id: SpanId,
    _terminal: Box<dyn Terminal>,
}

impl<P: StreamProvider + ?Sized> Recording<P> {
    /// Transfer this original recording without another start or publication.
    pub fn into_owned(self) -> OwnedRecording {
        OwnedRecording {
            id: self.id(),
            _terminal: Box::new(self),
        }
    }
}

impl OwnedRecording {
    /// Poll the original work under this parent, restoring the caller's scope
    /// after every poll, including suspension and cancellation.
    pub async fn within<F: std::future::Future>(&self, work: F) -> F::Output {
        super::context::within(self.id, work).await
    }

    /// Transfer the original terminal append without suspending the caller.
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub fn finish_submitted(
        self,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        self._terminal.submit(bytes, result)
    }
}
