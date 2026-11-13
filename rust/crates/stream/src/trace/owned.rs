//! Object-safe custody of the original provider-bound recording.
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use super::Outcome;
use super::{Recording, SpanId};
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use crate::StreamError;
use crate::{ProviderPlatform, StreamProvider};

trait Terminal: ProviderPlatform {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn available(&self) -> bool;
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        self: Box<Self>,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError>;
}

impl<P: StreamProvider + ?Sized> Terminal for Recording<P> {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn available(&self) -> bool {
        self.recorder.terminal_available()
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        self: Box<Self>,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        (*self).finish_submitted(bytes, result)
    }
}

struct TerminalGuard(Option<Box<dyn Terminal>>);

impl TerminalGuard {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        mut self,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        // Only this consuming method or Drop can take the private terminal.
        self.0
            .take()
            .expect("owned recording retains its sole terminal")
            .submit(bytes, result)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
        let _ = self.0.take();
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        if let Some(terminal) = self.0.take()
            && terminal.available()
        {
            let _completion = terminal.submit(None, Outcome::Unsettled);
        }
    }
}
/// The same admitted recording across an object-safe host boundary. Erasure
/// allocates only for a subscribed span and retains its original producer.
/// Dropping without settlement submits Unsettled to the original executor.
/// Without an available executor, the original start remains retained.
pub struct OwnedRecording {
    id: SpanId,
    producer: std::sync::Arc<dyn super::context::Producer>,
    _terminal: TerminalGuard,
}

impl<P: StreamProvider + ?Sized> Recording<P> {
    /// Transfer this original recording without another start or publication.
    pub fn into_owned(self) -> OwnedRecording {
        OwnedRecording {
            id: self.id(),
            producer: self.recorder.clone(),
            _terminal: TerminalGuard(Some(Box::new(self))),
        }
    }
}

impl OwnedRecording {
    /// Poll diagnostic admission alongside original work. Pending diagnostics
    /// cannot delay a ready result; abandoned admission remains incomplete.
    /// Ready admission supplies the original parent for the remaining polls.
    pub async fn accompany<T, E>(
        admission: Option<crate::BoxProviderFuture<'_, Result<Self, E>>>,
        work: impl std::future::Future<Output = T>,
    ) -> (T, Option<Self>) {
        let Some(admission) = admission else {
            return (work.await, None);
        };
        let work = std::pin::pin!(work);
        match futures::future::select(admission, work).await {
            futures::future::Either::Left((Ok(recording), work)) => {
                let result = recording.within(work).await;
                (result, Some(recording))
            }
            futures::future::Either::Left((Err(_), work)) => (work.await, None),
            futures::future::Either::Right((result, _)) => (result, None),
        }
    }
    /// Poll the original work under this parent, restoring the caller's scope
    /// after every poll, including suspension and cancellation.
    pub async fn within<F: std::future::Future>(&self, work: F) -> F::Output {
        super::context::within(self.id, self.producer.clone(), work).await
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

impl<P: StreamProvider + ?Sized> super::context::Producer for super::Recorder<P> {
    fn enabled(&self) -> bool {
        self.has_demand()
    }

    fn is_source(&self, path: &crate::StreamPath) -> bool {
        self.stream.path() == path
    }
    fn start(
        self: std::sync::Arc<Self>,
        request: super::Request,
    ) -> crate::BoxProviderFuture<'static, Result<OwnedRecording, crate::StreamError>> {
        Box::pin(async move {
            super::Recorder::start(
                &self,
                request.name,
                request.mechanism,
                request.parent,
                request.guarantee,
            )
            .await
            .map(Recording::into_owned)
        })
    }
}
