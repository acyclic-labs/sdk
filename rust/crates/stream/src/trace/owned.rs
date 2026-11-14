//! Object-safe custody of the original provider-bound recording.
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use super::{Comparison, Outcome};
use super::{Recording, SpanId};
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
        comparison: Option<Comparison>,
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
        comparison: Option<Comparison>,
    ) -> Result<crate::AppendPending, StreamError> {
        (*self).finish_compared(bytes, result, comparison)
    }
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
#[derive(Debug, thiserror::Error)]
#[error("owned trace terminal was already transferred")]
struct TerminalTransferred;
struct TerminalGuard(Option<Box<dyn Terminal>>);

impl TerminalGuard {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn submit(
        mut self,
        bytes: Option<u64>,
        result: Outcome,
        comparison: Option<Comparison>,
    ) -> Result<crate::AppendPending, StreamError> {
        // Only this consuming method or Drop can take the private terminal.
        self.0
            .take()
            .ok_or_else(|| StreamError::InvalidResponse.with_source(TerminalTransferred))?
            .submit(bytes, result, comparison)
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
            let _completion = terminal.submit(None, Outcome::Unsettled, None);
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
    /// Original span identity retained across ownership transfer.
    #[must_use]
    pub fn id(&self) -> SpanId {
        self.id
    }

    /// Observe `work` as a child of the span that first polls it. Pending
    /// diagnostics cannot delay a ready result; abandoned admission remains
    /// incomplete. Ready admission supplies the original parent for the
    /// remaining polls.
    pub(super) fn child<F: std::future::Future>(
        request: super::Request,
        work: F,
    ) -> Accompany<'static, F, StreamError> {
        Accompany {
            admission: Admission::Child(request),
            recording: None,
            work,
        }
    }

    /// Poll diagnostic admission alongside original work, as [`Self::child`]
    /// does for an admission already started.
    pub fn accompany<'a, F: std::future::Future, E>(
        admission: Option<crate::BoxProviderFuture<'a, Result<Self, E>>>,
        work: F,
    ) -> Accompany<'a, F, E> {
        Accompany {
            admission: admission.map_or(Admission::Settled, Admission::Starting),
            recording: None,
            work,
        }
    }

    /// Poll the original work under this parent, restoring the caller's scope
    /// after every poll, including suspension and cancellation.
    pub async fn within<F: std::future::Future>(&self, work: F) -> F::Output {
        super::context::within(self.id, self.producer.clone(), work).await
    }

    /// Run synchronous work under this owner, restoring its caller on return or unwind.
    pub fn within_sync<T>(&self, work: impl FnOnce() -> T) -> T {
        super::context::within_sync(self.id, self.producer.clone(), work)
    }

    /// Transfer the original terminal append without suspending the caller.
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub fn finish_submitted(
        self,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        self.finish_compared(bytes, result, None)
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub(super) fn finish_compared(
        self,
        bytes: Option<u64>,
        result: Outcome,
        comparison: Option<Comparison>,
    ) -> Result<crate::AppendPending, StreamError> {
        self._terminal.submit(bytes, result, comparison)
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
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        {
            let recording = self.submit_start(request).map(Recording::into_owned);
            Box::pin(std::future::ready(recording))
        }
        #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
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

/// Diagnostic admission of one observed future.
enum Admission<'a, E> {
    /// A child of whichever span first polls the work.
    Child(super::Request),
    /// The child's own admission, started on first poll.
    StartingChild(crate::BoxProviderFuture<'a, Result<OwnedRecording, StreamError>>),
    /// An admission the caller started, with its own error type.
    Starting(crate::BoxProviderFuture<'a, Result<OwnedRecording, E>>),
    Settled,
}

pin_project_lite::pin_project! {
    /// Original work polled alongside its diagnostic admission. The work is
    /// held once and its admission and recording beside it, so observation
    /// adds a constant number of bytes and one type level to the observed
    /// future, however deeply observations nest.
    #[must_use = "futures do nothing unless polled"]
    pub struct Accompany<'a, F, E> {
        admission: Admission<'a, E>,
        recording: Option<OwnedRecording>,
        #[pin]
        work: F,
    }
}

impl<F: std::future::Future, E> std::future::Future for Accompany<'_, F, E> {
    type Output = (F::Output, Option<OwnedRecording>);

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        if let Admission::Child(_) = this.admission
            && let Admission::Child(request) = std::mem::replace(this.admission, Admission::Settled)
        {
            *this.admission =
                super::context::child(request).map_or(Admission::Settled, Admission::StartingChild);
        }
        let started = match this.admission {
            Admission::StartingChild(start) => start.as_mut().poll(cx).map(Result::ok),
            Admission::Starting(start) => start.as_mut().poll(cx).map(Result::ok),
            Admission::Child(_) | Admission::Settled => std::task::Poll::Pending,
        };
        if let std::task::Poll::Ready(started) = started {
            *this.recording = started;
            *this.admission = Admission::Settled;
        }
        let mut work = this.work;
        let polled = match this.recording.as_ref() {
            Some(recording) => recording.within_sync(|| work.as_mut().poll(cx)),
            None => work.as_mut().poll(cx),
        };
        let output = std::task::ready!(polled);
        // Admission still pending when the work finishes is abandoned now,
        // remaining an incomplete start.
        *this.admission = Admission::Settled;
        std::task::Poll::Ready((output, this.recording.take()))
    }
}
