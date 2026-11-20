//! Causal custody follows an admitted future's poll, never a worker thread.
use super::{OwnedRecording, Request, SpanId};
use crate::{BoxProviderFuture, ProviderPlatform, StreamError};
use std::{cell::RefCell, future::Future, pin::Pin, sync::Arc};

/// The admitted producer a causal span belongs to.
pub trait Producer: ProviderPlatform {
    /// Whether this producer records spans at all.
    fn enabled(&self) -> bool;
    /// The source path this producer serves.
    fn source(&self) -> &crate::StreamPath;
    /// Starts one recording for `request`.
    fn start(
        self: Arc<Self>,
        request: Request,
    ) -> BoxProviderFuture<'static, Result<OwnedRecording, StreamError>>;
}

#[derive(Clone)]
struct Active {
    id: SpanId,
    producer: Arc<dyn Producer>,
}

thread_local! {
    static CURRENT: RefCell<Option<Active>> = const { RefCell::new(None) };
}

/// The span the current poll belongs to, if any.
pub fn current() -> Option<SpanId> {
    CURRENT.with_borrow(|active| active.as_ref().map(|active| active.id))
}

/// Starts a child recording under the current span.
pub fn child(
    request: &Request,
) -> Option<BoxProviderFuture<'static, Result<OwnedRecording, StreamError>>> {
    let active = CURRENT.with_borrow(|active| {
        active
            .as_ref()
            .filter(|active| active.producer.enabled())
            .cloned()
    })?;
    Some(active.producer.start(Request {
        parent: request.parent.or(Some(active.id)),
        ..*request
    }))
}

struct Restore(Option<Active>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.replace(self.0.take());
    }
}

pin_project_lite::pin_project! {
    /// A source that restores the inherited trace context around each poll.
    pub struct Inherited<F> {
        #[pin]
        source: Option<F>,
        active: Option<Active>,
    }
    impl<F> PinnedDrop for Inherited<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            let _restore = Restore(CURRENT.replace(this.active.clone()));
            this.source.set(None);
        }
    }
}

impl<F: Future> Future for Inherited<F> {
    type Output = F::Output;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let _restore = Restore(CURRENT.replace(this.active.clone()));
        this.source
            .as_pin_mut()
            .map_or(std::task::Poll::Pending, |source| source.poll(cx))
    }
}

fn scoped<F>(active: Option<Active>, future: F) -> Inherited<F> {
    Inherited {
        source: Some(future),
        active,
    }
}

/// Runs `work` as a child of `id`.
pub fn within_sync<T>(id: SpanId, producer: Arc<dyn Producer>, work: impl FnOnce() -> T) -> T {
    let _restore = Restore(CURRENT.replace(Some(Active { id, producer })));
    work()
}

/// Awaits `future` as a child of `id`.
pub async fn within<F: Future>(id: SpanId, producer: Arc<dyn Producer>, future: F) -> F::Output {
    scoped(Some(Active { id, producer }), future).await
}

/// Capture at submission, then restore the exact causal owner on each worker poll.
/// An unobserved submission carries no producer or per-agent background task.
pub fn inherit<F: Future>(future: F) -> Inherited<F> {
    let active = CURRENT.with_borrow(Clone::clone);
    scoped(active, future)
}

/// Awaits `future` with no active span.
pub async fn disabled<F: Future>(future: F) -> F::Output {
    scoped(None, future).await
}

/// Accompany original work with its admitted recording, retaining custody when
/// decoding or a streamed body must decide settlement after the future returns.
/// No active owner means no recording, allocation or diagnostic publication.
pub async fn accompany<T>(
    request: Request,
    work: impl Future<Output = T>,
) -> (T, Option<OwnedRecording>) {
    OwnedRecording::accompany(child(&request), work).await
}
/// Record [`super::observe_value`]'s already live work under the active
/// owner's child span. The classifier runs only for an admitted span.
pub fn record_value<T>(
    request: &Request,
    work: impl Future<Output = T>,
    _outcome: impl FnOnce(&T) -> Option<(Option<u64>, super::Outcome)>,
) -> impl Future<Output = T> {
    use futures::FutureExt as _;
    OwnedRecording::child(request, work).map(move |(result, _recording)| {
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        if let Some(recording) = _recording
            && let Some((bytes, outcome)) = _outcome(&result)
        {
            let _completion = recording.finish_submitted(bytes, outcome);
        }
        result
    })
}
/// Preserve the original cursor and snapshot; only admitted readers acquire an
/// observation wrapper. Pending diagnostic publication never owns delivery.
pub fn records(records: crate::RecordStream) -> crate::RecordStream {
    use futures::StreamExt as _;
    let active = CURRENT.with_borrow(|active| {
        active
            .as_ref()
            .filter(|active| active.producer.enabled())
            .cloned()
    });
    let Some(active) = active else {
        return records;
    };
    let finite = records.selection.end.is_some();
    records.map_cursor(|cursor| {
        Box::pin(futures::stream::unfold(
            (cursor, active),
            move |(mut cursor, active)| async move {
                let admission = active.producer.enabled().then(|| {
                    active.producer.clone().start(Request {
                        name: "stream.record_delivery",
                        mechanism: super::Mechanism::Wait,
                        parent: Some(active.id),
                        guarantee: Some("ordered_cursor_delivery"),
                        key: None,
                    })
                });
                let (next, recording) = OwnedRecording::accompany(admission, cursor.next()).await;
                #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
                if let Some(recording) = recording {
                    let bytes = match &next {
                        Some(Ok(record)) => u64::try_from(record.value.len()).ok(),
                        None => Some(0),
                        Some(Err(_)) => None,
                    };
                    if !matches!(next, Some(Err(_))) {
                        let _completion = recording.finish_submitted(
                            bytes,
                            if next.is_none() && !finite {
                                super::Outcome::Unsettled
                            } else {
                                super::Outcome::Succeeded
                            },
                        );
                    }
                }
                next.map(|value| (value, (cursor, active)))
            },
        ))
    })
}

/// Whether the current span's producer serves `path`.
pub fn is_source(path: &crate::StreamPath) -> bool {
    any_source(std::iter::once(path))
}

/// Diagnostic path traversal is lazy and only paid by an admitted owner.
pub fn any_source<'a>(paths: impl IntoIterator<Item = &'a crate::StreamPath>) -> bool {
    source_matches(|source| paths.into_iter().any(|path| path == source))
}

/// Whether the current producer's source satisfies `matches`.
pub fn source_matches(matches: impl FnOnce(&crate::StreamPath) -> bool) -> bool {
    CURRENT.with_borrow(|active| {
        active
            .as_ref()
            .filter(|active| active.producer.enabled())
            .is_some_and(|active| matches(active.producer.source()))
    })
}

/// Retain recording custody around original synchronous admission without
/// delaying or allocating for an unobserved in-process action.
pub fn accompany_sync<T>(
    request: Request,
    work: impl FnOnce() -> T,
) -> (T, Option<OwnedRecording>) {
    use futures::FutureExt as _;
    let recording = child(&request)
        .and_then(|admission| admission.now_or_never())
        .and_then(Result::ok);
    let result = if let Some(recording) = &recording {
        recording.within_sync(work)
    } else {
        work()
    };
    (result, recording)
}
/// Record [`super::observe_sync`]'s already live work. The classifier runs
/// only after admission.
pub fn record_sync<T>(
    request: Request,
    work: impl FnOnce() -> T,
    _outcome: impl FnOnce(&T) -> Option<(Option<u64>, super::Outcome, Option<super::Comparison>)>,
) -> T {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    {
        let (result, recording) = accompany_sync(request, work);
        let Some(recording) = recording else {
            return result;
        };
        if let Some((bytes, outcome, comparison)) = _outcome(&result) {
            let _completion = recording.finish_compared(bytes, outcome, comparison);
        }
        result
    }
    #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
    {
        let _ = request;
        work()
    }
}

#[cfg(not(target_arch = "wasm32"))]
pin_project_lite::pin_project! {
    struct InheritedStream<S> {
        #[pin]
        body: S,
        active: Option<Active>,
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: futures::Stream> futures::Stream for InheritedStream<S> {
    type Item = S::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let state = self.project();
        let _restore = Restore(CURRENT.replace(state.active.clone()));
        state.body.poll_next(cx)
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Restores the current span on each poll of `body`.
pub fn inherit_stream<S: futures::Stream>(body: S) -> impl futures::Stream<Item = S::Item> {
    InheritedStream {
        body,
        active: CURRENT.with_borrow(Clone::clone),
    }
}
