//! Causal custody follows an admitted future's poll, never a worker thread.
use super::{OwnedRecording, Request, SpanId};
use crate::{BoxProviderFuture, ProviderPlatform, StreamError};
use std::{cell::RefCell, future::Future, pin::Pin, sync::Arc};

pub(super) trait Producer: ProviderPlatform {
    fn enabled(&self) -> bool;
    fn is_source(&self, path: &crate::StreamPath) -> bool;
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

pub(super) fn current() -> Option<SpanId> {
    CURRENT.with_borrow(|active| active.as_ref().map(|active| active.id))
}

pub(super) fn child(
    mut request: Request,
) -> Option<BoxProviderFuture<'static, Result<OwnedRecording, StreamError>>> {
    let active = CURRENT.with_borrow(|active| {
        active
            .as_ref()
            .filter(|active| active.producer.enabled())
            .cloned()
    })?;
    request.parent = request.parent.or(Some(active.id));
    Some(active.producer.start(request))
}

struct Restore(Option<Active>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.replace(self.0.take());
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct Inherited<F> {
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

pub(super) fn within_sync<T>(
    id: SpanId,
    producer: Arc<dyn Producer>,
    work: impl FnOnce() -> T,
) -> T {
    let _restore = Restore(CURRENT.replace(Some(Active { id, producer })));
    work()
}

pub(super) async fn within<F: Future>(
    id: SpanId,
    producer: Arc<dyn Producer>,
    future: F,
) -> F::Output {
    scoped(Some(Active { id, producer }), future).await
}

/// Capture at submission, then restore the exact causal owner on each worker poll.
/// An unobserved submission carries no producer or per-agent background task.
pub(crate) fn inherit<F: Future>(future: F) -> Inherited<F> {
    let active = CURRENT.with_borrow(Clone::clone);
    scoped(active, future)
}

pub(super) async fn disabled<F: Future>(future: F) -> F::Output {
    scoped(None, future).await
}

/// Invoke the original work under the active owner's child span. Without an
/// admitted owner this performs one local context check, with no allocation,
/// clock, subscription lookup or diagnostic IO. Publication errors never replace
/// the original result; unretained failures remain explicitly unfinished.
pub fn observe<T, E>(
    request: Request,
    work: impl Future<Output = Result<T, E>>,
    bytes: impl FnOnce(&T) -> Option<u64>,
) -> impl Future<Output = Result<T, E>> {
    observe_outcome(request, work, |result| {
        result
            .as_ref()
            .ok()
            .map(|value| (bytes(value), super::Outcome::Succeeded))
    })
}

/// Classify the original result only for an admitted span. Failed classification
/// still requires the original retained cause through Outcome's type. No
/// classifier or terminal publication is evaluated on an unobserved request.
pub fn observe_outcome<T, E>(
    request: Request,
    work: impl Future<Output = Result<T, E>>,
    outcome: impl FnOnce(&Result<T, E>) -> Option<(Option<u64>, super::Outcome)>,
) -> impl Future<Output = Result<T, E>> {
    observe_value(request, work, outcome)
}

/// Observe an original result or value, including a borrowed lock guard, without
/// wrapping an infallible mechanism in a fabricated error type. The classifier
/// runs only for an admitted span and no terminal publication delays return.
/// The observed future holds the work once: observation adds a constant size
/// and one type level, however deeply observations nest.
pub fn observe_value<T>(
    request: Request,
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
pub(crate) fn records(records: crate::RecordStream) -> crate::RecordStream {
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

pub(crate) fn is_source(path: &crate::StreamPath) -> bool {
    CURRENT.with_borrow(|active| {
        active
            .as_ref()
            .is_some_and(|active| active.producer.is_source(path))
    })
}

/// Observe original synchronous work with optional original comparison evidence.
/// The classifier runs only after admission. Terminal and comparison share one
/// bounded append; neither waits for diagnostic IO before returning the value.
pub fn observe_sync<T>(
    request: Request,
    work: impl FnOnce() -> T,
    _outcome: impl FnOnce(&T) -> Option<(Option<u64>, super::Outcome, Option<super::Comparison>)>,
) -> T {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    {
        use futures::FutureExt as _;
        let recording = child(request)
            .and_then(|admission| admission.now_or_never())
            .and_then(Result::ok);
        let Some(recording) = recording else {
            return work();
        };
        let result = recording.within_sync(work);
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
