//! Causal custody follows an admitted future's poll, never a worker thread.
use super::{OwnedRecording, Request, SpanId};
use crate::{BoxProviderFuture, ProviderPlatform, StreamError};
use std::{cell::RefCell, future::Future, sync::Arc};

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

async fn scoped<F: Future>(active: Option<Active>, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    futures::future::poll_fn(|cx| {
        let _restore = Restore(CURRENT.replace(active.clone()));
        future.as_mut().poll(cx)
    })
    .await
}

pub(super) async fn within<F: Future>(
    id: SpanId,
    producer: Arc<dyn Producer>,
    future: F,
) -> F::Output {
    scoped(Some(Active { id, producer }), future).await
}

pub(super) async fn disabled<F: Future>(future: F) -> F::Output {
    scoped(None, future).await
}

/// Invoke the original work under the active owner's child span. Without an
/// admitted owner this performs one local context check, with no allocation,
/// clock, subscription lookup or diagnostic IO. Publication errors never replace
/// the original result; unretained failures remain explicitly unfinished.
pub async fn observe<T, E>(
    request: Request,
    work: impl Future<Output = Result<T, E>>,
    bytes: impl FnOnce(&T) -> Option<u64>,
) -> Result<T, E> {
    observe_outcome(request, work, |result| {
        result
            .as_ref()
            .ok()
            .map(|value| (bytes(value), super::Outcome::Succeeded))
    })
    .await
}

/// Classify the original result only for an admitted span. Failed classification
/// still requires the original retained cause through Outcome's type. No
/// classifier or terminal publication is evaluated on an unobserved request.
pub async fn observe_outcome<T, E>(
    request: Request,
    work: impl Future<Output = Result<T, E>>,
    _outcome: impl FnOnce(&Result<T, E>) -> Option<(Option<u64>, super::Outcome)>,
) -> Result<T, E> {
    let (result, recording) = OwnedRecording::accompany(child(request), work).await;
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    if let Some(recording) = recording
        && let Some((bytes, outcome)) = _outcome(&result)
    {
        let _completion = recording.finish_submitted(bytes, outcome);
    }
    result
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
    records.map_cursor(|cursor| {
        Box::pin(futures::stream::unfold(
            (cursor, active),
            |(mut cursor, active)| async move {
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
                        let _completion =
                            recording.finish_submitted(bytes, super::Outcome::Succeeded);
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
