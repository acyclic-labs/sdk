//! Span helpers for `docs/observability.md`. Each expands to nothing on wasm32,
//! which never links `tracing`.

#[cfg(not(target_arch = "wasm32"))]
use std::{
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::{Span, field::Empty};
/// wasm32 records nothing.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy)]
pub(crate) struct Span;

/// A result with a stable, path-free `error.kind` for a failure.
#[cfg_attr(
    target_arch = "wasm32",
    expect(dead_code, reason = "wasm32 records no outcome")
)]
pub(crate) trait Outcome {
    fn failure(&self) -> Option<&'static str>;
}
impl<T> Outcome for Result<T, crate::Error> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|error| error.code.as_str_name())
    }
}
impl<T> Outcome for Result<T, crate::body::BodyError> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|_| "unavailable")
    }
}
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<T> Outcome for Result<T, crate::physical::PhysicalError> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|error| match error {
            crate::physical::PhysicalError::Invalid(_) => "invalid",
            crate::physical::PhysicalError::Corrupt => "corrupt",
            crate::physical::PhysicalError::Io(_) => "io",
        })
    }
}

/// Keeps an operation incomplete until it returns or its body reaches validated EOF.
#[cfg(not(target_arch = "wasm32"))]
struct Lifetime(Option<Span>);

// Polling and destruction must both use the owner: closing a child may close
// its parent through the current dispatcher. Pin::set drops without moving T.
#[cfg(not(target_arch = "wasm32"))]
pin_project_lite::pin_project! {
    struct Origin<T> {
        #[pin]
        inner: Option<T>,
        dispatch: tracing::Dispatch,
    }
    impl<T> PinnedDrop for Origin<T> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            tracing::dispatcher::with_default(this.dispatch, || this.inner.set(None));
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<T> Origin<T> {
    fn new(inner: T, dispatch: tracing::Dispatch) -> Self {
        Self {
            inner: Some(inner),
            dispatch,
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<F: Future> Future for Origin<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        tracing::dispatcher::with_default(this.dispatch, || {
            this.inner
                .as_pin_mut()
                .map_or(Poll::Pending, |inner| inner.poll(cx))
        })
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<S: futures::Stream> futures::Stream for Origin<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        tracing::dispatcher::with_default(this.dispatch, || {
            this.inner
                .as_pin_mut()
                .map_or(Poll::Ready(None), |inner| inner.poll_next(cx))
        })
    }
}

/// Decoded chunks with their originating dispatcher and optional RPC lifetime.
pub struct BodyStream {
    #[cfg(not(target_arch = "wasm32"))]
    inner: Origin<BodyState>,
    #[cfg(target_arch = "wasm32")]
    inner: futures::stream::BoxStream<'static, Result<bytes::Bytes, crate::Error>>,
}
#[cfg(not(target_arch = "wasm32"))]
struct BodyState {
    body: futures::stream::BoxStream<'static, Result<bytes::Bytes, crate::Error>>,
    lifetime: Lifetime,
    context: Option<Span>,
}
impl From<futures::stream::BoxStream<'static, Result<bytes::Bytes, crate::Error>>> for BodyStream {
    fn from(body: futures::stream::BoxStream<'static, Result<bytes::Bytes, crate::Error>>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        return Self {
            inner: Origin::new(
                BodyState {
                    body,
                    lifetime: Lifetime(None),
                    context: Some(Span::none()),
                },
                tracing::dispatcher::get_default(Clone::clone),
            ),
        };
        #[cfg(target_arch = "wasm32")]
        Self { inner: body }
    }
}
impl futures::Stream for BodyStream {
    type Item = Result<bytes::Bytes, crate::Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl futures::Stream for BodyState {
    type Item = Result<bytes::Bytes, crate::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(span) = this.context.clone() else {
            return Poll::Ready(None);
        };
        let _entered = span.enter();
        let result = this.lifetime.scoped(|| this.body.as_mut().poll_next(cx));
        match &result {
            Poll::Ready(None) => {
                this.lifetime.finish(None);
                this.context = None;
            }
            Poll::Ready(Some(Err(error))) => {
                this.lifetime.finish(Some(error.code.as_str_name()));
                this.context = None;
            }
            _ => {}
        }
        result
    }
}

// Field recording follows the owned operation, even when its span is filtered.
// tracing's current span may be a visible ancestor in that case.
#[cfg(not(target_arch = "wasm32"))]
thread_local! {
    static RECORDING: std::cell::Cell<Option<Span>> = const { std::cell::Cell::new(None) };
}
#[cfg(not(target_arch = "wasm32"))]
struct Recording(Option<Span>);
#[cfg(not(target_arch = "wasm32"))]
impl Recording {
    fn enter(span: Option<Span>) -> Self {
        Self(RECORDING.with(|current| current.replace(span)))
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for Recording {
    fn drop(&mut self) {
        RECORDING.with(|current| current.set(self.0.take()));
    }
}
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn record_on_operation(fields: impl FnOnce(&Span)) {
    let current = Recording(RECORDING.with(std::cell::Cell::take));
    if let Some(span) = &current.0 {
        fields(span);
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Lifetime {
    fn dispatch(&self) -> tracing::Dispatch {
        self.0
            .as_ref()
            .and_then(|span| span.with_subscriber(|(_, dispatch)| dispatch.clone()))
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone))
    }
    fn scoped<R>(&self, work: impl FnOnce() -> R) -> R {
        let _recording = Recording::enter(self.0.clone());
        work()
    }
    fn finish(&mut self, failure: Option<&'static str>) {
        if let Some(span) = self.0.take() {
            match failure {
                None => span.record("outcome", "ok"),
                Some(kind) => span.record("outcome", "err").record("error.kind", kind),
            };
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.finish(Some("cancelled"));
    }
}
/// A native batch fails with its first failed item.
impl<T> Outcome for Vec<Result<T, crate::Error>> {
    fn failure(&self) -> Option<&'static str> {
        self.iter().find_map(Outcome::failure)
    }
}
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<T> Outcome for Result<T, crate::LocalOpenError> {
    fn failure(&self) -> Option<&'static str> {
        use crate::LocalOpenError::{AlreadyOwned, Corrupt, Invalid, Io, Unavailable};
        self.as_ref().err().map(|error| match error {
            Invalid => "invalid",
            Corrupt => "corrupt",
            AlreadyOwned => "already_owned",
            Unavailable => "unavailable",
            Io(_) => "io",
        })
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<T> Outcome for std::io::Result<T> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|_| "io")
    }
}

/// Runs `future` inside `span` and records its outcome there.
pub(crate) fn traced<R: Outcome>(
    span: Span,
    future: impl Future<Output = R>,
) -> impl Future<Output = R> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut lifetime = Lifetime(Some(span.clone()));
    #[cfg(not(target_arch = "wasm32"))]
    let dispatch = lifetime.dispatch();
    #[cfg(not(target_arch = "wasm32"))]
    return Origin::new(
        tracing::Instrument::instrument(
            async move {
                let mut future = std::pin::pin!(future);
                let result =
                    futures::future::poll_fn(|cx| lifetime.scoped(|| future.as_mut().poll(cx)))
                        .await;
                lifetime.finish(result.failure());
                result
            },
            span.or_current(),
        ),
        dispatch,
    );
    #[cfg(target_arch = "wasm32")]
    {
        let Span = span;
        future
    }
}

/// Transfers the RPC lifetime from header validation to the decoded body stream.
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn download(
    span: Span,
    future: impl Future<Output = Result<crate::v1::Download, crate::Error>>,
) -> impl Future<Output = Result<crate::v1::Download, crate::Error>> {
    let mut lifetime = Lifetime(Some(span.clone()));
    let context = span.clone().or_current();
    let dispatch = lifetime.dispatch();
    Origin::new(
        tracing::Instrument::instrument(
            async move {
                let mut future = std::pin::pin!(future);
                let result =
                    futures::future::poll_fn(|cx| lifetime.scoped(|| future.as_mut().poll(cx)))
                        .await;
                match result {
                    Ok(mut download) => {
                        download.body.inner.dispatch = lifetime.dispatch();
                        if let Some(body) = download.body.inner.inner.as_mut() {
                            body.lifetime = lifetime;
                            body.context = Some(context);
                        }
                        Ok(download)
                    }
                    Err(error) => {
                        lifetime.finish(Some(error.code.as_str_name()));
                        Err(error)
                    }
                }
            },
            span.or_current(),
        ),
        dispatch,
    )
}

/// Runs `body` inside `span` and records its outcome there.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) fn scoped<R: Outcome>(span: &Span, body: impl FnOnce() -> R) -> R {
    let mut lifetime = Lifetime(Some(span.clone()));
    tracing::dispatcher::with_default(&lifetime.dispatch(), || {
        span.in_scope(|| {
            let result = lifetime.scoped(body);
            lifetime.finish(result.failure());
            result
        })
    })
}

/// Opens a span at `level` with `outcome`/`error.kind` slots for [`traced`];
/// a unit [`Span`] on wasm32, where the field values are not evaluated.
macro_rules! span {
    ($level:ident, $name:literal $(, $($field:tt)+)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let span = ::tracing::span!(
            ::tracing::Level::$level,
            $name,
            outcome = ::tracing::field::Empty,
            error.kind = ::tracing::field::Empty
            $(, $($field)+)?
        );
        #[cfg(target_arch = "wasm32")]
        let span = crate::obs::Span;
        span
    }};
}
pub(crate) use span;

/// Records fields on the owned operation; only borrows the values on wasm32.
macro_rules! record {
    ($($field:literal = $value:expr),+ $(,)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::obs::record_on_operation(|span| {
                $(span.record($field, $value);)+
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            $(let _ = &$value;)+
        }
    }};
}
pub(crate) use record;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::body::tests::Capture;
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn lifetime_records_unpolled_pending_and_completed_futures() {
        use futures::FutureExt;
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _default = tracing::subscriber::set_default(subscriber);
        drop(traced(
            span!(DEBUG, "acyclic.objects.test.unpolled"),
            std::future::pending::<Result<(), crate::Error>>(),
        ));
        let mut pending = Box::pin(traced(
            span!(DEBUG, "acyclic.objects.test.pending"),
            std::future::pending::<Result<(), crate::Error>>(),
        ));
        assert!(pending.as_mut().now_or_never().is_none());
        drop(pending);
        traced(
            span!(DEBUG, "acyclic.objects.test.ok", items = Empty),
            async {
                let mut nested = Box::pin(traced(
                    span!(DEBUG, "acyclic.objects.test.nested_pending"),
                    std::future::pending::<Result<(), crate::Error>>(),
                ));
                assert!(nested.as_mut().now_or_never().is_none());
                record!("items" = 7_u64);
                drop(nested);
                Ok::<_, crate::Error>(())
            },
        )
        .await
        .unwrap_or_else(|_| unreachable!());
        for name in [
            "acyclic.objects.test.unpolled",
            "acyclic.objects.test.pending",
        ] {
            let records = capture.spans(name);
            assert_eq!(records.len(), 1);
            assert_eq!(
                records
                    .first()
                    .unwrap_or_else(|| unreachable!())
                    .fields
                    .get("error.kind")
                    .map(String::as_str),
                Some("cancelled")
            );
        }
        let records = capture.spans("acyclic.objects.test.ok");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records
                .first()
                .unwrap_or_else(|| unreachable!())
                .fields
                .get("items")
                .map(String::as_str),
            Some("7")
        );
        assert_eq!(
            records
                .first()
                .unwrap_or_else(|| unreachable!())
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("ok")
        );
    }

    #[tokio::test]
    async fn disabled_child_never_records_outcome_on_visible_parent() {
        use tracing_subscriber::filter::LevelFilter;
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry()
            .with(capture.clone())
            .with(LevelFilter::INFO);
        let _default = tracing::subscriber::set_default(subscriber);
        let parent = span!(INFO, "acyclic.objects.test.parent", rpc.code = Empty);
        let _entered = parent.enter();
        let parent_id = parent.id().unwrap_or_else(|| unreachable!()).into_u64();
        let future = traced(span!(DEBUG, "acyclic.objects.test.filtered"), async {
            span!(INFO, "acyclic.objects.test.visible_child").in_scope(|| {
                record!("rpc.code" = 13_u64);
                Err::<(), _>(crate::Error::from(crate::v1::wire::ErrorCode::Unavailable))
            })
        });
        let other = Capture::default();
        let other_default =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(other.clone()));
        assert!(future.await.is_err());
        drop(other_default);
        let children = capture.spans("acyclic.objects.test.visible_child");
        assert_eq!(children.len(), 1);
        assert_eq!(
            children.first().unwrap_or_else(|| unreachable!()).parent_id,
            Some(parent_id)
        );
        assert!(other.spans("acyclic.objects.test.visible_child").is_empty());
        drop(_entered);
        drop(parent);
        let records = capture.spans("acyclic.objects.test.parent");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records.first().unwrap_or_else(|| unreachable!()).parent_id,
            None
        );
        assert!(
            !records
                .first()
                .unwrap_or_else(|| unreachable!())
                .fields
                .contains_key("outcome")
        );
        assert!(
            !records
                .first()
                .unwrap_or_else(|| unreachable!())
                .fields
                .contains_key("error.kind")
        );
    }

    #[tokio::test]
    async fn helper_uses_owned_span_dispatch_when_constructed_elsewhere() {
        let origin = Capture::default();
        let origin_dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(origin.clone()));
        let owned = tracing::dispatcher::with_default(&origin_dispatch, || {
            span!(INFO, "acyclic.objects.test.moved_owner")
        });
        let owner_id = owned.id().unwrap_or_else(|| unreachable!()).into_u64();
        let other = Capture::default();
        let _other_default =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(other.clone()));
        traced(owned, async {
            span!(INFO, "acyclic.objects.test.moved_child").in_scope(|| Ok::<_, crate::Error>(()))
        })
        .await
        .unwrap_or_else(|_| unreachable!());
        let children = origin.spans("acyclic.objects.test.moved_child");
        assert_eq!(children.len(), 1);
        assert_eq!(
            children.first().unwrap_or_else(|| unreachable!()).parent_id,
            Some(owner_id)
        );
        assert!(other.spans("acyclic.objects.test.moved_child").is_empty());
        let owners = origin.spans("acyclic.objects.test.moved_owner");
        assert_eq!(owners.len(), 1);
        assert_eq!(
            owners
                .first()
                .unwrap_or_else(|| unreachable!())
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("ok")
        );
    }

    #[cfg(any(feature = "grpc", feature = "http"))]
    #[tokio::test]
    async fn no_subscriber_body_keeps_origin_when_consumer_enables_tracing() {
        use futures::StreamExt;
        let future = tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
            download(span!(INFO, "acyclic.objects.test.disabled_origin"), async {
                Ok(crate::v1::Download {
                    header: Default::default(),
                    body: futures::stream::iter([Ok(bytes::Bytes::new())])
                        .map(|item| {
                            span!(INFO, "acyclic.objects.test.disabled_decode").in_scope(|| item)
                        })
                        .boxed()
                        .into(),
                })
            })
        });
        let consumer = Capture::default();
        let _foreign =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(consumer.clone()));
        let mut download = future.await.unwrap_or_else(|_| unreachable!());
        assert!(
            download
                .body
                .next()
                .await
                .unwrap_or_else(|| unreachable!())
                .is_ok()
        );
        assert!(download.body.next().await.is_none());
        drop(download);
        assert!(
            consumer
                .spans("acyclic.objects.test.disabled_decode")
                .is_empty()
        );
    }

    #[cfg(any(feature = "grpc", feature = "http"))]
    #[tokio::test]
    async fn foreign_dispatch_future_drop_keeps_unpolled_and_pending_cancellation() {
        use futures::FutureExt;
        let origin = Capture::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(origin.clone()));
        for polled in [false, true] {
            let mut future = Box::pin(tracing::dispatcher::with_default(&dispatch, || {
                let caller = span!(INFO, "acyclic.objects.test.future_drop_caller");
                caller.in_scope(|| {
                    traced(span!(INFO, "acyclic.objects.test.future_drop_rpc"), async {
                        let _child = span!(INFO, "acyclic.objects.test.future_drop_decode");
                        std::future::pending::<Result<(), crate::Error>>().await
                    })
                })
            }));
            let other = Capture::default();
            let _foreign = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(other.clone()),
            );
            if polled {
                assert!(future.as_mut().now_or_never().is_none());
            }
            drop(future);
            assert!(
                other
                    .spans("acyclic.objects.test.future_drop_decode")
                    .is_empty()
            );
            assert!(
                other
                    .spans("acyclic.objects.test.future_drop_caller")
                    .is_empty()
            );
        }
        let rpc = origin.spans("acyclic.objects.test.future_drop_rpc");
        assert_eq!(rpc.len(), 2);
        assert!(
            rpc.iter()
                .all(|span| span.fields.get("error.kind").map(String::as_str) == Some("cancelled"))
        );
        assert_eq!(
            origin
                .spans("acyclic.objects.test.future_drop_decode")
                .len(),
            1
        );
        let callers = origin.spans("acyclic.objects.test.future_drop_caller");
        assert_eq!(callers.len(), 2);
        assert!(
            callers
                .iter()
                .all(|span| !span.fields.contains_key("outcome"))
        );
    }

    #[cfg(any(feature = "grpc", feature = "http"))]
    #[tokio::test]
    async fn foreign_dispatch_body_drop_closes_origin_children_and_cancels_rpc() {
        use futures::{FutureExt, StreamExt};
        use tracing_subscriber::filter::LevelFilter;
        for filtered in [false, true] {
            let origin = Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry()
                    .with(origin.clone())
                    .with(LevelFilter::INFO),
            );
            let future = tracing::dispatcher::with_default(&dispatch, || {
                let caller = span!(INFO, "acyclic.objects.test.drop_caller");
                caller.in_scope(|| {
                    let rpc = if filtered {
                        span!(DEBUG, "acyclic.objects.test.drop_rpc")
                    } else {
                        span!(INFO, "acyclic.objects.test.drop_rpc")
                    };
                    download(rpc, async {
                        // This retained child makes the parent's final close occur on drop.
                        let child = span!(INFO, "acyclic.objects.test.drop_decode");
                        Ok(crate::v1::Download {
                            header: Default::default(),
                            body: futures::stream::poll_fn(move |_| {
                                let _held = &child;
                                std::task::Poll::Pending
                            })
                            .boxed()
                            .into(),
                        })
                    })
                })
            });
            let other = Capture::default();
            let _foreign = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(other.clone()),
            );
            let mut download = future.await.unwrap_or_else(|_| unreachable!());
            assert!(download.body.next().now_or_never().is_none());
            // Deliberately do not install the source dispatcher to hide a destructor bug.
            drop(download);
            let rpc = origin.spans("acyclic.objects.test.drop_rpc");
            let callers = origin.spans("acyclic.objects.test.drop_caller");
            assert_eq!(callers.len(), 1);
            let caller = callers.first().unwrap_or_else(|| unreachable!());
            assert!(!caller.fields.contains_key("outcome"));
            let parent = if filtered {
                assert!(rpc.is_empty());
                caller.id
            } else {
                assert_eq!(rpc.len(), 1);
                let rpc = rpc.first().unwrap_or_else(|| unreachable!());
                assert_eq!(
                    rpc.fields.get("error.kind").map(String::as_str),
                    Some("cancelled")
                );
                rpc.id
            };
            let children = origin.spans("acyclic.objects.test.drop_decode");
            assert_eq!(children.len(), 1);
            assert_eq!(
                children.first().unwrap_or_else(|| unreachable!()).parent_id,
                Some(parent)
            );
            assert!(other.spans("acyclic.objects.test.drop_decode").is_empty());
            assert!(other.spans("acyclic.objects.test.drop_caller").is_empty());
        }
    }

    #[cfg(any(feature = "grpc", feature = "http"))]
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one fixture checks filtered dispatch, parent ownership and sealed terminal paths"
    )]
    async fn download_keeps_origin_dispatch_and_parent_with_filtered_rpc() {
        use futures::StreamExt;
        use tracing::Instrument;
        use tracing_subscriber::filter::LevelFilter;
        for (filtered, fails) in [(false, false), (true, false), (false, true), (true, true)] {
            let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let body_polls = polls.clone();
            let origin = Capture::default();
            let other = Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry()
                    .with(origin.clone())
                    .with(LevelFilter::INFO),
            );
            let (future, creator_id) = tracing::dispatcher::with_default(&dispatch, || {
                let creator = span!(INFO, "acyclic.objects.test.creator");
                let id = creator.id().unwrap_or_else(|| unreachable!()).into_u64();
                let future = creator.in_scope(|| {
                    let rpc = if filtered {
                        span!(DEBUG, "acyclic.objects.test.rpc")
                    } else {
                        span!(INFO, "acyclic.objects.test.rpc")
                    };
                    super::download(rpc, async move {
                        let body = futures::stream::poll_fn(move |_| {
                            let poll = body_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            // Deliberately non-fused: it can decode again after EOF/error.
                            let item = if poll == 1 {
                                None
                            } else {
                                Some(span!(INFO, "acyclic.objects.test.decode").in_scope(|| {
                                    if fails {
                                        Err(crate::Error::from(
                                            crate::v1::wire::ErrorCode::Unavailable,
                                        ))
                                    } else {
                                        Ok(bytes::Bytes::from_static(b"data"))
                                    }
                                }))
                            };
                            std::task::Poll::Ready(item)
                        })
                        .boxed();
                        Ok(crate::v1::Download {
                            header: Default::default(),
                            body: body.into(),
                        })
                    })
                });
                (future, id)
            });
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(other.clone()),
            );
            let consumer = span!(INFO, "acyclic.objects.test.consumer");
            let mut download = future
                .instrument(consumer.clone())
                .await
                .unwrap_or_else(|_| unreachable!());
            assert!(
                download
                    .body
                    .next()
                    .instrument(consumer.clone())
                    .await
                    .unwrap_or_else(|| unreachable!())
                    .is_err()
                    == fails
            );
            assert!(
                download
                    .body
                    .next()
                    .instrument(consumer.clone())
                    .await
                    .is_none()
            );
            let terminal = origin.spans("acyclic.objects.test.rpc");
            let terminal_fields: Vec<_> = terminal.iter().map(|span| span.fields.clone()).collect();
            for _ in 0..3 {
                assert!(download.body.next().await.is_none());
            }
            assert_eq!(
                polls.load(std::sync::atomic::Ordering::SeqCst),
                if fails { 1 } else { 2 }
            );
            let after = origin.spans("acyclic.objects.test.rpc");
            assert_eq!(
                after
                    .iter()
                    .map(|span| span.fields.clone())
                    .collect::<Vec<_>>(),
                terminal_fields
            );
            drop(download);
            let rpc = origin.spans("acyclic.objects.test.rpc");
            let parent_id = if filtered {
                assert!(rpc.is_empty());
                creator_id
            } else {
                assert_eq!(rpc.len(), 1);
                let rpc = rpc.first().unwrap_or_else(|| unreachable!());
                assert_eq!(rpc.parent_id, Some(creator_id));
                assert_eq!(
                    rpc.fields.get("outcome").map(String::as_str),
                    Some(if fails { "err" } else { "ok" })
                );
                rpc.id
            };
            let decode = origin.spans("acyclic.objects.test.decode");
            assert_eq!(decode.len(), 1);
            assert_eq!(
                decode.first().unwrap_or_else(|| unreachable!()).parent_id,
                Some(parent_id)
            );
            assert!(other.spans("acyclic.objects.test.decode").is_empty());
            let creators = origin.spans("acyclic.objects.test.creator");
            assert_eq!(creators.len(), 1);
            assert!(
                !creators
                    .first()
                    .unwrap_or_else(|| unreachable!())
                    .fields
                    .contains_key("outcome")
            );
        }
    }
}
