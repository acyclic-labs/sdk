//! Shared typed-call and transport/body lifetime observation for SDK gRPC clients.
//! Transport status and decoded typed-call results are recorded separately.
//! Explicit trailer status is wire evidence even when Tonic uses header OK;
//! trailers without status preserve a prior header status.

use http_body::Body as _;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{
    Status,
    body::Body,
    codegen::{BoxFuture, Bytes, Service, http},
};
use tracing::{Dispatch, Span};

/// Supplies a family's statically named call span without dynamic registration.
pub trait CallSpan: Clone {
    /// Opens the RPC span in the originating caller's context.
    fn span(rpc: Option<&str>) -> Span;
}

/// A channel whose call span closes exactly once at terminal status or cancellation.
#[derive(Clone, Debug)]
pub struct TracedChannel<S, F>(S, PhantomData<F>);

impl<S, F> TracedChannel<S, F> {
    /// Wraps one transport without changing its requests or errors.
    pub fn new(channel: S) -> Self {
        Self(channel, PhantomData)
    }
}

impl<S, F> Service<http::Request<Body>> for TracedChannel<S, F>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
    S::Error: 'static,
    F: CallSpan,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let origin = Origin {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            caller: Span::current(),
        };
        let span = F::span(
            request
                .extensions()
                .get::<tonic::GrpcMethod<'static>>()
                .map(tonic::GrpcMethod::method),
        );
        let future = origin.run(|| {
            let _entered = span.enter();
            self.0.call(request)
        });
        Box::pin(Observed {
            inner: Some(future),
            origin: Some(origin),
            span: Some(span),
            http_status: http::StatusCode::OK,
            header_code: None,
        })
    }
}

struct Origin {
    dispatch: Dispatch,
    caller: Span,
}
impl Origin {
    fn run<R>(&self, action: impl FnOnce() -> R) -> R {
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        let _caller = self.caller.enter();
        action()
    }
}

pin_project_lite::pin_project! {
    // One ownership mechanism for both the response future and its returned body.
    struct Observed<T> {
        #[pin]
        inner: Option<T>,
        origin: Option<Origin>,
        span: Option<Span>,
        http_status: http::StatusCode,
        header_code: Option<u8>,
    }

    impl<T> PinnedDrop for Observed<T> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            if let Some(Origin { dispatch, caller }) = this.origin.take() {
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _caller = caller.entered();
                { let _entered = this.span.as_ref().map(Span::enter); this.inner.set(None); }
                finish(this.span, Err("cancelled"));
            }
        }
    }
}

/// Observes the actual typed result, including readiness, encoding and decoding.
/// Cancellation drops the operation in its originating tracing context.
pub fn observe_call<M>(
    span: Span,
    future: impl std::future::Future<Output = Result<tonic::Response<M>, Status>>,
) -> impl std::future::Future<Output = Result<tonic::Response<M>, Status>> {
    Observed {
        inner: Some(future),
        origin: Some(Origin {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            caller: Span::current(),
        }),
        span: Some(span),
        http_status: http::StatusCode::OK,
        header_code: None,
    }
}

// Static completion preserves one poll/drop mechanism for both boundary types.
trait ObservedOutput: Sized {
    fn complete(self, origin: &mut Option<Origin>, span: &mut Option<Span>) -> Self;
}
impl<M> ObservedOutput for Result<tonic::Response<M>, Status> {
    fn complete(self, origin: &mut Option<Origin>, span: &mut Option<Span>) -> Self {
        if let Some(origin) = origin.as_ref() {
            origin.run(|| {
                finish(
                    span,
                    Ok(self.as_ref().map_or_else(|e| e.code() as u8, |_| 0)),
                );
            });
        }
        self
    }
}
impl<E> ObservedOutput for Result<http::Response<Body>, E> {
    fn complete(self, origin: &mut Option<Origin>, span: &mut Option<Span>) -> Self {
        match self {
            Err(error) => {
                if let Some(origin) = origin.as_ref() {
                    origin.run(|| finish(span, Err("transport")));
                }
                Err(error)
            }
            Ok(response) => {
                let status = response.status();
                let header_code = response
                    .headers()
                    .contains_key("grpc-status")
                    .then(|| code(Some(response.headers()), status));
                // Header OK does not suppress later body failure/cancellation.
                if (response.body().is_end_stream() || header_code.is_some_and(|code| code != 0))
                    && let Some(origin) = origin.as_ref()
                {
                    origin.run(|| {
                        finish(span, Ok(header_code.unwrap_or_else(|| code(None, status))));
                    });
                }
                Ok(response.map(|body| {
                    Body::new(Observed {
                        inner: Some(body),
                        origin: origin.take(),
                        span: span.take(),
                        http_status: status,
                        header_code,
                    })
                }))
            }
        }
    }
}
impl<T: std::future::Future> std::future::Future for Observed<T>
where
    T::Output: ObservedOutput,
{
    type Output = T::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(origin) = this.origin.as_ref() else {
            return Poll::Pending;
        };
        let result = origin.run(|| {
            let _entered = this.span.as_ref().map(Span::enter);
            let Some(inner) = this.inner.as_mut().as_pin_mut() else {
                return Poll::Pending;
            };
            let result = std::task::ready!(inner.poll(cx));
            this.inner.set(None);
            Poll::Ready(result)
        });
        Poll::Ready(std::task::ready!(result).complete(this.origin, this.span))
    }
}

impl http_body::Body for Observed<Body> {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        let mut this = self.project();
        let Some(origin) = this.origin.as_ref() else {
            return Poll::Ready(None);
        };
        origin.run(|| {
            let frame = {
                let _entered = this.span.as_ref().map(Span::enter);
                let Some(inner) = this.inner.as_mut().as_pin_mut() else {
                    return Poll::Ready(None);
                };
                std::task::ready!(inner.poll_frame(cx))
            };
            match &frame {
                Some(Err(_)) => finish(this.span, Err("transport")),
                Some(Ok(frame)) if frame.is_trailers() => {
                    let explicit = frame
                        .trailers_ref()
                        .filter(|h| h.contains_key("grpc-status"));
                    let terminal = explicit.map_or_else(
                        || {
                            this.header_code
                                .unwrap_or_else(|| code(None, *this.http_status))
                        },
                        |headers| code(Some(headers), *this.http_status),
                    );
                    finish(this.span, Ok(terminal));
                }
                _ if frame.is_none()
                    || this
                        .inner
                        .as_ref()
                        .get_ref()
                        .as_ref()
                        .is_some_and(Body::is_end_stream) =>
                {
                    finish(
                        this.span,
                        Ok(this
                            .header_code
                            .unwrap_or_else(|| code(None, *this.http_status))),
                    );
                }
                _ => {}
            }
            Poll::Ready(frame)
        })
    }
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(Body::is_end_stream)
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner
            .as_ref()
            .map_or_else(http_body::SizeHint::default, Body::size_hint)
    }
}

// Parse only canonical status bytes; never touch grpc-message/details or payloads.
fn code(headers: Option<&http::HeaderMap>, status: http::StatusCode) -> u8 {
    if let Some(value) = headers.and_then(|h| h.get("grpc-status")) {
        return match value.as_bytes() {
            [digit @ b'0'..=b'9'] => digit - b'0',
            [b'1', digit @ b'0'..=b'6'] => 10 + digit - b'0',
            _ => 2,
        };
    }
    match status.as_u16() {
        400 => 13,
        401 => 16,
        403 => 7,
        404 => 12,
        429 | 502 | 503 | 504 => 14,
        _ => 2,
    }
}

fn finish(span: &mut Option<Span>, terminal: Result<u8, &'static str>) {
    if let Some(span) = span.take() {
        match terminal {
            Ok(code) => {
                span.record("rpc.code", code)
                    .record("outcome", if code == 0 { "ok" } else { "err" });
                if code != 0 {
                    span.record("error.kind", "status");
                }
            }
            Err(kind) => {
                span.record("outcome", "err").record("error.kind", kind);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
