#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded test fixtures"
)]
use super::*;
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
};
use tracing_subscriber::{
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};

#[test]
fn canonical_status_and_missing_http_mapping_match_tonic_without_details_parsing() {
    for value in 0_u8..=16 {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", value.to_string().parse().unwrap());
        headers.insert("grpc-status-details-bin", "@@not-base64@@".parse().unwrap());
        assert_eq!(code(Some(&headers), http::StatusCode::BAD_GATEWAY), value);
    }
    for value in [
        b"".as_slice(),
        b"+0",
        b"00",
        b"01",
        b"17",
        b"100",
        b"18446744073709551616",
        b" 0",
        b"0 ",
        b"x",
        &[0xff],
    ] {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", http::HeaderValue::from_bytes(value).unwrap());
        assert_eq!(code(Some(&headers), http::StatusCode::OK), 2, "{value:?}");
    }
    for status in 100..=599 {
        let expected = match status {
            400 => 13,
            401 => 16,
            403 => 7,
            404 => 12,
            429 | 502 | 503 | 504 => 14,
            _ => 2,
        };
        assert_eq!(
            code(None, http::StatusCode::from_u16(status).unwrap()),
            expected
        );
        assert_eq!(
            code(
                Some(&http::HeaderMap::new()),
                http::StatusCode::from_u16(status).unwrap()
            ),
            expected
        );
    }
}

#[derive(Default)]
struct Seen {
    records: Vec<(String, String, String)>,
    closed: Vec<String>,
    events: Vec<(String, String)>,
}
type Log = Arc<Mutex<Seen>>;
struct Capture {
    log: Log,
    filtered: bool,
}
struct Fields(Vec<(String, String)>);
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push((
            field.name().into(),
            format!("{value:?}").trim_matches('"').into(),
        ));
    }
}
impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Capture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: LayerContext<'_, S>) -> bool {
        !self.filtered || metadata.name() != "test.grpc.call"
    }
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: LayerContext<'_, S>,
    ) {
        let mut fields = Fields(Vec::new());
        attrs.record(&mut fields);
        self.log.lock().unwrap().records.extend(
            fields
                .0
                .into_iter()
                .map(|(k, v)| (attrs.metadata().name().into(), k, v)),
        );
    }
    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: LayerContext<'_, S>,
    ) {
        let mut fields = Fields(Vec::new());
        values.record(&mut fields);
        let name = ctx.span(id).unwrap().name();
        self.log
            .lock()
            .unwrap()
            .records
            .extend(fields.0.into_iter().map(|(k, v)| (name.into(), k, v)));
    }
    fn on_close(&self, id: tracing::span::Id, ctx: LayerContext<'_, S>) {
        self.log
            .lock()
            .unwrap()
            .closed
            .push(ctx.span(&id).unwrap().name().into());
    }
    fn on_event(&self, event: &tracing::Event<'_>, ctx: LayerContext<'_, S>) {
        let mut fields = Fields(Vec::new());
        event.record(&mut fields);
        let name = ctx.lookup_current().map_or("none", |s| s.name());
        self.log.lock().unwrap().events.extend(
            fields
                .0
                .into_iter()
                .filter(|(k, _)| k == "step")
                .map(|(_, v)| (v, name.into())),
        );
    }
}
fn capture(filtered: bool) -> (Dispatch, Log) {
    let log = Log::default();
    (
        Dispatch::new(tracing_subscriber::registry().with(Capture {
            log: log.clone(),
            filtered,
        })),
        log,
    )
}
#[derive(Clone)]
struct Family;
impl CallSpan for Family {
    fn span(rpc: Option<&str>) -> Span {
        tracing::info_span!(
            "test.grpc.call",
            rpc,
            rpc.code = tracing::field::Empty,
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty
        )
    }
}
struct Frames {
    frames: VecDeque<Result<http_body::Frame<Bytes>, Status>>,
    end: bool,
    pending: bool,
}
impl http_body::Body for Frames {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        tracing::info!(step = "body.poll");
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(self.frames.pop_front())
        }
    }
    fn is_end_stream(&self) -> bool {
        self.end && self.frames.is_empty()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(123)
    }
}
impl Drop for Frames {
    fn drop(&mut self) {
        tracing::info!(step = "body.drop");
    }
}
#[derive(Debug, PartialEq)]
struct Failure(&'static str);
struct ResponseFuture {
    result: Option<Result<http::Response<Body>, Failure>>,
    pending: bool,
}
impl Future for ResponseFuture {
    type Output = Result<http::Response<Body>, Failure>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        tracing::info!(step = "future.poll");
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(self.result.take().unwrap())
        }
    }
}
impl Drop for ResponseFuture {
    fn drop(&mut self) {
        tracing::info!(step = "future.drop");
    }
}
struct Transport(Option<ResponseFuture>);
impl Service<http::Request<Body>> for Transport {
    type Response = http::Response<Body>;
    type Error = Failure;
    type Future = ResponseFuture;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> ResponseFuture {
        assert_eq!(request.headers()["authorization"], "private-token");
        tracing::info!(step = "service.call");
        self.0.take().unwrap()
    }
}
fn response(
    status: u16,
    header: Option<&'static str>,
    frames: Vec<Result<http_body::Frame<Bytes>, Status>>,
    end: bool,
    pending: bool,
) -> http::Response<Body> {
    let mut r = http::Response::builder()
        .status(status)
        .header("untouched", "value");
    if let Some(code) = header {
        r = r
            .header("grpc-status", code)
            .header("grpc-status-details-bin", "@@@invalid@@@");
    }
    r.body(Body::new(Frames {
        frames: frames.into(),
        end,
        pending,
    }))
    .unwrap()
}
fn call(
    dispatch: &Dispatch,
    result: Result<http::Response<Body>, Failure>,
    pending: bool,
) -> BoxFuture<http::Response<Body>, Failure> {
    tracing::dispatcher::with_default(dispatch, || {
        let parent = tracing::info_span!(
            "caller",
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty,
            rpc.code = tracing::field::Empty
        );
        let _entered = parent.enter();
        TracedChannel::<_, Family>::new(Transport(Some(ResponseFuture {
            result: Some(result),
            pending,
        })))
        .call(
            http::Request::builder()
                .uri("/v1.Service/Call")
                .header("authorization", "private-token")
                .body(Body::empty())
                .unwrap(),
        )
    })
}
fn poll(
    future: &mut BoxFuture<http::Response<Body>, Failure>,
) -> Poll<Result<http::Response<Body>, Failure>> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
}
fn frame(body: &mut Body) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
    Pin::new(body).poll_frame(&mut Context::from_waker(futures::task::noop_waker_ref()))
}
fn terminal(log: &Log, code: Option<u8>, kind: Option<&str>) {
    let log = log.lock().unwrap();
    assert_eq!(
        log.closed
            .iter()
            .filter(|n| n.as_str() == "test.grpc.call")
            .count(),
        1
    );
    let records: Vec<_> = log
        .records
        .iter()
        .filter(|(n, k, _)| n == "test.grpc.call" && k == "outcome")
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records.first().unwrap().2,
        if code == Some(0) { "ok" } else { "err" }
    );
    if let Some(code) = code {
        assert!(
            log.records.iter().any(|(n, k, v)| n == "test.grpc.call"
                && k == "rpc.code"
                && v == &code.to_string())
        );
    }
    if let Some(kind) = kind {
        assert!(
            log.records
                .iter()
                .any(|(n, k, v)| n == "test.grpc.call" && k == "error.kind" && v == kind)
        );
    }
    assert!(!format!("{:?}", log.records).contains("private-token"));
}

#[test]
fn terminal_body_frames_errors_and_eof_preserve_consumption_and_close_before_drop() {
    for (status, header, trailer, expected) in [
        (200, Some("0"), None, 0),
        (200, Some("00"), None, 2),
        (200, None, Some("14"), 14),
        (200, None, None, 2),
        (503, None, None, 14),
    ] {
        let (dispatch, log) = capture(false);
        let (foreign, other) = capture(false);
        let mut frames = vec![Ok(http_body::Frame::data(Bytes::from_static(b"unchanged")))];
        if let Some(value) = trailer {
            let mut h = http::HeaderMap::new();
            h.insert("grpc-status", value.parse().unwrap());
            frames.push(Ok(http_body::Frame::trailers(h)));
        }
        let mut future = call(
            &dispatch,
            Ok(response(status, header, frames, true, false)),
            false,
        );
        let Poll::Ready(Ok(mut r)) =
            tracing::dispatcher::with_default(&foreign, || poll(&mut future))
        else {
            panic!("ready response")
        };
        assert_eq!(r.status().as_u16(), status);
        assert_eq!(r.headers()["untouched"], "value");
        assert_eq!(r.body().size_hint().exact(), Some(123));
        assert!(!r.body().is_end_stream());
        let Poll::Ready(Some(Ok(data))) =
            tracing::dispatcher::with_default(&foreign, || frame(r.body_mut()))
        else {
            panic!("data")
        };
        assert_eq!(data.into_data().unwrap(), Bytes::from_static(b"unchanged"));
        if trailer.is_some() {
            assert!(
                matches!(tracing::dispatcher::with_default(&foreign,||frame(r.body_mut())),Poll::Ready(Some(Ok(f))) if f.is_trailers())
            );
        }
        terminal(&log, Some(expected), (expected != 0).then_some("status"));
        tracing::dispatcher::with_default(&foreign, || drop(r));
        drop(future);
        assert!(other.lock().unwrap().events.is_empty());
        terminal(&log, Some(expected), (expected != 0).then_some("status"));
    }
    let (dispatch, log) = capture(false);
    let mut future = call(
        &dispatch,
        Ok(response(
            200,
            None,
            vec![Err(Status::unavailable("private failure"))],
            false,
            false,
        )),
        false,
    );
    let Poll::Ready(Ok(mut r)) = poll(&mut future) else {
        panic!("ready")
    };
    assert!(
        matches!(frame(r.body_mut()),Poll::Ready(Some(Err(e))) if e.code()==tonic::Code::Unavailable&&e.message()=="private failure")
    );
    terminal(&log, None, Some("transport"));
    drop(r);
    terminal(&log, None, Some("transport"));
}

#[test]
fn origin_dispatch_caller_and_cancellation_survive_foreign_poll_and_drop() {
    for stage in 0..4 {
        let (origin, log) = capture(false);
        let (foreign, other) = capture(false);
        let mut future = call(
            &origin,
            Ok(response(200, None, vec![], false, true)),
            stage < 2,
        );
        tracing::dispatcher::with_default(&foreign, || {
            match stage {
                0 => {}
                1 => assert!(poll(&mut future).is_pending()),
                _ => {
                    let Poll::Ready(Ok(mut r)) = poll(&mut future) else {
                        panic!("ready")
                    };
                    if stage == 2 {
                        assert!(frame(r.body_mut()).is_pending());
                    }
                    drop(r);
                }
            }
            drop(future);
        });
        terminal(&log, None, Some("cancelled"));
        assert!(other.lock().unwrap().events.is_empty());
        let seen = log.lock().unwrap();
        assert!(seen.events.iter().all(|(_, span)| span == "test.grpc.call"));
        assert!(seen.events.iter().any(|(step, _)| step == "future.drop"));
        assert!(seen.events.iter().any(|(step, _)| step == "body.drop"));
    }
}

#[test]
fn filtered_operation_never_records_parent_and_transport_errors_are_unchanged() {
    let (origin, log) = capture(true);
    let (foreign, other) = capture(false);
    let mut future = call(&origin, Ok(response(200, None, vec![], false, true)), false);
    tracing::dispatcher::with_default(&foreign, || {
        let Poll::Ready(Ok(r)) = poll(&mut future) else {
            panic!("ready")
        };
        drop(r);
        drop(future);
    });
    let seen = log.lock().unwrap();
    assert!(
        seen.records.is_empty(),
        "filtered RPC must not write parent's declared fields"
    );
    assert!(seen.events.iter().all(|(_, span)| span == "caller"));
    assert!(other.lock().unwrap().events.is_empty());
    drop(seen);
    let (origin, log) = capture(false);
    let mut future = call(&origin, Err(Failure("same failure")), false);
    assert!(matches!(
        poll(&mut future),
        Poll::Ready(Err(Failure("same failure")))
    ));
    terminal(&log, None, Some("transport"));
    drop(future);
    terminal(&log, None, Some("transport"));
}

#[test]
fn header_ok_waits_for_body_and_missing_status_explicit_eof_is_unknown() {
    for header in [None, Some("0")] {
        let (dispatch, log) = capture(false);
        let mut future = call(
            &dispatch,
            Ok(response(200, header, vec![], false, false)),
            false,
        );
        let Poll::Ready(Ok(mut r)) = poll(&mut future) else {
            panic!("response")
        };
        assert!(
            log.lock()
                .unwrap()
                .closed
                .iter()
                .all(|n| n != "test.grpc.call")
        );
        assert!(matches!(frame(r.body_mut()), Poll::Ready(None)));
        terminal(
            &log,
            Some(if header.is_some() { 0 } else { 2 }),
            header.is_none().then_some("status"),
        );
        drop(r);
    }
    let (dispatch, log) = capture(false);
    let mut future = call(
        &dispatch,
        Ok(response(
            200,
            Some("0"),
            vec![Err(Status::unavailable("same body error"))],
            false,
            false,
        )),
        false,
    );
    let Poll::Ready(Ok(mut r)) = poll(&mut future) else {
        panic!("response")
    };
    assert!(
        matches!(frame(r.body_mut()),Poll::Ready(Some(Err(e))) if e.message()=="same body error")
    );
    terminal(&log, None, Some("transport"));
    drop(r);
    terminal(&log, None, Some("transport"));
    for (status, header, expected) in [
        (200, None, 2),
        (503, None, 14),
        (200, Some("0"), 0),
        (200, Some("+0"), 2),
    ] {
        let (dispatch, log) = capture(false);
        let mut response = http::Response::builder().status(status);
        if let Some(header) = header {
            response = response.header("grpc-status", header);
        }
        let mut future = call(&dispatch, Ok(response.body(Body::empty()).unwrap()), false);
        let Poll::Ready(Ok(r)) = poll(&mut future) else {
            panic!("empty response")
        };
        terminal(&log, Some(expected), (expected != 0).then_some("status"));
        drop(r);
    }
}

struct TypedFuture {
    pending: bool,
    failed: bool,
}
impl Future for TypedFuture {
    type Output = Result<tonic::Response<()>, Status>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        tracing::info!(step = "typed.poll");
        if self.pending {
            Poll::Pending
        } else if self.failed {
            Poll::Ready(Err(Status::internal("private decode failure")))
        } else {
            Poll::Ready(Ok(tonic::Response::new(())))
        }
    }
}
impl Drop for TypedFuture {
    fn drop(&mut self) {
        tracing::info!(step = "typed.drop");
    }
}
#[test]
fn typed_result_and_cancellation_keep_origin_without_mutating_filtered_parent() {
    for filtered in [false, true] {
        for stage in 0..4 {
            let (origin, log) = capture(filtered);
            let (foreign, other) = capture(false);
            let mut future = tracing::dispatcher::with_default(&origin, || {
                let caller = tracing::info_span!(
                    "caller",
                    outcome = tracing::field::Empty,
                    rpc.code = tracing::field::Empty,
                    error.kind = tracing::field::Empty
                );
                let _entered = caller.enter();
                Box::pin(observe_call(
                    Family::span(Some("Typed")),
                    TypedFuture {
                        pending: stage == 1,
                        failed: stage == 2,
                    },
                ))
            });
            tracing::dispatcher::with_default(&foreign, || {
                let mut cx = Context::from_waker(futures::task::noop_waker_ref());
                if stage != 0 {
                    match future.as_mut().poll(&mut cx) {
                        Poll::Pending => assert_eq!(stage, 1),
                        Poll::Ready(Err(error)) => {
                            assert_eq!(stage, 2);
                            assert_eq!(error.message(), "private decode failure");
                        }
                        Poll::Ready(Ok(_)) => assert_eq!(stage, 3),
                    }
                }
                drop(future);
            });
            let seen = log.lock().unwrap();
            assert!(!seen.records.iter().any(|(n, k, _)| n == "caller"
                && ["outcome", "rpc.code", "error.kind"].contains(&k.as_str())));
            if !filtered {
                assert_eq!(
                    seen.closed
                        .iter()
                        .filter(|n| n.as_str() == "test.grpc.call")
                        .count(),
                    1
                );
                let expected = if stage == 3 { "ok" } else { "err" };
                assert_eq!(
                    seen.records
                        .iter()
                        .filter(|(n, k, _)| n == "test.grpc.call" && k == "outcome")
                        .count(),
                    1
                );
                assert!(
                    seen.records
                        .iter()
                        .any(|(n, k, v)| n == "test.grpc.call" && k == "outcome" && v == expected)
                );
                if stage == 2 {
                    assert!(
                        seen.records
                            .iter()
                            .any(|(_, k, v)| k == "rpc.code" && v == "13")
                    );
                }
            }
            assert!(
                seen.events
                    .iter()
                    .all(|(_, n)| n == if filtered { "caller" } else { "test.grpc.call" })
            );
            assert!(other.lock().unwrap().events.is_empty());
        }
    }
}

#[test]
fn rpc_attribution_uses_only_canonical_method_metadata() {
    for method in [Some(tonic::GrpcMethod::new("v1.Service", "Call")), None] {
        let (dispatch, log) = capture(false);
        let mut request = http::Request::builder()
            .uri("/v1.Service/private-uri?query=private-query")
            .header("authorization", "private-token")
            .body(Body::empty())
            .unwrap();
        if let Some(method) = method.clone() {
            request.extensions_mut().insert(method);
        }
        let mut future = tracing::dispatcher::with_default(&dispatch, || {
            TracedChannel::<_, Family>::new(Transport(Some(ResponseFuture {
                result: Some(Err(Failure("same failure"))),
                pending: false,
            })))
            .call(request)
        });
        assert!(matches!(
            poll(&mut future),
            Poll::Ready(Err(Failure("same failure")))
        ));
        drop(future);
        terminal(&log, None, Some("transport"));
        let seen = log.lock().unwrap();
        assert_eq!(
            seen.records
                .iter()
                .any(|(_, k, v)| k == "rpc" && v == "Call"),
            method.is_some()
        );
        for secret in ["private-uri", "private-query", "private-token"] {
            assert!(!format!("{:?}", seen.records).contains(secret));
        }
    }
}
