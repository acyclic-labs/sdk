//! Shared SDK test fixtures and gRPC observation tests.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::type_complexity,
    reason = "test capture invariants fail immediately; plain fixture tuples avoid forwarding types"
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing_subscriber::{layer::Context, registry::LookupSpan};

/// A historical span, retained when registry IDs are reused.
#[derive(Clone, Debug, Default)]
pub struct CapturedSpan {
    /// Static operation name.
    pub name: &'static str,
    /// Registry identity.
    pub id: u64,
    /// Registry identity of the original parent.
    pub parent_id: Option<u64>,
    /// Historical index of the original parent.
    pub parent: Option<usize>,
    /// Every declared field, including fields never recorded.
    pub declared_fields: Vec<&'static str>,
    /// Latest recorded values, with strings unquoted.
    pub fields: BTreeMap<String, String>,
    /// Whether the final span reference has retired.
    pub closed: bool,
}

/// Collected span history, field updates, events and closes.
#[derive(Default)]
pub struct Evidence {
    /// Span history in creation order.
    pub spans: Vec<CapturedSpan>,
    live: HashMap<u64, usize>,
    /// Field updates as operation, field and debug value.
    pub records: Vec<(String, String, String)>,
    /// Closed operation names in retirement order.
    pub closes: Vec<String>,
    /// Parent of the last observed event.
    pub event_parent: Option<String>,
    /// Event parents and debug fields.
    pub events: Vec<(Option<String>, Vec<(String, String, String)>)>,
}

/// A cloneable capture layer and asynchronous change notification.
#[derive(Clone, Default)]
pub struct Capture(
    /// Collected history.
    pub Arc<Mutex<Evidence>>,
    /// Notification after a field update or close.
    pub Arc<tokio::sync::Notify>,
);

impl tracing::field::Visit for CapturedSpan {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}

struct DebugFields<'a>(&'a mut Vec<(String, String, String)>, &'a str);
impl tracing::field::Visit for DebugFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((self.1.into(), field.name().into(), format!("{value:?}")));
    }
}

impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let parent_id = ctx
            .span(id)
            .and_then(|span| span.parent().map(|parent| parent.id().into_u64()));
        let mut evidence = self.0.lock().unwrap();
        let mut captured = CapturedSpan {
            name: attrs.metadata().name(),
            id: id.into_u64(),
            parent_id,
            parent: parent_id.and_then(|id| evidence.live.get(&id).copied()),
            declared_fields: attrs
                .metadata()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect(),
            ..Default::default()
        };
        attrs.record(&mut captured);
        let index = evidence.spans.len();
        evidence.spans.push(captured);
        evidence.live.insert(id.into_u64(), index);
    }
    fn on_record(&self, id: &tracing::Id, record: &tracing::span::Record<'_>, _: Context<'_, S>) {
        let mut evidence = self.0.lock().unwrap();
        let index = evidence.live[&id.into_u64()];
        let name = evidence.spans[index].name;
        record.record(&mut evidence.spans[index]);
        record.record(&mut DebugFields(&mut evidence.records, name));
        self.1.notify_one();
    }
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let parent = ctx.event_span(event).map(|span| span.name().into());
        let mut fields = Vec::new();
        event.record(&mut DebugFields(&mut fields, event.metadata().target()));
        let mut evidence = self.0.lock().unwrap();
        evidence.event_parent = parent.clone();
        evidence.events.push((parent, fields));
    }
    fn on_close(&self, id: tracing::Id, _: Context<'_, S>) {
        let mut evidence = self.0.lock().unwrap();
        let index = evidence.live.remove(&id.into_u64()).unwrap();
        evidence.spans[index].closed = true;
        let name = evidence.spans[index].name.into();
        evidence.closes.push(name);
        self.1.notify_one();
    }
}

impl Capture {
    /// Parent name of the first matching span.
    pub fn parent(&self, name: &str) -> Option<&'static str> {
        let evidence = self.0.lock().unwrap();
        evidence
            .spans
            .iter()
            .find(|span| span.name == name)
            .and_then(|span| span.parent)
            .map(|index| evidence.spans[index].name)
    }
    /// Whether the first matching span has the named ancestor.
    pub fn has_ancestor(&self, name: &str, ancestor: &str) -> bool {
        let evidence = self.0.lock().unwrap();
        let mut parent = evidence
            .spans
            .iter()
            .find(|span| span.name == name)
            .and_then(|span| span.parent);
        while let Some(index) = parent {
            let span = &evidence.spans[index];
            if span.name == ancestor {
                return true;
            }
            parent = span.parent;
        }
        false
    }
    /// Number of retired matching spans.
    pub fn closed(&self, name: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name && span.closed)
            .count()
    }
    /// Latest fields of every matching span.
    pub fn all(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name)
            .map(|span| span.fields.clone())
            .collect()
    }
    /// Latest fields of the first matching span.
    pub fn fields(&self, name: &str) -> BTreeMap<String, String> {
        self.all(name).into_iter().next().unwrap_or_default()
    }
    /// Retired matching spans in creation order.
    pub fn spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name && span.closed)
            .cloned()
            .collect()
    }
}

/// A gRPC body that emits one unavailable trailer after 50 ms.
pub fn delayed_unavailable_body() -> tonic::body::Body {
    tonic::body::Body::new(Trailers(Some(Box::pin(tokio::time::sleep(
        std::time::Duration::from_millis(50),
    )))))
}

struct Trailers(Option<std::pin::Pin<Box<tokio::time::Sleep>>>);
impl http_body::Body for Trailers {
    type Data = tonic::codegen::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::{future::Future as _, task::Poll};
        let Some(sleep) = self.0.as_mut() else {
            return Poll::Ready(None);
        };
        std::task::ready!(sleep.as_mut().poll(cx));
        self.0 = None;
        let mut trailers = tonic::codegen::http::HeaderMap::new();
        trailers.insert("grpc-status", 14.into());
        Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))))
    }
}

/// Re-executes one TLS test in isolated trust environments, or returns its PEM inputs and mode.
pub fn tls_fixture(
    module: &str,
) -> Result<Option<(String, String, Vec<u8>, String)>, Box<dyn std::error::Error + Send + Sync>> {
    const FIXTURE: &str = "ACYCLIC_TLS_FIXTURE";
    const MODE: &str = "ACYCLIC_TLS_MODE";
    let Ok(directory) = std::env::var(FIXTURE) else {
        let directory = tempfile::tempdir()?;
        let trusted = directory.path().join("trusted");
        std::fs::create_dir(&trusted)?;
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        std::fs::write(trusted.join("root.pem"), certified.cert.pem())?;
        std::fs::write(
            directory.path().join("key.pem"),
            certified.signing_key.serialize_pem(),
        )?;
        let unrelated = rcgen::generate_simple_self_signed(["unrelated".to_owned()])?;
        std::fs::write(directory.path().join("unrelated.pem"), unrelated.cert.pem())?;
        std::fs::write(directory.path().join("empty.pem"), "")?;
        let test = format!(
            "{}::platform_roots_and_exclusive_ca",
            module.split_once("::").map_or(module, |(_, path)| path)
        );
        let modes = if cfg!(target_os = "linux") {
            &["unrelated", "file", "dir", "empty"][..]
        } else {
            &["unrelated"][..]
        };
        for mode in modes {
            let mut child = std::process::Command::new(std::env::current_exe()?);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt as _;
                child.creation_flags(0x08000000);
            }
            child
                .args([test.as_str(), "--exact", "--nocapture"])
                .env(FIXTURE, directory.path())
                .env(MODE, mode)
                .env_remove("SSL_CERT_FILE")
                .env_remove("SSL_CERT_DIR");
            match *mode {
                "file" => {
                    child.env("SSL_CERT_FILE", trusted.join("root.pem"));
                }
                "dir" => {
                    child.env("SSL_CERT_DIR", &trusted);
                }
                "empty" => {
                    child.env("SSL_CERT_FILE", directory.path().join("empty.pem"));
                }
                _ => {
                    child.env("SSL_CERT_FILE", directory.path().join("unrelated.pem"));
                }
            }
            let output = child.output()?;
            assert!(
                output.status.success(),
                "{mode}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return Ok(None);
    };
    let directory = std::path::Path::new(&directory);
    Ok(Some((
        std::fs::read_to_string(directory.join("trusted/root.pem"))?,
        std::fs::read_to_string(directory.join("key.pem"))?,
        std::fs::read(directory.join("unrelated.pem"))?,
        std::env::var(MODE)?,
    )))
}

#[cfg(all(test, feature = "runtime"))]
mod runtime {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        reason = "bounded test fixtures"
    )]
    use super::Capture;
    use crate::{TracedChannel, observe_call, runtime::code};
    use http_body::Body as _;
    use std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    use tonic::{
        Status,
        body::Body,
        codegen::{BoxFuture, Bytes, Service, http},
    };
    use tracing::{Dispatch, Span};
    use tracing_subscriber::{filter::filter_fn, prelude::*};

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

    fn capture(filtered: bool) -> (Dispatch, Capture) {
        let log = Capture::default();
        let layer = log.clone().with_filter(filter_fn(move |metadata| {
            !filtered || metadata.name() != "test.grpc.call"
        }));
        (
            Dispatch::new(tracing_subscriber::registry().with(layer)),
            log,
        )
    }
    fn call_span(rpc: Option<&str>) -> Span {
        tracing::info_span!(
            "test.grpc.call",
            rpc,
            rpc.code = tracing::field::Empty,
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty
        )
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
            TracedChannel::new(
                Transport(Some(ResponseFuture {
                    result: Some(result),
                    pending,
                })),
                call_span,
            )
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
    fn terminal(log: &Capture, code: Option<u8>, kind: Option<&str>) {
        let log = log.0.lock().unwrap();
        assert_eq!(
            log.closes
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
            records.first().unwrap().2.trim_matches('"'),
            if code == Some(0) { "ok" } else { "err" }
        );
        if let Some(code) = code {
            assert!(log.records.iter().any(|(n, k, v)| n == "test.grpc.call"
                && k == "rpc.code"
                && v == &code.to_string()));
        }
        if let Some(kind) = kind {
            assert!(log.records.iter().any(|(n, k, v)| n == "test.grpc.call"
                && k == "error.kind"
                && v.trim_matches('"') == kind));
        }
        assert!(!format!("{:?}{:?}", log.records, log.spans).contains("private-token"));
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
            assert!(other.0.lock().unwrap().events.is_empty());
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
            assert!(other.0.lock().unwrap().events.is_empty());
            let seen = log.0.lock().unwrap();
            assert!(
                seen.events
                    .iter()
                    .all(|(parent, _)| parent.as_deref() == Some("test.grpc.call"))
            );
            for step in ["future.drop", "body.drop"] {
                assert!(seen.events.iter().any(|(_, fields)| {
                    fields
                        .iter()
                        .any(|(_, name, value)| name == "step" && value.trim_matches('"') == step)
                }));
            }
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
        let seen = log.0.lock().unwrap();
        assert!(
            seen.records.is_empty(),
            "filtered RPC must not write parent's declared fields"
        );
        assert!(
            seen.events
                .iter()
                .all(|(parent, _)| parent.as_deref() == Some("caller"))
        );
        assert!(other.0.lock().unwrap().events.is_empty());
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
                log.0
                    .lock()
                    .unwrap()
                    .closes
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
                        call_span(Some("Typed")),
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
                let seen = log.0.lock().unwrap();
                assert!(!seen.records.iter().any(|(n, k, _)| n == "caller"
                    && ["outcome", "rpc.code", "error.kind"].contains(&k.as_str())));
                if !filtered {
                    assert_eq!(
                        seen.closes
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
                    assert!(seen.records.iter().any(|(n, k, v)| n == "test.grpc.call"
                        && k == "outcome"
                        && v.trim_matches('"') == expected));
                    if stage == 2 {
                        assert!(
                            seen.records
                                .iter()
                                .any(|(_, k, v)| k == "rpc.code" && v == "13")
                        );
                    }
                }
                assert!(seen.events.iter().all(|(parent, _)| parent.as_deref()
                    == Some(if filtered { "caller" } else { "test.grpc.call" })));
                assert!(other.0.lock().unwrap().events.is_empty());
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
                TracedChannel::new(
                    Transport(Some(ResponseFuture {
                        result: Some(Err(Failure("same failure"))),
                        pending: false,
                    })),
                    call_span,
                )
                .call(request)
            });
            assert!(matches!(
                poll(&mut future),
                Poll::Ready(Err(Failure("same failure")))
            ));
            drop(future);
            terminal(&log, None, Some("transport"));
            let seen = log.0.lock().unwrap();
            assert_eq!(
                seen.spans
                    .iter()
                    .any(|span| span.fields.get("rpc").is_some_and(|v| v == "Call")),
                method.is_some()
            );
            for secret in ["private-uri", "private-query", "private-token"] {
                assert!(!format!("{:?}{:?}", seen.records, seen.spans).contains(secret));
            }
        }
    }
}
