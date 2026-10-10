use super::{
    Error, HTTP_ROUTES, MemoryObjects, MemoryOptions, ObjectsProvider, http::HttpObjects, json,
    wire,
};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
};
use bytes::{Bytes, BytesMut};
use futures::stream;
use prost::Message;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Fixture {
    objects: MemoryObjects,
    calls: Arc<Mutex<std::collections::BTreeSet<String>>>,
}
fn invalid() -> Error {
    wire::ErrorCode::InvalidArgument.into()
}
fn framed(name: &str, value: &impl Message) -> Result<Vec<u8>, Error> {
    let mut bytes = json::encode(name, value)?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn frames<T: Message + Default>(name: &str, bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut frames = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if line.len() > 128 * 1024 {
            return Err(invalid());
        }
        let line = line.strip_suffix(b"\n").ok_or_else(invalid)?;
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        frames.push(json::decode(name, line, 128 * 1024)?);
    }
    Ok(frames)
}
#[allow(
    clippy::too_many_lines,
    reason = "explicit fixture dispatch covers every canonical route in one inventory"
)]
async fn operation(
    fixture: &Fixture,
    route: &str,
    bytes: &[u8],
) -> Result<(&'static str, Vec<u8>), Error> {
    let (_, input, output) = HTTP_ROUTES
        .iter()
        .find(|(path, _, _)| *path == route)
        .ok_or_else(invalid)?;
    macro_rules! unary {
        ($input:ty, $method:ident) => {{
            let query: $input = json::decode(input, bytes, 12 * 1024 * 1024)?;
            let result = fixture.objects.$method(query).await?;
            Ok(("application/json", json::encode(output, &result)?))
        }};
    }
    match route {
        "buckets/create" => unary!(wire::CreateBucketRequest, create_bucket),
        "buckets/head" => unary!(wire::HeadBucketRequest, head_bucket),
        "buckets/delete" => unary!(wire::DeleteBucketRequest, delete_bucket),
        "objects/head" => unary!(wire::HeadObjectRequest, head),
        "objects/delete" => unary!(wire::DeleteObjectRequest, delete),
        "objects/list" => unary!(wire::ListObjectsRequest, list),
        "multipart/create" => unary!(wire::CreateMultipartRequest, create_multipart),
        "multipart/list-parts" => unary!(wire::ListPartsRequest, list_parts),
        "multipart/complete" => unary!(wire::CompleteMultipartRequest, complete_multipart),
        "multipart/abort" => unary!(wire::AbortMultipartRequest, abort_multipart),
        "objects/put" => {
            let mut frames = frames::<wire::PutObjectRequest>(input, bytes)?.into_iter();
            let Some(wire::PutObjectRequest {
                frame: Some(wire::put_object_request::Frame::Header(header)),
            }) = frames.next()
            else {
                return Err(invalid());
            };
            let mut body = BytesMut::new();
            let mut framing = super::request::UploadFraming::default();
            for frame in frames {
                match frame.frame {
                    Some(wire::put_object_request::Frame::Body(bytes)) => {
                        framing.body(&bytes)?;
                        body.extend_from_slice(&bytes);
                    }
                    Some(wire::put_object_request::Frame::Complete(complete)) => {
                        framing.complete(complete)?;
                    }
                    _ => return Err(invalid()),
                }
            }
            framing.finish()?;
            let result = fixture.objects.put(header, body.freeze()).await?;
            Ok(("application/json", json::encode(output, &result)?))
        }
        "multipart/upload-part" => {
            let mut frames = frames::<wire::UploadPartRequest>(input, bytes)?.into_iter();
            let Some(wire::UploadPartRequest {
                frame: Some(wire::upload_part_request::Frame::Header(header)),
            }) = frames.next()
            else {
                return Err(invalid());
            };
            let mut body = BytesMut::new();
            let mut framing = super::request::UploadFraming::default();
            for frame in frames {
                match frame.frame {
                    Some(wire::upload_part_request::Frame::Body(bytes)) => {
                        framing.body(&bytes)?;
                        body.extend_from_slice(&bytes);
                    }
                    Some(wire::upload_part_request::Frame::Complete(complete)) => {
                        framing.complete(complete)?;
                    }
                    _ => return Err(invalid()),
                }
            }
            framing.finish()?;
            let result = fixture.objects.upload_part(header, body.freeze()).await?;
            Ok(("application/json", json::encode(output, &result)?))
        }
        "objects/get" => {
            let query: wire::GetObjectRequest = json::decode(input, bytes, 128 * 1024)?;
            let mode = query.object_key.clone();
            let mut selected = fixture.objects.get(query, 8 * 1024 * 1024).await?;
            if mode == "huge-header"
                && let Some(object) = &mut selected.header.object
            {
                object.size = u64::MAX;
            }
            let mut result = framed(
                output,
                &wire::GetObjectResponse {
                    frame: Some(wire::get_object_response::Frame::Header(
                        selected.header.clone(),
                    )),
                },
            )?;
            if mode == "malformed" {
                result.extend(framed(
                    output,
                    &wire::GetObjectResponse {
                        frame: Some(wire::get_object_response::Frame::Header(selected.header)),
                    },
                )?);
            }
            if mode == "terminal-error" {
                result.extend(framed(
                    output,
                    &wire::GetObjectResponse {
                        frame: Some(wire::get_object_response::Frame::Error(wire::ErrorDetail {
                            code: wire::ErrorCode::AccessDenied as i32,
                            request_id: "revoked".into(),
                        })),
                    },
                )?);
            } else {
                if mode == "truncated" {
                    selected
                        .body
                        .truncate(selected.body.len().saturating_sub(1));
                }
                let size = if mode == "oversized-frame" {
                    65537
                } else {
                    65536
                };
                for (index, bytes) in selected.body.chunks(size).enumerate() {
                    let body = if mode == "corrupt-codec" {
                        wire::Body {
                            codec: wire::Codec::Zstd as i32,
                            decoded_length: bytes.len() as u64,
                            data: b"invalid-zstd".to_vec(),
                        }
                    } else if index % 2 == 1 && size == 65536 {
                        super::response::zstd_body(bytes)
                    } else {
                        super::response::plain_body(bytes.to_vec())
                    };
                    result.extend(framed(
                        output,
                        &wire::GetObjectResponse {
                            frame: Some(wire::get_object_response::Frame::Body(body)),
                        },
                    )?);
                }
            }
            if mode == "missing-newline" {
                result.pop();
            }
            if mode == "wrong-content" {
                return Ok(("application/json", result));
            }
            Ok(("application/x-ndjson", result))
        }
        _ => Err(invalid()),
    }
}
fn failure(error: Error) -> Response {
    let status = match error.code {
        wire::ErrorCode::InvalidArgument => 400,
        wire::ErrorCode::NotFound => 404,
        wire::ErrorCode::AlreadyExists | wire::ErrorCode::IdempotencyMismatch => 409,
        wire::ErrorCode::PreconditionFailed => 412,
        wire::ErrorCode::QuotaExceeded => 413,
        wire::ErrorCode::AccessDenied => 403,
        wire::ErrorCode::RangeNotSatisfiable => 416,
        wire::ErrorCode::NotModified => 304,
        _ => 503,
    };
    let body = if status == 304 {
        Vec::new()
    } else {
        json::encode(
            "ErrorDetail",
            &wire::ErrorDetail {
                code: error.code as i32,
                request_id: "customer-request".into(),
            },
        )
        .unwrap_or_default()
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response.headers_mut().insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}
async fn handler(
    State(fixture): State<Fixture>,
    Path(route): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some("Bearer exact-token")
    {
        return failure(wire::ErrorCode::AccessDenied.into());
    }
    if let Ok(mut calls) = fixture.calls.lock() {
        calls.insert(route.clone());
    }
    match operation(&fixture, &route, &body).await {
        Ok((content_type, bytes)) => {
            // Break every NDJSON frame across arbitrary transport chunks.
            let chunks: Vec<_> = bytes
                .chunks(97)
                .map(|bytes| Ok::<_, std::io::Error>(Bytes::copy_from_slice(bytes)))
                .collect();
            let mut response = Response::new(Body::from_stream(stream::iter(chunks)));
            response.headers_mut().insert(
                "content-type",
                axum::http::HeaderValue::from_static(content_type),
            );
            response
        }
        Err(error) => failure(error),
    }
}
struct Server {
    endpoint: String,
    fixture: Fixture,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
async fn start() -> Result<Server, Box<dyn std::error::Error + Send + Sync>> {
    let fixture = Fixture {
        objects: MemoryObjects::new(MemoryOptions::default())?,
        calls: Arc::default(),
    };
    let app = Router::new()
        .route("/v1/objects/{*route}", post(handler))
        .layer(DefaultBodyLimit::max(12 * 1024 * 1024))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receive.await;
            })
            .await
    });
    Ok(Server {
        endpoint,
        fixture,
        shutdown,
        task,
    })
}
#[tokio::test]
async fn http_exercises_every_rpc_streaming_authentication_bounds_and_semantic_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 16 * 1024 * 1024)?;
    let unauthorized = HttpObjects::new(&server.endpoint, "wrong", 1024)?;
    assert_eq!(
        unauthorized
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.inputs".into(),
                mutation: None
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    super::conformance::verify(&client, "conformance-http").await?;
    super::tests::exercise_provider(&client).await?;
    super::tests::exercise_uploads(
        &client,
        |query, body| Box::pin(client.put_stream(query, body)),
        |query, body| Box::pin(client.upload_part_stream(query, body)),
    )
    .await?;
    super::tests::exercise_streaming(&client, |query, maximum| {
        Box::pin(client.get_stream(query, maximum))
    })
    .await?;
    let calls = server
        .fixture
        .calls
        .lock()
        .map_err(|_| "fixture lock poisoned")?
        .clone();
    assert_eq!(
        calls,
        HTTP_ROUTES
            .iter()
            .map(|(route, _, _)| (*route).to_owned())
            .collect()
    );
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}
#[tokio::test]
async fn ndjson_rejects_incomplete_oversized_and_wrong_media_responses()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 512 * 1024)?;
    client
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    let bucket = Some(wire::BucketRef {
        name: "customer.inputs".into(),
    });
    for key in [
        "truncated",
        "missing-newline",
        "wrong-content",
        "oversized-frame",
        "huge-header",
    ] {
        let bytes = if key == "oversized-frame" {
            Bytes::from(vec![b'x'; 65537])
        } else {
            Bytes::from_static(b"bytes")
        };
        client
            .put(
                wire::PutObjectHeader {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    ..Default::default()
                },
                bytes,
            )
            .await?;
        let error = client
            .get(
                wire::GetObjectRequest {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    ..Default::default()
                },
                8 * 1024 * 1024,
            )
            .await
            .err()
            .ok_or("malformed response accepted")?;
        assert_eq!(
            error.code,
            if key == "huge-header" {
                wire::ErrorCode::QuotaExceeded
            } else {
                wire::ErrorCode::Unavailable
            },
            "{key}"
        );
    }
    let bounded = HttpObjects::new(&server.endpoint, "exact-token", 8)?;
    assert_eq!(
        bounded
            .head_bucket(wire::HeadBucketRequest { bucket })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}
#[test]
fn customer_credentials_and_endpoint_configuration_are_bounded() {
    for endpoint in [
        "http://example.com",
        "https://user:password@example.com",
        "https://example.com?query=1",
        "https://example.com#fragment",
    ] {
        assert!(HttpObjects::new(endpoint, "token", 1024).is_err());
    }
    for token in [
        String::new(),
        "\nsecret".into(),
        "x".repeat(super::MAX_BEARER_TOKEN_BYTES + 1),
    ] {
        assert!(HttpObjects::new("https://example.com", &token, 1024).is_err());
    }
    assert!(
        HttpObjects::with_ca_certificate("https://example.com", "token", 1024, Some(&[])).is_err()
    );
    for endpoint in ["http://127.0.0.2:1", "http://[::1]:1", "http://localhost:1"] {
        assert!(HttpObjects::new(endpoint, "token", 1024).is_ok());
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one fixture checks every streaming trace terminal transition"
)]
async fn http_download_trace_requires_validated_eof_and_records_cancellation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::body::tests::Capture;
    use futures::StreamExt;
    use tracing_subscriber::prelude::*;

    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 512 * 1024)?;
    let bucket = Some(wire::BucketRef {
        name: "customer.inputs".into(),
    });
    client
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    let bytes = Bytes::from(vec![b'x'; 128 * 1024]);
    for key in [
        "ordinary",
        "truncated",
        "missing-newline",
        "malformed",
        "terminal-error",
        "corrupt-codec",
    ] {
        client
            .put(
                wire::PutObjectHeader {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    ..Default::default()
                },
                bytes.clone(),
            )
            .await?;
    }
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let query = |key: &str| wire::GetObjectRequest {
        bucket: bucket.clone(),
        object_key: key.into(),
        ..Default::default()
    };
    let span_name = "acyclic.objects.http.call";
    // Receipt of headers, and even receipt of all bytes, is not validated EOF.
    let mut download = client.get_stream(query("ordinary"), 512 * 1024).await?;
    assert!(capture.spans(span_name).is_empty());
    assert_eq!(
        download
            .body
            .next()
            .await
            .transpose()?
            .map(|bytes| bytes.len()),
        Some(65536)
    );
    assert!(capture.spans(span_name).is_empty());
    assert_eq!(
        download
            .body
            .next()
            .await
            .transpose()?
            .map(|bytes| bytes.len()),
        Some(65536)
    );
    assert!(capture.spans(span_name).is_empty());
    assert!(download.body.next().await.is_none());
    let completed = capture.spans(span_name);
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed
            .first()
            .ok_or("missing completed span")?
            .fields
            .get("outcome")
            .map(String::as_str),
        Some("ok")
    );
    assert!(
        !completed
            .first()
            .ok_or("missing completed span")?
            .fields
            .contains_key("error.kind")
    );
    drop(download);

    for consumed in [0, 1, 2] {
        let before = capture.spans(span_name).len();
        let mut download = client.get_stream(query("ordinary"), 512 * 1024).await?;
        for _ in 0..consumed {
            assert!(download.body.next().await.transpose()?.is_some());
        }
        assert_eq!(capture.spans(span_name).len(), before);
        drop(download);
        let spans = capture.spans(span_name);
        assert_eq!(spans.len(), before + 1);
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("err")
        );
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("error.kind")
                .map(String::as_str),
            Some("cancelled")
        );
    }
    for key in [
        "truncated",
        "missing-newline",
        "malformed",
        "terminal-error",
        "corrupt-codec",
    ] {
        let before = capture.spans(span_name).len();
        let mut download = client.get_stream(query(key), 512 * 1024).await?;
        assert_eq!(capture.spans(span_name).len(), before, "{key}");
        let error = loop {
            match download.body.next().await {
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => return Err(format!("accepted invalid body: {key}").into()),
            }
        };
        // The error itself terminates the trace; later drop cannot rewrite it.
        assert_eq!(capture.spans(span_name).len(), before + 1, "{key}");
        drop(download);
        let spans = capture.spans(span_name);
        assert_eq!(spans.len(), before + 1);
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("err")
        );
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("error.kind")
                .map(String::as_str),
            Some(error.code.as_str_name())
        );
    }
    assert!(capture.spans(span_name).iter().all(|span| {
        span.fields.keys().all(|key| {
            matches!(
                key.as_str(),
                "route" | "rpc.code" | "outcome" | "error.kind"
            )
        }) && span
            .fields
            .values()
            .all(|value| !value.contains("customer.inputs") && !value.contains("exact-token"))
    }));
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one gated response checks decode completion and in-flight cancellation"
)]
async fn http_unary_trace_waits_for_delayed_body_and_json_decode()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::body::tests::Capture;
    use tracing_subscriber::prelude::*;

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/v1/objects/buckets/head",
        post({
            let gate = gate.clone();
            let started = started.clone();
            move || {
                let gate = gate.clone();
                let started = started.clone();
                async move {
                    let body = Body::from_stream(stream::once(async move {
                        started.notify_one();
                        let permit = gate.acquire_owned().await.map_err(std::io::Error::other)?;
                        permit.forget();
                        // Successful headers followed by an invalid unary response.
                        Ok::<_, std::io::Error>(Bytes::from_static(b"{"))
                    }));
                    let mut response = Response::new(body);
                    response.headers_mut().insert(
                        "content-type",
                        axum::http::HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpObjects::new(
        &format!("http://{}", listener.local_addr()?),
        "exact-token",
        1024,
    )?;
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receive.await;
            })
            .await
    });
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let query = || wire::HeadBucketRequest {
        bucket: Some(wire::BucketRef {
            name: "customer.inputs".into(),
        }),
    };
    let span_name = "acyclic.objects.http.call";
    {
        let request = client.head_bucket(query());
        tokio::pin!(request);
        tokio::select! {
            result = &mut request => return Err(format!("body gate bypassed: {result:?}").into()),
            () = started.notified() => {},
        }
        // Poll past headers into the blocked body read, without completing it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        assert!(capture.spans(span_name).is_empty());
        gate.add_permits(1);
        assert!(request.await.is_err());
    }
    let spans = capture.spans(span_name);
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans
            .first()
            .ok_or("missing completed span")?
            .fields
            .get("rpc.code")
            .map(String::as_str),
        Some("200")
    );
    assert_eq!(
        spans
            .first()
            .ok_or("missing completed span")?
            .fields
            .get("outcome")
            .map(String::as_str),
        Some("err")
    );
    assert_eq!(
        spans
            .first()
            .ok_or("missing completed span")?
            .fields
            .get("error.kind")
            .map(String::as_str),
        Some(wire::ErrorCode::Unavailable.as_str_name())
    );
    {
        let request = client.head_bucket(query());
        tokio::pin!(request);
        tokio::select! {
            result = &mut request => return Err(format!("body gate bypassed: {result:?}").into()),
            () = started.notified() => {},
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        assert_eq!(capture.spans(span_name).len(), 1);
    }
    let spans = capture.spans(span_name);
    assert_eq!(spans.len(), 2);
    assert_eq!(
        spans
            .get(1)
            .ok_or("missing cancelled span")?
            .fields
            .get("error.kind")
            .map(String::as_str),
        Some("cancelled")
    );
    gate.add_permits(1);
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one isolated exporter checks real serialized RPC spans against customer-data sentinels"
)]
fn http_chrome_export_keeps_customer_data_out_of_serialized_trace()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Write;
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct Sink(Arc<Mutex<Vec<u8>>>);
    impl Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("trace sink poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    const BUCKET: &str = "trace-private-bucket-sentinel";
    const KEY: &str = "private/object-key-sentinel";
    const BODY: &str = "trace-private-body-sentinel";
    const PATH: &str = "C:/private-customer-path-sentinel/source.dat";
    for filtered in [false, true] {
        let sink = Sink(Arc::default());
        let exported = sink.0.clone();
        // tracing-chrome caches its sender in thread-local state. A fresh thread
        // gives this exporter an independent sender even when tests reuse workers.
        std::thread::spawn(
            move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let (chrome, flush) = tracing_chrome::ChromeLayerBuilder::new()
                    .writer(sink)
                    .include_args(true)
                    .include_locations(false)
                    // Async exports creation/close, including time between polls.
                    .trace_style(tracing_chrome::TraceStyle::Async)
                    .build();
                {
                    let chrome = chrome.with_filter(tracing_subscriber::filter::filter_fn(
                        move |metadata| !filtered || metadata.name() != "acyclic.objects.http.call",
                    ));
                    let _subscriber = tracing::subscriber::set_default(
                        tracing_subscriber::registry().with(chrome),
                    );
                    let caller = tracing::info_span!(
                        "trace-export-caller",
                        rpc.code = tracing::field::Empty,
                        outcome = tracing::field::Empty,
                        error.kind = tracing::field::Empty
                    );
                    let _caller = caller.enter();
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?
                        .block_on(async {
                            let server = start().await?;
                            let client =
                                HttpObjects::new(&server.endpoint, "exact-token", 512 * 1024)?;
                            client
                                .create_bucket(wire::CreateBucketRequest {
                                    name: BUCKET.into(),
                                    mutation: None,
                                })
                                .await?;
                            let bucket = Some(wire::BucketRef {
                                name: BUCKET.into(),
                            });
                            client
                                .put(
                                    wire::PutObjectHeader {
                                        bucket: bucket.clone(),
                                        object_key: KEY.into(),
                                        metadata: Some(wire::ObjectMetadata {
                                            user: [("source-path".into(), PATH.into())]
                                                .into_iter()
                                                .collect(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    },
                                    Bytes::from_static(BODY.as_bytes()),
                                )
                                .await?;
                            let download = client
                                .get_stream(
                                    wire::GetObjectRequest {
                                        bucket: bucket.clone(),
                                        object_key: KEY.into(),
                                        ..Default::default()
                                    },
                                    512 * 1024,
                                )
                                .await?;
                            assert_eq!(
                                download.collect(512 * 1024).await?.body.as_ref(),
                                BODY.as_bytes()
                            );
                            // An exported failed RPC ensures error outcomes are covered too.
                            assert!(
                                client
                                    .head(wire::HeadObjectRequest {
                                        bucket,
                                        object_key: "missing-key-sentinel".into(),
                                        ..Default::default()
                                    })
                                    .await
                                    .is_err()
                            );
                            drop(client);
                            let _ = server.shutdown.send(());
                            server.task.await??;
                            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                        })?;
                }
                // Drop joins the exporter thread and writes the closing JSON delimiter.
                drop(flush);
                Ok(())
            },
        )
        .join()
        .map_err(|_| "trace-export test thread panicked")??;
        let bytes = exported.lock().map_err(|_| "trace sink poisoned")?.clone();
        let trace: serde_json::Value = serde_json::from_slice(&bytes)?;
        let caller = trace
            .as_array()
            .ok_or("trace is not an event array")?
            .iter()
            .find(|event| {
                event.get("name").and_then(serde_json::Value::as_str) == Some("trace-export-caller")
                    && event.get("ph").and_then(serde_json::Value::as_str) == Some("e")
            })
            .ok_or("missing completed caller span")?;
        if let Some(args) = caller.get("args").and_then(serde_json::Value::as_object) {
            assert!(
                ["rpc.code", "outcome", "error.kind"]
                    .iter()
                    .all(|field| !args.contains_key(*field)),
                "RPC recording polluted caller arguments with filtered={filtered}"
            );
        }
        let calls = trace
            .as_array()
            .ok_or("trace is not an event array")?
            .iter()
            .filter(|event| {
                event.get("name").and_then(serde_json::Value::as_str)
                    == Some("acyclic.objects.http.call")
            })
            .collect::<Vec<_>>();
        if filtered {
            assert!(calls.is_empty(), "filtered RPC spans reached the exporter");
            for name in ["acyclic.objects.put", "acyclic.objects.get"] {
                assert!(
                    trace
                        .as_array()
                        .ok_or("trace is not an event array")?
                        .iter()
                        .any(|event| {
                            event.get("name").and_then(serde_json::Value::as_str) == Some(name)
                                && event.get("ph").and_then(serde_json::Value::as_str) == Some("e")
                                && event
                                    .get("args")
                                    .and_then(|args| args.get("outcome"))
                                    .and_then(serde_json::Value::as_str)
                                    .and_then(|value| serde_json::from_str::<String>(value).ok())
                                    .as_deref()
                                    == Some("ok")
                        }),
                    "filtered privacy checks must observe completed provider span {name}"
                );
            }
        } else {
            assert!(
                !calls.is_empty(),
                "sentinel checks must observe real exported RPCs"
            );
            for outcome in ["ok", "err"] {
                assert!(
                    calls.iter().any(|event| {
                        let terminal =
                            event.get("ph").and_then(serde_json::Value::as_str) == Some("e");
                        // tracing-chrome 0.7.2 serializes arguments with Debug, so
                        // string values contain another pair of quotation marks.
                        let exported_outcome = event
                            .get("args")
                            .and_then(|args| args.get("outcome"))
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| serde_json::from_str::<String>(value).ok());
                        terminal && exported_outcome.as_deref() == Some(outcome)
                    }),
                    "missing serialized {outcome} RPC outcome"
                );
            }
        }
        for event in calls {
            let args = event
                .get("args")
                .and_then(serde_json::Value::as_object)
                .ok_or("RPC event lacks serialized arguments")?;
            assert!(args.keys().all(|key| matches!(
                key.as_str(),
                "route" | "rpc.code" | "outcome" | "error.kind"
            )));
        }
        let serialized = String::from_utf8(bytes)?;
        for sentinel in [
            "exact-token",
            BUCKET,
            KEY,
            BODY,
            "private-customer-path-sentinel",
            "missing-key-sentinel",
        ] {
            assert!(
                !serialized.contains(sentinel),
                "customer sentinel leaked into serialized trace: {sentinel}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn http_rejects_the_obsolete_namespace_for_an_existing_bucket()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 1024)?;
    let bucket = wire::BucketRef {
        name: "namespace.inputs".into(),
    };
    client
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            mutation: None,
        })
        .await?;
    client
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        })
        .await?;
    let response = reqwest::Client::new()
        .post(format!("{}/v2/objects/buckets/head", server.endpoint))
        .bearer_auth("exact-token")
        .header("content-type", "application/json")
        .body(r#"{"bucket":{"name":"namespace.inputs"}}"#)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    client
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: Some(bucket),
            mutation: None,
        })
        .await?;
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn http_unary_trace_records_semantic_response_failure()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::body::tests::Capture;
    use tracing_subscriber::prelude::*;

    let bucket = wire::Bucket {
        bucket: Some(wire::BucketRef {
            name: "different.bucket".into(),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        }),
    };
    super::response::bucket(
        &bucket,
        &wire::BucketRef {
            name: "different.bucket".into(),
        },
    )?;
    let body = json::encode("Bucket", &bucket)?;
    let app = Router::new().route(
        "/v1/objects/buckets/head",
        post(move || {
            let body = body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpObjects::new(
        &format!("http://{}", listener.local_addr()?),
        "exact-token",
        1024,
    )?;
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receive.await;
            })
            .await
    });
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let result = client
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
        })
        .await;
    let _ = shutdown.send(());
    server.await??;
    let error = result.err().ok_or("accepted a different bucket")?;
    let spans = capture.spans("acyclic.objects.http.call");
    assert_eq!(spans.len(), 1);
    let fields = &spans.first().ok_or("missing completed span")?.fields;
    assert_eq!(fields.get("rpc.code").map(String::as_str), Some("200"));
    assert_eq!(fields.get("outcome").map(String::as_str), Some("err"));
    assert_eq!(
        fields.get("error.kind").map(String::as_str),
        Some(error.code.as_str_name())
    );
    Ok(())
}
