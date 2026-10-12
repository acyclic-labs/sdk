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
            let result = fixture.objects.put(header, body.freeze().into()).await?;
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
            let result = fixture
                .objects
                .upload_part(header, body.freeze().into())
                .await?;
            Ok(("application/json", json::encode(output, &result)?))
        }
        "objects/get" => {
            let query: wire::GetObjectRequest = json::decode(input, bytes, 128 * 1024)?;
            let mode = query.object_key.clone();
            let selected = fixture.objects.get(query, 8 * 1024 * 1024).await?;
            let mut header = selected.header().clone();
            if mode == "huge-header"
                && let Some(object) = &mut header.object
            {
                object.size = u64::MAX;
            }
            let mut result = framed(
                output,
                &wire::GetObjectResponse {
                    frame: Some(wire::get_object_response::Frame::Header(header.clone())),
                },
            )?;
            if mode == "malformed" {
                result.extend(framed(
                    output,
                    &wire::GetObjectResponse {
                        frame: Some(wire::get_object_response::Frame::Header(header)),
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
                let selected_body = if mode == "truncated" {
                    selected
                        .body()
                        .slice(..selected.body().len().saturating_sub(1))
                } else {
                    selected.body().clone()
                };
                let size = if mode == "oversized-frame" {
                    65537
                } else {
                    65536
                };
                for (index, bytes) in selected_body.chunks(size).enumerate() {
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
    let client = HttpObjects::new(&server.endpoint, "exact-token", 16 * 1024 * 1024, None)?;
    let unauthorized = HttpObjects::new(&server.endpoint, "wrong", 1024, None)?;
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
    let client = HttpObjects::new(&server.endpoint, "exact-token", 512 * 1024, None)?;
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
                bytes.into(),
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
    let bounded = HttpObjects::new(&server.endpoint, "exact-token", 8, None)?;
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
        assert!(HttpObjects::new(endpoint, "token", 1024, None).is_err());
    }
    for token in [
        String::new(),
        "\nsecret".into(),
        "x".repeat(super::MAX_BEARER_TOKEN_BYTES + 1),
    ] {
        assert!(HttpObjects::new("https://example.com", &token, 1024, None).is_err());
    }
    assert!(HttpObjects::new("https://example.com", "token", 1024, Some(&[])).is_err());
    for endpoint in ["http://127.0.0.2:1", "http://[::1]:1", "http://localhost:1"] {
        assert!(HttpObjects::new(endpoint, "token", 1024, None).is_ok());
    }
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
        // Fresh threads avoid tracing-chrome sender caching.
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
                            let client = HttpObjects::new(
                                &server.endpoint,
                                "exact-token",
                                512 * 1024,
                                None,
                            )?;
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
                                    Bytes::from_static(BODY.as_bytes()).into(),
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
                                download.collect(512 * 1024).await?.body().as_ref(),
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
                        // tracing-chrome serializes argument strings with Debug quotes.
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
    let client = HttpObjects::new(&server.endpoint, "exact-token", 1024, None)?;
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
async fn http_rejects_a_substituted_bucket() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let bucket = wire::Bucket {
        bucket: Some(wire::BucketRef {
            name: "different.bucket".into(),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        }),
    };
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
        None,
    )?;
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receive.await;
            })
            .await
    });
    let result = client
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
        })
        .await;
    let _ = shutdown.send(());
    server.await??;
    assert!(matches!(
        result,
        Err(Error {
            code: wire::ErrorCode::Unavailable
        })
    ));
    Ok(())
}
