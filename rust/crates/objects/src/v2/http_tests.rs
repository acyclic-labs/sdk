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
        if line.len() > super::HTTP_JSON_FRAME_BYTES {
            return Err(invalid());
        }
        let line = line.strip_suffix(b"\n").ok_or_else(invalid)?;
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        frames.push(json::decode(name, line, super::HTTP_JSON_FRAME_BYTES)?);
    }
    Ok(frames)
}
#[allow(clippy::too_many_lines)] // Explicit fixture dispatch covers every canonical route in one inventory.
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
            let query: wire::GetObjectRequest =
                json::decode(input, bytes, super::HTTP_JSON_FRAME_BYTES)?;
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
                    super::HTTP_BODY_FRAME_BYTES + 1
                } else {
                    super::HTTP_BODY_FRAME_BYTES
                };
                for bytes in selected.body.chunks(size) {
                    result.extend(framed(
                        output,
                        &wire::GetObjectResponse {
                            frame: Some(wire::get_object_response::Frame::Body(bytes.to_vec())),
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
        .route("/v2/objects/{*route}", post(handler))
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
    for token in [String::new(), "\nsecret".into(), "x".repeat(8193)] {
        assert!(HttpObjects::new("https://example.com", &token, 1024).is_err());
    }
    assert!(
        HttpObjects::with_ca_certificate("https://example.com", "token", 1024, Some(&[])).is_err()
    );
    assert!(HttpObjects::new("http://127.0.0.1:1", "token", 1024).is_ok());
}
