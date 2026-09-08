#![deny(unsafe_code)]
//! Thin HTTP, SSE, and WebSocket framing over [`acyclic_harness::wire_api`].

use acyclic_harness::{
    Error,
    wire::{self, client_frame, server_frame},
    wire_api::{
        HarnessWireApi, validate_admission, validate_command_protocol, validate_resume_protocol,
    },
};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Deserializer;
use std::{
    convert::Infallible,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc;

/// Default maximum accepted HTTP or WebSocket message size.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1_048_576;

#[derive(Clone)]
struct AppState {
    api: Arc<dyn HarnessWireApi>,
    maximum_frame_bytes: usize,
}

/// Builds all public harness HTTP routes over one transport-neutral implementation.
pub fn router(api: Arc<dyn HarnessWireApi>, maximum_frame_bytes: usize) -> Router {
    let state = AppState {
        api,
        maximum_frame_bytes: maximum_frame_bytes.max(1),
    };
    Router::new()
        .route("/v1/harness/handshake", post(handshake))
        .route("/v1/harness/commands", post(submit))
        .route("/v1/harness/replay", post(replay))
        .route("/v1/harness/ws", get(websocket))
        .with_state(state)
}

async fn handshake(State(state): State<AppState>, body: Bytes) -> Response {
    let request = match decode_json::<wire::HandshakeRequest>(
        "acyclic.harness.v1.HandshakeRequest",
        &body,
        state.maximum_frame_bytes,
    ) {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    match state.api.handshake(request).await {
        Ok(response) => message_response("acyclic.harness.v1.HandshakeResponse", &response),
        Err(error) => error_response(error),
    }
}

async fn submit(State(state): State<AppState>, body: Bytes) -> Response {
    let command = match decode_json::<wire::CommandEnvelope>(
        "acyclic.harness.v1.CommandEnvelope",
        &body,
        state.maximum_frame_bytes,
    ) {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    if let Err(error) = validate_command_protocol(&command) {
        return error_response(error);
    }
    let admission = match state.api.submit(command.clone()).await {
        Ok(value) => value,
        Err(error) => admission_from_error(command.operation.clone(), error),
    };
    if let Err(error) = validate_admission(&command, &admission) {
        return error_response(error);
    }
    message_response("acyclic.harness.v1.Admission", &admission)
}

async fn replay(State(state): State<AppState>, body: Bytes) -> Response {
    let request = match decode_json::<wire::ResumeRequest>(
        "acyclic.harness.v1.ResumeRequest",
        &body,
        state.maximum_frame_bytes,
    ) {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    if let Err(error) = validate_resume_protocol(&request) {
        return error_response(error);
    }
    let stream = match state.api.replay(request).await {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    let body = stream.map(|item| {
        let frame = match item {
            Ok(delivery) => wire::ServerFrame {
                frame: Some(server_frame::Frame::Delivery(delivery)),
            },
            Err(error) => wire::ServerFrame {
                frame: Some(server_frame::Frame::Error(acyclic_harness::encode_error(
                    &error,
                ))),
            },
        };
        let encoded =
            encode_json("acyclic.harness.v1.ServerFrame", &frame).unwrap_or_else(|error| {
                format!(
                    "{{\"error\":{}}}",
                    serde_json::to_string(&error.to_string())
                        .unwrap_or_else(|_| "\"encoding failure\"".into())
                )
            });
        Ok::<Bytes, Infallible>(Bytes::from(format!("data: {encoded}\n\n")))
    });
    let mut response = Response::new(Body::from_stream(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}

async fn websocket(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade
        .max_message_size(state.maximum_frame_bytes)
        .on_upgrade(move |socket| websocket_session(state, socket))
}

async fn websocket_session(state: AppState, mut socket: WebSocket) {
    let Some(Ok(first)) = socket.recv().await else {
        return;
    };
    let Ok(frame) = decode_ws(&first, state.maximum_frame_bytes) else {
        let _ = socket.close().await;
        return;
    };
    let Some(client_frame::Frame::Handshake(request)) = frame.frame else {
        let _ = socket.close().await;
        return;
    };
    let response = match state.api.handshake(request).await {
        Ok(value) => wire::ServerFrame {
            frame: Some(server_frame::Frame::Handshake(value)),
        },
        Err(error) => {
            let response = wire::ServerFrame {
                frame: Some(server_frame::Frame::Error(acyclic_harness::encode_error(
                    &error,
                ))),
            };
            let _ = send_ws(&mut socket, &response).await;
            let _ = socket.close().await;
            return;
        }
    };
    if send_ws(&mut socket, &response).await.is_err() {
        return;
    }

    struct Outbound {
        replay_epoch: Option<u64>,
        frame: wire::ServerFrame,
    }
    let (mut sender, mut receiver) = socket.split();
    let replay_epoch = Arc::new(AtomicU64::new(0));
    let writer_epoch = Arc::clone(&replay_epoch);
    let (tx, mut rx) = mpsc::channel::<Outbound>(64);
    let writer = tokio::spawn(async move {
        while let Some(outbound) = rx.recv().await {
            if outbound
                .replay_epoch
                .is_some_and(|epoch| epoch != writer_epoch.load(Ordering::Acquire))
            {
                continue;
            }
            let Ok(value) = encode_json("acyclic.harness.v1.ServerFrame", &outbound.frame) else {
                break;
            };
            if sender.send(Message::Text(value.into())).await.is_err() {
                break;
            }
        }
    });
    let mut replay_task: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(Ok(message)) = receiver.next().await {
        let Ok(frame) = decode_ws(&message, state.maximum_frame_bytes) else {
            break;
        };
        match frame.frame {
            Some(client_frame::Frame::Command(command)) => {
                if let Err(error) = validate_command_protocol(&command) {
                    let _ = tx
                        .send(Outbound {
                            replay_epoch: None,
                            frame: wire::ServerFrame {
                                frame: Some(server_frame::Frame::Error(
                                    acyclic_harness::encode_error(&error),
                                )),
                            },
                        })
                        .await;
                    continue;
                }
                let admission = match state.api.submit(command.clone()).await {
                    Ok(value) => value,
                    Err(error) => admission_from_error(command.operation.clone(), error),
                };
                let frame = if validate_admission(&command, &admission).is_ok() {
                    server_frame::Frame::Admission(admission)
                } else {
                    server_frame::Frame::Error(acyclic_harness::encode_error(&Error::Conflict(
                        "admission identity mismatch".into(),
                    )))
                };
                if tx
                    .send(Outbound {
                        replay_epoch: None,
                        frame: wire::ServerFrame { frame: Some(frame) },
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Some(client_frame::Frame::Resume(request)) => {
                let epoch = replay_epoch.fetch_add(1, Ordering::AcqRel) + 1;
                if let Some(active) = replay_task.take() {
                    active.abort();
                }
                if let Err(error) = validate_resume_protocol(&request) {
                    let _ = tx
                        .send(Outbound {
                            replay_epoch: None,
                            frame: wire::ServerFrame {
                                frame: Some(server_frame::Frame::Error(
                                    acyclic_harness::encode_error(&error),
                                )),
                            },
                        })
                        .await;
                    continue;
                }
                let Ok(mut stream) = state.api.replay(request).await else {
                    break;
                };
                let output = tx.clone();
                replay_task = Some(tokio::spawn(async move {
                    while let Some(item) = stream.next().await {
                        let frame = match item {
                            Ok(value) => server_frame::Frame::Delivery(value),
                            Err(error) => {
                                server_frame::Frame::Error(acyclic_harness::encode_error(&error))
                            }
                        };
                        if output
                            .send(Outbound {
                                replay_epoch: Some(epoch),
                                frame: wire::ServerFrame { frame: Some(frame) },
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }));
            }
            Some(client_frame::Frame::Acknowledge(_)) => {}
            Some(client_frame::Frame::Handshake(_)) | None => break,
        }
    }
    replay_epoch.fetch_add(1, Ordering::AcqRel);
    if let Some(active) = replay_task {
        active.abort();
    }
    drop(tx);
    let _ = writer.await;
}

fn admission_from_error(
    operation: Option<wire::OperationIdentity>,
    error: Error,
) -> wire::Admission {
    let state = if matches!(error, Error::Storage(_) | Error::Indeterminate(_)) {
        wire::AdmissionState::Indeterminate
    } else {
        wire::AdmissionState::Rejected
    };
    wire::Admission {
        operation,
        state: state as i32,
        error: Some(acyclic_harness::encode_error(&error)),
    }
}

fn decode_ws(message: &Message, maximum: usize) -> Result<wire::ClientFrame, Error> {
    match message {
        Message::Text(value) => {
            decode_json("acyclic.harness.v1.ClientFrame", value.as_bytes(), maximum)
        }
        Message::Binary(value) if value.len() <= maximum => {
            wire::ClientFrame::decode(value.as_ref())
                .map_err(|error| Error::Invalid(error.to_string()))
        }
        _ => Err(Error::Invalid("unsupported WebSocket frame".into())),
    }
}

async fn send_ws(socket: &mut WebSocket, frame: &wire::ServerFrame) -> Result<(), Error> {
    let value = encode_json("acyclic.harness.v1.ServerFrame", frame)?;
    socket
        .send(Message::Text(value.into()))
        .await
        .map_err(|error| Error::Storage(error.to_string()))
}

fn decode_json<M: prost::Message + Default>(
    name: &str,
    bytes: &[u8],
    maximum: usize,
) -> Result<M, Error> {
    if bytes.len() > maximum {
        return Err(Error::Invalid("wire frame exceeds configured limit".into()));
    }
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| Error::Storage("protobuf descriptor is missing".into()))?;
    let mut deserializer = Deserializer::from_slice(bytes);
    let dynamic = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    M::decode(dynamic.encode_to_vec().as_slice()).map_err(|error| Error::Invalid(error.to_string()))
}

fn encode_json<M: prost::Message>(name: &str, message: &M) -> Result<String, Error> {
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| Error::Storage("protobuf descriptor is missing".into()))?;
    let dynamic = DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice())
        .map_err(|error| Error::Storage(error.to_string()))?;
    serde_json::to_string(&dynamic).map_err(|error| Error::Storage(error.to_string()))
}

fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(acyclic_harness::FILE_DESCRIPTOR_SET).unwrap_or_default()
    })
}

fn message_response<M: prost::Message>(name: &str, message: &M) -> Response {
    match encode_json(name, message) {
        Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(error) => error_response(error),
    }
}

fn error_response(error: Error) -> Response {
    let status = match error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Unauthorized(_) => StatusCode::FORBIDDEN,
        Error::NotFound(_) => StatusCode::NOT_FOUND,
        Error::Unsupported(_) => StatusCode::UNPROCESSABLE_ENTITY,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::Storage(_) | Error::Indeterminate(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, error.to_string()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        Result,
        wire_api::{current_protocol, negotiate},
    };
    use futures::{FutureExt as _, stream};
    use tower::ServiceExt as _;

    struct FakeApi;

    impl HarnessWireApi for FakeApi {
        fn handshake<'a>(
            &'a self,
            request: wire::HandshakeRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::HandshakeResponse>> {
            async move { negotiate(&request, &wire::CapabilitySet::default()) }.boxed()
        }

        fn submit<'a>(
            &'a self,
            command: wire::CommandEnvelope,
        ) -> futures::future::BoxFuture<'a, Result<wire::Admission>> {
            async move {
                Ok(wire::Admission {
                    operation: command.operation,
                    state: wire::AdmissionState::Accepted as i32,
                    error: None,
                })
            }
            .boxed()
        }

        fn replay<'a>(
            &'a self,
            _: wire::ResumeRequest,
        ) -> futures::future::BoxFuture<
            'a,
            Result<futures::stream::BoxStream<'static, Result<wire::Delivery>>>,
        > {
            async move {
                let delivery = wire::Delivery {
                    authority: None,
                    generation: "generation".into(),
                    from_revision: 0,
                    through_revision: 0,
                    events: Vec::new(),
                    live: true,
                };
                Ok(Box::pin(stream::once(async { Ok(delivery) })) as _)
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn http_routes_share_protocol_json_and_sse_framing() -> Result<()> {
        let app = router(Arc::new(FakeApi), DEFAULT_MAX_FRAME_BYTES);
        let request = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: Some(wire::CapabilitySet::default()),
        };
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/v1/harness/handshake")
                    .body(Body::from(encode_json(
                        "acyclic.harness.v1.HandshakeRequest",
                        &request,
                    )?))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), DEFAULT_MAX_FRAME_BYTES)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let decoded: wire::HandshakeResponse = decode_json(
            "acyclic.harness.v1.HandshakeResponse",
            &body,
            DEFAULT_MAX_FRAME_BYTES,
        )?;
        assert_eq!(decoded.protocol, request.protocol);

        let response = app
            .oneshot(
                axum::http::Request::post("/v1/harness/replay")
                    .body(Body::from(encode_json(
                        "acyclic.harness.v1.ResumeRequest",
                        &wire::ResumeRequest {
                            protocol: Some(current_protocol()),
                            cursors: Vec::new(),
                        },
                    )?))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/event-stream"))
        );
        let body = axum::body::to_bytes(response.into_body(), DEFAULT_MAX_FRAME_BYTES)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(body.starts_with(b"data: "));
        Ok(())
    }

    #[tokio::test]
    async fn oversized_frames_fail_before_the_api() -> Result<()> {
        let response = router(Arc::new(FakeApi), 2)
            .oneshot(
                axum::http::Request::post("/v1/harness/handshake")
                    .body(Body::from("{}\n"))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[test]
    fn canonical_json_rejects_trailing_content() {
        let result = decode_json::<wire::HandshakeRequest>(
            "acyclic.harness.v1.HandshakeRequest",
            b"{} {}",
            DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(matches!(result, Err(Error::Invalid(_))));
    }
}
