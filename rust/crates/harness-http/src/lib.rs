#![deny(unsafe_code)]
//! Thin HTTP, SSE, and WebSocket framing over [`acyclic_harness::wire_api`].

use acyclic_harness::{
    Error,
    wire::{self, client_frame, server_frame},
    wire_api::{
        HarnessWireApi, validate_admission, validate_cancel_request, validate_cancel_response,
        validate_command_protocol, validate_observe_request, validate_operation_status,
        validate_resume_protocol,
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
        .route("/v1/harness/operations/observe", post(observe))
        .route("/v1/harness/operations/cancel", post(cancel))
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

async fn observe(State(state): State<AppState>, body: Bytes) -> Response {
    let request = match decode_json::<wire::ObserveRequest>(
        "acyclic.harness.v1.ObserveRequest",
        &body,
        state.maximum_frame_bytes,
    ) {
        Ok(value) => value,
        Err(error) => return wire_error_response(error),
    };
    let control = match validate_observe_request(&request) {
        Ok(value) => value,
        Err(error) => return wire_error_response(error),
    };
    if let Err(error) = state.api.authorize_operation_control(&control).await {
        return wire_error_response(error);
    }
    match state.api.observe(request.clone()).await {
        Ok(status) => match validate_operation_status(&request, &status) {
            Ok(()) => message_response("acyclic.harness.v1.OperationStatus", &status),
            Err(error) => wire_error_response(error),
        },
        Err(error) => wire_error_response(error),
    }
}

async fn cancel(State(state): State<AppState>, body: Bytes) -> Response {
    let request = match decode_json::<wire::CancelRequest>(
        "acyclic.harness.v1.CancelRequest",
        &body,
        state.maximum_frame_bytes,
    ) {
        Ok(value) => value,
        Err(error) => return wire_error_response(error),
    };
    let (control, _, _) = match validate_cancel_request(&request) {
        Ok(value) => value,
        Err(error) => return wire_error_response(error),
    };
    if let Err(error) = state.api.authorize_operation_control(&control).await {
        return wire_error_response(error);
    }
    match state.api.cancel(request.clone()).await {
        Ok(response) => match validate_cancel_response(&request, &response) {
            Ok(()) => message_response("acyclic.harness.v1.CancelResponse", &response),
            Err(error) => wire_error_response(error),
        },
        Err(error) => wire_error_response(error),
    }
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
            Some(client_frame::Frame::Observe(request)) => {
                let frame = match validate_observe_request(&request) {
                    Ok(control) => match state.api.authorize_operation_control(&control).await {
                        Ok(()) => match state.api.observe(request.clone()).await {
                            Ok(status) if validate_operation_status(&request, &status).is_ok() => {
                                server_frame::Frame::Status(status)
                            }
                            Ok(_) => server_frame::Frame::Error(control_error(
                                &Error::Conflict("operation status identity mismatch".into()),
                                &request.operation_id,
                            )),
                            Err(error) => server_frame::Frame::Error(control_error(
                                &error,
                                &request.operation_id,
                            )),
                        },
                        Err(error) => {
                            server_frame::Frame::Error(control_error(&error, &request.operation_id))
                        }
                    },
                    Err(error) => {
                        server_frame::Frame::Error(control_error(&error, &request.operation_id))
                    }
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
            Some(client_frame::Frame::Cancel(request)) => {
                let frame = match validate_cancel_request(&request) {
                    Ok((control, _, _)) => {
                        match state.api.authorize_operation_control(&control).await {
                            Ok(()) => match state.api.cancel(request.clone()).await {
                                Ok(response)
                                    if validate_cancel_response(&request, &response).is_ok() =>
                                {
                                    server_frame::Frame::Cancellation(response)
                                }
                                Ok(_) => server_frame::Frame::Error(control_error(
                                    &Error::Conflict("cancellation identity mismatch".into()),
                                    &request.operation_id,
                                )),
                                Err(error) => server_frame::Frame::Error(control_error(
                                    &error,
                                    &request.operation_id,
                                )),
                            },
                            Err(error) => server_frame::Frame::Error(control_error(
                                &error,
                                &request.operation_id,
                            )),
                        }
                    }
                    Err(error) => {
                        server_frame::Frame::Error(control_error(&error, &request.operation_id))
                    }
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

fn control_error(error: &Error, operation_id: &str) -> wire::Error {
    let mut encoded = acyclic_harness::encode_error(error);
    encoded.operation_id = operation_id.into();
    encoded
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
    let status = error_status(&error);
    (status, error.to_string()).into_response()
}

fn wire_error_response(error: Error) -> Response {
    let status = error_status(&error);
    match encode_json(
        "acyclic.harness.v1.Error",
        &acyclic_harness::encode_error(&error),
    ) {
        Ok(body) => (status, [(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(encoding) => error_response(encoding),
    }
}

fn error_status(error: &Error) -> StatusCode {
    match error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Unauthorized(_) => StatusCode::FORBIDDEN,
        Error::NotFound(_) => StatusCode::NOT_FOUND,
        Error::Unsupported(_) => StatusCode::UNPROCESSABLE_ENTITY,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::Storage(_) | Error::Indeterminate(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
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

    #[derive(Default)]
    struct FakeApi {
        deny_control: bool,
    }

    impl HarnessWireApi for FakeApi {
        fn authorize_operation_control<'a>(
            &'a self,
            _: &'a acyclic_harness::wire_api::OperationControlRequest,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async move {
                if self.deny_control {
                    Err(Error::Unauthorized("scope proof is invalid".into()))
                } else {
                    Ok(())
                }
            }
            .boxed()
        }

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

        fn observe<'a>(
            &'a self,
            request: wire::ObserveRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::OperationStatus>> {
            async move {
                Ok(wire::OperationStatus {
                    operation: Some(wire::OperationIdentity {
                        operation_id: request.operation_id,
                        idempotency_key: String::new(),
                    }),
                    state: wire::CompletionState::Running as i32,
                    error: None,
                    protocol: request.protocol,
                    owner: request.owner,
                    cancellation_requested: false,
                    revision: 1,
                })
            }
            .boxed()
        }

        fn cancel<'a>(
            &'a self,
            request: wire::CancelRequest,
        ) -> futures::future::BoxFuture<'a, Result<wire::CancelResponse>> {
            async move {
                let operation = Some(wire::OperationIdentity {
                    operation_id: request.operation_id,
                    idempotency_key: request.idempotency_key,
                });
                Ok(wire::CancelResponse {
                    status: Some(wire::OperationStatus {
                        operation: Some(wire::OperationIdentity {
                            operation_id: operation
                                .as_ref()
                                .map_or_else(String::new, |value| value.operation_id.clone()),
                            idempotency_key: String::new(),
                        }),
                        state: wire::CompletionState::Cancelled as i32,
                        error: None,
                        protocol: request.protocol,
                        owner: request.owner,
                        cancellation_requested: false,
                        revision: 2,
                    }),
                    operation,
                })
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn http_routes_share_protocol_json_and_sse_framing() -> Result<()> {
        let app = router(Arc::new(FakeApi::default()), DEFAULT_MAX_FRAME_BYTES);
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
    async fn http_operation_control_routes_preserve_identity() -> Result<()> {
        let app = router(Arc::new(FakeApi::default()), DEFAULT_MAX_FRAME_BYTES);
        let operation_id = crate_operation_id();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "owner".into(),
        };
        let scope = wire::Scope {
            id: "control".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            issuer: "runtime".into(),
            parent_proof: Vec::new(),
            proof: vec![1; 32],
        };
        let observe = wire::ObserveRequest {
            protocol: Some(current_protocol()),
            owner: Some(owner.clone()),
            operation_id: operation_id.clone(),
            scope: Some(scope.clone()),
        };
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/v1/harness/operations/observe")
                    .body(Body::from(encode_json(
                        "acyclic.harness.v1.ObserveRequest",
                        &observe,
                    )?))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(response.status(), StatusCode::OK);

        let operation = wire::OperationIdentity {
            operation_id: operation_id.clone(),
            idempotency_key: "cancel-1".into(),
        };
        let cancel = wire::CancelRequest {
            operation_id,
            protocol: Some(current_protocol()),
            owner: Some(owner),
            scope: Some(scope),
            recursive: true,
            idempotency_key: operation.idempotency_key.clone(),
        };
        let response = app
            .oneshot(
                axum::http::Request::post("/v1/harness/operations/cancel")
                    .body(Body::from(encode_json(
                        "acyclic.harness.v1.CancelRequest",
                        &cancel,
                    )?))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), DEFAULT_MAX_FRAME_BYTES)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let decoded: wire::CancelResponse = decode_json(
            "acyclic.harness.v1.CancelResponse",
            &body,
            DEFAULT_MAX_FRAME_BYTES,
        )?;
        assert_eq!(decoded.operation, Some(operation));
        Ok(())
    }

    #[tokio::test]
    async fn http_operation_control_requires_the_api_authorizer() -> Result<()> {
        let request = wire::ObserveRequest {
            operation_id: crate_operation_id(),
            protocol: Some(current_protocol()),
            owner: Some(wire::Authority {
                kind: wire::AggregateKind::Task as i32,
                id: "owner".into(),
            }),
            scope: Some(wire::Scope {
                id: "control".into(),
                capabilities: vec!["operation:observe".into()],
                issuer: "runtime".into(),
                parent_proof: Vec::new(),
                proof: vec![1; 32],
            }),
        };
        let response = router(
            Arc::new(FakeApi { deny_control: true }),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .oneshot(
            axum::http::Request::post("/v1/harness/operations/observe")
                .body(Body::from(encode_json(
                    "acyclic.harness.v1.ObserveRequest",
                    &request,
                )?))
                .map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), DEFAULT_MAX_FRAME_BYTES)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let error: wire::Error =
            decode_json("acyclic.harness.v1.Error", &body, DEFAULT_MAX_FRAME_BYTES)?;
        assert_eq!(error.code, wire::ErrorCode::Unauthorized as i32);
        Ok(())
    }

    fn crate_operation_id() -> String {
        acyclic_harness::OperationId::from_bytes([11; 16]).to_string()
    }

    #[tokio::test]
    async fn oversized_frames_fail_before_the_api() -> Result<()> {
        let response = router(Arc::new(FakeApi::default()), 2)
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
