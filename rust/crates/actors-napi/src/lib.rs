//! N-API bridge for the canonical Rust remote Actors client.
//!
//! Protobuf request and response bytes cross the JavaScript boundary. The
//! bridge owns no transport or contract policy: authenticated transport
//! selection and operation behavior remain in `acyclic_actors::client::Client`.

use std::{future::Future, sync::Arc};

use acyclic_actors::{client, wire};
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi_derive::napi;
use prost::Message;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[napi(object)]
#[derive(Clone, Serialize)]
pub struct NativeActorsErrorMetadata {
    /// Stable cross-platform error code.
    pub code: String,
    /// Human-readable contract or service message.
    pub message: String,
    #[serde(rename = "grpcCode", skip_serializing_if = "Option::is_none")]
    /// gRPC status name when a transport status exists.
    pub grpc_code: Option<String>,
    #[serde(rename = "serviceCode", skip_serializing_if = "Option::is_none")]
    /// Numeric service detail code, including zero and unknown values.
    pub service_code: Option<i32>,
}

/// Typed operation result used by the generated platform wrapper.
#[napi(object)]
pub struct NativeActorsOperationResult {
    /// Encoded protobuf response when the operation succeeded.
    pub value: Option<Buffer>,
    /// Structured Rust-owned error when the operation failed.
    pub error: Option<NativeActorsErrorMetadata>,
}

type ErrorMetadata = NativeActorsErrorMetadata;

impl NativeActorsOperationResult {
    fn success(value: Buffer) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    fn failure(error: ErrorMetadata) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }
}

fn napi_error(metadata: ErrorMetadata) -> Error {
    let reason = match serde_json::to_string(&metadata) {
        Ok(reason) => reason,
        Err(_) => String::from(
            r#"{"code":"internal","message":"failed to encode Actors error metadata"}"#,
        ),
    };
    Error::new(Status::GenericFailure, reason)
}

fn native_metadata(context: &str, error: impl std::fmt::Display) -> ErrorMetadata {
    ErrorMetadata {
        code: String::from("invalid_argument"),
        message: format!("{context}: {error}"),
        grpc_code: None,
        service_code: None,
    }
}

fn native_error(context: &str, error: impl std::fmt::Display) -> Error {
    napi_error(native_metadata(context, error))
}

fn grpc_code_name(code: i32) -> String {
    match code {
        0 => "ok",
        1 => "cancelled",
        2 => "unknown",
        3 => "invalid_argument",
        4 => "deadline_exceeded",
        5 => "not_found",
        6 => "already_exists",
        7 => "permission_denied",
        8 => "resource_exhausted",
        9 => "failed_precondition",
        10 => "aborted",
        11 => "out_of_range",
        12 => "unimplemented",
        13 => "internal",
        14 => "unavailable",
        15 => "data_loss",
        16 => "unauthenticated",
        _ => "unknown",
    }
    .to_owned()
}

fn client_error_metadata(error: client::Error) -> ErrorMetadata {
    match error {
        client::Error::Configuration(message) => ErrorMetadata {
            code: String::from("invalid_argument"),
            message,
            grpc_code: None,
            service_code: None,
        },
        client::Error::Transport(message) => ErrorMetadata {
            code: String::from("unknown"),
            message,
            grpc_code: Some(String::from("unknown")),
            service_code: None,
        },
        client::Error::Contract(error) => ErrorMetadata {
            code: match error {
                acyclic_actors::ContractError::InvalidArgument => String::from("invalid_argument"),
                acyclic_actors::ContractError::LimitExceeded => String::from("limit_exceeded"),
                acyclic_actors::ContractError::DuplicateName => String::from("duplicate_name"),
            },
            message: error.to_string(),
            grpc_code: None,
            service_code: None,
        },
        client::Error::Service { grpc_code, detail } => ErrorMetadata {
            code: grpc_code_name(grpc_code),
            message: detail
                .as_ref()
                .map(|value| value.message.clone())
                .unwrap_or_else(|| String::from("Actors service failure")),
            grpc_code: Some(grpc_code_name(grpc_code)),
            service_code: detail.map(|value| value.code),
        },
        client::Error::Cancelled => ErrorMetadata {
            code: String::from("cancelled"),
            message: String::from("Actors operation cancelled"),
            grpc_code: Some(String::from("cancelled")),
            service_code: None,
        },
    }
}

fn decode_wire<T: Message + Default>(value: &Buffer, operation: &str) -> Result<T> {
    T::decode(value.as_ref()).map_err(|error| native_error(operation, error))
}

fn encode_wire<T: Message>(value: &T, operation: &str) -> Result<Buffer> {
    let mut bytes = Vec::with_capacity(value.encoded_len());
    value
        .encode(&mut bytes)
        .map_err(|error| native_error(operation, error))?;
    Ok(Buffer::from(bytes))
}

fn decode_wire_metadata<T: Message + Default>(
    value: &Buffer,
    operation: &str,
) -> std::result::Result<T, ErrorMetadata> {
    T::decode(value.as_ref()).map_err(|error| native_metadata(operation, error))
}

fn encode_wire_metadata<T: Message>(
    value: &T,
    operation: &str,
) -> std::result::Result<Buffer, ErrorMetadata> {
    let mut bytes = Vec::with_capacity(value.encoded_len());
    value
        .encode(&mut bytes)
        .map_err(|error| native_metadata(operation, error))?;
    Ok(Buffer::from(bytes))
}

async fn cancellable<T, Operation>(
    operation: Operation,
    cancellation: Option<CancellationState>,
) -> Result<T>
where
    Operation: Future<Output = std::result::Result<T, client::Error>>,
{
    cancellable_metadata(operation, cancellation)
        .await
        .map_err(napi_error)
}

async fn cancellable_metadata<T, Operation>(
    operation: Operation,
    cancellation: Option<CancellationState>,
) -> std::result::Result<T, ErrorMetadata>
where
    Operation: Future<Output = std::result::Result<T, client::Error>>,
{
    let Some(cancellation) = cancellation else {
        return operation.await.map_err(client_error_metadata);
    };
    client::run_with_cancellation(operation, Some(cancellation.token))
        .await
        .map_err(client_error_metadata)
}

#[derive(Clone)]
struct CancellationState {
    token: CancellationToken,
}

impl CancellationState {
    fn cancel(&self) {
        self.token.cancel();
    }
}

/// A monotonic cancellation handle for native Actors calls.
///
/// Once cancelled, a handle stays cancelled. Callers that start another
/// operation must create a fresh handle, matching `AbortSignal` semantics.
#[napi]
pub struct NativeActorsCancellation {
    state: CancellationState,
}

impl Default for NativeActorsCancellation {
    fn default() -> Self {
        Self {
            state: CancellationState {
                token: CancellationToken::new(),
            },
        }
    }
}

#[napi]
impl NativeActorsCancellation {
    /// Creates a cancellation handle in the non-cancelled state.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels attached Rust operations and wakes their transport futures.
    #[napi]
    pub fn cancel(&self) {
        self.state.cancel();
    }

    /// Whether cancellation has been requested.
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.state.token.is_cancelled()
    }
}

fn cancellation_state(
    cancellation: Option<&NativeActorsCancellation>,
) -> Option<CancellationState> {
    cancellation.map(|value| value.state.clone())
}

macro_rules! typed_operation {
    ($name:ident, $request:ty, $method:ident) => {
        async fn $name(
            client: &client::Client,
            request: Buffer,
            cancellation: Option<&NativeActorsCancellation>,
        ) -> std::result::Result<Buffer, ErrorMetadata> {
            let request = decode_wire_metadata::<$request>(&request, stringify!($method))?;
            let response =
                cancellable_metadata(client.$method(&request), cancellation_state(cancellation))
                    .await?;
            encode_wire_metadata(&response, stringify!($method))
        }
    };
}

typed_operation!(create_actor_typed, wire::CreateActorRequest, create_actor);
typed_operation!(update_actor_typed, wire::UpdateActorRequest, update_actor);
typed_operation!(
    inspect_actor_typed,
    wire::InspectActorRequest,
    inspect_actor
);
typed_operation!(
    add_subscription_typed,
    wire::AddSubscriptionRequest,
    add_subscription
);
typed_operation!(
    remove_subscription_typed,
    wire::RemoveSubscriptionRequest,
    remove_subscription
);
typed_operation!(
    resume_subscription_typed,
    wire::ResumeSubscriptionRequest,
    resume_subscription
);
typed_operation!(
    checkpoint_actor_typed,
    wire::CheckpointActorRequest,
    checkpoint_actor
);
typed_operation!(invoke_actor_typed, wire::InvokeActorRequest, invoke_actor);

async fn operation_result<F>(operation: F) -> Result<NativeActorsOperationResult>
where
    F: Future<Output = std::result::Result<Buffer, ErrorMetadata>>,
{
    Ok(match operation.await {
        Ok(value) => NativeActorsOperationResult::success(value),
        Err(error) => NativeActorsOperationResult::failure(error),
    })
}

/// Native Actors client backed directly by the canonical Rust client.
#[napi]
pub struct NativeActorsClient {
    inner: Arc<client::Client>,
}

impl NativeActorsClient {
    fn from_client(inner: client::Client) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[napi]
impl NativeActorsClient {
    /// Connects using the Rust-owned native transport preference and contract.
    #[napi(factory)]
    pub async fn connect(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<Self> {
        let inner = cancellable(
            client::connect(&endpoint, &token),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Connects with a caller-pinned native CA certificate.
    #[napi(factory, js_name = "connectWithCa")]
    pub async fn connect_with_ca(
        endpoint: String,
        token: String,
        ca_certificate_pem: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<Self> {
        let inner = cancellable(
            client::connect_with_ca_certificate(
                &endpoint,
                &token,
                Some(ca_certificate_pem.as_ref()),
            ),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Returns the native bridge package version.
    #[napi]
    pub fn version() -> String {
        PACKAGE_VERSION.to_owned()
    }

    /// Returns the transport selected by the canonical Rust client.
    #[napi]
    pub fn transport(&self) -> String {
        self.inner.transport().to_owned()
    }

    /// Executes CreateActor and returns a typed success/error envelope.
    #[napi(js_name = "createActorResult")]
    pub async fn create_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(create_actor_typed(&self.inner, request, cancellation)).await
    }

    /// Executes UpdateActor and returns a typed success/error envelope.
    #[napi(js_name = "updateActorResult")]
    pub async fn update_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(update_actor_typed(&self.inner, request, cancellation)).await
    }

    /// Executes InspectActor and returns a typed success/error envelope.
    #[napi(js_name = "inspectActorResult")]
    pub async fn inspect_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(inspect_actor_typed(&self.inner, request, cancellation)).await
    }

    /// Executes AddSubscription and returns a typed success/error envelope.
    #[napi(js_name = "addSubscriptionResult")]
    pub async fn add_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(add_subscription_typed(&self.inner, request, cancellation)).await
    }

    /// Executes RemoveSubscription and returns a typed success/error envelope.
    #[napi(js_name = "removeSubscriptionResult")]
    pub async fn remove_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(remove_subscription_typed(
            &self.inner,
            request,
            cancellation,
        ))
        .await
    }

    /// Executes ResumeSubscription and returns a typed success/error envelope.
    #[napi(js_name = "resumeSubscriptionResult")]
    pub async fn resume_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(resume_subscription_typed(
            &self.inner,
            request,
            cancellation,
        ))
        .await
    }

    /// Executes CheckpointActor and returns a typed success/error envelope.
    #[napi(js_name = "checkpointActorResult")]
    pub async fn checkpoint_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(checkpoint_actor_typed(&self.inner, request, cancellation)).await
    }

    /// Executes InvokeActor and returns a typed success/error envelope.
    #[napi(js_name = "invokeActorResult")]
    pub async fn invoke_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        operation_result(invoke_actor_typed(&self.inner, request, cancellation)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protobuf_boundary_preserves_empty_request() -> Result<()> {
        let request = wire::InvokeActorRequest::default();
        let bytes = encode_wire(&request, "invoke_actor request")?;
        let decoded = decode_wire::<wire::InvokeActorRequest>(&bytes, "invoke_actor request")?;
        assert_eq!(decoded, request);
        Ok(())
    }

    #[test]
    fn malformed_protobuf_is_rejected_at_the_boundary() -> Result<()> {
        let Err(error) = decode_wire::<wire::InvokeActorRequest>(
            &Buffer::from(vec![0xff]),
            "invoke_actor request",
        ) else {
            return Err(native_error(
                "malformed protobuf test",
                "decoder accepted invalid bytes",
            ));
        };
        assert!(error.to_string().contains("invoke_actor request"));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_operation() -> Result<()> {
        let cancellation = NativeActorsCancellation::new();
        let pending = tokio::spawn(cancellable(
            std::future::pending::<std::result::Result<(), client::Error>>(),
            cancellation_state(Some(&cancellation)),
        ));
        cancellation.cancel();
        let result = pending
            .await
            .map_err(|join| native_error("cancellation test", join))?;
        let Err(error) = result else {
            return Err(native_error(
                "cancellation test",
                "cancelled operation completed successfully",
            ));
        };
        assert!(error.to_string().contains("Actors operation cancelled"));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_is_monotonic_and_requires_a_fresh_handle() -> Result<()> {
        let cancellation = NativeActorsCancellation::new();
        cancellation.cancel();
        let result = cancellable(
            async { Ok::<(), client::Error>(()) },
            cancellation_state(Some(&cancellation)),
        )
        .await;
        let Err(error) = result else {
            return Err(native_error(
                "cancellation test",
                "cancelled handle was reused",
            ));
        };
        assert!(error.to_string().contains("Actors operation cancelled"));
        assert!(cancellation.cancelled());

        let fresh = NativeActorsCancellation::new();
        let result = cancellable(
            async { Ok::<(), client::Error>(()) },
            cancellation_state(Some(&fresh)),
        )
        .await?;
        assert_eq!(result, ());
        Ok(())
    }
}
