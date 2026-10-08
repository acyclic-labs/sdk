//! Native N-API bridge for the Rust-owned Actors client.
//!
//! Protobuf bytes are the only operation payload crossing this boundary. The
//! Rust client owns endpoint, credential, contract, transport, and size policy.

use std::{future::Future, sync::Arc};

use acyclic_actors::{client, domain, wire};
use napi::bindgen_prelude::{BigInt, Buffer, Error, Result, Status, Uint8Array};
use napi_derive::napi;
use prost::Message;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Structured Rust-owned error metadata.
#[napi(object)]
#[derive(Clone, Default, Serialize)]
pub struct NativeActorsErrorMetadata {
    /// Stable category.
    pub code: String,
    /// Human-readable reason.
    pub message: String,
    #[napi(js_name = "grpcCode")]
    #[serde(rename = "grpcCode", skip_serializing_if = "Option::is_none")]
    /// Numeric gRPC status code.
    pub grpc_code: Option<i32>,
    #[napi(js_name = "grpcName")]
    #[serde(rename = "grpcName", skip_serializing_if = "Option::is_none")]
    /// gRPC status name.
    pub grpc_name: Option<String>,
    #[napi(js_name = "serviceCode")]
    #[serde(rename = "serviceCode", skip_serializing_if = "Option::is_none")]
    /// Service detail code.
    pub service_code: Option<i32>,
    #[napi(js_name = "serviceMessage")]
    #[serde(rename = "serviceMessage", skip_serializing_if = "Option::is_none")]
    /// Service detail message.
    pub service_message: Option<String>,
    #[napi(js_name = "contractCode")]
    #[serde(rename = "contractCode", skip_serializing_if = "Option::is_none")]
    /// Contract category.
    pub contract_code: Option<String>,
}

/// Result envelope used by generated TypeScript adapters.
#[napi(object)]
pub struct NativeActorsOperationResult {
    /// Encoded protobuf response on success.
    pub value: Option<Buffer>,
    /// Structured error on failure.
    pub error: Option<NativeActorsErrorMetadata>,
}

/// Connection result envelope.
#[napi(object, object_from_js = false)]
pub struct NativeActorsConnectResult {
    /// Native client on success.
    pub client: Option<NativeActorsClient>,
    /// Structured error on failure.
    pub error: Option<NativeActorsErrorMetadata>,
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "N-API error conversion receives owned metadata from fallible boundary adapters"
)]
fn napi_error(metadata: NativeActorsErrorMetadata) -> Error {
    let reason = serde_json::to_string(&metadata)
        .unwrap_or_else(|_| String::from(r#"{"code":"internal","message":"Actors error"}"#));
    Error::new(Status::GenericFailure, reason)
}

fn nominal_error(error: domain::DomainError) -> NativeActorsErrorMetadata {
    let code = error.code_name();
    NativeActorsErrorMetadata {
        code: code.to_owned(),
        message: error.to_string(),
        ..Default::default()
    }
}

/// Validate and return a nominal Actor identity.
#[napi(js_name = "ActorId")]
pub fn actor_id(value: String) -> Result<String> {
    domain::ActorId::new(value)
        .map(|value| value.as_str().to_owned())
        .map_err(|error| napi_error(nominal_error(error)))
}

/// Validate and return a nominal SHA-256 digest.
#[napi(js_name = "CodeSha256")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "N-API receives typed byte buffers by value"
)]
pub fn code_sha256(value: Uint8Array) -> Result<Uint8Array> {
    domain::CodeSha256::new(value.as_ref().to_vec())
        .map(|value| Uint8Array::from(value.as_bytes().to_vec()))
        .map_err(|error| napi_error(nominal_error(error)))
}

/// Validate and return a nominal positive integer.
#[napi(js_name = "PositiveU64")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "N-API receives BigInt handles by value"
)]
pub fn positive_u64(value: BigInt) -> Result<BigInt> {
    let (sign, raw, lossless) = value.get_u64();
    if sign || !lossless {
        return Err(napi_error(NativeActorsErrorMetadata {
            code: String::from("not_positive"),
            message: String::from("value must be a lossless positive u64"),
            ..Default::default()
        }));
    }
    domain::PositiveU64::new(raw)
        .map(|value| BigInt::from(value.get()))
        .map_err(|error| napi_error(nominal_error(error)))
}

/// Validate the nominal current-head marker.  The wire value is still a
/// boolean for descriptor compatibility, but the semantic contract admits
/// only `true`.
#[napi(js_name = "CurrentHeadMarker")]
pub fn current_head_marker(value: bool) -> Result<bool> {
    domain::subscription_start::CurrentHeadMarker::try_from(value)
        .map(bool::from)
        .map_err(|error| napi_error(nominal_error(error)))
}

fn client_error(error: client::Error) -> NativeActorsErrorMetadata {
    let code = error.code_name();
    match error {
        client::Error::Configuration(message) | client::Error::Transport(message) => {
            NativeActorsErrorMetadata {
                code: code.to_owned(),
                message,
                ..Default::default()
            }
        }
        client::Error::Contract(error) => NativeActorsErrorMetadata {
            code: code.to_owned(),
            message: error.to_string(),
            contract_code: Some(code.to_owned()),
            ..Default::default()
        },
        client::Error::Semantic(error) => nominal_error(error),
        client::Error::Service { grpc_code, detail } => NativeActorsErrorMetadata {
            code: code.to_owned(),
            message: detail.as_ref().map_or_else(
                || String::from("Actors service failure"),
                |value| value.message.clone(),
            ),
            grpc_code: Some(grpc_code),
            grpc_name: Some(code.to_owned()),
            service_code: detail.as_ref().map(|value| value.code),
            service_message: detail.map(|value| value.message),
            ..Default::default()
        },
        client::Error::Cancelled => NativeActorsErrorMetadata {
            code: code.to_owned(),
            message: String::from("Actors operation cancelled"),
            grpc_code: Some(1),
            grpc_name: Some(code.to_owned()),
            ..Default::default()
        },
    }
}

fn decode<T: Message + Default>(value: &Buffer, operation: &str) -> Result<T> {
    T::decode(value.as_ref()).map_err(|error| {
        napi_error(NativeActorsErrorMetadata {
            code: String::from("invalid_argument"),
            message: format!("{operation}: {error}"),
            ..Default::default()
        })
    })
}

fn decode_semantic<T, D>(value: &Buffer, operation: &str) -> Result<D>
where
    T: Message + Default,
    D: TryFrom<T>,
    D::Error: std::fmt::Display,
{
    let wire = decode::<T>(value, operation)?;
    D::try_from(wire).map_err(|error| {
        napi_error(NativeActorsErrorMetadata {
            code: String::from("invalid_argument"),
            message: format!("{operation}: {error}"),
            ..Default::default()
        })
    })
}

#[allow(
    clippy::result_large_err,
    reason = "The structured N-API error is intentionally lossless at the ABI boundary"
)]
fn encode<T: Message>(
    value: &T,
    operation: &str,
) -> std::result::Result<Buffer, NativeActorsErrorMetadata> {
    let mut bytes = Vec::with_capacity(value.encoded_len());
    value
        .encode(&mut bytes)
        .map_err(|error| NativeActorsErrorMetadata {
            code: String::from("internal"),
            message: format!("{operation}: {error}"),
            ..Default::default()
        })?;
    Ok(Buffer::from(bytes))
}

#[derive(Clone)]
struct CancellationState {
    token: CancellationToken,
}

/// Monotonic native cancellation handle.
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
    /// Create a fresh cancellation handle.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }
    /// Cancel attached operations.
    #[napi]
    pub fn cancel(&self) {
        self.state.token.cancel();
    }
    /// Whether the handle is cancelled.
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.state.token.is_cancelled()
    }
}

#[allow(
    clippy::result_large_err,
    reason = "The structured N-API error is intentionally lossless at the ABI boundary"
)]
async fn cancellable<T, F: Future<Output = std::result::Result<T, client::Error>>>(
    future: F,
    cancellation: Option<CancellationState>,
) -> std::result::Result<T, NativeActorsErrorMetadata> {
    client::run_with_cancellation(future, cancellation.map(|value| value.token))
        .await
        .map_err(client_error)
}

/// Native client backed directly by the canonical Rust client.
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

async fn connect_envelope<
    F: Future<Output = std::result::Result<client::Client, client::Error>>,
>(
    future: F,
    cancellation: Option<CancellationState>,
) -> NativeActorsConnectResult {
    match cancellable(future, cancellation).await {
        Ok(client) => NativeActorsConnectResult {
            client: Some(NativeActorsClient::from_client(client)),
            error: None,
        },
        Err(error) => NativeActorsConnectResult {
            client: None,
            error: Some(error),
        },
    }
}

#[napi]
impl NativeActorsClient {
    /// Connect using native TLS and Rust-owned policy.
    #[napi(factory)]
    pub async fn connect(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<Self> {
        cancellable(
            client::connect(&endpoint, &token),
            cancellation.map(|value| value.state.clone()),
        )
        .await
        .map(Self::from_client)
        .map_err(napi_error)
    }
    /// Structured connection result.
    #[napi(js_name = "connectResult")]
    pub async fn connect_result(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsConnectResult> {
        Ok(connect_envelope(
            client::connect(&endpoint, &token),
            cancellation.map(|value| value.state.clone()),
        )
        .await)
    }
    /// Connect with an additional native CA certificate.
    #[napi(factory, js_name = "connectWithCa")]
    pub async fn connect_with_ca(
        endpoint: String,
        token: String,
        ca: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<Self> {
        cancellable(
            client::connect_with_ca_certificate(&endpoint, &token, Some(ca.as_ref())),
            cancellation.map(|value| value.state.clone()),
        )
        .await
        .map(Self::from_client)
        .map_err(napi_error)
    }
    /// Structured connection result with a native CA certificate.
    #[napi(js_name = "connectWithCaResult")]
    pub async fn connect_with_ca_result(
        endpoint: String,
        token: String,
        ca: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsConnectResult> {
        Ok(connect_envelope(
            client::connect_with_ca_certificate(&endpoint, &token, Some(ca.as_ref())),
            cancellation.map(|value| value.state.clone()),
        )
        .await)
    }
    /// Native bridge package version.
    pub fn version() -> String {
        PACKAGE_VERSION.to_owned()
    }
    /// Selected transport.
    #[napi(getter, js_name = "transport")]
    pub fn transport(&self) -> String {
        self.inner.transport().to_owned()
    }
}

#[napi]
impl NativeActorsClient {
    #[napi(js_name = "createActorResult")]
    /// Execute `CreateActor` and return encoded bytes or structured error.
    pub async fn create_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<wire::CreateActorRequest, domain::CreateActorRequest>(
            &request,
            "create_actor",
        )?;
        let value = cancellable(
            self.inner.create_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(&wire::CreateActorResponse::from(value), "create_actor")
                        .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "updateActorResult")]
    /// Execute `UpdateActor` and return encoded bytes or structured error.
    pub async fn update_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<wire::UpdateActorRequest, domain::UpdateActorRequest>(
            &request,
            "update_actor",
        )?;
        let value = cancellable(
            self.inner.update_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(&wire::UpdateActorResponse::from(value), "update_actor")
                        .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "inspectActorResult")]
    /// Execute `InspectActor` and return encoded bytes or structured error.
    pub async fn inspect_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<wire::InspectActorRequest, domain::InspectActorRequest>(
            &request,
            "inspect_actor",
        )?;
        let value = cancellable(
            self.inner.inspect_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(&wire::InspectActorResponse::from(value), "inspect_actor")
                        .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "addSubscriptionResult")]
    /// Execute `AddSubscription` and return encoded bytes or structured error.
    pub async fn add_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<
            wire::AddSubscriptionRequest,
            domain::AddSubscriptionRequest,
        >(&request, "add_subscription")?;
        let value = cancellable(
            self.inner.add_subscription(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(
                        &wire::AddSubscriptionResponse::from(value),
                        "add_subscription",
                    )
                    .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "removeSubscriptionResult")]
    /// Execute `RemoveSubscription` and return encoded bytes or structured error.
    pub async fn remove_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<
            wire::RemoveSubscriptionRequest,
            domain::RemoveSubscriptionRequest,
        >(&request, "remove_subscription")?;
        let value = cancellable(
            self.inner.remove_subscription(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(
                        &wire::RemoveSubscriptionResponse::from(value),
                        "remove_subscription",
                    )
                    .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "resumeSubscriptionResult")]
    /// Execute `ResumeSubscription` and return encoded bytes or structured error.
    pub async fn resume_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<
            wire::ResumeSubscriptionRequest,
            domain::ResumeSubscriptionRequest,
        >(&request, "resume_subscription")?;
        let value = cancellable(
            self.inner.resume_subscription(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(
                        &wire::ResumeSubscriptionResponse::from(value),
                        "resume_subscription",
                    )
                    .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "checkpointActorResult")]
    /// Execute `CheckpointActor` and return encoded bytes or structured error.
    pub async fn checkpoint_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<
            wire::CheckpointActorRequest,
            domain::CheckpointActorRequest,
        >(&request, "checkpoint_actor")?;
        let value = cancellable(
            self.inner.checkpoint_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(
                        &wire::CheckpointActorResponse::from(value),
                        "checkpoint_actor",
                    )
                    .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
    #[napi(js_name = "invokeActorResult")]
    /// Execute `InvokeActor` and return encoded bytes or structured error.
    pub async fn invoke_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = decode_semantic::<wire::InvokeActorRequest, domain::InvokeActorRequest>(
            &request,
            "invoke_actor",
        )?;
        let value = cancellable(
            self.inner.invoke_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(&wire::InvokeActorResponse::from(value), "invoke_actor")
                        .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult {
                value: None,
                error: Some(error),
            },
        })
    }
}
