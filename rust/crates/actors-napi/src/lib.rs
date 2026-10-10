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
#[derive(Default, Serialize)]
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
    /// Exact status detail bytes, including malformed payloads.
    #[napi(js_name = "rawDetails")]
    #[serde(
        rename = "rawDetails",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_raw_details"
    )]
    pub raw_details: Option<Buffer>,
}

/// Result envelope used by generated TypeScript adapters.
#[napi(object)]
pub struct NativeActorsOperationResult {
    /// Encoded protobuf response on success.
    pub value: Option<Buffer>,
    /// Structured error on failure.
    pub error: Option<NativeActorsErrorMetadata>,
}

impl NativeActorsOperationResult {
    fn failure(error: NativeActorsErrorMetadata) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }
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

fn serialize_raw_details<S: serde::Serializer>(
    value: &Option<Buffer>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_bytes(value.as_ref()),
        None => serializer.serialize_none(),
    }
}

fn client_error(error: client::Error) -> NativeActorsErrorMetadata {
    let metadata = error.metadata();
    NativeActorsErrorMetadata {
        code: metadata.code,
        message: metadata.message,
        grpc_code: metadata.grpc_code,
        grpc_name: metadata.grpc_name,
        service_code: metadata.service_code,
        service_message: metadata.service_message,
        contract_code: metadata.contract_code,
        raw_details: metadata.raw_details.map(Buffer::from),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserve structured error metadata at the ABI boundary"
)]
fn decode<T: Message + Default>(
    value: &Buffer,
    operation: &str,
) -> std::result::Result<T, NativeActorsErrorMetadata> {
    T::decode(value.as_ref()).map_err(|error| NativeActorsErrorMetadata {
        code: String::from("invalid_argument"),
        message: format!("{operation}: {error}"),
        ..Default::default()
    })
}

#[allow(
    clippy::result_large_err,
    reason = "Preserve structured error metadata at the ABI boundary"
)]
fn decode_semantic<T, D>(
    value: &Buffer,
    operation: &str,
) -> std::result::Result<D, NativeActorsErrorMetadata>
where
    T: Message + Default,
    D: TryFrom<T>,
    D::Error: std::fmt::Display,
{
    let wire = decode::<T>(value, operation)?;
    D::try_from(wire).map_err(|error| NativeActorsErrorMetadata {
        code: String::from("invalid_argument"),
        message: format!("{operation}: {error}"),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_metadata_preserves_original_message_unknown_code_and_bytes() {
        let metadata = client_error(client::Error::Service {
            grpc_code: 7,
            message: String::from("original grpc diagnostic"),
            raw_details: vec![255, 0],
            detail: Some(wire::Error {
                code: 99,
                message: String::from("future detail"),
            }),
        });
        assert_eq!(metadata.message, "original grpc diagnostic");
        assert_eq!(metadata.service_code, Some(99));
        assert_eq!(metadata.service_message.as_deref(), Some("future detail"));
        assert_eq!(
            metadata.raw_details.as_ref().map(AsRef::as_ref),
            Some([255, 0].as_slice())
        );
        let json = serde_json::to_value(&metadata).expect("metadata must serialize");
        assert_eq!(json.get("rawDetails"), Some(&serde_json::json!([255, 0])));
    }

    #[test]
    fn malformed_wire_returns_structured_invalid_argument() {
        let error = decode::<wire::CreateActorRequest>(&Buffer::from(vec![255]), "create_actor")
            .expect_err("malformed protobuf must fail before dispatch");
        assert_eq!(error.code, "invalid_argument");
        assert!(error.message.starts_with("create_actor: "));
    }

    #[test]
    fn invalid_semantics_returns_structured_invalid_argument() {
        let error = decode_semantic::<wire::CreateActorRequest, domain::CreateActorRequest>(
            &Buffer::from(Vec::<u8>::new()),
            "create_actor",
        )
        .expect_err("empty request must fail Rust semantic validation before dispatch");
        assert_eq!(error.code, "invalid_argument");
        assert!(error.message.starts_with("create_actor: "));
    }
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
    #[napi]
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
        let decoded = match decode_semantic::<wire::CreateActorRequest, domain::CreateActorRequest>(
            &request,
            "create_actor",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "updateActorResult")]
    /// Execute `UpdateActor` and return encoded bytes or structured error.
    pub async fn update_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<wire::UpdateActorRequest, domain::UpdateActorRequest>(
            &request,
            "update_actor",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "inspectActorResult")]
    /// Execute `InspectActor` and return encoded bytes or structured error.
    pub async fn inspect_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<wire::InspectActorRequest, domain::InspectActorRequest>(
            &request,
            "inspect_actor",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "addSubscriptionResult")]
    /// Execute `AddSubscription` and return encoded bytes or structured error.
    pub async fn add_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<
            wire::AddSubscriptionRequest,
            domain::AddSubscriptionRequest,
        >(&request, "add_subscription")
        {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "removeSubscriptionResult")]
    /// Execute `RemoveSubscription` and return encoded bytes or structured error.
    pub async fn remove_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<
            wire::RemoveSubscriptionRequest,
            domain::RemoveSubscriptionRequest,
        >(&request, "remove_subscription")
        {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "resumeSubscriptionResult")]
    /// Execute `ResumeSubscription` and return encoded bytes or structured error.
    pub async fn resume_subscription_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<
            wire::ResumeSubscriptionRequest,
            domain::ResumeSubscriptionRequest,
        >(&request, "resume_subscription")
        {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "checkpointActorResult")]
    /// Execute `CheckpointActor` and return encoded bytes or structured error.
    pub async fn checkpoint_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<
            wire::CheckpointActorRequest,
            domain::CheckpointActorRequest,
        >(&request, "checkpoint_actor")
        {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "deleteActorResult")]
    /// Execute `DeleteActor` and return encoded bytes or structured error.
    pub async fn delete_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<wire::DeleteActorRequest, domain::DeleteActorRequest>(
            &request,
            "delete_actor",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
        let value = cancellable(
            self.inner.delete_actor(&decoded),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match value {
            Ok(value) => NativeActorsOperationResult {
                value: Some(
                    encode(&wire::DeleteActorResponse::from(value), "delete_actor")
                        .map_err(napi_error)?,
                ),
                error: None,
            },
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
    #[napi(js_name = "invokeActorResult")]
    /// Execute `InvokeActor` and return encoded bytes or structured error.
    pub async fn invoke_actor_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let decoded = match decode_semantic::<wire::InvokeActorRequest, domain::InvokeActorRequest>(
            &request,
            "invoke_actor",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
        };
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
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
}
