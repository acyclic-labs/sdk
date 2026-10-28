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
    #[napi(js_name = "credentialRejected")]
    #[serde(rename = "credentialRejected")]
    /// Authenticated remote rejection of the exact request credential.
    pub credential_rejected: bool,
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

/// Rust-backed exact namespace label constructor.
#[napi(js_name = "ActorNamespace")]
pub fn actor_namespace(value: String) -> Result<String> {
    domain::ActorNamespace::new(value)
        .map(|v| v.as_str().to_owned())
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
        credential_rejected: metadata.credential_rejected,
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
        assert_eq!(
            json.get("credentialRejected"),
            Some(&serde_json::json!(false))
        );
    }

    #[test]
    fn malformed_wire_returns_structured_invalid_argument() {
        let error = decode::<wire::DeploymentRequest>(&Buffer::from(vec![255]), "deploy_namespace")
            .expect_err("malformed protobuf must fail before dispatch");
        assert_eq!(error.code, "invalid_argument");
        assert!(error.message.starts_with("deploy_namespace: "));
    }

    #[test]
    fn invalid_semantics_returns_structured_invalid_argument() {
        let error = decode_semantic::<wire::DeploymentRequest, domain::DeploymentRequest>(
            &Buffer::from(Vec::<u8>::new()),
            "deploy_namespace",
        )
        .expect_err("empty request must fail Rust semantic validation before dispatch");
        assert_eq!(error.code, "invalid_argument");
        assert!(error.message.starts_with("deploy_namespace: "));
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
            client::connect(&endpoint, &token, None),
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
            client::connect(&endpoint, &token, None),
            cancellation.map(|value| value.state.clone()),
        )
        .await)
    }
    /// Connect trusting exclusively the supplied native PEM CA certificate.
    #[napi(factory, js_name = "connectWithCa")]
    pub async fn connect_with_ca(
        endpoint: String,
        token: String,
        ca: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<Self> {
        cancellable(
            client::connect(&endpoint, &token, Some(ca.as_ref())),
            cancellation.map(|value| value.state.clone()),
        )
        .await
        .map(Self::from_client)
        .map_err(napi_error)
    }
    /// Structured connection result trusting exclusively the supplied native PEM CA.
    #[napi(js_name = "connectWithCaResult")]
    pub async fn connect_with_ca_result(
        endpoint: String,
        token: String,
        ca: Buffer,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsConnectResult> {
        Ok(connect_envelope(
            client::connect(&endpoint, &token, Some(ca.as_ref())),
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
    /// Execute one operation with a request-local credential on the existing connection.
    #[napi(js_name = "callWithCredentialResult")]
    pub async fn call_with_credential_result(
        &self,
        method: String,
        request: Buffer,
        token: String,
        cancellation: Option<&NativeActorsCancellation>,
    ) -> Result<NativeActorsOperationResult> {
        let result = cancellable(
            self.inner.call_with_credential(&method, &request, &token),
            cancellation.map(|value| value.state.clone()),
        )
        .await;
        Ok(match result {
            Ok(value) => NativeActorsOperationResult {
                value: Some(value.into()),
                error: None,
            },
            Err(error) => NativeActorsOperationResult::failure(error),
        })
    }
}

// The ABI owns only names and carriers; validation, cancellation and metadata
// stay in the same Rust semantic client path for every operation.
macro_rules! operation_wrappers {
    ($(($export:ident, $js:literal, $method:ident, $input:ident, $output:ident)),+ $(,)?) => {
        #[napi]
        impl NativeActorsClient {
            $(
            #[napi(js_name = $js)]
            #[doc = concat!("Execute `", stringify!($method), "` and return encoded bytes or structured error.")]
            pub async fn $export(
                &self,
                request: Buffer,
                cancellation: Option<&NativeActorsCancellation>,
            ) -> Result<NativeActorsOperationResult> {
                let decoded = match decode_semantic::<wire::$input, domain::$input>(
                    &request,
                    stringify!($method),
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(NativeActorsOperationResult::failure(error)),
                };
                let value = cancellable(
                    self.inner.$method(&decoded),
                    cancellation.map(|value| value.state.clone()),
                )
                .await;
                Ok(match value {
                    Ok(value) => NativeActorsOperationResult {
                        value: Some(
                            encode(&wire::$output::from(value), stringify!($method))
                                .map_err(napi_error)?,
                        ),
                        error: None,
                    },
                    Err(error) => NativeActorsOperationResult::failure(error),
                })
            }
            )+
        }
    };
}

operation_wrappers! {
    (deploy_namespace_result, "deployNamespaceResult", deploy_namespace, DeploymentRequest, DeploymentResponse),
    (deploy_worker_result, "deployWorkerResult", deploy_worker, DeploymentRequest, DeploymentResponse),
    (invoke_worker_result, "invokeWorkerResult", invoke_worker, InvokeWorkerRequest, InvokeActorResponse),
    (delete_worker_result, "deleteWorkerResult", delete_worker, DeleteWorkerRequest, DeleteWorkerResponse),
    (update_namespace_result, "updateNamespaceResult", update_namespace, UpdateNamespaceRequest, UpdateNamespaceResponse),
    (inspect_actor_result, "inspectActorResult", inspect_actor, InspectActorRequest, InspectActorResponse),
    (add_subscription_result, "addSubscriptionResult", add_subscription, AddSubscriptionRequest, AddSubscriptionResponse),
    (remove_subscription_result, "removeSubscriptionResult", remove_subscription, RemoveSubscriptionRequest, RemoveSubscriptionResponse),
    (resume_subscription_result, "resumeSubscriptionResult", resume_subscription, ResumeSubscriptionRequest, ResumeSubscriptionResponse),
    (checkpoint_actor_result, "checkpointActorResult", checkpoint_actor, CheckpointActorRequest, CheckpointActorResponse),
    (delete_all_result, "deleteAllResult", delete_all, DeleteAllRequest, DeleteAllResponse),
    (invoke_actor_result, "invokeActorResult", invoke_actor, InvokeActorRequest, InvokeActorResponse),
}
