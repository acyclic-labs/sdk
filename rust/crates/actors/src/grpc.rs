//! Canonical Actors v1 status projection and authenticated client.

pub use crate::client::BearerAuth;
use crate::wire;
use prost::Message;
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};
use tracing::field::Empty;

/// Generated client with account authentication on every request.
pub type Client = wire::actors_service_client::ActorsServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Shared transport-status channel retaining the originating trace context.
pub type TracedChannel =
    acyclic_grpc_observability::TracedChannel<acyclic_auth::channel::CredentialChannel>;

/// Encodes a semantic failure with its producer's original optional attribution.
///
/// The status message retains the canonical protocol name. Overload retry hints
/// belong to the admission producer and are not inferred from semantic codes.
#[must_use]
pub fn error_status(error: wire::ErrorCode, cause: Option<acyclic_auth::refusal::Cause>) -> Status {
    use tonic::Code;
    use wire::ErrorCode as E;
    let (code, name) = match error {
        E::Unspecified => (Code::Internal, "ERROR_CODE_UNSPECIFIED"),
        E::InvalidArgument => (Code::InvalidArgument, "ERROR_CODE_INVALID_ARGUMENT"),
        E::CapabilityDenied => (Code::PermissionDenied, "ERROR_CODE_CAPABILITY_DENIED"),
        E::CapabilityExpired => (Code::Unauthenticated, "ERROR_CODE_CAPABILITY_EXPIRED"),
        E::ActorNotFound => (Code::NotFound, "ERROR_CODE_ACTOR_NOT_FOUND"),
        E::SubscriptionNotFound => (Code::NotFound, "ERROR_CODE_SUBSCRIPTION_NOT_FOUND"),
        E::IdempotencyMismatch => (Code::FailedPrecondition, "ERROR_CODE_IDEMPOTENCY_MISMATCH"),
        E::Conflict => (Code::FailedPrecondition, "ERROR_CODE_CONFLICT"),
        E::AdmissionDenied => (Code::PermissionDenied, "ERROR_CODE_ADMISSION_DENIED"),
        E::CheckpointFailed => (Code::Aborted, "ERROR_CODE_CHECKPOINT_FAILED"),
        E::DependencyUnavailable => (Code::Unavailable, "ERROR_CODE_DEPENDENCY_UNAVAILABLE"),
    };
    let mut detail = crate::domain::ServiceError::new(error, name.into());
    if let Some(cause) = cause {
        detail = detail.with_cause(cause);
    }
    let detail: wire::Error = detail.into();
    let bytes = detail.encode_to_vec();
    Status::with_details(code, detail.message, bytes.into())
}

/// Statically named actors RPC span factory.
pub(crate) fn transport_span(rpc: Option<&str>) -> tracing::Span {
    tracing::trace_span!(
        "acyclic.actors.grpc.transport",
        rpc,
        rpc.code = Empty,
        outcome = Empty,
        outcome.scope = "grpc_transport",
        error.kind = Empty
    )
}
/// Client configuration error.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Endpoint must use authenticated TLS.
    #[error("endpoint must use https")]
    InsecureEndpoint,
    /// Credential must be valid nonempty HTTP metadata.
    #[error("invalid bearer credential")]
    InvalidCredential,
    /// Private CA must contain between one byte and 64 KiB.
    #[error("invalid private CA")]
    InvalidCaCertificate,
    /// The platform certificate verifier could not be initialized.
    #[error("platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
    /// URI or TLS connection failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Connects a bounded TLS channel.
async fn connect_channel(endpoint: &str, ca: Option<&[u8]>) -> Result<Channel, ConnectError> {
    let configured = Endpoint::from_shared(endpoint.to_owned())?
        .http2_max_header_list_size(tonic_web_wasm_client::limits::HEADER_LIST_BYTES);
    let peer = if let Some(ca) = ca {
        if ca.is_empty() || ca.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCaCertificate);
        }
        acyclic_auth::channel::PeerTrust::PrivateCa(ca)
    } else {
        acyclic_auth::channel::PeerTrust::Platform
    };
    acyclic_auth::channel::connect(configured, peer, None)
        .await
        .map_err(|error| match error {
            acyclic_auth::channel::ConnectError::Endpoint => ConnectError::InsecureEndpoint,
            acyclic_auth::channel::ConnectError::Platform(error) => {
                ConnectError::PlatformVerifier(error)
            }
            acyclic_auth::channel::ConnectError::Transport(error) => ConnectError::Transport(error),
        })
}

/// Connect trusting exactly `ca` when supplied, otherwise the platform's roots.
pub async fn connect(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    let authorization = BearerAuth::new(token).map_err(|_| ConnectError::InvalidCredential)?;
    connect_authorized(endpoint, authorization, ca).await
}

pub(crate) async fn connect_authorized(
    endpoint: &str,
    authorization: BearerAuth,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    let channel = connect_channel(endpoint, ca).await?;
    Ok(over(channel, authorization))
}

pub(crate) fn over(channel: Channel, authorization: BearerAuth) -> Client {
    wire::actors_service_client::ActorsServiceClient::with_interceptor(
        TracedChannel::new(acyclic_auth::channel::guard(channel), transport_span),
        authorization,
    )
    .max_decoding_message_size(16 * 1024 * 1024)
    .max_encoding_message_size(16 * 1024 * 1024)
}

/// Decode the canonical semantic error carried in gRPC status details.
#[must_use]
pub fn error_detail(status: &Status) -> Option<wire::Error> {
    let detail = wire::Error::decode(status.details()).ok()?;
    (wire::ErrorCode::try_from(detail.code).ok()? != wire::ErrorCode::Unspecified).then_some(detail)
}
