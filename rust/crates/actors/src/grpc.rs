//! Authenticated client for the generated Actors v1 service.

pub use crate::client::BearerAuth;
use crate::wire;
use prost::Message;
use std::sync::Arc;
use tonic::{
    Status,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::field::Empty;

/// Generated client with account authentication on every request.
pub type Client = wire::actors_service_client::ActorsServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Shared transport-status channel retaining the originating trace context.
pub type TracedChannel = acyclic_grpc_observability::TracedChannel<Channel>;

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
    if !reqwest::Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    }) {
        return Err(ConnectError::InsecureEndpoint);
    }
    let configured = Endpoint::from_shared(endpoint.to_owned())?
        .http2_max_header_list_size(tonic_web_wasm_client::limits::HEADER_LIST_BYTES);
    let configured = if let Some(ca) = ca {
        if ca.is_empty() || ca.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCaCertificate);
        }
        configured.tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))?
    } else {
        let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .map_err(ConnectError::PlatformVerifier)?;
        configured.tls_config_with_verifier(ClientTlsConfig::new(), Arc::new(verifier))?
    };
    Ok(configured.connect().await?)
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
    Ok(
        wire::actors_service_client::ActorsServiceClient::with_interceptor(
            TracedChannel::new(channel, transport_span),
            authorization,
        )
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024),
    )
}

/// Decode the canonical semantic error carried in gRPC status details.
#[must_use]
pub fn error_detail(status: &Status) -> Option<wire::Error> {
    let detail = wire::Error::decode(status.details()).ok()?;
    (wire::ErrorCode::try_from(detail.code).ok()? != wire::ErrorCode::Unspecified).then_some(detail)
}
