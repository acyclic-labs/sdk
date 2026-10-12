//! Authenticated client for the generated Workers v1 service.

use crate::wire;
use prost::Message;
use std::sync::Arc;
use tonic::{
    Status,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::field::Empty;

pub use crate::BearerAuth;

/// Generated client with account authentication on every request.
pub type Client = wire::workers_service_client::WorkersServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Shared transport-status channel retaining the originating trace context.
pub type TracedChannel = acyclic_grpc_observability::TracedChannel<Channel, CallSpan>;

/// Statically named workers RPC span factory.
#[derive(Clone, Debug)]
pub struct CallSpan;

impl acyclic_grpc_observability::CallSpan for CallSpan {
    fn span(rpc: Option<&str>) -> tracing::Span {
        tracing::trace_span!(
            "acyclic.workers.grpc.transport",
            rpc,
            rpc.code = Empty,
            outcome = Empty,
            outcome.scope = "grpc_transport",
            error.kind = Empty
        )
    }
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

/// Connects a bounded TLS channel with sensitive bearer metadata.
#[doc(hidden)]
pub async fn connect_channel(
    endpoint: &str, token: &str, ca: Option<&[u8]>,
) -> Result<(Channel, BearerAuth), ConnectError> {
    if !crate::client_config::valid_endpoint(endpoint) {
        return Err(ConnectError::InsecureEndpoint);
    }
    let authorization =
        BearerAuth::new(token).map_err(|()| ConnectError::InvalidCredential)?;
    let configured = Endpoint::from_shared(endpoint.to_owned())?
        .http2_max_header_list_size(tonic_web_wasm_client::limits::HEADER_LIST_BYTES);
    let configured = match ca {
        Some(ca) => {
            if ca.is_empty() || ca.len() > 64 * 1024 {
                return Err(ConnectError::InvalidCaCertificate);
            }
            configured.tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))?
        }
        None => {
            let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(ConnectError::PlatformVerifier)?;
            configured.tls_config_with_verifier(ClientTlsConfig::new(), Arc::new(verifier))?
        }
    };
    Ok((configured.connect().await?, authorization))
}

/// Connect trusting the operating system's roots with account-bound bearer metadata.
pub async fn connect(endpoint: &str, token: &str) -> Result<Client, ConnectError> {
    connect_with_ca_certificate(endpoint, token, None).await
}

/// Connect trusting exactly `ca` when supplied, otherwise the platform's roots.
pub async fn connect_with_ca_certificate(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    let (channel, authorization) = connect_channel(endpoint, token, ca).await?;
    Ok(
        wire::workers_service_client::WorkersServiceClient::with_origin(
            tonic::service::interceptor::InterceptedService::new(
                TracedChannel::new(channel),
                authorization,
            ),
            endpoint
                .trim_end_matches('/')
                .parse()
                .map_err(|_| ConnectError::InsecureEndpoint)?,
        )
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024),
    )
}

/// Decode the wire error carried in gRPC status details, retaining unknown codes.
#[must_use]
pub fn error_detail(status: &Status) -> Option<wire::Error> {
    let detail = wire::Error::decode(status.details()).ok()?;
    (detail.code != wire::ErrorCode::Unspecified as i32).then_some(detail)
}

#[cfg(test)]
#[path = "grpc_tls_tests.rs"]
mod tls_tests;
