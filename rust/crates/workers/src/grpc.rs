//! Authenticated client for the generated Workers v1 service.

use crate::wire;
use prost::Message;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::field::Empty;

/// Bearer metadata applied to every generated RPC.
#[derive(Clone)]
pub struct BearerAuth(MetadataValue<Ascii>);

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if !request.metadata().contains_key("authorization") {
            request
                .metadata_mut()
                .insert("authorization", self.0.clone());
        }
        Ok(request)
    }
}

/// Generated client with account authentication on every request.
pub type Client = wire::workers_service_client::WorkersServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Shared transport-status channel retaining the originating trace context.
///
/// A transport OK does not certify typed protobuf decoding or a unary message.
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
    #[error("Workers endpoints must use https")]
    InsecureEndpoint,
    /// Credential must be valid nonempty HTTP metadata.
    #[error("invalid Workers bearer credential")]
    InvalidCredential,
    /// Private CA must contain between one byte and 64 KiB.
    #[error("invalid Workers private CA")]
    InvalidCaCertificate,
    /// URI or TLS connection failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Connect using standard TLS roots and account-bound bearer metadata.
///
/// # Errors
/// Returns an error for invalid configuration or an unavailable TLS endpoint.
pub async fn connect(endpoint: &str, token: &str) -> Result<Client, ConnectError> {
    connect_with_ca_certificate(endpoint, token, None).await
}

/// Connect with an optional additional private CA, scoped to this connection.
///
/// # Errors
/// Returns an error for invalid configuration, CA bytes, or TLS connection.
pub async fn connect_with_ca_certificate(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    if !crate::client_config::valid_endpoint(endpoint) {
        return Err(ConnectError::InsecureEndpoint);
    }
    let authorization =
        crate::client_config::authorization(token).map_err(|()| ConnectError::InvalidCredential)?;
    let mut tls = ClientTlsConfig::new().with_webpki_roots();
    if let Some(ca) = ca {
        if ca.is_empty() || ca.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCaCertificate);
        }
        tls = tls.ca_certificate(Certificate::from_pem(ca));
    }
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .http2_max_header_list_size(tonic_web_wasm_client::limits::HEADER_LIST_BYTES)
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(
        wire::workers_service_client::WorkersServiceClient::with_origin(
            tonic::service::interceptor::InterceptedService::new(
                TracedChannel::new(channel),
                BearerAuth(authorization),
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
