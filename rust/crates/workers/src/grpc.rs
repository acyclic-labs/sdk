//! Authenticated client for the generated Workers v1 service.

use crate::wire;
use prost::Message;
use std::task::{Context, Poll};
use tonic::{
    Request, Status,
    body::Body,
    codegen::{BoxFuture, Service, http},
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
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

/// Generated client with account authentication on every request.
pub type Client = wire::workers_service_client::WorkersServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Channel that opens one `acyclic.workers.grpc.call` span per RPC.
#[derive(Clone, Debug)]
pub struct TracedChannel(pub(crate) Channel);

impl Service<http::Request<Body>> for TracedChannel {
    type Response = http::Response<Body>;
    type Error = tonic::transport::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let span = tracing::info_span!(
            "acyclic.workers.grpc.call",
            rpc = request.uri().path().rsplit('/').next(),
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty,
        );
        let response = self.0.call(request);
        Box::pin(tracing::Instrument::instrument(
            async move {
                let result = response.await;
                // A status sent only in trailers (rare for unary errors) reads as OK here.
                let code = result.as_ref().map(|response| {
                    response
                        .headers()
                        .get("grpc-status")
                        .and_then(|code| code.to_str().ok()?.parse::<u64>().ok())
                        .unwrap_or(0)
                });
                let span = tracing::Span::current();
                match code {
                    Ok(0) => span.record("rpc.code", 0).record("outcome", "ok"),
                    Ok(code) => span
                        .record("rpc.code", code)
                        .record("outcome", "err")
                        .record("error.kind", "status"),
                    Err(_) => span
                        .record("outcome", "err")
                        .record("error.kind", "transport"),
                };
                result
            },
            span,
        ))
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
    let valid_endpoint = reqwest::Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    });
    if !valid_endpoint {
        return Err(ConnectError::InsecureEndpoint);
    }
    if !crate::valid_token(token) {
        return Err(ConnectError::InvalidCredential);
    }
    let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|_| ConnectError::InvalidCredential)?;
    authorization.set_sensitive(true);
    let mut tls = ClientTlsConfig::new().with_webpki_roots();
    if let Some(ca) = ca {
        if ca.is_empty() || ca.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCaCertificate);
        }
        tls = tls.ca_certificate(Certificate::from_pem(ca));
    }
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(
        wire::workers_service_client::WorkersServiceClient::with_interceptor(
            TracedChannel(channel),
            BearerAuth(authorization),
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
