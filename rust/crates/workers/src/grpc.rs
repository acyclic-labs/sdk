//! Authenticated client for the generated Workers v1 service.

use crate::wire;
use http_body::Body as _;
use prost::Message;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{
    Request, Status,
    body::Body,
    codegen::{BoxFuture, Bytes, Service, http},
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

/// Channel that opens one `acyclic.workers.grpc.call` span per RPC, open until the
/// response body ends so it covers the body and a status sent in trailers.
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
                let span = tracing::Span::current();
                match response.await {
                    Err(error) => {
                        record(&span, Err("transport"));
                        Err(error)
                    }
                    // A trailers-only response carries its status in the headers.
                    Ok(response)
                        if response.headers().contains_key("grpc-status")
                            || response.body().is_end_stream() =>
                    {
                        record(&span, Ok(Some(response.headers())));
                        Ok(response)
                    }
                    Ok(response) => {
                        Ok(response.map(|body| Body::new(TracedBody(body, Some(span)))))
                    }
                }
            },
            span,
        ))
    }
}

/// Response body that records the final gRPC status and closes the span at its end.
struct TracedBody(Body, Option<tracing::Span>);

impl TracedBody {
    fn finish(&mut self, status: Result<Option<&http::HeaderMap>, &'static str>) {
        if let Some(span) = self.1.take() {
            record(&span, status);
        }
    }
}

impl http_body::Body for TracedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        let frame = std::task::ready!(Pin::new(&mut self.0).poll_frame(cx));
        match &frame {
            Some(Ok(frame)) => {
                if let Some(trailers) = frame.trailers_ref() {
                    self.finish(Ok(Some(trailers)));
                }
            }
            Some(Err(_)) => self.finish(Err("transport")),
            None => self.finish(Ok(None)),
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

impl Drop for TracedBody {
    fn drop(&mut self) {
        self.finish(Err("cancelled"));
    }
}

/// Records `grpc-status` (absent means OK) or a failure kind on an RPC span.
fn record(span: &tracing::Span, status: Result<Option<&http::HeaderMap>, &'static str>) {
    let code = status.map(|headers| {
        headers
            .and_then(|headers| headers.get("grpc-status"))
            .and_then(|code| code.to_str().ok()?.parse::<u64>().ok())
            .unwrap_or(0)
    });
    match code {
        Ok(0) => span.record("rpc.code", 0).record("outcome", "ok"),
        Ok(code) => span
            .record("rpc.code", code)
            .record("outcome", "err")
            .record("error.kind", "status"),
        Err(kind) => span.record("outcome", "err").record("error.kind", kind),
    };
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
