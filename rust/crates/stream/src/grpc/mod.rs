//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{future::Future, pin::Pin, sync::Arc, task::Poll, time::Duration};

pub use acyclic_auth::credential::{CredentialError, CredentialProvider};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use prost::Message;
use thiserror::Error;
use tonic::{
    Code, Request, Response, Status,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::field::Empty;

use crate::obs;
use crate::wire_codec::{
    append_from_wire, append_outcome_from_wire, append_outcome_to_wire, commit_from_wire,
    commit_id, commit_outcome_from_wire, commit_outcome_to_wire, commit_to_wire,
    envelope_from_wire, envelope_to_wire, fork_receipt_to_wire, observation_from_wire,
    observation_wire, optional_key, path, read_response_wire, record_wire,
};
use crate::{
    AppendOutcome, AppendRequest, Child, ChildStream, ChildrenPage, ChildrenPageRequest,
    ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, ReadRequest, Record, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, wire,
};
pub use crate::{MAX_BEARER_TOKEN_BYTES, MAX_CA_CERTIFICATE_BYTES};

#[cfg(test)]
mod children_tests;
mod client;
#[cfg(test)]
mod credential_tests;
#[cfg(test)]
mod latency_tests;
mod server;
#[cfg(test)]
pub(crate) mod tests;

const OPERATION_DEADLINE: Duration = Duration::from_secs(10);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Connection configuration failure.
#[derive(Debug, Error)]
pub enum ConnectError {
    /// Endpoint URI was invalid or the TLS connection failed.
    #[error("invalid or unavailable Stream endpoint: {0}")]
    Endpoint(#[from] tonic::transport::Error),
    /// Customer endpoints must use authenticated TLS.
    #[error("Stream endpoints must use https")]
    InsecureEndpoint,
    /// Bearer credential cannot be represented as HTTP metadata.
    #[error("invalid Stream bearer credential")]
    InvalidCredential,
    /// Caller-supplied private CA bundle is empty or exceeds its fixed bound.
    #[error("invalid Stream private CA certificate")]
    InvalidCaCertificate,
    /// The platform certificate verifier could not be initialized.
    #[error("Stream platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
}

/// Authenticated remote provider over one TLS channel.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<CredentialProvider>,
    acknowledge_deliveries: bool,
    sessions: Arc<crate::session::Cache>,
    session_ca: Option<Arc<[u8]>>,
    session_verifier: Option<Arc<dyn rustls::client::danger::ServerCertVerifier>>,
}

/// Thin server adapter from the canonical wire service to one provider.
pub struct Service<P> {
    provider: Arc<P>,
    #[cfg(test)]
    test_delivery: Option<(Bytes, Arc<std::sync::atomic::AtomicUsize>)>,
}

impl<P> Service<P> {
    /// Binds the generated server to one semantic provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            #[cfg(test)]
            test_delivery: None,
        }
    }
}

impl<P> Clone for Service<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            #[cfg(test)]
            test_delivery: self.test_delivery.clone(),
        }
    }
}

impl Client {
    /// Uses a request-local credential while retaining this client's TLS channel.
    pub fn with_credential(&self, bearer_token: &str) -> Result<Self, ConnectError> {
        let mut client = self.clone();
        client.authorization = Some(authorization(bearer_token)?);
        client.credential = None;
        client.sessions = Arc::new(crate::session::Cache::new());
        Ok(client)
    }

    /// Resolve a new credential for every physical RPC, including retries.
    pub fn with_credential_provider(&self, credential: CredentialProvider) -> Self {
        let mut client = self.clone();
        client.authorization = None;
        client.credential = Some(credential);
        client.sessions = Arc::new(crate::session::Cache::new());
        client
    }

    /// Connect without resolving a credential until the first RPC attempt.
    pub async fn connect_eager_with_credential(
        endpoint: impl AsRef<str>,
        credential: CredentialProvider,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        let mut client = Self::from_channel_with_credential(
            Self::connect_channel(endpoint.as_ref(), certificate_pem).await?,
            credential,
        );
        client.session_ca = certificate_pem.map(Arc::from);
        Ok(client)
    }

    /// Connects eagerly through TLS and HTTP/2 trusting the platform roots.
    pub async fn connect_eager(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_tls(endpoint.as_ref(), bearer_token.as_ref(), None).await
    }

    /// Connects eagerly trusting exclusively the caller-pinned private CA.
    pub async fn connect_eager_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        let pem = certificate_pem.as_ref();
        if pem.is_empty() || pem.len() > MAX_CA_CERTIFICATE_BYTES {
            return Err(ConnectError::InvalidCaCertificate);
        }
        Self::connect_tls(endpoint.as_ref(), bearer_token.as_ref(), Some(pem)).await
    }

    async fn connect_tls(
        endpoint: &str,
        bearer_token: &str,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        let authorization = authorization(bearer_token)?;
        let channel = Self::connect_channel(endpoint, certificate_pem).await?;
        let mut client = Self::from_channel(channel, authorization);
        client.session_ca = certificate_pem.map(Arc::from);
        Ok(client)
    }

    async fn connect_channel(
        endpoint: &str,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Channel, ConnectError> {
        if certificate_pem.is_some_and(|pem| pem.is_empty() || pem.len() > MAX_CA_CERTIFICATE_BYTES)
        {
            return Err(ConnectError::InvalidCaCertificate);
        }
        channel_tls(endpoint, certificate_pem).await
    }

    pub(crate) fn from_channel(channel: Channel, authorization: MetadataValue<Ascii>) -> Self {
        Self {
            channel,
            authorization: Some(authorization),
            credential: None,
            acknowledge_deliveries: true,
            sessions: Arc::new(crate::session::Cache::new()),
            session_ca: None,
            session_verifier: None,
        }
    }

    pub(crate) fn from_channel_with_credential(
        channel: Channel,
        credential: CredentialProvider,
    ) -> Self {
        Self {
            channel,
            authorization: None,
            credential: Some(credential),
            acknowledge_deliveries: true,
            sessions: Arc::new(crate::session::Cache::new()),
            session_ca: None,
            session_verifier: None,
        }
    }

    #[cfg(feature = "http")]
    pub(crate) fn with_session_verifier(
        mut self,
        verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
    ) -> Self {
        self.session_verifier = Some(verifier);
        self
    }

    fn service(&self) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(self.channel.clone())
    }

    async fn request<T>(&self, body: T) -> Result<Request<T>, Status> {
        let authorization =
            request_authorization(self.authorization.as_ref(), self.credential.as_ref()).await?;
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        Ok(request)
    }

    /// Retries transport uncertainty with the same request until the operation deadline.
    async fn unary<T, U, F>(
        &self,
        attempt_timeout: Duration,
        body: T,
        mut call: F,
    ) -> Result<(U, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut attempt = 0_u64;
        loop {
            attempt += 1;
            // One span describes one physical RPC attempt.
            let mut completion = Completion::new(tracing::info_span!(
                "acyclic.stream.grpc.call",
                rpc = std::any::type_name::<T>().rsplit("::").next(),
                attempt,
                rpc.code = Empty,
                outcome = Empty,
                error.kind = Empty
            ));
            let attempt_deadline = deadline.min(tokio::time::Instant::now() + attempt_timeout);
            let result = completion
                .span
                .scope(tokio::time::timeout_at(attempt_deadline, async {
                    call(self.service(), self.request(body.clone()).await?).await
                }))
                .await;
            let error = match result {
                Ok(Ok(response)) => return Ok((response.into_inner(), completion)),
                Ok(Err(error)) => error,
                Err(_) => Status::deadline_exceeded("attempt expired"),
            };
            completion.status(&error);
            if !retryable(&error) {
                return Err(status(&error));
            }
            tracing::warn!(
                attempt,
                rpc.code = error.code() as u64,
                "stream rpc retried"
            );
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(status(&error));
            }
        }
    }
}

pub(crate) async fn channel_tls(
    endpoint: &str,
    certificate_pem: Option<&[u8]>,
) -> Result<Channel, ConnectError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())?.connect_timeout(ATTEMPT_TIMEOUT);
    if endpoint.uri().scheme_str() != Some("https") {
        return Err(ConnectError::InsecureEndpoint);
    }
    let endpoint = if let Some(pem) = certificate_pem {
        endpoint.tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)))?
    } else {
        let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .map_err(ConnectError::PlatformVerifier)?;
        endpoint.tls_config_with_verifier(ClientTlsConfig::new(), Arc::new(verifier))?
    };
    Ok(endpoint.tcp_nodelay(true).connect().await?)
}

pub(crate) async fn channel_with_verifier(
    endpoint: &str,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<Channel, ConnectError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())?.connect_timeout(ATTEMPT_TIMEOUT);
    if endpoint.uri().scheme_str() != Some("https") {
        return Err(ConnectError::InsecureEndpoint);
    }
    Ok(endpoint
        .tls_config_with_verifier(ClientTlsConfig::new(), verifier)?
        .tcp_nodelay(true)
        .connect()
        .await?)
}

pub(crate) async fn request_authorization(
    fixed: Option<&MetadataValue<Ascii>>,
    credential: Option<&CredentialProvider>,
) -> Result<MetadataValue<Ascii>, Status> {
    if let Some(provider) = credential {
        let token = provider
            .resolve()
            .await
            .map_err(|_| Status::unauthenticated("credential provider failed"))?;
        authorization(&token).map_err(|_| Status::unauthenticated("invalid credential"))
    } else {
        fixed
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing credential"))
    }
}

fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
    if !acyclic_auth::credential::valid_credential(bearer_token) {
        return Err(ConnectError::InvalidCredential);
    }
    let mut value = format!("Bearer {bearer_token}")
        .parse::<MetadataValue<Ascii>>()
        .map_err(|_| ConnectError::InvalidCredential)?;
    value.set_sensitive(true);
    Ok(value)
}

/// Whether the outcome of an attempt is unknown because the transport was lost.
pub(crate) fn retryable(error: &Status) -> bool {
    match error.code() {
        Code::Unavailable | Code::DeadlineExceeded => return true,
        Code::Unknown if std::error::Error::source(error).is_none() => return true,
        Code::Unknown | Code::Cancelled => {}
        _ => return false,
    }
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        if error.code() == Code::Cancelled {
            // Hyper cancels queued dispatch when its connection driver disappears; a peer's
            // application-level Cancelled status is not transport loss.
            if current
                .downcast_ref::<hyper::Error>()
                .is_some_and(hyper::Error::is_canceled)
                || current.downcast_ref::<h2::Error>().is_some_and(|error| {
                    error.is_remote() && error.reason() == Some(h2::Reason::CANCEL)
                })
            {
                return true;
            }
        } else if current
            .downcast_ref::<std::io::Error>()
            .or_else(|| {
                current
                    .downcast_ref::<h2::Error>()
                    .and_then(h2::Error::get_io)
            })
            .is_some_and(|error| {
                use std::io::ErrorKind::*;
                matches!(
                    error.kind(),
                    UnexpectedEof
                        | ConnectionReset
                        | ConnectionAborted
                        | BrokenPipe
                        | NotConnected
                        | TimedOut
                )
            })
        {
            return true;
        }
        cause = current.source();
    }
    false
}

fn error_status(error: &StreamError) -> Status {
    match error {
        StreamError::InvalidPath => Status::invalid_argument("invalid_path"),
        StreamError::InvalidArgument => Status::invalid_argument("invalid_argument"),
        StreamError::LimitExceeded => Status::invalid_argument("limit_exceeded"),
        StreamError::NotFound => Status::not_found(error.to_string()),
        StreamError::AlreadyExists => Status::already_exists(error.to_string()),
        StreamError::OutOfRange => Status::out_of_range(error.to_string()),
        StreamError::HierarchyChanged => Status::failed_precondition("hierarchy_changed"),
        StreamError::AccessDenied => Status::permission_denied(error.to_string()),
        StreamError::Capacity => Status::resource_exhausted(error.to_string()),
        StreamError::IdempotencyMismatch => Status::failed_precondition("idempotency_mismatch"),
        StreamError::PrefixNotRetained => Status::failed_precondition("prefix_not_retained"),
        StreamError::Unavailable => Status::unavailable(error.to_string()),
        StreamError::DeadlineElapsed => Status::failed_precondition("deadline_elapsed"),
        StreamError::Unsupported => Status::unimplemented("unsupported_capability"),
    }
}

pub(crate) use crate::wire_codec::status;

/// Records one operation's terminal result exactly once; dropping it unfinished records `cancelled`.
struct Completion {
    span: obs::OperationSpan,
    finished: bool,
}

impl Completion {
    fn new(span: tracing::Span) -> Self {
        Self {
            span: obs::OperationSpan::new(span),
            finished: false,
        }
    }

    fn finish(&mut self, kind: Option<&'static str>, code: Option<Code>) {
        if std::mem::replace(&mut self.finished, true) {
            return;
        }
        match kind {
            Some(kind) => obs::failed(&self.span, kind),
            None => {
                self.span.record("outcome", "ok");
            }
        }
        if let Some(code) = code {
            self.span.record("rpc.code", code as u64);
        }
    }

    fn ok(&mut self) {
        self.finish(None, Some(Code::Ok));
    }

    fn status(&mut self, error: &Status) {
        let kind = if error.code() == Code::Cancelled {
            "cancelled"
        } else {
            status(error).code()
        };
        self.finish(Some(kind), Some(error.code()));
    }

    /// A local semantic rejection, not an observed gRPC status.
    fn invalid_response(&mut self) {
        self.finish(Some("invalid_response"), None);
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Some("cancelled"), None);
    }
}

/// Decodes a unary body; tonic already observed its successful trailers.
fn decode_response<T, U>(
    (body, mut completion): (T, Completion),
    decode: impl FnOnce(T) -> Result<U, StreamError>,
) -> Result<U, StreamError> {
    completion.span.record("rpc.code", Code::Ok as u64);
    let result = decode(body);
    match &result {
        Ok(_) => completion.ok(),
        Err(_) => completion.invalid_response(),
    }
    result
}
