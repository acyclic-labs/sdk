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
    transport::{Channel, Endpoint},
};
use tracing::field::Empty;

use crate::StreamProviderExt as _;
use crate::obs;
use crate::wire_codec::{
    append_from_wire, append_outcome_from_wire, append_outcome_to_wire, commit_from_wire,
    commit_id, commit_outcome_from_wire, commit_outcome_to_wire, commit_to_wire,
    envelope_from_wire, envelope_to_wire, fork_receipt_to_wire, observation_from_wire,
    observation_wire, path, read_response_wire, record_wire,
};
use crate::{
    AppendOutcome, AppendRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, ReadRequest, Record,
    RecordStream, StreamError, StreamProvider, wire,
};
pub use crate::{MAX_BEARER_TOKEN_BYTES, MAX_CA_CERTIFICATE_BYTES};

mod client;
#[cfg(test)]
mod credential_tests;
#[cfg(test)]
mod latency_tests;
#[cfg(test)]
use crate::StreamPath;
#[cfg(test)]
mod receipt_tests;
mod server;
#[cfg(test)]
pub(crate) mod tests;

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

impl ConnectError {
    pub(crate) fn transient(&self) -> bool {
        let Self::Endpoint(error) = self else {
            return false;
        };
        let mut cause = std::error::Error::source(error);
        while let Some(current) = cause {
            if transient_io(current) {
                return true;
            }
            cause = current.source();
        }
        false
    }
}

/// Authenticated remote provider over one TLS channel.
///
/// Routed sessions retain the supplied connection policy, including peer
/// verification and mutual TLS, through [`Client::from_authenticated_channel`].
#[derive(Clone)]
pub struct Client {
    operation_claims: Arc<crate::OperationClaims>,
    channel: Channel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<CredentialProvider>,
    acknowledge_deliveries: bool,
    sessions: Arc<crate::session::Cache>,
    /// Read-your-writes floors; kept across credential changes, which only
    /// replace the session pool.
    revisions: crate::query_sync::Arc<crate::RevisionTracker>,
    session_ca: Option<Arc<[u8]>>,
    session_verifier: Option<Arc<dyn rustls::client::danger::ServerCertVerifier>>,
    session_connector: Option<ChannelConnector>,
}

/// Caller-owned policy for a service-provided session route. The connector must
/// authenticate the advertised member and admit the endpoint before connecting;
/// a route's URL is not itself trust evidence. Use Auth's shared channel setup
/// with the same member/SVID policy and client identity as the home channel.
pub type ChannelConnector = Arc<
    dyn Fn(wire::SessionNode) -> futures::future::BoxFuture<'static, Result<Channel, StreamError>>
        + Send
        + Sync,
>;

/// Thin server adapter from the canonical wire service to one provider.
pub struct Service<P> {
    provider: Arc<P>,
}

impl<P> Service<P> {
    /// Binds the generated server to one semantic provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

impl<P> Clone for Service<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
        }
    }
}

impl Client {
    /// Adopt an authenticated home channel and an explicit policy for every
    /// routed connection. No credential resolution or connection starts here.
    /// There is no fallback from the supplied connector to platform trust.
    pub fn from_authenticated_channel(
        channel: Channel,
        credential: CredentialProvider,
        connect: ChannelConnector,
    ) -> Self {
        let mut client = Self::from_channel_with_credential(channel, credential);
        client.session_connector = Some(connect);
        client
    }

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

    /// Connects eagerly over plain HTTP/2 to a loopback endpoint on this
    /// machine, such as a self-hosted server under test. Only an `http://`
    /// URL whose host is a loopback IP literal or `localhost` is admitted;
    /// every other endpoint needs TLS.
    pub async fn connect_loopback(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        let authorization = authorization(bearer_token.as_ref())?;
        let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())?;
        let uri = endpoint.uri();
        let loopback = uri.host().is_some_and(|host| {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if uri.scheme_str() != Some("http") || !loopback {
            return Err(ConnectError::InsecureEndpoint);
        }
        Ok(Self::from_channel(
            endpoint.tcp_nodelay(true).connect().await?,
            authorization,
        ))
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
            operation_claims: Arc::new(crate::OperationClaims::default()),
            sessions: Arc::new(crate::session::Cache::new()),
            revisions: crate::query_sync::Arc::default(),
            session_ca: None,
            session_verifier: None,
            session_connector: None,
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
            operation_claims: Arc::new(crate::OperationClaims::default()),
            sessions: Arc::new(crate::session::Cache::new()),
            revisions: crate::query_sync::Arc::default(),
            session_ca: None,
            session_verifier: None,
            session_connector: None,
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

    #[cfg(feature = "http")]
    pub(crate) fn with_operation_claims(mut self, claims: Arc<crate::OperationClaims>) -> Self {
        self.operation_claims = claims;
        self
    }

    fn service(
        &self,
    ) -> wire::stream_service_client::StreamServiceClient<acyclic_auth::channel::CredentialChannel>
    {
        wire::stream_service_client::StreamServiceClient::new(acyclic_auth::channel::guard(
            self.channel.clone(),
        ))
    }

    async fn request<T>(&self, body: T) -> Result<Request<T>, Status> {
        let (authorization, lease) =
            request_authorization(self.authorization.as_ref(), self.credential.as_ref()).await?;
        let mut request = Request::new(body);
        if let Some(lease) = lease {
            request.extensions_mut().insert(lease);
        }
        request
            .metadata_mut()
            .insert("authorization", authorization);
        Ok(request)
    }

    /// Retries observed transient transport failures with the same request.
    /// The caller owns cancellation and any request deadline.
    async fn unary<T, U, F>(&self, body: T, mut call: F) -> Result<(U, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<
                acyclic_auth::channel::CredentialChannel,
            >,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let mut attempt = 0_u64;
        loop {
            attempt = attempt.saturating_add(1);
            // One span describes one physical RPC attempt.
            let mut completion = Completion::new(tracing::info_span!(
                "acyclic.stream.grpc.call",
                rpc = std::any::type_name::<T>().rsplit("::").next(),
                attempt,
                rpc.code = Empty,
                outcome = Empty,
                error.kind = Empty
            ));
            let result = completion
                .span
                .scope(async {
                    let request = self.request(body.clone()).await?;
                    let lease = request
                        .extensions()
                        .get::<acyclic_auth::credential::ResolvedCredential>()
                        .cloned();
                    let result = call(self.service(), request).await;
                    if result.as_ref().is_err_and(|error| {
                        error.code() == Code::Unauthenticated
                            && std::error::Error::source(error).is_none()
                    }) {
                        if let Some(lease) = &lease {
                            lease.reject().await.map_err(|_| {
                                Status::unauthenticated("credential source unavailable")
                            })?;
                        }
                    }
                    if result.is_ok() {
                        if let Some(lease) = &lease {
                            lease.check().await.map_err(|error| {
                                let mut status = Status::unauthenticated(
                                    "credential source unavailable or changed",
                                );
                                status.set_source(Arc::new(error));
                                status
                            })?;
                        }
                    }
                    result
                })
                .await;
            let error = match result {
                Ok(response) => return Ok((response.into_inner(), completion)),
                Err(error) => error,
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
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

pub(crate) async fn channel_tls(
    endpoint: &str,
    certificate_pem: Option<&[u8]>,
) -> Result<Channel, ConnectError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
    if endpoint.uri().scheme_str() != Some("https") {
        return Err(ConnectError::InsecureEndpoint);
    }
    let peer = if let Some(pem) = certificate_pem {
        acyclic_auth::channel::PeerTrust::PrivateCa(pem)
    } else {
        acyclic_auth::channel::PeerTrust::Platform
    };
    shared_channel(endpoint, peer).await
}

pub(crate) async fn channel_with_verifier(
    endpoint: &str,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<Channel, ConnectError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
    if endpoint.uri().scheme_str() != Some("https") {
        return Err(ConnectError::InsecureEndpoint);
    }
    shared_channel(
        endpoint,
        acyclic_auth::channel::PeerTrust::Verifier(verifier),
    )
    .await
}

async fn shared_channel(
    endpoint: Endpoint,
    peer: acyclic_auth::channel::PeerTrust<'_>,
) -> Result<Channel, ConnectError> {
    acyclic_auth::channel::connect(endpoint.tcp_nodelay(true), peer, None)
        .await
        .map_err(|error| match error {
            acyclic_auth::channel::ConnectError::Endpoint => ConnectError::InsecureEndpoint,
            acyclic_auth::channel::ConnectError::Platform(error) => {
                ConnectError::PlatformVerifier(error)
            }
            acyclic_auth::channel::ConnectError::Transport(error) => ConnectError::Endpoint(error),
        })
}

pub(crate) async fn request_authorization(
    fixed: Option<&MetadataValue<Ascii>>,
    credential: Option<&CredentialProvider>,
) -> Result<
    (
        MetadataValue<Ascii>,
        Option<acyclic_auth::credential::ResolvedCredential>,
    ),
    Status,
> {
    if let Some(provider) = credential {
        let token = provider
            .resolve()
            .await
            .map_err(|_| Status::unauthenticated("credential provider failed"))?;
        token
            .check()
            .await
            .map_err(|_| Status::unauthenticated("credential expired or changed"))?;
        Ok((
            authorization(&token).map_err(|_| Status::unauthenticated("invalid credential"))?,
            Some(token),
        ))
    } else {
        fixed
            .cloned()
            .map(|header| (header, None))
            .ok_or_else(|| Status::unauthenticated("missing credential"))
    }
}

fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
    if !acyclic_auth::credential::valid_credential(bearer_token) {
        return Err(ConnectError::InvalidCredential);
    }
    let mut value = MetadataValue::try_from(acyclic_auth::credential::bearer_bytes(bearer_token))
        .map_err(|_| ConnectError::InvalidCredential)?;
    value.set_sensitive(true);
    Ok(value)
}

/// Whether the outcome of an attempt is unknown because the transport was lost.
pub(crate) fn retryable(error: &Status) -> bool {
    match error.code() {
        Code::Unavailable => return true,
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
        } else if transient_io(current) {
            return true;
        }
        cause = current.source();
    }
    false
}

fn transient_io(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .or_else(|| {
            error
                .downcast_ref::<h2::Error>()
                .and_then(h2::Error::get_io)
        })
        .is_some_and(|error| {
            use std::io::ErrorKind::*;
            matches!(
                error.kind(),
                ConnectionRefused
                    | ConnectionReset
                    | ConnectionAborted
                    | BrokenPipe
                    | NotConnected
                    | UnexpectedEof
                    | TimedOut
            )
        })
}

use crate::wire_codec::error_status;

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
