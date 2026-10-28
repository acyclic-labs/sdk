//! Shared native TLS channel and exact credential dispatch. A cloned tonic
//! Channel shares its connection; no per-product authority pool or refresh loop.
use crate::credential::{CredentialError, CredentialProvider, ResolvedCredential};
use std::{future::Future, sync::Arc};
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

/// Authenticated peer policy, including member/SVID verification supplied by
/// the trust custodian. No identity or issuer is inferred from bearer claims.
pub enum PeerTrust<'a> {
    /// Operating system trust roots and the endpoint's DNS name.
    Platform,
    /// Exactly the caller's private CA bundle and the endpoint's DNS name.
    PrivateCa(&'a [u8]),
    /// Custodian verifier for the expected member or workload SVID.
    Verifier(Arc<dyn rustls::client::danger::ServerCertVerifier>),
}

/// Channel setup errors, without credentials or endpoint user information.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Reject non-TLS endpoints and URI-embedded authority or credentials.
    #[error("invalid authenticated endpoint")]
    Endpoint,
    /// The platform trust verifier could not be initialized.
    #[error("platform verifier unavailable")]
    Platform(#[source] rustls::Error),
    /// The maintained transport could not establish authenticated TLS.
    #[error("authenticated transport unavailable")]
    Transport(#[from] tonic::transport::Error),
}

/// Recheck the retained request lease after tonic channel readiness, immediately
/// before transport dispatch. Requests carrying fixed credentials pass through.
#[derive(Clone, Copy)]
pub struct LeaseCheck;

impl tonic::service::Interceptor for LeaseCheck {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(lease) = request.extensions().get::<ResolvedCredential>() {
            lease.check().map_err(credential_error)?;
        }
        Ok(request)
    }
}

fn credential_error(error: CredentialError) -> Status {
    let mut status = Status::unauthenticated("credential source unavailable or changed");
    status.set_source(Arc::new(error));
    status
}

/// Reusable channel with a dispatch-boundary lease check.
pub type CredentialChannel = tonic::service::interceptor::InterceptedService<Channel, LeaseCheck>;

/// Retain the same pooled connection while checking each request's exact lease.
pub fn guard(channel: Channel) -> CredentialChannel {
    tonic::service::interceptor::InterceptedService::new(channel, LeaseCheck)
}

/// Establish one reusable native channel. Configure the original connection
/// deadline, HTTP/2 credit and transport settings on `endpoint` before calling.
/// `identity` enables mutual TLS and stays inside the native TLS custodian.
pub async fn connect(
    endpoint: Endpoint,
    peer: PeerTrust<'_>,
    identity: Option<Identity>,
) -> Result<Channel, ConnectError> {
    let url = url::Url::parse(&endpoint.uri().to_string()).map_err(|_| ConnectError::Endpoint)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConnectError::Endpoint);
    }
    let mut tls = ClientTlsConfig::new();
    if let Some(identity) = identity {
        tls = tls.identity(identity);
    }
    let endpoint = match peer {
        PeerTrust::Platform => {
            let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(ConnectError::Platform)?;
            endpoint.tls_config_with_verifier(tls, Arc::new(verifier))?
        }
        PeerTrust::PrivateCa(ca) => {
            if ca.is_empty() {
                return Err(ConnectError::Endpoint);
            }
            endpoint.tls_config(tls.ca_certificate(Certificate::from_pem(ca)))?
        }
        PeerTrust::Verifier(verifier) => endpoint.tls_config_with_verifier(tls, verifier)?,
    };
    Ok(endpoint.connect().await?)
}

/// Apply a source partition's exact lease under the operation's original
/// monotonic deadline, including credential refresh, channel readiness and RPC.
/// Dropping this future cancels its interest. No retries or renewed timeout.
pub async fn call<T, U, F, Fut>(
    channel: Channel,
    credential: &CredentialProvider,
    deadline: tokio::time::Instant,
    body: T,
    dispatch: F,
) -> Result<Response<U>, Status>
where
    F: FnOnce(CredentialChannel, Request<T>) -> Fut,
    Fut: Future<Output = Result<Response<U>, Status>>,
{
    tokio::time::timeout_at(deadline, async {
        let lease = credential.resolve().await.map_err(credential_error)?;
        let request = request(body, &lease, deadline)?;
        let response = dispatch(guard(channel), request).await;
        if response.as_ref().is_err_and(|error| {
            error.code() == tonic::Code::Unauthenticated
                && std::error::Error::source(error).is_none()
        }) {
            lease.reject().map_err(credential_error)?;
        }
        if response.is_ok() {
            lease.check().map_err(credential_error)?;
        }
        response
    })
    .await
    .map_err(|_| Status::deadline_exceeded("operation deadline expired"))?
}

/// Build metadata while retaining the exact lease in native request extensions.
/// The caller still owns cancellation/deadline around dispatch and the response.
pub fn request<T>(
    body: T,
    lease: &ResolvedCredential,
    deadline: tokio::time::Instant,
) -> Result<Request<T>, Status> {
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("operation deadline expired"))?;
    lease.check().map_err(credential_error)?;
    let mut authorization = format!("Bearer {}", lease.as_str())
        .parse::<tonic::metadata::MetadataValue<_>>()
        .map_err(|_| credential_error(CredentialError::Invalid))?;
    authorization.set_sensitive(true);
    let mut request = Request::new(body);
    request.set_timeout(remaining);
    request
        .metadata_mut()
        .insert("authorization", authorization);
    request.extensions_mut().insert(lease.clone());
    Ok(request)
}
