//! Shared native TLS channel and exact credential dispatch. A cloned tonic
//! Channel shares its connection; no per-product authority pool or refresh loop.
use crate::credential::{
    CredentialAction, CredentialError, CredentialPending, CredentialProvider, ResolvedCredential,
};
use futures::FutureExt;
use std::{
    future::{Future, Ready},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{
    Request, Response, Status,
    body::Body,
    codegen::{Service, http},
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

fn credential_error(error: CredentialError) -> Status {
    let mut status = Status::unauthenticated("credential source unavailable or changed");
    status.set_source(Arc::new(error));
    status
}

fn stopped_status(reason: crate::operation::Stopped) -> Status {
    let mut status = match reason {
        crate::operation::Stopped::Deadline => {
            Status::deadline_exceeded("operation deadline expired")
        }
        crate::operation::Stopped::Cancelled => Status::cancelled("operation wait cancelled"),
    };
    status.set_source(Arc::new(reason));
    status
}

fn check_context(context: Option<&crate::operation::Context>) -> Result<(), Status> {
    if let Some(context) = context {
        context.check().map_err(stopped_status)?;
    }
    Ok(())
}

/// Reusable channel with a dispatch-boundary lease check. Local refusals remain
/// typed service errors; an interceptor would serialize them as remote statuses.
#[derive(Clone)]
pub struct CredentialChannel {
    channel: Channel,
    context: Option<crate::operation::Context>,
}

impl std::fmt::Debug for CredentialChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialChannel")
            .field("channel", &self.channel)
            .field("operation", &self.context.is_some())
            .finish()
    }
}

impl CredentialChannel {
    /// Check the original operation after a credential owner's admission,
    /// immediately before handing the request to the maintained transport.
    pub fn with_context(mut self, context: Option<crate::operation::Context>) -> Self {
        self.context = context;
        self
    }
}

/// Dispatch adds no allocation to the maintained channel's future.
/// A foreign owner retains the already-ready channel until admission completes.
pub struct CredentialDispatch(Dispatch);

enum Dispatch {
    Refused(Ready<Result<http::Response<Body>, Status>>),
    Admission {
        check: CredentialPending<()>,
        channel: Channel,
        request: Option<http::Request<Body>>,
        context: Option<crate::operation::Context>,
    },
    Transport(<Channel as Service<http::Request<Body>>>::Future),
}

impl Future for CredentialDispatch {
    type Output = Result<http::Response<Body>, Status>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.0 {
                Dispatch::Refused(result) => return Pin::new(result).poll(cx),
                Dispatch::Transport(result) => {
                    return Pin::new(result).poll(cx).map_err(transport_error);
                }
                Dispatch::Admission {
                    check,
                    channel,
                    request,
                    context,
                } => {
                    let result = futures::ready!(check.as_mut().poll(cx));
                    self.0 = match result {
                        Ok(()) => {
                            let request = request.take().expect("admission owns request");
                            match check_context(context.as_ref()) {
                                Ok(()) => Dispatch::Transport(channel.call(request)),
                                Err(error) => Dispatch::Refused(std::future::ready(Err(error))),
                            }
                        }
                        Err(error) => {
                            Dispatch::Refused(std::future::ready(Err(credential_error(error))))
                        }
                    };
                }
            }
        }
    }
}

fn transport_error(error: tonic::transport::Error) -> Status {
    Status::from_error(Box::new(error))
}

impl Service<http::Request<Body>> for CredentialChannel {
    type Response = http::Response<Body>;
    type Error = Status;
    type Future = CredentialDispatch;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Status>> {
        self.channel.poll_ready(cx).map_err(transport_error)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if let Some(lease) = request.extensions().get::<ResolvedCredential>() {
            match lease.check() {
                CredentialAction::Ready(result) => {
                    if let Err(error) = result.now_or_never().expect("ready credential action") {
                        return CredentialDispatch(Dispatch::Refused(std::future::ready(Err(
                            credential_error(error),
                        ))));
                    }
                }
                CredentialAction::Pending(check) => {
                    // The original handle owns the readiness reservation. Its
                    // clone must acquire its own reservation on the next call.
                    let replacement = self.channel.clone();
                    let channel = std::mem::replace(&mut self.channel, replacement);
                    return CredentialDispatch(Dispatch::Admission {
                        check,
                        channel,
                        request: Some(request),
                        context: self.context.clone(),
                    });
                }
            }
        }
        if let Err(error) = check_context(self.context.as_ref()) {
            return CredentialDispatch(Dispatch::Refused(std::future::ready(Err(error))));
        }
        CredentialDispatch(Dispatch::Transport(self.channel.call(request)))
    }
}

/// Retain the same pooled connection while checking each request's exact lease.
pub fn guard(channel: Channel) -> CredentialChannel {
    CredentialChannel {
        channel,
        context: None,
    }
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

/// Local wait ownership for one physical call. A bound operation reuses its
/// host's event and timer; an unbound call retains its monotonic deadline.
#[derive(Clone, Copy)]
pub enum Budget<'a> {
    /// No caller deadline. Dropping the call still cancels its local interest.
    Unbounded,
    /// One monotonic deadline covering refresh, readiness and the response.
    Deadline(tokio::time::Instant),
    /// The original operation event and its projected wire deadline.
    Operation {
        /// Caller-owned stop event shared with every other stage.
        context: &'a crate::operation::Context,
        /// Original absolute deadline projected to the native clock.
        deadline: tokio::time::Instant,
    },
}

impl Budget<'_> {
    fn deadline(self) -> Option<tokio::time::Instant> {
        match self {
            Self::Unbounded => None,
            Self::Deadline(deadline) | Self::Operation { deadline, .. } => Some(deadline),
        }
    }

    /// Wait using exactly the selected primitive. A stop names its original
    /// typed reason and establishes no remote mutation outcome.
    pub async fn run<T>(self, work: impl Future<Output = Result<T, Status>>) -> Result<T, Status> {
        match self {
            Self::Unbounded => work.await,
            Self::Deadline(deadline) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Status::deadline_exceeded("operation deadline expired"));
                }
                let result = tokio::time::timeout_at(deadline, work)
                    .await
                    .map_err(|_| Status::deadline_exceeded("operation deadline expired"))?;
                // The transport can complete with its own timeout in the same
                // poll. The original deadline still owns result admission.
                if tokio::time::Instant::now() >= deadline {
                    return Err(Status::deadline_exceeded("operation deadline expired"));
                }
                result
            }
            Self::Operation { context, .. } => context.run(work).await.map_err(stopped_status)?,
        }
    }
}

/// Apply a source partition's exact lease under the operation's original
/// monotonic deadline, including credential refresh, channel readiness and RPC.
/// Dropping this future cancels its interest. No retries or renewed timeout.
pub async fn call<T, U, F, Fut>(
    channel: Channel,
    credential: &CredentialProvider,
    budget: Budget<'_>,
    body: T,
    dispatch: F,
) -> Result<Response<U>, Status>
where
    F: FnOnce(CredentialChannel, Request<T>) -> Fut,
    Fut: Future<Output = Result<Response<U>, Status>>,
{
    let deadline = budget.deadline();
    budget
        .run(async {
            let lease = credential.resolve().await.map_err(credential_error)?;
            if let Budget::Operation { context, .. } = budget {
                context.check().map_err(stopped_status)?;
            }
            let request = match deadline {
                Some(deadline) => request(body, &lease, deadline)?,
                None => retained_request(body, &lease)?,
            };
            let context = match budget {
                Budget::Operation { context, .. } => Some(context.clone()),
                Budget::Deadline(_) | Budget::Unbounded => None,
            };
            let response = dispatch(guard(channel).with_context(context), request).await;
            if response.as_ref().is_err_and(|error| {
                error.code() == tonic::Code::Unauthenticated
                    && std::error::Error::source(error).is_none()
            }) {
                lease.reject().await.map_err(credential_error)?;
            }
            if response.is_ok() {
                lease.check().await.map_err(credential_error)?;
            }
            response
        })
        .await
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
    let mut request = retained_request(body, lease)?;
    request.set_timeout(remaining);
    Ok(request)
}

/// Retain the original credential on a streaming request whose lifetime is
/// governed by its producer, without imposing a periodic reconnect deadline.
/// Dispatch performs the same exact-lease check as a finite request.
pub fn retained_request<T>(body: T, lease: &ResolvedCredential) -> Result<Request<T>, Status> {
    let mut authorization =
        tonic::metadata::MetadataValue::try_from(crate::credential::bearer_bytes(lease.as_str()))
            .map_err(|_| credential_error(CredentialError::Invalid))?;
    authorization.set_sensitive(true);
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", authorization);
    request.extensions_mut().insert(lease.clone());
    Ok(request)
}
