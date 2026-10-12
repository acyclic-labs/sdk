//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{future::Future, pin::Pin, sync::Arc, task::Poll, time::Duration};

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
    commit_id, commit_outcome_from_wire, commit_outcome_to_wire, condition_wire,
    envelope_from_wire, envelope_to_wire, fork_receipt_to_wire, mutation_wire,
    observation_from_wire, observation_wire, optional_key, path, read_response_records,
    read_response_wire, record, record_wire,
};
use crate::{
    AppendOutcome, AppendRequest, Child, ChildStream, ChildrenPage, ChildrenPageRequest,
    ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, ReadRequest, Record, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, wire,
};
pub use crate::{MAX_BEARER_TOKEN_BYTES, MAX_CA_CERTIFICATE_BYTES};

mod client;
mod server;
#[cfg(test)]
mod tests;

const OPERATION_DEADLINE: Duration = Duration::from_secs(10);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const FOLLOW_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
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
    authorization: MetadataValue<Ascii>,
    acknowledge_deliveries: bool,
}

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
        Self::new(Arc::clone(&self.provider))
    }
}

impl Client {
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
        let endpoint =
            Endpoint::from_shared(endpoint.to_owned())?.connect_timeout(ATTEMPT_TIMEOUT);
        if endpoint.uri().scheme_str() != Some("https") {
            return Err(ConnectError::InsecureEndpoint);
        }
        let endpoint = match certificate_pem {
            Some(pem) => endpoint
                .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)))?,
            None => {
                let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .map_err(ConnectError::PlatformVerifier)?;
                endpoint.tls_config_with_verifier(ClientTlsConfig::new(), Arc::new(verifier))?
            }
        };
        Ok(Self::from_channel(endpoint.connect().await?, authorization))
    }

    fn from_channel(channel: Channel, authorization: MetadataValue<Ascii>) -> Self {
        Self {
            channel,
            authorization,
            acknowledge_deliveries: true,
        }
    }

    fn service(&self) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(self.channel.clone())
    }

    fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
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
                .scope(tokio::time::timeout_at(
                    attempt_deadline,
                    call(self.service(), self.request(body.clone())),
                ))
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
            tracing::warn!(attempt, rpc.code = error.code() as u64, "stream rpc retried");
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(status(&error));
            }
        }
    }
}

fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
    if bearer_token.trim().is_empty()
        || bearer_token.len() > MAX_BEARER_TOKEN_BYTES
        || bearer_token.bytes().any(|byte| matches!(byte, b'\r' | b'\n'))
    {
        return Err(ConnectError::InvalidCredential);
    }
    let mut value = format!("Bearer {bearer_token}")
        .parse::<MetadataValue<Ascii>>()
        .map_err(|_| ConnectError::InvalidCredential)?;
    value.set_sensitive(true);
    Ok(value)
}

/// Whether the outcome of an attempt is unknown because the transport was lost.
fn retryable(error: &Status) -> bool {
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
            .or_else(|| current.downcast_ref::<h2::Error>().and_then(h2::Error::get_io))
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

fn status(error: &Status) -> StreamError {
    match (error.code(), error.message()) {
        (Code::InvalidArgument, "invalid_path") => StreamError::InvalidPath,
        (Code::InvalidArgument, "limit_exceeded") => StreamError::LimitExceeded,
        (Code::InvalidArgument, _) => StreamError::InvalidArgument,
        (Code::NotFound, _) => StreamError::NotFound,
        (Code::AlreadyExists, _) => StreamError::AlreadyExists,
        (Code::OutOfRange, _) => StreamError::OutOfRange,
        (Code::PermissionDenied | Code::Unauthenticated, _) => StreamError::AccessDenied,
        (Code::ResourceExhausted, _) => StreamError::Capacity,
        (Code::FailedPrecondition, "hierarchy_changed") => StreamError::HierarchyChanged,
        (Code::FailedPrecondition, "idempotency_mismatch") => StreamError::IdempotencyMismatch,
        (Code::FailedPrecondition, "prefix_not_retained") => StreamError::PrefixNotRetained,
        (Code::FailedPrecondition, "deadline_elapsed") => StreamError::DeadlineElapsed,
        (Code::Unimplemented, "unsupported_capability") => StreamError::Unsupported,
        _ => StreamError::Unavailable,
    }
}

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
