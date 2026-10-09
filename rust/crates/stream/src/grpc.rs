//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

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
#[cfg(test)]
use tracing::Instrument as _;
use tracing::field::Empty;
#[cfg(test)]
use tracing::instrument::WithSubscriber as _;

use crate::obs;
use crate::wire_codec::{
    append_outcome_from_wire, append_outcome_wire, commit_id, commit_outcome_from_wire,
    commit_outcome_wire, condition_from_wire, condition_wire, envelope_from_wire, envelope_wire,
    fork_receipt_wire, mutation_from_wire, mutation_wire, observation_from_wire, observation_wire,
    optional_key, path, read_response_records, read_response_wire, record, record_wire,
};
use crate::{
    AppendOutcome, AppendRequest, Child, ChildStream, ChildrenPage, ChildrenPageRequest,
    ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, ReadRequest, Record, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, wire,
};

const OPERATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const OPERATION_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10);
/// Maximum independently reachable endpoints in one operation or follow pool.
pub const MAX_ENDPOINTS: usize = 16;
/// Maximum canonical URI bytes accepted for one endpoint.
pub const MAX_ENDPOINT_URI_BYTES: usize = 2_048;
pub use crate::MAX_BEARER_TOKEN_BYTES;
pub use crate::MAX_CA_CERTIFICATE_BYTES;

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
    /// At least one independently reachable endpoint is required.
    #[error("at least one Stream endpoint is required")]
    NoEndpoints,
    /// Endpoint count or URI bytes exceed the fixed client bound.
    #[error("Stream endpoint pool exceeds its fixed bound")]
    EndpointLimit,
}

/// Authenticated remote provider.
#[derive(Clone)]
pub struct Client {
    channels: Arc<[Channel]>,
    authorization: MetadataValue<Ascii>,
    preferred: Arc<AtomicUsize>,
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
        Self {
            provider: Arc::clone(&self.provider),
        }
    }
}

fn check_command_size<T: Message>(request: &T) -> Result<(), Status> {
    if request.encoded_len() > crate::MAX_COMMAND_BYTES {
        Err(error_status(&StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

impl Client {
    /// Connects to a TLS endpoint with an account-bound bearer credential.
    pub async fn connect(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_endpoints([endpoint], bearer_token).await
    }

    /// Connects to independently reachable TLS endpoints with transparent failover.
    pub async fn connect_endpoints<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_tls(endpoints, bearer_token, None)
    }

    /// Connects to a TLS endpoint augmented by one caller-pinned private CA certificate.
    pub async fn connect_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_endpoints_with_ca_certificate(
            [endpoint],
            bearer_token,
            certificate_pem.as_ref(),
        )
        .await
    }

    /// Connects eagerly through TLS and HTTP/2, retaining the resulting channel
    /// for later operations. Native callers use this path when cancellation
    /// must cover connection establishment itself; [`Self::connect`] remains
    /// lazy for the ordinary managed client.
    pub async fn connect_eager(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_endpoints([endpoint], bearer_token, None).await
    }

    /// Eagerly connects through ambient roots plus one caller-pinned private CA.
    pub async fn connect_eager_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_endpoints([endpoint], bearer_token, Some(certificate_pem.as_ref()))
            .await
    }

    /// Connects to independently reachable endpoints using one caller-pinned private CA.
    pub async fn connect_endpoints_with_ca_certificate<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_tls(endpoints, bearer_token, Some(certificate_pem.as_ref()))
    }

    fn connect_with_tls<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let channels = Self::channels_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        Self::from_channels(channels, bearer_token)
    }

    async fn connect_eager_endpoints<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let authorization = Self::authorization(bearer_token.as_ref())?;
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let endpoints = Self::endpoints_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        let mut channels = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            channels.push(endpoint.connect().await?);
        }
        Ok(Self::from_authorization(channels.into(), authorization))
    }

    fn channels_with_tls<I, S>(
        endpoints: I,
        certificate_pem: Option<&[u8]>,
        connect_timeout: std::time::Duration,
    ) -> Result<Arc<[Channel]>, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(
            Self::endpoints_with_tls(endpoints, certificate_pem, connect_timeout)?
                .into_iter()
                .map(|endpoint| endpoint.connect_lazy())
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn endpoints_with_tls<I, S>(
        endpoints: I,
        certificate_pem: Option<&[u8]>,
        connect_timeout: std::time::Duration,
    ) -> Result<Vec<Endpoint>, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut configured = Vec::new();
        for endpoint in endpoints.into_iter().take(MAX_ENDPOINTS + 1) {
            if configured.len() == MAX_ENDPOINTS {
                return Err(ConnectError::EndpointLimit);
            }
            let endpoint = endpoint.as_ref();
            if endpoint.len() > MAX_ENDPOINT_URI_BYTES {
                return Err(ConnectError::EndpointLimit);
            }
            // `Endpoint::new` enables the compiled WebPKI roots for HTTPS. The lower-level
            // `from_shared` constructor leaves TLS disabled and fails only on first use.
            let mut endpoint = Endpoint::new(endpoint.to_owned())?.connect_timeout(connect_timeout);
            if endpoint.uri().scheme_str() != Some("https") {
                return Err(ConnectError::InsecureEndpoint);
            }
            if let Some(certificate_pem) = certificate_pem {
                endpoint = endpoint.tls_config(
                    ClientTlsConfig::new()
                        .with_enabled_roots()
                        .ca_certificate(Certificate::from_pem(certificate_pem)),
                )?;
            }
            configured.push(endpoint);
        }
        if configured.is_empty() {
            return Err(ConnectError::NoEndpoints);
        }
        Ok(configured)
    }

    fn from_channels(
        channels: Arc<[Channel]>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        let authorization = Self::authorization(bearer_token.as_ref())?;
        Ok(Self::from_authorization(channels, authorization))
    }

    fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
        if bearer_token.trim().is_empty()
            || bearer_token.len() > MAX_BEARER_TOKEN_BYTES
            || bearer_token
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n'))
        {
            return Err(ConnectError::InvalidCredential);
        }
        let mut authorization = format!("Bearer {bearer_token}")
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ConnectError::InvalidCredential)?;
        authorization.set_sensitive(true);
        Ok(authorization)
    }

    fn from_authorization(channels: Arc<[Channel]>, authorization: MetadataValue<Ascii>) -> Self {
        Self {
            channels,
            authorization,
            preferred: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "callers derive index via `(start + offset) % channels.len()` against this same slice, so it is always in-bounds"
    )]
    fn service(
        channels: &[Channel],
        index: usize,
    ) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(channels[index].clone())
    }

    fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }

    async fn unary<T, U, F>(&self, body: T, mut call: F) -> Result<RpcResponse<U>, StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        self.unary_on(
            &self.channels,
            &self.preferred,
            body,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
            &mut call,
        )
        .await
        .map(|(body, _, completion)| RpcResponse { body, completion })
    }

    async fn follow_unary<T, U, F>(
        &self,
        body: T,
        mut call: F,
    ) -> Result<(U, usize, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        self.unary_on(
            &self.channels,
            &self.preferred,
            body,
            FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT,
            &mut call,
        )
        .await
    }

    async fn unary_on<T, U, F>(
        &self,
        channels: &[Channel],
        preferred: &AtomicUsize,
        body: T,
        attempt_timeout: std::time::Duration,
        call: &mut F,
    ) -> Result<(U, usize, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut last = None;
        let mut attempt = 0_u64;
        loop {
            let start = preferred.load(Ordering::Relaxed) % channels.len();
            for offset in 0..channels.len() {
                let index = (start + offset) % channels.len();
                let attempt_deadline = deadline.min(tokio::time::Instant::now() + attempt_timeout);
                attempt += 1;
                // One span describes one physical RPC attempt; the ordinal is not a logical retry total.
                let span = tracing::info_span!(
                    "acyclic.stream.grpc.call",
                    rpc = std::any::type_name::<T>().rsplit("::").next(),
                    attempt,
                    rpc.code = Empty,
                    outcome = Empty,
                    error.kind = Empty
                );
                let mut completion = Completion::new(span);
                let result = completion
                    .span
                    .scope(tokio::time::timeout_at(
                        attempt_deadline,
                        call(Self::service(channels, index), self.request(body.clone())),
                    ))
                    .await;
                let error = match result {
                    Ok(Ok(response)) => {
                        preferred.store(index, Ordering::Relaxed);
                        return Ok((response.into_inner(), index, completion));
                    }
                    Ok(Err(error)) => error,
                    Err(_) => Status::deadline_exceeded("endpoint attempt expired"),
                };
                completion.status(&error);
                if !retryable(&error) {
                    return Err(status(&error));
                }
                completion.span.in_scope(|| {
                    tracing::warn!(
                        attempt,
                        rpc.code = error.code() as u64,
                        "stream rpc retried"
                    );
                });
                last = Some(error);
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(last.as_ref().map_or(StreamError::Unavailable, status));
            }
        }
    }

    async fn records(
        &self,
        path: StreamPath,
        from: u64,
        limit: Option<u32>,
    ) -> Result<RecordStream, StreamError> {
        let span = if limit.is_some() {
            tracing::info_span!(
                "acyclic.stream.grpc.read",
                rev = from,
                items = limit,
                outcome = Empty,
                error.kind = Empty
            )
        } else {
            tracing::info_span!(
                "acyclic.stream.grpc.follow",
                rev = from,
                outcome = Empty,
                error.kind = Empty
            )
        };
        let completion = Completion::new(span);
        let context = completion.span.clone();
        Ok(context
            .scope(stream::unfold(
                RecordCursor {
                    client: self.clone(),
                    path,
                    next: from,
                    remaining: limit,
                    active: None,
                    buffered: VecDeque::new(),
                    completion,
                },
                |mut cursor| async move {
                    loop {
                        if cursor.remaining == Some(0) {
                            cursor.completion.ok();
                            return None;
                        }
                        if let Some(record) = cursor.buffered.pop_front() {
                            if record.sequence == cursor.next {
                                cursor.next = cursor.next.saturating_add(1);
                                if let Some(remaining) = &mut cursor.remaining {
                                    *remaining = remaining.saturating_sub(1);
                                    if *remaining == 0 {
                                        cursor.completion.ok();
                                    }
                                }
                                return Some((Ok(record), cursor));
                            }
                            if record.sequence < cursor.next {
                                continue;
                            }
                            if let Some(active) = cursor.active.as_mut() {
                                active.completion.invalid_response();
                            }
                            cursor.completion.invalid_response();
                            return cursor.failure(StreamError::Unavailable);
                        }
                        if cursor.active.is_none() {
                            match cursor.completion.span.scope(cursor.open()).await {
                                Ok(active) => cursor.active = Some(active),
                                Err(error) => return cursor.failure(error),
                            }
                        }
                        let Some(active) = cursor.active.as_mut() else {
                            return cursor.failure(StreamError::Unavailable);
                        };
                        let active_endpoint = active.endpoint;
                        let response = active.records.next().await;
                        match &response {
                            Some(Err(error)) => active.completion.status(error),
                            None => active.completion.ok(),
                            _ => {}
                        }
                        match response {
                            Some(Ok(response)) => match read_response(response) {
                                Ok(records) => cursor.buffered = records,
                                Err(error) => {
                                    active.completion.invalid_response();
                                    cursor.completion.invalid_response();
                                    return cursor.failure(error);
                                }
                            },
                            Some(Err(error)) if !retryable(&error) => {
                                return cursor.failure(status(&error));
                            }
                            None if cursor.remaining.is_some() => {
                                cursor.completion.ok();
                                return None;
                            }
                            Some(Err(_)) | None => {
                                cursor.advance_follow(active_endpoint);
                                tokio::time::sleep(RETRY_DELAY).await;
                                cursor.active = None;
                            }
                        }
                    }
                },
            ))
            .boxed())
    }
}

fn bounded_ca_certificate(value: &[u8]) -> Result<&[u8], ConnectError> {
    if value.is_empty() || value.len() > MAX_CA_CERTIFICATE_BYTES {
        Err(ConnectError::InvalidCaCertificate)
    } else {
        Ok(value)
    }
}

struct RecordCursor {
    client: Client,
    path: StreamPath,
    next: u64,
    remaining: Option<u32>,
    active: Option<ActiveRecords>,
    /// Decoded records from the latest frame that the caller has not yet taken.
    buffered: VecDeque<Record>,
    completion: Completion,
}

struct ActiveRecords {
    records: obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
    endpoint: usize,
    completion: Completion,
}

impl RecordCursor {
    /// Delivered errors terminate the logical cursor. Retryable transport failures
    /// reconnect internally before reaching this boundary.
    fn failure(mut self, error: StreamError) -> Option<(Result<Record, StreamError>, Self)> {
        self.completion.finish(Some(error.code()), None);
        self.active = None;
        self.buffered.clear();
        self.remaining = Some(0);
        Some((Err(error), self))
    }

    fn advance_follow(&self, observed: usize) {
        if self.remaining.is_some() {
            return;
        }
        let next = (observed + 1) % self.client.channels.len();
        let _ = self.client.preferred.compare_exchange(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    async fn open(&self) -> Result<ActiveRecords, StreamError> {
        if let Some(limit) = self.remaining {
            self.client
                .unary(
                    wire::ReadRequest {
                        path: self.path.to_string(),
                        from: self.next,
                        limit,
                    },
                    |mut service, request| Box::pin(async move { service.read(request).await }),
                )
                .await
                .map(|response| {
                    let RpcResponse { body, completion } = response;
                    let records = completion.span.scope(body);
                    ActiveRecords {
                        records,
                        completion,
                        endpoint: 0,
                    }
                })
        } else {
            self.client
                .follow_unary(
                    wire::FollowRequest {
                        path: self.path.to_string(),
                        from: self.next,
                    },
                    |mut service, request| Box::pin(async move { service.follow(request).await }),
                )
                .await
                .map(|(records, endpoint, completion)| {
                    let records = completion.span.scope(records);
                    ActiveRecords {
                        records,
                        endpoint,
                        completion,
                    }
                })
        }
    }
}

fn retryable(error: &Status) -> bool {
    if matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded) {
        return true;
    }
    // Missing final trailers become Unknown without a retained transport cause.
    // Treat that outcome as ambiguous, never successful: every mutation retains
    // its idempotency key and every resumed read retains its cursor/deadline.
    // An explicit peer Unknown is indistinguishable and gets the same bounded retry.
    if error.code() == Code::Unknown && std::error::Error::source(error).is_none() {
        return true;
    }
    if !matches!(error.code(), Code::Unknown | Code::Cancelled) {
        return false;
    }
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        // Hyper cancels queued dispatch when its connection driver disappears. This is
        // transport uncertainty, unlike a peer's application-level Cancelled status.
        if error.code() == Code::Cancelled
            && current
                .downcast_ref::<hyper::Error>()
                .is_some_and(hyper::Error::is_canceled)
        {
            return true;
        }
        if error.code() == Code::Cancelled
            && current.downcast_ref::<h2::Error>().is_some_and(|error| {
                error.is_remote() && error.reason() == Some(h2::Reason::CANCEL)
            })
        {
            return true;
        }
        let io = current.downcast_ref::<std::io::Error>().or_else(|| {
            current
                .downcast_ref::<h2::Error>()
                .and_then(h2::Error::get_io)
        });
        if error.code() == Code::Unknown
            && io.is_some_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::TimedOut
                )
            })
        {
            return true;
        }
        cause = current.source();
    }
    false
}

#[async_trait]
impl StreamProvider for Client {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let response = self
            .unary(
                wire::InspectIdempotencyRequest {
                    idempotency_key: Bytes::copy_from_slice(idempotency_key.as_bytes()),
                },
                |mut service, request| {
                    Box::pin(async move { service.inspect_idempotency(request).await })
                },
            )
            .await?;
        response.decode(|response| bind_observation(&idempotency_key, response.observation))
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.unary(
            wire::TailRequest {
                path: path.to_string(),
            },
            |mut service, request| Box::pin(async move { service.tail(request).await }),
        )
        .await?
        .decode(|response| Ok(response.tail))
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        let response = self
            .unary(
                wire::TailRequest {
                    path: path.to_string(),
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        response.decode(|response| {
            Ok(StreamBounds {
                tail: response.tail,
            })
        })
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let response = self
            .unary(
                wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request.records,
                    if_tail: request.if_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.append(request).await }),
            )
            .await?;
        response.decode(append_outcome_from_wire)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let receipt = self
            .unary(
                wire::ForkRequest {
                    source: request.source.to_string(),
                    destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.fork(request).await }),
            )
            .await?;
        receipt.decode(|receipt| {
            Ok(ForkReceipt {
                source: path(receipt.source)?,
                destination: path(receipt.destination)?,
                forked_at: receipt.forked_at,
                tail: receipt.tail,
                commit_id: commit_id(&receipt.commit_id)?,
            })
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.records(request.path, request.from, Some(request.limit))
            .await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.records(path, from, None).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let body = wire::ChildrenRequest {
            parent: request.parent.map(|path| path.to_string()),
            limit: request.limit,
        };
        let mut last = None;
        for _ in 0..self.channels.len() {
            let response = self
                .unary(body.clone(), |mut service, request| {
                    Box::pin(async move { service.children(request).await })
                })
                .await?;
            let RpcResponse {
                body: response,
                mut completion,
            } = response;
            let mut response = completion.span.scope(response);
            let result = async {
                let mut children = Vec::new();
                while let Some(item) = response.next().await {
                    let item = item.map_err(|error| {
                        completion.status(&error);
                        status(&error)
                    })?;
                    let child = item
                        .child
                        .ok_or(StreamError::Unavailable)
                        .and_then(|child| {
                            Ok(Child {
                                path: path(child.path)?,
                            })
                        })
                        .inspect_err(|_| completion.invalid_response())?;
                    children.push(child);
                }
                completion.ok();
                Ok(children)
            }
            .await;
            match result {
                Ok(children) => return Ok(stream::iter(children.into_iter().map(Ok)).boxed()),
                Err(error) if error != StreamError::Unavailable => return Err(error),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or(StreamError::Unavailable))
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let response = self
            .unary(
                wire::ChildrenPageRequest {
                    parent: request.parent.map(|path| path.to_string()),
                    after: request.after.map(|path| path.to_string()),
                    hierarchy_version: request
                        .hierarchy_version
                        .map(|version| Bytes::copy_from_slice(version.as_bytes())),
                    limit: request.limit,
                },
                |mut service, request| {
                    Box::pin(async move { service.children_page(request).await })
                },
            )
            .await?;
        response.decode(|response| {
            Ok(ChildrenPage {
                hierarchy_version: commit_id(&response.hierarchy_version)?,
                children: response
                    .children
                    .into_iter()
                    .map(|child| {
                        Ok(Child {
                            path: path(child.path)?,
                        })
                    })
                    .collect::<Result<_, StreamError>>()?,
                next_after: response.next_after.map(path).transpose()?,
            })
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
                    deadline_unix_millis: None,
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        response.decode(commit_outcome_from_wire)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
                    deadline_unix_millis: Some(deadline_unix_millis),
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        response.decode(commit_outcome_from_wire)
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let envelope = self
            .unary(
                wire::ReadCommitRequest {
                    commit_id: Bytes::copy_from_slice(commit_id.as_bytes()),
                },
                |mut service, request| Box::pin(async move { service.read_commit(request).await }),
            )
            .await?;
        envelope.decode(envelope_from_wire)
    }
}

#[async_trait]
impl<P: StreamProvider> wire::stream_service_server::StreamService for Service<P> {
    type ReadStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type FollowStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type ChildrenStream =
        futures::stream::BoxStream<'static, Result<wire::ChildrenResponse, Status>>;

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.inspect_idempotency",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let key = IdempotencyKey::new(request.into_inner().idempotency_key)
                    .map_err(|error| error_status(&error))?;
                let observation = self
                    .provider
                    .inspect_idempotency(key)
                    .await
                    .map_err(|error| error_status(&error))?
                    .map(observation_wire);
                Ok(Response::new(wire::InspectIdempotencyResponse {
                    observation,
                }))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.append",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let outcome = self
                    .provider
                    .append(AppendRequest {
                        path: path(request.path).map_err(|error| error_status(&error))?,
                        records: request.records,
                        if_tail: request.if_tail,
                        idempotency_key: optional_key(request.idempotency_key)
                            .map_err(|error| error_status(&error))?,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(append_outcome_wire(outcome)))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.tail",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let path = path(request.into_inner().path).map_err(|error| error_status(&error))?;
                let bounds = self
                    .provider
                    .bounds(path)
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(wire::TailResponse { tail: bounds.tail }))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.fork",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let receipt = self
                    .provider
                    .fork(ForkRequest {
                        source: path(request.source).map_err(|error| error_status(&error))?,
                        destination: path(request.destination)
                            .map_err(|error| error_status(&error))?,
                        at_tail: request.at_tail,
                        idempotency_key: optional_key(request.idempotency_key)
                            .map_err(|error| error_status(&error))?,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(fork_receipt_wire(&receipt)))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.read",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let records = self
                    .provider
                    .read(ReadRequest {
                        path: path(request.path).map_err(|error| error_status(&error))?,
                        from: request.from,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        records.map(|record| {
                            record
                                .map(record_wire)
                                .map(|record| read_response_wire(vec![record]))
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            })
            .await;
        finish_served(&mut completion, result, true)
    }

    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.follow",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let records = self
                    .provider
                    .follow(
                        path(request.path).map_err(|error| error_status(&error))?,
                        request.from,
                    )
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        records.map(|record| {
                            record
                                .map(record_wire)
                                .map(|record| read_response_wire(vec![record]))
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            })
            .await;
        finish_served(&mut completion, result, true)
    }

    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.children",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let children = self
                    .provider
                    .children(ChildrenRequest {
                        parent: request
                            .parent
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        children.map(|child| {
                            child
                                .map(|child| wire::ChildrenResponse {
                                    child: Some(wire::Child {
                                        path: child.path.to_string(),
                                    }),
                                })
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            })
            .await;
        finish_served(&mut completion, result, true)
    }

    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.children_page",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                let request = request.into_inner();
                let page = self
                    .provider
                    .children_page(ChildrenPageRequest {
                        parent: request
                            .parent
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        after: request
                            .after
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        hierarchy_version: request
                            .hierarchy_version
                            .as_deref()
                            .map(commit_id)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(wire::ChildrenPageResponse {
                    hierarchy_version: Bytes::copy_from_slice(page.hierarchy_version.as_bytes()),
                    children: page
                        .children
                        .into_iter()
                        .map(|child| wire::Child {
                            path: child.path.to_string(),
                        })
                        .collect(),
                    next_after: page.next_after.map(|path| path.to_string()),
                }))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.commit",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let deadline_unix_millis = request.deadline_unix_millis;
                let request = CommitRequest {
                    conditions: request
                        .conditions
                        .into_iter()
                        .map(condition_from_wire)
                        .collect::<Result<_, _>>()
                        .map_err(|error| error_status(&error))?,
                    mutations: request
                        .mutations
                        .into_iter()
                        .map(mutation_from_wire)
                        .collect::<Result<_, _>>()
                        .map_err(|error| error_status(&error))?,
                    idempotency_key: IdempotencyKey::new(request.idempotency_key)
                        .map_err(|error| error_status(&error))?,
                };
                let outcome = if let Some(deadline) = deadline_unix_millis {
                    self.provider.commit_before(request, deadline).await
                } else {
                    self.provider.commit(request).await
                }
                .map_err(|error| error_status(&error))?;
                Ok(Response::new(commit_outcome_wire(outcome)))
            })
            .await;
        finish_served(&mut completion, result, false)
    }

    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        let mut completion = Completion::new(tracing::info_span!(
            "acyclic.stream.grpc.serve.read_commit",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        let span = completion.span.clone();
        let result = span
            .scope(async {
                check_command_size(request.get_ref())?;
                let commit_id = commit_id(&request.into_inner().commit_id)
                    .map_err(|error| error_status(&error))?;
                let envelope = self
                    .provider
                    .read_commit(commit_id)
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(envelope_wire(envelope)))
            })
            .await;
        finish_served(&mut completion, result, false)
    }
}

fn bind_observation(
    requested: &IdempotencyKey,
    observation: Option<wire::IdempotencyObservation>,
) -> Result<Option<IdempotencyObservation>, StreamError> {
    let observation = observation.map(observation_from_wire).transpose()?;
    if observation
        .as_ref()
        .is_some_and(|observation| &observation.idempotency_key != requested)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(observation)
}

fn finish_served<T>(
    completion: &mut Completion,
    result: Result<T, Status>,
    streaming: bool,
) -> Result<T, Status> {
    match &result {
        Err(error) => completion.status(error),
        // The returned body guard now owns this same span. Headers are not terminal success.
        Ok(_) if streaming => completion.finished = true,
        Ok(_) => completion.ok(),
    }
    result
}

/// A single owned operation, retaining its creating dispatch even if its span is filtered.
/// Only the first terminal observation wins; dropping pending work is cancellation.
struct Completion {
    span: obs::OwnedSpan,
    finished: bool,
}

impl Completion {
    fn new(span: tracing::Span) -> Self {
        Self {
            span: obs::OwnedSpan::new(span),
            finished: false,
        }
    }
    fn finish(&mut self, kind: Option<&'static str>, code: Option<Code>) {
        if self.finished {
            return;
        }
        self.finished = true;
        match kind {
            Some(kind) => obs::failed(&self.span, kind),
            None => obs::record(&self.span, "outcome", "ok"),
        }
        if let Some(code) = code {
            obs::record(&self.span, "rpc.code", code as u64);
        }
    }
    fn ok(&mut self) {
        self.finish(None, Some(Code::Ok));
    }
    fn status(&mut self, error: &Status) {
        self.finish(
            Some(if error.code() == Code::Cancelled {
                "cancelled"
            } else {
                status(error).code()
            }),
            Some(error.code()),
        );
    }
    fn invalid_response(&mut self) {
        // This is local semantic rejection, not an observed gRPC trailer status.
        self.finish(Some("invalid_response"), None);
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Some("cancelled"), Some(Code::Cancelled));
    }
}

/// Keeps successful response headers pending until the unary response is decoded.
struct RpcResponse<T> {
    body: T,
    completion: Completion,
}
impl<T> RpcResponse<T> {
    fn decode<U>(
        mut self,
        decode: impl FnOnce(T) -> Result<U, StreamError>,
    ) -> Result<U, StreamError> {
        // Tonic unary completion already observed the successful trailers.
        obs::record(&self.completion.span, "rpc.code", Code::Ok as u64);
        let result = decode(self.body);
        match &result {
            Ok(_) => self.completion.ok(),
            Err(_) => self.completion.invalid_response(),
        }
        result
    }
}

/// Completes a streaming RPC on exhaustion, first error, or unfinished drop.
/// Response headers alone do not acknowledge delivery of the body.
struct ObservedBody<S> {
    body: obs::Scoped<S>,
    completion: Completion,
}
fn observe_body<S>(span: &tracing::Span, body: S) -> ObservedBody<S> {
    let completion = Completion::new(span.clone());
    let body = completion.span.scope(body);
    ObservedBody { body, completion }
}
impl<S, T> Stream for ObservedBody<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.completion.finished {
            return Poll::Ready(None);
        }
        let next = Pin::new(&mut this.body).poll_next(cx);
        match &next {
            Poll::Ready(Some(Err(error))) => this.completion.status(error),
            Poll::Ready(None) => this.completion.ok(),
            _ => {}
        }
        next
    }
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

fn status(error: &tonic::Status) -> StreamError {
    match error.code() {
        Code::InvalidArgument if error.message() == "invalid_path" => StreamError::InvalidPath,
        Code::InvalidArgument if error.message() == "limit_exceeded" => StreamError::LimitExceeded,
        Code::InvalidArgument => StreamError::InvalidArgument,
        Code::NotFound => StreamError::NotFound,
        Code::AlreadyExists => StreamError::AlreadyExists,
        Code::OutOfRange => StreamError::OutOfRange,
        Code::FailedPrecondition if error.message() == "hierarchy_changed" => {
            StreamError::HierarchyChanged
        }
        Code::PermissionDenied | Code::Unauthenticated => StreamError::AccessDenied,
        Code::ResourceExhausted => StreamError::Capacity,
        Code::FailedPrecondition if error.message() == "idempotency_mismatch" => {
            StreamError::IdempotencyMismatch
        }
        Code::FailedPrecondition if error.message() == "prefix_not_retained" => {
            StreamError::PrefixNotRetained
        }
        Code::FailedPrecondition if error.message() == "deadline_elapsed" => {
            StreamError::DeadlineElapsed
        }
        Code::Unimplemented if error.message() == "unsupported_capability" => {
            StreamError::Unsupported
        }
        _ => StreamError::Unavailable,
    }
}

fn read_response(value: wire::ReadResponse) -> Result<VecDeque<Record>, StreamError> {
    read_response_records(value)?
        .into_iter()
        .map(record)
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "fixture indexes and captured field lookups intentionally fail the test if expected evidence is absent"
)]
mod tests {
    use super::*;
    use crate::MemoryStream;
    use rcgen::generate_simple_self_signed;
    use tokio::{io::AsyncReadExt, net::TcpListener};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};
    use wire::stream_service_server::{StreamService, StreamServiceServer};

    #[tokio::test]
    async fn served_body_errors_remain_on_owned_span_when_polled_by_another_caller()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        for filtered in [false, true] {
            let captured = obs::tests::Capture::default();
            let subscriber = tracing_subscriber::Registry::default()
                .with(captured.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || !metadata.name().starts_with("acyclic.stream")
                }));
            let _default = tracing::subscriber::set_default(subscriber);
            let provider = Arc::new(FiniteFollow {
                inner: MemoryStream::default(),
                follows: AtomicUsize::new(0),
                tail_delay: std::time::Duration::ZERO,
                denial: Some(1),
            });
            provider
                .inner
                .append(AppendRequest {
                    path: StreamPath::new("events")?,
                    records: vec![Bytes::from_static(b"one")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
            let service = Service::new(provider);
            let caller = tracing::info_span!(
                "opening_caller",
                outcome = "caller",
                error.kind = "caller",
                rpc.code = 99
            );
            let mut body = async {
                let invalid = StreamService::tail(
                    &service,
                    Request::new(wire::TailRequest {
                        path: String::new(),
                    }),
                )
                .await;
                assert_eq!(invalid.unwrap_err().code(), Code::InvalidArgument);
                Ok::<_, Status>(
                    StreamService::follow(
                        &service,
                        Request::new(wire::FollowRequest {
                            path: "events".to_owned(),
                            from: 0,
                        }),
                    )
                    .await?
                    .into_inner(),
                )
            }
            .instrument(caller)
            .await?;
            let polling_caller = tracing::info_span!(
                "polling_caller",
                outcome = "caller",
                error.kind = "caller",
                rpc.code = 99
            );
            async {
                assert!(body.next().await.unwrap().is_ok());
                assert_eq!(
                    body.next().await.unwrap().unwrap_err().code(),
                    Code::PermissionDenied
                );
                drop(body);
            }
            .instrument(polling_caller)
            .await;
            for name in ["opening_caller", "polling_caller"] {
                let fields = captured.fields(name);
                assert_eq!(fields["outcome"], "caller");
                assert_eq!(fields["error.kind"], "caller");
                assert_eq!(fields["rpc.code"], "99");
            }
            if !filtered {
                let fields = captured.fields("acyclic.stream.grpc.serve.follow");
                assert_eq!(fields["outcome"], "err");
                assert_eq!(fields["error.kind"], "access_denied");
                assert_eq!(
                    fields["rpc.code"],
                    (Code::PermissionDenied as u64).to_string()
                );
                assert_eq!(
                    captured.fields("acyclic.stream.grpc.serve.tail")["error.kind"],
                    "invalid_path"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn observed_body_completes_only_at_terminal_delivery() {
        use tracing_subscriber::layer::SubscriberExt as _;
        for (filtered, scenario) in [false, true].into_iter().flat_map(|filtered| {
            ["unpolled", "partial", "pending", "exhausted", "error"]
                .into_iter()
                .map(move |scenario| (filtered, scenario))
        }) {
            let captured = obs::tests::Capture::default();
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default()
                    .with(captured.clone())
                    .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                        !filtered || metadata.name() != "body"
                    })),
            );
            let span = tracing::info_span!(
                "body",
                outcome = Empty,
                error.kind = Empty,
                rpc.code = Empty
            );
            let source: futures::stream::BoxStream<'static, Result<u8, Status>> = match scenario {
                "pending" => stream::pending().boxed(),
                "error" => stream::iter([Err(Status::permission_denied("denied"))]).boxed(),
                _ => stream::iter([Ok(1), Ok(2)]).boxed(),
            };
            let mut body = observe_body(&span, source);
            assert!(!captured.fields("body").contains_key("outcome"));
            let caller = tracing::info_span!(
                "polling",
                outcome = "caller",
                error.kind = "caller",
                rpc.code = 99
            );
            async {
                match scenario {
                    "unpolled" => {}
                    "partial" => {
                        assert_eq!(body.next().await.unwrap().unwrap(), 1);
                    }
                    "pending" => {
                        assert!(futures::poll!(body.next()).is_pending());
                    }
                    "exhausted" => {
                        while let Some(item) = body.next().await {
                            assert!(item.is_ok());
                        }
                    }
                    "error" => {
                        assert!(body.next().await.unwrap().is_err());
                        assert!(body.next().await.is_none());
                    }
                    _ => unreachable!(),
                }
                drop(body);
            }
            .instrument(caller)
            .await;
            let fields = captured.fields("body");
            if !filtered {
                match scenario {
                    "exhausted" => {
                        assert_eq!(fields["outcome"], "ok");
                        assert_eq!(fields["rpc.code"], "0");
                    }
                    "error" => {
                        assert_eq!(fields["outcome"], "err");
                        assert_eq!(fields["error.kind"], "access_denied");
                        assert_eq!(fields["rpc.code"], "7");
                    }
                    _ => {
                        assert_eq!(fields["outcome"], "err");
                        assert_eq!(fields["error.kind"], "cancelled");
                        assert_eq!(fields["rpc.code"], "1");
                    }
                }
            }
            assert_eq!(captured.fields("polling")["outcome"], "caller");
            assert_eq!(captured.fields("polling")["rpc.code"], "99");
        }
    }

    #[tokio::test]
    async fn observed_body_fuses_after_error_or_eof() {
        for error in [false, true] {
            let polls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&polls);
            let source = stream::poll_fn(move |_| {
                let polled = count.fetch_add(1, Ordering::SeqCst);
                if polled == 0 {
                    if error {
                        Poll::Ready(Some(Err::<u8, _>(Status::permission_denied("denied"))))
                    } else {
                        Poll::Ready(None)
                    }
                } else {
                    Poll::Ready(Some(Ok(99)))
                }
            });
            let mut body = observe_body(&tracing::Span::none(), source);
            if error {
                assert!(body.next().await.unwrap().is_err());
            } else {
                assert!(body.next().await.is_none());
            }
            assert!(body.next().await.is_none());
            assert!(body.next().await.is_none());
            assert_eq!(
                polls.load(Ordering::SeqCst),
                1,
                "non-fused source must never be repolled after closure"
            );
        }
    }

    #[tokio::test]
    async fn observed_body_reinstalls_owned_dispatch_even_when_span_is_filtered() {
        use tracing_subscriber::layer::SubscriberExt as _;
        for filtered in [false, true] {
            let original = obs::tests::Capture::default();
            let subscriber = tracing_subscriber::Registry::default()
                .with(original.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || metadata.name() != "body"
                }));
            let (span, dispatch) = tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!(
                    "body",
                    outcome = Empty,
                    error.kind = Empty,
                    rpc.code = Empty
                );
                (span, tracing::dispatcher::get_default(Clone::clone))
            });
            let other = obs::tests::Capture::default();
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(other.clone()),
            );
            let source = stream::poll_fn(|_| {
                let _child = tracing::info_span!("body_child");
                Poll::Ready(Some(Ok::<_, Status>(1_u8)))
            });
            // Enabled spans supply their original dispatch even if the guard is
            // constructed under another subscriber. Disabled spans retain the
            // dispatch at operation creation because they carry no subscriber.
            let mut body = if filtered {
                tracing::dispatcher::with_default(&dispatch, || observe_body(&span, source))
            } else {
                observe_body(&span, source)
            };
            let caller = tracing::info_span!("caller", outcome = "caller");
            async {
                assert_eq!(body.next().await.unwrap().unwrap(), 1);
                drop(body);
            }
            .instrument(caller)
            .await;
            assert_eq!(original.all("body_child").len(), 1);
            assert!(other.all("body_child").is_empty());
            assert_eq!(other.fields("caller")["outcome"], "caller");
            if !filtered {
                assert_eq!(original.fields("body")["error.kind"], "cancelled");
            }
        }
    }

    #[tokio::test]
    async fn observed_body_terminal_and_drop_close_real_parent_under_origin() {
        use tracing_subscriber::layer::SubscriberExt as _;
        for scenario in ["pending", "error", "eof"] {
            let captured = obs::tests::Capture::default();
            let mut body = tracing::subscriber::with_default(
                tracing_subscriber::Registry::default().with(captured.clone()),
                || {
                    let parent = tracing::info_span!("origin_parent");
                    let span = tracing::info_span!(parent: &parent, "body", outcome = Empty, error.kind = Empty, rpc.code = Empty);
                    let nested = span.in_scope(|| tracing::info_span!("nested_body"));
                    let source = stream::poll_fn(move |_| {
                        let _retained = &nested;
                        match scenario {
                            "error" => {
                                Poll::Ready(Some(Err::<u8, _>(Status::permission_denied("denied"))))
                            }
                            "eof" => Poll::Ready(None),
                            _ => Poll::Pending,
                        }
                    });
                    observe_body(&span, source)
                },
            );
            let other = obs::tests::Capture::default();
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(other.clone()),
            );
            let parent = tracing::info_span!("foreign_parent");
            let _entered = parent.enter();
            match scenario {
                "pending" => {
                    assert!(futures::poll!(body.next()).is_pending());
                }
                "error" => {
                    assert!(body.next().await.unwrap().is_err());
                }
                "eof" => {
                    assert!(body.next().await.is_none());
                }
                _ => unreachable!(),
            }
            assert_eq!(captured.closed("origin_parent"), 0);
            drop(body);
            for name in ["nested_body", "body", "origin_parent"] {
                assert_eq!(captured.closed(name), 1, "{name}");
            }
            assert_eq!(other.closed("foreign_parent"), 0);
            let fields = captured.fields("body");
            assert_eq!(
                fields["outcome"],
                if scenario == "eof" { "ok" } else { "err" }
            );
            if scenario == "pending" {
                assert_eq!(fields["error.kind"], "cancelled");
            }
        }
    }

    #[tokio::test]
    async fn cancelled_server_handler_records_owned_rpc_and_closes_origin_parent() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let service = Service::new(Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::from_secs(60),
            denial: None,
        }));
        let captured = obs::tests::Capture::default();
        let future = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
                parent.scope(StreamService::tail(
                    &service,
                    Request::new(wire::TailRequest {
                        path: "pending".to_owned(),
                    }),
                ))
            },
        );
        let mut future = Box::pin(future);
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert!(
            !captured
                .fields("acyclic.stream.grpc.serve.tail")
                .contains_key("outcome")
        );
        let other = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        drop(future);
        let fields = captured.fields("acyclic.stream.grpc.serve.tail");
        assert_eq!(fields["outcome"], "err");
        assert_eq!(fields["error.kind"], "cancelled");
        assert_eq!(fields["rpc.code"], "1");
        assert_eq!(captured.closed("acyclic.stream.grpc.serve.tail"), 1);
        assert_eq!(captured.closed("origin_parent"), 1);
        assert_eq!(other.closed("foreign_parent"), 0);
    }

    struct FiniteFollow {
        inner: MemoryStream,
        follows: AtomicUsize,
        tail_delay: std::time::Duration,
        /// `Some(n)`: Follow yields the first `n` records, then a denied credential;
        /// `Some(0)` denies the Follow when it opens.
        denial: Option<usize>,
    }

    #[async_trait]
    impl StreamProvider for FiniteFollow {
        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }

        async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
            tokio::time::sleep(self.tail_delay).await;
            self.inner.tail(path).await
        }

        async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
            tokio::time::sleep(self.tail_delay).await;
            self.inner.bounds(path).await
        }

        async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
            self.inner.append(request).await
        }

        async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
            self.inner.fork(request).await
        }

        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            self.inner.read(request).await
        }

        async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
            self.follows.fetch_add(1, Ordering::Relaxed);
            match self.denial {
                None => Ok(stream::empty().boxed()),
                Some(0) => Err(StreamError::AccessDenied),
                Some(count) => {
                    let limit = u32::try_from(count).map_err(|_| StreamError::InvalidArgument)?;
                    let records = self.inner.read(ReadRequest { path, from, limit }).await?;
                    Ok(records
                        .chain(stream::once(async { Err(StreamError::AccessDenied) }))
                        .boxed())
                }
            }
        }

        async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
            self.inner.children(request).await
        }

        async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
            self.inner.commit(request).await
        }

        async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
            self.inner.read_commit(commit_id).await
        }
    }

    fn in_memory_channel(provider: Arc<MemoryStream>) -> Channel {
        provider_channel(Service::new(provider))
    }

    fn provider_channel<S: StreamService + Clone>(service: S) -> Channel {
        Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
            tower::service_fn(move |_| {
                let service = service.clone();
                async move {
                    let (client, server) = tokio::io::duplex(64 * 1024);
                    tokio::spawn(async move {
                        let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                        let _ = tonic::transport::Server::builder()
                            .add_service(StreamServiceServer::new(service))
                            .serve_with_incoming(incoming)
                            .await;
                    });
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
                }
            }),
        )
    }

    #[tokio::test]
    async fn grpc_transport_passes_the_public_suite()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let channel = in_memory_channel(Arc::new(MemoryStream::default()));
        let transport = Client::from_channels(Arc::from([channel.clone()]), "fixture")?;
        crate::conformance::verify(&transport).await?;
        let mut obsolete = tonic::client::Grpc::new(channel);
        obsolete.ready().await?;
        let result: Result<Response<wire::TailResponse>, Status> = obsolete
            .unary(
                Request::new(wire::TailRequest {
                    path: "events".into(),
                }),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/acyclic.stream.v2.StreamService/Tail",
                ),
                tonic_prost::ProstCodec::default(),
            )
            .await;
        assert_eq!(
            result.err().map(|error| error.code()),
            Some(tonic::Code::Unimplemented)
        );
        Ok(())
    }

    /// Regroups every Read frame into batches of three records that alternate
    /// between Zstandard and uncompressed frames.
    #[derive(Clone)]
    struct MixedFrames(Service<MemoryStream>, Option<&'static str>);

    #[async_trait]
    impl StreamService for MixedFrames {
        type ReadStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
        type FollowStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
        type ChildrenStream =
            futures::stream::BoxStream<'static, Result<wire::ChildrenResponse, Status>>;

        async fn inspect_idempotency(
            &self,
            request: Request<wire::InspectIdempotencyRequest>,
        ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
            self.0.inspect_idempotency(request).await
        }
        async fn append(
            &self,
            request: Request<wire::AppendRequest>,
        ) -> Result<Response<wire::AppendResponse>, Status> {
            self.0.append(request).await
        }
        async fn tail(
            &self,
            request: Request<wire::TailRequest>,
        ) -> Result<Response<wire::TailResponse>, Status> {
            self.0.tail(request).await
        }
        async fn fork(
            &self,
            request: Request<wire::ForkRequest>,
        ) -> Result<Response<wire::ForkReceipt>, Status> {
            self.0.fork(request).await
        }
        async fn read(
            &self,
            request: Request<wire::ReadRequest>,
        ) -> Result<Response<Self::ReadStream>, Status> {
            if let Some(fault) = self.1 {
                let valid = read_response_wire(vec![wire::Record {
                    commit_id: Bytes::from(vec![0; 32]),
                    ..Default::default()
                }]);
                let frames: Self::ReadStream = match fault {
                    "malformed" => {
                        stream::iter([Ok(read_response_wire(vec![wire::Record::default()]))])
                            .boxed()
                    }
                    "error" => {
                        stream::iter([Ok(valid), Err(Status::permission_denied("denied"))]).boxed()
                    }
                    "pending" => stream::once(async move { Ok(valid) })
                        .chain(stream::pending())
                        .boxed(),
                    _ => stream::iter([Ok(valid)]).boxed(),
                };
                return Ok(Response::new(frames));
            }
            let frames = self
                .0
                .read(request)
                .await?
                .into_inner()
                .collect::<Vec<_>>()
                .await;
            let mut records = Vec::new();
            for frame in frames {
                records
                    .extend(read_response_records(frame?).map_err(|error| error_status(&error))?);
            }
            let regrouped = records
                .chunks(3)
                .enumerate()
                .map(|(index, chunk)| {
                    Ok(if index % 2 == 0 {
                        crate::wire_codec::zstd_read_response(chunk.to_vec())
                    } else {
                        read_response_wire(chunk.to_vec())
                    })
                })
                .collect::<Vec<_>>();
            Ok(Response::new(stream::iter(regrouped).boxed()))
        }
        async fn follow(
            &self,
            request: Request<wire::FollowRequest>,
        ) -> Result<Response<Self::FollowStream>, Status> {
            self.0.follow(request).await
        }
        async fn children(
            &self,
            request: Request<wire::ChildrenRequest>,
        ) -> Result<Response<Self::ChildrenStream>, Status> {
            if self.1 == Some("malformed_children") {
                return Ok(Response::new(
                    stream::iter([Ok(wire::ChildrenResponse { child: None })]).boxed(),
                ));
            }
            self.0.children(request).await
        }
        async fn children_page(
            &self,
            request: Request<wire::ChildrenPageRequest>,
        ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
            self.0.children_page(request).await
        }
        async fn commit(
            &self,
            request: Request<wire::CommitRequest>,
        ) -> Result<Response<wire::CommitResponse>, Status> {
            self.0.commit(request).await
        }
        async fn read_commit(
            &self,
            request: Request<wire::ReadCommitRequest>,
        ) -> Result<Response<wire::CommittedEnvelope>, Status> {
            self.0.read_commit(request).await
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one adversarial matrix compares filtered and visible lifecycle evidence across body boundaries"
    )]
    async fn client_body_completion_observes_decode_trailers_and_drop()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        for (filtered, scenario) in [false, true].into_iter().flat_map(|filtered| {
            ["unpolled", "partial", "eof", "error", "malformed", "limit"]
                .into_iter()
                .map(move |scenario| (filtered, scenario))
        }) {
            let captured = obs::tests::Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::Registry::default()
                    .with(captured.clone())
                    .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                        !filtered || !metadata.name().starts_with("acyclic.stream")
                    })),
            );
            let source = MixedFrames(
                Service::new(Arc::new(MemoryStream::default())),
                Some(match scenario {
                    "partial" | "limit" => "pending",
                    _ => scenario,
                }),
            );
            let client = Client::from_channels(Arc::from([provider_channel(source)]), "fixture")?;
            let body = client
                .read(ReadRequest {
                    path: StreamPath::new("events")?,
                    from: 0,
                    limit: if scenario == "limit" { 1 } else { 10 },
                })
                .with_subscriber(dispatch)
                .await?;
            assert!(
                captured.all("acyclic.stream.grpc.call").is_empty(),
                "creating a lazy cursor performs no RPC"
            );
            let other = obs::tests::Capture::default();
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(other.clone()),
            );
            let caller = tracing::info_span!(
                "polling_caller",
                outcome = "caller",
                error.kind = "caller",
                rpc.code = 99
            );
            async {
                let mut body = body;
                match scenario {
                    "unpolled" => {}
                    "malformed" => {
                        assert_eq!(
                            body.next().await.unwrap().unwrap_err(),
                            StreamError::Unavailable
                        );
                        assert!(body.next().await.is_none());
                        assert!(body.next().await.is_none());
                    }
                    _ => {
                        assert!(body.next().await.unwrap().is_ok());
                        if !filtered {
                            assert!(
                                !captured
                                    .fields("acyclic.stream.grpc.call")
                                    .contains_key("outcome"),
                                "response headers and one delivered record do not observe trailers"
                            );
                        }
                        if scenario == "error" {
                            assert_eq!(
                                body.next().await.unwrap().unwrap_err(),
                                StreamError::AccessDenied
                            );
                            assert!(body.next().await.is_none());
                        }
                        if scenario == "eof" {
                            assert!(body.next().await.is_none());
                        }
                    }
                }
                drop(body);
            }
            .instrument(caller)
            .await;
            assert!(
                other.all("acyclic.stream.grpc.call").is_empty(),
                "polling dispatch must not steal owned RPC spans"
            );
            assert_eq!(other.fields("polling_caller")["outcome"], "caller");
            assert_eq!(other.fields("polling_caller")["rpc.code"], "99");
            if filtered {
                assert!(captured.all("acyclic.stream.grpc.call").is_empty());
                continue;
            }
            let logical = captured.fields("acyclic.stream.grpc.read");
            let rpc = captured.fields("acyclic.stream.grpc.call");
            match scenario {
                "unpolled" => {
                    assert_eq!(logical["error.kind"], "cancelled");
                    assert!(rpc.is_empty());
                }
                "partial" => {
                    assert_eq!(logical["error.kind"], "cancelled");
                    assert_eq!(rpc["error.kind"], "cancelled");
                    assert_eq!(rpc["rpc.code"], "1");
                }
                "limit" => {
                    assert_eq!(logical["outcome"], "ok");
                    assert_eq!(rpc["error.kind"], "cancelled");
                    assert_eq!(rpc["rpc.code"], "1");
                }
                "eof" => {
                    assert_eq!(logical["outcome"], "ok");
                    assert_eq!(rpc["outcome"], "ok");
                    assert_eq!(rpc["rpc.code"], "0");
                }
                "error" => {
                    assert_eq!(logical["error.kind"], "access_denied");
                    assert_eq!(rpc["error.kind"], "access_denied");
                    assert_eq!(rpc["rpc.code"], "7");
                }
                "malformed" => {
                    assert_eq!(logical["error.kind"], "invalid_response");
                    assert_eq!(rpc["error.kind"], "invalid_response");
                    assert!(!rpc.contains_key("rpc.code"));
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn eager_children_semantic_decode_failure_keeps_rpc_trailers_unobserved()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
        );
        let service = MixedFrames(
            Service::new(Arc::new(MemoryStream::default())),
            Some("malformed_children"),
        );
        let client = Client::from_channels(Arc::from([provider_channel(service)]), "fixture")?;
        assert!(matches!(
            client
                .children(ChildrenRequest {
                    parent: None,
                    limit: 10
                })
                .await,
            Err(StreamError::Unavailable)
        ));
        let fields = captured.fields("acyclic.stream.grpc.call");
        assert_eq!(fields["error.kind"], "invalid_response");
        assert!(!fields.contains_key("rpc.code"));
        Ok(())
    }

    #[tokio::test]
    async fn client_follow_reconnects_close_rpc_spans_without_closing_cursor()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
        );
        let ended = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::ZERO,
            denial: None,
        });
        let live = Arc::new(MemoryStream::default());
        let path = StreamPath::new("events")?;
        live.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"record")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
        let client = Client::from_channels(
            Arc::from([
                provider_channel(Service::new(Arc::clone(&ended))),
                in_memory_channel(live),
            ]),
            "fixture",
        )?;
        let mut cursor = client.follow(path, 0).await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), cursor.next())
                .await?
                .unwrap()
                .is_ok()
        );
        assert_eq!(ended.follows.load(Ordering::Relaxed), 1);
        let rpcs = captured.all("acyclic.stream.grpc.call");
        assert_eq!(rpcs.len(), 2);
        assert_eq!(
            rpcs.iter()
                .filter(|fields| fields.get("outcome").is_some_and(|value| value == "ok"))
                .count(),
            1
        );
        assert_eq!(
            rpcs.iter()
                .filter(|fields| !fields.contains_key("outcome"))
                .count(),
            1
        );
        assert!(
            !captured
                .fields("acyclic.stream.grpc.follow")
                .contains_key("outcome")
        );
        drop(cursor);
        assert_eq!(captured.all("acyclic.stream.grpc.follow").len(), 1);
        assert_eq!(
            captured.fields("acyclic.stream.grpc.follow")["error.kind"],
            "cancelled"
        );
        let rpcs = captured.all("acyclic.stream.grpc.call");
        assert!(
            rpcs.iter()
                .any(|fields| fields.get("rpc.code").is_some_and(|value| value == "0"))
        );
        assert!(
            rpcs.iter()
                .any(|fields| fields.get("rpc.code").is_some_and(|value| value == "1"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn unary_retry_attempts_finish_independently()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
        );
        let client = Client::connect("https://fixture.invalid", "fixture").await?;
        let mut first = true;
        let response = client
            .unary((), |_, _| {
                let result = if std::mem::take(&mut first) {
                    Err(Status::unavailable("retry"))
                } else {
                    Ok(Response::new(()))
                };
                Box::pin(async move { result })
            })
            .await?;
        let attempts = captured.all("acyclic.stream.grpc.call");
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0]["attempt"], "1");
        assert_eq!(attempts[1]["attempt"], "2");
        assert_eq!(attempts[0]["outcome"], "err");
        assert_eq!(attempts[0]["rpc.code"], "14");
        assert!(!attempts[1].contains_key("outcome"));
        response.decode(Ok)?;
        let attempts = captured.all("acyclic.stream.grpc.call");
        assert_eq!(attempts[0]["outcome"], "err");
        assert_eq!(attempts[1]["outcome"], "ok");
        assert_eq!(attempts[1]["rpc.code"], "0");
        Ok(())
    }

    #[tokio::test]
    async fn unary_headers_wait_for_decode_and_cancel_pending_requests()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
        );
        let client = Client::connect("https://fixture.invalid", "fixture").await?;
        let response = client
            .unary((), |_, _| {
                Box::pin(async { Ok(Response::new(wire::AppendResponse::default())) })
            })
            .await?;
        assert!(
            !captured
                .fields("acyclic.stream.grpc.call")
                .contains_key("outcome")
        );
        assert!(response.decode(append_outcome_from_wire).is_err());
        let fields = captured.fields("acyclic.stream.grpc.call");
        assert_eq!(fields["outcome"], "err");
        assert_eq!(fields["error.kind"], "invalid_response");
        assert_eq!(
            fields["rpc.code"], "0",
            "unary successful trailers remain distinct from semantic rejection"
        );
        let mut pending = Box::pin(client.unary((), |_, _| {
            Box::pin(std::future::pending::<Result<Response<()>, Status>>())
        }));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        assert!(
            captured
                .all("acyclic.stream.grpc.call")
                .iter()
                .any(|fields| fields
                    .get("error.kind")
                    .is_some_and(|value| value == "cancelled"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn client_reassembles_mixed_compressed_and_plain_read_frames()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let service = MixedFrames(Service::new(Arc::new(MemoryStream::default())), None);
        let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
            tower::service_fn(move |_| {
                let service = service.clone();
                async move {
                    let (client, server) = tokio::io::duplex(64 * 1024);
                    tokio::spawn(async move {
                        let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                        let _ = tonic::transport::Server::builder()
                            .add_service(StreamServiceServer::new(service))
                            .serve_with_incoming(incoming)
                            .await;
                    });
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
                }
            }),
        );
        let transport = Client::from_channels(Arc::from([channel]), "fixture")?;
        let path = StreamPath::new("accounts/mixed")?;
        let values = (0..20_u8)
            .map(|value| Bytes::from(vec![value; 64]))
            .collect::<Vec<_>>();
        transport
            .append(AppendRequest {
                path: path.clone(),
                records: values.clone(),
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let records = transport
            .read(ReadRequest {
                path: path.clone(),
                from: 2,
                limit: 17,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            (2..19).collect::<Vec<_>>()
        );
        assert_eq!(
            records
                .into_iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            values.get(2..19).unwrap_or_default().to_vec()
        );
        crate::conformance::verify(&transport).await?;
        Ok(())
    }

    #[tokio::test]
    async fn service_rejects_oversized_wire_commands_before_provider_dispatch() {
        let service = Service::new(Arc::new(MemoryStream::default()));
        let request = wire::AppendRequest {
            path: "x".repeat(crate::MAX_PATH_BYTES),
            records: vec![Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]); 16],
            if_tail: None,
            idempotency_key: None,
        };
        assert!(request.encoded_len() > crate::MAX_COMMAND_BYTES);
        let error = service.append(Request::new(request)).await.err();
        assert_eq!(
            error.as_ref().map(Status::code),
            Some(Code::InvalidArgument)
        );
        assert_eq!(error.as_ref().map(Status::message), Some("limit_exceeded"));
    }

    #[test]
    fn private_ca_bundle_is_exactly_bounded() {
        assert!(matches!(
            bounded_ca_certificate(&[]),
            Err(ConnectError::InvalidCaCertificate)
        ));
        assert!(matches!(
            bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES]),
            Ok(certificate) if certificate.len() == MAX_CA_CERTIFICATE_BYTES
        ));
        assert!(matches!(
            bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]),
            Err(ConnectError::InvalidCaCertificate)
        ));
    }

    fn unavailable_channel() -> Channel {
        Endpoint::from_static("http://unavailable.invalid").connect_with_connector_lazy(
            tower::service_fn(|_| async {
                Err::<hyper_util::rt::TokioIo<tokio::io::DuplexStream>, _>(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "unavailable",
                ))
            }),
        )
    }

    fn stalled_channel() -> Channel {
        Endpoint::from_static("http://stalled.invalid").connect_with_connector_lazy(
            tower::service_fn(|_| async {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let _server = server;
                    std::future::pending::<()>().await;
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }),
        )
    }

    #[tokio::test]
    async fn endpoint_pool_is_explicit_and_bounded()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let shared = Client::connect_endpoints(
            ["https://data-a.invalid", "https://data-b.invalid"],
            "fixture",
        )
        .await?;
        assert_eq!(shared.channels.len(), 2);

        assert!(matches!(
            Client::connect_endpoints(std::iter::empty::<&str>(), "fixture").await,
            Err(ConnectError::NoEndpoints)
        ));
        assert!(matches!(
            Client::connect_endpoints(std::iter::repeat("https://data.invalid"), "fixture",).await,
            Err(ConnectError::EndpointLimit)
        ));
        let oversized = format!("https://{}.invalid", "x".repeat(MAX_ENDPOINT_URI_BYTES));
        assert!(matches!(
            Client::connect_endpoints([oversized], "fixture").await,
            Err(ConnectError::EndpointLimit)
        ));
        let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
        for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
            assert!(matches!(
                Client::connect_endpoints(["https://data.invalid"], token).await,
                Err(ConnectError::InvalidCredential)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn dropping_eager_connect_releases_a_stalled_tls_socket()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
        let certificate = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certificate.cert.pem();
        let (accepted_sender, accepted_receiver) = tokio::sync::oneshot::channel();
        let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let _ = accepted_sender.send(());
            let mut buffer = [0_u8; 1024];
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = closed_sender.send(());
            Ok::<(), std::io::Error>(())
        });
        let pending = tokio::spawn(Client::connect_eager_with_ca_certificate(
            endpoint,
            "fixture",
            certificate_pem,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), accepted_receiver).await??;
        pending.abort();
        let _ = pending.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), closed_receiver).await??;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn malformed_bearer_is_rejected_before_eager_network_io()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
        let Err(error) = Client::connect_eager(endpoint, "\r\n").await else {
            return Err("malformed bearer unexpectedly connected".into());
        };
        assert!(matches!(error, ConnectError::InvalidCredential));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept(),)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn default_https_pool_installs_tls_before_first_use()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::connect("https://127.0.0.1:1", "fixture").await?;
        let mut service = Client::service(&client.channels, 0);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            service.tail(Request::new(wire::TailRequest {
                path: "accounts/events".to_owned(),
            })),
        )
        .await?
        .err()
        .ok_or("closed local endpoint unexpectedly answered")?;
        assert!(
            !error.to_string().contains("TLS is not enabled"),
            "HTTPS channel was built without TLS: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn private_ca_https_connection_preserves_ambient_roots_and_exact_bearer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let service = StreamServiceServer::with_interceptor(
            Service::new(Arc::new(MemoryStream::default())),
            |request: Request<()>| {
                if request
                    .metadata()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer exact-token")
                {
                    return Err(Status::unauthenticated("missing exact bearer credential"));
                }
                Ok(request)
            },
        );
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let endpoint = format!("https://localhost:{}", address.port());
        let path = StreamPath::new("accounts/events")?;
        let ambient = Client::connect(&endpoint, "exact-token").await?;
        let mut ambient_service = Client::service(&ambient.channels, 0);
        assert!(
            ambient_service
                .tail(Request::new(wire::TailRequest {
                    path: path.to_string(),
                }))
                .await
                .is_err()
        );
        let explicit = crate::StreamClient::connect_with_ca_certificate(
            endpoint,
            "exact-token",
            certificate_pem,
        )
        .await?;
        assert_eq!(
            explicit.stream(path.as_str())?.tail().await,
            Err(StreamError::NotFound)
        );

        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn hung_operation_fails_over_without_truncating_a_healthy_slow_response()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::from_millis(600),
            denial: None,
        });
        provider
            .inner
            .append(AppendRequest {
                path: StreamPath::new("accounts/events")?,
                records: vec![Bytes::from_static(b"seed")],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"slow-tail"))?),
            })
            .await?;
        let transport = Client::from_channels(
            Arc::from([stalled_channel(), provider_channel(Service::new(provider))]),
            "fixture",
        )?;
        let tail = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            transport.tail(StreamPath::new("accounts/events")?),
        )
        .await??;
        assert_eq!(tail, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_denied_follow_reports_access_denied_once_and_then_ends()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::ZERO,
            denial: Some(2),
        });
        provider
            .inner
            .append(AppendRequest {
                path: StreamPath::new("accounts/events")?,
                records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let admissions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&admissions);
        let service = StreamServiceServer::with_interceptor(
            Service::new(Arc::clone(&provider)),
            move |request: Request<()>| {
                observed.fetch_add(1, Ordering::Relaxed);
                if request
                    .metadata()
                    .get("authorization")
                    .is_some_and(|value| value == "Bearer denied-open")
                {
                    return Err(Status::permission_denied("fixture authorization denied"));
                }
                Ok(request)
            },
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let mut server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let bound = std::time::Duration::from_secs(2);
        let result = tokio::time::timeout(bound, async {
            let channel = Endpoint::from_shared(format!("http://{address}"))?
                .connect()
                .await?;
            let mut sequences = Vec::new();
            for token in ["fixture", "denied-open"] {
                let client = Client::from_channels(Arc::from([channel.clone()]), token)?;
                let items = client
                    .follow(StreamPath::new("accounts/events")?, 0)
                    .await?
                    .collect::<Vec<_>>()
                    .await;
                sequences.push(
                    items
                        .into_iter()
                        .map(|item| item.map(|record| record.sequence))
                        .collect::<Vec<_>>(),
                );
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(sequences)
        })
        .await;
        let _ = shutdown_tx.send(());
        let stopped = tokio::time::timeout(bound, &mut server).await;
        if stopped.is_err() {
            server.abort();
            let _ = tokio::time::timeout(bound, &mut server).await;
        }
        let sequences = result??;
        stopped???;
        assert_eq!(
            sequences,
            vec![
                vec![Ok(0), Ok(1), Err(StreamError::AccessDenied)],
                vec![Err(StreamError::AccessDenied)],
            ]
        );
        assert_eq!(admissions.load(Ordering::Relaxed), 2);
        assert_eq!(provider.follows.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn clean_follow_eof_advances_once_to_the_next_endpoint()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ended = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::ZERO,
            denial: None,
        });
        let durable = Arc::new(MemoryStream::default());
        durable
            .append(AppendRequest {
                path: StreamPath::new("accounts/events")?,
                records: vec![Bytes::from_static(b"resumed")],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"resume"))?),
            })
            .await?;
        let transport = Client::from_channels(
            Arc::from([
                provider_channel(Service::new(Arc::clone(&ended))),
                in_memory_channel(durable),
            ]),
            "fixture",
        )?;
        let preferred = Arc::clone(&transport.preferred);
        let stream = crate::StreamClient::new(Arc::new(transport)).stream("accounts/events")?;

        let record = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            stream
                .follow(0)
                .await?
                .next()
                .await
                .ok_or(StreamError::Unavailable)?
        })
        .await??;
        assert_eq!(record.value, Bytes::from_static(b"resumed"));
        assert_eq!(ended.follows.load(Ordering::Relaxed), 1);
        assert_eq!(preferred.load(Ordering::Relaxed), 1);

        // A stale cursor cannot overwrite a newer successful endpoint choice.
        let stale = RecordCursor {
            client: Client::from_channels(
                Arc::from([unavailable_channel(), unavailable_channel()]),
                "fixture",
            )?,
            path: StreamPath::new("accounts/events")?,
            next: 0,
            remaining: None,
            active: None,
            buffered: VecDeque::new(),
            completion: Completion::new(tracing::Span::none()),
        };
        stale.client.preferred.store(1, Ordering::Relaxed);
        stale.advance_follow(0);
        assert_eq!(stale.client.preferred.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn lost_connection_dispatch_retries_but_peer_cancellation_does_not()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (client_io, _peer) = tokio::io::duplex(4096);
        let (mut sender, connection) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(client_io),
        )
        .await?;
        // Losing the driver closes Hyper's dispatch queue, independently of a peer status.
        // No background task or network listener is needed to reproduce this transport error.
        drop(connection);
        let request = tonic::codegen::http::Request::new(tonic::body::Body::empty());
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sender.send_request(request),
        )
        .await?
        .err()
        .ok_or("closed connection accepted a request")?;
        assert!(error.is_canceled());
        let error = Status::from_error(Box::new(error));
        assert_eq!(error.code(), Code::Cancelled);
        assert!(retryable(&error), "{error:?}");
        assert_unary_retry(error, true).await?;
        assert_unary_retry(Status::cancelled("application cancellation"), false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_response_keeps_the_same_request_retryable()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tonic::codec::Codec;
        let mut codec =
            tonic_prost::ProstCodec::<wire::AppendResponse, wire::AppendResponse>::default();
        let mut response = tonic::Streaming::new_response(
            codec.decoder(),
            tonic::body::Body::empty(),
            tonic::codegen::http::StatusCode::OK,
            None,
            Some(1024),
        );
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), response.message())
            .await?
            .err()
            .ok_or("missing terminal status must not succeed")?;
        assert_eq!(error.code(), Code::Unknown);
        assert!(std::error::Error::source(&error).is_none());
        assert!(
            retryable(&error),
            "incomplete response must preserve the retry identity: {error}"
        );
        assert_unary_retry(error, true).await?;
        assert_unary_retry(Status::unknown("peer outcome unknown"), true).await?;
        Ok(())
    }

    async fn assert_unary_retry(
        error: Status,
        retry: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::connect("https://fixture.invalid", "fixture").await?;
        let body = wire::AppendRequest {
            path: "/fixture".to_owned(),
            records: vec![Bytes::from_static(b"mutation")],
            if_tail: Some(3),
            idempotency_key: Some(Bytes::from_static(b"stable-key")),
        };
        let mut attempts = Vec::new();
        let mut first = Some(error);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.unary(body.clone(), |_, request| {
                attempts.push(request.into_inner());
                let result = first.take().map_or_else(|| Ok(Response::new(())), Err);
                Box::pin(async move { result })
            }),
        )
        .await?
        .and_then(|response| response.decode(Ok));
        if retry {
            assert_eq!(result, Ok(()));
            assert_eq!(attempts, vec![body.clone(), body]);
        } else {
            assert!(result.is_err());
            assert_eq!(attempts, vec![body]);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_unknown_expires_without_changing_request_identity()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::connect("https://fixture.invalid", "fixture").await?;
        let started = tokio::time::Instant::now();
        let mut attempts = 0;
        let result: Result<(), StreamError> = client
            .unary(Bytes::from_static(b"stable-key-and-body"), |_, request| {
                assert_eq!(
                    request.into_inner(),
                    Bytes::from_static(b"stable-key-and-body")
                );
                attempts += 1;
                Box::pin(async {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Err(Status::unknown("outcome unavailable"))
                })
            })
            .await
            .and_then(|response| response.decode(Ok));
        assert_eq!(result, Err(StreamError::Unavailable));
        assert_eq!(tokio::time::Instant::now() - started, OPERATION_DEADLINE);
        assert!((2..=100).contains(&attempts));
        Ok(())
    }

    #[tokio::test]
    async fn remote_request_reset_is_retryable_without_retrying_protocol_errors()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for reason in [h2::Reason::CANCEL, h2::Reason::PROTOCOL_ERROR] {
            let mut tasks = tokio::task::JoinSet::new();
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                let (client_io, server_io) = tokio::io::duplex(4096);
                tasks.spawn(async move {
                    let mut connection = h2::server::handshake(server_io).await?;
                    let (_, mut response) = connection.accept().await.ok_or("request absent")??;
                    response.send_reset(reason);
                    connection.graceful_shutdown();
                    while connection.accept().await.is_some() {}
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                });
                let (mut sender, connection) = h2::client::handshake(client_io).await?;
                tasks.spawn(async move {
                    connection.await?;
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                });
                let request = tonic::codegen::http::Request::builder()
                    .uri("http://fixture.invalid/stream")
                    .body(())?;
                let (response, _) = sender.send_request(request, true)?;
                response.await.err().ok_or_else(|| {
                    Box::<dyn std::error::Error + Send + Sync>::from("reset response succeeded")
                })
            })
            .await;
            tasks.shutdown().await;
            let error = outcome??;
            assert!(error.is_remote());
            let error = Status::from_error(Box::new(error));
            assert_eq!(retryable(&error), reason == h2::Reason::CANCEL, "{error:?}");
            assert_unary_retry(error, reason == h2::Reason::CANCEL).await?;
        }
        Ok(())
    }

    #[test]
    fn retries_transport_loss_but_not_peer_errors_or_corrupt_data() {
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::TimedOut,
        ] {
            let mut error = Status::unknown("response body interrupted");
            error.set_source(Arc::new(std::io::Error::from(kind)));
            assert!(retryable(&error), "{kind:?}");
        }
        for error in [
            Status::internal("protocol error"),
            Status::data_loss("corrupt record"),
            Status::permission_denied("denied"),
            Status::cancelled("caller cancelled"),
            Status::from_error(Box::new(std::io::Error::from(
                std::io::ErrorKind::InvalidData,
            ))),
        ] {
            assert!(!retryable(&error), "{error:?}");
        }
    }

    #[test]
    fn inspection_rejects_an_observation_for_another_retry_identity() -> Result<(), StreamError> {
        let requested = IdempotencyKey::new(Bytes::from_static(b"requested"))?;
        let forged = wire::IdempotencyObservation {
            idempotency_key: Bytes::from_static(b"other"),
            request_digest: Bytes::from_static(&[7; 32]),
            outcome: Some(wire::idempotency_observation::Outcome::Append(
                append_outcome_wire(AppendOutcome::TailConflict { actual_tail: 3 }),
            )),
        };
        assert_eq!(
            bind_observation(&requested, Some(forged)),
            Err(StreamError::Unavailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn service_maps_the_canonical_contract_without_an_alternate_state_machine()
    -> Result<(), Status> {
        let service = Service::new(Arc::new(MemoryStream::default()));
        let append = service
            .append(Request::new(wire::AppendRequest {
                path: "accounts/events".to_owned(),
                records: vec![Bytes::from_static(b"one")],
                if_tail: Some(0),
                idempotency_key: Some(Bytes::from_static(b"wire-append")),
            }))
            .await?
            .into_inner();
        let Some(wire::append_response::Outcome::Committed(receipt)) = append.outcome else {
            return Err(Status::internal("append outcome missing"));
        };
        if receipt.start != 0 || receipt.end != 1 || receipt.commit_id.len() != 32 {
            return Err(Status::internal("append receipt changed"));
        }
        let observation = service
            .inspect_idempotency(Request::new(wire::InspectIdempotencyRequest {
                idempotency_key: Bytes::from_static(b"wire-append"),
            }))
            .await?
            .into_inner()
            .observation
            .ok_or_else(|| Status::internal("idempotency observation missing"))?;
        if observation.request_digest.len() != 32
            || !matches!(
                observation.outcome,
                Some(wire::idempotency_observation::Outcome::Append(_))
            )
        {
            return Err(Status::internal("idempotency observation changed"));
        }
        let mut read = service
            .read(Request::new(wire::ReadRequest {
                path: "accounts/events".to_owned(),
                from: 0,
                limit: 1,
            }))
            .await?
            .into_inner();
        let frame = read
            .next()
            .await
            .ok_or_else(|| Status::internal("read ended"))??;
        let record = read_response_records(frame)
            .map_err(|error| error_status(&error))?
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("record missing"))?;
        if record.sequence != 0 || record.value != Bytes::from_static(b"one") {
            return Err(Status::internal("record changed"));
        }
        Ok(())
    }
}
