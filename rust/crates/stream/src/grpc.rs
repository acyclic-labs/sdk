//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use thiserror::Error;
use tonic::{
    Code, Request, Response, Status,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

use crate::wire_codec::{
    condition_from_wire, condition_wire, mutation_from_wire, mutation_wire, optional_key, path,
    required_key,
};
use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenRequest,
    CommitConflict, CommitId, CommitOutcome, CommitRequest, CommittedAppend, CommittedDelete,
    CommittedEnvelope, CommittedFork, CommittedMutation, CommittedTrim, DeleteReceipt, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, ReadRequest, Record,
    RecordStream, StreamError, StreamPath, StreamProvider, TrimReceipt, wire,
};

const OPERATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const OPERATION_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10);
/// Maximum independently reachable endpoints in one operation or follow pool.
pub const MAX_ENDPOINTS: usize = 16;
/// Maximum canonical URI bytes accepted for one endpoint.
pub const MAX_ENDPOINT_URI_BYTES: usize = 2_048;

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
    follow_channels: Arc<[Channel]>,
    authorization: MetadataValue<Ascii>,
    preferred: Arc<AtomicUsize>,
    follow_preferred: Arc<AtomicUsize>,
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

    /// Connects ordinary operations and long-lived follows through independent endpoint pools.
    ///
    /// Follow endpoints may include disposable Relay processes followed by durable data endpoints;
    /// every other operation always uses `endpoints`.
    pub async fn connect_endpoints_with_follow_endpoints<I, S, F, T>(
        endpoints: I,
        follow_endpoints: F,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        F: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        Self::connect_pools_with_tls(endpoints, follow_endpoints, bearer_token, None)
    }

    /// Connects to a TLS endpoint augmented by one caller-pinned private CA certificate.
    pub async fn connect_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        let certificate_pem = certificate_pem.as_ref();
        if certificate_pem.is_empty() || certificate_pem.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCredential);
        }
        Self::connect_endpoints_with_ca_certificate([endpoint], bearer_token, certificate_pem).await
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
        let certificate_pem = certificate_pem.as_ref();
        if certificate_pem.is_empty() || certificate_pem.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCredential);
        }
        Self::connect_with_tls(endpoints, bearer_token, Some(certificate_pem))
    }

    /// Connects independent operation and follow pools using one caller-pinned private CA.
    pub async fn connect_endpoints_with_follow_endpoints_and_ca_certificate<I, S, F, T>(
        endpoints: I,
        follow_endpoints: F,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        F: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let certificate_pem = certificate_pem.as_ref();
        if certificate_pem.is_empty() || certificate_pem.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCredential);
        }
        Self::connect_pools_with_tls(
            endpoints,
            follow_endpoints,
            bearer_token,
            Some(certificate_pem),
        )
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
        let channels = Self::channels_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        let follow_channels = Arc::clone(&channels);
        Self::from_pools(channels, follow_channels, bearer_token)
    }

    fn connect_pools_with_tls<I, S, F, T>(
        endpoints: I,
        follow_endpoints: F,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        F: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let channels = Self::channels_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        let follow_channels = Self::channels_with_tls(
            follow_endpoints,
            certificate_pem,
            FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        Self::from_pools(channels, follow_channels, bearer_token)
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
        let mut channels = Vec::new();
        for endpoint in endpoints.into_iter().take(MAX_ENDPOINTS + 1) {
            if channels.len() == MAX_ENDPOINTS {
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
                    ClientTlsConfig::new().ca_certificate(Certificate::from_pem(certificate_pem)),
                )?;
            }
            channels.push(endpoint.connect_lazy());
        }
        if channels.is_empty() {
            return Err(ConnectError::NoEndpoints);
        }
        Ok(channels.into())
    }

    fn from_pools(
        channels: Arc<[Channel]>,
        follow_channels: Arc<[Channel]>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        let authorization = format!("Bearer {}", bearer_token.as_ref())
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ConnectError::InvalidCredential)?;
        Ok(Self {
            channels,
            follow_channels,
            authorization,
            preferred: Arc::new(AtomicUsize::new(0)),
            follow_preferred: Arc::new(AtomicUsize::new(0)),
        })
    }

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

    async fn unary<T, U, F>(&self, body: T, mut call: F) -> Result<U, StreamError>
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
        .map(|(response, _)| response)
    }

    async fn follow_unary<T, U, F>(&self, body: T, mut call: F) -> Result<(U, usize), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        self.unary_on(
            &self.follow_channels,
            &self.follow_preferred,
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
    ) -> Result<(U, usize), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut last = None;
        loop {
            let start = preferred.load(Ordering::Relaxed) % channels.len();
            for offset in 0..channels.len() {
                let index = (start + offset) % channels.len();
                let attempt_deadline = deadline.min(tokio::time::Instant::now() + attempt_timeout);
                match tokio::time::timeout_at(
                    attempt_deadline,
                    call(Self::service(channels, index), self.request(body.clone())),
                )
                .await
                {
                    Ok(Ok(response)) => {
                        preferred.store(index, Ordering::Relaxed);
                        return Ok((response.into_inner(), index));
                    }
                    Ok(Err(error)) if retryable(&error) => last = Some(error),
                    Ok(Err(error)) => return Err(status(error)),
                    Err(_) => last = Some(Status::deadline_exceeded("endpoint attempt expired")),
                }
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(last.map_or(StreamError::Unavailable, status));
            }
        }
    }

    async fn records(
        &self,
        path: StreamPath,
        from: u64,
        limit: Option<u32>,
    ) -> Result<RecordStream, StreamError> {
        let client = self.clone();
        Ok(stream::unfold(
            RecordCursor {
                client,
                path,
                next: from,
                remaining: limit,
                active: None,
            },
            |mut cursor| async move {
                loop {
                    if cursor.remaining == Some(0) {
                        return None;
                    }
                    if cursor.active.is_none() {
                        match cursor.open().await {
                            Ok(active) => cursor.active = Some(active),
                            Err(error) => return Some((Err(error), cursor)),
                        }
                    }
                    let Some(active) = cursor.active.as_mut() else {
                        return Some((Err(StreamError::Unavailable), cursor));
                    };
                    let active_endpoint = active.endpoint;
                    match active.records.next().await {
                        Some(Ok(response)) => match read_response(response) {
                            Ok(record) if record.sequence == cursor.next => {
                                cursor.next = cursor.next.saturating_add(1);
                                if let Some(remaining) = &mut cursor.remaining {
                                    *remaining = remaining.saturating_sub(1);
                                }
                                return Some((Ok(record), cursor));
                            }
                            Ok(record) if record.sequence < cursor.next => continue,
                            Ok(_) => return Some((Err(StreamError::Unavailable), cursor)),
                            Err(error) => return Some((Err(error), cursor)),
                        },
                        Some(Err(error)) if retryable(&error) => {
                            cursor.advance_follow(active_endpoint);
                            tokio::time::sleep(RETRY_DELAY).await;
                            cursor.active = None;
                        }
                        Some(Err(error)) => return Some((Err(status(error)), cursor)),
                        None if cursor.remaining.is_none() => {
                            cursor.advance_follow(active_endpoint);
                            tokio::time::sleep(RETRY_DELAY).await;
                            cursor.active = None;
                        }
                        None => return None,
                    }
                }
            },
        )
        .boxed())
    }
}

struct RecordCursor {
    client: Client,
    path: StreamPath,
    next: u64,
    remaining: Option<u32>,
    active: Option<ActiveRecords>,
}

struct ActiveRecords {
    records: tonic::Streaming<wire::ReadResponse>,
    endpoint: usize,
}

impl RecordCursor {
    fn advance_follow(&self, observed: usize) {
        if self.remaining.is_some() {
            return;
        }
        let next = (observed + 1) % self.client.follow_channels.len();
        let _ = self.client.follow_preferred.compare_exchange(
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
                .map(|records| ActiveRecords {
                    records,
                    endpoint: 0,
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
                .map(|(records, endpoint)| ActiveRecords { records, endpoint })
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
        bind_observation(idempotency_key, response.observation)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.unary(
            wire::TailRequest {
                path: path.to_string(),
            },
            |mut service, request| Box::pin(async move { service.tail(request).await }),
        )
        .await
        .map(|response| response.tail)
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
        append_outcome_from_wire(response)
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
        Ok(ForkReceipt {
            source: path(receipt.source)?,
            destination: path(receipt.destination)?,
            forked_at: receipt.forked_at,
            tail: receipt.tail,
            commit_id: commit_id(&receipt.commit_id)?,
        })
    }

    async fn trim(
        &self,
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TrimReceipt, StreamError> {
        let receipt = self
            .unary(
                wire::TrimRequest {
                    path: path.to_string(),
                    before,
                    idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
                },
                |mut service, request| Box::pin(async move { service.trim(request).await }),
            )
            .await?;
        Ok(TrimReceipt {
            path: crate::grpc::path(receipt.path)?,
            trim_point: receipt.trim_point,
            commit_id: commit_id(&receipt.commit_id)?,
        })
    }

    async fn delete(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<DeleteReceipt, StreamError> {
        let receipt = self
            .unary(
                wire::DeleteRequest {
                    path: path.to_string(),
                    idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
                },
                |mut service, request| Box::pin(async move { service.delete(request).await }),
            )
            .await?;
        Ok(DeleteReceipt {
            path: crate::grpc::path(receipt.path)?,
            commit_id: commit_id(&receipt.commit_id)?,
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
            let collected = response
                .map(|item| {
                    let child = item
                        .map_err(status)?
                        .child
                        .ok_or(StreamError::Unavailable)?;
                    Ok(Child {
                        path: path(child.path)?,
                    })
                })
                .collect::<Vec<_>>()
                .await;
            if collected.iter().all(Result::is_ok) {
                return Ok(stream::iter(collected).boxed());
            }
            let error = collected
                .into_iter()
                .find_map(Result::err)
                .unwrap_or(StreamError::Unavailable);
            if error != StreamError::Unavailable {
                return Err(error);
            }
            last = Some(error);
        }
        Err(last.unwrap_or(StreamError::Unavailable))
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        commit_outcome_from_wire(response)
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
        envelope_from_wire(envelope)
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
        let key =
            IdempotencyKey::new(request.into_inner().idempotency_key).map_err(error_status)?;
        let observation = self
            .provider
            .inspect_idempotency(key)
            .await
            .map_err(error_status)?
            .map(observation_wire);
        Ok(Response::new(wire::InspectIdempotencyResponse {
            observation,
        }))
    }

    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        let request = request.into_inner();
        let outcome = self
            .provider
            .append(AppendRequest {
                path: path(request.path).map_err(error_status)?,
                records: request.records,
                if_tail: request.if_tail,
                idempotency_key: optional_key(request.idempotency_key).map_err(error_status)?,
            })
            .await
            .map_err(error_status)?;
        Ok(Response::new(append_outcome_wire(outcome)))
    }

    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        let path = path(request.into_inner().path).map_err(error_status)?;
        let tail = self.provider.tail(path).await.map_err(error_status)?;
        Ok(Response::new(wire::TailResponse { tail }))
    }

    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        let request = request.into_inner();
        let receipt = self
            .provider
            .fork(ForkRequest {
                source: path(request.source).map_err(error_status)?,
                destination: path(request.destination).map_err(error_status)?,
                at_tail: request.at_tail,
                idempotency_key: optional_key(request.idempotency_key).map_err(error_status)?,
            })
            .await
            .map_err(error_status)?;
        Ok(Response::new(fork_receipt_wire(receipt)))
    }

    async fn trim(
        &self,
        request: Request<wire::TrimRequest>,
    ) -> Result<Response<wire::TrimReceipt>, Status> {
        let request = request.into_inner();
        let receipt = self
            .provider
            .trim(
                path(request.path).map_err(error_status)?,
                request.before,
                required_key(request.idempotency_key).map_err(error_status)?,
            )
            .await
            .map_err(error_status)?;
        Ok(Response::new(trim_receipt_wire(receipt)))
    }

    async fn delete(
        &self,
        request: Request<wire::DeleteRequest>,
    ) -> Result<Response<wire::DeleteReceipt>, Status> {
        let request = request.into_inner();
        let receipt = self
            .provider
            .delete(
                path(request.path).map_err(error_status)?,
                required_key(request.idempotency_key).map_err(error_status)?,
            )
            .await
            .map_err(error_status)?;
        Ok(Response::new(delete_receipt_wire(receipt)))
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        let records = self
            .provider
            .read(ReadRequest {
                path: path(request.path).map_err(error_status)?,
                from: request.from,
                limit: request.limit,
            })
            .await
            .map_err(error_status)?;
        Ok(Response::new(
            records
                .map(|record| {
                    record
                        .map(record_wire)
                        .map(|record| wire::ReadResponse {
                            record: Some(record),
                        })
                        .map_err(error_status)
                })
                .boxed(),
        ))
    }

    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let request = request.into_inner();
        let records = self
            .provider
            .follow(path(request.path).map_err(error_status)?, request.from)
            .await
            .map_err(error_status)?;
        Ok(Response::new(
            records
                .map(|record| {
                    record
                        .map(record_wire)
                        .map(|record| wire::ReadResponse {
                            record: Some(record),
                        })
                        .map_err(error_status)
                })
                .boxed(),
        ))
    }

    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        let request = request.into_inner();
        let children = self
            .provider
            .children(ChildrenRequest {
                parent: request.parent.map(path).transpose().map_err(error_status)?,
                limit: request.limit,
            })
            .await
            .map_err(error_status)?;
        Ok(Response::new(
            children
                .map(|child| {
                    child
                        .map(|child| wire::ChildrenResponse {
                            child: Some(wire::Child {
                                path: child.path.to_string(),
                            }),
                        })
                        .map_err(error_status)
                })
                .boxed(),
        ))
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        let request = request.into_inner();
        let outcome = self
            .provider
            .commit(CommitRequest {
                conditions: request
                    .conditions
                    .into_iter()
                    .map(condition_from_wire)
                    .collect::<Result<_, _>>()
                    .map_err(error_status)?,
                mutations: request
                    .mutations
                    .into_iter()
                    .map(mutation_from_wire)
                    .collect::<Result<_, _>>()
                    .map_err(error_status)?,
                idempotency_key: IdempotencyKey::new(request.idempotency_key)
                    .map_err(error_status)?,
            })
            .await
            .map_err(error_status)?;
        Ok(Response::new(commit_outcome_wire(outcome)))
    }

    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        let commit_id = commit_id(&request.into_inner().commit_id).map_err(error_status)?;
        let envelope = self
            .provider
            .read_commit(commit_id)
            .await
            .map_err(error_status)?;
        Ok(Response::new(envelope_wire(envelope)))
    }
}

fn observation_from_wire(
    value: wire::IdempotencyObservation,
) -> Result<IdempotencyObservation, StreamError> {
    let request_digest = <[u8; 32]>::try_from(value.request_digest.as_ref())
        .map_err(|_| StreamError::Unavailable)?;
    let outcome = match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::idempotency_observation::Outcome::Append(value) => {
            IdempotencyOutcome::Append(append_outcome_from_wire(value)?)
        }
        wire::idempotency_observation::Outcome::Fork(value) => {
            IdempotencyOutcome::Fork(ForkReceipt {
                source: path(value.source)?,
                destination: path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Trim(value) => {
            IdempotencyOutcome::Trim(TrimReceipt {
                path: path(value.path)?,
                trim_point: value.trim_point,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Delete(value) => {
            IdempotencyOutcome::Delete(DeleteReceipt {
                path: path(value.path)?,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Commit(value) => {
            IdempotencyOutcome::Commit(commit_outcome_from_wire(value)?)
        }
    };
    Ok(IdempotencyObservation {
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
        request_digest,
        outcome,
    })
}

fn bind_observation(
    requested: IdempotencyKey,
    observation: Option<wire::IdempotencyObservation>,
) -> Result<Option<IdempotencyObservation>, StreamError> {
    let observation = observation.map(observation_from_wire).transpose()?;
    if observation
        .as_ref()
        .is_some_and(|observation| observation.idempotency_key != requested)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(observation)
}

fn observation_wire(value: IdempotencyObservation) -> wire::IdempotencyObservation {
    let outcome = match value.outcome {
        IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_wire(value))
        }
        IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_wire(value))
        }
        IdempotencyOutcome::Trim(value) => {
            wire::idempotency_observation::Outcome::Trim(trim_receipt_wire(value))
        }
        IdempotencyOutcome::Delete(value) => {
            wire::idempotency_observation::Outcome::Delete(delete_receipt_wire(value))
        }
        IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_outcome_wire(value))
        }
    };
    wire::IdempotencyObservation {
        idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
        request_digest: Bytes::copy_from_slice(&value.request_digest),
        outcome: Some(outcome),
    }
}

fn append_outcome_from_wire(value: wire::AppendResponse) -> Result<AppendOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::append_response::Outcome::Committed(receipt) => {
            Ok(AppendOutcome::Committed(append_receipt(receipt)?))
        }
        wire::append_response::Outcome::Conflict(conflict) => Ok(AppendOutcome::TailConflict {
            actual_tail: conflict.actual_tail,
        }),
    }
}

fn append_outcome_wire(value: AppendOutcome) -> wire::AppendResponse {
    let outcome = match value {
        AppendOutcome::Committed(receipt) => {
            wire::append_response::Outcome::Committed(append_receipt_wire(receipt))
        }
        AppendOutcome::TailConflict { actual_tail } => {
            wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
        }
    };
    wire::AppendResponse {
        outcome: Some(outcome),
    }
}

fn commit_outcome_from_wire(value: wire::CommitResponse) -> Result<CommitOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::commit_response::Outcome::Committed(envelope) => {
            Ok(CommitOutcome::Committed(envelope_from_wire(envelope)?))
        }
        wire::commit_response::Outcome::Conflict(conflicts) => Ok(CommitOutcome::Conflict(
            conflicts
                .conflicts
                .into_iter()
                .map(conflict_from_wire)
                .collect::<Result<_, _>>()?,
        )),
    }
}

fn commit_outcome_wire(value: CommitOutcome) -> wire::CommitResponse {
    let outcome = match value {
        CommitOutcome::Committed(envelope) => {
            wire::commit_response::Outcome::Committed(envelope_wire(envelope))
        }
        CommitOutcome::Conflict(conflicts) => {
            wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                conflicts: conflicts.into_iter().map(conflict_wire).collect(),
            })
        }
    };
    wire::CommitResponse {
        outcome: Some(outcome),
    }
}

fn error_status(error: StreamError) -> Status {
    match error {
        StreamError::InvalidPath => Status::invalid_argument("invalid_path"),
        StreamError::InvalidArgument => Status::invalid_argument("invalid_argument"),
        StreamError::LimitExceeded => Status::invalid_argument("limit_exceeded"),
        StreamError::NotFound => Status::not_found(error.to_string()),
        StreamError::AlreadyExists => Status::already_exists(error.to_string()),
        StreamError::OutOfRange => Status::out_of_range(error.to_string()),
        StreamError::AccessDenied => Status::permission_denied(error.to_string()),
        StreamError::Capacity => Status::resource_exhausted(error.to_string()),
        StreamError::IdempotencyMismatch => Status::failed_precondition("idempotency_mismatch"),
        StreamError::Retired => Status::failed_precondition("retired"),
        StreamError::PrefixNotRetained => Status::failed_precondition("prefix_not_retained"),
        StreamError::Unavailable => Status::unavailable(error.to_string()),
    }
}

fn status(error: tonic::Status) -> StreamError {
    match error.code() {
        Code::InvalidArgument if error.message() == "invalid_path" => StreamError::InvalidPath,
        Code::InvalidArgument if error.message() == "limit_exceeded" => StreamError::LimitExceeded,
        Code::InvalidArgument => StreamError::InvalidArgument,
        Code::NotFound => StreamError::NotFound,
        Code::AlreadyExists => StreamError::AlreadyExists,
        Code::OutOfRange => StreamError::OutOfRange,
        Code::PermissionDenied | Code::Unauthenticated => StreamError::AccessDenied,
        Code::ResourceExhausted => StreamError::Capacity,
        Code::FailedPrecondition if error.message() == "idempotency_mismatch" => {
            StreamError::IdempotencyMismatch
        }
        Code::FailedPrecondition if error.message() == "retired" => StreamError::Retired,
        Code::FailedPrecondition if error.message() == "prefix_not_retained" => {
            StreamError::PrefixNotRetained
        }
        _ => StreamError::Unavailable,
    }
}

fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| StreamError::Unavailable)?;
    Ok(CommitId::from_bytes(bytes))
}

fn record(value: wire::Record) -> Result<Record, StreamError> {
    Ok(Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: commit_id(&value.commit_id)?,
    })
}

fn read_response(value: wire::ReadResponse) -> Result<Record, StreamError> {
    record(value.record.ok_or(StreamError::Unavailable)?)
}

fn append_receipt(value: wire::AppendReceipt) -> Result<AppendReceipt, StreamError> {
    Ok(AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: commit_id(&value.commit_id)?,
    })
}

fn record_wire(value: Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn append_receipt_wire(value: AppendReceipt) -> wire::AppendReceipt {
    wire::AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn fork_receipt_wire(value: ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn trim_receipt_wire(value: TrimReceipt) -> wire::TrimReceipt {
    wire::TrimReceipt {
        path: value.path.to_string(),
        trim_point: value.trim_point,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn delete_receipt_wire(value: DeleteReceipt) -> wire::DeleteReceipt {
    wire::DeleteReceipt {
        path: value.path.to_string(),
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn envelope_from_wire(value: wire::CommittedEnvelope) -> Result<CommittedEnvelope, StreamError> {
    Ok(CommittedEnvelope {
        commit_id: commit_id(&value.commit_id)?,
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation)
            .collect::<Result<_, _>>()?,
    })
}

fn envelope_wire(value: CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation_wire)
            .collect(),
    }
}

fn committed_mutation(value: wire::CommittedMutation) -> Result<CommittedMutation, StreamError> {
    match value.mutation.ok_or(StreamError::Unavailable)? {
        wire::committed_mutation::Mutation::Append(value) => {
            Ok(CommittedMutation::Append(CommittedAppend {
                path: path(value.path)?,
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value
                    .records
                    .into_iter()
                    .map(record)
                    .collect::<Result<_, _>>()?,
            }))
        }
        wire::committed_mutation::Mutation::Fork(value) => {
            Ok(CommittedMutation::Fork(CommittedFork {
                source: path(value.source)?,
                destination: path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
            }))
        }
        wire::committed_mutation::Mutation::Trim(value) => {
            Ok(CommittedMutation::Trim(CommittedTrim {
                path: path(value.path)?,
                trim_point: value.trim_point,
            }))
        }
        wire::committed_mutation::Mutation::Delete(value) => {
            Ok(CommittedMutation::Delete(CommittedDelete {
                path: path(value.path)?,
            }))
        }
    }
}

fn committed_mutation_wire(value: CommittedMutation) -> wire::CommittedMutation {
    let mutation = match value {
        CommittedMutation::Append(value) => {
            wire::committed_mutation::Mutation::Append(wire::CommittedAppend {
                path: value.path.to_string(),
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
        CommittedMutation::Fork(value) => {
            wire::committed_mutation::Mutation::Fork(wire::CommittedFork {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
            })
        }
        CommittedMutation::Trim(value) => {
            wire::committed_mutation::Mutation::Trim(wire::CommittedTrim {
                path: value.path.to_string(),
                trim_point: value.trim_point,
            })
        }
        CommittedMutation::Delete(value) => {
            wire::committed_mutation::Mutation::Delete(wire::CommittedDelete {
                path: value.path.to_string(),
            })
        }
    };
    wire::CommittedMutation {
        mutation: Some(mutation),
    }
}

fn conflict_from_wire(value: wire::CommitConflict) -> Result<CommitConflict, StreamError> {
    match value.conflict.ok_or(StreamError::Unavailable)? {
        wire::commit_conflict::Conflict::Tail(value) => Ok(CommitConflict::Tail {
            path: path(value.path)?,
            expected: value.expected,
            actual: value.actual,
        }),
        wire::commit_conflict::Conflict::Exists(value) => Ok(CommitConflict::Exists {
            path: path(value.path)?,
        }),
        wire::commit_conflict::Conflict::Retired(value) => Ok(CommitConflict::Retired {
            path: path(value.path)?,
        }),
    }
}

fn conflict_wire(value: CommitConflict) -> wire::CommitConflict {
    let conflict = match value {
        CommitConflict::Tail {
            path,
            expected,
            actual,
        } => wire::commit_conflict::Conflict::Tail(wire::TailCommitConflict {
            path: path.to_string(),
            expected,
            actual,
        }),
        CommitConflict::Exists { path } => {
            wire::commit_conflict::Conflict::Exists(wire::ExistsCommitConflict {
                path: path.to_string(),
            })
        }
        CommitConflict::Retired { path } => {
            wire::commit_conflict::Conflict::Retired(wire::RetiredCommitConflict {
                path: path.to_string(),
            })
        }
    };
    wire::CommitConflict {
        conflict: Some(conflict),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStream;
    use wire::stream_service_server::{StreamService, StreamServiceServer};

    struct FiniteFollow {
        inner: MemoryStream,
        follows: AtomicUsize,
        tail_delay: std::time::Duration,
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

        async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
            self.inner.append(request).await
        }

        async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
            self.inner.fork(request).await
        }

        async fn trim(
            &self,
            path: StreamPath,
            before: u64,
            key: IdempotencyKey,
        ) -> Result<TrimReceipt, StreamError> {
            self.inner.trim(path, before, key).await
        }

        async fn delete(
            &self,
            path: StreamPath,
            key: IdempotencyKey,
        ) -> Result<DeleteReceipt, StreamError> {
            self.inner.delete(path, key).await
        }

        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            self.inner.read(request).await
        }

        async fn follow(&self, _path: StreamPath, _from: u64) -> Result<RecordStream, StreamError> {
            self.follows.fetch_add(1, Ordering::Relaxed);
            Ok(stream::empty().boxed())
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

    fn provider_channel<P: StreamProvider>(service: Service<P>) -> Channel {
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
    async fn endpoint_pools_are_explicit_bounded_and_default_to_one_authority()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let shared = Client::connect_endpoints(
            ["https://data-a.invalid", "https://data-b.invalid"],
            "fixture",
        )
        .await?;
        assert_eq!(shared.channels.len(), 2);
        assert!(Arc::ptr_eq(&shared.channels, &shared.follow_channels));

        let split = Client::connect_endpoints_with_follow_endpoints(
            ["https://data.invalid"],
            ["https://relay-a.invalid", "https://data.invalid"],
            "fixture",
        )
        .await?;
        assert_eq!(split.channels.len(), 1);
        assert_eq!(split.follow_channels.len(), 2);
        assert!(!Arc::ptr_eq(&split.channels, &split.follow_channels));
        assert!(!Arc::ptr_eq(&split.preferred, &split.follow_preferred));

        assert!(matches!(
            Client::connect_endpoints_with_follow_endpoints(
                std::iter::empty::<&str>(),
                ["https://relay.invalid"],
                "fixture",
            )
            .await,
            Err(ConnectError::NoEndpoints)
        ));
        assert!(matches!(
            Client::connect_endpoints_with_follow_endpoints(
                ["https://data.invalid"],
                std::iter::empty::<&str>(),
                "fixture",
            )
            .await,
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
    async fn hung_operation_fails_over_without_truncating_a_healthy_slow_response()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::from_millis(600),
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
        let transport = Client::from_pools(
            Arc::from([stalled_channel(), provider_channel(Service::new(provider))]),
            Arc::from([unavailable_channel()]),
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
    async fn finite_reads_use_data_while_follows_use_the_independent_pool()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = Arc::new(MemoryStream::default());
        let relay = Arc::new(MemoryStream::default());
        for (provider, value, key) in [
            (&data, Bytes::from_static(b"durable"), b"data".as_slice()),
            (&relay, Bytes::from_static(b"relayed"), b"relay".as_slice()),
        ] {
            provider
                .append(AppendRequest {
                    path: StreamPath::new("accounts/events")?,
                    records: vec![value],
                    if_tail: Some(0),
                    idempotency_key: Some(IdempotencyKey::new(Bytes::copy_from_slice(key))?),
                })
                .await?;
        }

        let transport = Client::from_pools(
            Arc::from([unavailable_channel(), in_memory_channel(data)]),
            Arc::from([stalled_channel(), in_memory_channel(relay)]),
            "fixture",
        )?;
        let data_preferred = Arc::clone(&transport.preferred);
        let follow_preferred = Arc::clone(&transport.follow_preferred);
        let stream = crate::StreamClient::new(Arc::new(transport)).stream("accounts/events")?;

        let finite = stream
            .read(0, 1)
            .await?
            .next()
            .await
            .ok_or("finite read ended")??;
        assert_eq!(finite.value, Bytes::from_static(b"durable"));
        assert_eq!(data_preferred.load(Ordering::Relaxed), 1);
        assert_eq!(follow_preferred.load(Ordering::Relaxed), 0);

        let followed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            stream
                .follow(0)
                .await?
                .next()
                .await
                .ok_or(StreamError::Unavailable)?
        })
        .await??;
        assert_eq!(followed.value, Bytes::from_static(b"relayed"));
        assert_eq!(follow_preferred.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn clean_follow_eof_advances_once_to_the_next_endpoint()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ended = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::ZERO,
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
        let transport = Client::from_pools(
            Arc::from([in_memory_channel(Arc::clone(&durable))]),
            Arc::from([
                provider_channel(Service::new(Arc::clone(&ended))),
                in_memory_channel(durable),
            ]),
            "fixture",
        )?;
        let preferred = Arc::clone(&transport.follow_preferred);
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
            client: Client::from_pools(
                Arc::from([unavailable_channel()]),
                Arc::from([unavailable_channel(), unavailable_channel()]),
                "fixture",
            )?,
            path: StreamPath::new("accounts/events")?,
            next: 0,
            remaining: None,
            active: None,
        };
        stale.client.follow_preferred.store(1, Ordering::Relaxed);
        stale.advance_follow(0);
        assert_eq!(stale.client.follow_preferred.load(Ordering::Relaxed), 1);
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
        .await?;
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
            .await;
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
            bind_observation(requested, Some(forged)),
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
        let record = read
            .next()
            .await
            .ok_or_else(|| Status::internal("read ended"))??
            .record
            .ok_or_else(|| Status::internal("record missing"))?;
        if record.sequence != 0 || record.value != Bytes::from_static(b"one") {
            return Err(Status::internal("record changed"));
        }
        Ok(())
    }
}
