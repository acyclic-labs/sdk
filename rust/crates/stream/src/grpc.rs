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

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenRequest,
    CommitCondition, CommitConflict, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    CommittedAppend, CommittedDelete, CommittedEnvelope, CommittedFork, CommittedMutation,
    CommittedTrim, DeleteReceipt, ForkReceipt, ForkRequest, IdempotencyKey, ReadRequest, Record,
    RecordStream, StreamError, StreamPath, StreamProvider, TrimReceipt, wire,
};

const OPERATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(1);

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

    fn connect_with_tls<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let channels = endpoints
            .into_iter()
            .map(|endpoint| {
                let mut endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())?;
                if endpoint.uri().scheme_str() != Some("https") {
                    return Err(ConnectError::InsecureEndpoint);
                }
                if let Some(certificate_pem) = certificate_pem {
                    endpoint = endpoint.tls_config(
                        ClientTlsConfig::new()
                            .ca_certificate(Certificate::from_pem(certificate_pem)),
                    )?;
                }
                Ok(endpoint.connect_lazy())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if channels.is_empty() {
            return Err(ConnectError::NoEndpoints);
        }
        let authorization = format!("Bearer {}", bearer_token.as_ref())
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ConnectError::InvalidCredential)?;
        Ok(Self {
            channels: channels.into(),
            authorization,
            preferred: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn service(&self, index: usize) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(self.channels[index].clone())
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
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut last = None;
        loop {
            let start = self.preferred.load(Ordering::Relaxed) % self.channels.len();
            for offset in 0..self.channels.len() {
                let index = (start + offset) % self.channels.len();
                match tokio::time::timeout_at(
                    deadline,
                    call(self.service(index), self.request(body.clone())),
                )
                .await
                {
                    Ok(Ok(response)) => {
                        self.preferred.store(index, Ordering::Relaxed);
                        return Ok(response.into_inner());
                    }
                    Ok(Err(error)) if retryable(&error) => last = Some(error),
                    Ok(Err(error)) => return Err(status(error)),
                    Err(_) => return Err(last.map_or(StreamError::Unavailable, status)),
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
                    match active.next().await {
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
                        Some(Err(error)) if retryable(&error) => cursor.active = None,
                        Some(Err(error)) => return Some((Err(status(error)), cursor)),
                        None if cursor.remaining.is_none() => cursor.active = None,
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
    active: Option<tonic::Streaming<wire::ReadResponse>>,
}

impl RecordCursor {
    async fn open(&self) -> Result<tonic::Streaming<wire::ReadResponse>, StreamError> {
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
        } else {
            self.client
                .unary(
                    wire::FollowRequest {
                        path: self.path.to_string(),
                        from: self.next,
                    },
                    |mut service, request| Box::pin(async move { service.follow(request).await }),
                )
                .await
        }
    }
}

fn retryable(error: &Status) -> bool {
    matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded)
}

#[async_trait]
impl StreamProvider for Client {
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
        let response = self
            .unary(
                wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request.records,
                    if_tail: request.if_tail,
                    idempotency_key: request
                        .idempotency_key
                        .map(|key| Bytes::copy_from_slice(key.as_bytes())),
                },
                |mut service, request| Box::pin(async move { service.append(request).await }),
            )
            .await?;
        match response.outcome.ok_or(StreamError::Unavailable)? {
            wire::append_response::Outcome::Committed(receipt) => {
                Ok(AppendOutcome::Committed(append_receipt(receipt)?))
            }
            wire::append_response::Outcome::Conflict(conflict) => Ok(AppendOutcome::TailConflict {
                actual_tail: conflict.actual_tail,
            }),
        }
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let receipt = self
            .unary(
                wire::ForkRequest {
                    source: request.source.to_string(),
                    destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: request
                        .idempotency_key
                        .map(|key| Bytes::copy_from_slice(key.as_bytes())),
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
        match response.outcome.ok_or(StreamError::Unavailable)? {
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
        let outcome = match outcome {
            AppendOutcome::Committed(receipt) => {
                wire::append_response::Outcome::Committed(append_receipt_wire(receipt))
            }
            AppendOutcome::TailConflict { actual_tail } => {
                wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
            }
        };
        Ok(Response::new(wire::AppendResponse {
            outcome: Some(outcome),
        }))
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
        let outcome = match outcome {
            CommitOutcome::Committed(envelope) => {
                wire::commit_response::Outcome::Committed(envelope_wire(envelope))
            }
            CommitOutcome::Conflict(conflicts) => {
                wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                    conflicts: conflicts.into_iter().map(conflict_wire).collect(),
                })
            }
        };
        Ok(Response::new(wire::CommitResponse {
            outcome: Some(outcome),
        }))
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

fn path(value: String) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
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

fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
    value.map(IdempotencyKey::new).transpose()
}

fn required_key(value: Option<Bytes>) -> Result<IdempotencyKey, StreamError> {
    value
        .ok_or(StreamError::InvalidArgument)
        .and_then(IdempotencyKey::new)
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

fn condition_wire(value: CommitCondition) -> wire::CommitCondition {
    let condition = match value {
        CommitCondition::Tail { path, expected } => {
            wire::commit_condition::Condition::Tail(wire::TailCondition {
                path: path.to_string(),
                expected,
            })
        }
        CommitCondition::Absent { path } => {
            wire::commit_condition::Condition::Absent(wire::AbsentCondition {
                path: path.to_string(),
            })
        }
    };
    wire::CommitCondition {
        condition: Some(condition),
    }
}

fn condition_from_wire(value: wire::CommitCondition) -> Result<CommitCondition, StreamError> {
    match value.condition.ok_or(StreamError::InvalidArgument)? {
        wire::commit_condition::Condition::Tail(value) => Ok(CommitCondition::Tail {
            path: path(value.path)?,
            expected: value.expected,
        }),
        wire::commit_condition::Condition::Absent(value) => Ok(CommitCondition::Absent {
            path: path(value.path)?,
        }),
    }
}

fn mutation_wire(value: CommitMutation) -> wire::CommitMutation {
    let mutation = match value {
        CommitMutation::Append { path, records } => {
            wire::commit_mutation::Mutation::Append(wire::AppendMutation {
                path: path.to_string(),
                records,
            })
        }
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
        } => wire::commit_mutation::Mutation::Fork(wire::ForkMutation {
            source: source.to_string(),
            destination: destination.to_string(),
            at_tail,
        }),
        CommitMutation::Trim { path, before } => {
            wire::commit_mutation::Mutation::Trim(wire::TrimMutation {
                path: path.to_string(),
                before,
            })
        }
        CommitMutation::Delete { path } => {
            wire::commit_mutation::Mutation::Delete(wire::DeleteMutation {
                path: path.to_string(),
            })
        }
    };
    wire::CommitMutation {
        mutation: Some(mutation),
    }
}

fn mutation_from_wire(value: wire::CommitMutation) -> Result<CommitMutation, StreamError> {
    match value.mutation.ok_or(StreamError::InvalidArgument)? {
        wire::commit_mutation::Mutation::Append(value) => Ok(CommitMutation::Append {
            path: path(value.path)?,
            records: value.records,
        }),
        wire::commit_mutation::Mutation::Fork(value) => Ok(CommitMutation::Fork {
            source: path(value.source)?,
            destination: path(value.destination)?,
            at_tail: value.at_tail,
        }),
        wire::commit_mutation::Mutation::Trim(value) => Ok(CommitMutation::Trim {
            path: path(value.path)?,
            before: value.before,
        }),
        wire::commit_mutation::Mutation::Delete(value) => Ok(CommitMutation::Delete {
            path: path(value.path)?,
        }),
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
    use wire::stream_service_server::StreamService;

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
