//! N-API bridge for the canonical Rust Stream gRPC client.
//!
//! Protobuf request and response bytes cross the JavaScript boundary. The
//! bridge owns no transport or retry policy: endpoint failover, cursor
//! recovery, idempotency, and domain validation remain in
//! `acyclic_stream::grpc::Client` and `StreamClient`.

use std::{future::Future, sync::Arc};

use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildrenPageRequest, ChildrenRequest, CommitCondition,
    CommitConflict, CommitId, CommitMutation, CommitOutcome, CommittedEnvelope, CommittedMutation,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome,
    ReadRequest, Record, StreamClient, StreamError, StreamPath, grpc, wire,
};
use bytes::Bytes;
use futures::StreamExt;
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi_derive::napi;
use prost::Message;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Structured Rust-owned error metadata shared by connection, operation, and
/// follow results.
#[napi(object)]
#[derive(Clone, Default)]
pub struct NativeStreamErrorMetadata {
    /// Stable Stream error code.
    pub code: String,
    /// Rust-owned diagnostic message.
    pub message: String,
}

/// Typed unary operation result. Exactly one of `value` and `error` is set.
#[napi(object)]
pub struct NativeStreamOperationResult {
    /// Canonical protobuf response bytes on success.
    pub value: Option<Buffer>,
    /// Structured Rust error on failure.
    pub error: Option<NativeStreamErrorMetadata>,
}

impl NativeStreamOperationResult {
    fn success(value: Buffer) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    fn failure(error: NativeStreamErrorMetadata) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }
}

/// Typed connection result used by generated wrappers.
#[napi(object, object_from_js = false)]
pub struct NativeStreamConnectResult {
    /// Connected Rust client on success.
    pub client: Option<NativeStreamClient>,
    /// Structured Rust error on failure.
    pub error: Option<NativeStreamErrorMetadata>,
}

impl NativeStreamConnectResult {
    fn success(client: NativeStreamClient) -> Self {
        Self {
            client: Some(client),
            error: None,
        }
    }

    fn failure(error: NativeStreamErrorMetadata) -> Self {
        Self {
            client: None,
            error: Some(error),
        }
    }
}

/// Result of opening a Rust-owned follow cursor.
#[napi(object, object_from_js = false)]
pub struct NativeStreamFollowResult {
    /// Follow cursor on success.
    pub follow: Option<NativeStreamFollow>,
    /// Structured Rust error on failure.
    pub error: Option<NativeStreamErrorMetadata>,
}

impl NativeStreamFollowResult {
    fn success(follow: NativeStreamFollow) -> Self {
        Self {
            follow: Some(follow),
            error: None,
        }
    }

    fn failure(error: NativeStreamErrorMetadata) -> Self {
        Self {
            follow: None,
            error: Some(error),
        }
    }
}

/// Result of one cancellable follow read.
#[napi(object)]
pub struct NativeStreamNextResult {
    /// Encoded `ReadResponse` when a record was available.
    pub value: Option<Buffer>,
    /// Structured Rust error on failure.
    pub error: Option<NativeStreamErrorMetadata>,
}

/// Typed result for bounded stream or child-list operations. Exactly one of
/// `values` and `error` is populated at the operation level; an empty values
/// vector is a successful empty result.
#[napi(object)]
pub struct NativeStreamSequenceResult {
    /// Encoded protobuf response messages on success.
    pub values: Vec<Buffer>,
    /// Structured Rust error on failure.
    pub error: Option<NativeStreamErrorMetadata>,
}

impl NativeStreamSequenceResult {
    fn success(values: Vec<Buffer>) -> Self {
        Self {
            values,
            error: None,
        }
    }

    fn failure(error: NativeStreamErrorMetadata) -> Self {
        Self {
            values: Vec::new(),
            error: Some(error),
        }
    }
}

impl NativeStreamNextResult {
    fn value(value: Buffer) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    fn end() -> Self {
        Self {
            value: None,
            error: None,
        }
    }

    fn failure(error: NativeStreamErrorMetadata) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }
}

fn stream_error(error: StreamError) -> NativeStreamErrorMetadata {
    let code = match &error {
        StreamError::InvalidPath => "invalid_path",
        StreamError::InvalidArgument => "invalid_argument",
        StreamError::LimitExceeded => "limit_exceeded",
        StreamError::NotFound => "not_found",
        StreamError::AlreadyExists => "already_exists",
        StreamError::PrefixNotRetained => "prefix_not_retained",
        StreamError::OutOfRange => "out_of_range",
        StreamError::HierarchyChanged => "hierarchy_changed",
        StreamError::IdempotencyMismatch => "idempotency_mismatch",
        StreamError::Capacity => "capacity",
        StreamError::AccessDenied => "access_denied",
        StreamError::Unavailable => "unavailable",
        StreamError::DeadlineElapsed => "deadline_elapsed",
        StreamError::Unsupported => "unsupported",
    };
    NativeStreamErrorMetadata {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

fn connect_error(error: grpc::ConnectError) -> NativeStreamErrorMetadata {
    let code = match &error {
        grpc::ConnectError::Endpoint(_) => "transport",
        grpc::ConnectError::InsecureEndpoint
        | grpc::ConnectError::InvalidCredential
        | grpc::ConnectError::InvalidCaCertificate
        | grpc::ConnectError::NoEndpoints
        | grpc::ConnectError::EndpointLimit => "configuration",
    };
    NativeStreamErrorMetadata {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

fn napi_error(error: NativeStreamErrorMetadata) -> Error {
    Error::new(
        Status::GenericFailure,
        format!("{}: {}", error.code, error.message),
    )
}

fn decode<T: Message + Default>(
    request: &Buffer,
    operation: &str,
) -> Result<T, NativeStreamErrorMetadata> {
    T::decode(request.as_ref()).map_err(|error| NativeStreamErrorMetadata {
        code: "invalid_argument".to_owned(),
        message: format!("{operation} request is not valid Stream protobuf: {error}"),
    })
}

fn encode<T: Message>(value: &T, operation: &str) -> Result<Buffer, NativeStreamErrorMetadata> {
    let mut bytes = Vec::with_capacity(value.encoded_len());
    value
        .encode(&mut bytes)
        .map_err(|error| NativeStreamErrorMetadata {
            code: "unavailable".to_owned(),
            message: format!("{operation} response could not be encoded: {error}"),
        })?;
    Ok(Buffer::from(bytes))
}

fn cancellation_state(value: Option<&NativeStreamCancellation>) -> Option<CancellationToken> {
    value.map(|value| value.token.clone())
}

async fn run_with_cancellation<T, F>(
    operation: F,
    cancellation: Option<CancellationToken>,
) -> std::result::Result<T, NativeStreamErrorMetadata>
where
    F: Future<Output = std::result::Result<T, NativeStreamErrorMetadata>>,
{
    let Some(cancellation) = cancellation else {
        return operation.await;
    };
    if cancellation.is_cancelled() {
        return Err(NativeStreamErrorMetadata {
            code: "cancelled".to_owned(),
            message: "Stream operation cancelled".to_owned(),
        });
    }
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(NativeStreamErrorMetadata {
            code: "cancelled".to_owned(),
            message: "Stream operation cancelled".to_owned(),
        }),
        result = operation => result,
    }
}

async fn operation_result<F>(operation: F) -> Result<NativeStreamOperationResult>
where
    F: Future<Output = std::result::Result<Buffer, NativeStreamErrorMetadata>>,
{
    Ok(match operation.await {
        Ok(value) => NativeStreamOperationResult::success(value),
        Err(error) => NativeStreamOperationResult::failure(error),
    })
}

#[derive(Clone)]
struct FollowState {
    records: Arc<Mutex<Option<acyclic_stream::RecordStream>>>,
    token: CancellationToken,
}

/// Monotonic cancellation handle for native Stream calls.
#[napi]
pub struct NativeStreamCancellation {
    token: CancellationToken,
}

impl Default for NativeStreamCancellation {
    fn default() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }
}

#[napi]
impl NativeStreamCancellation {
    /// Creates a non-cancelled handle. A cancelled handle stays cancelled.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels attached operations and wakes pending follow reads.
    #[napi]
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Whether cancellation has been requested.
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// One Rust-owned live follow cursor. A fresh cursor is required after close.
#[napi]
pub struct NativeStreamFollow {
    state: FollowState,
}

#[napi]
impl NativeStreamFollow {
    /// Returns the next canonical `ReadResponse`, or an empty result at end/close.
    #[napi]
    pub async fn next_result(&self) -> Result<NativeStreamNextResult> {
        let Some(mut records) = self.state.records.lock().await.take() else {
            return Ok(NativeStreamNextResult::end());
        };
        let next = run_with_cancellation(
            async { records.next().await.transpose().map_err(stream_error) },
            Some(self.state.token.clone()),
        )
        .await;
        match next {
            Ok(Some(record)) => {
                let response = wire::ReadResponse {
                    record: Some(record_wire(record)),
                };
                self.state.records.lock().await.replace(records);
                Ok(NativeStreamNextResult::value(
                    encode(&response, "follow").map_err(napi_error)?,
                ))
            }
            Ok(None) => Ok(NativeStreamNextResult::end()),
            Err(error) => {
                // `RecordStream` is a recovery-aware stream: a transient item
                // error is yielded together with its cursor so the next poll
                // can retry the active endpoint. Keep that cursor alive unless
                // this handle was explicitly cancelled.
                if !self.state.token.is_cancelled() {
                    self.state.records.lock().await.replace(records);
                }
                Ok(NativeStreamNextResult::failure(error))
            }
        }
    }

    /// Cancels this cursor and releases its transport stream.
    #[napi]
    pub fn close(&self) {
        self.state.token.cancel();
        if let Ok(mut records) = self.state.records.try_lock() {
            records.take();
        }
    }
}

/// Native Stream client backed directly by `StreamClient<grpc::Client>`.
#[napi]
pub struct NativeStreamClient {
    inner: Arc<StreamClient<grpc::Client>>,
}

impl NativeStreamClient {
    fn from_client(inner: StreamClient<grpc::Client>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[napi]
impl NativeStreamClient {
    /// Connects using the canonical Rust native gRPC transport.
    #[napi(factory)]
    pub async fn connect(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<Self> {
        let result = run_with_cancellation(
            async {
                StreamClient::<grpc::Client>::connect(&endpoint, &token)
                    .await
                    .map_err(connect_error)
            },
            cancellation_state(cancellation),
        )
        .await
        .map_err(napi_error)?;
        Ok(Self::from_client(result))
    }

    /// Connects and returns a structured success/error envelope.
    #[napi(js_name = "connectResult")]
    pub async fn connect_result(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamConnectResult> {
        let result = run_with_cancellation(
            async {
                StreamClient::<grpc::Client>::connect(&endpoint, &token)
                    .await
                    .map_err(connect_error)
            },
            cancellation_state(cancellation),
        )
        .await;
        Ok(match result {
            Ok(client) => NativeStreamConnectResult::success(Self::from_client(client)),
            Err(error) => NativeStreamConnectResult::failure(error),
        })
    }

    /// Connects with one caller-pinned private CA certificate.
    #[napi(factory, js_name = "connectWithCa")]
    pub async fn connect_with_ca(
        endpoint: String,
        token: String,
        ca_certificate_pem: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<Self> {
        let ca = ca_certificate_pem.to_vec();
        let result = run_with_cancellation(
            async {
                StreamClient::<grpc::Client>::connect_with_ca_certificate(&endpoint, &token, &ca)
                    .await
                    .map_err(connect_error)
            },
            cancellation_state(cancellation),
        )
        .await
        .map_err(napi_error)?;
        Ok(Self::from_client(result))
    }

    /// Connects with a pinned CA and returns a structured result.
    #[napi(js_name = "connectWithCaResult")]
    pub async fn connect_with_ca_result(
        endpoint: String,
        token: String,
        ca_certificate_pem: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamConnectResult> {
        let ca = ca_certificate_pem.to_vec();
        let result = run_with_cancellation(
            async {
                StreamClient::<grpc::Client>::connect_with_ca_certificate(&endpoint, &token, &ca)
                    .await
                    .map_err(connect_error)
            },
            cancellation_state(cancellation),
        )
        .await;
        Ok(match result {
            Ok(client) => NativeStreamConnectResult::success(Self::from_client(client)),
            Err(error) => NativeStreamConnectResult::failure(error),
        })
    }

    /// Returns the native bridge package version.
    #[napi]
    pub fn version() -> String {
        PACKAGE_VERSION.to_owned()
    }

    /// Returns the transport selected by this native client.
    #[napi]
    pub fn transport(&self) -> String {
        "grpc".to_owned()
    }

    /// Executes a canonical idempotency inspection request.
    #[napi(js_name = "inspectIdempotencyResult")]
    pub async fn inspect_idempotency_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(inspect_idempotency(&self.inner, request, cancellation)).await
    }

    /// Executes a canonical tail request.
    #[napi(js_name = "tailResult")]
    pub async fn tail_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(tail(&self.inner, request, cancellation)).await
    }

    /// Observes the canonical atomic replay bound for one stream path.
    #[napi(js_name = "boundsResult")]
    pub async fn bounds_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(bounds(&self.inner, request, cancellation)).await
    }

    /// Executes a canonical append request.
    #[napi(js_name = "appendResult")]
    pub async fn append_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(append(&self.inner, request, cancellation)).await
    }

    /// Executes a canonical fork request.
    #[napi(js_name = "forkResult")]
    pub async fn fork_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(fork(&self.inner, request, cancellation)).await
    }

    /// Reads one bounded page as encoded `ReadResponse` values.
    #[napi(js_name = "readResult")]
    pub async fn read_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamSequenceResult> {
        Ok(match read(&self.inner, request, cancellation).await {
            Ok(values) => NativeStreamSequenceResult::success(values),
            Err(error) => NativeStreamSequenceResult::failure(error),
        })
    }

    /// Opens a Rust-owned follow cursor from an encoded request.
    #[napi(js_name = "openFollowResult")]
    pub async fn open_follow_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamFollowResult> {
        Ok(
            match open_follow(&self.inner, request, cancellation).await {
                Ok(follow) => NativeStreamFollowResult::success(follow),
                Err(error) => NativeStreamFollowResult::failure(error),
            },
        )
    }

    /// Executes a canonical children-page request.
    #[napi(js_name = "childrenPageResult")]
    pub async fn children_page_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(children_page(&self.inner, request, cancellation)).await
    }

    /// Executes a canonical commit request, retaining an optional provider deadline.
    #[napi(js_name = "commitResult")]
    pub async fn commit_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(commit(&self.inner, request, cancellation)).await
    }

    /// Reads one committed envelope by its canonical identity.
    #[napi(js_name = "readCommitResult")]
    pub async fn read_commit_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(read_commit(&self.inner, request, cancellation)).await
    }

    /// Reads all children from one fixed snapshot as encoded responses.
    #[napi(js_name = "childrenResult")]
    pub async fn children_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamSequenceResult> {
        Ok(match children(&self.inner, request, cancellation).await {
            Ok(values) => NativeStreamSequenceResult::success(values),
            Err(error) => NativeStreamSequenceResult::failure(error),
        })
    }
}

fn path(value: String) -> std::result::Result<StreamPath, NativeStreamErrorMetadata> {
    StreamPath::new(value).map_err(stream_error)
}

fn key(value: Bytes) -> std::result::Result<IdempotencyKey, NativeStreamErrorMetadata> {
    IdempotencyKey::new(value).map_err(stream_error)
}

fn commit_id(value: &[u8]) -> std::result::Result<CommitId, NativeStreamErrorMetadata> {
    let value = <[u8; 32]>::try_from(value).map_err(|_| NativeStreamErrorMetadata {
        code: "invalid_argument".to_owned(),
        message: "commit identity must contain exactly 32 bytes".to_owned(),
    })?;
    Ok(CommitId::from_bytes(value))
}

fn append_request(
    value: wire::AppendRequest,
) -> std::result::Result<AppendRequest, NativeStreamErrorMetadata> {
    Ok(AppendRequest {
        path: path(value.path)?,
        records: value.records,
        if_tail: value.if_tail,
        idempotency_key: value.idempotency_key.map(key).transpose()?,
    })
}

fn fork_request(
    value: wire::ForkRequest,
) -> std::result::Result<ForkRequest, NativeStreamErrorMetadata> {
    Ok(ForkRequest {
        source: path(value.source)?,
        destination: path(value.destination)?,
        at_tail: value.at_tail,
        idempotency_key: value.idempotency_key.map(key).transpose()?,
    })
}

fn read_request(
    value: wire::ReadRequest,
) -> std::result::Result<ReadRequest, NativeStreamErrorMetadata> {
    Ok(ReadRequest {
        path: path(value.path)?,
        from: value.from,
        limit: value.limit,
    })
}

fn children_request(
    value: wire::ChildrenRequest,
) -> std::result::Result<ChildrenRequest, NativeStreamErrorMetadata> {
    Ok(ChildrenRequest {
        parent: value.parent.map(path).transpose()?,
        limit: value.limit,
    })
}

fn children_page_request(
    value: wire::ChildrenPageRequest,
) -> std::result::Result<ChildrenPageRequest, NativeStreamErrorMetadata> {
    Ok(ChildrenPageRequest {
        parent: value.parent.map(path).transpose()?,
        after: value.after.map(path).transpose()?,
        hierarchy_version: value
            .hierarchy_version
            .as_deref()
            .map(commit_id)
            .transpose()?,
        limit: value.limit,
    })
}

fn condition(
    value: wire::CommitCondition,
) -> std::result::Result<CommitCondition, NativeStreamErrorMetadata> {
    match value.condition.ok_or_else(|| NativeStreamErrorMetadata {
        code: "invalid_argument".to_owned(),
        message: "commit condition is missing".to_owned(),
    })? {
        wire::commit_condition::Condition::Tail(value) => Ok(CommitCondition::Tail {
            path: path(value.path)?,
            expected: value.expected,
        }),
        wire::commit_condition::Condition::Absent(value) => Ok(CommitCondition::Absent {
            path: path(value.path)?,
        }),
    }
}

fn mutation(
    value: wire::CommitMutation,
) -> std::result::Result<CommitMutation, NativeStreamErrorMetadata> {
    match value.mutation.ok_or_else(|| NativeStreamErrorMetadata {
        code: "invalid_argument".to_owned(),
        message: "commit mutation is missing".to_owned(),
    })? {
        wire::commit_mutation::Mutation::Append(value) => Ok(CommitMutation::Append {
            path: path(value.path)?,
            records: value.records,
        }),
        wire::commit_mutation::Mutation::Fork(value) => Ok(CommitMutation::Fork {
            source: path(value.source)?,
            destination: path(value.destination)?,
            at_tail: value.at_tail,
            records: value.records,
        }),
    }
}

fn commit_request(
    value: wire::CommitRequest,
) -> std::result::Result<(acyclic_stream::CommitRequest, Option<u64>), NativeStreamErrorMetadata> {
    Ok((
        acyclic_stream::CommitRequest {
            conditions: value
                .conditions
                .into_iter()
                .map(condition)
                .collect::<std::result::Result<_, _>>()?,
            mutations: value
                .mutations
                .into_iter()
                .map(mutation)
                .collect::<std::result::Result<_, _>>()?,
            idempotency_key: key(value.idempotency_key)?,
        },
        value.deadline_unix_millis,
    ))
}

fn record_wire(value: Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
    }
}

fn append_response(value: AppendOutcome) -> wire::AppendResponse {
    let outcome = match value {
        AppendOutcome::Committed(value) => {
            wire::append_response::Outcome::Committed(wire::AppendReceipt {
                start: value.start,
                end: value.end,
                tail: value.tail,
                commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
            })
        }
        AppendOutcome::TailConflict { actual_tail } => {
            wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
        }
    };
    wire::AppendResponse {
        outcome: Some(outcome),
    }
}

fn fork_receipt(value: ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

fn committed_mutation(value: CommittedMutation) -> wire::CommittedMutation {
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
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
    };
    wire::CommittedMutation {
        mutation: Some(mutation),
    }
}

fn envelope(value: CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation)
            .collect(),
    }
}

fn commit_response(value: CommitOutcome) -> wire::CommitResponse {
    let outcome = match value {
        CommitOutcome::Committed(value) => {
            wire::commit_response::Outcome::Committed(envelope(value))
        }
        CommitOutcome::Conflict(values) => {
            wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                conflicts: values
                    .into_iter()
                    .map(|value| {
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
                                wire::commit_conflict::Conflict::Exists(
                                    wire::ExistsCommitConflict {
                                        path: path.to_string(),
                                    },
                                )
                            }
                        };
                        wire::CommitConflict {
                            conflict: Some(conflict),
                        }
                    })
                    .collect(),
            })
        }
    };
    wire::CommitResponse {
        outcome: Some(outcome),
    }
}

fn observation(value: IdempotencyObservation) -> wire::IdempotencyObservation {
    let outcome = match value.outcome {
        IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_response(value))
        }
        IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt(value))
        }
        IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_response(value))
        }
    };
    wire::IdempotencyObservation {
        idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
        request_digest: Bytes::copy_from_slice(&value.request_digest),
        outcome: Some(outcome),
    }
}

type Client = Arc<StreamClient<grpc::Client>>;

async fn inspect_idempotency(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::InspectIdempotencyRequest>(&request, "inspect_idempotency")?;
    let key = key(request.idempotency_key)?;
    let value = run_with_cancellation(
        async { client.inspect_idempotency(key).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire::InspectIdempotencyResponse {
            observation: value.map(observation),
        },
        "inspect_idempotency",
    )
}

async fn tail(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::TailRequest>(&request, "tail")?;
    let stream = client.stream(request.path).map_err(stream_error)?;
    let value = run_with_cancellation(
        async { stream.tail().await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire::TailResponse { tail: value }, "tail")
}

async fn bounds(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::TailRequest>(&request, "bounds")?;
    let value = run_with_cancellation(
        async { client.bounds(&request.path).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire::TailResponse { tail: value.tail }, "bounds")
}

async fn append(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = append_request(decode::<wire::AppendRequest>(&request, "append")?)?;
    let stream = client
        .stream(request.path.to_string())
        .map_err(stream_error)?;
    let value = run_with_cancellation(
        async move {
            stream
                .append_batch(request.records, request.if_tail, request.idempotency_key)
                .await
                .map_err(stream_error)
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&append_response(value), "append")
}

async fn fork(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = fork_request(decode::<wire::ForkRequest>(&request, "fork")?)?;
    let stream = client
        .stream(request.source.to_string())
        .map_err(stream_error)?;
    let value = run_with_cancellation(
        async move {
            stream
                .fork(
                    request.destination,
                    request.at_tail,
                    request.idempotency_key,
                )
                .await
                .map_err(stream_error)
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&fork_receipt(value), "fork")
}

async fn read(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request = read_request(decode::<wire::ReadRequest>(&request, "read")?)?;
    let stream = client
        .stream(request.path.to_string())
        .map_err(stream_error)?;
    let token = cancellation_state(cancellation);
    let mut records = stream.read(request.from, request.limit);
    let mut records =
        run_with_cancellation(async { records.await.map_err(stream_error) }, token.clone()).await?;
    let mut output = Vec::new();
    while let Some(value) = run_with_cancellation(
        async { records.next().await.transpose().map_err(stream_error) },
        token.clone(),
    )
    .await?
    {
        output.push(encode(
            &wire::ReadResponse {
                record: Some(record_wire(value)),
            },
            "read",
        )?);
    }
    Ok(output)
}

async fn open_follow(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request = decode::<wire::FollowRequest>(&request, "follow")?;
    let stream = client.stream(request.path).map_err(stream_error)?;
    let records = run_with_cancellation(
        async move { stream.follow(request.from).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(records))),
            token: cancellation_state(cancellation).unwrap_or_default(),
        },
    })
}

async fn children(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request = children_request(decode::<wire::ChildrenRequest>(&request, "children")?)?;
    let parent = request.parent.as_ref().map(ToString::to_string);
    let token = cancellation_state(cancellation);
    let mut values = run_with_cancellation(
        async {
            client
                .children(parent.as_deref(), request.limit)
                .await
                .map_err(stream_error)
        },
        token.clone(),
    )
    .await?;
    let mut output = Vec::new();
    while let Some(value) = run_with_cancellation(
        async { values.next().await.transpose().map_err(stream_error) },
        token.clone(),
    )
    .await?
    {
        output.push(encode(
            &wire::ChildrenResponse {
                child: Some(wire::Child {
                    path: value.path.to_string(),
                }),
            },
            "children",
        )?);
    }
    Ok(output)
}

async fn children_page(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = children_page_request(decode::<wire::ChildrenPageRequest>(
        &request,
        "children_page",
    )?)?;
    let value = run_with_cancellation(
        async { client.children_page(request).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire::ChildrenPageResponse {
            hierarchy_version: Bytes::copy_from_slice(value.hierarchy_version.as_bytes()),
            children: value
                .children
                .into_iter()
                .map(|value| wire::Child {
                    path: value.path.to_string(),
                })
                .collect(),
            next_after: value.next_after.map(|value| value.to_string()),
        },
        "children_page",
    )
}

async fn commit(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let (request, deadline) = commit_request(decode::<wire::CommitRequest>(&request, "commit")?)?;
    let value = run_with_cancellation(
        async {
            match deadline {
                Some(deadline) => client.commit_before(request, deadline).await,
                None => client.commit(request).await,
            }
            .map_err(stream_error)
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&commit_response(value), "commit")
}

async fn read_commit(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::ReadCommitRequest>(&request, "read_commit")?;
    let commit_id = commit_id(&request.commit_id)?;
    let value = run_with_cancellation(
        async { client.read_commit(commit_id).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&envelope(value), "read_commit")
}
