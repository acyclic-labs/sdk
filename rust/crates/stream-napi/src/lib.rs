//! N-API bridge for the canonical Rust Stream gRPC client.
//!
//! Protobuf request and response bytes cross the JavaScript boundary. The
//! bridge owns no transport or retry policy: endpoint failover, cursor
//! recovery, idempotency, and domain validation remain in
//! `acyclic_stream::grpc::Client` and `StreamClient`.

use std::{future::Future, sync::Arc};

use acyclic_stream::{StreamClient, StreamError, StreamPath, grpc, wire, wire_codec};
use futures::StreamExt;
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi_derive::napi;
use prost::Message;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Structured Rust-owned error metadata shared by connection, operation, and
/// follow results.
#[napi(object)]
#[derive(Clone, Default, Serialize)]
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
    let reason = match serde_json::to_string(&error) {
        Ok(reason) => reason,
        Err(_) => String::from(
            r#"{"code":"internal","message":"failed to encode Stream error metadata"}"#,
        ),
    };
    Error::new(Status::GenericFailure, reason)
}

fn decode<T: Message + Default>(
    request: &Buffer,
    operation: &str,
) -> std::result::Result<T, NativeStreamErrorMetadata> {
    T::decode(request.as_ref()).map_err(|error| NativeStreamErrorMetadata {
        code: "invalid_argument".to_owned(),
        message: format!("{operation} request is not valid Stream protobuf: {error}"),
    })
}

fn encode<T: Message>(
    value: &T,
    operation: &str,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
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
                    record: Some(wire_codec::record_wire(record)),
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

    /// Requests cancellation of this cursor.
    ///
    /// A pending `nextResult` call is woken by the cancellation token and
    /// releases its transport stream when that call returns. A cursor cannot
    /// be reopened after it has been cancelled.
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

async fn connect_native(
    endpoint: String,
    token: String,
    ca_certificate_pem: Option<Vec<u8>>,
    cancellation: Option<CancellationToken>,
) -> std::result::Result<StreamClient<grpc::Client>, NativeStreamErrorMetadata> {
    run_with_cancellation(
        async move {
            match ca_certificate_pem {
                Some(ca_certificate_pem) => {
                    StreamClient::<grpc::Client>::connect_with_ca_certificate(
                        &endpoint,
                        &token,
                        &ca_certificate_pem,
                    )
                    .await
                }
                None => StreamClient::<grpc::Client>::connect(&endpoint, &token).await,
            }
            .map_err(connect_error)
        },
        cancellation,
    )
    .await
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
        let result = connect_native(endpoint, token, None, cancellation_state(cancellation))
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
        let result = connect_native(endpoint, token, None, cancellation_state(cancellation)).await;
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
        let result = connect_native(
            endpoint,
            token,
            Some(ca_certificate_pem.to_vec()),
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
        let result = connect_native(
            endpoint,
            token,
            Some(ca_certificate_pem.to_vec()),
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

type Client = Arc<StreamClient<grpc::Client>>;

async fn inspect_idempotency(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::InspectIdempotencyRequest>(&request, "inspect_idempotency")?;
    let key = wire_codec::required_key(Some(request.idempotency_key)).map_err(stream_error)?;
    let value = run_with_cancellation(
        async { client.inspect_idempotency(key).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire::InspectIdempotencyResponse {
            observation: value.map(wire_codec::observation_wire),
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
    let request = wire_codec::append_from_wire(
        decode::<wire::AppendRequest>(&request, "append")?,
    )
    .map_err(stream_error)?;
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
    encode(&wire_codec::append_outcome_to_wire(value), "append")
}

async fn fork(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::fork_from_wire(
        decode::<wire::ForkRequest>(&request, "fork")?,
    )
    .map_err(stream_error)?;
    let stream = client
        .stream(request.source.to_string())
        .map_err(stream_error)?;
    let value = run_with_cancellation(
        async move {
            stream
                .fork(
                    request.destination.to_string(),
                    request.at_tail,
                    request.idempotency_key,
                )
                .await
                .map_err(stream_error)
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::fork_receipt_to_wire(&value), "fork")
}

async fn read(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request = wire_codec::read_from_wire(
        decode::<wire::ReadRequest>(&request, "read")?,
    )
    .map_err(stream_error)?;
    let stream = client
        .stream(request.path.to_string())
        .map_err(stream_error)?;
    let token = cancellation_state(cancellation);
    let records = stream.read(request.from, request.limit);
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
                record: Some(wire_codec::record_wire(value)),
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
    let request = wire_codec::children_from_wire(
        decode::<wire::ChildrenRequest>(&request, "children")?,
    )
    .map_err(stream_error)?;
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
    let request = wire_codec::children_page_from_wire(
        decode::<wire::ChildrenPageRequest>(&request, "children_page")?,
    )
    .map_err(stream_error)?;
    let parent = request.parent.as_ref().map(StreamPath::as_str);
    let after = request.after.as_ref().map(StreamPath::as_str);
    let value = run_with_cancellation(
        async {
            client
                .children_page(parent, after, request.hierarchy_version, request.limit)
                .await
                .map_err(stream_error)
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire_codec::children_page_to_wire(value),
        "children_page",
    )
}

async fn commit(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::CommitRequest>(&request, "commit")?;
    let deadline = request.deadline_unix_millis;
    let request = wire_codec::commit_from_wire(request).map_err(stream_error)?;
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
    encode(&wire_codec::commit_outcome_to_wire(value), "commit")
}

async fn read_commit(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::ReadCommitRequest>(&request, "read_commit")?;
    let commit_id = wire_codec::commit_id(&request.commit_id).map_err(stream_error)?;
    let value = run_with_cancellation(
        async { client.read_commit(commit_id).await.map_err(stream_error) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::envelope_to_wire(value), "read_commit")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;

    fn cancellation_error(
        result: std::result::Result<(), NativeStreamErrorMetadata>,
    ) -> NativeStreamErrorMetadata {
        match result {
            Err(error) => error,
            Ok(()) => panic!("cancellation helper unexpectedly completed the operation"),
        }
    }

    #[tokio::test]
    async fn cancellation_is_checked_before_polling_operation() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let polled = Arc::new(AtomicBool::new(false));
        let operation_polled = Arc::clone(&polled);
        let operation = std::future::poll_fn(move |_| {
            operation_polled.store(true, Ordering::SeqCst);
            Poll::Ready(Ok::<(), NativeStreamErrorMetadata>(()))
        });

        let error = cancellation_error(run_with_cancellation(operation, Some(cancellation)).await);
        assert_eq!(error.code, "cancelled");
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn pending_operation_is_interrupted_by_cancellation() {
        let cancellation = CancellationToken::new();
        let polled = Arc::new(AtomicBool::new(false));
        let operation_polled = Arc::clone(&polled);
        let operation = std::future::poll_fn(move |_| {
            operation_polled.store(true, Ordering::SeqCst);
            Poll::Pending::<std::result::Result<(), NativeStreamErrorMetadata>>
        });
        let pending = tokio::spawn(run_with_cancellation(operation, Some(cancellation.clone())));

        while !polled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();

        let error = cancellation_error(pending.await.expect("cancellation task panicked"));
        assert_eq!(error.code, "cancelled");
    }

    #[tokio::test]
    async fn ready_operation_finishes_if_cancellation_arrives_during_its_poll() {
        let cancellation = CancellationToken::new();
        let first_poll = Arc::new(AtomicBool::new(false));
        let operation_first_poll = Arc::clone(&first_poll);
        let operation_cancellation = cancellation.clone();
        let operation = std::future::poll_fn(move |_| {
            if !operation_first_poll.swap(true, Ordering::SeqCst) {
                // The cancellation branch was already observed as pending
                // for this select poll. Cancellation cannot preempt an
                // operation that returns Ready from its current poll.
                operation_cancellation.cancel();
            }
            Poll::Ready(Ok::<(), NativeStreamErrorMetadata>(()))
        });

        let result = run_with_cancellation(operation, Some(cancellation)).await;
        assert!(result.is_ok());
        assert!(first_poll.load(Ordering::SeqCst));
    }
}
