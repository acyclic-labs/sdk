//! N-API bridge for the canonical Rust Stream gRPC client.
//!
//! Protobuf request and response bytes cross the JavaScript boundary. The
//! bridge owns no transport or retry policy: endpoint failover, cursor
//! recovery, idempotency, and domain validation remain in
//! `acyclic_stream::grpc::Client` and `StreamClient`.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

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

fn stream_error(error: &StreamError) -> NativeStreamErrorMetadata {
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

fn connect_error(error: &grpc::ConnectError) -> NativeStreamErrorMetadata {
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

fn napi_error(error: &NativeStreamErrorMetadata) -> Error {
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
    closed: Arc<AtomicBool>,
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
        if self.state.closed.load(Ordering::Acquire) {
            return Ok(NativeStreamNextResult::end());
        }
        let mut records = self.state.records.lock().await;
        if self.state.closed.load(Ordering::Acquire) {
            records.take();
            return Ok(NativeStreamNextResult::end());
        }
        let Some(stream) = records.as_mut() else {
            return Ok(NativeStreamNextResult::end());
        };
        let next = run_with_cancellation(
            async {
                stream
                    .next()
                    .await
                    .transpose()
                    .map_err(|error| stream_error(&error))
            },
            Some(self.state.token.clone()),
        )
        .await;
        match next {
            Ok(Some(record)) => {
                if self.state.closed.load(Ordering::Acquire) {
                    records.take();
                    return Ok(NativeStreamNextResult::end());
                }
                let response = wire::ReadResponse {
                    record: Some(wire_codec::record_wire(record)),
                };
                Ok(NativeStreamNextResult::value(
                    encode(&response, "follow").map_err(|error| napi_error(&error))?,
                ))
            }
            Ok(None) => {
                records.take();
                Ok(NativeStreamNextResult::end())
            }
            Err(error) => {
                // `RecordStream` is a recovery-aware stream: a transient item
                // error is yielded together with its cursor so the next poll
                // can retry the active endpoint. Keep that cursor alive unless
                // this handle was explicitly closed or cancelled.
                if self.state.closed.load(Ordering::Acquire) {
                    records.take();
                    return Ok(NativeStreamNextResult::end());
                }
                if self.state.token.is_cancelled() {
                    records.take();
                }
                Ok(NativeStreamNextResult::failure(error))
            }
        }
    }

    /// Cancels this cursor and waits for its transport stream to be released.
    ///
    /// A pending `nextResult` call is woken by the cancellation token and
    /// releases its transport stream when that call returns. A cursor cannot
    /// be reopened after it has been cancelled.
    #[napi]
    pub async fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.token.cancel();
        self.state.records.lock().await.take();
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
            .map_err(|error| connect_error(&error))
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
            .map_err(|error| napi_error(&error))?;
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
        .map_err(|error| napi_error(&error))?;
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
    let key = wire_codec::required_key(Some(request.idempotency_key))
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            client
                .inspect_idempotency(key)
                .await
                .map_err(|error| stream_error(&error))
        },
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
    let stream = client
        .stream(request.path)
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async { stream.tail().await.map_err(|error| stream_error(&error)) },
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
        async {
            client
                .bounds(&request.path)
                .await
                .map_err(|error| stream_error(&error))
        },
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
    let request = wire_codec::append_from_wire(decode::<wire::AppendRequest>(&request, "append")?)
        .map_err(|error| stream_error(&error))?;
    let stream = client
        .stream(request.path.to_string())
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            stream
                .append_batch(request.records, request.if_tail, request.idempotency_key)
                .await
                .map_err(|error| stream_error(&error))
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
    let request = wire_codec::fork_from_wire(decode::<wire::ForkRequest>(&request, "fork")?)
        .map_err(|error| stream_error(&error))?;
    let stream = client
        .stream(request.source.to_string())
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            stream
                .fork(
                    request.destination.to_string(),
                    request.at_tail,
                    request.idempotency_key,
                )
                .await
                .map_err(|error| stream_error(&error))
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
    let request = wire_codec::read_from_wire(decode::<wire::ReadRequest>(&request, "read")?)
        .map_err(|error| stream_error(&error))?;
    let stream = client
        .stream(request.path.to_string())
        .map_err(|error| stream_error(&error))?;
    let token = cancellation_state(cancellation);
    let records = stream.read(request.from, request.limit);
    let mut records = run_with_cancellation(
        async { records.await.map_err(|error| stream_error(&error)) },
        token.clone(),
    )
    .await?;
    let mut output = Vec::new();
    while let Some(value) = run_with_cancellation(
        async {
            records
                .next()
                .await
                .transpose()
                .map_err(|error| stream_error(&error))
        },
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
    let stream = client
        .stream(request.path)
        .map_err(|error| stream_error(&error))?;
    let records = run_with_cancellation(
        async move {
            stream
                .follow(request.from)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(records))),
            token: cancellation_state(cancellation).unwrap_or_default(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    })
}

async fn children(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request =
        wire_codec::children_from_wire(decode::<wire::ChildrenRequest>(&request, "children")?)
            .map_err(|error| stream_error(&error))?;
    let parent = request.parent.as_ref().map(ToString::to_string);
    let token = cancellation_state(cancellation);
    let mut values = run_with_cancellation(
        async {
            client
                .children(parent.as_deref(), request.limit)
                .await
                .map_err(|error| stream_error(&error))
        },
        token.clone(),
    )
    .await?;
    let mut output = Vec::new();
    while let Some(value) = run_with_cancellation(
        async {
            values
                .next()
                .await
                .transpose()
                .map_err(|error| stream_error(&error))
        },
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
    let request = wire_codec::children_page_from_wire(decode::<wire::ChildrenPageRequest>(
        &request,
        "children_page",
    )?)
    .map_err(|error| stream_error(&error))?;
    let parent = request.parent.as_ref().map(StreamPath::as_str);
    let after = request.after.as_ref().map(StreamPath::as_str);
    let value = run_with_cancellation(
        async {
            client
                .children_page(parent, after, request.hierarchy_version, request.limit)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::children_page_to_wire(value), "children_page")
}

async fn commit(
    client: &Client,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::CommitRequest>(&request, "commit")?;
    let deadline = request.deadline_unix_millis;
    let request = wire_codec::commit_from_wire(request).map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            match deadline {
                Some(deadline) => client.commit_before(request, deadline).await,
                None => client.commit(request).await,
            }
            .map_err(|error| stream_error(&error))
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
    let commit_id =
        wire_codec::commit_id(&request.commit_id).map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            client
                .read_commit(commit_id)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::envelope_to_wire(value), "read_commit")
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{
        AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest,
        ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkRequest,
        IdempotencyKey, IdempotencyObservation, ReadRequest, RecordStream, StreamBounds,
        StreamProvider,
    };
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tonic::transport::{Identity, Server, ServerTlsConfig};
    use tonic::{Request, Status};

    struct DropSentinel(Arc<AtomicBool>);

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

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

    #[tokio::test]
    async fn concurrent_follow_reads_are_serialized_instead_of_false_end() {
        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let stream_polled = Arc::clone(&polled);
        let stream_dropped = Arc::clone(&dropped);
        let sentinel = DropSentinel(stream_dropped);
        let records: acyclic_stream::RecordStream = Box::pin(futures::stream::poll_fn(move |_| {
            let _sentinel = &sentinel;
            stream_polled.store(true, Ordering::SeqCst);
            Poll::Pending
        }));
        let follow = Arc::new(NativeStreamFollow {
            state: FollowState {
                records: Arc::new(Mutex::new(Some(records))),
                token: CancellationToken::new(),
                closed: Arc::new(AtomicBool::new(false)),
            },
        });

        let first = tokio::spawn({
            let follow = Arc::clone(&follow);
            async move { follow.next_result().await }
        });
        while !polled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let mut second = tokio::spawn({
            let follow = Arc::clone(&follow);
            async move { follow.next_result().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err()
        );

        follow.close().await;
        assert!(dropped.load(Ordering::SeqCst));
        let first = first
            .await
            .expect("first follow task panicked")
            .expect("first read failed");
        assert!(first.value.is_none());
        assert!(first.error.is_none());
        let second = second
            .await
            .expect("second follow task panicked")
            .expect("second read failed");
        assert!(second.value.is_none());
        assert!(second.error.is_none());
    }

    #[tokio::test]
    async fn close_during_ready_poll_drops_cursor_without_reinsertion() {
        use std::sync::Mutex as StdMutex;

        let follow_state = FollowState {
            records: Arc::new(Mutex::new(None)),
            token: CancellationToken::new(),
            closed: Arc::new(AtomicBool::new(false)),
        };
        let follow = NativeStreamFollow {
            state: follow_state.clone(),
        };
        let close_task = Arc::new(StdMutex::new(None));
        let close_task_slot = Arc::clone(&close_task);
        let record = acyclic_stream::Record {
            sequence: 0,
            value: vec![1_u8, 2].into(),
            commit_id: acyclic_stream::CommitId::default(),
            committed_at_micros: 0,
        };
        let mut record = Some(record);
        let records: acyclic_stream::RecordStream = Box::pin(futures::stream::poll_fn(move |_| {
            let close_state = follow_state.clone();
            close_state.closed.store(true, Ordering::Release);
            close_state.token.cancel();
            let close_task = tokio::spawn(async move {
                NativeStreamFollow { state: close_state }.close().await;
            });
            *close_task_slot.lock().expect("close task slot poisoned") = Some(close_task);
            Poll::Ready(record.take().map(|record| Ok(record)))
        }));
        *follow.state.records.lock().await = Some(records);

        let first = follow.next_result().await.expect("first read failed");
        assert!(first.value.is_none());
        assert!(first.error.is_none());
        let close_task = close_task
            .lock()
            .expect("close task slot poisoned")
            .take()
            .expect("ready poll did not start close");
        close_task.await.expect("close task panicked");
        let second = follow.next_result().await.expect("second read failed");
        assert!(second.value.is_none());
        assert!(second.error.is_none());
    }

    struct TcpDropProbe {
        started: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    #[async_trait]
    impl StreamProvider for TcpDropProbe {
        async fn inspect_idempotency(
            &self,
            _idempotency_key: IdempotencyKey,
        ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn tail(&self, _path: StreamPath) -> std::result::Result<u64, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn bounds(
            &self,
            _path: StreamPath,
        ) -> std::result::Result<StreamBounds, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn append(
            &self,
            _request: AppendRequest,
        ) -> std::result::Result<AppendOutcome, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn fork(
            &self,
            _request: ForkRequest,
        ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn read(
            &self,
            _request: ReadRequest,
        ) -> std::result::Result<RecordStream, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn follow(
            &self,
            _path: StreamPath,
            _from: u64,
        ) -> std::result::Result<RecordStream, StreamError> {
            self.started.store(true, Ordering::SeqCst);
            let dropped = Arc::clone(&self.dropped);
            let sentinel = DropSentinel(dropped);
            Ok(futures::stream::poll_fn(move |_| {
                let _sentinel = &sentinel;
                Poll::Pending
            })
            .boxed())
        }

        async fn children(
            &self,
            _request: ChildrenRequest,
        ) -> std::result::Result<ChildStream, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn children_page(
            &self,
            _request: ChildrenPageRequest,
        ) -> std::result::Result<ChildrenPage, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn commit(
            &self,
            _request: CommitRequest,
        ) -> std::result::Result<CommitOutcome, StreamError> {
            Err(StreamError::Unsupported)
        }

        async fn read_commit(
            &self,
            _commit_id: CommitId,
        ) -> std::result::Result<CommittedEnvelope, StreamError> {
            Err(StreamError::Unsupported)
        }
    }

    #[tokio::test]
    async fn real_tcp_follow_close_releases_the_server_stream() {
        let result = async {
            let identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
            let certificate_pem = identity.cert.pem();
            let private_key_pem = identity.signing_key.serialize_pem();
            let client_ca = certificate_pem.as_bytes().to_vec();
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let started = Arc::new(AtomicBool::new(false));
            let dropped = Arc::new(AtomicBool::new(false));
            let provider = Arc::new(TcpDropProbe {
                started: Arc::clone(&started),
                dropped: Arc::clone(&dropped),
            });
            let service =
                acyclic_stream::wire::stream_service_server::StreamServiceServer::with_interceptor(
                    acyclic_stream::grpc::Service::new(provider),
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
            let incoming = futures::stream::unfold(listener, |listener| async move {
                listener
                    .accept()
                    .await
                    .ok()
                    .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
            });
            let server = tokio::spawn(async move {
                Server::builder()
                    .tls_config(
                        ServerTlsConfig::new()
                            .identity(Identity::from_pem(certificate_pem, private_key_pem)),
                    )?
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let endpoint = format!("https://localhost:{}", address.port());
            let connected = NativeStreamClient::connect_with_ca_result(
                endpoint,
                "exact-token".to_owned(),
                Buffer::from(client_ca),
                None,
            )
            .await?;
            assert!(
                connected.error.is_none(),
                "connect returned an error envelope"
            );
            let client = connected.client.expect("connect result omitted client");
            let request = wire::FollowRequest {
                path: "accounts/events".to_owned(),
                from: 0,
            };
            let opened = client
                .open_follow_result(Buffer::from(request.encode_to_vec()), None)
                .await?;
            assert!(opened.error.is_none(), "open returned an error envelope");
            let follow = Arc::new(opened.follow.expect("open result omitted follow"));
            let pending = tokio::spawn({
                let follow = Arc::clone(&follow);
                async move { follow.next_result().await }
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                while !started.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;

            follow.close().await;
            let closed = match tokio::time::timeout(Duration::from_secs(1), pending).await?? {
                Ok(value) => value,
                Err(_) => panic!("pending next_result rejected"),
            };
            assert!(closed.value.is_none());
            assert!(closed.error.is_none());
            let closed_again = follow.next_result().await?;
            assert!(closed_again.value.is_none());
            assert!(closed_again.error.is_none());

            tokio::time::timeout(Duration::from_secs(1), async {
                while !dropped.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| "server follow stream was not dropped after close")?;

            let _ = shutdown_tx.send(());
            server.await??;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        assert!(
            result.is_ok(),
            "real TCP follow lifecycle failed: {result:?}"
        );
    }
}
