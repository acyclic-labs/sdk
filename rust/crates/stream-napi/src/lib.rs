//! N-API bridge for the canonical Rust Stream gRPC client.
//!
//! Protobuf request and response bytes cross the JavaScript boundary. The
//! bridge owns no transport or retry policy: endpoint failover, cursor
//! recovery, idempotency, and domain validation remain in
//! `acyclic_stream::grpc::Client`.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use acyclic_stream::{StreamError, StreamPath, StreamProvider, grpc, wire, wire_codec};
use futures::StreamExt;
use napi::Env;
use napi::bindgen_prelude::{Buffer, Error, Promise, PromiseRaw, Result, Status};
use napi::threadsafe_function::ThreadsafeFunction;
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
        grpc::ConnectError::Endpoint(_) | grpc::ConnectError::PlatformVerifier(_) => "transport",
        grpc::ConnectError::InsecureEndpoint
        | grpc::ConnectError::InvalidCredential
        | grpc::ConnectError::InvalidCaCertificate => "configuration",
    };
    NativeStreamErrorMetadata {
        code: code.to_owned(),
        message: if code == "transport" {
            format!("{error:?}")
        } else {
            error.to_string()
        },
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

type FollowFrames = futures::stream::BoxStream<'static, std::result::Result<Vec<u8>, StreamError>>;
#[derive(Clone)]
struct FollowState {
    // Lock order: cursor -> provider cursor; cancellation never acquires this lock.
    records: Arc<Mutex<Option<FollowFrames>>>,
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
    /// Returns the next canonical response for this cursor, or an empty result at end/close.
    ///
    /// Session recovery belongs to the provider. Every terminal error, including
    /// `Unavailable`, is returned in the result envelope.
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
            async { Ok(stream.next().await) },
            Some(self.state.token.clone()),
        )
        .await;
        match next {
            Ok(Some(Ok(bytes))) => {
                if self.state.closed.load(Ordering::Acquire) {
                    records.take();
                    return Ok(NativeStreamNextResult::end());
                }
                Ok(NativeStreamNextResult::value(Buffer::from(bytes)))
            }
            Ok(Some(Err(error))) => {
                if self.state.closed.load(Ordering::Acquire) {
                    records.take();
                    return Ok(NativeStreamNextResult::end());
                }
                Ok(NativeStreamNextResult::failure(stream_error(&error)))
            }
            Ok(None) => {
                records.take();
                Ok(NativeStreamNextResult::end())
            }
            Err(error) => {
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

/// Native Stream client and its authenticated gRPC transport.
#[napi]
pub struct NativeStreamClient {
    provider: Arc<grpc::Client>,
}

impl NativeStreamClient {
    fn from_client(provider: grpc::Client) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }
}

async fn connect_native(
    endpoint: String,
    token: String,
    ca_certificate_pem: Option<Vec<u8>>,
    cancellation: Option<CancellationToken>,
) -> std::result::Result<grpc::Client, NativeStreamErrorMetadata> {
    run_with_cancellation(
        async move {
            match ca_certificate_pem {
                Some(ca_certificate_pem) => {
                    grpc::Client::connect_eager_with_ca_certificate(
                        &endpoint,
                        &token,
                        &ca_certificate_pem,
                    )
                    .await
                }
                None => grpc::Client::connect_eager(&endpoint, &token).await,
            }
            .map_err(|error| connect_error(&error))
        },
        cancellation,
    )
    .await
}

#[napi]
impl NativeStreamClient {
    /// Resolves JavaScript credentials inside each Rust retry and reconnect.
    #[napi(js_name = "withCredentialProvider")]
    pub fn with_credential_provider(
        &self,
        callback: ThreadsafeFunction<(), Promise<String>, (), Status, false, true>,
    ) -> Self {
        Self::from_client(
            self.provider
                .with_credential_provider(credentials::provider(callback)),
        )
    }

    /// Establishes TLS without calling the provider until a physical RPC starts.
    #[napi(js_name = "connectWithCredentialResult")]
    pub async fn connect_with_credential_result(
        endpoint: String,
        callback: ThreadsafeFunction<(), Promise<String>, (), Status, false, true>,
        ca_certificate_pem: Option<Buffer>,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamConnectResult> {
        let credential = credentials::provider(callback);
        let certificate = ca_certificate_pem.map(|bytes| bytes.to_vec());
        let result = run_with_cancellation(
            async move {
                grpc::Client::connect_eager_with_credential(
                    &endpoint,
                    credential,
                    certificate.as_deref(),
                )
                .await
                .map_err(|error| connect_error(&error))
            },
            cancellation_state(cancellation),
        )
        .await;
        Ok(match result {
            Ok(client) => NativeStreamConnectResult::success(Self::from_client(client)),
            Err(error) => NativeStreamConnectResult::failure(error),
        })
    }

    /// Uses a request-local credential while retaining the existing TLS channel.
    #[napi(js_name = "withCredential")]
    pub fn with_credential(&self, token: String) -> Result<Self> {
        self.provider
            .with_credential(&token)
            .map(Self::from_client)
            .map_err(|error| napi_error(&connect_error(&error)))
    }

    /// Abandons pending handshakes before cancelling their credential work.
    #[napi]
    pub fn abandon_session_attempts(&self) -> Result<()> {
        self.provider
            .abandon_session_attempts()
            .map_err(|error| napi_error(&stream_error(&error)))
    }

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

    /// Connects trusting exclusively the caller-pinned private PEM CA certificate.
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
        operation_result(inspect_idempotency(&self.provider, request, cancellation)).await
    }

    /// Executes a canonical tail request.
    #[napi(js_name = "tailResult")]
    pub async fn tail_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(tail(&self.provider, request, cancellation)).await
    }

    /// Observes the canonical atomic replay bound for one stream path.
    #[napi(js_name = "boundsResult")]
    pub async fn bounds_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(bounds(&self.provider, request, cancellation)).await
    }

    /// Executes a canonical append request.
    #[napi(js_name = "appendResult")]
    pub async fn append_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(append(&self.provider, request, cancellation)).await
    }

    /// Executes a canonical fork request.
    #[napi(js_name = "forkResult")]
    pub async fn fork_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(fork(&self.provider, request, cancellation)).await
    }

    /// Reads one bounded page as encoded `ReadResponse` values.
    #[napi(js_name = "readResult")]
    pub async fn read_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamSequenceResult> {
        Ok(match read(&self.provider, request, cancellation).await {
            Ok(values) => NativeStreamSequenceResult::success(values),
            Err(error) => NativeStreamSequenceResult::failure(error),
        })
    }

    /// Opens a Rust-owned follow cursor from an encoded request.
    #[napi(js_name = "openFollowResult")]
    pub fn open_follow_result<'env>(
        &self,
        env: &'env Env,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<PromiseRaw<'env, NativeStreamFollowResult>> {
        let client = Arc::clone(&self.provider);
        let cancellation = cancellation.map(|value| NativeStreamCancellation {
            token: value.token.clone(),
        });
        let work = async move {
            Ok(
                match open_follow(&client, request, cancellation.as_ref()).await {
                    Ok(follow) => NativeStreamFollowResult::success(follow),
                    Err(error) => NativeStreamFollowResult::failure(error),
                },
            )
        };
        let work = napi::bindgen_prelude::within_runtime_if_available(|| eager::start(work));
        env.spawn_future(work)
    }

    /// Opens live child discovery using the client's shared Streams session.
    #[napi(js_name = "openChildrenResult")]
    pub fn open_children_result<'env>(
        &self,
        env: &'env Env,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<PromiseRaw<'env, NativeStreamFollowResult>> {
        let client = Arc::clone(&self.provider);
        let cancellation = cancellation.map(|value| NativeStreamCancellation {
            token: value.token.clone(),
        });
        let work = async move {
            Ok(
                match open_children(&client, request, cancellation.as_ref()).await {
                    Ok(follow) => NativeStreamFollowResult::success(follow),
                    Err(error) => NativeStreamFollowResult::failure(error),
                },
            )
        };
        let work = napi::bindgen_prelude::within_runtime_if_available(|| eager::start(work));
        env.spawn_future(work)
    }

    /// Executes a canonical children-page request.
    #[napi(js_name = "childrenPageResult")]
    pub async fn children_page_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(children_page(&self.provider, request, cancellation)).await
    }

    /// Executes a canonical commit request, retaining an optional provider deadline.
    #[napi(js_name = "commitResult")]
    pub async fn commit_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(commit(&self.provider, request, cancellation)).await
    }

    /// Reads one committed envelope by its canonical identity.
    #[napi(js_name = "readCommitResult")]
    pub async fn read_commit_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamOperationResult> {
        operation_result(read_commit(&self.provider, request, cancellation)).await
    }

    /// Reads all children from one fixed snapshot as encoded responses.
    #[napi(js_name = "childrenResult")]
    pub async fn children_result(
        &self,
        request: Buffer,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeStreamSequenceResult> {
        Ok(
            match children(&self.provider, request, cancellation).await {
                Ok(values) => NativeStreamSequenceResult::success(values),
                Err(error) => NativeStreamSequenceResult::failure(error),
            },
        )
    }
}

mod credentials;
mod eager;
mod operations;
use operations::*;

#[cfg(test)]
mod tests;
