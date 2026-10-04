#![allow(unsafe_code)]
//! Rust-owned synchronous C ABI for the LuaJIT remote facade.
//!
//! LuaJIT only supplies `ffi.cdef` calls and byte buffers. Endpoint pooling,
//! TLS, retries, idempotent append behaviour, streaming, and cancellation all
//! remain in the canonical `acyclic-stream` client.

use std::{
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr, slice,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use acyclic_sdk_contract_wire::{FAMILY_VIEWS, family_view};
use acyclic_stream::{AppendOutcome, StreamClient, StreamError, grpc};
use bytes::Bytes;
use futures::StreamExt as _;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor};
use reqwest::Url;
use tokio::{runtime::Runtime, sync::Notify, task::JoinHandle};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

const ABI_VERSION: u32 = 1;
const READ_MODE: u32 = 0;
const FOLLOW_MODE: u32 = 1;
const NO_TERMINAL: u32 = u32::MAX;
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_ENDPOINTS: usize = 16;

/// Stable result categories shared by all LuaJIT calls.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcyclicRemoteStatus {
    /// The operation returned a value.
    Ok = 0,
    /// A bounded reader reached the end.
    End = 1,
    /// A live reader has not received a record yet.
    Pending = 2,
    /// The caller cancelled or closed the reader.
    Cancelled = 3,
    /// The caller supplied an invalid argument or handle.
    InvalidArgument = 4,
    /// The canonical remote provider returned an error.
    ProviderError = 5,
    /// The ABI contained a panic.
    Panic = 7,
}

/// Bytes owned by Rust until explicit release.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AcyclicRemoteBuffer {
    /// Monotonic allocation identity.
    pub id: u64,
    /// Pointer to owned bytes, or null when empty.
    pub ptr: *mut u8,
    /// Number of initialized bytes.
    pub len: usize,
    /// Allocation capacity.
    pub capacity: usize,
}

/// Result of one remote append.
#[repr(C)]
pub struct AcyclicRemoteAppendResult {
    /// Result category.
    pub status: AcyclicRemoteStatus,
    /// First committed sequence.
    pub start: u64,
    /// Exclusive committed end.
    pub end: u64,
    /// Resulting or observed tail.
    pub tail: u64,
    /// Diagnostic text, if any.
    pub message: AcyclicRemoteBuffer,
}

/// Result of opening a remote reader.
#[repr(C)]
pub struct AcyclicRemoteOpenResult {
    /// Result category.
    pub status: AcyclicRemoteStatus,
    /// Reader handle on success.
    pub reader: u64,
    /// Diagnostic text, if any.
    pub message: AcyclicRemoteBuffer,
}

/// Result of opening a Rust-owned client or bidirectional streaming call.
#[repr(C)]
pub struct AcyclicRemoteDuplexOpenResult {
    /// Result category.
    pub status: AcyclicRemoteStatus,
    /// Reader handle for response messages.
    pub reader: u64,
    /// Writer handle for request messages.
    pub writer: u64,
    /// Diagnostic text, if any.
    pub message: AcyclicRemoteBuffer,
}

/// Result of pulling one remote record.
#[repr(C)]
pub struct AcyclicRemoteNextResult {
    /// Result category.
    pub status: AcyclicRemoteStatus,
    /// Record sequence on success.
    pub sequence: u64,
    /// Record value on success.
    pub value: AcyclicRemoteBuffer,
    /// Diagnostic text, if any.
    pub message: AcyclicRemoteBuffer,
}

struct RemoteRuntime(Option<Runtime>);
impl RemoteRuntime {
    fn new() -> Result<Self, String> {
        Runtime::new()
            .map(|runtime| Self(Some(runtime)))
            .map_err(|error| error.to_string())
    }
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.0
            .as_ref()
            .expect("runtime remains alive")
            .block_on(future)
    }
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0
            .as_ref()
            .expect("runtime remains alive")
            .spawn(future)
    }
}
impl Drop for RemoteRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.0.take() else { return };
        if tokio::runtime::Handle::try_current().is_ok() {
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

struct RemoteClient {
    runtime: RemoteRuntime,
    provider: Arc<grpc::Client>,
    grpc_channel: Channel,
    http: reqwest::Client,
    endpoint: Url,
    token: String,
}

/// Tonic codec that keeps protobuf reflection in Rust while exposing no
/// generated language-specific wire implementation to the FFI consumer.
#[derive(Clone)]
struct DynamicCodec {
    output: prost_reflect::MessageDescriptor,
}

struct DynamicEncoder;
struct DynamicDecoder {
    descriptor: prost_reflect::MessageDescriptor,
}

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            descriptor: self.output.clone(),
        }
    }
}

impl Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(dst)
            .map_err(|error| Status::internal(format!("encode dynamic protobuf: {error}")))
    }
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        DynamicMessage::decode(self.descriptor.clone(), src)
            .map(Some)
            .map_err(|error| Status::internal(format!("decode dynamic protobuf: {error}")))
    }
}
struct RemoteReader {
    _client: Arc<RemoteClient>,
    receiver: Mutex<std::sync::mpsc::Receiver<ReaderMessage>>,
    cancelled: Arc<AtomicBool>,
    terminal: AtomicU32,
    wake: Arc<Notify>,
    task: Mutex<Option<JoinHandle<()>>>,
    input: Option<Arc<RemoteWriter>>,
}

struct RemoteWriter {
    sender: Mutex<Option<tokio::sync::mpsc::Sender<DynamicMessage>>>,
    input: prost_reflect::MessageDescriptor,
    finished: AtomicBool,
}
enum ReaderMessage {
    Record(acyclic_stream::Record),
    Wire(Vec<u8>),
    End,
    Error(StreamError),
}
struct Allocation {
    ptr: usize,
    len: usize,
    capacity: usize,
    _bytes: Vec<u8>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CLIENTS: OnceLock<Mutex<HashMap<u64, Arc<RemoteClient>>>> = OnceLock::new();
static READERS: OnceLock<Mutex<HashMap<u64, Arc<RemoteReader>>>> = OnceLock::new();
static WRITERS: OnceLock<Mutex<HashMap<u64, Arc<RemoteWriter>>>> = OnceLock::new();
static BUFFERS: OnceLock<Mutex<HashMap<u64, Allocation>>> = OnceLock::new();
fn clients() -> &'static Mutex<HashMap<u64, Arc<RemoteClient>>> {
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn readers() -> &'static Mutex<HashMap<u64, Arc<RemoteReader>>> {
    READERS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn writers() -> &'static Mutex<HashMap<u64, Arc<RemoteWriter>>> {
    WRITERS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn buffers() -> &'static Mutex<HashMap<u64, Allocation>> {
    BUFFERS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn next_id() -> Option<u64> {
    let mut current = NEXT_ID.load(Ordering::Relaxed);
    loop {
        if current == u64::MAX {
            return None;
        }
        match NEXT_ID.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(current),
            Err(observed) => current = observed,
        }
    }
}
fn client_lookup(id: u64) -> Option<Arc<RemoteClient>> {
    clients().lock().ok()?.get(&id).cloned()
}
fn reader_lookup(id: u64) -> Option<Arc<RemoteReader>> {
    readers().lock().ok()?.get(&id).cloned()
}
fn writer_lookup(id: u64) -> Option<Arc<RemoteWriter>> {
    writers().lock().ok()?.get(&id).cloned()
}
fn empty_buffer() -> AcyclicRemoteBuffer {
    AcyclicRemoteBuffer {
        id: 0,
        ptr: ptr::null_mut(),
        len: 0,
        capacity: 0,
    }
}
fn owned_buffer(bytes: impl Into<Vec<u8>>) -> AcyclicRemoteBuffer {
    let mut bytes = bytes.into();
    if bytes.is_empty() {
        return empty_buffer();
    }
    let Some(id) = next_id() else {
        return empty_buffer();
    };
    let result = AcyclicRemoteBuffer {
        id,
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
        capacity: bytes.capacity(),
    };
    let allocation = Allocation {
        ptr: result.ptr as usize,
        len: result.len,
        capacity: result.capacity,
        _bytes: bytes,
    };
    if let Ok(mut all) = buffers().lock() {
        all.insert(id, allocation);
        result
    } else {
        empty_buffer()
    }
}
fn message(value: impl Into<String>) -> AcyclicRemoteBuffer {
    owned_buffer(value.into().into_bytes())
}
fn runtime_reentry() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}
fn input_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], &'static str> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err("nonempty input has a null pointer");
    }
    // SAFETY: C callers provide a valid read-only byte region for this call.
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}
fn input_text<'a>(ptr: *const u8, len: usize) -> Result<&'a str, &'static str> {
    std::str::from_utf8(input_bytes(ptr, len)?).map_err(|_| "input is not UTF-8")
}
fn invalid_append(text: &'static str) -> AcyclicRemoteAppendResult {
    AcyclicRemoteAppendResult {
        status: AcyclicRemoteStatus::InvalidArgument,
        start: 0,
        end: 0,
        tail: 0,
        message: message(text),
    }
}
fn invalid_open(text: &'static str) -> AcyclicRemoteOpenResult {
    AcyclicRemoteOpenResult {
        status: AcyclicRemoteStatus::InvalidArgument,
        reader: 0,
        message: message(text),
    }
}
fn invalid_next(text: &'static str) -> AcyclicRemoteNextResult {
    AcyclicRemoteNextResult {
        status: AcyclicRemoteStatus::InvalidArgument,
        sequence: 0,
        value: empty_buffer(),
        message: message(text),
    }
}
fn provider_append(error: impl ToString) -> AcyclicRemoteAppendResult {
    AcyclicRemoteAppendResult {
        status: AcyclicRemoteStatus::ProviderError,
        start: 0,
        end: 0,
        tail: 0,
        message: message(error.to_string()),
    }
}
fn provider_open(error: impl ToString) -> AcyclicRemoteOpenResult {
    AcyclicRemoteOpenResult {
        status: AcyclicRemoteStatus::ProviderError,
        reader: 0,
        message: message(error.to_string()),
    }
}
fn provider_next(error: impl ToString) -> AcyclicRemoteNextResult {
    AcyclicRemoteNextResult {
        status: AcyclicRemoteStatus::ProviderError,
        sequence: 0,
        value: empty_buffer(),
        message: message(error.to_string()),
    }
}
fn terminal_status(value: u32) -> Option<AcyclicRemoteStatus> {
    match value {
        1 => Some(AcyclicRemoteStatus::End),
        3 => Some(AcyclicRemoteStatus::Cancelled),
        _ => None,
    }
}
fn terminal_result(status: AcyclicRemoteStatus) -> AcyclicRemoteNextResult {
    AcyclicRemoteNextResult {
        status,
        sequence: 0,
        value: empty_buffer(),
        message: empty_buffer(),
    }
}
fn cancel_reader(reader: &RemoteReader) {
    if reader.cancelled.swap(true, Ordering::AcqRel) {
        return;
    }
    if let Some(input) = reader.input.as_ref() {
        cancel_writer(input);
    }
    let _ = reader.terminal.compare_exchange(
        NO_TERMINAL,
        AcyclicRemoteStatus::Cancelled as u32,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    reader.wake.notify_waiters();
    if let Ok(mut task) = reader.task.lock() {
        if let Some(task) = task.take() {
            task.abort();
        }
    }
}

#[repr(C)]
pub struct AcyclicRemoteWireResult {
    pub status: AcyclicRemoteStatus,
    pub response: AcyclicRemoteBuffer,
    pub message: AcyclicRemoteBuffer,
}
fn invalid_wire(text: &'static str) -> AcyclicRemoteWireResult {
    AcyclicRemoteWireResult {
        status: AcyclicRemoteStatus::InvalidArgument,
        response: empty_buffer(),
        message: message(text),
    }
}
fn provider_wire(error: impl ToString) -> AcyclicRemoteWireResult {
    AcyclicRemoteWireResult {
        status: AcyclicRemoteStatus::ProviderError,
        response: empty_buffer(),
        message: message(error.to_string()),
    }
}

fn invalid_duplex(text: &'static str) -> AcyclicRemoteDuplexOpenResult {
    AcyclicRemoteDuplexOpenResult {
        status: AcyclicRemoteStatus::InvalidArgument,
        reader: 0,
        writer: 0,
        message: message(text),
    }
}

fn provider_duplex(error: impl ToString) -> AcyclicRemoteDuplexOpenResult {
    AcyclicRemoteDuplexOpenResult {
        status: AcyclicRemoteStatus::ProviderError,
        reader: 0,
        writer: 0,
        message: message(error.to_string()),
    }
}

fn cancel_writer(writer: &RemoteWriter) {
    writer.finished.store(true, Ordering::Release);
    if let Ok(mut sender) = writer.sender.lock() {
        sender.take();
    }
}

fn write_writer(writer: &RemoteWriter, payload: &[u8]) -> AcyclicRemoteStatus {
    if writer.finished.load(Ordering::Acquire) {
        return AcyclicRemoteStatus::InvalidArgument;
    }
    let message = match DynamicMessage::decode(writer.input.clone(), payload) {
        Ok(message) => message,
        Err(_) => return AcyclicRemoteStatus::InvalidArgument,
    };
    let sender = match writer.sender.lock() {
        Ok(sender) => sender,
        Err(_) => return AcyclicRemoteStatus::Panic,
    };
    let Some(sender) = sender.as_ref() else {
        return AcyclicRemoteStatus::InvalidArgument;
    };
    match sender.try_send(message) {
        Ok(()) => AcyclicRemoteStatus::Ok,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => AcyclicRemoteStatus::Pending,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            AcyclicRemoteStatus::ProviderError
        }
    }
}

const MAX_HTTP_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

fn family_operation_names(family: &str) -> Vec<String> {
    let Some(view) = family_view(family) else {
        return Vec::new();
    };
    let Ok(pool) = DescriptorPool::decode(view.model.descriptor().as_slice()) else {
        return Vec::new();
    };
    pool.services()
        .filter(|service| service.parent_file().package() == view.package())
        .flat_map(|service| {
            service.methods().map(|method| {
                let rpc = format!("{}/{}", service.full_name(), method.name(),);
                view.routes()
                    .iter()
                    .find(|route| route.rpc.ends_with(&rpc))
                    .map_or_else(
                        || {
                            let mut name = method.name().to_owned();
                            if let Some(first) = name.get_mut(..1) {
                                first.make_ascii_lowercase();
                            }
                            name
                        },
                        |route| route.operation_id.to_owned(),
                    )
            })
        })
        .collect()
}

async fn http_json_wire_call(
    client: &RemoteClient,
    family: &str,
    operation: &str,
    request: &[u8],
) -> Result<Vec<u8>, String> {
    let view =
        family_view(family).ok_or_else(|| format!("unknown Rust contract family: {family}"))?;
    let route = view
        .routes()
        .iter()
        .find(|route| route.operation_id == operation || route.rpc == operation)
        .ok_or_else(|| {
            format!("operation {operation} has no Rust-owned HTTP projection for family {family}")
        })?;
    let pool = DescriptorPool::decode(view.model.descriptor().as_slice())
        .map_err(|error| format!("decode {family} descriptor: {error}"))?;
    let input_descriptor = pool
        .get_message_by_name(route.request)
        .ok_or_else(|| format!("missing request descriptor {}", route.request))?;
    let output_descriptor = pool
        .get_message_by_name(route.response)
        .ok_or_else(|| format!("missing response descriptor {}", route.response))?;
    let message = DynamicMessage::decode(input_descriptor, request)
        .map_err(|error| format!("decode {family}/{operation} request: {error}"))?;
    let body = serde_json::to_vec(&message)
        .map_err(|error| format!("encode {family}/{operation} JSON request: {error}"))?;
    let url = client
        .endpoint
        .join(route.path)
        .map_err(|error| format!("resolve {family}/{operation} route: {error}"))?;
    let mut response = client
        .http
        .post(url)
        .bearer_auth(&client.token)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| format!("{family}/{operation} transport: {error}"))?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_HTTP_RESPONSE_BYTES as u64)
    {
        return Err(format!(
            "{family}/{operation} response exceeds bounded limit"
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("{family}/{operation} response: {error}"))?
    {
        if chunk.len() > MAX_HTTP_RESPONSE_BYTES.saturating_sub(bytes.len()) {
            return Err(format!(
                "{family}/{operation} response exceeds bounded limit"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(format!(
            "{family}/{operation} service returned HTTP {}",
            status.as_u16()
        ));
    }
    let mut json = serde_json::Deserializer::from_slice(&bytes);
    let response = DynamicMessage::deserialize(output_descriptor, &mut json)
        .map_err(|error| format!("decode {family}/{operation} JSON response: {error}"))?;
    json.end()
        .map_err(|error| format!("decode {family}/{operation} JSON response: {error}"))?;
    Ok(response.encode_to_vec())
}

fn family_method(family: &str, operation: &str) -> Result<(MethodDescriptor, String), String> {
    let view =
        family_view(family).ok_or_else(|| format!("unknown Rust contract family: {family}"))?;
    let pool = DescriptorPool::decode(view.model.descriptor().as_slice())
        .map_err(|error| format!("decode {family} descriptor: {error}"))?;
    pool.services()
        .filter(|service| service.parent_file().package() == view.package())
        .flat_map(|service| {
            service.methods().map(|method| {
                let rpc = format!("{}/{}", service.full_name(), method.name());
                let route = view.routes().iter().find(|route| {
                    (route.operation_id == operation || route.rpc == operation)
                        && route.rpc.ends_with(&rpc)
                });
                let mut fallback = method.name().to_owned();
                if let Some(first) = fallback.get_mut(..1) {
                    first.make_ascii_lowercase();
                }
                let matches =
                    route.is_some() || fallback == operation || method.name() == operation;
                (method, format!("/{rpc}"), matches)
            })
        })
        .find_map(|(method, path, matches)| matches.then_some((method, path)))
        .ok_or_else(|| format!("unknown Rust-owned operation {family}/{operation}"))
}

async fn grpc_wire_call(
    client: &RemoteClient,
    family: &str,
    operation: &str,
    request: &[u8],
) -> Result<Vec<u8>, String> {
    let (method, path) = family_method(family, operation)?;
    if method.is_client_streaming() || method.is_server_streaming() {
        return Err(format!(
            "{family}/{operation} is streaming and requires the stream handle ABI"
        ));
    }
    let input = DynamicMessage::decode(method.input(), request)
        .map_err(|error| format!("decode {family}/{operation} request: {error}"))?;
    let mut request = Request::new(input);
    let authorization = MetadataValue::try_from(format!("Bearer {}", client.token))
        .map_err(|_| "invalid Rust-owned bearer credential".to_owned())?;
    request
        .metadata_mut()
        .insert("authorization", authorization);
    let mut grpc = tonic::client::Grpc::new(client.grpc_channel.clone());
    grpc.ready()
        .await
        .map_err(|error| format!("{family}/{operation} gRPC readiness: {error}"))?;
    let response = grpc
        .unary(
            request,
            PathAndQuery::try_from(path)
                .map_err(|error| format!("{family}/{operation} gRPC path: {error}"))?,
            DynamicCodec {
                output: method.output(),
            },
        )
        .await
        .map_err(|error| format!("{family}/{operation} gRPC: {error}"))?;
    Ok(response.into_inner().encode_to_vec())
}

fn open_grpc_family_stream(
    client: Arc<RemoteClient>,
    family: &str,
    operation: &str,
    request: &[u8],
) -> Result<RemoteReader, StreamError> {
    if runtime_reentry() {
        return Err(StreamError::InvalidArgument);
    }
    let (method, path) =
        family_method(family, operation).map_err(|_| StreamError::InvalidArgument)?;
    if !method.is_server_streaming() || method.is_client_streaming() {
        return Err(StreamError::Unsupported);
    }
    let input = DynamicMessage::decode(method.input(), request)
        .map_err(|_| StreamError::InvalidArgument)?;
    let path = PathAndQuery::try_from(path).map_err(|_| StreamError::InvalidArgument)?;
    let authorization = MetadataValue::try_from(format!("Bearer {}", client.token))
        .map_err(|_| StreamError::InvalidArgument)?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(64);
    let cancelled = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    let task_cancelled = Arc::clone(&cancelled);
    let task_wake = Arc::clone(&wake);
    let channel = client.grpc_channel.clone();
    let task = client.runtime.spawn(async move {
        let mut request = Request::new(input);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        let mut grpc = tonic::client::Grpc::new(channel);
        if grpc.ready().await.is_err() {
            let _ = sender.send(ReaderMessage::Error(StreamError::Unavailable));
            return;
        }
        let response = match grpc
            .server_streaming(
                request,
                path,
                DynamicCodec {
                    output: method.output(),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(_) => {
                let _ = sender.send(ReaderMessage::Error(StreamError::Unavailable));
                return;
            }
        };
        let mut stream = response.into_inner();
        loop {
            if task_cancelled.load(Ordering::Acquire) {
                return;
            }
            let next = tokio::select! {
                _ = task_wake.notified() => return,
                item = stream.message() => item,
            };
            match next {
                Ok(Some(value)) => {
                    if sender
                        .send(ReaderMessage::Wire(value.encode_to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => {
                    let _ = sender.send(ReaderMessage::End);
                    return;
                }
                Err(_) => {
                    let _ = sender.send(ReaderMessage::Error(StreamError::Unavailable));
                    return;
                }
            }
        }
    });
    Ok(RemoteReader {
        _client: client,
        receiver: Mutex::new(receiver),
        cancelled,
        terminal: AtomicU32::new(NO_TERMINAL),
        wake,
        task: Mutex::new(Some(task)),
        input: None,
    })
}

fn open_grpc_family_duplex(
    client: Arc<RemoteClient>,
    family: &str,
    operation: &str,
) -> Result<(RemoteReader, Arc<RemoteWriter>), StreamError> {
    if runtime_reentry() {
        return Err(StreamError::InvalidArgument);
    }
    let (method, path) =
        family_method(family, operation).map_err(|_| StreamError::InvalidArgument)?;
    if !method.is_client_streaming() {
        return Err(StreamError::Unsupported);
    }
    let input = method.input();
    let output = method.output();
    let server_streaming = method.is_server_streaming();
    let path = PathAndQuery::try_from(path).map_err(|_| StreamError::InvalidArgument)?;
    let authorization = MetadataValue::try_from(format!("Bearer {}", client.token))
        .map_err(|_| StreamError::InvalidArgument)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(64);
    let writer = Arc::new(RemoteWriter {
        sender: Mutex::new(Some(sender)),
        input,
        finished: AtomicBool::new(false),
    });
    let (response_sender, response_receiver) = std::sync::mpsc::sync_channel(64);
    let cancelled = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    let task_cancelled = Arc::clone(&cancelled);
    let task_wake = Arc::clone(&wake);
    let channel = client.grpc_channel.clone();
    let task = client.runtime.spawn(async move {
        let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
        let mut grpc = tonic::client::Grpc::new(channel);
        if grpc.ready().await.is_err() {
            let _ = response_sender.send(ReaderMessage::Error(StreamError::Unavailable));
            return;
        }
        if server_streaming {
            let response = tokio::select! {
                _ = task_wake.notified() => return,
                result = grpc.streaming(
                    request,
                    path,
                    DynamicCodec { output },
                ) => result,
            };
            let response = match response {
                Ok(response) => response,
                Err(_) => {
                    let _ = response_sender.send(ReaderMessage::Error(StreamError::Unavailable));
                    return;
                }
            };
            let mut stream = response.into_inner();
            loop {
                if task_cancelled.load(Ordering::Acquire) {
                    return;
                }
                let next = tokio::select! {
                    _ = task_wake.notified() => return,
                    item = stream.message() => item,
                };
                match next {
                    Ok(Some(value)) => {
                        if response_sender
                            .send(ReaderMessage::Wire(value.encode_to_vec()))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = response_sender.send(ReaderMessage::End);
                        return;
                    }
                    Err(_) => {
                        let _ =
                            response_sender.send(ReaderMessage::Error(StreamError::Unavailable));
                        return;
                    }
                }
            }
        }
        let response = tokio::select! {
            _ = task_wake.notified() => return,
            result = grpc.client_streaming(
                request,
                path,
                DynamicCodec { output },
            ) => result,
        };
        match response {
            Ok(response) => {
                let _ = response_sender
                    .send(ReaderMessage::Wire(response.into_inner().encode_to_vec()));
                let _ = response_sender.send(ReaderMessage::End);
            }
            Err(_) => {
                let _ = response_sender.send(ReaderMessage::Error(StreamError::Unavailable));
            }
        }
    });
    let reader = RemoteReader {
        _client: client,
        receiver: Mutex::new(response_receiver),
        cancelled,
        terminal: AtomicU32::new(NO_TERMINAL),
        wake,
        task: Mutex::new(Some(task)),
        input: Some(Arc::clone(&writer)),
    };
    Ok((reader, writer))
}

fn spawn_reader(
    client: Arc<RemoteClient>,
    path: String,
    from: u64,
    limit: u32,
    mode: u32,
) -> Result<RemoteReader, StreamError> {
    if mode == READ_MODE && limit == 0 {
        return Err(StreamError::InvalidArgument);
    }
    let stream = client.runtime.block_on(async {
        let stream = StreamClient::new(Arc::clone(&client.provider)).stream(path)?;
        match mode {
            READ_MODE => stream.read(from, limit).await,
            FOLLOW_MODE => stream.follow(from).await,
            _ => Err(StreamError::InvalidArgument),
        }
    })?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(64);
    let cancelled = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    let task_cancelled = Arc::clone(&cancelled);
    let task_wake = Arc::clone(&wake);
    let task = client.runtime.spawn(async move {
        let mut stream = stream;
        loop {
            if task_cancelled.load(Ordering::Acquire) {
                return;
            }
            let next =
                tokio::select! { _ = task_wake.notified() => return, item = stream.next() => item };
            match next {
                Some(Ok(record)) => {
                    if sender.send(ReaderMessage::Record(record)).is_err() {
                        return;
                    }
                }
                Some(Err(error)) => {
                    let _ = sender.send(ReaderMessage::Error(error));
                    return;
                }
                None => {
                    let _ = sender.send(ReaderMessage::End);
                    return;
                }
            }
        }
    });
    Ok(RemoteReader {
        _client: client,
        receiver: Mutex::new(receiver),
        cancelled,
        terminal: AtomicU32::new(NO_TERMINAL),
        wake,
        task: Mutex::new(Some(task)),
        input: None,
    })
}

/// Returns the number of Rust-owned contract families in the operation graph.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_count() -> usize {
    FAMILY_VIEWS.len()
}

/// Returns an owned name from the Rust-owned contract family inventory.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_name(index: usize) -> AcyclicRemoteBuffer {
    FAMILY_VIEWS.get(index).map_or_else(empty_buffer, |family| {
        owned_buffer(family.name.as_bytes().to_vec())
    })
}

/// Returns the Rust-owned hosted operation count for one family.
///
/// Families without a Rust-owned HTTP projection return zero and remain
/// explicitly unsupported by the generic remote ABI.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_operation_count(
    family_ptr: *const u8,
    family_len: usize,
) -> usize {
    let Ok(family) = input_text(family_ptr, family_len) else {
        return 0;
    };
    family_operation_names(family).len()
}

/// Returns an owned Rust operation ID for one family and route index.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_operation_name(
    family_ptr: *const u8,
    family_len: usize,
    index: usize,
) -> AcyclicRemoteBuffer {
    let Ok(family) = input_text(family_ptr, family_len) else {
        return empty_buffer();
    };
    family_operation_names(family)
        .get(index)
        .map_or_else(empty_buffer, |operation| {
            owned_buffer(operation.as_bytes().to_vec())
        })
}

/// Executes one Rust-owned hosted operation through its generated JSON
/// projection. Request and response bytes stay protobuf-native at the ABI
/// boundary; Rust performs JSON transcoding from the family descriptor.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_wire_call(
    client: u64,
    family_ptr: *const u8,
    family_len: usize,
    operation_ptr: *const u8,
    operation_len: usize,
    request_ptr: *const u8,
    request_len: usize,
) -> AcyclicRemoteWireResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return invalid_wire("synchronous ABI call cannot run from a Tokio runtime");
        }
        let Some(client) = client_lookup(client) else {
            return invalid_wire("client handle is invalid");
        };
        let family = match input_text(family_ptr, family_len) {
            Ok(value) => value,
            Err(error) => return invalid_wire(error),
        };
        let operation = match input_text(operation_ptr, operation_len) {
            Ok(value) => value,
            Err(error) => return invalid_wire(error),
        };
        let request = match input_bytes(request_ptr, request_len) {
            Ok(value) => value,
            Err(error) => return invalid_wire(error),
        };
        match client
            .runtime
            .block_on(grpc_wire_call(&client, family, operation, request))
        {
            Ok(response) => AcyclicRemoteWireResult {
                status: AcyclicRemoteStatus::Ok,
                response: owned_buffer(response),
                message: empty_buffer(),
            },
            Err(error) if error.starts_with("unknown Rust-owned operation") => {
                AcyclicRemoteWireResult {
                    status: AcyclicRemoteStatus::InvalidArgument,
                    response: empty_buffer(),
                    message: message(error),
                }
            }
            Err(error) => provider_wire(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteWireResult {
        status: AcyclicRemoteStatus::Panic,
        response: empty_buffer(),
        message: message("panic contained at ABI boundary"),
    })
}

/// Opens a Rust-owned server stream for one descriptor method. Each reader
/// value is the encoded protobuf response for that method and cancellation
/// aborts the underlying tonic task through the shared reader handle.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_stream_open(
    client: u64,
    family_ptr: *const u8,
    family_len: usize,
    operation_ptr: *const u8,
    operation_len: usize,
    request_ptr: *const u8,
    request_len: usize,
) -> AcyclicRemoteOpenResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(client) = client_lookup(client) else {
            return invalid_open("client handle is invalid");
        };
        let family = match input_text(family_ptr, family_len) {
            Ok(value) => value,
            Err(error) => return invalid_open(error),
        };
        let operation = match input_text(operation_ptr, operation_len) {
            Ok(value) => value,
            Err(error) => return invalid_open(error),
        };
        let request = match input_bytes(request_ptr, request_len) {
            Ok(value) => value,
            Err(error) => return invalid_open(error),
        };
        match open_grpc_family_stream(Arc::clone(&client), family, operation, request) {
            Ok(reader) => {
                let Some(id) = next_id() else {
                    return invalid_open("reader ID exhausted");
                };
                if let Ok(mut all) = readers().lock() {
                    all.insert(id, Arc::new(reader));
                    AcyclicRemoteOpenResult {
                        status: AcyclicRemoteStatus::Ok,
                        reader: id,
                        message: empty_buffer(),
                    }
                } else {
                    invalid_open("reader mutex unavailable")
                }
            }
            Err(StreamError::Unsupported) => {
                invalid_open("operation streaming shape is unsupported")
            }
            Err(error) => provider_open(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteOpenResult {
        status: AcyclicRemoteStatus::Panic,
        reader: 0,
        message: message("panic contained at ABI boundary"),
    })
}

/// Opens a Rust-owned client or bidirectional stream. Request messages are
/// submitted with `acyclic_remote_stream_write`; bounded backpressure returns
/// `Pending` and `acyclic_remote_stream_finish` closes the request side.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_family_duplex_open(
    client: u64,
    family_ptr: *const u8,
    family_len: usize,
    operation_ptr: *const u8,
    operation_len: usize,
) -> AcyclicRemoteDuplexOpenResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(client) = client_lookup(client) else {
            return invalid_duplex("client handle is invalid");
        };
        let family = match input_text(family_ptr, family_len) {
            Ok(value) => value,
            Err(error) => return invalid_duplex(error),
        };
        let operation = match input_text(operation_ptr, operation_len) {
            Ok(value) => value,
            Err(error) => return invalid_duplex(error),
        };
        match open_grpc_family_duplex(Arc::clone(&client), family, operation) {
            Ok((reader, writer)) => {
                let (Some(reader_id), Some(writer_id)) = (next_id(), next_id()) else {
                    cancel_writer(&writer);
                    return invalid_duplex("stream handle ID exhausted");
                };
                let reader = Arc::new(reader);
                if let Ok(mut all) = readers().lock() {
                    all.insert(reader_id, reader);
                } else {
                    cancel_writer(&writer);
                    return invalid_duplex("reader mutex unavailable");
                }
                if let Ok(mut all) = writers().lock() {
                    all.insert(writer_id, Arc::clone(&writer));
                    AcyclicRemoteDuplexOpenResult {
                        status: AcyclicRemoteStatus::Ok,
                        reader: reader_id,
                        writer: writer_id,
                        message: empty_buffer(),
                    }
                } else {
                    if let Ok(mut all) = readers().lock() {
                        all.remove(&reader_id);
                    }
                    cancel_writer(&writer);
                    invalid_duplex("writer mutex unavailable")
                }
            }
            Err(StreamError::Unsupported) => {
                invalid_duplex("operation is not client or bidirectional streaming")
            }
            Err(error) => provider_duplex(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteDuplexOpenResult {
        status: AcyclicRemoteStatus::Panic,
        reader: 0,
        writer: 0,
        message: message("panic contained at ABI boundary"),
    })
}

/// Attempts to enqueue one encoded protobuf request message. A full bounded
/// queue returns `Pending`, allowing a synchronous FFI consumer to retry.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_write(
    writer: u64,
    request_ptr: *const u8,
    request_len: usize,
) -> AcyclicRemoteStatus {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(writer) = writer_lookup(writer) else {
            return AcyclicRemoteStatus::InvalidArgument;
        };
        let request = match input_bytes(request_ptr, request_len) {
            Ok(value) => value,
            Err(_) => return AcyclicRemoteStatus::InvalidArgument,
        };
        write_writer(&writer, request)
    }));
    result.unwrap_or(AcyclicRemoteStatus::Panic)
}

/// Closes the request side of a client or bidirectional stream.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_finish(writer: u64) -> AcyclicRemoteStatus {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(writer) = writer_lookup(writer) else {
            return AcyclicRemoteStatus::InvalidArgument;
        };
        if writer.finished.swap(true, Ordering::AcqRel) {
            return AcyclicRemoteStatus::Ok;
        }
        if let Ok(mut sender) = writer.sender.lock() {
            sender.take();
            AcyclicRemoteStatus::Ok
        } else {
            AcyclicRemoteStatus::Panic
        }
    }));
    result.unwrap_or(AcyclicRemoteStatus::Panic)
}

/// Cancels a request stream and drops any queued request messages.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_cancel(writer: u64) -> AcyclicRemoteStatus {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(writer) = writer_lookup(writer) else {
            return AcyclicRemoteStatus::InvalidArgument;
        };
        cancel_writer(&writer);
        AcyclicRemoteStatus::Cancelled
    }));
    result.unwrap_or(AcyclicRemoteStatus::Panic)
}

/// Closes and removes a request stream handle.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_close(writer: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(mut all) = writers().lock() {
            if let Some(writer) = all.remove(&writer) {
                cancel_writer(&writer);
            }
        }
    }));
}

/// Executes one canonical unary Stream protobuf operation through Rust.
///
/// The operation name and protobuf request bytes are generated from the Rust
/// Stream contract. LuaJIT owns only byte lifetimes; validation, transport,
/// retries, and response encoding remain in Rust.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_wire_call(
    client: u64,
    operation_ptr: *const u8,
    operation_len: usize,
    request_ptr: *const u8,
    request_len: usize,
) -> AcyclicRemoteWireResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return invalid_wire("synchronous ABI call cannot run from a Tokio runtime");
        }
        let Some(client) = client_lookup(client) else {
            return invalid_wire("client handle is invalid");
        };
        let operation = match input_text(operation_ptr, operation_len) {
            Ok(value) => value,
            Err(error) => return invalid_wire(error),
        };
        let request = match input_bytes(request_ptr, request_len) {
            Ok(value) => value,
            Err(error) => return invalid_wire(error),
        };
        let result = client.runtime.block_on(async {
            let client = StreamClient::new(Arc::clone(&client.provider));
            let encoded = match operation {
                "inspect_idempotency" => client
                    .inspect_idempotency_wire(
                        acyclic_stream::wire::InspectIdempotencyRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "append" => client
                    .append_wire(
                        acyclic_stream::wire::AppendRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "tail" => client
                    .tail_wire(
                        acyclic_stream::wire::TailRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "fork" => client
                    .fork_wire(
                        acyclic_stream::wire::ForkRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "children_page" => client
                    .children_page_wire(
                        acyclic_stream::wire::ChildrenPageRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "commit" => client
                    .commit_wire(
                        acyclic_stream::wire::CommitRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "read_commit" => client
                    .read_commit_wire(
                        acyclic_stream::wire::ReadCommitRequest::decode(request)
                            .map_err(|_| StreamError::InvalidArgument)?,
                    )
                    .await?
                    .encode_to_vec(),
                "read" | "follow" | "children" => return Err(StreamError::Unsupported),
                _ => return Err(StreamError::InvalidArgument),
            };
            Ok::<_, StreamError>(encoded)
        });
        match result {
            Ok(response) => AcyclicRemoteWireResult {
                status: AcyclicRemoteStatus::Ok,
                response: owned_buffer(response),
                message: empty_buffer(),
            },
            Err(error) => provider_wire(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteWireResult {
        status: AcyclicRemoteStatus::Panic,
        response: empty_buffer(),
        message: message("panic contained at ABI boundary"),
    })
}
const STREAM_OPERATIONS: &[&str] = &[
    "inspect_idempotency",
    "append",
    "tail",
    "fork",
    "read",
    "follow",
    "children",
    "children_page",
    "commit",
    "read_commit",
];

/// Returns the number of canonical Stream operations exposed by this ABI.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_operation_count() -> usize {
    STREAM_OPERATIONS.len()
}

/// Returns an owned operation name for the generated Stream inventory.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_stream_operation_name(index: usize) -> AcyclicRemoteBuffer {
    STREAM_OPERATIONS
        .get(index)
        .map_or_else(empty_buffer, |name| owned_buffer(name.as_bytes().to_vec()))
}
/// Returns the Rust-owned ABI revision.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_abi_version() -> u32 {
    ABI_VERSION
}

/// Connects to newline-separated TLS endpoints. A nonempty CA buffer enables private-CA TLS.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_client_open(
    endpoints_ptr: *const u8,
    endpoints_len: usize,
    token_ptr: *const u8,
    token_len: usize,
    ca_ptr: *const u8,
    ca_len: usize,
) -> u64 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return 0;
        }
        let endpoints = match input_text(endpoints_ptr, endpoints_len) {
            Ok(value) => value,
            Err(_) => return 0,
        };
        let token = match input_text(token_ptr, token_len) {
            Ok(value) => value,
            Err(_) => return 0,
        };
        if token.is_empty() {
            return 0;
        }
        let list = endpoints
            .split('\n')
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        if list.is_empty() || list.len() > MAX_ENDPOINTS {
            return 0;
        }
        let ca = match input_bytes(ca_ptr, ca_len) {
            Ok(value) => value,
            Err(_) => return 0,
        };
        let endpoint = match Url::parse(list[0]) {
            Ok(endpoint) => endpoint,
            Err(_) => return 0,
        };
        let loopback = matches!(
            endpoint.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return 0;
        }
        let runtime = match RemoteRuntime::new() {
            Ok(runtime) => runtime,
            Err(_) => return 0,
        };
        let mut http_builder =
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if !ca.is_empty() {
            let certificate = match reqwest::Certificate::from_pem(ca) {
                Ok(certificate) => certificate,
                Err(_) => return 0,
            };
            http_builder = http_builder.add_root_certificate(certificate);
        }
        let http = match http_builder.build() {
            Ok(http) => http,
            Err(_) => return 0,
        };
        let mut grpc_endpoint = match Endpoint::from_shared(list[0].to_owned()) {
            Ok(endpoint) => endpoint,
            Err(_) => return 0,
        };
        if !ca.is_empty() {
            grpc_endpoint = match grpc_endpoint.tls_config(
                ClientTlsConfig::new()
                    .with_enabled_roots()
                    .ca_certificate(Certificate::from_pem(ca)),
            ) {
                Ok(endpoint) => endpoint,
                Err(_) => return 0,
            };
        }
        let grpc_channel = grpc_endpoint.connect_lazy();
        let provider = match runtime.block_on(async {
            if ca.is_empty() {
                grpc::Client::connect_endpoints(list, token).await
            } else {
                grpc::Client::connect_endpoints_with_ca_certificate(list, token, ca).await
            }
        }) {
            Ok(provider) => provider,
            Err(_) => return 0,
        };
        let client = Arc::new(RemoteClient {
            runtime,
            provider: Arc::new(provider),
            grpc_channel,
            http,
            endpoint,
            token: token.to_owned(),
        });
        let Some(id) = next_id() else { return 0 };
        if let Ok(mut all) = clients().lock() {
            all.insert(id, client);
            id
        } else {
            0
        }
    }));
    result.unwrap_or(0)
}

/// Closes a client. Open readers retain their client until closed.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_client_close(client: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(mut all) = clients().lock() {
            all.remove(&client);
        }
    }));
}

/// Appends one record through the canonical Rust remote provider.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_append(
    client: u64,
    path_ptr: *const u8,
    path_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> AcyclicRemoteAppendResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return invalid_append("synchronous ABI call cannot run from a Tokio runtime");
        }
        let Some(client) = client_lookup(client) else {
            return invalid_append("client handle is invalid");
        };
        let path = match input_text(path_ptr, path_len) {
            Ok(value) => value.to_owned(),
            Err(error) => return invalid_append(error),
        };
        let value = match input_bytes(value_ptr, value_len) {
            Ok(value) => Bytes::copy_from_slice(value),
            Err(error) => return invalid_append(error),
        };
        match client.runtime.block_on(async {
            StreamClient::new(Arc::clone(&client.provider))
                .stream(path)?
                .append(value)
                .await
        }) {
            Ok(AppendOutcome::Committed(receipt)) => AcyclicRemoteAppendResult {
                status: AcyclicRemoteStatus::Ok,
                start: receipt.start,
                end: receipt.end,
                tail: receipt.tail,
                message: empty_buffer(),
            },
            Ok(AppendOutcome::TailConflict { actual_tail }) => AcyclicRemoteAppendResult {
                status: AcyclicRemoteStatus::ProviderError,
                start: 0,
                end: 0,
                tail: actual_tail,
                message: message("tail conflict"),
            },
            Err(error) => provider_append(error),
        }
    }));
    result.unwrap_or_else(|_| invalid_append("panic contained at ABI boundary"))
}

/// Opens a bounded reader (`mode = 0`) or a live follow reader (`mode = 1`).
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_open(
    client: u64,
    path_ptr: *const u8,
    path_len: usize,
    from: u64,
    limit: u32,
    mode: u32,
) -> AcyclicRemoteOpenResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return invalid_open("synchronous ABI call cannot run from a Tokio runtime");
        }
        let Some(client) = client_lookup(client) else {
            return invalid_open("client handle is invalid");
        };
        if mode != READ_MODE && mode != FOLLOW_MODE {
            return invalid_open("unknown reader mode");
        }
        let path = match input_text(path_ptr, path_len) {
            Ok(value) => value.to_owned(),
            Err(error) => return invalid_open(error),
        };
        match spawn_reader(Arc::clone(&client), path, from, limit, mode) {
            Ok(reader) => {
                let Some(id) = next_id() else {
                    return invalid_open("reader ID exhausted");
                };
                if let Ok(mut all) = readers().lock() {
                    all.insert(id, Arc::new(reader));
                    AcyclicRemoteOpenResult {
                        status: AcyclicRemoteStatus::Ok,
                        reader: id,
                        message: empty_buffer(),
                    }
                } else {
                    invalid_open("reader registry unavailable")
                }
            }
            Err(error) => provider_open(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteOpenResult {
        status: AcyclicRemoteStatus::Panic,
        reader: 0,
        message: message("panic contained at ABI boundary"),
    })
}

/// Pulls one record, returning `Pending` for a live reader when no record arrived promptly.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_next(reader: u64) -> AcyclicRemoteNextResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(reader) = reader_lookup(reader) else {
            return invalid_next("reader handle is invalid");
        };
        if let Some(status) = terminal_status(reader.terminal.load(Ordering::Acquire)) {
            return terminal_result(status);
        }
        if reader.cancelled.load(Ordering::Acquire) {
            return terminal_result(AcyclicRemoteStatus::Cancelled);
        }
        let receiver = match reader.receiver.lock() {
            Ok(receiver) => receiver,
            Err(_) => return invalid_next("reader mutex poisoned"),
        };
        match receiver.recv_timeout(POLL_INTERVAL) {
            Ok(ReaderMessage::Record(record)) => AcyclicRemoteNextResult {
                status: AcyclicRemoteStatus::Ok,
                sequence: record.sequence,
                value: owned_buffer(record.value.to_vec()),
                message: empty_buffer(),
            },
            Ok(ReaderMessage::Wire(value)) => AcyclicRemoteNextResult {
                status: AcyclicRemoteStatus::Ok,
                sequence: 0,
                value: owned_buffer(value),
                message: empty_buffer(),
            },
            Ok(ReaderMessage::End) => {
                reader
                    .terminal
                    .store(AcyclicRemoteStatus::End as u32, Ordering::Release);
                terminal_result(AcyclicRemoteStatus::End)
            }
            Ok(ReaderMessage::Error(error)) => {
                reader
                    .terminal
                    .store(AcyclicRemoteStatus::ProviderError as u32, Ordering::Release);
                provider_next(error)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => AcyclicRemoteNextResult {
                status: AcyclicRemoteStatus::Pending,
                sequence: 0,
                value: empty_buffer(),
                message: empty_buffer(),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                reader
                    .terminal
                    .store(AcyclicRemoteStatus::ProviderError as u32, Ordering::Release);
                provider_next("reader task disconnected")
            }
        }
    }));
    result.unwrap_or_else(|_| invalid_next("panic contained at ABI boundary"))
}

/// Cancels a reader and wakes its Rust stream task.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_cancel(reader: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(reader) = reader_lookup(reader) {
            cancel_reader(&reader);
        }
    }));
}

/// Closes and removes a reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_close(reader: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(mut all) = readers().lock() {
            if let Some(reader) = all.remove(&reader) {
                cancel_reader(&reader);
            }
        }
    }));
}

/// Releases one buffer returned by the ABI.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_buffer_release(
    buffer: AcyclicRemoteBuffer,
) -> AcyclicRemoteStatus {
    if buffer.id == 0 {
        return AcyclicRemoteStatus::Ok;
    }
    match buffers().lock() {
        Ok(mut all) => match all.get(&buffer.id) {
            Some(value)
                if value.ptr == buffer.ptr as usize
                    && value.len == buffer.len
                    && value.capacity == buffer.capacity =>
            {
                all.remove(&buffer.id);
                AcyclicRemoteStatus::Ok
            }
            _ => AcyclicRemoteStatus::InvalidArgument,
        },
        Err(_) => AcyclicRemoteStatus::Panic,
    }
}

/// Releases a result's diagnostic buffer.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_append_result_release(result: AcyclicRemoteAppendResult) {
    let _ = acyclic_remote_buffer_release(result.message);
}
/// Releases a generic wire result.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_wire_result_release(result: AcyclicRemoteWireResult) {
    let _ = acyclic_remote_buffer_release(result.response);
    let _ = acyclic_remote_buffer_release(result.message);
}

/// Releases an open result and any unclaimed reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_open_result_release(result: AcyclicRemoteOpenResult) {
    let _ = acyclic_remote_buffer_release(result.message);
    if result.reader != 0 {
        acyclic_remote_reader_close(result.reader);
    }
}

/// Releases an open client or bidirectional stream result and both handles.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_duplex_open_result_release(result: AcyclicRemoteDuplexOpenResult) {
    let _ = acyclic_remote_buffer_release(result.message);
    if result.reader != 0 {
        acyclic_remote_reader_close(result.reader);
    }
    if result.writer != 0 {
        acyclic_remote_stream_close(result.writer);
    }
}

/// Releases a next result's value and diagnostic buffers.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_next_result_release(result: AcyclicRemoteNextResult) {
    let _ = acyclic_remote_buffer_release(result.value);
    let _ = acyclic_remote_buffer_release(result.message);
}
