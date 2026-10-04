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
    sync::{Arc, Mutex, OnceLock, atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}},
    time::Duration,
};

use acyclic_stream::{AppendOutcome, StreamClient, StreamError, grpc};
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::{runtime::Runtime, sync::Notify, task::JoinHandle};

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
    fn new() -> Result<Self, String> { Runtime::new().map(|runtime| Self(Some(runtime))).map_err(|error| error.to_string()) }
    fn block_on<F: Future>(&self, future: F) -> F::Output { self.0.as_ref().expect("runtime remains alive").block_on(future) }
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static { self.0.as_ref().expect("runtime remains alive").spawn(future) }
}
impl Drop for RemoteRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.0.take() else { return };
        if tokio::runtime::Handle::try_current().is_ok() { runtime.shutdown_background(); } else { drop(runtime); }
    }
}

struct RemoteClient { runtime: RemoteRuntime, provider: Arc<grpc::Client> }
struct RemoteReader {
    _client: Arc<RemoteClient>,
    receiver: Mutex<std::sync::mpsc::Receiver<ReaderMessage>>,
    cancelled: Arc<AtomicBool>,
    terminal: AtomicU32,
    wake: Arc<Notify>,
    task: Mutex<Option<JoinHandle<()>>>,
}
enum ReaderMessage { Record(acyclic_stream::Record), End, Error(StreamError) }
struct Allocation { ptr: usize, len: usize, capacity: usize, _bytes: Vec<u8> }

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CLIENTS: OnceLock<Mutex<HashMap<u64, Arc<RemoteClient>>>> = OnceLock::new();
static READERS: OnceLock<Mutex<HashMap<u64, Arc<RemoteReader>>>> = OnceLock::new();
static BUFFERS: OnceLock<Mutex<HashMap<u64, Allocation>>> = OnceLock::new();
fn clients() -> &'static Mutex<HashMap<u64, Arc<RemoteClient>>> { CLIENTS.get_or_init(|| Mutex::new(HashMap::new())) }
fn readers() -> &'static Mutex<HashMap<u64, Arc<RemoteReader>>> { READERS.get_or_init(|| Mutex::new(HashMap::new())) }
fn buffers() -> &'static Mutex<HashMap<u64, Allocation>> { BUFFERS.get_or_init(|| Mutex::new(HashMap::new())) }
fn next_id() -> Option<u64> {
    let mut current = NEXT_ID.load(Ordering::Relaxed);
    loop {
        if current == u64::MAX { return None; }
        match NEXT_ID.compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed) { Ok(_) => return Some(current), Err(observed) => current = observed }
    }
}
fn client_lookup(id: u64) -> Option<Arc<RemoteClient>> { clients().lock().ok()?.get(&id).cloned() }
fn reader_lookup(id: u64) -> Option<Arc<RemoteReader>> { readers().lock().ok()?.get(&id).cloned() }
fn empty_buffer() -> AcyclicRemoteBuffer { AcyclicRemoteBuffer { id: 0, ptr: ptr::null_mut(), len: 0, capacity: 0 } }
fn owned_buffer(bytes: impl Into<Vec<u8>>) -> AcyclicRemoteBuffer {
    let mut bytes = bytes.into();
    if bytes.is_empty() { return empty_buffer(); }
    let Some(id) = next_id() else { return empty_buffer() };
    let result = AcyclicRemoteBuffer { id, ptr: bytes.as_mut_ptr(), len: bytes.len(), capacity: bytes.capacity() };
    let allocation = Allocation { ptr: result.ptr as usize, len: result.len, capacity: result.capacity, _bytes: bytes };
    if let Ok(mut all) = buffers().lock() { all.insert(id, allocation); result } else { empty_buffer() }
}
fn message(value: impl Into<String>) -> AcyclicRemoteBuffer { owned_buffer(value.into().into_bytes()) }
fn runtime_reentry() -> bool { tokio::runtime::Handle::try_current().is_ok() }
fn input_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], &'static str> {
    if len == 0 { return Ok(&[]); }
    if ptr.is_null() { return Err("nonempty input has a null pointer"); }
    // SAFETY: C callers provide a valid read-only byte region for this call.
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}
fn input_text<'a>(ptr: *const u8, len: usize) -> Result<&'a str, &'static str> { std::str::from_utf8(input_bytes(ptr, len)?).map_err(|_| "input is not UTF-8") }
fn invalid_append(text: &'static str) -> AcyclicRemoteAppendResult { AcyclicRemoteAppendResult { status: AcyclicRemoteStatus::InvalidArgument, start: 0, end: 0, tail: 0, message: message(text) } }
fn invalid_open(text: &'static str) -> AcyclicRemoteOpenResult { AcyclicRemoteOpenResult { status: AcyclicRemoteStatus::InvalidArgument, reader: 0, message: message(text) } }
fn invalid_next(text: &'static str) -> AcyclicRemoteNextResult { AcyclicRemoteNextResult { status: AcyclicRemoteStatus::InvalidArgument, sequence: 0, value: empty_buffer(), message: message(text) } }
fn provider_append(error: impl ToString) -> AcyclicRemoteAppendResult { AcyclicRemoteAppendResult { status: AcyclicRemoteStatus::ProviderError, start: 0, end: 0, tail: 0, message: message(error.to_string()) } }
fn provider_open(error: impl ToString) -> AcyclicRemoteOpenResult { AcyclicRemoteOpenResult { status: AcyclicRemoteStatus::ProviderError, reader: 0, message: message(error.to_string()) } }
fn provider_next(error: impl ToString) -> AcyclicRemoteNextResult { AcyclicRemoteNextResult { status: AcyclicRemoteStatus::ProviderError, sequence: 0, value: empty_buffer(), message: message(error.to_string()) } }
fn terminal_status(value: u32) -> Option<AcyclicRemoteStatus> { match value { 1 => Some(AcyclicRemoteStatus::End), 3 => Some(AcyclicRemoteStatus::Cancelled), _ => None } }
fn terminal_result(status: AcyclicRemoteStatus) -> AcyclicRemoteNextResult { AcyclicRemoteNextResult { status, sequence: 0, value: empty_buffer(), message: empty_buffer() } }
fn cancel_reader(reader: &RemoteReader) {
    if reader.cancelled.swap(true, Ordering::AcqRel) { return; }
    let _ = reader.terminal.compare_exchange(NO_TERMINAL, AcyclicRemoteStatus::Cancelled as u32, Ordering::AcqRel, Ordering::Acquire);
    reader.wake.notify_waiters();
    if let Ok(mut task) = reader.task.lock() { if let Some(task) = task.take() { task.abort(); } }
}

fn spawn_reader(client: Arc<RemoteClient>, path: String, from: u64, limit: u32, mode: u32) -> Result<RemoteReader, StreamError> {
    if mode == READ_MODE && limit == 0 { return Err(StreamError::InvalidArgument); }
    let stream = client.runtime.block_on(async {
        let stream = StreamClient::new(Arc::clone(&client.provider)).stream(path)?;
        match mode { READ_MODE => stream.read(from, limit).await, FOLLOW_MODE => stream.follow(from).await, _ => Err(StreamError::InvalidArgument) }
    })?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(64);
    let cancelled = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    let task_cancelled = Arc::clone(&cancelled);
    let task_wake = Arc::clone(&wake);
    let task = client.runtime.spawn(async move {
        let mut stream = stream;
        loop {
            if task_cancelled.load(Ordering::Acquire) { return; }
            let next = tokio::select! { _ = task_wake.notified() => return, item = stream.next() => item };
            match next {
                Some(Ok(record)) => { if sender.send(ReaderMessage::Record(record)).is_err() { return; } }
                Some(Err(error)) => { let _ = sender.send(ReaderMessage::Error(error)); return; }
                None => { let _ = sender.send(ReaderMessage::End); return; }
            }
        }
    });
    Ok(RemoteReader { _client: client, receiver: Mutex::new(receiver), cancelled, terminal: AtomicU32::new(NO_TERMINAL), wake, task: Mutex::new(Some(task)) })
}

/// Returns the Rust-owned ABI revision.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_abi_version() -> u32 { ABI_VERSION }

/// Connects to newline-separated TLS endpoints. A nonempty CA buffer enables private-CA TLS.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_client_open(endpoints_ptr: *const u8, endpoints_len: usize, token_ptr: *const u8, token_len: usize, ca_ptr: *const u8, ca_len: usize) -> u64 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() { return 0; }
        let endpoints = match input_text(endpoints_ptr, endpoints_len) { Ok(value) => value, Err(_) => return 0 };
        let token = match input_text(token_ptr, token_len) { Ok(value) => value, Err(_) => return 0 };
        if token.is_empty() { return 0; }
        let list = endpoints.split('\n').filter(|value| !value.is_empty()).collect::<Vec<_>>();
        if list.is_empty() || list.len() > MAX_ENDPOINTS { return 0; }
        let ca = match input_bytes(ca_ptr, ca_len) { Ok(value) => value, Err(_) => return 0 };
        let runtime = match RemoteRuntime::new() { Ok(runtime) => runtime, Err(_) => return 0 };
        let provider = match runtime.block_on(async {
            if ca.is_empty() { grpc::Client::connect_endpoints(list, token).await }
            else { grpc::Client::connect_endpoints_with_ca_certificate(list, token, ca).await }
        }) { Ok(provider) => provider, Err(_) => return 0 };
        let client = Arc::new(RemoteClient { runtime, provider: Arc::new(provider) });
        let Some(id) = next_id() else { return 0 };
        if let Ok(mut all) = clients().lock() { all.insert(id, client); id } else { 0 }
    }));
    result.unwrap_or(0)
}

/// Closes a client. Open readers retain their client until closed.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_client_close(client: u64) { let _ = catch_unwind(AssertUnwindSafe(|| { if let Ok(mut all) = clients().lock() { all.remove(&client); } })); }

/// Appends one record through the canonical Rust remote provider.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_append(client: u64, path_ptr: *const u8, path_len: usize, value_ptr: *const u8, value_len: usize) -> AcyclicRemoteAppendResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() { return invalid_append("synchronous ABI call cannot run from a Tokio runtime"); }
        let Some(client) = client_lookup(client) else { return invalid_append("client handle is invalid"); };
        let path = match input_text(path_ptr, path_len) { Ok(value) => value.to_owned(), Err(error) => return invalid_append(error) };
        let value = match input_bytes(value_ptr, value_len) { Ok(value) => Bytes::copy_from_slice(value), Err(error) => return invalid_append(error) };
        match client.runtime.block_on(async { StreamClient::new(Arc::clone(&client.provider)).stream(path)?.append(value).await }) {
            Ok(AppendOutcome::Committed(receipt)) => AcyclicRemoteAppendResult { status: AcyclicRemoteStatus::Ok, start: receipt.start, end: receipt.end, tail: receipt.tail, message: empty_buffer() },
            Ok(AppendOutcome::TailConflict { actual_tail }) => AcyclicRemoteAppendResult { status: AcyclicRemoteStatus::ProviderError, start: 0, end: 0, tail: actual_tail, message: message("tail conflict") },
            Err(error) => provider_append(error),
        }
    }));
    result.unwrap_or_else(|_| invalid_append("panic contained at ABI boundary"))
}

/// Opens a bounded reader (`mode = 0`) or a live follow reader (`mode = 1`).
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_open(client: u64, path_ptr: *const u8, path_len: usize, from: u64, limit: u32, mode: u32) -> AcyclicRemoteOpenResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() { return invalid_open("synchronous ABI call cannot run from a Tokio runtime"); }
        let Some(client) = client_lookup(client) else { return invalid_open("client handle is invalid"); };
        if mode != READ_MODE && mode != FOLLOW_MODE { return invalid_open("unknown reader mode"); }
        let path = match input_text(path_ptr, path_len) { Ok(value) => value.to_owned(), Err(error) => return invalid_open(error) };
        match spawn_reader(Arc::clone(&client), path, from, limit, mode) {
            Ok(reader) => {
                let Some(id) = next_id() else { return invalid_open("reader ID exhausted") };
                if let Ok(mut all) = readers().lock() { all.insert(id, Arc::new(reader)); AcyclicRemoteOpenResult { status: AcyclicRemoteStatus::Ok, reader: id, message: empty_buffer() } } else { invalid_open("reader registry unavailable") }
            }
            Err(error) => provider_open(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicRemoteOpenResult { status: AcyclicRemoteStatus::Panic, reader: 0, message: message("panic contained at ABI boundary") })
}

/// Pulls one record, returning `Pending` for a live reader when no record arrived promptly.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_next(reader: u64) -> AcyclicRemoteNextResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(reader) = reader_lookup(reader) else { return invalid_next("reader handle is invalid"); };
        if let Some(status) = terminal_status(reader.terminal.load(Ordering::Acquire)) { return terminal_result(status); }
        if reader.cancelled.load(Ordering::Acquire) { return terminal_result(AcyclicRemoteStatus::Cancelled); }
        let receiver = match reader.receiver.lock() { Ok(receiver) => receiver, Err(_) => return invalid_next("reader mutex poisoned") };
        match receiver.recv_timeout(POLL_INTERVAL) {
            Ok(ReaderMessage::Record(record)) => AcyclicRemoteNextResult { status: AcyclicRemoteStatus::Ok, sequence: record.sequence, value: owned_buffer(record.value.to_vec()), message: empty_buffer() },
            Ok(ReaderMessage::End) => { reader.terminal.store(AcyclicRemoteStatus::End as u32, Ordering::Release); terminal_result(AcyclicRemoteStatus::End) }
            Ok(ReaderMessage::Error(error)) => { reader.terminal.store(AcyclicRemoteStatus::ProviderError as u32, Ordering::Release); provider_next(error) }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => AcyclicRemoteNextResult { status: AcyclicRemoteStatus::Pending, sequence: 0, value: empty_buffer(), message: empty_buffer() },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => { reader.terminal.store(AcyclicRemoteStatus::ProviderError as u32, Ordering::Release); provider_next("reader task disconnected") }
        }
    }));
    result.unwrap_or_else(|_| invalid_next("panic contained at ABI boundary"))
}

/// Cancels a reader and wakes its Rust stream task.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_cancel(reader: u64) { let _ = catch_unwind(AssertUnwindSafe(|| { if let Some(reader) = reader_lookup(reader) { cancel_reader(&reader); } })); }

/// Closes and removes a reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_reader_close(reader: u64) { let _ = catch_unwind(AssertUnwindSafe(|| { if let Ok(mut all) = readers().lock() { if let Some(reader) = all.remove(&reader) { cancel_reader(&reader); } } })); }

/// Releases one buffer returned by the ABI.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_buffer_release(buffer: AcyclicRemoteBuffer) -> AcyclicRemoteStatus {
    if buffer.id == 0 { return AcyclicRemoteStatus::Ok; }
    match buffers().lock() {
        Ok(mut all) => match all.get(&buffer.id) {
            Some(value) if value.ptr == buffer.ptr as usize && value.len == buffer.len && value.capacity == buffer.capacity => { all.remove(&buffer.id); AcyclicRemoteStatus::Ok }
            _ => AcyclicRemoteStatus::InvalidArgument,
        },
        Err(_) => AcyclicRemoteStatus::Panic,
    }
}

/// Releases a result's diagnostic buffer.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_append_result_release(result: AcyclicRemoteAppendResult) { let _ = acyclic_remote_buffer_release(result.message); }
/// Releases an open result and any unclaimed reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_open_result_release(result: AcyclicRemoteOpenResult) { let _ = acyclic_remote_buffer_release(result.message); if result.reader != 0 { acyclic_remote_reader_close(result.reader); } }
/// Releases a next result's value and diagnostic buffers.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_remote_next_result_release(result: AcyclicRemoteNextResult) { let _ = acyclic_remote_buffer_release(result.value); let _ = acyclic_remote_buffer_release(result.message); }
