//! Bounded embedded SDK proof over the canonical Rust `Stream` memory engine.
//!
//! The exported surface deliberately contains no Rust references, futures, trait objects, or
//! layout-dependent values.  A caller owns an opaque engine and reader handle, copies bytes into
//! an append call, pulls records with `next`, and releases returned buffers explicitly.

use std::{
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr, slice,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use acyclic_stream::{AppendOutcome, MemoryLimits, MemoryStream, StreamClient, StreamError};
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::{runtime::Runtime, sync::Notify, task::JoinHandle};

#[cfg(feature = "uniffi")]
mod uniffi_polling;
#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

const ABI_VERSION: u32 = 1;
const READ_MODE: u32 = 0;
const FOLLOW_MODE: u32 = 1;
const QUEUE_LIMIT: usize = 64;
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const NO_TERMINAL: u32 = u32::MAX;

/// Stable result categories for the prototype boundary.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcyclicStatus {
    /// The operation returned a value.
    Ok = 0,
    /// A finite reader reached its end.
    End = 1,
    /// The reader has not received a record yet.
    Pending = 2,
    /// The caller cancelled or closed the reader.
    Cancelled = 3,
    /// The caller supplied an invalid argument or handle.
    InvalidArgument = 4,
    /// The canonical provider returned an error.
    ProviderError = 5,
    /// The bounded bridge queue rejected another record.
    Capacity = 6,
    /// A panic was contained at the ABI boundary.
    Panic = 7,
}

/// Owned bytes returned by the ABI. Release every nonempty buffer exactly once.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AcyclicBuffer {
    /// Monotonic allocation identity used for checked release.
    pub id: u64,
    /// Pointer to bytes owned by the ABI until explicit release, or null for an empty buffer.
    pub ptr: *mut u8,
    /// Number of initialized bytes.
    pub len: usize,
    /// Allocation capacity required by `acyclic_buffer_release`.
    pub capacity: usize,
}

/// Result of an append operation.
#[repr(C)]
pub struct AcyclicAppendResult {
    /// Result category.
    pub status: AcyclicStatus,
    /// First sequence when committed.
    pub start: u64,
    /// Exclusive end sequence when committed.
    pub end: u64,
    /// Resulting stream tail when committed, or observed tail for a conflict.
    pub tail: u64,
    /// UTF-8 diagnostic bytes, if any.
    pub message: AcyclicBuffer,
}

/// Result of opening a reader.
#[repr(C)]
pub struct AcyclicOpenResult {
    /// Result category.
    pub status: AcyclicStatus,
    /// Owned reader handle on success.
    pub reader: u64,
    /// UTF-8 diagnostic bytes, if any.
    pub message: AcyclicBuffer,
}

/// Result of one pull from a reader.
#[repr(C)]
pub struct AcyclicNextResult {
    /// Result category.
    pub status: AcyclicStatus,
    /// Record sequence on success.
    pub sequence: u64,
    /// Record payload on success.
    pub value: AcyclicBuffer,
    /// UTF-8 diagnostic bytes, if any.
    pub message: AcyclicBuffer,
}

/// Opaque process-local engine handle.
pub struct AcyclicEngine {
    runtime: EngineRuntime,
    provider: Arc<MemoryStream>,
}

/// Owns the private Tokio runtime without synchronously dropping it from a Tokio worker.
///
/// Synchronous ABI calls reject runtime reentry. The custom drop also handles a reader closed
/// from an async callback: Tokio's normal `Drop` panics when it waits for worker threads from an
/// async context, while `shutdown_background` is safe at this ownership boundary.
struct EngineRuntime(Option<Runtime>);

impl EngineRuntime {
    fn new() -> Result<Self, ()> {
        Runtime::new()
            .map(|runtime| Self(Some(runtime)))
            .map_err(|_| ())
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.0
            .as_ref()
            .expect("engine runtime is alive")
            .block_on(future)
    }

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0
            .as_ref()
            .expect("engine runtime is alive")
            .spawn(future)
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.0.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

struct AcyclicReader {
    _engine: Arc<AcyclicEngine>,
    receiver: Mutex<std::sync::mpsc::Receiver<ReaderMessage>>,
    cancel_sender: std::sync::mpsc::SyncSender<ReaderMessage>,
    cancelled: Arc<AtomicBool>,
    terminal: AtomicU32,
    /// Serializes the point at which a pulled record or terminal message becomes observable.
    /// Cancellation marks `cancelled` before taking this lock, so a pull that has not committed
    /// a record yet will always observe the cancellation, even if the record is already queued.
    delivery: Mutex<()>,
    queued: Arc<AtomicUsize>,
    wake: Arc<Notify>,
    task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    test_hook: Mutex<Option<ReaderTestHook>>,
}

#[cfg(test)]
struct ReaderTestHook {
    message_received: std::sync::mpsc::Sender<()>,
    allow_commit: Arc<std::sync::Barrier>,
}

enum ReaderMessage {
    Record(acyclic_stream::Record),
    End,
    Cancelled,
    Error(StreamError),
}

struct BufferAllocation {
    ptr: usize,
    len: usize,
    capacity: usize,
    _bytes: Vec<u8>,
}

// IDs are monotonic and never reused. A stale foreign handle therefore cannot alias a later
// engine, reader, or buffer, and failed lookups never dereference the caller value.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static ENGINES: OnceLock<Mutex<HashMap<u64, Arc<AcyclicEngine>>>> = OnceLock::new();
static READERS: OnceLock<Mutex<HashMap<u64, Arc<AcyclicReader>>>> = OnceLock::new();
static BUFFERS: OnceLock<Mutex<HashMap<u64, BufferAllocation>>> = OnceLock::new();

fn engines() -> &'static Mutex<HashMap<u64, Arc<AcyclicEngine>>> {
    ENGINES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn readers() -> &'static Mutex<HashMap<u64, Arc<AcyclicReader>>> {
    READERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn buffers() -> &'static Mutex<HashMap<u64, BufferAllocation>> {
    BUFFERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn allocate_id(counter: &AtomicU64) -> Option<u64> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current == u64::MAX {
            return None;
        }
        match counter.compare_exchange_weak(
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

fn next_id() -> Option<u64> {
    allocate_id(&NEXT_ID)
}

fn engine_lookup(id: u64) -> Option<Arc<AcyclicEngine>> {
    engines().lock().ok()?.get(&id).cloned()
}

fn reader_lookup(id: u64) -> Option<Arc<AcyclicReader>> {
    readers().lock().ok()?.get(&id).cloned()
}

/// The ABI is deliberately synchronous. Calling `Runtime::block_on` from an async worker would
/// panic (or starve a current-thread executor), so callers must cross this boundary from a plain
/// thread. Returning `true` lets every exported operation fail deterministically instead.
fn runtime_reentry() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

fn terminal_status(value: u32) -> Option<AcyclicStatus> {
    match value {
        value if value == AcyclicStatus::End as u32 => Some(AcyclicStatus::End),
        value if value == AcyclicStatus::Cancelled as u32 => Some(AcyclicStatus::Cancelled),
        value if value == AcyclicStatus::Capacity as u32 => Some(AcyclicStatus::Capacity),
        value if value == AcyclicStatus::ProviderError as u32 => Some(AcyclicStatus::ProviderError),
        _ => None,
    }
}

fn publish_terminal(reader: &AcyclicReader, status: AcyclicStatus) -> AcyclicStatus {
    let _ = reader.terminal.compare_exchange(
        NO_TERMINAL,
        status as u32,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    terminal_status(reader.terminal.load(Ordering::Acquire)).unwrap_or(AcyclicStatus::Panic)
}

fn terminal_result(status: AcyclicStatus) -> AcyclicNextResult {
    AcyclicNextResult {
        status,
        sequence: 0,
        value: empty_buffer(),
        message: empty_buffer(),
    }
}

fn reserve_record_slot(queued: &AtomicUsize) -> bool {
    let mut observed = queued.load(Ordering::Acquire);
    while observed < QUEUE_LIMIT {
        match queued.compare_exchange_weak(
            observed,
            observed + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(next) => observed = next,
        }
    }
    false
}

fn cancel_reader(reader: &AcyclicReader) {
    if reader.cancelled.swap(true, Ordering::AcqRel) {
        return;
    }
    // Cancellation wins over queued records and terminal messages that have not already been
    // observed. Marking the flag before taking the delivery lock means a pull that has received a
    // record but has not committed it yet will see cancellation at its delivery linearization
    // point. The compare-and-set makes a terminal result stable on all later pulls.
    if let Ok(_delivery) = reader.delivery.lock() {
        publish_terminal(reader, AcyclicStatus::Cancelled);
    } else {
        // A poisoned lock can only follow a panic in the delivery section. Preserve the terminal
        // ABI guarantee even when recovering from that panic.
        publish_terminal(reader, AcyclicStatus::Cancelled);
    }
    reader.wake.notify_waiters();
    let _ = reader.cancel_sender.try_send(ReaderMessage::Cancelled);
    if let Ok(mut task) = reader.task.lock() {
        if let Some(task) = task.take() {
            task.abort();
        }
    }
}

fn empty_buffer() -> AcyclicBuffer {
    AcyclicBuffer {
        id: 0,
        ptr: ptr::null_mut(),
        len: 0,
        capacity: 0,
    }
}

fn owned_buffer(bytes: impl Into<Vec<u8>>) -> AcyclicBuffer {
    let mut bytes = bytes.into();
    if bytes.is_empty() {
        return empty_buffer();
    }
    let Some(id) = next_id() else {
        return empty_buffer();
    };
    let result = AcyclicBuffer {
        id,
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
        capacity: bytes.capacity(),
    };
    let allocation = BufferAllocation {
        ptr: result.ptr as usize,
        len: result.len,
        capacity: result.capacity,
        _bytes: bytes,
    };
    if let Ok(mut allocations) = buffers().lock() {
        allocations.insert(id, allocation);
    } else {
        return empty_buffer();
    }
    result
}

fn message(text: impl Into<String>) -> AcyclicBuffer {
    owned_buffer(text.into().into_bytes())
}

fn invalid_result(text: &'static str) -> AcyclicAppendResult {
    AcyclicAppendResult {
        status: AcyclicStatus::InvalidArgument,
        start: 0,
        end: 0,
        tail: 0,
        message: message(text),
    }
}

fn invalid_open(text: &'static str) -> AcyclicOpenResult {
    AcyclicOpenResult {
        status: AcyclicStatus::InvalidArgument,
        reader: 0,
        message: message(text),
    }
}

fn invalid_next(text: &'static str) -> AcyclicNextResult {
    AcyclicNextResult {
        status: AcyclicStatus::InvalidArgument,
        sequence: 0,
        value: empty_buffer(),
        message: message(text),
    }
}

fn provider_append_error(error: StreamError) -> AcyclicAppendResult {
    AcyclicAppendResult {
        status: AcyclicStatus::ProviderError,
        start: 0,
        end: 0,
        tail: 0,
        message: message(error.to_string()),
    }
}

fn provider_open_error(error: StreamError) -> AcyclicOpenResult {
    AcyclicOpenResult {
        status: AcyclicStatus::ProviderError,
        reader: 0,
        message: message(error.to_string()),
    }
}

fn provider_next_error(error: StreamError) -> AcyclicNextResult {
    let status = if error == StreamError::Capacity {
        AcyclicStatus::Capacity
    } else {
        AcyclicStatus::ProviderError
    };
    AcyclicNextResult {
        status,
        sequence: 0,
        value: empty_buffer(),
        message: message(error.to_string()),
    }
}

fn runtime_reentry_append() -> AcyclicAppendResult {
    AcyclicAppendResult {
        status: AcyclicStatus::Panic,
        start: 0,
        end: 0,
        tail: 0,
        message: message("synchronous ABI call cannot run from a Tokio runtime"),
    }
}

fn runtime_reentry_open() -> AcyclicOpenResult {
    AcyclicOpenResult {
        status: AcyclicStatus::Panic,
        reader: 0,
        message: message("synchronous ABI call cannot run from a Tokio runtime"),
    }
}

fn runtime_reentry_next() -> AcyclicNextResult {
    AcyclicNextResult {
        status: AcyclicStatus::Panic,
        sequence: 0,
        value: empty_buffer(),
        message: message("synchronous ABI call cannot run from a Tokio runtime"),
    }
}

unsafe fn input_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], &'static str> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err("nonempty input has a null pointer");
    }
    // SAFETY: the caller owns a valid read-only region for `len` bytes for this call.
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

unsafe fn input_path<'a>(ptr: *const u8, len: usize) -> Result<&'a str, &'static str> {
    let bytes = unsafe { input_bytes(ptr, len)? };
    std::str::from_utf8(bytes).map_err(|_| "path is not UTF-8")
}

fn spawn_reader(
    runtime: &EngineRuntime,
    engine: Arc<AcyclicEngine>,
    provider: Arc<MemoryStream>,
    path: String,
    from: u64,
    limit: u32,
    mode: u32,
) -> Result<AcyclicReader, StreamError> {
    let stream = runtime.block_on(async {
        let stream = StreamClient::new(provider).stream(path)?;
        match mode {
            READ_MODE => stream.read(from, limit).await,
            FOLLOW_MODE => stream.follow(from).await,
            _ => Err(StreamError::InvalidArgument),
        }
    })?;
    // Two extra slots are reserved for a terminal capacity error and cancellation wakeup; at most
    // QUEUE_LIMIT records are admitted, and the channel itself is bounded.
    let (sender, receiver) = std::sync::mpsc::sync_channel(QUEUE_LIMIT + 2);
    let cancelled = Arc::new(AtomicBool::new(false));
    let queued = Arc::new(AtomicUsize::new(0));
    let wake = Arc::new(Notify::new());
    let task_cancelled = Arc::clone(&cancelled);
    let task_queued = Arc::clone(&queued);
    let task_wake = Arc::clone(&wake);
    let task_sender = sender.clone();
    let task = runtime.spawn(async move {
        let mut stream = stream;
        loop {
            if task_cancelled.load(Ordering::Acquire) {
                return;
            }
            let next = tokio::select! {
                _ = task_wake.notified() => return,
                item = stream.next() => item,
            };
            match next {
                Some(Ok(record)) => {
                    // Reserve a record slot with a checked CAS. This keeps the accounting bounded
                    // even if a future producer path is added or a consumer disappears.
                    if !reserve_record_slot(&task_queued) {
                        let _ = task_sender.try_send(ReaderMessage::Error(StreamError::Capacity));
                        return;
                    }
                    if task_sender.try_send(ReaderMessage::Record(record)).is_err() {
                        // `try_send` did not transfer ownership into the channel. Return the
                        // reservation so the count cannot drift upward after disconnect/full.
                        task_queued.fetch_sub(1, Ordering::AcqRel);
                        return;
                    }
                }
                Some(Err(error)) => {
                    let _ = task_sender.try_send(ReaderMessage::Error(error));
                    return;
                }
                None => {
                    let _ = task_sender.try_send(ReaderMessage::End);
                    return;
                }
            }
        }
    });
    Ok(AcyclicReader {
        _engine: engine,
        receiver: Mutex::new(receiver),
        cancel_sender: sender,
        cancelled,
        terminal: AtomicU32::new(NO_TERMINAL),
        delivery: Mutex::new(()),
        queued,
        wake,
        task: Mutex::new(Some(task)),
        #[cfg(test)]
        test_hook: Mutex::new(None),
    })
}

/// Returns the ABI version used by this prototype.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_abi_version() -> u32 {
    ABI_VERSION
}

/// Opens a process-local engine backed by the canonical Rust `MemoryStream`.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_engine_open() -> u64 {
    match catch_unwind(|| {
        Arc::new(AcyclicEngine {
            runtime: EngineRuntime::new().expect("tokio runtime"),
            provider: Arc::new(MemoryStream::new(MemoryLimits::default())),
        })
    }) {
        Ok(engine) => {
            let Some(id) = next_id() else {
                return 0;
            };
            if let Ok(mut engines) = engines().lock() {
                engines.insert(id, engine);
                id
            } else {
                0
            }
        }
        Err(_) => 0,
    }
}

/// Closes an engine ID. Reader IDs retain their own engine reference until closed.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_engine_close(engine: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(mut engines) = engines().lock() {
            engines.remove(&engine);
        }
    }));
}

/// Appends one copied payload to a path.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_engine_append(
    engine: u64,
    path_ptr: *const u8,
    path_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> AcyclicAppendResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return runtime_reentry_append();
        }
        let Some(engine) = engine_lookup(engine) else {
            return invalid_result("engine handle is null");
        };
        let path = match unsafe { input_path(path_ptr, path_len) } {
            Ok(path) => path.to_owned(),
            Err(error) => return invalid_result(error),
        };
        let value = match unsafe { input_bytes(value_ptr, value_len) } {
            Ok(value) => value.to_vec(),
            Err(error) => return invalid_result(error),
        };
        match engine.runtime.block_on(async {
            StreamClient::new(Arc::clone(&engine.provider))
                .stream(path)?
                .append(Bytes::from(value))
                .await
        }) {
            Ok(AppendOutcome::Committed(receipt)) => AcyclicAppendResult {
                status: AcyclicStatus::Ok,
                start: receipt.start,
                end: receipt.end,
                tail: receipt.tail,
                message: empty_buffer(),
            },
            Ok(AppendOutcome::TailConflict { actual_tail }) => AcyclicAppendResult {
                status: AcyclicStatus::ProviderError,
                start: 0,
                end: 0,
                tail: actual_tail,
                message: message("tail conflict"),
            },
            Err(error) => provider_append_error(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicAppendResult {
        status: AcyclicStatus::Panic,
        start: 0,
        end: 0,
        tail: 0,
        message: message("panic contained at ABI boundary"),
    })
}

/// Opens a finite (`mode = 0`) or live (`mode = 1`) reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_reader_open(
    engine: u64,
    path_ptr: *const u8,
    path_len: usize,
    from: u64,
    limit: u32,
    mode: u32,
) -> AcyclicOpenResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if runtime_reentry() {
            return runtime_reentry_open();
        }
        let Some(engine) = engine_lookup(engine) else {
            return invalid_open("engine handle is null");
        };
        if mode == READ_MODE && limit == 0 {
            return invalid_open("read limit must be nonzero");
        }
        if mode != READ_MODE && mode != FOLLOW_MODE {
            return invalid_open("unknown reader mode");
        }
        let path = match unsafe { input_path(path_ptr, path_len) } {
            Ok(path) => path.to_owned(),
            Err(error) => return invalid_open(error),
        };
        match spawn_reader(
            &engine.runtime,
            Arc::clone(&engine),
            Arc::clone(&engine.provider),
            path,
            from,
            limit,
            mode,
        ) {
            Ok(reader) => {
                let Some(id) = next_id() else {
                    return invalid_open("reader ID exhausted");
                };
                let reader = Arc::new(reader);
                if let Ok(mut readers) = readers().lock() {
                    readers.insert(id, reader);
                    AcyclicOpenResult {
                        status: AcyclicStatus::Ok,
                        reader: id,
                        message: empty_buffer(),
                    }
                } else {
                    invalid_open("reader registry unavailable")
                }
            }
            Err(error) => provider_open_error(error),
        }
    }));
    result.unwrap_or_else(|_| AcyclicOpenResult {
        status: AcyclicStatus::Panic,
        reader: 0,
        message: message("panic contained at ABI boundary"),
    })
}

/// Pulls one record, waiting briefly for a live reader before returning `Pending`.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_reader_next(reader: u64) -> AcyclicNextResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let Some(reader) = reader_lookup(reader) else {
            return invalid_next("reader handle is null");
        };
        let terminal = reader.terminal.load(Ordering::Acquire);
        if terminal != NO_TERMINAL {
            return terminal_result(terminal_status(terminal).unwrap_or(AcyclicStatus::Panic));
        }
        if reader.cancelled.load(Ordering::Acquire) {
            return terminal_result(publish_terminal(&reader, AcyclicStatus::Cancelled));
        }
        if runtime_reentry() {
            return runtime_reentry_next();
        }
        // The bridge task is owned by the engine runtime. Drive one scheduler turn before
        // entering the foreign blocking pull so finite readers make progress even when no other
        // ABI operation is currently calling `block_on`.
        let _ = reader._engine.runtime.block_on(async {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        });
        let receiver = match reader.receiver.lock() {
            Ok(receiver) => receiver,
            Err(_) => {
                return AcyclicNextResult {
                    status: AcyclicStatus::Panic,
                    sequence: 0,
                    value: empty_buffer(),
                    message: message("reader mutex poisoned"),
                };
            }
        };
        let received = receiver.recv_timeout(POLL_INTERVAL);
        drop(receiver);
        #[cfg(test)]
        if let Ok(mut hook) = reader.test_hook.lock() {
            if let Some(hook) = hook.take() {
                let _ = hook.message_received.send(());
                hook.allow_commit.wait();
            }
        }
        // Cancellation has a linearization point before this check. A queued record or terminal
        // message must never overwrite a cancellation that raced while the foreign pull waited.
        if let Some(status) = terminal_status(reader.terminal.load(Ordering::Acquire)) {
            return terminal_result(status);
        }
        match received {
            Ok(ReaderMessage::Record(record)) => {
                reader.queued.fetch_sub(1, Ordering::AcqRel);
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                if reader.cancelled.load(Ordering::Acquire) {
                    let status = publish_terminal(&reader, AcyclicStatus::Cancelled);
                    drop(delivery);
                    return terminal_result(status);
                }
                let result = AcyclicNextResult {
                    status: AcyclicStatus::Ok,
                    sequence: record.sequence,
                    value: owned_buffer(record.value.to_vec()),
                    message: empty_buffer(),
                };
                drop(delivery);
                result
            }
            Ok(ReaderMessage::End) => {
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                let status = if reader.cancelled.load(Ordering::Acquire) {
                    publish_terminal(&reader, AcyclicStatus::Cancelled)
                } else {
                    publish_terminal(&reader, AcyclicStatus::End)
                };
                drop(delivery);
                terminal_result(status)
            }
            Ok(ReaderMessage::Cancelled) => {
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                let status = publish_terminal(&reader, AcyclicStatus::Cancelled);
                drop(delivery);
                terminal_result(status)
            }
            Ok(ReaderMessage::Error(error)) => {
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                if reader.cancelled.load(Ordering::Acquire) {
                    let status = publish_terminal(&reader, AcyclicStatus::Cancelled);
                    drop(delivery);
                    return terminal_result(status);
                }
                let result = provider_next_error(error);
                let status = publish_terminal(&reader, result.status);
                drop(delivery);
                if status == result.status {
                    result
                } else {
                    terminal_result(status)
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                if let Some(status) = terminal_status(reader.terminal.load(Ordering::Acquire)) {
                    drop(delivery);
                    terminal_result(status)
                } else if reader.cancelled.load(Ordering::Acquire) {
                    let status = publish_terminal(&reader, AcyclicStatus::Cancelled);
                    drop(delivery);
                    terminal_result(status)
                } else {
                    let result = AcyclicNextResult {
                        status: AcyclicStatus::Pending,
                        sequence: 0,
                        value: empty_buffer(),
                        message: empty_buffer(),
                    };
                    drop(delivery);
                    result
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                let delivery = match reader.delivery.lock() {
                    Ok(delivery) => delivery,
                    Err(_) => {
                        return AcyclicNextResult {
                            status: AcyclicStatus::Panic,
                            sequence: 0,
                            value: empty_buffer(),
                            message: message("reader delivery mutex poisoned"),
                        };
                    }
                };
                let status = publish_terminal(&reader, AcyclicStatus::Cancelled);
                drop(delivery);
                terminal_result(status)
            }
        }
    }));
    result.unwrap_or_else(|_| AcyclicNextResult {
        status: AcyclicStatus::Panic,
        sequence: 0,
        value: empty_buffer(),
        message: message("panic contained at ABI boundary"),
    })
}

/// Cancels a reader and wakes its async task.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_reader_cancel(reader: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(reader) = reader_lookup(reader) else {
            return;
        };
        cancel_reader(&reader);
    }));
}

/// Closes and frees a reader handle.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_embedded_reader_close(reader: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(mut readers) = readers().lock() {
            if let Some(reader) = readers.remove(&reader) {
                cancel_reader(&reader);
            }
        }
    }));
}

/// Releases one owned byte buffer returned by this ABI.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_buffer_release(buffer: AcyclicBuffer) -> AcyclicStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if buffer.id == 0 {
            return if buffer.ptr.is_null() && buffer.len == 0 && buffer.capacity == 0 {
                AcyclicStatus::Ok
            } else {
                AcyclicStatus::InvalidArgument
            };
        }
        if let Ok(mut allocations) = buffers().lock() {
            let matches = allocations.get(&buffer.id).is_some_and(|allocation| {
                allocation.ptr == buffer.ptr as usize
                    && allocation.len == buffer.len
                    && allocation.capacity == buffer.capacity
            });
            if matches {
                // Removing drops the Rust-owned allocation. A second release finds no ID.
                allocations.remove(&buffer.id);
                AcyclicStatus::Ok
            } else {
                AcyclicStatus::InvalidArgument
            }
        } else {
            AcyclicStatus::Panic
        }
    }))
    .unwrap_or(AcyclicStatus::Panic)
}

/// Releases all buffers in an append result.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_append_result_release(result: AcyclicAppendResult) {
    acyclic_buffer_release(result.message);
}

/// Releases all buffers in an open result and closes an unclaimed reader.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_open_result_release(result: AcyclicOpenResult) {
    acyclic_buffer_release(result.message);
    if result.reader != 0 {
        acyclic_embedded_reader_close(result.reader);
    }
}

/// Transfers the reader handle out of an open result. The result can then be released without
/// closing the transferred reader. Passing null is a no-op and returns zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn acyclic_open_result_take_reader(result: *mut AcyclicOpenResult) -> u64 {
    catch_unwind(AssertUnwindSafe(|| {
        if result.is_null() {
            return 0;
        }
        // SAFETY: the caller owns a valid mutable result pointer for this call.
        let result = unsafe { &mut *result };
        let reader = result.reader;
        result.reader = 0;
        reader
    }))
    .unwrap_or(0)
}

/// Releases all buffers in a next result.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_next_result_release(result: AcyclicNextResult) {
    acyclic_buffer_release(result.value);
    acyclic_buffer_release(result.message);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(value: &[u8]) -> (*const u8, usize) {
        (value.as_ptr(), value.len())
    }

    fn install_message_hook(
        reader: u64,
    ) -> (std::sync::mpsc::Receiver<()>, Arc<std::sync::Barrier>) {
        let (message_received, received) = std::sync::mpsc::channel();
        let allow_commit = Arc::new(std::sync::Barrier::new(2));
        let state = reader_lookup(reader).expect("reader remains registered");
        *state.test_hook.lock().expect("test hook mutex is healthy") = Some(ReaderTestHook {
            message_received,
            allow_commit: Arc::clone(&allow_commit),
        });
        (received, allow_commit)
    }

    #[test]
    fn abi_append_read_and_release_uses_real_memory_provider() {
        let engine = acyclic_embedded_engine_open();
        assert_ne!(engine, 0);
        let path = b"actors/demo";
        let value = b"hello";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let appended =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(appended.status, AcyclicStatus::Ok);
        assert_eq!((appended.start, appended.end, appended.tail), (0, 1, 1));
        acyclic_append_result_release(appended);

        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 10, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let next = acyclic_embedded_reader_next(reader);
        assert_eq!(next.status, AcyclicStatus::Ok);
        assert_eq!(next.sequence, 0);
        // SAFETY: the result owns this initialized payload.
        assert_eq!(
            unsafe { std::slice::from_raw_parts(next.value.ptr, next.value.len) },
            value
        );
        acyclic_next_result_release(next);
        let end = acyclic_embedded_reader_next(reader);
        assert_eq!(end.status, AcyclicStatus::End);
        acyclic_next_result_release(end);
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
        acyclic_embedded_reader_close(reader);
        assert_eq!(
            acyclic_embedded_reader_next(reader).status,
            AcyclicStatus::InvalidArgument
        );
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn follow_receives_append_and_cancels() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/live";
        let (path_ptr, path_len) = bytes(path);
        let initial = b"initial";
        let (initial_ptr, initial_len) = bytes(initial);
        let initial_append =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, initial_ptr, initial_len);
        assert_eq!(initial_append.status, AcyclicStatus::Ok);
        acyclic_append_result_release(initial_append);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 0, FOLLOW_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let value = b"live";
        let (value_ptr, value_len) = bytes(value);
        let appended =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(appended.status, AcyclicStatus::Ok);
        acyclic_append_result_release(appended);
        let next = acyclic_embedded_reader_next(reader);
        assert_eq!(next.status, AcyclicStatus::Ok);
        assert_eq!(next.sequence, 0);
        acyclic_next_result_release(next);
        let live = acyclic_embedded_reader_next(reader);
        assert_eq!(live.status, AcyclicStatus::Ok);
        assert_eq!(live.sequence, 1);
        acyclic_next_result_release(live);
        acyclic_embedded_reader_cancel(reader);
        let cancelled = acyclic_embedded_reader_next(reader);
        assert_eq!(cancelled.status, AcyclicStatus::Cancelled);
        acyclic_next_result_release(cancelled);
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn invalid_input_and_panics_stay_inside_status_boundary() {
        let invalid = acyclic_embedded_engine_append(0, ptr::null(), 0, ptr::null(), 0);
        assert_eq!(invalid.status, AcyclicStatus::InvalidArgument);
        acyclic_append_result_release(invalid);
        assert_eq!(acyclic_embedded_abi_version(), ABI_VERSION);
    }

    #[test]
    fn provider_failures_are_explicit_and_valid_calls_recover() {
        let engine = acyclic_embedded_engine_open();
        let path = b"recovery/provider-error";
        let (path_ptr, path_len) = bytes(path);

        let invalid_path = acyclic_embedded_engine_append(engine, ptr::null(), 0, ptr::null(), 0);
        assert_eq!(invalid_path.status, AcyclicStatus::ProviderError);
        assert!(invalid_path.message.len > 0);
        acyclic_append_result_release(invalid_path);

        let out_of_range =
            acyclic_embedded_reader_open(engine, path_ptr, path_len, 1, 1, READ_MODE);
        assert_eq!(out_of_range.status, AcyclicStatus::ProviderError);
        assert!(out_of_range.message.len > 0);
        acyclic_open_result_release(out_of_range);

        let value = b"recovered";
        let (value_ptr, value_len) = bytes(value);
        let append =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(append.status, AcyclicStatus::Ok);
        acyclic_append_result_release(append);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 1, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let next = acyclic_embedded_reader_next(opened.reader);
        assert_eq!(next.status, AcyclicStatus::Ok);
        acyclic_next_result_release(next);
        acyclic_embedded_reader_close(opened.reader);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn id_overflow_fails_closed_without_mutating_global_ids() {
        let counter = AtomicU64::new(u64::MAX);
        assert_eq!(allocate_id(&counter), None);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn queue_admission_is_bounded_and_releases_failed_reservations() {
        let queued = AtomicUsize::new(QUEUE_LIMIT);
        assert!(!reserve_record_slot(&queued));
        assert_eq!(queued.load(Ordering::Acquire), QUEUE_LIMIT);
        queued.fetch_sub(1, Ordering::AcqRel);
        assert!(reserve_record_slot(&queued));
        assert_eq!(queued.load(Ordering::Acquire), QUEUE_LIMIT);
    }

    #[test]
    fn malformed_empty_buffer_metadata_is_rejected() {
        let malformed = AcyclicBuffer {
            id: 0,
            ptr: 1usize as *mut u8,
            len: 0,
            capacity: 0,
        };
        assert_eq!(
            acyclic_buffer_release(malformed),
            AcyclicStatus::InvalidArgument
        );
        assert_eq!(acyclic_buffer_release(empty_buffer()), AcyclicStatus::Ok);
    }

    #[test]
    fn synchronous_calls_reject_tokio_runtime_reentry() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/runtime-reentry";
        let value = b"value";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let runtime = Runtime::new().expect("test runtime");
        let append = runtime.block_on(async {
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len)
        });
        assert_eq!(append.status, AcyclicStatus::Panic);
        acyclic_append_result_release(append);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn closing_last_engine_reference_from_tokio_worker_is_safe() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/runtime-close";
        let value = b"value";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let append =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(append.status, AcyclicStatus::Ok);
        acyclic_append_result_release(append);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 1, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let runtime = Runtime::new().expect("test runtime");
        runtime.block_on(async move {
            acyclic_embedded_engine_close(engine);
            acyclic_embedded_reader_close(reader);
        });
    }

    #[test]
    fn cancellation_wins_over_a_queued_record_and_stays_terminal() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/cancel-queued";
        let value = b"queued";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let append =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(append.status, AcyclicStatus::Ok);
        acyclic_append_result_release(append);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 0, FOLLOW_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let (message_received, allow_commit) = install_message_hook(reader);
        let pull = std::thread::spawn(move || {
            let result = acyclic_embedded_reader_next(reader);
            let status = result.status;
            acyclic_next_result_release(result);
            status
        });
        message_received
            .recv()
            .expect("pull received the queued record before commit");
        acyclic_embedded_reader_cancel(reader);
        allow_commit.wait();
        let status = pull.join().expect("reader pull did not panic");
        assert_eq!(status, AcyclicStatus::Cancelled);
        let repeated = acyclic_embedded_reader_next(reader);
        assert_eq!(repeated.status, AcyclicStatus::Cancelled);
        acyclic_next_result_release(repeated);
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn cancel_wakes_a_blocked_reader_and_repeats_terminal_status() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/cancel-wakeup";
        let (path_ptr, path_len) = bytes(path);
        let value = b"initial";
        let (value_ptr, value_len) = bytes(value);
        let appended =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(appended.status, AcyclicStatus::Ok);
        acyclic_append_result_release(appended);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 1, 0, FOLLOW_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let (message_received, allow_commit) = install_message_hook(reader);
        let blocked = std::thread::spawn(move || {
            let result = acyclic_embedded_reader_next(reader);
            let status = result.status;
            acyclic_next_result_release(result);
            status
        });
        message_received
            .recv()
            .expect("pull reached the pending commit point");
        acyclic_embedded_reader_cancel(reader);
        allow_commit.wait();
        let cancelled = blocked.join().expect("reader pull did not panic");
        assert_eq!(cancelled, AcyclicStatus::Cancelled);
        assert_eq!(
            acyclic_embedded_reader_next(reader).status,
            AcyclicStatus::Cancelled
        );
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn cancellation_prevents_a_queued_end_from_overwriting_cancelled() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/cancel-end";
        let value = b"one";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let append =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(append.status, AcyclicStatus::Ok);
        acyclic_append_result_release(append);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 1, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let first = acyclic_embedded_reader_next(reader);
        assert_eq!(first.status, AcyclicStatus::Ok);
        acyclic_next_result_release(first);

        let (message_received, allow_commit) = install_message_hook(reader);
        let pull = std::thread::spawn(move || {
            let result = acyclic_embedded_reader_next(reader);
            let status = result.status;
            acyclic_next_result_release(result);
            status
        });
        message_received
            .recv()
            .expect("pull received the queued end before commit");
        acyclic_embedded_reader_cancel(reader);
        allow_commit.wait();
        let status = pull.join().expect("reader pull did not panic");
        assert_eq!(status, AcyclicStatus::Cancelled);
        assert_eq!(
            acyclic_embedded_reader_next(reader).status,
            AcyclicStatus::Cancelled
        );
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
    }

    #[test]
    fn engine_close_can_race_a_reader_pull_without_invalidating_it() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/engine-close-race";
        let value = b"one";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let appended =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(appended.status, AcyclicStatus::Ok);
        acyclic_append_result_release(appended);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 1, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let pull = std::thread::spawn(move || {
            start_rx.recv().expect("test start signal");
            let result = acyclic_embedded_reader_next(reader);
            let status = result.status;
            acyclic_next_result_release(result);
            status
        });
        start_tx.send(()).expect("reader thread is alive");
        acyclic_embedded_engine_close(engine);
        let next = pull.join().expect("reader pull did not panic");
        assert_eq!(next, AcyclicStatus::Ok);
        acyclic_embedded_reader_close(reader);
    }

    #[test]
    fn finite_reader_terminal_status_is_stable() {
        let engine = acyclic_embedded_engine_open();
        let path = b"actors/stable-terminal";
        let value = b"one";
        let (path_ptr, path_len) = bytes(path);
        let (value_ptr, value_len) = bytes(value);
        let appended =
            acyclic_embedded_engine_append(engine, path_ptr, path_len, value_ptr, value_len);
        assert_eq!(appended.status, AcyclicStatus::Ok);
        acyclic_append_result_release(appended);
        let opened = acyclic_embedded_reader_open(engine, path_ptr, path_len, 0, 1, READ_MODE);
        assert_eq!(opened.status, AcyclicStatus::Ok);
        let reader = opened.reader;
        let value_result = acyclic_embedded_reader_next(reader);
        assert_eq!(value_result.status, AcyclicStatus::Ok);
        acyclic_next_result_release(value_result);
        let first_end = acyclic_embedded_reader_next(reader);
        let second_end = acyclic_embedded_reader_next(reader);
        assert_eq!(first_end.status, AcyclicStatus::End);
        assert_eq!(second_end.status, AcyclicStatus::End);
        acyclic_next_result_release(first_end);
        acyclic_next_result_release(second_end);
        acyclic_embedded_reader_close(reader);
        acyclic_embedded_engine_close(engine);
    }
}
