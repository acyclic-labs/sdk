//! Private native file primitives shared by durable local providers.

use bytes::Bytes;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Cancellation {
    cancelled: AtomicBool,
    #[cfg(target_os = "linux")]
    linux_event: Mutex<Option<Arc<linux::CancellationEvent>>>,
    #[cfg(windows)]
    windows_handle: Mutex<Option<isize>>,
    #[cfg(target_vendor = "apple")]
    apple_channel: Mutex<Option<dispatch2::DispatchRetained<dispatch2::DispatchIO>>>,
}

impl Cancellation {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        #[cfg(windows)]
        self.cancel_windows();
        #[cfg(target_os = "linux")]
        self.cancel_linux();
        #[cfg(target_vendor = "apple")]
        self.cancel_apple();
    }

    #[cfg(windows)]
    fn register_windows(&self, handle: isize) {
        *self
            .windows_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        if self.is_cancelled() {
            self.cancel_windows();
        }
    }

    #[cfg(windows)]
    fn clear_windows(&self, handle: isize) {
        let mut active = self
            .windows_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *active == Some(handle) {
            *active = None;
        }
    }

    #[cfg(windows)]
    fn with_windows_submission<T>(
        &self,
        handle: isize,
        submit: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let active = self
            .windows_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_cancelled() || *active != Some(handle) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "native I/O cancelled",
            ));
        }
        submit()
    }

    #[cfg(windows)]
    fn cancel_windows(&self) {
        let active = self
            .windows_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(handle) = *active {
            windows::cancel(handle);
        }
    }

    #[cfg(target_os = "linux")]
    fn register_linux(&self, event: &Arc<linux::CancellationEvent>) {
        *self
            .linux_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(event));
        if self.is_cancelled() {
            event.signal();
        }
    }

    #[cfg(target_os = "linux")]
    fn clear_linux(&self, event: &Arc<linux::CancellationEvent>) {
        let mut active = self
            .linux_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, event))
        {
            *active = None;
        }
    }

    #[cfg(target_os = "linux")]
    fn cancel_linux(&self) {
        let event = self
            .linux_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(event) = event {
            event.signal();
        }
    }

    #[cfg(target_vendor = "apple")]
    fn register_apple(&self, channel: &dispatch2::DispatchIO) {
        use dispatch2::DispatchObject as _;

        *self
            .apple_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(channel.retain());
        if self.is_cancelled() {
            self.cancel_apple();
        }
    }

    #[cfg(target_vendor = "apple")]
    fn clear_apple(&self, channel: &dispatch2::DispatchIO) {
        let mut active = self
            .apple_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active
            .as_deref()
            .is_some_and(|candidate| std::ptr::eq(candidate, channel))
        {
            *active = None;
        }
    }

    #[cfg(target_vendor = "apple")]
    fn cancel_apple(&self) {
        use dispatch2::DispatchIOCloseFlags;

        let active = self
            .apple_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(channel) = active {
            channel.close(DispatchIOCloseFlags::DISPATCH_IO_STOP);
        }
    }
}

/// One file owned by the native I/O runtime.
///
/// Callers retain responsibility for capability-safe path resolution and pass
/// the resulting handle here exactly once.
pub struct NativeFile {
    file: File,
}

impl NativeFile {
    /// Transfers ownership of an already-authorized file handle to the runtime.
    #[must_use]
    pub const fn from_file(file: File) -> Self {
        Self { file }
    }

    /// Changes the logical file length.
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    /// Borrows the underlying handle for platform-specific control operations.
    #[must_use]
    pub const fn as_file(&self) -> &File {
        &self.file
    }

    /// Submits owned writes while retaining this handle for later operations.
    pub fn write_all_batch_async(&self, writes: Vec<OwnedWrite>) -> io::Result<WriteBatch> {
        Ok(write_all_batch_async(self.file.try_clone()?, writes))
    }

    /// Flushes file contents and metadata according to `durability`.
    pub fn sync(&self, durability: Durability) -> io::Result<()> {
        sync_file(&self.file, durability)
    }
}

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(windows)]
mod windows;

/// Required durability boundary for one file operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Flush file contents and metadata to stable storage.
    Full,
    /// Order earlier writes before later writes without forcing the device cache to drain.
    ///
    /// This capability is currently exposed only where the operating system provides that
    /// exact primitive. Other targets return [`io::ErrorKind::Unsupported`].
    Barrier,
}

/// Flushes file contents and metadata according to `durability`.
pub fn sync_file(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_data(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    sync_parent_impl(path, durability)
}

/// Destination behavior for one durable rename.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenameMode {
    /// Fail atomically when the destination already exists.
    NoReplace,
    /// Atomically replace a compatible destination entry.
    Replace,
}

/// Renames one filesystem entry and durably publishes the affected namespace.
pub fn durable_rename(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    durable_rename_impl(from, to, mode)
}

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    read_at_impl(file, offset, destination)
}

/// Writes every byte at an absolute offset without changing the file cursor.
pub fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    write_all_at_impl(file, offset, bytes)
}

/// One owned positional write whose buffer remains live through completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWrite {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Bytes written completely or reported as an error.
    pub bytes: Bytes,
}

/// One owned positional read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRead {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Maximum bytes returned for this request.
    pub length: usize,
}

/// Submits an owned read batch to the shared native completion worker.
pub fn read_batch_async(file: File, reads: Vec<OwnedRead>) -> ReadBatch {
    ReadBatch::submit(file, reads)
}

/// Submits an owned write batch to the shared native completion workers.
pub fn write_all_batch_async(file: File, writes: Vec<OwnedWrite>) -> WriteBatch {
    WriteBatch::submit(file, writes)
}

/// Runtime-independent future for one owned native read batch.
pub struct ReadBatch {
    state: Arc<Mutex<ReadBatchState>>,
    pending: Option<NativeJob>,
    waiter: Option<u64>,
    cancellation: Arc<Cancellation>,
}

/// Runtime-independent future for one owned native write batch.
pub struct WriteBatch {
    state: Arc<Mutex<WriteBatchState>>,
    pending: Option<NativeJob>,
    waiter: Option<u64>,
    cancellation: Arc<Cancellation>,
}

struct ReadBatchState {
    result: Option<io::Result<Vec<Bytes>>>,
    waker: Option<Waker>,
}

struct WriteBatchState {
    result: Option<io::Result<()>>,
    waker: Option<Waker>,
}

enum NativeJob {
    Read {
        file: File,
        reads: Vec<OwnedRead>,
        state: Arc<Mutex<ReadBatchState>>,
        cancellation: Arc<Cancellation>,
    },
    Write {
        file: File,
        writes: Vec<OwnedWrite>,
        state: Arc<Mutex<WriteBatchState>>,
        cancellation: Arc<Cancellation>,
    },
}

impl NativeJob {
    fn run(self) -> Option<Waker> {
        match self {
            Self::Read {
                file,
                reads,
                state,
                cancellation,
            } => {
                let result = if cancellation.is_cancelled() {
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "native read cancelled before execution",
                    ))
                } else {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        read_batch_impl(&file, &reads, &cancellation)
                    }))
                    .unwrap_or_else(|_| Err(io::Error::other("native read worker panicked")))
                };
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.result = Some(result);
                state.waker.take()
            }
            Self::Write {
                file,
                writes,
                state,
                cancellation,
            } => {
                let result = if cancellation.is_cancelled() {
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "native write cancelled before execution",
                    ))
                } else {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        write_all_batch_owned(&file, writes, &cancellation)
                    }))
                    .unwrap_or_else(|_| Err(io::Error::other("native write worker panicked")))
                };
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.result = Some(result);
                state.waker.take()
            }
        }
    }
}

struct NativeWorkers {
    sender: mpsc::SyncSender<NativeJob>,
    capacity_waiters: Arc<Mutex<Vec<(u64, Waker)>>>,
    next_waiter: AtomicU64,
}

impl ReadBatch {
    fn submit(file: File, reads: Vec<OwnedRead>) -> Self {
        let state = Arc::new(Mutex::new(ReadBatchState {
            result: None,
            waker: None,
        }));
        let cancellation = Arc::new(Cancellation::default());
        Self {
            pending: Some(NativeJob::Read {
                file,
                reads,
                state: Arc::clone(&state),
                cancellation: Arc::clone(&cancellation),
            }),
            state,
            waiter: None,
            cancellation,
        }
    }
}

impl WriteBatch {
    fn submit(file: File, writes: Vec<OwnedWrite>) -> Self {
        let state = Arc::new(Mutex::new(WriteBatchState {
            result: None,
            waker: None,
        }));
        let cancellation = Arc::new(Cancellation::default());
        Self {
            pending: Some(NativeJob::Write {
                file,
                writes,
                state: Arc::clone(&state),
                cancellation: Arc::clone(&cancellation),
            }),
            state,
            waiter: None,
            cancellation,
        }
    }
}

impl Future for ReadBatch {
    type Output = io::Result<Vec<Bytes>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match poll_submission(&mut this.pending, &mut this.waiter, context) {
            Poll::Ready(result) => result?,
            Poll::Pending => return Poll::Pending,
        }
        let mut state = this
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl Future for WriteBatch {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match poll_submission(&mut this.pending, &mut this.waiter, context) {
            Poll::Ready(result) => result?,
            Poll::Pending => return Poll::Pending,
        }
        let mut state = this
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

fn poll_submission(
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<u64>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    let Some(job) = pending.take() else {
        return Poll::Ready(Ok(()));
    };
    let workers = match native_workers() {
        Ok(workers) => workers,
        Err(error) => return Poll::Ready(Err(error)),
    };
    match workers.sender.try_send(job) {
        Ok(()) => Poll::Ready(Ok(())),
        Err(mpsc::TrySendError::Full(job)) => {
            let identity = *waiter
                .get_or_insert_with(|| workers.next_waiter.fetch_add(1, Ordering::Relaxed).max(1));
            let mut waiters = workers
                .capacity_waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((_, waker)) = waiters
                .iter_mut()
                .find(|(candidate, _)| *candidate == identity)
            {
                waker.clone_from(context.waker());
            } else {
                waiters.push((identity, context.waker().clone()));
            }
            match workers.sender.try_send(job) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(mpsc::TrySendError::Full(job)) => {
                    *pending = Some(job);
                    Poll::Pending
                }
                Err(mpsc::TrySendError::Disconnected(_)) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native I/O workers stopped",
                ))),
            }
        }
        Err(mpsc::TrySendError::Disconnected(_)) => Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "native I/O workers stopped",
        ))),
    }
}

fn native_workers() -> io::Result<&'static NativeWorkers> {
    static WORKERS: OnceLock<io::Result<NativeWorkers>> = OnceLock::new();
    WORKERS
        .get_or_init(|| {
            let worker_count = std::thread::available_parallelism()
                .map_or(1, std::num::NonZero::get)
                .min(4);
            let (sender, receiver) = mpsc::sync_channel::<NativeJob>(worker_count * 4);
            let receiver = Arc::new(Mutex::new(receiver));
            let capacity_waiters = Arc::new(Mutex::new(Vec::<(u64, Waker)>::new()));
            for worker_index in 0..worker_count {
                let receiver = Arc::clone(&receiver);
                let capacity_waiters = Arc::clone(&capacity_waiters);
                std::thread::Builder::new()
                    .name(format!("acyclic-native-io-{worker_index}"))
                    .spawn(move || {
                        loop {
                            let job = {
                                let receiver = receiver
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                receiver.recv()
                            };
                            let Ok(job) = job else {
                                break;
                            };
                            let waiters = {
                                let mut waiters = capacity_waiters
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                std::mem::take(&mut *waiters)
                            };
                            for (_, waker) in waiters {
                                let _ =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        waker.wake();
                                    }));
                            }
                            let waker = job.run();
                            if let Some(waker) = waker {
                                let _ =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        waker.wake();
                                    }));
                            }
                        }
                    })?;
            }
            Ok(NativeWorkers {
                sender,
                capacity_waiters,
                next_waiter: AtomicU64::new(1),
            })
        })
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

impl Drop for ReadBatch {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker = None;
        let Some(waiter) = self.waiter else {
            return;
        };
        let Ok(workers) = native_workers() else {
            return;
        };
        let mut waiters = workers
            .capacity_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        waiters.retain(|(identity, _)| *identity != waiter);
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker = None;
        let Some(waiter) = self.waiter else {
            return;
        };
        let Ok(workers) = native_workers() else {
            return;
        };
        let mut waiters = workers
            .capacity_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        waiters.retain(|(identity, _)| *identity != waiter);
    }
}

/// Bounded positional source for one file range.
pub struct RangeReader<'a> {
    file: &'a File,
    offset: u64,
    remaining: u64,
}

/// Bounded asynchronous positional source backed by the native completion path.
pub struct AsyncRangeReader {
    file: File,
    offset: u64,
    remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes starting at `offset`.
    pub const fn new(file: File, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
    }

    /// Reads the next bounded chunk without blocking the caller's executor.
    pub async fn read(&mut self, maximum: usize) -> io::Result<Bytes> {
        let length = usize::try_from(self.remaining.min(maximum as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range too large"))?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut result = read_batch_async(
            self.file.try_clone()?,
            vec![OwnedRead {
                offset: self.offset,
                length,
            }],
        )
        .await?;
        let bytes = result
            .pop()
            .ok_or_else(|| io::Error::other("native read returned no result"))?;
        self.offset = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow"))?;
        self.remaining -= bytes.len() as u64;
        Ok(bytes)
    }
}

impl<'a> RangeReader<'a> {
    /// Creates a source for at most `length` bytes starting at `offset`.
    pub const fn new(file: &'a File, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
    }
}

impl Read for RangeReader<'_> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        let maximum = usize::try_from(self.remaining.min(destination.len() as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range too large"))?;
        if maximum == 0 {
            return Ok(0);
        }
        let selected = destination.get_mut(..maximum).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "range exceeds destination")
        })?;
        let count = read_at(self.file, self.offset, selected)?;
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow"))?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

#[cfg(target_os = "linux")]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    linux::read_at(file, offset, destination)
}

#[cfg(target_os = "linux")]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    linux::write_all_at(file, offset, bytes)
}

#[cfg(target_os = "linux")]
fn write_all_batch_owned(
    file: &File,
    writes: Vec<OwnedWrite>,
    cancellation: &Cancellation,
) -> io::Result<()> {
    linux::write_all_batch_owned(file, writes, cancellation)
}

#[cfg(target_os = "linux")]
fn read_batch_impl(
    file: &File,
    reads: &[OwnedRead],
    cancellation: &Cancellation,
) -> io::Result<Vec<Bytes>> {
    linux::read_batch(file, reads, cancellation)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt as _;
    file.read_at(destination, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.write_all_at(bytes, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn write_all_batch_owned(
    file: &File,
    writes: Vec<OwnedWrite>,
    cancellation: &Cancellation,
) -> io::Result<()> {
    apple::write_all_batch_owned(file, writes, cancellation)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn read_batch_impl(
    file: &File,
    reads: &[OwnedRead],
    cancellation: &Cancellation,
) -> io::Result<Vec<Bytes>> {
    apple::read_batch(file, reads, cancellation)
}

#[cfg(windows)]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::windows::fs::FileExt as _;
    file.seek_read(destination, offset)
}

#[cfg(windows)]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    let mut remaining = bytes;
    let mut position = offset;
    while !remaining.is_empty() {
        let count = file.seek_write(remaining, position)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "positional write returned zero",
            ));
        }
        position = position
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        remaining = remaining.get(count..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "write exceeded submitted length",
            )
        })?;
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_batch_owned(
    file: &File,
    writes: Vec<OwnedWrite>,
    cancellation: &Cancellation,
) -> io::Result<()> {
    windows::write_all_batch_owned(file, writes, cancellation)
}

#[cfg(windows)]
fn read_batch_impl(
    file: &File,
    reads: &[OwnedRead],
    cancellation: &Cancellation,
) -> io::Result<Vec<Bytes>> {
    windows::read_batch(file, reads, cancellation)
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
fn barrier_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: `F_BARRIERFSYNC` acts only on the live descriptor.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_vendor = "apple"))]
fn barrier_sync(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "ordered write barrier is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    sync_file(&File::open(path)?, durability)
}

#[cfg(windows)]
fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ_WRITE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    sync_file(&directory, durability)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "renameat2 receives two live C paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let from_path = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let to_path = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let flags = match mode {
        RenameMode::NoReplace => libc::RENAME_NOREPLACE,
        RenameMode::Replace => 0,
    };
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from_path.as_ptr(),
            libc::AT_FDCWD,
            to_path.as_ptr(),
            flags,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    sync_rename_parents(from, to)
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "renamex_np receives two live C paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let from_path = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let to_path = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let flags = match mode {
        RenameMode::NoReplace => libc::RENAME_EXCL,
        RenameMode::Replace => 0,
    };
    if unsafe { libc::renamex_np(from_path.as_ptr(), to_path.as_ptr(), flags) } != 0 {
        return Err(io::Error::last_os_error());
    }
    sync_rename_parents(from, to)
}

#[cfg(unix)]
fn sync_rename_parents(from: &Path, to: &Path) -> io::Result<()> {
    fn namespace_parent(path: &Path) -> &Path {
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }
    let to_parent = namespace_parent(to);
    sync_parent(to_parent, Durability::Full)?;
    let from_parent = namespace_parent(from);
    if from_parent != to_parent {
        sync_parent(from_parent, Durability::Full)?;
    }
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code, reason = "MoveFileExW receives terminated UTF-16 paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let mut from: Vec<u16> = from.as_os_str().encode_wide().collect();
    let mut to: Vec<u16> = to.as_os_str().encode_wide().collect();
    if from.contains(&0) || to.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename path contains NUL",
        ));
    }
    from.push(0);
    to.push(0);
    let flags = MOVEFILE_WRITE_THROUGH
        | if mode == RenameMode::Replace {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    // SAFETY: both arguments are live, NUL-terminated UTF-16 path buffers.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::task::{Poll, Wake};
    use std::thread::Thread;
    use std::time::{Duration, Instant};

    struct ThreadWake(Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    fn completion_waker() -> Waker {
        Waker::from(Arc::new(ThreadWake(std::thread::current())))
    }

    #[test]
    fn durable_rename_moves_and_replaces_entries() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"first")?;
        durable_rename(&source, &destination, RenameMode::NoReplace)?;
        assert!(!source.exists());
        assert_eq!(std::fs::read(&destination)?, b"first");

        std::fs::write(&source, b"second")?;
        durable_rename(&source, &destination, RenameMode::Replace)?;
        assert!(!source.exists());
        assert_eq!(std::fs::read(destination)?, b"second");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn durable_rename_accepts_relative_sibling_paths() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let original = std::env::current_dir()?;
        std::env::set_current_dir(temporary.path())?;
        let result = (|| {
            std::fs::write("from", b"relative")?;
            durable_rename(Path::new("from"), Path::new("to"), RenameMode::NoReplace)?;
            assert_eq!(std::fs::read("to")?, b"relative");
            Ok(())
        })();
        std::env::set_current_dir(original)?;
        result
    }

    #[test]
    fn no_replace_preserves_both_entries() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"source")?;
        std::fs::write(&destination, b"destination")?;
        let Err(error) = durable_rename(&source, &destination, RenameMode::NoReplace) else {
            return Err(io::Error::other(
                "existing destination accepted no-replace rename",
            ));
        };
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(source)?, b"source");
        assert_eq!(std::fs::read(destination)?, b"destination");
        Ok(())
    }

    fn complete_read(mut read: ReadBatch) -> io::Result<Vec<Bytes>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let waker = completion_waker();
        loop {
            match Pin::new(&mut read).poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => return result,
                Poll::Pending if Instant::now() < deadline => {
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                Poll::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "owned native read did not complete",
                    ));
                }
            }
        }
    }

    fn complete_write(mut write: WriteBatch) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let waker = completion_waker();
        loop {
            match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => return result,
                Poll::Pending if Instant::now() < deadline => {
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                Poll::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "owned native write did not complete",
                    ));
                }
            }
        }
    }

    #[test]
    fn positional_io_preserves_cursor_and_bounds_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("positional");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 2, &[0x22; 3])?;
        write_all_at(&file, 16, &[0x33; 5])?;
        sync_file(&file, Durability::Full)?;

        let mut bytes = [0_u8; 24];
        assert_eq!(read_at(&file, 0, &mut bytes)?, 21);
        assert_eq!(&bytes[..8], &[0, 0, 0x22, 0x22, 0x22, 0, 0, 0]);
        assert_eq!(&bytes[8..16], &[0; 8]);
        assert_eq!(&bytes[16..21], &[0x33; 5]);
        assert_eq!(read_at(&file, 21, &mut bytes)?, 0);

        let mut range = RangeReader::new(&file, 15, 8);
        let mut selected = Vec::new();
        range.read_to_end(&mut selected)?;
        assert_eq!(selected, [0, 0x33, 0x33, 0x33, 0x33, 0x33]);

        complete_write(write_all_batch_async(
            file.try_clone()?,
            vec![
                OwnedWrite {
                    offset: 32,
                    bytes: Bytes::from_static(&[0x44; 5]),
                },
                OwnedWrite {
                    offset: 48,
                    bytes: Bytes::from_static(&[0x55; 7]),
                },
            ],
        ))?;
        let mut batch = [0_u8; 23];
        assert_eq!(read_at(&file, 32, &mut batch)?, 23);
        assert_eq!(&batch[..5], &[0x44; 5]);
        assert_eq!(&batch[5..16], &[0; 11]);
        assert_eq!(&batch[16..], &[0x55; 7]);
        Ok(())
    }

    #[test]
    fn batch_larger_than_native_queue_preserves_every_write() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("large-batch"))?;
        let writes: Vec<_> = (0_u8..17)
            .map(|value| OwnedWrite {
                offset: u64::from(value) * 4,
                bytes: Bytes::from(vec![value; 3]),
            })
            .collect();
        complete_write(write_all_batch_async(file.try_clone()?, writes))?;

        let mut actual = [0_u8; 67];
        assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
        for value in 0_u8..17 {
            let start = usize::from(value) * 4;
            assert_eq!(actual.get(start..start + 3), Some([value; 3].as_slice()));
            if value != 16 {
                assert_eq!(actual.get(start + 3), Some(&0));
            }
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn completed_batches_retire_cancellation_polls() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("retired-cancellation"))?;
        for value in 0_u16..256 {
            complete_write(write_all_batch_async(
                file.try_clone()?,
                vec![OwnedWrite {
                    offset: u64::from(value),
                    bytes: Bytes::copy_from_slice(&[value.to_le_bytes()[0]]),
                }],
            ))?;
        }
        let contents = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 256,
            }],
        ))?;
        assert_eq!(contents.first().map(Bytes::len), Some(256));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_write_batch_preserves_empty_writes_and_port_reuse() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("write-batch-reuse"))?;
        for value in 0_u8..8 {
            complete_write(write_all_batch_async(
                file.try_clone()?,
                vec![
                    OwnedWrite {
                        offset: u64::from(value) * 2,
                        bytes: Bytes::copy_from_slice(&[value]),
                    },
                    OwnedWrite {
                        offset: u64::from(value) * 2 + 1,
                        bytes: Bytes::new(),
                    },
                ],
            ))?;
        }
        let mut actual = [0_u8; 15];
        assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
        for value in 0_u8..8 {
            assert_eq!(actual.get(usize::from(value) * 2), Some(&value));
        }
        Ok(())
    }

    #[test]
    fn owned_write_outlives_the_callers_file_and_buffers() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-write");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let write = write_all_batch_async(
            file,
            vec![OwnedWrite {
                offset: 3,
                bytes: Bytes::from_static(b"owned"),
            }],
        );
        complete_write(write)?;
        let mut actual = Vec::new();
        File::open(path)?.read_to_end(&mut actual)?;
        assert_eq!(actual, [0, 0, 0, b'o', b'w', b'n', b'e', b'd']);
        Ok(())
    }

    #[test]
    fn owned_native_file_preserves_positional_contract() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-native-file");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        let native = NativeFile::from_file(file);
        native.set_len(32)?;
        write_all_at(native.as_file(), 7, b"native")?;
        complete_write(native.write_all_batch_async(vec![
            OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"one"),
            },
            OwnedWrite {
                offset: 20,
                bytes: Bytes::from_static(b"two"),
            },
        ])?)?;
        native.sync(Durability::Full)?;

        let mut actual = [0_u8; 32];
        assert_eq!(read_at(native.as_file(), 0, &mut actual)?, actual.len());
        assert_eq!(actual.get(..3), Some(b"one".as_slice()));
        assert_eq!(actual.get(7..13), Some(b"native".as_slice()));
        assert_eq!(actual.get(20..23), Some(b"two".as_slice()));
        Ok(())
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn barrier_durability_rejects_targets_without_an_exact_primitive() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = File::create(temporary.path().join("barrier"))?;
        assert!(matches!(
            sync_file(&file, Durability::Barrier),
            Err(error) if error.kind() == io::ErrorKind::Unsupported
        ));
        assert!(matches!(
            sync_data(&file, Durability::Barrier),
            Err(error) if error.kind() == io::ErrorKind::Unsupported
        ));
        Ok(())
    }

    #[test]
    fn native_read_batch_preserves_order_and_short_reads() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("read-batch"))?;
        write_all_at(&file, 0, b"abcdefgh")?;
        let reads = complete_read(read_batch_async(
            file,
            vec![
                OwnedRead {
                    offset: 5,
                    length: 8,
                },
                OwnedRead {
                    offset: 1,
                    length: 3,
                },
            ],
        ))?;
        assert_eq!(
            reads,
            [Bytes::from_static(b"fgh"), Bytes::from_static(b"bcd")]
        );
        Ok(())
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn native_batches_reject_all_invalid_offsets_before_submission() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("invalid-offset-batch");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        write_all_at(&file, 0, b"stable")?;

        let read = complete_read(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 1,
                },
                OwnedRead {
                    offset: u64::MAX,
                    length: 1,
                },
            ],
        ));
        assert!(matches!(read, Err(error) if error.kind() == io::ErrorKind::InvalidInput));

        let write = complete_write(write_all_batch_async(
            file,
            vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"changed"),
                },
                OwnedWrite {
                    offset: u64::MAX,
                    bytes: Bytes::from_static(b"x"),
                },
            ],
        ));
        assert!(matches!(write, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
        assert_eq!(std::fs::read(path)?, b"stable");
        Ok(())
    }

    #[test]
    fn owned_read_outlives_the_callers_file_and_request() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-read");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 0, b"abcdefgh")?;
        let read = read_batch_async(
            file,
            vec![OwnedRead {
                offset: 2,
                length: 4,
            }],
        );
        let actual = complete_read(read)?;
        assert_eq!(actual, [Bytes::from_static(b"cdef")]);
        Ok(())
    }

    #[test]
    fn dropped_admission_waiters_do_not_block_later_reads() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("admission-cancellation");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 0, b"x")?;
        let mut reads = Vec::new();
        for _ in 0..64 {
            let mut read = read_batch_async(
                file.try_clone()?,
                vec![OwnedRead {
                    offset: 0,
                    length: 1,
                }],
            );
            let _ = Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop()));
            reads.push(read);
        }
        reads.truncate(8);
        for read in reads {
            assert_eq!(complete_read(read)?, [Bytes::from_static(b"x")]);
        }
        let final_read = read_batch_async(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 1,
            }],
        );
        assert_eq!(complete_read(final_read)?, [Bytes::from_static(b"x")]);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_read_batch_preserves_empty_and_eof_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("read-batch-eof"))?;
        write_all_at(&file, 0, b"abc")?;
        let reads = complete_read(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 0,
                },
                OwnedRead {
                    offset: 3,
                    length: 4,
                },
                OwnedRead {
                    offset: 30,
                    length: 4,
                },
            ],
        ))?;
        assert_eq!(reads, [Bytes::new(), Bytes::new(), Bytes::new()]);
        for (offset, expected) in b"abc".iter().copied().enumerate() {
            let repeated = complete_read(read_batch_async(
                file.try_clone()?,
                vec![OwnedRead {
                    offset: offset as u64,
                    length: 1,
                }],
            ))?;
            assert_eq!(repeated, [Bytes::copy_from_slice(&[expected])]);
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_chunks_large_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("large-read-batch"))?;
        let expected = Bytes::from(vec![0x5a; 1024 * 1024 + 1]);
        write_all_at(&file, 7, &expected)?;
        let actual = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 7,
                length: expected.len(),
            }],
        ))?;
        assert_eq!(actual, [expected]);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_windows_requests_beyond_the_ring() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("windowed-read-batch"))?;
        let expected: Vec<u8> = (0..17).collect();
        write_all_at(&file, 0, &expected)?;
        let requests = (0_u64..17)
            .map(|offset| OwnedRead { offset, length: 1 })
            .collect();
        let actual = complete_read(read_batch_async(file, requests))?;
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|value| Bytes::copy_from_slice(&[value]))
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_windows_one_range_beyond_the_ring() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("windowed-large-read"))?;
        let expected = Bytes::from(vec![0x69; 16 * 1024 * 1024 + 1]);
        write_all_at(&file, 3, &expected)?;
        let actual = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 3,
                length: expected.len(),
            }],
        ))?;
        assert_eq!(actual, [expected]);
        Ok(())
    }
}
