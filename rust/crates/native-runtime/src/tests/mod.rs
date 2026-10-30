mod health;
use super::*;
use std::fs::OpenOptions;
use std::io::{Seek as _, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, Wake};
use std::thread::Thread;
use std::time::Instant;

mod admission;
mod claim;
mod completion;
mod custody;
mod namespace;
mod native_allocation;
mod positional;
mod prepared_order;
mod range;
mod source;
mod spare;
use source::SourceFixture;
mod write_validation;

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

/// Polls a pending host task until a worker queue accepted its job, parking
/// until its admission waker fires between polls.
fn submit<T>(task: &mut NativeCompletion<T>) {
    let waker = completion_waker();
    while task.pending.is_some() {
        assert!(
            Pin::new(&mut *task)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if task.pending.is_some() {
            std::thread::park();
        }
    }
}

/// Drives `future` on this thread, parking until its producer waker unparks it.
pub(crate) fn complete<T>(future: impl Future<Output = io::Result<T>>) -> io::Result<T> {
    let waker = completion_waker();
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => return result,
            // A wake before park leaves the token set, so none is lost.
            Poll::Pending => std::thread::park(),
        }
    }
}

const EXCLUSIVE_CHILD: &str = "ACYCLIC_NATIVE_RUNTIME_EXCLUSIVE_TEST";

/// Whether this process runs a single test started by
/// [`passed_in_exclusive_process`], so that test owns the process-global pools.
pub(crate) fn in_exclusive_process() -> bool {
    std::env::var_os(EXCLUSIVE_CHILD).is_some()
}

/// Runs `test` alone in a fresh test process and returns true here once it
/// passed there; returns false inside that process, which runs the body.
///
/// The worker pools are process-global, so a test that asserts exact charges
/// or forces admission order must not share them with concurrent tests.
pub(crate) fn passed_in_exclusive_process(test: &str) -> io::Result<bool> {
    if std::env::var_os(EXCLUSIVE_CHILD).is_some_and(|name| name == test) {
        return Ok(false);
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--nocapture"])
            .env(EXCLUSIVE_CHILD, test),
        CaptureSchedule::Unbounded,
        8 * 1024 * 1024,
    )?;
    assert!(
        output.status.success(),
        "{test} failed in its exclusive process:
{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(true)
}

/// Holds every worker of a pool and fills its queue, so the next submission
/// waits for admission until this drops.
pub(crate) struct Saturation {
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    _held: Vec<NativeCompletion<()>>,
}

impl Saturation {
    pub(crate) fn new(pool: &'static LazyLock<io::Result<NativeWorkers>>) -> io::Result<Self> {
        let threads = workers(pool, false)?.exclusive_threads();
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut held = Vec::new();
        loop {
            let started = started_tx.clone();
            let closed = Arc::clone(&gate);
            let mut job = task_completion(
                move || {
                    let _ = started.send(());
                    let (open, changed) = &*closed;
                    let mut open = open.lock().unwrap();
                    while !*open {
                        open = changed.wait(open).unwrap();
                    }
                    Ok(())
                },
                None,
                None,
                None,
                pool,
            )?;
            assert!(
                Pin::new(&mut job)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if job.waiter.is_some() {
                // The queue is full; dropping the waiting job unregisters it.
                return Ok(Self { gate, _held: held });
            }
            held.push(job);
            if held.len() == threads {
                // Every worker holds one gated job, so queued jobs stay queued.
                for _ in 0..threads {
                    started_rx.recv().map_err(io::Error::other)?;
                }
            }
        }
    }
}

impl Drop for Saturation {
    fn drop(&mut self) {
        let (open, changed) = &*self.gate;
        *open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }
}

#[derive(Default)]
pub(super) struct ReadBudgetMeter {
    #[cfg(windows)]
    shared_owner: std::sync::Weak<Self>,
    #[cfg(windows)]
    pub(super) bytes_recorded: Mutex<Option<mpsc::Sender<(u64, u64)>>>,
    pub(super) used_operations: AtomicU64,
    pub(super) read_attempts: AtomicU64,
    pub(super) write_attempts: AtomicU64,
    pub(super) used_bytes: AtomicU64,
    pub(super) used_allocations: AtomicU64,
    examined_items: AtomicU64,
    encoded: AtomicU64,
    copied: AtomicU64,
    hashed: AtomicU64,
    pub(super) item_limit: Option<u64>,
    read: AtomicU64,
    pub(super) memory: AtomicU64,
    /// Terminal handoff for tests that need the last physical owner's release.
    pub(super) memory_retired: Mutex<Option<mpsc::Sender<()>>>,
    operation_limit: u64,
    byte_limit: u64,
    allocation_limit: u64,
    pub(super) deny_allocations: AtomicBool,
    pub(super) operation_denial: Option<NativeOperationKind>,
    pub(super) cancel_after_operation: bool,
    pub(super) fail_recorded_operation: Option<u64>,
    memory_limit: u64,
    pub(super) cancelled: AtomicBool,
    cancel_after_read: bool,
    panic_after_read: bool,
    /// Runs when a write moved nothing because its pipe was full.
    pub(super) blocked_write: Option<Box<dyn Fn() + Send + Sync>>,
    // Last: the shared ownership primitive observes the final meter cleanup.
    _retirement: Option<OwnershipAnchor>,
}

impl ReadBudgetMeter {
    /// After the original operation ends, await its final allocation release.
    /// The release callback and this registration share one mutex, including
    /// the already-retired case, so neither ordering loses the terminal event.
    fn wait_for_memory_retirement(&self) -> io::Result<()> {
        let receiver = {
            let mut retired = self
                .memory_retired
                .lock()
                .map_err(|_| io::Error::other("allocation retirement registration poisoned"))?;
            if self.memory.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            if retired.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "allocation retirement already observed",
                ));
            }
            let (sender, receiver) = mpsc::channel();
            *retired = Some(sender);
            receiver
        };
        receiver.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "allocation release producer lost",
            )
        })?;
        assert_eq!(self.memory.load(Ordering::Acquire), 0);
        Ok(())
    }
    #[cfg(windows)]
    pub(super) fn shared(operation_limit: u64, byte_limit: u64) -> Arc<Self> {
        Arc::new_cyclic(|owner| Self {
            shared_owner: owner.clone(),
            ..Self::new(operation_limit, byte_limit)
        })
    }
    pub(super) fn new(operation_limit: u64, byte_limit: u64) -> Self {
        Self {
            operation_limit,
            byte_limit,
            allocation_limit: u64::MAX,
            memory_limit: 1024 * 1024,
            ..Self::default()
        }
    }
    fn charge(counter: &AtomicU64, amount: u64, limit: u64) -> io::Result<u64> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|total| *total <= limit)
            })
            .map_err(|_| Self::refuse())
    }
    fn preflight(&self, counter: &AtomicU64, amount: u64, limit: u64) -> io::Result<()> {
        self.check()?;
        counter
            .load(Ordering::Acquire)
            .checked_add(amount)
            .filter(|total| *total <= limit)
            .map(|_| ())
            .ok_or_else(Self::refuse)
    }
    fn refuse() -> io::Error {
        io::ErrorKind::Other.into()
    }
}

impl NativeReadMeter for ReadBudgetMeter {
    #[cfg(windows)]
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        self.shared_owner
            .upgrade()
            .map(|owner| owner as Arc<dyn NativeReadMeter>)
    }
    fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
        self.check()?;
        if self.operation_denial == Some(kind) {
            return Err(Self::refuse());
        }
        Self::charge(&self.used_operations, count, self.operation_limit).map(|_| ())
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, count: u64) {
        assert!(self.used_operations.fetch_sub(count, Ordering::AcqRel) >= count);
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.read_attempts.fetch_add(1, Ordering::AcqRel);
        }
        if kind == NativeOperationKind::Write {
            self.write_attempts.fetch_add(1, Ordering::AcqRel);
        }
        if self.cancel_after_operation {
            self.cancelled.store(true, Ordering::Release);
        }
        if self.fail_recorded_operation == Some(self.used_operations.load(Ordering::Acquire)) {
            return Err(Self::refuse());
        }
        Ok(())
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
        assert_ne!(kind, NativeOperationKind::Durability);
        self.check()?;
        Self::charge(&self.used_bytes, maximum, self.byte_limit).map(|_| ())
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, maximum: u64) {
        assert!(self.used_bytes.fetch_sub(maximum, Ordering::AcqRel) >= maximum);
    }
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()> {
        assert!(actual <= maximum);
        if kind == NativeOperationKind::Write
            && actual == 0
            && let Some(blocked) = &self.blocked_write
        {
            blocked();
        }
        assert!(
            self.used_bytes
                .fetch_sub(maximum - actual, Ordering::AcqRel)
                >= maximum
        );
        self.read.fetch_add(actual, Ordering::AcqRel);
        #[cfg(windows)]
        if kind == NativeOperationKind::Write {
            if let Some(recorded) = self.bytes_recorded.lock().unwrap().take() {
                let _ = recorded.send((maximum, actual));
            }
        }
        if self.cancel_after_read && actual != 0 {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        self.reserve_allocation(bytes)?;
        self.record_reserved_allocation(bytes)
    }
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
        self.check()?;
        if self.deny_allocations.load(Ordering::Acquire) {
            return Err(Self::refuse());
        }
        Self::charge(&self.used_allocations, 1, self.allocation_limit)?;
        if let Err(error) = self.adopt(bytes) {
            self.used_allocations.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        assert!(self.used_allocations.fetch_sub(1, Ordering::AcqRel) != 0);
        self.release(bytes);
    }
    fn record_reserved_allocation(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        io::Error::new(io::ErrorKind::OutOfMemory, error)
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        self.check()?;
        for (counter, bytes) in [
            (&self.encoded, encoded),
            (&self.copied, copied),
            (&self.hashed, hashed),
        ] {
            self.preflight(counter, bytes, u64::MAX)?;
        }
        Ok(())
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        for (counter, bytes) in [
            (&self.encoded, encoded),
            (&self.copied, copied),
            (&self.hashed, hashed),
        ] {
            Self::charge(counter, bytes, u64::MAX)?;
        }
        Ok(())
    }
    fn allocate_refcount_header(&self) -> io::Result<()> {
        self.allocate(0)
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.check()?;
        Self::charge(&self.memory, bytes, self.memory_limit).map(|_| ())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        match Self::charge(&self.memory, bytes, u64::MAX) {
            Err(error) => (0, Err(error)),
            Ok(previous) => (
                bytes,
                if previous + bytes > self.memory_limit {
                    Err(Self::refuse())
                } else {
                    self.check()
                },
            ),
        }
    }
    fn release(&self, bytes: u64) {
        let previous = self.memory.fetch_sub(bytes, Ordering::AcqRel);
        assert!(previous >= bytes);
        if previous == bytes
            && let Some(retired) = self
                .memory_retired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        {
            let _ = retired.send(());
        }
    }
    fn examine(&self, items: u64, _: u64) -> io::Result<()> {
        self.check()?;
        self.record_examination(items, 0)
    }
    fn admit_examination(&self, items: u64, _: u64) -> io::Result<()> {
        self.check()?;
        match self.item_limit {
            Some(limit) => self.preflight(&self.examined_items, items, limit),
            None => Ok(()),
        }
    }
    fn record_examination(&self, items: u64, _: u64) -> io::Result<()> {
        match self.item_limit {
            Some(limit) => Self::charge(&self.examined_items, items, limit).map(|_| ()),
            None => Ok(()),
        }
    }
    fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
        self.preflight(&self.used_bytes, maximum as u64, self.byte_limit)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.admit_bytes(maximum)?;
        self.reserve_operations(NativeOperationKind::Read, 1)
    }
    fn record_read(&self, bytes: usize) -> io::Result<()> {
        self.used_bytes.fetch_add(bytes as u64, Ordering::AcqRel);
        self.read.fetch_add(bytes as u64, Ordering::AcqRel);
        if self.cancel_after_read && bytes != 0 {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn finish_read(&self, _: usize, completed: usize) -> io::Result<()> {
        if self.panic_after_read && completed != 0 {
            std::panic::panic_any(0x7072_6570_6172_6564_u64);
        }
        self.check()
    }
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> {
        self.admit_bytes(maximum)
    }
    fn admit_write(&self, maximum: usize) -> io::Result<()> {
        self.admit_read(maximum)
    }
    fn record_write(&self, bytes: usize) -> io::Result<()> {
        self.record_read(bytes)
    }
    fn admit_durability(&self) -> io::Result<()> {
        self.admit_write(0)
    }
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        self.reserve_operations(NativeOperationKind::Write, 1)?;
        Ok(1)
    }
    fn cancel_cleanup_write(&self, token: u64) {
        self.cancel_reserved_operations(NativeOperationKind::Write, token);
    }
    fn record_cleanup_write(&self, _: u64) -> io::Result<()> {
        self.record_reserved_operation(NativeOperationKind::Write)
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[allow(
    unsafe_code,
    reason = "the fixture opens the required overlapped handle"
)]
fn bounded_write_file(path: &Path, writable: bool) -> io::Result<NativeFile> {
    initialize_native_io_services()?;
    let mut options = OpenOptions::new();
    options.read(true).write(writable);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
        // SAFETY: the exact open above supplies the required flag.
        unsafe {
            NativeFile::from_overlapped_file_unchecked(options.open(path)?, None)
                .map_err(|(error, _)| error)
        }
    }
    #[cfg(not(windows))]
    NativeFile::from_file(options.open(path)?, None).map_err(|(error, _)| error)
}

struct ObservedWriteBytes {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for ObservedWriteBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for ObservedWriteBytes {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

struct ObservedWriteCustody {
    bytes_dropped: Arc<AtomicBool>,
    retired: Arc<AtomicBool>,
    meter: Arc<ReadBudgetMeter>,
    actual_prefix: u64,
}

impl Drop for ObservedWriteCustody {
    fn drop(&mut self) {
        assert!(self.bytes_dropped.load(Ordering::Acquire));
        assert_eq!(self.meter.read.load(Ordering::Acquire), self.actual_prefix);
        self.retired.store(true, Ordering::Release);
    }
}

fn prepared_metadata_task(
    file: File,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<NativeCompletion<io::Result<u64>>> {
    let mut operation =
        NativeOperationReservation::new(Arc::clone(&meter), NativeOperationKind::Read, 1)?;
    run_blocking_io(
        move || {
            let (result, recorded) =
                operation.attempt(|| file.metadata().map(|metadata| metadata.len()));
            result.and_then(|length| recorded.map(|()| length))
        },
        Some((meter, true)),
    )
}
