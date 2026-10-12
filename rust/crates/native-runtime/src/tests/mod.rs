mod health;
use super::*;
use std::fs::OpenOptions;
use std::io::{Seek as _, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, Wake};
use std::thread::Thread;
use std::time::{Duration, Instant};

mod admission;
mod completion;
mod custody;
mod namespace;
mod positional;

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

fn complete<T>(future: impl Future<Output = io::Result<T>>) -> io::Result<T> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let waker = completion_waker();
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => return result,
            Poll::Pending if Instant::now() < deadline => {
                std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            Poll::Pending => return Err(io::ErrorKind::TimedOut.into()),
        }
    }
}

pub(super) struct ReadBudgetMeter {
    used_operations: AtomicU64,
    used_bytes: AtomicU64,
    used_allocations: AtomicU64,
    read: AtomicU64,
    pub(super) memory: AtomicU64,
    operation_limit: u64,
    byte_limit: u64,
    allocation_limit: u64,
    pub(super) deny_allocations: AtomicBool,
    pub(super) operation_denial: Option<NativeOperationKind>,
    pub(super) cancel_after_operation: bool,
    memory_limit: u64,
    pub(super) cancelled: AtomicBool,
    cancel_after_read: bool,
    panic_after_read: bool,
}

impl ReadBudgetMeter {
    pub(super) fn new(operation_limit: u64, byte_limit: u64) -> Self {
        Self {
            used_operations: AtomicU64::new(0),
            used_bytes: AtomicU64::new(0),
            used_allocations: AtomicU64::new(0),
            read: AtomicU64::new(0),
            memory: AtomicU64::new(0),
            operation_limit,
            byte_limit,
            allocation_limit: u64::MAX,
            memory_limit: 1024 * 1024,
            deny_allocations: AtomicBool::new(false),
            operation_denial: None,
            cancel_after_operation: false,
            cancelled: AtomicBool::new(false),
            cancel_after_read: false,
            panic_after_read: false,
        }
    }
    fn refuse() -> io::Error {
        io::ErrorKind::Other.into()
    }
}

impl NativeReadMeter for ReadBudgetMeter {
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
        self.used_operations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(count)
                    .filter(|total| *total <= self.operation_limit)
            })
            .map(|_| ())
            .map_err(|_| Self::refuse())
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, count: u64) {
        assert!(self.used_operations.fetch_sub(count, Ordering::AcqRel) >= count);
    }
    fn record_reserved_operation(&self, _: NativeOperationKind) -> io::Result<()> {
        if self.cancel_after_operation {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
        assert_ne!(kind, NativeOperationKind::Durability);
        self.check()?;
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(maximum)
                    .filter(|total| *total <= self.byte_limit)
            })
            .map(|_| ())
            .map_err(|_| Self::refuse())
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, maximum: u64) {
        assert!(self.used_bytes.fetch_sub(maximum, Ordering::AcqRel) >= maximum);
    }
    fn record_reserved_bytes(
        &self,
        _: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()> {
        assert!(actual <= maximum);
        assert!(
            self.used_bytes
                .fetch_sub(maximum - actual, Ordering::AcqRel)
                >= maximum
        );
        self.read.fetch_add(actual, Ordering::AcqRel);
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
        self.used_allocations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(1)
                    .filter(|total| *total <= self.allocation_limit)
            })
            .map_err(|_| Self::refuse())?;
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
    fn allocate_refcount_header(&self) -> io::Result<()> {
        self.allocate(0)
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.check()?;
        self.memory
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.memory_limit)
            })
            .map(|_| ())
            .map_err(|_| Self::refuse())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        let previous = match self
            .memory
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
            }) {
            Ok(previous) => previous,
            Err(_) => return (0, Err(Self::refuse())),
        };
        (
            bytes,
            if previous + bytes > self.memory_limit {
                Err(Self::refuse())
            } else {
                self.check()
            },
        )
    }
    fn release(&self, bytes: u64) {
        assert!(self.memory.fetch_sub(bytes, Ordering::AcqRel) >= bytes);
    }
    fn examine(&self, _: u64, _: u64) -> io::Result<()> {
        self.check()
    }
    fn admit_examination(&self, _: u64, _: u64) -> io::Result<()> {
        self.check()
    }
    fn record_examination(&self, _: u64, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
        self.check()?;
        self.used_bytes
            .load(Ordering::Acquire)
            .checked_add(maximum as u64)
            .filter(|total| *total <= self.byte_limit)
            .map(|_| ())
            .ok_or_else(Self::refuse)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.admit_bytes(maximum)?;
        self.reserve_operations(NativeOperationKind::Read, 1)
    }
    fn admit_operations(&self, count: u64) -> io::Result<()> {
        self.check()?;
        self.used_operations
            .load(Ordering::Acquire)
            .checked_add(count)
            .filter(|total| *total <= self.operation_limit)
            .map(|_| ())
            .ok_or_else(Self::refuse)
    }
    fn record_operations(&self, count: u64) -> io::Result<()> {
        self.used_operations.fetch_add(count, Ordering::AcqRel);
        Ok(())
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
    fn admit_write_operations(&self, count: u64) -> io::Result<()> {
        self.admit_operations(count)
    }
    fn record_write_operations(&self, count: u64) -> io::Result<()> {
        self.record_operations(count)
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
#[allow(unsafe_code)]
fn bounded_write_file(path: &Path, writable: bool) -> io::Result<NativeFile> {
    initialize_native_io_services()?;
    let mut options = OpenOptions::new();
    options.read(true).write(writable);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
        // SAFETY: the exact open above supplies the required flag.
        return unsafe {
            NativeFile::from_overlapped_file_unchecked(options.open(path)?, None)
                .map_err(|(error, _)| error)
        };
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
