//! Linux positional I/O through one owned Compio completion path.

#![allow(
    unsafe_code,
    reason = "positional I/O submits raw descriptors to the Compio driver"
)]

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::NativeFilePayload;
use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use compio_buf::{IntoInner, SetLen, Slice};
use compio_driver::{
    Key, Proactor, PushEntry, SharedFd,
    op::{ReadAt, WriteAt},
};

const IO_ENTRIES: usize = 16;
const MAX_IO_BYTES: usize = 1024 * 1024;

struct PendingUnwind(bool);

impl Drop for PendingUnwind {
    fn drop(&mut self) {
        if self.0 && std::thread::panicking() {
            std::process::abort();
        }
    }
}

fn ring_unavailable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EPERM | libc::EACCES | libc::ENOSYS | libc::EOPNOTSUPP)
    )
}

#[derive(Debug)]
struct UncertainCompletion(io::Error);

impl std::fmt::Display for UncertainCompletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "uncertain io_uring completion: {}", self.0)
    }
}

impl std::error::Error for UncertainCompletion {}

pub(super) fn is_uncertain_completion(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.downcast_ref::<UncertainCompletion>().is_some())
}

pub(super) fn uncertain_completion(error: io::Error) -> io::Error {
    io::Error::other(UncertainCompletion(error))
}

pub(super) fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    validate_io_range(offset, destination.len())?;
    read_at_validated(file, offset, destination)
}

#[allow(unsafe_code)]
pub(super) fn read_at_validated(
    file: &File,
    offset: u64,
    destination: &mut [u8],
) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let length = destination.len().min(MAX_IO_BYTES);
    // SAFETY: validation precedes this call; the descriptor and writable buffer
    // remain live for the single syscall, which never exceeds the buffer.
    let result = unsafe {
        libc::pread(
            file.as_raw_fd(),
            destination.as_mut_ptr().cast(),
            length,
            offset as libc::off_t,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

pub(super) fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    validate_io_range(offset, bytes.len())?;
    file.write_all_at(bytes, offset)
}

#[allow(unsafe_code)]
pub(super) fn sync_once(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: fsync only acts on the live descriptor.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn chunk_owned_writes(writes: Vec<OwnedWrite>) -> impl Iterator<Item = io::Result<OwnedWrite>> {
    writes
        .into_iter()
        .filter(|write| !write.bytes.is_empty())
        .flat_map(|write| {
            (0..write.bytes.len())
                .step_by(MAX_IO_BYTES)
                .map(move |start| {
                    let offset = write
                        .offset
                        .checked_add(u64::try_from(start).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
                        })?)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
                        })?;
                    let end = start.saturating_add(MAX_IO_BYTES).min(write.bytes.len());
                    Ok(OwnedWrite {
                        offset,
                        bytes: write.bytes.slice(start..end),
                    })
                })
        })
}

pub(super) fn validate_io_range(offset: u64, length: usize) -> io::Result<()> {
    let length = u64::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?;
    let end = offset
        .checked_add(length)
        .ok_or(io::ErrorKind::InvalidInput)?;
    if offset > libc::off_t::MAX as u64 || end > libc::off_t::MAX as u64 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(())
}

const OWNER_THREADS: usize = 4;
const OWNER_MAILBOX: usize = 16;
const OWNER_POLL_SLICE: Duration = Duration::from_millis(100);
const OWNER_IO_DEADLINE: Duration = Duration::from_secs(30);
// Each owner retains at most one retired request and its original buffer.

type OwnedReadOp = ReadAt<Slice<Vec<u8>>, SharedFd<Arc<NativeFilePayload>>>;
type OwnedWriteOp = WriteAt<Bytes, SharedFd<Arc<NativeFilePayload>>>;
type ReadFinish = Box<dyn FnOnce(io::Result<Vec<Bytes>>) + Send>;
type WriteFinish = Box<dyn FnOnce(io::Result<()>) + Send>;

enum OwnerJob {
    Read(Arc<NativeFilePayload>, Vec<OwnedRead>, ReadFinish),
    Write(Arc<NativeFilePayload>, Vec<OwnedWrite>, WriteFinish),
    Stop,
}

struct OwnerPool {
    senders: Vec<SyncSender<OwnerJob>>,
    cursor: AtomicUsize,
}

fn owner_pool() -> io::Result<&'static OwnerPool> {
    static POOL: OnceLock<OwnerPool> = OnceLock::new();
    static INITIALIZING: Mutex<()> = Mutex::new(());
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }
    let _lock = INITIALIZING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }
    let mut senders = Vec::with_capacity(OWNER_THREADS);
    let mut threads = Vec::with_capacity(OWNER_THREADS);
    for index in 0..OWNER_THREADS {
        let (sender, receiver) = mpsc::sync_channel(OWNER_MAILBOX);
        match std::thread::Builder::new()
            .name(format!("acyclic-compio-{index}"))
            .spawn(move || {
                if std::panic::catch_unwind(|| Owner::new().run(&receiver)).is_err() {
                    std::process::abort();
                }
            }) {
            Ok(thread) => {
                senders.push(sender);
                threads.push(thread);
            }
            Err(error) => {
                for sender in senders {
                    let _ = sender.send(OwnerJob::Stop);
                }
                for thread in threads {
                    if thread.join().is_err() {
                        std::process::abort();
                    }
                }
                return Err(error);
            }
        }
    }
    Ok(POOL.get_or_init(|| OwnerPool {
        senders,
        cursor: AtomicUsize::new(0),
    }))
}

impl OwnerPool {
    fn submit(&self, mut job: OwnerJob) -> io::Result<()> {
        let count = self.senders.len();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % count;
        for index in 0..count {
            let sender = &self.senders[(start + index) % count];
            match sender.try_send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Full(returned)) => job = returned,
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(io::Error::other(
                        "native I/O owner stopped before admission",
                    ));
                }
            }
        }
        self.senders[start]
            .send(job)
            .map_err(|_| io::Error::other("native I/O owner stopped before admission"))
    }
}

pub(super) fn submit_read(
    file: Arc<NativeFilePayload>,
    reads: Vec<OwnedRead>,
    finish: ReadFinish,
) -> io::Result<()> {
    for read in &reads {
        validate_io_range(read.offset, read.length)?;
    }
    owner_pool()?.submit(OwnerJob::Read(file, reads, finish))
}

pub(super) fn submit_write(
    file: Arc<NativeFilePayload>,
    writes: Vec<OwnedWrite>,
    finish: WriteFinish,
) -> io::Result<()> {
    for write in &writes {
        validate_io_range(write.offset, write.bytes.len())?;
    }
    owner_pool()?.submit(OwnerJob::Write(file, writes, finish))
}

enum RetiredKey {
    Read(Key<OwnedReadOp>),
    Write(Key<OwnedWriteOp>),
}

struct RetiredProactor {
    driver: Proactor,
    key: Option<RetiredKey>,
}
impl Drop for RetiredProactor {
    fn drop(&mut self) {
        if self.key.is_some() {
            std::process::abort();
        }
    }
}
impl RetiredProactor {
    fn poll_terminal(&mut self) -> bool {
        let _ = self.driver.poll(Some(Duration::ZERO));
        self.key = match self.key.take() {
            Some(RetiredKey::Read(key)) => match self.driver.pop(key) {
                PushEntry::Ready(_) => None,
                PushEntry::Pending(key) => Some(RetiredKey::Read(key)),
            },
            Some(RetiredKey::Write(key)) => match self.driver.pop(key) {
                PushEntry::Ready(_) => None,
                PushEntry::Pending(key) => Some(RetiredKey::Write(key)),
            },
            None => None,
        };
        self.key.is_none()
    }
}

struct Owner {
    driver: Option<Proactor>,
    unavailable: bool,
    retired: Option<RetiredProactor>,
}

impl Owner {
    fn new() -> Self {
        Self {
            driver: None,
            unavailable: false,
            retired: None,
        }
    }

    fn run(mut self, receiver: &mpsc::Receiver<OwnerJob>) {
        loop {
            let next = if self.retired.is_none() {
                receiver
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
            } else {
                receiver.recv_timeout(Duration::from_millis(100))
            };
            let job = match next {
                Ok(job) => job,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.reap_retired();
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            self.reap_retired();
            match job {
                OwnerJob::Read(file, reads, finish) => finish(self.read_batch(&file, &reads)),
                OwnerJob::Write(file, writes, finish) => finish(self.write_batch(&file, writes)),
                OwnerJob::Stop => break,
            }
        }
        if !self.retired.is_none() {
            std::process::abort();
        }
    }

    fn reap_retired(&mut self) {
        if self
            .retired
            .as_mut()
            .is_some_and(RetiredProactor::poll_terminal)
        {
            self.retired = None;
        }
    }

    fn ensure_driver(&mut self) -> io::Result<bool> {
        if self.unavailable || self.retired.is_some() {
            return Ok(false);
        }
        if self.driver.is_none() {
            match Proactor::new() {
                Ok(driver) => self.driver = Some(driver),
                Err(error) if ring_unavailable(&error) => {
                    self.unavailable = true;
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    fn retire(&mut self, key: RetiredKey, error: io::Error) -> io::Error {
        let driver = self.driver.take().unwrap_or_else(|| std::process::abort());
        self.retired = Some(RetiredProactor {
            driver,
            key: Some(key),
        });
        uncertain_completion(error)
    }

    fn read_batch(
        &mut self,
        file: &Arc<NativeFilePayload>,
        reads: &[OwnedRead],
    ) -> io::Result<Vec<Bytes>> {
        reads
            .iter()
            .map(|read| self.read_one(file, *read))
            .collect()
    }

    fn read_one(&mut self, file: &Arc<NativeFilePayload>, read: OwnedRead) -> io::Result<Bytes> {
        let mut bytes = Vec::new();
        let mut count = 0;
        while count < read.length {
            let offset = read.offset + count as u64;
            if !self.ensure_driver()? {
                let length = (read.length - count).min(MAX_IO_BYTES);
                let mut chunk = vec![0; length];
                let received = file.read_at(&mut chunk, offset)?;
                bytes.extend_from_slice(&chunk[..received]);
                count += received;
                if received < length {
                    break;
                }
                continue;
            }
            let length = (read.length - count).min(MAX_IO_BYTES * IO_ENTRIES);
            let operation = ReadAt::new(
                SharedFd::new(Arc::clone(file)),
                offset,
                compio_buf::IoBuf::slice(Vec::with_capacity(length), ..length),
            );
            let completion = self.finish(operation, RetiredKey::Read)?;
            let (received, operation) = completion.into_parts();
            let received = match received {
                Ok(count) => count,
                Err(error) => {
                    self.driver = None;
                    return Err(error);
                }
            };
            if received > length {
                return Err(io::Error::other("overlong native read"));
            }
            let mut buffer = operation.into_inner();
            // SAFETY: terminal completion initialized the bounded reported prefix.
            unsafe {
                buffer.set_len(received);
            }
            let chunk = Bytes::from(buffer.into_inner());
            count += received;
            let eof = received < length;
            if bytes.is_empty() && (eof || count == read.length) {
                return Ok(chunk);
            }
            bytes.extend_from_slice(&chunk);
            if eof {
                break;
            }
        }
        Ok(Bytes::from(bytes))
    }

    fn write_batch(
        &mut self,
        file: &Arc<NativeFilePayload>,
        writes: Vec<OwnedWrite>,
    ) -> io::Result<()> {
        for write in chunk_owned_writes(writes) {
            let write = write?;
            if !self.ensure_driver()? {
                file.write_all_at(&write.bytes, write.offset)?;
                continue;
            }
            let result = self.finish(
                WriteAt::new(
                    SharedFd::new(Arc::clone(file)),
                    write.offset,
                    write.bytes.clone(),
                ),
                RetiredKey::Write,
            )?;
            let count = match result.into_parts().0 {
                Ok(count) => count,
                Err(error) => {
                    self.driver = None;
                    return Err(error);
                }
            };
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            if count > write.bytes.len() {
                return Err(io::Error::other("overlong native write"));
            }
            if count < write.bytes.len() {
                file.write_all_at(&write.bytes[count..], write.offset + count as u64)?;
            }
        }
        Ok(())
    }

    fn finish<O: compio_driver::OpCode + 'static>(
        &mut self,
        operation: O,
        retired: fn(Key<O>) -> RetiredKey,
    ) -> io::Result<compio_buf::BufResult<usize, O>> {
        let mut fence = PendingUnwind(true);
        let driver = self
            .driver
            .as_mut()
            .unwrap_or_else(|| std::process::abort());
        let mut key = match driver.push(operation) {
            PushEntry::Ready(result) => {
                fence.0 = false;
                return Ok(result);
            }
            PushEntry::Pending(key) => key,
        };
        let deadline = std::time::Instant::now() + OWNER_IO_DEADLINE;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(self.retire(retired(key), io::ErrorKind::TimedOut.into()));
            }
            match driver.poll(Some(OWNER_POLL_SLICE)) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(self.retire(retired(key), error)),
            }
            match driver.pop(key) {
                PushEntry::Pending(pending) => key = pending,
                PushEntry::Ready(result) => {
                    fence.0 = false;
                    return Ok(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
