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
    read_at_validated(
        file,
        offset,
        crate::file::ReadDestination::Slice(destination),
    )
}

#[allow(
    unsafe_code,
    reason = "pread initializes only the validated held buffer"
)]
pub(super) fn read_at_validated(
    file: &File,
    offset: u64,
    mut destination: crate::file::ReadDestination<'_>,
) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let (pointer, length) = destination.parts()?;
    let length = length.min(MAX_IO_BYTES);
    // SAFETY: validation precedes this call; the descriptor and writable buffer
    // remain live for the single syscall, which never exceeds the buffer.
    let result = unsafe {
        libc::pread(
            file.as_raw_fd(),
            pointer.cast(),
            length,
            libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        let count = usize::try_from(result).map_err(|_| io::ErrorKind::InvalidData)?;
        if count > length {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let crate::file::ReadDestination::Spare { data, .. } = destination {
            // SAFETY: pread initialized exactly count bytes in the checked spare slice.
            unsafe { data.set_len(data.len() + count) };
        }
        Ok(count)
    }
}

pub(super) fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    validate_io_range(offset, bytes.len())?;
    file.write_all_at(bytes, offset)
}

#[allow(unsafe_code, reason = "fsync acts only on the held descriptor")]
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

mod backend;
mod retired;
use backend::Owner;
