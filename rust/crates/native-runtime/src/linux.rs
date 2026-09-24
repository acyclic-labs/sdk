//! Linux positional I/O through one owned Compio completion path.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use compio_buf::{IntoInner, SetLen, Slice};
use compio_driver::{
    Key, Proactor, PushEntry, SharedFd,
    op::{ReadAt, WriteAt},
};

const IO_ENTRIES: usize = 16;
const MAX_IO_BYTES: usize = 1024 * 1024;

const MAX_TRACKED_FILE_IDENTITIES: usize = 4_096;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct FileIdentity {
    device: u64,
    inode: u64,
}

pub(super) fn file_identity_capacity() -> usize {
    static CAPACITY: OnceLock<usize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid writable rlimit structure.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return 128;
        }
        usize::try_from(limit.rlim_cur)
            .unwrap_or(usize::MAX)
            .saturating_div(4)
            .clamp(1, MAX_TRACKED_FILE_IDENTITIES)
    })
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

struct SubmittedWindowUnwindFence(bool);

impl SubmittedWindowUnwindFence {
    const fn new() -> Self {
        Self(true)
    }

    fn disarm(&mut self) {
        self.0 = false;
    }
}

impl Drop for SubmittedWindowUnwindFence {
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
    validate_io_range(offset, destination.len(), "read")?;
    let length = destination.len().min(MAX_IO_BYTES);
    let destination = destination
        .get_mut(..length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read exceeds destination"))?;
    file.read_at(destination, offset)
}

pub(super) fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    validate_io_range(offset, bytes.len(), "write")?;
    file.write_all_at(bytes, offset)
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

fn validate_io_range(offset: u64, length: usize, operation: &str) -> io::Result<()> {
    if offset == u64::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{operation} offset aliases the shared file cursor"),
        ));
    }
    let length = u64::try_from(length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{operation} length is invalid"),
        )
    })?;
    offset.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{operation} range overflows"),
        )
    })?;
    Ok(())
}

const OWNER_THREADS: usize = 4;
const OWNER_MAILBOX: usize = 16;
const OWNER_POLL_SLICE: Duration = Duration::from_millis(100);
const OWNER_IO_DEADLINE: Duration = Duration::from_secs(30);
// Four owners * one retired ring * sixteen <=1 MiB owned buffers = 64 MiB.
const MAX_RETIRED_PER_OWNER: usize = 1;

type OwnedReadOp = ReadAt<Slice<Vec<u8>>, SharedFd<Arc<File>>>;
type OwnedWriteOp = WriteAt<Bytes, SharedFd<Arc<File>>>;
type ReadFinish = Box<dyn FnOnce(io::Result<Vec<Bytes>>) + Send>;
type WriteFinish = Box<dyn FnOnce(io::Result<()>) + Send>;

enum OwnerJob {
    Read(Arc<File>, Vec<OwnedRead>, ReadFinish),
    Write(Arc<File>, Vec<OwnedWrite>, WriteFinish),
    Stop,
}

struct OwnerPool {
    senders: Vec<SyncSender<OwnerJob>>,
    cursor: AtomicUsize,
}

struct OwnerFactory {
    pool: OnceLock<OwnerPool>,
    initializing: Mutex<()>,
}

impl OwnerFactory {
    const fn new() -> Self {
        Self {
            pool: OnceLock::new(),
            initializing: Mutex::new(()),
        }
    }

    fn get_or_start(&self) -> io::Result<&OwnerPool> {
        if let Some(pool) = self.pool.get() {
            return Ok(pool);
        }
        let _lock = self
            .initializing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pool) = self.pool.get() {
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
        Ok(self.pool.get_or_init(|| OwnerPool {
            senders,
            cursor: AtomicUsize::new(0),
        }))
    }
}

fn owner_pool() -> io::Result<&'static OwnerPool> {
    static FACTORY: OwnerFactory = OwnerFactory::new();
    FACTORY.get_or_start()
}

impl OwnerPool {
    fn submit(&self, mut job: OwnerJob) -> io::Result<()> {
        let count = self.senders.len();
        if count == 0 {
            return Err(io::Error::other("missing native I/O owners"));
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % count;
        for index in 0..count {
            let sender = self
                .senders
                .get((start + index) % count)
                .ok_or_else(|| io::Error::other("missing native I/O owner"))?;
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
        self.senders
            .get(start)
            .ok_or_else(|| io::Error::other("missing native I/O owner"))?
            .send(job)
            .map_err(|_| io::Error::other("native I/O owner stopped before admission"))
    }
}

pub(super) fn submit_read(
    file: Arc<File>,
    reads: Vec<OwnedRead>,
    finish: ReadFinish,
) -> io::Result<()> {
    for read in &reads {
        validate_io_range(read.offset, read.length, "batch read")?;
    }
    owner_pool()?.submit(OwnerJob::Read(file, reads, finish))
}

pub(super) fn submit_write(
    file: Arc<File>,
    writes: Vec<OwnedWrite>,
    finish: WriteFinish,
) -> io::Result<()> {
    for write in &writes {
        validate_io_range(write.offset, write.bytes.len(), "batch write")?;
    }
    owner_pool()?.submit(OwnerJob::Write(file, writes, finish))
}

enum RetiredKey {
    Read(Key<OwnedReadOp>),
    Write(Key<OwnedWriteOp>),
}

struct RetiredProactor {
    driver: Proactor,
    keys: Vec<RetiredKey>,
    armed: bool,
}

impl Drop for RetiredProactor {
    fn drop(&mut self) {
        if self.armed {
            // A pending typed key still owns a kernel-visible buffer. It may
            // leave this owner only after a terminal pop.
            std::process::abort();
        }
    }
}

impl RetiredProactor {
    fn poll_terminal(&mut self) -> bool {
        // Even a failed poll may have processed CQEs before returning; only
        // typed key completion, never the poll status, releases owned buffers.
        let _ = self.driver.poll(Some(Duration::ZERO));
        let mut pending = Vec::with_capacity(self.keys.len());
        for key in std::mem::take(&mut self.keys) {
            match key {
                RetiredKey::Read(key) => match self.driver.pop(key) {
                    PushEntry::Ready(_) => {}
                    PushEntry::Pending(key) => pending.push(RetiredKey::Read(key)),
                },
                RetiredKey::Write(key) => match self.driver.pop(key) {
                    PushEntry::Ready(_) => {}
                    PushEntry::Pending(key) => pending.push(RetiredKey::Write(key)),
                },
            }
        }
        self.keys = pending;
        if self.keys.is_empty() {
            self.armed = false;
        }
        !self.armed
    }
}

struct Owner {
    driver: Option<Proactor>,
    unavailable: bool,
    retired: Vec<RetiredProactor>,
}

impl Owner {
    fn new() -> Self {
        Self {
            driver: None,
            unavailable: false,
            retired: Vec::new(),
        }
    }

    fn run(mut self, receiver: &mpsc::Receiver<OwnerJob>) {
        loop {
            let next = if self.retired.is_empty() {
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
        if !self.retired.is_empty() {
            std::process::abort();
        }
    }

    fn reap_retired(&mut self) {
        self.retired.retain_mut(|retired| !retired.poll_terminal());
    }

    fn ensure_driver(&mut self) -> io::Result<bool> {
        if self.unavailable || self.retired.len() >= MAX_RETIRED_PER_OWNER {
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

    fn retire(&mut self, keys: Vec<RetiredKey>, error: io::Error) -> io::Error {
        let driver = self.driver.take().unwrap_or_else(|| std::process::abort());
        self.retired.push(RetiredProactor {
            driver,
            keys,
            armed: true,
        });
        uncertain_completion(error)
    }

    fn read_batch(&mut self, file: &Arc<File>, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
        let mut output = Vec::with_capacity(reads.len());
        let mut remaining = reads;
        while let Some(first) = remaining.first() {
            // Independent small reads share one bounded SQ window. Large
            // reads retain the single-key, zero-copy fast path in read_one.
            let parallel = remaining
                .iter()
                .take(IO_ENTRIES)
                .take_while(|read| read.length <= MAX_IO_BYTES)
                .count();
            if parallel < 2 {
                output.push(self.read_one(file, *first)?);
                remaining = remaining.get(1..).unwrap_or_else(|| std::process::abort());
                continue;
            }
            let batch = remaining
                .get(..parallel)
                .unwrap_or_else(|| std::process::abort());
            output.extend(self.read_small_batch(file, batch)?);
            remaining = remaining
                .get(parallel..)
                .unwrap_or_else(|| std::process::abort());
        }
        Ok(output)
    }

    fn read_small_batch(
        &mut self,
        file: &Arc<File>,
        reads: &[OwnedRead],
    ) -> io::Result<Vec<Bytes>> {
        if !self.ensure_driver()? {
            return reads
                .iter()
                .map(|read| self.read_one(file, *read))
                .collect();
        }
        let lengths: Vec<_> = reads.iter().map(|read| read.length).collect();
        let mut results = vec![None; reads.len()];
        let mut pending = Vec::with_capacity(reads.len());
        let mut unwind_fence = SubmittedWindowUnwindFence::new();
        let mut first_error = None;
        let mut push_error = None;
        let driver = self
            .driver
            .as_mut()
            .unwrap_or_else(|| std::process::abort());
        for (index, read) in reads.iter().enumerate() {
            if read.length == 0 {
                if let Some(slot) = results.get_mut(index) {
                    *slot = Some(Bytes::new());
                }
                continue;
            }
            let operation = ReadAt::new(
                SharedFd::new(Arc::clone(file)),
                read.offset,
                compio_buf::IoBuf::slice(Vec::with_capacity(read.length), ..read.length),
            );
            match driver.push(operation) {
                PushEntry::Ready(result) => {
                    let (status, operation) = result.into_parts();
                    let count = match status {
                        Ok(count) => count,
                        Err(error) => {
                            push_error = Some(error);
                            break;
                        }
                    };
                    let slot = results
                        .get_mut(index)
                        .unwrap_or_else(|| std::process::abort());
                    if let Err(error) = record_owned_read(
                        compio_buf::BufResult(Ok(count), operation),
                        read.length,
                        slot,
                    ) {
                        first_error.get_or_insert(error);
                    }
                }
                PushEntry::Pending(key) => pending.push((index, key)),
            }
        }
        if let Some(error) = self.drain_read_window(pending, &lengths, &mut results)? {
            first_error.get_or_insert(error);
        }
        if let Some(error) = push_error {
            unwind_fence.disarm();
            self.driver = None;
            return Err(error);
        }
        if let Some(error) = first_error {
            unwind_fence.disarm();
            return Err(error);
        }
        unwind_fence.disarm();
        results
            .into_iter()
            .map(|result| result.ok_or_else(|| io::Error::other("missing native read completion")))
            .collect()
    }

    fn read_one(&mut self, file: &Arc<File>, read: OwnedRead) -> io::Result<Bytes> {
        let mut bytes = Vec::new();
        let mut count = 0_usize;
        while count < read.length {
            if !self.ensure_driver()? {
                let length = (read.length - count).min(MAX_IO_BYTES);
                let offset = read.offset + count as u64;
                let mut chunk = vec![0_u8; length];
                let received = file.read_at(&mut chunk, offset)?;
                chunk.truncate(received);
                bytes.extend_from_slice(&chunk);
                count += received;
                if received < length {
                    break;
                }
                continue;
            }
            let lengths = owned_read_window_lengths(read.length - count);
            let mut results = vec![None; lengths.len()];
            let mut pending = Vec::with_capacity(lengths.len());
            let mut unwind_fence = SubmittedWindowUnwindFence::new();
            let mut first_error = None;
            let mut push_error = None;
            let driver = self
                .driver
                .as_mut()
                .unwrap_or_else(|| std::process::abort());
            let mut offset_in_window = 0_usize;
            for (index, length) in lengths.iter().copied().enumerate() {
                let op = ReadAt::new(
                    SharedFd::new(Arc::clone(file)),
                    read.offset + (count + offset_in_window) as u64,
                    compio_buf::IoBuf::slice(Vec::with_capacity(length), ..length),
                );
                match driver.push(op) {
                    PushEntry::Ready(result) => {
                        let (status, operation) = result.into_parts();
                        let count = match status {
                            Ok(count) => count,
                            Err(error) => {
                                push_error = Some(error);
                                break;
                            }
                        };
                        let slot = results
                            .get_mut(index)
                            .unwrap_or_else(|| std::process::abort());
                        if let Err(error) = record_owned_read(
                            compio_buf::BufResult(Ok(count), operation),
                            length,
                            slot,
                        ) {
                            first_error.get_or_insert(error);
                        }
                    }
                    PushEntry::Pending(key) => pending.push((index, key)),
                }
                offset_in_window += length;
            }
            if let Some(error) = self.drain_read_window(pending, &lengths, &mut results)? {
                first_error.get_or_insert(error);
            }
            if let Some(error) = push_error {
                unwind_fence.disarm();
                self.driver = None;
                return Err(error);
            }
            if let Some(error) = first_error {
                unwind_fence.disarm();
                return Err(error);
            }
            unwind_fence.disarm();
            let (window, eof) = assemble_owned_read_window(lengths, results)?;
            count += window.len();
            if bytes.is_empty() && (eof || count == read.length) {
                return Ok(window);
            }
            bytes.extend_from_slice(&window);
            if eof {
                break;
            }
        }
        Ok(Bytes::from(bytes))
    }

    fn drain_read_window(
        &mut self,
        mut pending: Vec<(usize, Key<OwnedReadOp>)>,
        lengths: &[usize],
        results: &mut [Option<Bytes>],
    ) -> io::Result<Option<io::Error>> {
        let mut unwind_fence = SubmittedWindowUnwindFence::new();
        let mut first_error = None;
        let deadline = std::time::Instant::now() + OWNER_IO_DEADLINE;
        while !pending.is_empty() {
            if std::time::Instant::now() >= deadline {
                let keys = pending
                    .into_iter()
                    .map(|(_, key)| RetiredKey::Read(key))
                    .collect();
                return Err(self.retire(keys, io::ErrorKind::TimedOut.into()));
            }
            let driver = self
                .driver
                .as_mut()
                .unwrap_or_else(|| std::process::abort());
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
                Err(error) => {
                    let keys = pending
                        .into_iter()
                        .map(|(_, key)| RetiredKey::Read(key))
                        .collect();
                    return Err(self.retire(keys, error));
                }
            }
            let mut still_pending = Vec::with_capacity(pending.len());
            for (index, key) in pending {
                match driver.pop(key) {
                    PushEntry::Ready(result) => {
                        let length = lengths
                            .get(index)
                            .copied()
                            .unwrap_or_else(|| std::process::abort());
                        let slot = results
                            .get_mut(index)
                            .unwrap_or_else(|| std::process::abort());
                        if let Err(error) = record_owned_read(result, length, slot) {
                            first_error.get_or_insert(error);
                        }
                    }
                    PushEntry::Pending(key) => still_pending.push((index, key)),
                }
            }
            pending = still_pending;
        }
        unwind_fence.disarm();
        Ok(first_error)
    }

    fn write_batch(&mut self, file: &Arc<File>, writes: Vec<OwnedWrite>) -> io::Result<()> {
        let mut chunks = chunk_owned_writes(writes);
        loop {
            let window: Vec<_> = chunks
                .by_ref()
                .take(IO_ENTRIES)
                .collect::<io::Result<_>>()?;
            if window.is_empty() {
                break;
            }
            if !self.ensure_driver()? {
                for write in window {
                    file.write_all_at(&write.bytes, write.offset)?;
                }
                continue;
            }
            let driver = self
                .driver
                .as_mut()
                .unwrap_or_else(|| std::process::abort());
            let mut pending = Vec::with_capacity(window.len());
            let mut unwind_fence = SubmittedWindowUnwindFence::new();
            let mut push_error = None;
            let mut completed: Vec<Option<io::Result<usize>>> =
                std::iter::repeat_with(|| None).take(window.len()).collect();
            for (index, write) in window.iter().enumerate() {
                let op = WriteAt::new(
                    SharedFd::new(Arc::clone(file)),
                    write.offset,
                    write.bytes.clone(),
                );
                match driver.push(op) {
                    PushEntry::Ready(result) => {
                        let (status, _) = result.into_parts();
                        let count = match status {
                            Ok(count) => count,
                            Err(error) => {
                                push_error = Some(error);
                                break;
                            }
                        };
                        *completed
                            .get_mut(index)
                            .unwrap_or_else(|| std::process::abort()) = Some(Ok(count));
                    }
                    PushEntry::Pending(key) => pending.push((index, key)),
                }
            }
            let deadline = std::time::Instant::now() + OWNER_IO_DEADLINE;
            while !pending.is_empty() {
                if std::time::Instant::now() >= deadline {
                    let keys = pending
                        .into_iter()
                        .map(|(_, key)| RetiredKey::Write(key))
                        .collect();
                    return Err(self.retire(keys, io::ErrorKind::TimedOut.into()));
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
                    Err(error) => {
                        let keys = pending
                            .into_iter()
                            .map(|(_, key)| RetiredKey::Write(key))
                            .collect();
                        return Err(self.retire(keys, error));
                    }
                }
                let mut still_pending = Vec::with_capacity(pending.len());
                for (index, key) in pending {
                    match driver.pop(key) {
                        PushEntry::Ready(result) => {
                            *completed
                                .get_mut(index)
                                .unwrap_or_else(|| std::process::abort()) =
                                Some(result.into_parts().0);
                        }
                        PushEntry::Pending(key) => still_pending.push((index, key)),
                    }
                }
                pending = still_pending;
            }
            unwind_fence.disarm();
            if let Some(error) = push_error {
                self.driver = None;
                return Err(error);
            }
            finish_owned_write_window(file, window, completed)?;
        }
        Ok(())
    }
}

fn owned_read_window_lengths(remaining: usize) -> Vec<usize> {
    // One owned request per 16 MiB window avoids per-key completion overhead.
    // The window remains within the same retirement memory bound as 16 x 1 MiB.
    vec![remaining.min(MAX_IO_BYTES * IO_ENTRIES)]
}

fn assemble_owned_read_window(
    lengths: Vec<usize>,
    results: Vec<Option<Bytes>>,
) -> io::Result<(Bytes, bool)> {
    if results.len() == 1 {
        let length = lengths
            .first()
            .copied()
            .ok_or_else(|| io::Error::other("missing native read length"))?;
        let chunk = results
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| io::Error::other("missing native read completion"))?;
        let eof = chunk.len() < length;
        return Ok((chunk, eof));
    }
    let mut output = Vec::new();
    let mut eof = false;
    for (length, result) in lengths.into_iter().zip(results) {
        let chunk = result.ok_or_else(|| io::Error::other("missing native read completion"))?;
        if !eof {
            output.extend_from_slice(&chunk);
            eof = chunk.len() < length;
        }
    }
    Ok((Bytes::from(output), eof))
}

fn finish_owned_write_window(
    file: &File,
    window: Vec<OwnedWrite>,
    completed: Vec<Option<io::Result<usize>>>,
) -> io::Result<()> {
    let mut first_error = None;
    let mut short = Vec::new();
    for (write, result) in window.into_iter().zip(completed) {
        match result.ok_or_else(|| io::Error::other("missing native write completion"))? {
            Ok(0) => {
                first_error.get_or_insert_with(|| io::ErrorKind::WriteZero.into());
            }
            Ok(count) if count > write.bytes.len() => {
                first_error.get_or_insert_with(|| io::Error::other("overlong native write"));
            }
            Ok(count) if count < write.bytes.len() => short.push((write, count)),
            Ok(_) => {}
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    for (write, count) in short {
        let suffix = write
            .bytes
            .get(count..)
            .ok_or_else(|| io::Error::other("invalid native write suffix"))?;
        file.write_all_at(suffix, write.offset + count as u64)?;
    }
    Ok(())
}

fn record_owned_read(
    result: compio_buf::BufResult<usize, OwnedReadOp>,
    requested: usize,
    slot: &mut Option<Bytes>,
) -> io::Result<()> {
    let (count, operation) = result.into_parts();
    let count = count?;
    if count > requested {
        return Err(io::Error::other("overlong native read"));
    }
    let mut buffer = operation.into_inner();
    // SAFETY: terminal completion initialized the reported prefix, bounded
    // above by the submitted slice length.
    unsafe { buffer.set_len(count) };
    *slot = Some(Bytes::from(buffer.into_inner()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_chunks_share_the_owned_buffer() -> io::Result<()> {
        let bytes = Bytes::from(vec![0x5a; MAX_IO_BYTES + 3]);
        let start = bytes.as_ptr();
        let chunks = chunk_owned_writes(vec![OwnedWrite { offset: 7, bytes }])
            .collect::<io::Result<Vec<_>>>()?;
        let [first, second] = chunks.as_slice() else {
            return Err(io::Error::other("write chunk count changed"));
        };
        assert_eq!(first.offset, 7);
        assert_eq!(
            second.offset,
            7 + u64::try_from(MAX_IO_BYTES).unwrap_or_default()
        );
        assert_eq!(first.bytes.as_ptr(), start);
        assert_eq!(second.bytes.as_ptr(), start.wrapping_add(MAX_IO_BYTES));
        Ok(())
    }

    #[test]
    fn compio_owner_callbacks_preserve_owned_batch_results() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = Arc::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temporary.path().join("owned-owner"))?,
        );
        let (written, write_result) = mpsc::sync_channel(1);
        submit_write(
            Arc::clone(&file),
            vec![OwnedWrite {
                offset: 2,
                bytes: Bytes::from_static(b"owned"),
            }],
            Box::new(move |result| {
                let _ = written.send(result);
            }),
        )?;
        write_result
            .recv_timeout(Duration::from_secs(2))
            .map_err(io::Error::other)??;

        let (read, read_result) = mpsc::sync_channel(1);
        submit_read(
            file,
            vec![
                OwnedRead {
                    offset: 2,
                    length: 5,
                },
                OwnedRead {
                    offset: 7,
                    length: 5,
                },
            ],
            Box::new(move |result| {
                let _ = read.send(result);
            }),
        )?;
        assert_eq!(
            read_result
                .recv_timeout(Duration::from_secs(2))
                .map_err(io::Error::other)??,
            vec![Bytes::from_static(b"owned"), Bytes::new()]
        );
        Ok(())
    }

    #[test]
    fn compio_parallel_read_window_preserves_order_empty_and_eof() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = Arc::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temporary.path().join("parallel-reads"))?,
        );
        let mut requests = Vec::new();
        for index in 0..16_usize {
            let offset = ((index * 7) % 16) * 8192;
            file.write_all_at(
                &vec![u8::try_from(index).unwrap_or_default(); 8192],
                offset as u64,
            )?;
            requests.push(OwnedRead {
                offset: offset as u64,
                length: 8192,
            });
        }
        requests.push(OwnedRead {
            offset: 0,
            length: 0,
        });
        requests.push(OwnedRead {
            offset: 16 * 8192,
            length: 8192,
        });
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_read(
            file,
            requests,
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        let actual = receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        for (index, bytes) in actual.iter().take(16).enumerate() {
            assert_eq!(bytes.len(), 8192);
            assert!(
                bytes
                    .iter()
                    .all(|byte| *byte == u8::try_from(index).unwrap_or_default())
            );
        }
        assert!(actual.get(16).is_some_and(Bytes::is_empty));
        assert!(actual.get(17).is_some_and(Bytes::is_empty));
        Ok(())
    }

    #[test]
    fn compio_owner_drains_multiple_submission_windows_and_eof() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = Arc::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temporary.path().join("owned-windows"))?,
        );
        let writes = (0..32_u64)
            .map(|offset| OwnedWrite {
                offset,
                bytes: Bytes::from(vec![u8::try_from(offset).unwrap_or_default()]),
            })
            .collect();
        let (sent, received) = mpsc::sync_channel(1);
        submit_write(
            Arc::clone(&file),
            writes,
            Box::new(move |result| {
                let _ = sent.send(result);
            }),
        )?;
        received
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        let (sent, received) = mpsc::sync_channel(1);
        submit_read(
            Arc::clone(&file),
            vec![OwnedRead {
                offset: 0,
                length: 64,
            }],
            Box::new(move |result| {
                let _ = sent.send(result);
            }),
        )?;
        let result = received
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        let first = result
            .first()
            .ok_or_else(|| io::Error::other("missing first read"))?;
        assert_eq!(first.len(), 32);
        assert_eq!(first.as_ref(), &(0..32_u8).collect::<Vec<_>>());

        file.set_len(17 * MAX_IO_BYTES as u64)?;
        let (sent, received) = mpsc::sync_channel(1);
        submit_read(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 18 * MAX_IO_BYTES,
            }],
            Box::new(move |result| {
                let _ = sent.send(result);
            }),
        )?;
        let result = received
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        assert_eq!(result.first().map(Bytes::len), Some(17 * MAX_IO_BYTES));
        Ok(())
    }

    #[test]
    fn compio_poll_failure_retains_key_and_allows_unrelated_progress() -> io::Result<()> {
        let mut owner = Owner::new();
        if !owner.ensure_driver()? {
            return Ok(());
        }
        let temporary = tempfile::tempdir()?;
        let affected = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(temporary.path().join("affected"))?,
        );
        affected.set_len(MAX_IO_BYTES as u64)?;
        let op = ReadAt::new(
            SharedFd::new(Arc::clone(&affected)),
            0,
            compio_buf::IoBuf::slice(Vec::with_capacity(MAX_IO_BYTES), ..MAX_IO_BYTES),
        );
        let key = match owner
            .driver
            .as_mut()
            .ok_or_else(|| io::Error::other("missing proactor"))?
            .push(op)
        {
            PushEntry::Pending(key) => key,
            PushEntry::Ready(_) => return Err(io::Error::other("read did not enter proactor")),
        };
        let error = owner.retire(
            vec![RetiredKey::Read(key)],
            io::Error::other("injected poll failure"),
        );
        assert!(is_uncertain_completion(&error));
        assert_eq!(owner.retired.len(), 1);
        assert!(!owner.ensure_driver()?);

        let unrelated = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(temporary.path().join("unrelated"))?,
        );
        owner.write_batch(
            &unrelated,
            vec![OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"progress"),
            }],
        )?;
        assert_eq!(
            owner.read_batch(
                &unrelated,
                &[OwnedRead {
                    offset: 0,
                    length: 8
                }]
            )?,
            vec![Bytes::from_static(b"progress")]
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !owner.retired.is_empty() {
            owner.reap_retired();
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::other(
                    "retired proactor did not reach terminal completion",
                ));
            }
            std::thread::yield_now();
        }
        Ok(())
    }

    #[test]
    fn retired_compio_proactor_cannot_drop_pending_key() -> io::Result<()> {
        use std::os::unix::process::ExitStatusExt;

        const CHILD: &str = "ACYCLIC_TEST_RETIRED_COMPIO_DROP";
        let mut driver = match Proactor::new() {
            Ok(driver) => driver,
            Err(error) if ring_unavailable(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        if std::env::var_os(CHILD).is_some() {
            let temporary = tempfile::tempdir()?;
            let file = Arc::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(temporary.path().join("pending"))?,
            );
            file.set_len(MAX_IO_BYTES as u64)?;
            let op = ReadAt::new(
                SharedFd::new(file),
                0,
                compio_buf::IoBuf::slice(Vec::with_capacity(MAX_IO_BYTES), ..MAX_IO_BYTES),
            );
            let key = match driver.push(op) {
                PushEntry::Pending(key) => key,
                PushEntry::Ready(_) => return Err(io::Error::other("read did not enter proactor")),
            };
            drop(RetiredProactor {
                driver,
                keys: vec![RetiredKey::Read(key)],
                armed: true,
            });
            return Err(io::Error::other("pending proactor drop did not abort"));
        }
        drop(driver);
        let status = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("linux::tests::retired_compio_proactor_cannot_drop_pending_key")
            .env(CHILD, "1")
            .status()?;
        assert_eq!(status.signal(), Some(libc::SIGABRT));
        Ok(())
    }
}
