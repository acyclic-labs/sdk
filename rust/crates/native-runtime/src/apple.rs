//! Positional I/O for arbitrary Apple file handles.
//!
//! Reads fill uninitialized spare capacity, and each run of contiguous
//! requests, up to the platform `IOV_MAX`, moves through one vectored call
//! instead of one call per request. Read buffers of a page or more come from
//! a bounded pool and return to it when the last `Bytes` view drops, so a
//! steady stream of large reads lands in memory the kernel has already mapped
//! instead of faulting fresh pages in during the copy. Once one run of a
//! scattered batch proves uncached, the kernel is asked to start reading every
//! later run, so they overlap on the device instead of queueing behind one
//! worker.

#![allow(
    unsafe_code,
    reason = "positional transfers hand held descriptors and owned buffers to the kernel"
)]

use crate::sync::Mutex;
use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::ffi::{CStr, c_int, c_void};
use std::fs::File;
use std::io;
use std::ops::DerefMut;
use std::os::fd::{AsRawFd as _, RawFd};
use std::sync::{OnceLock, PoisonError};
use std::time::{Duration, Instant};

/// Darwin's `IOV_MAX`, used when `sysconf` cannot report the limit.
const IOV_MAX_FALLBACK: usize = 1024;
/// Darwin rejects a single transfer of more than `INT_MAX` bytes.
const MAX_TRANSFER: usize = i32::MAX as usize;
/// A run that takes longer than this was not served from the page cache:
/// cached reads finish in single-digit microseconds, device reads take tens
/// to hundreds.
const UNCACHED: Duration = Duration::from_micros(40);

type Vectored = unsafe extern "C" fn(c_int, *const libc::iovec, c_int, libc::off_t) -> isize;

#[derive(Clone, Copy)]
enum Direction {
    Read,
    Write,
}

/// `preadv`/`pwritev` exist from macOS 11. Older `x86_64` deployments resolve
/// neither and move each segment through its own positional call.
fn vectored(direction: Direction) -> Option<Vectored> {
    static READ: OnceLock<Option<Vectored>> = OnceLock::new();
    static WRITE: OnceLock<Option<Vectored>> = OnceLock::new();
    let (slot, name): (_, &CStr) = match direction {
        Direction::Read => (&READ, c"preadv"),
        Direction::Write => (&WRITE, c"pwritev"),
    };
    *slot.get_or_init(|| {
        // SAFETY: dlsym only reads the NUL-terminated symbol name.
        let symbol = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
        // SAFETY: both symbols have exactly the `Vectored` C signature.
        (!symbol.is_null()).then(|| unsafe { std::mem::transmute::<*mut c_void, Vectored>(symbol) })
    })
}

/// Segments per vectored call: the platform `IOV_MAX`, queried once.
fn segment_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        // SAFETY: sysconf only reads a system configuration value.
        let reported = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
        usize::try_from(reported)
            .ok()
            .filter(|&limit| limit > 0)
            .unwrap_or(IOV_MAX_FALLBACK)
    })
}

/// Pooled buffers are sized in whole Apple silicon pages, and smaller reads
/// keep plain exact allocations (the allocator already reuses those).
const POOL_PAGE: usize = 16 * 1024;
/// Idle bytes the pool may hold across all shards.
const POOL_BUDGET: usize = 64 << 20;
const POOL_SHARDS: usize = 8;

/// Idle read buffers, grouped by capacity. Each I/O worker takes from its own
/// shard, and a buffer returns to the shard it came from, so a worker
/// contends only with threads releasing its earlier results.
///
/// Lock order: the shard lock is a leaf. Nothing else is locked, woken or
/// called while it is held, and a buffer the pool rejects is freed only after
/// it is released. The lock comes from `crate::sync`, so the Loom model drives
/// this same code.
struct Shard {
    idle: Mutex<Idle>,
}

struct Idle {
    classes: Vec<(usize, Vec<Vec<u8>>)>,
    bytes: usize,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    broken_accounting: bool,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    broken_ownership: bool,
}

impl Idle {
    const EMPTY: Self = Self {
        classes: Vec::new(),
        bytes: 0,
        #[cfg(all(test, acyclic_native_runtime_loom))]
        broken_accounting: false,
        #[cfg(all(test, acyclic_native_runtime_loom))]
        broken_ownership: false,
    };
}

#[cfg(not(acyclic_native_runtime_loom))]
static SHARDS: [Shard; POOL_SHARDS] = [const { Shard::new() }; POOL_SHARDS];

thread_local! {
    static LOCAL_SHARD: Cell<Option<&'static Shard>> = const { Cell::new(None) };
}

impl Shard {
    #[cfg(not(acyclic_native_runtime_loom))]
    const fn new() -> Self {
        Self {
            idle: Mutex::new(Idle::EMPTY),
        }
    }

    /// Model builds create each shard inside the model, with a Loom lock.
    #[cfg(acyclic_native_runtime_loom)]
    fn new() -> Self {
        Self {
            idle: Mutex::new(Idle::EMPTY),
        }
    }

    /// The calling worker's shard, assigned round-robin on first use.
    #[cfg(not(acyclic_native_runtime_loom))]
    fn local() -> &'static Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        LOCAL_SHARD.with(|local| {
            local.get().unwrap_or_else(|| {
                let index = NEXT.fetch_add(1, Ordering::Relaxed) % POOL_SHARDS;
                let shard = SHARDS.get(index).unwrap_or(&SHARDS[0]);
                local.set(Some(shard));
                shard
            })
        })
    }

    /// Model builds have no process-wide shards; each thread gets its own.
    #[cfg(acyclic_native_runtime_loom)]
    fn local() -> &'static Self {
        LOCAL_SHARD.with(|local| {
            local.get().unwrap_or_else(|| {
                let shard: &'static Self = Box::leak(Box::new(Self::new()));
                local.set(Some(shard));
                shard
            })
        })
    }

    /// Gives the calling thread a shard no other thread uses.
    #[cfg(test)]
    fn isolate() -> &'static Self {
        let shard: &'static Self = Box::leak(Box::new(Self::new()));
        LOCAL_SHARD.with(|local| local.set(Some(shard)));
        shard
    }

    fn idle(&self) -> impl DerefMut<Target = Idle> + '_ {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// An empty buffer with exactly `class` capacity, reused when one is idle.
    fn take(&self, class: usize) -> io::Result<Vec<u8>> {
        let reused = {
            let mut idle = self.idle();
            let reused = idle
                .classes
                .iter_mut()
                .find(|(capacity, _)| *capacity == class)
                .and_then(|(_, buffers)| buffers.pop());
            #[cfg(all(test, acyclic_native_runtime_loom))]
            let broken = idle.broken_accounting;
            #[cfg(not(all(test, acyclic_native_runtime_loom)))]
            let broken = false;
            if reused.is_some() && !broken {
                idle.bytes -= class;
            }
            reused
        };
        if let Some(mut buffer) = reused {
            buffer.clear();
            return Ok(buffer);
        }
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(class)?;
        Ok(buffer)
    }

    /// Keeps `buffer` for reuse while the shard is under its budget; otherwise
    /// it is freed after the lock is released.
    fn put(&self, buffer: Vec<u8>) {
        let rejected = self.idle().keep(buffer);
        drop(rejected);
    }
}

impl Idle {
    /// Keeps `buffer` within the budget, or hands it back to be freed.
    fn keep(&mut self, buffer: Vec<u8>) -> Option<Vec<u8>> {
        let class = buffer.capacity();
        if self.bytes + class > POOL_BUDGET / POOL_SHARDS {
            return Some(buffer);
        }
        #[cfg(all(test, acyclic_native_runtime_loom))]
        if self.broken_ownership {
            return Some(buffer);
        }
        if !self.classes.iter().any(|(capacity, _)| *capacity == class) {
            if self.classes.try_reserve(1).is_err() {
                return Some(buffer);
            }
            self.classes.push((class, Vec::new()));
        }
        let Some((_, buffers)) = self
            .classes
            .iter_mut()
            .find(|(capacity, _)| *capacity == class)
        else {
            return Some(buffer);
        };
        if buffers.try_reserve(1).is_err() {
            return Some(buffer);
        }
        buffers.push(buffer);
        self.bytes += class;
        None
    }
}

/// The owner behind a pooled read result: the bytes the kernel filled, and
/// the shard its storage returns to when the last view drops.
struct Recycled {
    buffer: Vec<u8>,
    shard: &'static Shard,
}

impl AsRef<[u8]> for Recycled {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl Drop for Recycled {
    fn drop(&mut self) {
        self.shard.put(std::mem::take(&mut self.buffer));
    }
}

const fn pooled(length: usize) -> bool {
    length >= POOL_PAGE
}

/// An empty buffer for a `length`-byte read, from `shard` when pooled.
fn read_buffer(length: usize, shard: &'static Shard) -> io::Result<Vec<u8>> {
    if pooled(length) {
        return shard.take(length.div_ceil(POOL_PAGE) * POOL_PAGE);
    }
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(length)?;
    Ok(buffer)
}

/// The caller-facing view of a filled `length`-byte read buffer. A pooled
/// buffer returns to `shard`, the one it was taken from.
fn read_result(buffer: Vec<u8>, length: usize, shard: &'static Shard) -> Bytes {
    if pooled(length) {
        Bytes::from_owner(Recycled { buffer, shard })
    } else {
        Bytes::from(buffer)
    }
}

/// Per-thread transfer state. Each I/O worker reserves it once, at the segment
/// limit, on its first batch and reuses it, so a steady-state read allocates
/// only its result buffers and a write allocates nothing.
#[derive(Default)]
struct Scratch {
    segments: Vec<libc::iovec>,
    buffers: Vec<Vec<u8>>,
}

thread_local! {
    static SCRATCH: RefCell<Scratch> = RefCell::default();
}

fn with_scratch<T>(run: impl FnOnce(&mut Scratch, usize) -> io::Result<T>) -> io::Result<T> {
    let limit = segment_limit();
    SCRATCH.with_borrow_mut(|scratch| {
        scratch.segments.try_reserve_exact(limit)?;
        scratch.buffers.try_reserve_exact(limit)?;
        let result = run(scratch, limit);
        // Drop kernel-facing pointers and any buffers an error left behind.
        scratch.segments.clear();
        scratch.buffers.clear();
        result
    })
}

fn validate_range(offset: u64, length: usize) -> io::Result<()> {
    let length = u64::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "I/O length is too large"))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "I/O range overflow"))?;
    i64::try_from(end)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "I/O range is too large"))?;
    Ok(())
}

/// Length of the leading run, at most `limit`, whose ranges each start where
/// the previous one ends.
fn contiguous<T>(items: &[T], limit: usize, span: impl Fn(&T) -> (u64, usize)) -> usize {
    1 + items
        .iter()
        .zip(items.iter().skip(1))
        .take(limit.saturating_sub(1))
        .take_while(|(previous, next)| {
            let (offset, size) = span(previous);
            span(next).0 == offset + size as u64
        })
        .count()
}

/// Up to `limit` non-empty segments totalling at most `MAX_TRANSFER` bytes,
/// built in the worker's reserved segment list.
struct Segments<'a> {
    list: &'a mut Vec<libc::iovec>,
    limit: usize,
    requested: usize,
}

impl<'a> Segments<'a> {
    fn new(list: &'a mut Vec<libc::iovec>, limit: usize) -> Self {
        list.clear();
        Self {
            list,
            limit,
            requested: 0,
        }
    }

    /// Starts an empty list in the same storage.
    fn reset(self) -> Self {
        Self::new(self.list, self.limit)
    }

    /// Adds up to `length` bytes at `base`; the reserved list never grows.
    fn push(&mut self, base: *mut c_void, length: usize) {
        let length = length.min(MAX_TRANSFER - self.requested);
        if length > 0 && self.list.len() < self.limit.min(self.list.capacity()) {
            self.list.push(libc::iovec {
                iov_base: base,
                iov_len: length,
            });
            self.requested += length;
        }
    }

    /// Moves the segments at `offset`; fewer bytes than requested means end of file.
    fn transfer(
        &self,
        fd: RawFd,
        direction: Direction,
        offset: u64,
        vectored: Option<Vectored>,
    ) -> io::Result<usize> {
        let moved = transfer(fd, direction, self.list, offset, vectored)?;
        if moved > self.requested {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(moved)
    }
}

/// Moves `segments` at `offset`, one vectored call when available, else one
/// positional call per segment until the first short transfer. A write that
/// moves nothing for a non-empty segment is `WriteZero` even after earlier
/// segments succeeded, so no caller can mistake it for progress.
fn transfer(
    fd: RawFd,
    direction: Direction,
    segments: &[libc::iovec],
    offset: u64,
    vectored: Option<Vectored>,
) -> io::Result<usize> {
    let Some(call) = vectored.filter(|_| segments.len() > 1) else {
        let mut moved = 0;
        for segment in segments {
            let at = offset + moved as u64;
            let count = retry(|| attempt(fd, direction, std::slice::from_ref(segment), at, None))?;
            if count == 0 && matches!(direction, Direction::Write) {
                return Err(io::ErrorKind::WriteZero.into());
            }
            moved += count;
            if count < segment.iov_len {
                break;
            }
        }
        return Ok(moved);
    };
    retry(|| attempt(fd, direction, segments, offset, Some(call)))
}

/// One raw positional call: the only place bytes cross the kernel boundary.
/// Tests record every attempt here, retries included, and can inject faults.
fn attempt(
    fd: RawFd,
    direction: Direction,
    segments: &[libc::iovec],
    offset: u64,
    call: Option<Vectored>,
) -> io::Result<usize> {
    #[cfg(test)]
    acyclic_test_support::differential::record((
        direction.name(call.is_some()),
        offset,
        segments
            .iter()
            .map(|segment| segment.iov_len as u64)
            .sum::<u64>(),
    ));
    let at = position(offset)?;
    let raw = |segments: &[libc::iovec]| {
        let count = match (call, segments) {
            (Some(call), _) => {
                let count =
                    c_int::try_from(segments.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
                // SAFETY: every segment names live owned storage; count is at most IOV_MAX.
                unsafe { call(fd, segments.as_ptr(), count, at) }
            }
            // SAFETY: the segment names live owned storage of iov_len bytes.
            (None, [segment]) => unsafe {
                match direction {
                    Direction::Read => libc::pread(fd, segment.iov_base, segment.iov_len, at),
                    Direction::Write => libc::pwrite(fd, segment.iov_base, segment.iov_len, at),
                }
            },
            (None, _) => return Err(io::ErrorKind::InvalidInput.into()),
        };
        usize::try_from(count).map_err(|_| io::Error::last_os_error())
    };
    #[cfg(test)]
    return faults::attempt(segments, raw);
    #[cfg(not(test))]
    raw(segments)
}

impl Direction {
    #[cfg(test)]
    const fn name(self, vectored: bool) -> &'static str {
        match (self, vectored) {
            (Self::Read, false) => "pread",
            (Self::Read, true) => "preadv",
            (Self::Write, false) => "pwrite",
            (Self::Write, true) => "pwritev",
        }
    }
}

fn position(offset: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput.into())
}

/// Repeats an attempt interrupted by a signal before it moved any bytes.
fn retry(mut attempt: impl FnMut() -> io::Result<usize>) -> io::Result<usize> {
    loop {
        let result = attempt();
        if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::Interrupted)
            || !retries_interrupts()
        {
            return result;
        }
    }
}

fn retries_interrupts() -> bool {
    #[cfg(test)]
    let retries = !faults::mutated(faults::Mutation::DropRetry);
    #[cfg(not(test))]
    let retries = true;
    retries
}

pub(super) fn read_batch(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    read_batch_with(file, reads, vectored(Direction::Read))
}

fn read_batch_with(
    file: &File,
    reads: &[OwnedRead],
    vectored: Option<Vectored>,
) -> io::Result<Vec<Bytes>> {
    for read in reads {
        validate_range(read.offset, read.length)?;
    }
    let mut results = Vec::new();
    results.try_reserve_exact(reads.len())?;
    let shard = Shard::local();
    with_scratch(|scratch, limit| {
        let mut rest = reads;
        let mut advised = false;
        let mut index = 0_usize;
        while !rest.is_empty() {
            let (run, next) =
                rest.split_at(contiguous(rest, limit, |read| (read.offset, read.length)));
            for read in run {
                scratch.buffers.push(read_buffer(read.length, shard)?);
            }
            // Only a batch with later runs can benefit, and it advises once.
            // Timing runs 1, 2, 4, 8, ... keeps a cached batch at a logarithmic
            // number of clock reads while still catching a cold batch early.
            index += 1;
            let timed = !advised && !next.is_empty() && sampled(index);
            let started = timed.then(Instant::now);
            fill(
                file.as_raw_fd(),
                run,
                &mut scratch.buffers,
                Segments::new(&mut scratch.segments, limit),
                vectored,
            )?;
            results.extend(
                scratch
                    .buffers
                    .drain(..)
                    .zip(run)
                    .map(|(buffer, read)| read_result(buffer, read.length, shard)),
            );
            if started.is_some_and(uncached) {
                advise(file.as_raw_fd(), next, limit);
                advised = true;
            }
            rest = next;
        }
        Ok(results)
    })
}

/// Whether the `index`th run (from 1) of a batch is timed.
fn sampled(index: usize) -> bool {
    #[cfg(test)]
    if faults::mutated(faults::Mutation::TimeEveryRun) {
        return true;
    }
    index.is_power_of_two()
}

/// Whether the run that began at `started` had to wait for the device.
fn uncached(started: Instant) -> bool {
    #[cfg(test)]
    return faults::uncached(started);
    #[cfg(not(test))]
    exceeds(started, UNCACHED)
}

fn exceeds(started: Instant, threshold: Duration) -> bool {
    started.elapsed() >= threshold
}

/// Asks the kernel to start reading every run of `reads` now. `F_RDADVISE`
/// queues asynchronous read-ahead and returns without waiting, so the later
/// positional reads find their pages arriving or resident. It is only advice:
/// a refusal changes nothing the reads themselves will not report.
fn advise(fd: RawFd, reads: &[OwnedRead], limit: usize) {
    let mut rest = reads;
    while !rest.is_empty() {
        let (run, next) = rest.split_at(contiguous(rest, limit, |read| (read.offset, read.length)));
        rest = next;
        let Some(first) = run.first() else {
            continue;
        };
        let bytes = run
            .iter()
            .fold(0_usize, |total, read| total.saturating_add(read.length));
        let (Ok(count), Ok(offset)) = (
            c_int::try_from(bytes.min(MAX_TRANSFER)),
            position(first.offset),
        ) else {
            continue;
        };
        if count == 0 {
            continue;
        }
        #[cfg(test)]
        acyclic_test_support::differential::record(("rdadvise", first.offset, bytes as u64));
        let advice = libc::radvisory {
            ra_offset: offset,
            ra_count: count,
        };
        // SAFETY: F_RDADVISE only reads the advice record for the live descriptor.
        let _ = unsafe { libc::fcntl(fd, libc::F_RDADVISE, &raw const advice) };
    }
}

/// Fills each buffer's spare capacity from its contiguous range until full or end of file.
fn fill(
    fd: RawFd,
    reads: &[OwnedRead],
    buffers: &mut [Vec<u8>],
    mut segments: Segments<'_>,
    vectored: Option<Vectored>,
) -> io::Result<()> {
    loop {
        segments = segments.reset();
        let mut offset = None;
        for (buffer, read) in buffers
            .iter_mut()
            .zip(reads)
            .skip_while(|(buffer, read)| buffer.len() == read.length)
        {
            offset.get_or_insert(read.offset + buffer.len() as u64);
            segments.push(
                buffer.spare_capacity_mut().as_mut_ptr().cast(),
                read.length - buffer.len(),
            );
        }
        let Some(offset) = offset else {
            return Ok(());
        };
        let received = segments.transfer(fd, Direction::Read, offset, vectored)?;
        #[cfg(test)]
        let received = faults::reported(received, segments.requested);
        let mut remaining = received;
        for (buffer, read) in buffers
            .iter_mut()
            .zip(reads)
            .skip_while(|(buffer, read)| buffer.len() == read.length)
        {
            let take = remaining.min(read.length - buffer.len());
            // SAFETY: the kernel initialized `take` bytes at the start of this
            // buffer's spare capacity, in segment order.
            unsafe { buffer.set_len(buffer.len() + take) };
            remaining -= take;
        }
        if received < segments.requested {
            return Ok(());
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "takes the owned batch like the portable write_all_batch_owned it serves"
)]
pub(super) fn write_all_batch_owned(file: &File, writes: Vec<OwnedWrite>) -> io::Result<()> {
    write_batch_with(file, &writes, vectored(Direction::Write))
}

fn write_batch_with(
    file: &File,
    writes: &[OwnedWrite],
    vectored: Option<Vectored>,
) -> io::Result<()> {
    // Reject an invalid batch before it can partially change the file.
    for write in writes {
        validate_range(write.offset, write.bytes.len())?;
    }
    with_scratch(|scratch, limit| {
        let mut rest = writes;
        while !rest.is_empty() {
            let (run, next) = rest.split_at(contiguous(rest, limit, |write| {
                (write.offset, write.bytes.len())
            }));
            drain(
                file.as_raw_fd(),
                run,
                Segments::new(&mut scratch.segments, limit),
                vectored,
            )?;
            rest = next;
        }
        Ok(())
    })
}

/// Writes a contiguous run completely, resuming after short transfers.
fn drain(
    fd: RawFd,
    writes: &[OwnedWrite],
    mut segments: Segments<'_>,
    vectored: Option<Vectored>,
) -> io::Result<()> {
    let Some(first) = writes.first() else {
        return Ok(());
    };
    let total: usize = writes.iter().map(|write| write.bytes.len()).sum();
    let mut done = 0;
    while done < total {
        segments = segments.reset();
        #[cfg(not(test))]
        let mut skip = done;
        #[cfg(test)]
        let mut skip = faults::resume_point(writes, done);
        for write in writes {
            let length = write.bytes.len();
            let pending = write.bytes.get(skip.min(length)..).unwrap_or_default();
            skip = skip.saturating_sub(length);
            segments.push(pending.as_ptr().cast_mut().cast(), pending.len());
        }
        let moved =
            segments.transfer(fd, Direction::Write, first.offset + done as u64, vectored)?;
        if moved == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        done += moved;
    }
    Ok(())
}

pub(super) fn sync_once(file: &File, durability: crate::Durability) -> io::Result<()> {
    // SAFETY: both operations act only on the live descriptor.
    let result = unsafe {
        match durability {
            crate::Durability::Full => libc::fsync(file.as_raw_fd()),
            crate::Durability::Barrier => libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC),
        }
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
#[path = "apple_faults.rs"]
mod faults;

#[cfg(test)]
#[path = "apple_tests.rs"]
mod tests;

#[cfg(all(test, acyclic_native_runtime_loom))]
#[path = "apple_model.rs"]
mod model;
