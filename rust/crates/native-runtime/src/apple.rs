//! Positional I/O for arbitrary Apple file handles.
//!
//! Reads fill uninitialized spare capacity, and each run of contiguous
//! requests, up to the platform `IOV_MAX`, moves through one vectored call
//! instead of one call per request.

#![allow(
    unsafe_code,
    reason = "positional transfers hand held descriptors and owned buffers to the kernel"
)]

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::cell::RefCell;
use std::ffi::{CStr, c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, RawFd};
use std::sync::OnceLock;

/// Darwin's `IOV_MAX`, used when `sysconf` cannot report the limit.
const IOV_MAX_FALLBACK: usize = 1024;
/// Darwin rejects a single transfer of more than `INT_MAX` bytes.
const MAX_TRANSFER: usize = i32::MAX as usize;

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
/// positional call per segment until the first short transfer.
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
    with_scratch(|scratch, limit| {
        let mut rest = reads;
        while !rest.is_empty() {
            let (run, next) =
                rest.split_at(contiguous(rest, limit, |read| (read.offset, read.length)));
            for read in run {
                let mut buffer = Vec::new();
                buffer.try_reserve_exact(read.length)?;
                scratch.buffers.push(buffer);
            }
            fill(
                file.as_raw_fd(),
                run,
                &mut scratch.buffers,
                Segments::new(&mut scratch.segments, limit),
                vectored,
            )?;
            results.extend(scratch.buffers.drain(..).map(Bytes::from));
            rest = next;
        }
        Ok(results)
    })
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
