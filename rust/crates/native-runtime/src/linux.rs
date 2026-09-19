//! Linux positional I/O through `io_uring`.

#![allow(unsafe_code)]

use io_uring::{IoUring, opcode, types};
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;

const RING_ENTRIES: u32 = 16;
const MAX_IO_BYTES: usize = 1024 * 1024;

thread_local! {
    static STATE: RefCell<RingState> = RefCell::new(RingState {
        ring: IoUring::new(RING_ENTRIES).ok(),
        quarantine: None,
    });
}

struct RingState {
    ring: Option<IoUring>,
    quarantine: Option<QuarantinedIo>,
}

#[allow(dead_code)]
struct QuarantinedIo {
    _ring: IoUring,
    _buffers: QuarantinedBuffers,
}

#[allow(dead_code)]
enum QuarantinedBuffers {
    Reads(Vec<Vec<u8>>),
    Writes(Vec<OwnedWrite>),
}

pub(super) fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    if destination.is_empty() {
        return Ok(0);
    }
    let length = destination.len().min(MAX_IO_BYTES);
    let len = u32::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "read exceeds io_uring limit"))?;
    let destination = destination
        .get_mut(..length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read exceeds destination"))?;
    let fd = file.as_raw_fd();
    STATE.with_borrow_mut(|state| {
        if state.quarantine.is_some() {
            return Err(quarantined_error());
        }
        let entry = opcode::Read::new(types::Fd(fd), destination.as_mut_ptr(), len)
            .offset(offset)
            .build();
        match submit_retry(&mut state.ring, &entry) {
            Ok(count) if count <= destination.len() => Ok(count),
            Ok(_) => Err(io::Error::other("read exceeded submitted length")),
            Err(error) => Err(error),
        }
    })
}

pub(super) fn write_all_at(file: &File, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    let fd = file.as_raw_fd();
    while !bytes.is_empty() {
        let count = STATE.with_borrow_mut(|state| {
            if state.quarantine.is_some() {
                return Err(quarantined_error());
            }
            let length = u32::try_from(bytes.len().min(MAX_IO_BYTES))
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "write too large"))?;
            let entry = opcode::Write::new(types::Fd(fd), bytes.as_ptr(), length)
                .offset(offset)
                .build();
            submit_retry(&mut state.ring, &entry)
        })?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "positional write returned zero",
            ));
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        bytes = bytes.get(count..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "write exceeded submitted length",
            )
        })?;
    }
    Ok(())
}

pub(super) fn write_all_batch_owned(file: &File, writes: Vec<OwnedWrite>) -> io::Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    let fd = file.as_raw_fd();
    let mut remainders = Vec::new();
    STATE.with_borrow_mut(|state| {
        if state.quarantine.is_some() {
            return Err(quarantined_error());
        }
        let mut writes = writes.into_iter();
        loop {
            let window: Vec<_> = writes.by_ref().take(RING_ENTRIES as usize).collect();
            if window.is_empty() {
                break;
            }
            let mut entries = Vec::new();
            entries.try_reserve_exact(window.len())?;
            for (index, write) in window.iter().enumerate() {
                let length = u32::try_from(write.bytes.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "batch write exceeds io_uring limit",
                    )
                })?;
                entries.push(
                    opcode::Write::new(types::Fd(fd), write.bytes.as_ptr(), length)
                        .offset(write.offset)
                        .build()
                        .user_data(index as u64),
                );
            }
            let completions = match submit_batch(&mut state.ring, &entries, &window) {
                Ok(completions) => completions,
                Err(error) => {
                    if let BatchSubmitError::Uncertain(error) = error {
                        let failed = state.ring.take().ok_or_else(|| {
                            io::Error::other("io_uring disappeared during quarantine")
                        })?;
                        state.quarantine = Some(QuarantinedIo {
                            _ring: failed,
                            _buffers: QuarantinedBuffers::Writes(window),
                        });
                        return Err(error);
                    }
                    return Err(error.into_error());
                }
            };
            for (write, completed) in window.iter().zip(completions) {
                if completed < write.bytes.len() {
                    let offset = write.offset.checked_add(completed as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
                    })?;
                    let remaining = write.bytes.get(completed..).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "write exceeded submitted length",
                        )
                    })?;
                    remainders.push((offset, Bytes::copy_from_slice(remaining)));
                }
            }
        }
        Ok(())
    })?;
    for (offset, remaining) in remainders {
        write_all_at(file, offset, &remaining)?;
    }
    Ok(())
}

pub(super) fn read_batch(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let mut buffers = Vec::new();
    buffers.try_reserve_exact(reads.len())?;
    for read in reads {
        buffers.push(vec![0_u8; read.length]);
    }
    let fd = file.as_raw_fd();
    STATE.with_borrow_mut(|state| {
        if state.quarantine.is_some() {
            return Err(quarantined_error());
        }
        let mut completions = Vec::new();
        let mut request = 0_usize;
        while request < reads.len() {
            let mut window_end = request;
            let mut operation_count = 0_usize;
            while window_end < reads.len() {
                let operations = reads
                    .get(window_end)
                    .ok_or_else(|| io::Error::other("invalid read request"))?
                    .length
                    .div_ceil(MAX_IO_BYTES)
                    .max(1);
                if operation_count != 0
                    && operation_count.saturating_add(operations) > RING_ENTRIES as usize
                {
                    break;
                }
                if operations > RING_ENTRIES as usize {
                    break;
                }
                operation_count = operation_count.saturating_add(operations);
                window_end += 1;
            }
            if window_end == request {
                let read = *reads
                    .get(request)
                    .ok_or_else(|| io::Error::other("invalid read request"))?;
                let buffer = buffers
                    .get_mut(request)
                    .ok_or_else(|| io::Error::other("invalid read buffer"))?;
                let completed = match read_large(&mut state.ring, fd, read, buffer) {
                    Ok(completed) => completed,
                    Err(LargeReadError::Completed(error)) => return Err(error),
                    Err(LargeReadError::Uncertain(error)) => {
                        let failed = state.ring.take().ok_or_else(|| {
                            io::Error::other("io_uring disappeared during quarantine")
                        })?;
                        state.quarantine = Some(QuarantinedIo {
                            _ring: failed,
                            _buffers: QuarantinedBuffers::Reads(buffers),
                        });
                        return Err(error);
                    }
                };
                completions.extend(completed);
                request += 1;
                continue;
            }
            let window_reads = reads
                .get(request..window_end)
                .ok_or_else(|| io::Error::other("invalid read window"))?;
            let window_buffers = buffers
                .get_mut(request..window_end)
                .ok_or_else(|| io::Error::other("invalid read buffer window"))?;
            let operation_count =
                push_read_window(&mut state.ring, fd, window_reads, window_buffers)?;
            let Some(active) = state.ring.as_mut() else {
                return Err(io::Error::other("io_uring is unavailable"));
            };
            match drain_reads(active, operation_count) {
                Ok(window) => completions.extend(window),
                Err(DrainReadError::Completed(error)) => return Err(error),
                Err(DrainReadError::Uncertain(error)) => {
                    let failed = state.ring.take().ok_or_else(|| {
                        io::Error::other("io_uring disappeared during quarantine")
                    })?;
                    state.quarantine = Some(QuarantinedIo {
                        _ring: failed,
                        _buffers: QuarantinedBuffers::Reads(buffers),
                    });
                    return Err(error);
                }
            }
            request = window_end;
        }
        assemble_reads(buffers, completions)
    })
}

fn quarantined_error() -> io::Error {
    io::Error::other("io_uring is quarantined after an uncertain completion")
}

fn read_large(
    ring: &mut Option<IoUring>,
    fd: i32,
    read: OwnedRead,
    buffer: &mut [u8],
) -> Result<Vec<Option<usize>>, LargeReadError> {
    let mut completions = Vec::new();
    let mut offset = read.offset;
    for window in buffer.chunks_mut(MAX_IO_BYTES * RING_ENTRIES as usize) {
        let Some(active) = ring.as_mut() else {
            return Err(LargeReadError::Completed(io::Error::other(
                "io_uring is unavailable",
            )));
        };
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(window.len().div_ceil(MAX_IO_BYTES))
            .map_err(|error| LargeReadError::Completed(error.into()))?;
        for (identity, chunk) in window.chunks_mut(MAX_IO_BYTES).enumerate() {
            let length = u32::try_from(chunk.len()).map_err(|_| {
                LargeReadError::Completed(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read is too large",
                ))
            })?;
            entries.push(
                opcode::Read::new(types::Fd(fd), chunk.as_mut_ptr(), length)
                    .offset(offset)
                    .build()
                    .user_data(identity as u64),
            );
            offset = offset.checked_add(chunk.len() as u64).ok_or_else(|| {
                LargeReadError::Completed(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read offset overflow",
                ))
            })?;
        }
        {
            let mut submission = active.submission();
            // SAFETY: every descriptor and buffer remains live until all matching CQEs are drained.
            unsafe { submission.push_multiple(&entries) }.map_err(|_| {
                LargeReadError::Completed(io::Error::other("io_uring submission queue is full"))
            })?;
        }
        match drain_reads(active, entries.len()) {
            Ok(window) => completions.extend(window),
            Err(DrainReadError::Completed(error)) => return Err(LargeReadError::Completed(error)),
            Err(DrainReadError::Uncertain(error)) => return Err(LargeReadError::Uncertain(error)),
        }
    }
    Ok(completions)
}

enum LargeReadError {
    Completed(io::Error),
    Uncertain(io::Error),
}

fn push_read_window(
    ring: &mut Option<IoUring>,
    fd: i32,
    reads: &[OwnedRead],
    buffers: &mut [Vec<u8>],
) -> io::Result<usize> {
    let Some(active) = ring.as_mut() else {
        return Err(io::Error::other("io_uring is unavailable"));
    };
    let mut entries = Vec::new();
    entries.try_reserve_exact(reads.len())?;
    let mut operation = 0_u64;
    for (read, buffer) in reads.iter().zip(buffers) {
        for (chunk_index, chunk) in buffer.chunks_mut(MAX_IO_BYTES).enumerate() {
            let offset = read
                .offset
                .checked_add((chunk_index * MAX_IO_BYTES) as u64)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow")
                })?;
            let length = u32::try_from(chunk.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "read is too large"))?;
            entries.push(
                opcode::Read::new(types::Fd(fd), chunk.as_mut_ptr(), length)
                    .offset(offset)
                    .build()
                    .user_data(operation),
            );
            operation += 1;
        }
        if buffer.is_empty() {
            entries.push(
                opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), 0)
                    .offset(read.offset)
                    .build()
                    .user_data(operation),
            );
            operation += 1;
        }
    }
    let mut submission = active.submission();
    // SAFETY: every descriptor and buffer remains live until all matching CQEs are drained.
    unsafe { submission.push_multiple(&entries) }
        .map_err(|_| io::Error::other("io_uring submission queue is full"))?;
    Ok(entries.len())
}

enum DrainReadError {
    Completed(io::Error),
    Uncertain(io::Error),
}

fn drain_reads(ring: &mut IoUring, count: usize) -> Result<Vec<Option<usize>>, DrainReadError> {
    let mut batch = ReadCompletions::new(count);
    while batch.remaining() != 0 {
        match ring.submit_and_wait(batch.remaining()) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(DrainReadError::Uncertain(error)),
        }
        for completion in ring.completion() {
            if let Err(error) = batch.record(completion.user_data(), completion.result()) {
                return Err(DrainReadError::Uncertain(error));
            }
        }
    }
    batch.finish().map_err(DrainReadError::Completed)
}

struct ReadCompletions {
    completed: Vec<Option<usize>>,
    remaining: usize,
    first_error: Option<io::Error>,
}

impl ReadCompletions {
    fn new(count: usize) -> Self {
        Self {
            completed: vec![None; count],
            remaining: count,
            first_error: None,
        }
    }

    const fn remaining(&self) -> usize {
        self.remaining
    }

    fn record(&mut self, identity: u64, result: i32) -> io::Result<()> {
        let Ok(index) = usize::try_from(identity) else {
            return Err(io::Error::other("invalid read identity"));
        };
        let Some(slot) = self.completed.get_mut(index) else {
            return Err(io::Error::other("unknown read identity"));
        };
        if slot.is_some() {
            return Err(io::Error::other("duplicate read identity"));
        }
        self.remaining -= 1;
        match completion_result_code(result) {
            Ok(bytes) => *slot = Some(bytes),
            Err(error) => {
                self.first_error.get_or_insert(error);
                *slot = Some(0);
            }
        }
        Ok(())
    }

    fn finish(self) -> io::Result<Vec<Option<usize>>> {
        if self.remaining != 0 && self.first_error.is_none() {
            return Err(io::Error::other("missing read completions"));
        }
        self.first_error.map_or(Ok(self.completed), Err)
    }
}

fn assemble_reads(
    buffers: Vec<Vec<u8>>,
    completions: Vec<Option<usize>>,
) -> io::Result<Vec<Bytes>> {
    let mut completed = completions.into_iter();
    let mut output = Vec::new();
    output.try_reserve_exact(buffers.len())?;
    for mut buffer in buffers {
        let mut total = 0_usize;
        let mut reached_eof = false;
        let chunks = buffer.len().div_ceil(MAX_IO_BYTES).max(1);
        for chunk in 0..chunks {
            let count = completed
                .next()
                .flatten()
                .ok_or_else(|| io::Error::other("missing read completion"))?;
            let submitted = buffer
                .len()
                .saturating_sub(chunk * MAX_IO_BYTES)
                .min(MAX_IO_BYTES);
            if count > submitted {
                return Err(io::Error::other("read exceeded submitted length"));
            }
            if !reached_eof {
                total = total
                    .checked_add(count)
                    .ok_or_else(|| io::Error::other("read result overflow"))?;
                reached_eof = count != submitted;
            }
        }
        buffer.truncate(total);
        output.push(Bytes::from(buffer));
    }
    Ok(output)
}

fn submit_batch(
    ring: &mut Option<IoUring>,
    entries: &[io_uring::squeue::Entry],
    writes: &[OwnedWrite],
) -> Result<Vec<usize>, BatchSubmitError> {
    let Some(active) = ring.as_mut() else {
        return Err(BatchSubmitError::Completed(io::Error::other(
            "io_uring is unavailable",
        )));
    };
    {
        let mut submission = active.submission();
        // SAFETY: all file descriptors and owned buffers remain live until every CQE is drained.
        unsafe { submission.push_multiple(entries) }.map_err(|_| {
            BatchSubmitError::Completed(io::Error::other("io_uring submission queue is full"))
        })?;
    }
    let mut batch = BatchCompletions::new(writes);
    while batch.remaining() != 0 {
        match active.submit_and_wait(batch.remaining()) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BatchSubmitError::Uncertain(error)),
        };
        let completions: Vec<_> = active.completion().collect();
        for completion in completions {
            if let Err(error) = batch.record(completion.user_data(), completion.result()) {
                return Err(BatchSubmitError::Uncertain(error));
            }
        }
    }
    batch.finish().map_err(BatchSubmitError::Completed)
}

enum BatchSubmitError {
    Completed(io::Error),
    Uncertain(io::Error),
}

impl BatchSubmitError {
    fn into_error(self) -> io::Error {
        match self {
            Self::Completed(error) | Self::Uncertain(error) => error,
        }
    }
}

struct BatchCompletions<'a> {
    writes: &'a [OwnedWrite],
    completed: Vec<bool>,
    remaining: usize,
    first_error: Option<io::Error>,
    lengths: Vec<usize>,
}

impl<'a> BatchCompletions<'a> {
    fn new(writes: &'a [OwnedWrite]) -> Self {
        Self {
            writes,
            completed: vec![false; writes.len()],
            remaining: writes.len(),
            first_error: None,
            lengths: vec![0; writes.len()],
        }
    }

    const fn remaining(&self) -> usize {
        self.remaining
    }

    fn record_error(&mut self, error: io::Error) {
        self.first_error.get_or_insert(error);
    }

    fn record(&mut self, identity: u64, result: i32) -> io::Result<()> {
        let Ok(index) = usize::try_from(identity) else {
            return Err(io::Error::other("invalid batch completion identity"));
        };
        let Some(write) = self.writes.get(index) else {
            return Err(io::Error::other("unknown batch completion identity"));
        };
        let Some(completed) = self.completed.get_mut(index) else {
            return Err(io::Error::other("unknown batch completion identity"));
        };
        if std::mem::replace(completed, true) {
            return Err(io::Error::other("duplicate batch completion identity"));
        }
        self.remaining = self.remaining.saturating_sub(1);
        match completion_result_code(result) {
            Ok(count) if count > 0 && count <= write.bytes.len() => {
                let Some(length) = self.lengths.get_mut(index) else {
                    return Err(io::Error::other("unknown batch completion identity"));
                };
                *length = count;
            }
            Ok(0) => self.record_error(io::Error::new(
                io::ErrorKind::WriteZero,
                "positional write returned zero",
            )),
            Ok(_) => self.record_error(io::Error::other("overlong batch write")),
            Err(error) => self.record_error(error),
        }
        Ok(())
    }

    fn finish(self) -> io::Result<Vec<usize>> {
        if self.remaining != 0 && self.first_error.is_none() {
            return Err(io::Error::other("missing batch completions"));
        }
        self.first_error.map_or(Ok(self.lengths), Err)
    }
}

fn submit_retry(ring: &mut Option<IoUring>, entry: &io_uring::squeue::Entry) -> io::Result<usize> {
    submit_one(ring, entry)
}

fn submit_one(ring: &mut Option<IoUring>, entry: &io_uring::squeue::Entry) -> io::Result<usize> {
    let Some(active) = ring.as_mut() else {
        return Err(io::Error::other("io_uring is unavailable"));
    };
    // SAFETY: callers keep the descriptor and buffer live until the matching CQE.
    // This ring has no SQPOLL thread and this function never returns while an SQE
    // might still be executing in the kernel.
    unsafe { active.submission().push(entry) }
        .map_err(|_| io::Error::other("io_uring submission queue is full"))?;
    let mut submission_error = None;
    let result = loop {
        match active.submit_and_wait(1) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                if let Some(completion) = active.completion().next() {
                    break completion_result(&completion);
                }
                continue;
            }
            Err(error) => {
                submission_error = Some(error);
                if let Some(completion) = active.completion().next() {
                    break completion_result(&completion);
                }
                let mut submissions = active.submission();
                submissions.sync();
                if submissions.is_empty() {
                    // The SQE left userspace but no CQE proves completion. The
                    // borrowed buffer must remain live, so fail closed by
                    // retaining this thread and ring until the CQE arrives.
                    // Once the matching completion arrives, its result is the
                    // authoritative operation outcome. The submission error
                    // only quarantines this ring from subsequent reuse.
                    continue;
                }
                break Err(io::Error::other("io_uring submission was not consumed"));
            }
        }
        if let Some(completion) = active.completion().next() {
            break completion_result(&completion);
        }
    };
    if submission_error.is_some() {
        *ring = None;
    }
    result
}

fn completion_result(completion: &io_uring::cqueue::Entry) -> io::Result<usize> {
    completion_result_code(completion.result())
}

fn completion_result_code(result: i32) -> io::Result<usize> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        usize::try_from(result).map_err(|_| io::Error::other("invalid io_uring completion"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writes() -> [OwnedWrite; 3] {
        [
            OwnedWrite {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"one"),
            },
            OwnedWrite {
                offset: 8,
                bytes: bytes::Bytes::from_static(b"four"),
            },
            OwnedWrite {
                offset: 16,
                bytes: bytes::Bytes::from_static(b"five!"),
            },
        ]
    }

    #[test]
    fn completion_errors_preserve_operating_system_kinds() {
        let Err(interrupted) = completion_result_code(-libc::EINTR) else {
            unreachable!()
        };
        assert_eq!(interrupted.kind(), io::ErrorKind::Interrupted);
        let Err(unsupported) = completion_result_code(-libc::EOPNOTSUPP) else {
            unreachable!()
        };
        assert_eq!(unsupported.raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(completion_result_code(17).ok(), Some(17));
    }

    #[test]
    fn batch_completions_accept_out_of_order_results() -> io::Result<()> {
        let writes = writes();
        let mut batch = BatchCompletions::new(&writes);
        batch.record(2, 5)?;
        batch.record(0, 3)?;
        batch.record(1, 4)?;
        assert_eq!(batch.remaining(), 0);
        assert_eq!(batch.finish().ok().as_deref(), Some([3, 4, 5].as_slice()));
        Ok(())
    }

    #[test]
    fn short_chunk_does_not_shift_later_read_completions() -> io::Result<()> {
        let buffers = vec![vec![0_u8; MAX_IO_BYTES + 1], vec![b'x']];
        let reads = assemble_reads(buffers, vec![Some(0), Some(0), Some(1)])?;
        assert_eq!(reads, [Bytes::new(), Bytes::from_static(b"x")]);
        Ok(())
    }

    #[test]
    fn read_completions_fail_closed_on_duplicate_or_unknown_identities() -> io::Result<()> {
        let mut batch = ReadCompletions::new(2);
        batch.record(0, 3)?;
        let Err(duplicate) = batch.record(0, 3) else {
            return Err(io::Error::other("duplicate read identity was accepted"));
        };
        assert_eq!(duplicate.to_string(), "duplicate read identity");
        let Err(unknown) = batch.record(9, 1) else {
            return Err(io::Error::other("unknown read identity was accepted"));
        };
        assert_eq!(unknown.to_string(), "unknown read identity");
        assert_eq!(batch.remaining(), 1);
        batch.record(1, 4)?;
        assert_eq!(batch.remaining(), 0);
        assert_eq!(batch.finish()?, [Some(3), Some(4)]);
        Ok(())
    }

    #[test]
    fn batch_completions_drain_after_first_error() -> io::Result<()> {
        let writes = writes();
        let mut batch = BatchCompletions::new(&writes);
        batch.record(1, 2)?;
        assert_eq!(batch.remaining(), 2);
        batch.record(2, -libc::EIO)?;
        batch.record(0, 3)?;
        let error = batch
            .finish()
            .err()
            .unwrap_or_else(|| io::Error::other("short write unexpectedly succeeded"));
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(error.to_string(), "Input/output error (os error 5)");
        Ok(())
    }

    #[test]
    fn batch_completions_reject_duplicate_and_unknown_identities() -> io::Result<()> {
        let writes = writes();
        let mut duplicate = BatchCompletions::new(&writes);
        duplicate.record(0, 3)?;
        let Err(error) = duplicate.record(0, 3) else {
            return Err(io::Error::other("duplicate batch identity was accepted"));
        };
        assert_eq!(error.to_string(), "duplicate batch completion identity");

        let mut unknown = BatchCompletions::new(&writes);
        let Err(error) = unknown.record(9, 1) else {
            return Err(io::Error::other("unknown batch identity was accepted"));
        };
        assert_eq!(error.to_string(), "unknown batch completion identity");
        Ok(())
    }

    #[test]
    fn unfinished_batch_reports_missing_completion() -> io::Result<()> {
        let writes = writes();
        let mut batch = BatchCompletions::new(&writes);
        batch.record(0, 3)?;
        let error = batch
            .finish()
            .err()
            .unwrap_or_else(|| io::Error::other("missing completions unexpectedly succeeded"));
        assert_eq!(error.to_string(), "missing batch completions");
        Ok(())
    }
}
