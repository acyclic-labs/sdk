//! Test-only fault injection on the raw positional syscall, plus deliberate
//! mutations that negative controls use to prove the fault oracles can fail.

use super::{OwnedWrite, io};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

/// What the next raw attempt does instead of (or before) the real call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    /// Performs the real call unchanged.
    Pass,
    /// Fails with `EINTR` without entering the kernel.
    Interrupt,
    /// Performs the real call on at most this many leading bytes.
    Short(usize),
    /// Returns zero bytes moved without entering the kernel.
    Zero,
}

/// A deliberate defect in the transfer path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mutation {
    None,
    /// After a short write, resumes data at the next segment boundary.
    WrongResume,
    /// Returns `EINTR` instead of retrying it.
    DropRetry,
    /// Forgets the last byte of a short read that did move bytes.
    LoseByte,
}

thread_local! {
    static FAULTS: RefCell<VecDeque<Fault>> = const { RefCell::new(VecDeque::new()) };
    static MUTATION: Cell<Mutation> = const { Cell::new(Mutation::None) };
}

/// Queues faults for this thread's next raw attempts and sets the mutation.
pub(super) fn arm(faults: &[Fault], mutation: Mutation) {
    FAULTS.with_borrow_mut(|queue| {
        queue.clear();
        queue.extend(faults);
    });
    MUTATION.set(mutation);
}

/// Clears this thread's faults and mutation, returning how many faults were consumed.
pub(super) fn disarm(armed: usize) -> usize {
    MUTATION.set(Mutation::None);
    armed - FAULTS.with_borrow_mut(std::mem::take).len()
}

pub(super) fn mutated(mutation: Mutation) -> bool {
    MUTATION.get() == mutation
}

/// How many bytes a read reports after the kernel moved `received` of `requested`.
pub(super) fn reported(received: usize, requested: usize) -> usize {
    if mutated(Mutation::LoseByte) && received > 0 && received < requested {
        received - 1
    } else {
        received
    }
}

/// Where a write run resumes after `done` bytes moved. `WrongResume` keeps the
/// first attempt intact and moves only later resumes to the next boundary.
pub(super) fn resume_point(writes: &[OwnedWrite], done: usize) -> usize {
    if done == 0 || !mutated(Mutation::WrongResume) {
        return done;
    }
    writes
        .iter()
        .scan(0, |end, write| {
            *end += write.bytes.len();
            Some(*end)
        })
        .find(|&end| end >= done)
        .unwrap_or(done)
}

/// Applies the next queued fault to one raw attempt.
pub(super) fn attempt(
    segments: &[libc::iovec],
    raw: impl FnOnce(&[libc::iovec]) -> io::Result<usize>,
) -> io::Result<usize> {
    match FAULTS.with_borrow_mut(VecDeque::pop_front) {
        None | Some(Fault::Pass) => raw(segments),
        Some(Fault::Interrupt) => Err(io::Error::from_raw_os_error(libc::EINTR)),
        Some(Fault::Zero) => Ok(0),
        Some(Fault::Short(limit)) => {
            let mut left = limit;
            let truncated: Vec<libc::iovec> = segments
                .iter()
                .map_while(|segment| {
                    (left > 0).then(|| {
                        let length = segment.iov_len.min(left);
                        left -= length;
                        libc::iovec {
                            iov_base: segment.iov_base,
                            iov_len: length,
                        }
                    })
                })
                .collect();
            if truncated.is_empty() {
                Ok(0)
            } else {
                raw(&truncated)
            }
        }
    }
}
