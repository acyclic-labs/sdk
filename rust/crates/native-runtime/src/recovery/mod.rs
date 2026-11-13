use crate::*;

mod frame;
pub use frame::LogFrame;

/// What recovery found after the last valid frame of an append-only log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogTail {
    /// Nothing valid followed the invalid frame, so it was a torn append; the
    /// log now ends at the caller's retained prefix.
    Torn,
    /// A valid frame follows the invalid one, so a committed frame was damaged. The log is
    Corrupt,
}

impl LogTail {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Torn => "torn",
            Self::Corrupt => "corrupt",
        }
    }
}

/// Recovers an exclusively owned append-only log whose frame at
/// `invalid_start` is invalid or missing. `retained_end` is the end of the last
/// complete logical operation: intact fragments after it are still uncommitted.
/// A torn tail is truncated once to that prefix and synchronized; a later valid
/// frame proves corruption and prevents truncation.
///
/// Returns `InvalidInput` without editing the file unless
/// `retained_end <= invalid_start <= file length` and the frame bound is positive.
pub fn recover_log_tail(
    file: &File,
    retained_end: u64,
    invalid_start: u64,
    maximum_frame_bytes: usize,
    durability: Durability,
    mut valid_frame: impl FnMut(&[u8]) -> bool,
) -> io::Result<LogTail> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.recover_log_tail",
        bytes = obs::Empty,
        tail = obs::Empty,
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| {
        use std::io::{Read as _, Seek as _, SeekFrom};

        let mut file = file;
        let tail = (|| {
            let end = file.metadata()?.len();
            if retained_end > invalid_start || invalid_start > end || maximum_frame_bytes == 0 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            operation.record("bytes", end.saturating_sub(retained_end));
            let frame_bytes = maximum_frame_bytes;
            let first = invalid_start.saturating_add(1);
            let mut window = Vec::new();
            let mut window_start = first;
            let mut read_to = first;
            file.seek(SeekFrom::Start(first))?;
            for offset in first..end {
                let ahead = usize::try_from(read_to - offset).unwrap_or(usize::MAX);
                if ahead < frame_bytes && read_to < end {
                    window
                        .drain(..usize::try_from(offset - window_start).map_err(io::Error::other)?);
                    window_start = offset;
                    let wanted = u64::try_from(frame_bytes.saturating_mul(2)).unwrap_or(u64::MAX);
                    let fill_end = end.min(offset.saturating_add(wanted));
                    let buffered = window.len();
                    window.resize(
                        usize::try_from(fill_end - window_start).map_err(io::Error::other)?,
                        0,
                    );
                    file.read_exact(&mut window[buffered..])?;
                    read_to = fill_end;
                }
                let skip = usize::try_from(offset - window_start).map_err(io::Error::other)?;
                let candidate = window.get(skip..).unwrap_or_default();
                let candidate = candidate
                    .get(..candidate.len().min(frame_bytes))
                    .unwrap_or_default();
                if valid_frame(candidate) {
                    return Ok(LogTail::Corrupt);
                }
            }
            file.set_len(retained_end)?;
            sync_file(file, durability, None)?;
            file.seek(SeekFrom::Start(retained_end))?;
            Ok(LogTail::Torn)
        })();
        if let Ok(tail) = &tail {
            operation.record("tail", tail.name());
        }
        operation.finish(tail)
    })
}

/// Whether an exclusive file-lock failure proves that another owner holds the lock.
#[must_use]
pub fn is_exclusive_lock_contention(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || cfg!(windows) && error.raw_os_error() == Some(33)
}

#[cfg(test)]
mod tests;
