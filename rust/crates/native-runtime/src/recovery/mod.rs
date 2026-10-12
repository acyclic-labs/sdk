use crate::*;

/// What recovery found after the last valid frame of an append-only log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogTail {
    /// Nothing valid followed the invalid frame, so it was a torn append; the log now ends
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

/// Recovers an append-only log whose frame at `invalid_start` is invalid.
pub fn recover_log_tail(
    file: &mut File,
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

        let tail = (|| {
            let end = file.metadata()?.len();
            operation.record("bytes", end.saturating_sub(invalid_start));
            let frame_bytes = maximum_frame_bytes.max(1);
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
                    let mut buffer =
                        vec![0; usize::try_from(fill_end - read_to).map_err(io::Error::other)?];
                    file.read_exact(&mut buffer)?;
                    window.extend_from_slice(&buffer);
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
            file.set_len(invalid_start)?;
            sync_file(file, durability, None)?;
            file.seek(SeekFrom::Start(invalid_start))?;
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
