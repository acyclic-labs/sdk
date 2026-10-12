use super::*;

pub(crate) fn is_uncertain_io_error(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::is_uncertain_completion(error)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        false
    }
}

/// Required durability boundary for one file operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Flush file contents and metadata to stable storage.
    Full,
    /// Order earlier writes before later writes without forcing the device cache to drain.
    Barrier,
}

/// Flushes file contents and metadata according to `durability`.
pub fn sync_file(file: &File, durability: Durability, meter: Option<&dyn NativeReadMeter>) -> io::Result<()> {
    if let Some(meter) = meter {
        #[cfg(not(target_vendor = "apple"))]
        if durability == Durability::Barrier {
            return barrier_sync(file);
        }
        loop {
            meter.check()?;
            meter.admit_durability()?;
            #[cfg(target_os = "linux")]
            let result = linux::sync_once(file);
            #[cfg(target_vendor = "apple")]
            let result = apple::sync_once(file, durability);
            #[cfg(windows)]
            let result = windows::sync_once(file);
            #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
            let result = sync_file_unsequenced(file, durability);
            match result {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return result.and_then(|()| meter.check()),
            }
        }
    }
    let operation = obs::span!(DEBUG, "acyclic.runtime.sync_file", outcome = obs::Empty, error.kind = obs::Empty);
    operation.in_scope(|| operation.finish(with_file_admission(file, || sync_file_unsequenced(file, durability))))
}

pub(crate) fn sync_file_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(DEBUG, "acyclic.runtime.sync_data", outcome = obs::Empty, error.kind = obs::Empty);
    operation.in_scope(|| {
        operation.finish(with_file_admission(file, || match durability {
            Durability::Full => file.sync_data(),
            Durability::Barrier => barrier_sync(file),
        }))
    })
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(DEBUG, "acyclic.runtime.sync_parent", outcome = obs::Empty, error.kind = obs::Empty);
    operation.in_scope(|| operation.finish(sync_parent_impl(path, durability)))
}

/// What recovery found after the last valid frame of an append-only log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogTail {
    /// Nothing valid followed the invalid frame, so it was a torn append; the log now ends
    Torn,
    /// A valid frame follows the invalid one, so a committed frame was damaged. The log is
    Corrupt,
}

impl LogTail {
    const fn name(self) -> &'static str {
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
                    window.drain(..usize::try_from(offset - window_start).map_err(io::Error::other)?);
                    window_start = offset;
                    let wanted = u64::try_from(frame_bytes.saturating_mul(2)).unwrap_or(u64::MAX);
                    let fill_end = end.min(offset.saturating_add(wanted));
                    let mut buffer = vec![0; usize::try_from(fill_end - read_to).map_err(io::Error::other)?];
                    file.read_exact(&mut buffer)?;
                    window.extend_from_slice(&buffer);
                    read_to = fill_end;
                }
                let skip = usize::try_from(offset - window_start).map_err(io::Error::other)?;
                let candidate = window.get(skip..).unwrap_or_default();
                let candidate = candidate.get(..candidate.len().min(frame_bytes)).unwrap_or_default();
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

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code, reason = "F_BARRIERFSYNC is reachable only through fcntl")]
pub(crate) fn barrier_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: `F_BARRIERFSYNC` acts only on the live descriptor.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_vendor = "apple"))]
pub(crate) fn barrier_sync(_file: &File) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "ordered write barrier is unavailable on this platform"))
}

#[cfg(unix)]
pub(crate) fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    sync_file(&File::open(path)?, durability, None)
}

#[cfg(windows)]
pub(crate) fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ_WRITE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    sync_file(&directory, durability, None)
}
