use super::*;

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    with_file_admission(file, || read_at_impl(file, offset, destination))
}

/// Writes positionally; a metered held handle must support positional I/O and own its file admission.
pub fn write_all_at(
    file: &File,
    offset: u64,
    bytes: &[u8],
    meter: Option<(&dyn NativeReadMeter, bool)>,
) -> io::Result<()> {
    let Some((meter, held)) = meter else {
        return with_file_admission(file, || write_all_at_impl(file, offset, bytes));
    };
    offset.checked_add(bytes.len() as u64).ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(windows)]
    if !held {
        return windows::with_overlapped(file, 0x0012_0116, false, Some(meter), |file| {
            write_bounded_in_place(file, offset, bytes, meter)
        });
    }
    #[cfg(not(windows))]
    let _ = held;
    write_bounded_in_place(file, offset, bytes, meter)
}

/// Completes one device request on a held handle, optionally admitting its operation kind.
#[cfg(windows)]
pub fn device_control_in_place(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    meter: Option<(&dyn NativeReadMeter, NativeOperationKind)>,
) -> io::Result<usize> {
    if let Some((meter, _)) = meter {
        meter.check()?;
    }
    windows::control_in_place(file, code, input, output, meter)
}

/// One owned positional write whose buffer remains live through completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWrite {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Bytes written completely or reported as an error.
    pub bytes: Bytes,
}

/// One owned positional read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRead {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Maximum bytes returned for this request.
    pub length: usize,
}

/// Submits an owned read batch to the shared native completion worker.
pub fn read_batch_async(file: File, reads: Vec<OwnedRead>) -> NativeCompletion<Vec<Bytes>> {
    let file = Arc::new(NativeFilePayload { file, _allocation: None });
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file, None) {
            Ok(health) => NativeCompletion::<Vec<Bytes>>::submit(
                file,
                reads,
                Some(health),
                obs::file_span!(TRACE, "acyclic.runtime.read_batch"),
                #[cfg(windows)]
                false,
            ),
            Err(error) => NativeCompletion::<Vec<Bytes>>::ready(Err(error)),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    NativeCompletion::<Vec<Bytes>>::submit(file, reads, None, obs::file_span!(TRACE, "acyclic.runtime.read_batch"))
}

/// Bounded asynchronous positional source for one range of a native file,
pub struct AsyncRangeReader {
    pub(crate) file: Arc<NativeFile>,
    pub(crate) offset: u64,
    pub(crate) remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes of `file` starting at `offset`.
    #[must_use]
    pub const fn new(file: Arc<NativeFile>, offset: u64, length: u64) -> Self {
        Self { file, offset, remaining: length }
    }

    /// Bytes not yet returned from this original bounded range.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Reads the next bounded chunk, optionally requiring its full declared length.
    pub async fn read(
        &mut self,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<NativeReadResult> {
        meter.check()?;
        let length = usize::try_from(self.remaining.min(maximum as u64))
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        if length == 0 {
            return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
        }
        let result = self.file.read_bounded_async(self.offset, length, meter, require_full)?.await?;
        self.offset = self
            .offset
            .checked_add(result.bytes.len() as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.remaining -= result.bytes.len() as u64;
        Ok(result)
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) fn write_all_batch_owned(file: &Arc<NativeFilePayload>, writes: Vec<OwnedWrite>) -> io::Result<()> {
    validate_write_batch(&writes)?;
    write_all_batch_impl(file, writes)
}

pub(crate) fn validate_write_batch(writes: &[OwnedWrite]) -> io::Result<()> {
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(writes.len())?;
    for write in writes {
        if write.offset == u64::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch write offset aliases the shared file cursor",
            ));
        }
        let length = u64::try_from(write.bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "batch write length is invalid"))?;
        let end = write
            .offset
            .checked_add(length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "batch write range overflows"))?;
        if !write.bytes.is_empty() {
            ranges.push((write.offset, end));
        }
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| matches!(pair, [first, second] if first.1 > second.0)) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "overlapping batch write ranges"));
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
pub(crate) fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: the held file and writable destination live through this syscall.
    let result = unsafe { libc::pread(file.as_raw_fd(), destination.as_mut_ptr().cast(), destination.len(), offset) };
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(result as usize) }
}

#[cfg(all(unix, not(target_os = "linux"), not(target_vendor = "apple")))]
pub(crate) fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt as _;
    file.read_at(destination, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.write_all_at(bytes, offset)
}
