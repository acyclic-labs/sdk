use crate::*;

/// An owned native read and its actual retained data-buffer capacity.
pub struct NativeReadResult {
    /// Bytes actually returned, including an ordinary EOF-short prefix.
    pub bytes: Bytes,
    /// Actual retained vector capacity, excluding reference-count headers.
    pub buffer_capacity: usize,
    pub(crate) _allocation: Option<ReadAllocation>,
}

pub(crate) fn read_bounded_in_place(
    file: &File,
    offset: u64,
    length: usize,
    require_full: bool,
    meter: &Arc<dyn NativeReadMeter>,
    #[cfg(windows)] known_overlapped: bool,
) -> io::Result<NativeReadResult> {
    meter.check()?;
    meter.admit_bytes(length)?;
    if length == 0 {
        return Ok(NativeReadResult {
            bytes: Bytes::new(),
            buffer_capacity: 0,
            _allocation: None,
        });
    }
    let read = |file: &File| {
        let mut allocation = ReadAllocation::new(meter, length)?;
        let mut data = Vec::new();
        data.try_reserve_exact(length)
            .map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        data.resize(length, 0);
        let filled = read_bounded_slice(file, offset, &mut data, require_full, meter.as_ref())?;
        if filled == 0 {
            return Ok(NativeReadResult {
                bytes: Bytes::new(),
                buffer_capacity: 0,
                _allocation: None,
            });
        }
        let buffer_capacity = data.capacity();
        if filled != buffer_capacity {
            meter.allocate_refcount_header()?;
        }
        data.truncate(filled);
        Ok(NativeReadResult {
            bytes: Bytes::from(data),
            buffer_capacity,
            _allocation: Some(allocation),
        })
    };
    #[cfg(windows)]
    {
        windows::with_overlapped(
            file,
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            known_overlapped,
            Some(meter.as_ref()),
            read,
        )
    }
    #[cfg(not(windows))]
    read(file)
}

pub(crate) fn validate_prepared_read_range(offset: u64, length: usize) -> io::Result<()> {
    let end = offset
        .checked_add(length as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(windows)]
    if length != 0 && (length > u32::MAX as usize || offset == u64::MAX) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(unix)]
    if length != 0 && (offset > i64::MAX as u64 || end > i64::MAX as u64) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(not(unix))]
    let _ = end;
    Ok(())
}

pub(crate) fn read_prepared_in_place(
    file: &File,
    offset: u64,
    destination: &mut [u8],
    require_full: bool,
    meter: &dyn NativeReadMeter,
    #[cfg(windows)] known_overlapped: bool,
) -> io::Result<usize> {
    meter.check()?;
    meter.admit_bytes(destination.len())?;
    validate_prepared_read_range(offset, destination.len())?;
    if destination.is_empty() {
        return Ok(0);
    }
    #[cfg(windows)]
    {
        windows::with_overlapped(
            file,
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            known_overlapped,
            Some(meter),
            |file| read_bounded_slice(file, offset, destination, require_full, meter),
        )
    }
    #[cfg(not(windows))]
    read_bounded_slice(file, offset, destination, require_full, meter)
}

pub(crate) fn read_bounded_slice(
    file: &File,
    offset: u64,
    data: &mut [u8],
    require_full: bool,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    let length = data.len();
    let mut filled = 0;
    while filled < length {
        meter.check()?;
        let destination = &mut data[filled..];
        #[cfg(windows)]
        let result =
            windows::read_at_known_overlapped(file, offset + filled as u64, destination, meter);
        #[cfg(unix)]
        let result = read_declared_source_bytes(file, offset + filled as u64, destination, meter);
        let count = match result {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        meter.check()?;
        if count == 0 {
            break;
        }
        filled = filled
            .checked_add(count)
            .filter(|count| *count <= length)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    }
    meter.finish_read(length, filled)?;
    if require_full && filled != length {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(filled)
}

/// Reads one bounded positional range from an owned native file.
pub struct AsyncRangeReader {
    pub(crate) file: Arc<NativeFile>,
    pub(crate) offset: u64,
    pub(crate) remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes of `file` starting at `offset`.
    #[must_use]
    pub const fn new(file: Arc<NativeFile>, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
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
            return Ok(NativeReadResult {
                bytes: Bytes::new(),
                buffer_capacity: 0,
                _allocation: None,
            });
        }
        let result = self
            .file
            .read_bounded_async(self.offset, length, meter, require_full)?
            .await?;
        self.offset = self
            .offset
            .checked_add(result.bytes.len() as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.remaining -= result.bytes.len() as u64;
        Ok(result)
    }
}
