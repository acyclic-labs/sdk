use super::*;

/// Reads one declared source prefix into caller-admitted storage without changing the cursor or allocating.
#[cfg(unix)]
pub fn read_declared_source_bytes(
    file: &std::fs::File,
    offset: u64,
    destination: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    if destination.is_empty() {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    linux::validate_io_range(offset, destination.len())?;
    #[cfg(not(target_os = "linux"))]
    {
        let length = u64::try_from(destination.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let end = offset.checked_add(length).ok_or(io::ErrorKind::InvalidInput)?;
        if offset > i64::MAX as u64 || end > i64::MAX as u64 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    let mut operation = NativeOperationReservation::new(meter, NativeOperationKind::Read, 1)?;
    let mut bytes = NativeByteReservation::new(meter, NativeOperationKind::Read, destination.len())?;
    meter.check()?;
    #[cfg(target_os = "linux")]
    let (result, recorded_operation) = operation.attempt(|| linux::read_at_validated(file, offset, destination));
    #[cfg(not(target_os = "linux"))]
    let (result, recorded_operation) = operation.attempt(|| read_at_impl(file, offset, destination));
    let recorded_bytes = bytes.record(result.as_ref().copied().unwrap_or(0));
    recorded_bytes?;
    recorded_operation?;
    let count = result?;
    meter.check()?;
    Ok(count)
}

/// An owned native read and its actual retained data-buffer capacity.
pub struct NativeReadResult {
    /// Bytes actually returned, including an ordinary EOF-short prefix.
    pub bytes: Bytes,
    /// Actual retained vector capacity, excluding reference-count headers.
    pub buffer_capacity: usize,
    pub(crate) _allocation: Option<ReadAllocation>,
}

/// Resizes an already-held, ordered handle. Each retry is separately admitted.
pub fn set_len(file: &File, size: u64, meter: &dyn NativeReadMeter) -> io::Result<()> {
    i64::try_from(size).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    loop {
        meter.check()?;
        meter.admit_write(0)?;
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let result = {
            use std::os::fd::AsRawFd as _;
            let size = libc::off_t::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: ftruncate acts only on this live descriptor.
            #[allow(unsafe_code)]
            if unsafe { libc::ftruncate(file.as_raw_fd(), size) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        };
        #[cfg(windows)]
        let result = windows::set_len_once(file, size);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result = file.set_len(size);
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result.and_then(|()| meter.check()),
        }
    }
}

pub(crate) fn write_bounded_in_place(
    file: &File,
    mut offset: u64,
    mut bytes: &[u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    while !bytes.is_empty() {
        meter.check()?;
        // The same native worker owns each request through terminal completion.
        let length = bytes.len().min(1024 * 1024);
        let chunk = &bytes[..length];
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let result = (|| {
            use std::os::fd::AsRawFd as _;
            validate_prepared_read_range(offset, length)?;
            let mut transfer = NativeByteReservation::new(meter, NativeOperationKind::Write, length)?;
            meter.admit_write(0)?;
            // SAFETY: the validated range and borrowed payload remain live through pwrite.
            #[allow(unsafe_code)]
            let count = unsafe { libc::pwrite(file.as_raw_fd(), chunk.as_ptr().cast(), length, offset as libc::off_t) };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            transfer.record(count as usize)?;
            Ok(count as usize)
        })();
        #[cfg(windows)]
        let result = windows::write_at_known_overlapped(file, offset, chunk, meter);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result: io::Result<usize> = Err(io::Error::from(io::ErrorKind::Unsupported));
        let count = match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count > length {
            std::process::abort();
        }
        meter.check()?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        offset += count as u64;
        bytes = &bytes[count..];
    }
    Ok(())
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
        return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
    }
    let read = |file: &File| {
        let mut allocation = ReadAllocation::new(meter, length)?;
        let mut data = Vec::new();
        data.try_reserve_exact(length).map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        data.resize(length, 0);
        let filled = read_bounded_slice(file, offset, &mut data, require_full, meter.as_ref())?;
        if filled == 0 {
            return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
        }
        let buffer_capacity = data.capacity();
        if filled != buffer_capacity {
            meter.allocate_refcount_header()?;
        }
        data.truncate(filled);
        Ok(NativeReadResult { bytes: Bytes::from(data), buffer_capacity, _allocation: Some(allocation) })
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
    let end = offset.checked_add(length as u64).ok_or(io::ErrorKind::InvalidInput)?;
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
        let result = windows::read_at_known_overlapped(file, offset + filled as u64, destination, meter);
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
