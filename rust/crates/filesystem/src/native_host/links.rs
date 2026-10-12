use super::*;

#[cfg(windows)]
pub(crate) fn read_link_held_wide<W: NativeHostWork>(
    parent: &Dir, name: &[u16], maximum: u64, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    read_windows_link_wide(parent, name, maximum, work, || {
        io::Error::new(io::ErrorKind::InvalidInput, "native link exceeds its bounded buffer").into()
    })
}

#[cfg(windows)]
pub(crate) fn read_link_held<W: NativeHostWork>(
    parent: &Dir, name: &Path, maximum: u64, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    require_leaf(name)?;
    with_wide_path(name, work, |name, work| read_link_held_wide(parent, name, maximum, work))
}

#[cfg(windows)]
pub(super) fn read_windows_link_wide<W: NativeHostWork>(
    parent: &Dir, name: &[u16], maximum: u64, work: &mut W,
    invalid_request: impl Fn() -> W::Error,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let file = open_windows_link(parent, name, work, &invalid_request)?;
    let size = maximum.saturating_mul(4).saturating_add(32).min(16 * 1024);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    let allocation = work.claim(size)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(size as usize) {
        work.release(allocation)?;
        return Err(W::allocation_failed(error));
    }
    buffer.resize(size as usize, 0u8);
    let result = (|| {
        let returned = read_windows_link_record(&file, &mut buffer, work, &invalid_request)?;
        let target = windows_link_target_range::<W>(&buffer[..returned], maximum, &invalid_request)?;
        let target = &buffer[target];
        let length = target.len();
        let copied = WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let allocation = work.claim(length as u64)?;
        let mut wide = Vec::new();
        if let Err(error) = wide.try_reserve_exact(length / 2) {
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        wide.extend(target.chunks_exact(2).map(|pair| u16::from_le_bytes([pair[0], pair[1]])));
        if let Err(error) = work.record(copied).and_then(|()| work.check()) {
            drop(wide);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((wide, allocation))
    })();
    drop(buffer);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
pub(super) fn open_windows_link<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W, invalid_request: impl Fn() -> W::Error,
) -> Result<cap_std::fs::File, W::Error> {
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    open_windows_wide_at(parent, name, FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work, &invalid_request)?
        .ok_or_else(invalid_request)
}

#[cfg(windows)]
pub(super) fn read_windows_link_record<W: NativeHostWork>(
    file: &cap_std::fs::File, buffer: &mut [u8], work: &mut W, invalid_request: impl Fn() -> W::Error,
) -> Result<usize, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;
    let mut returned = 0u32;
    work.admit(WorkCounters { source_bytes_read: buffer.len() as u64, ..WorkCounters::UNCHARGED })?;
    native_read(work)?;
    // SAFETY: the held no-follow leaf and initialized fixed output remain live.
    let read = unsafe { DeviceIoControl(HANDLE(file.as_raw_handle()), FSCTL_GET_REPARSE_POINT,
        None, 0, Some(buffer.as_mut_ptr().cast()), buffer.len() as u32, Some(&raw mut returned), None) };
    work.record(WorkCounters { source_bytes_read: u64::from(returned), ..WorkCounters::UNCHARGED })?;
    if let Err(error) = read {
        let code = error.code().0 & 0xffff;
        if code == 122 || code == 234 { return Err(invalid_request()); }
        return Err(io::Error::from_raw_os_error(code).into());
    }
    let returned = returned as usize;
    if returned > buffer.len() { return Err(io::Error::from(io::ErrorKind::InvalidData).into()); }
    Ok(returned)
}

#[cfg(windows)]
pub(super) fn windows_link_target_range<W: NativeHostWork>(
    bytes: &[u8], maximum: u64, invalid_request: impl Fn() -> W::Error,
) -> Result<std::ops::Range<usize>, W::Error> {
    let malformed = || W::Error::from(io::Error::from(io::ErrorKind::InvalidData));
    let u16_at = |offset| -> Result<u16, W::Error> {
        let data = bytes.get(offset..offset + 2).ok_or_else(&malformed)?;
        Ok(u16::from_le_bytes([data[0], data[1]]))
    };
    let data = bytes.get(..4).ok_or_else(&malformed)?;
    let tag = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let header = match tag {
        0xA000_000C => 20,
        0xA000_0003 => 16,
        _ => return Err(invalid_request()),
    };
    let offset = usize::from(u16_at(8)?);
    let length = usize::from(u16_at(10)?);
    if !offset.is_multiple_of(2) || !length.is_multiple_of(2) { return Err(malformed()); }
    let mut start = header + offset;
    let end = start + length;
    let target = bytes.get(start..end).ok_or_else(&malformed)?;
    let relative = tag == 0xA000_000C && u16_at(16)? & 1 != 0;
    if !relative && target.starts_with(&[92, 0, 63, 0, 63, 0, 92, 0]) { start += 8; }
    if (end - start) as u64 > maximum { return Err(invalid_request()); }
    Ok(start..end)
}

pub(super) fn prepare_source_link_buffer<W: crate::demand::SourceDemandReadWork>(
    size: u64, work: &mut W,
) -> Result<acyclic_stream::PreparedOwnedBytes<crate::demand::SourcePayloadOwnership>, DemandError> {
    let original = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?
        .materialization_work()?;
    let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
    let mut allocation = work.claim(size)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(size as usize) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(W::allocation_failed(error));
    }
    if let Err(error) = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(error);
    }
    if let Err(error) = ownership.retain(allocation, work) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(error);
    }
    let copied = original.reserve_work(crate::WorkCounters { bytes_copied: allocation, ..crate::WorkCounters::UNCHARGED }).map_err(crate::native_name::original_name_error)?;
    buffer.resize(buffer.capacity(), 0);
    { let mut copy = copied; copy.record_completed(crate::WorkCounters { bytes_copied: allocation, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)?;
    acyclic_stream::PreparedOwnedBytes::new(buffer, ownership, original.runtime_meter())
        .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                crate::native_name::original_name_error(original.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            => DemandError::InvalidRequest,
        })
}

#[cfg(windows)]
pub(crate) fn read_source_link_held<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &Path, maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    require_leaf(name)?;
    with_wide_path(name, work, |name, work| {
        read_source_link_held_wide(parent, name, maximum_bytes, work)
    })
}

#[cfg(windows)]
pub(super) fn read_source_link_held_wide<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &[u16], maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    let file = open_windows_link(parent, name, work, W::invalid_request)?;
    let size = maximum_bytes.saturating_mul(4).saturating_add(32).min(16 * 1024);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    let mut prepared = prepare_source_link_buffer(size, work)?;
    let returned = read_windows_link_record(
        &file, &mut prepared.writable()[..size as usize], work, W::invalid_request,
    )?;
    let target = windows_link_target_range::<W>(
        &prepared.writable()[..returned], maximum_bytes, W::invalid_request,
    )?;
    work.check()?;
    let bytes = prepared.freeze(returned).map_err(|failed| {
        drop(failed);
        DemandError::InvalidRequest
    })?;
    Ok(bytes.slice(target))
}

#[cfg(unix)]
pub(crate) fn read_source_link_held<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &Path, maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    require_leaf(name)?;
    let maximum = maximum_bytes.saturating_add(1).min(isize::MAX as u64);
    let mut size = maximum.min(256);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    unix_name(name.as_os_str(), work, |name, work| loop {
        work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
        let mut prepared = prepare_source_link_buffer(size, work)?;
        let count = read_unix_link_into(
            parent, name, &mut prepared.writable()[..size as usize], work,
        )?;
        if count < size as usize && count as u64 <= maximum_bytes {
            work.check()?;
            return prepared.freeze(count).map_err(|failed| {
                drop(failed);
                DemandError::InvalidRequest
            });
        }
        drop(prepared);
        if size == maximum { return Err(DemandError::InvalidRequest); }
        size = size.saturating_mul(2).min(maximum);
    })
}

#[cfg(unix)]
pub(crate) fn read_link_held<W: NativeHostWork>(
    parent: &Dir, name: &Path, maximum: u64, work: &mut W,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
    require_leaf(name)?;
    read_unix_link(parent, name.as_os_str(), maximum, work, || {
        io::Error::new(io::ErrorKind::InvalidInput, "native link exceeds its bounded buffer").into()
    })
}

#[cfg(unix)]
pub(super) fn read_unix_link<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, maximum_bytes: u64, work: &mut W,
    invalid_request: impl Fn() -> W::Error,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
    let maximum = maximum_bytes.saturating_add(1).min(isize::MAX as u64);
    let mut size = maximum.min(256);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    let (buffer, buffer_allocation, count) = unix_name(name, work, |name, work| loop {
        work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
        let allocation = work.claim(size)?;
        let mut buffer = Vec::new();
        if let Err(error) = buffer.try_reserve_exact(size as usize) {
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        buffer.resize(size as usize, 0u8);
        let result = read_unix_link_into(parent, name, &mut buffer, work);
        let count = match result {
            Ok(count) => count,
            Err(error) => {
                drop(buffer);
                work.release(allocation)?;
                return Err(error);
            }
        };
        if count < buffer.len() && count as u64 <= maximum_bytes {
            return Ok((buffer, allocation, count));
        }
        drop(buffer);
        work.release(allocation)?;
        if size == maximum { return Err(invalid_request()); }
        size = size.saturating_mul(2).min(maximum);
    })?;
    let result = (|| {
        let copied = WorkCounters { bytes_copied: count as u64, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let allocation = work.claim(count as u64)?;
        let mut exact = Vec::new();
        if let Err(error) = exact.try_reserve_exact(count) {
            work.release(allocation)?;
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, error).into());
        }
        exact.extend_from_slice(&buffer[..count]);
        if let Err(error) = work.record(copied).and_then(|()| work.check()) {
            drop(exact);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((exact, allocation))
    })();
    drop(buffer);
    work.release(buffer_allocation)?;
    result
}

#[cfg(unix)]
pub(super) fn read_unix_link_into<W: NativeHostWork>(
    parent: &Dir, name: &std::ffi::CStr, buffer: &mut [u8], work: &mut W,
) -> Result<usize, W::Error> {
    let count = loop {
        work.admit(WorkCounters { source_bytes_read: buffer.len() as u64, ..WorkCounters::UNCHARGED })?;
        native_read(work)?;
        // SAFETY: held parent, terminated name and initialized output are live.
        let count = unsafe { libc::readlinkat(parent.as_raw_fd(), name.as_ptr(),
            buffer.as_mut_ptr().cast(), buffer.len()) };
        if count >= 0 { break count as usize; }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
    };
    work.record(WorkCounters { source_bytes_read: count as u64, ..WorkCounters::UNCHARGED })?;
    Ok(count)
}
