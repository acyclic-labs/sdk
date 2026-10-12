use super::*;

#[cfg(windows)]
pub(crate) fn read_link_held_wide<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    maximum: u64,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let file = open_windows_link(parent, name, work)?;
    let size = maximum.saturating_mul(4).saturating_add(32).min(16 * 1024);
    work.admit(WorkCounters {
        source_bytes_read: size,
        ..WorkCounters::UNCHARGED
    })?;
    let (mut buffer, allocation) = buffer(size as usize, work)?;
    buffer.resize(size as usize, 0u8);
    let result = (|| {
        let returned = read_windows_link_record(&file, &mut buffer, work)?;
        let target = windows_link_target_range::<W>(&buffer[..returned], maximum)?;
        let target = &buffer[target];
        let length = target.len();
        let copied = copy_work(length as u64);
        work.admit(copied)?;
        let (mut wide, allocation) = super::buffer(length / 2, work)?;
        wide.extend(
            target
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]])),
        );
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
    parent: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<cap_std::fs::File, W::Error> {
    use windows::{Wdk::Storage::FileSystem as nt, Win32::Storage::FileSystem as fs};
    let file = open_windows_at(
        parent,
        name,
        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        nt::FILE_OPEN,
        nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    Ok(cap_std::fs::File::from_std(file))
}

#[cfg(windows)]
pub(super) fn read_windows_link_record<W: NativeHostWork>(
    file: &cap_std::fs::File,
    buffer: &mut [u8],
    work: &mut W,
) -> Result<usize, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;
    let mut returned = 0u32;
    work.admit(WorkCounters {
        source_bytes_read: buffer.len() as u64,
        ..WorkCounters::UNCHARGED
    })?;
    work.started_io(READ)?;
    // SAFETY: the held no-follow leaf and initialized fixed output remain live.
    let read = unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            Some(buffer.as_mut_ptr().cast()),
            buffer.len() as u32,
            Some(&raw mut returned),
            None,
        )
    };
    work.record(WorkCounters {
        source_bytes_read: u64::from(returned),
        ..WorkCounters::UNCHARGED
    })?;
    if let Err(error) = read {
        let code = error.code().0 & 0xffff;
        if code == 122 || code == 234 {
            return Err(W::invalid_request());
        }
        return Err(io::Error::from_raw_os_error(code).into());
    }
    let returned = returned as usize;
    if returned > buffer.len() {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    Ok(returned)
}

#[cfg(windows)]
pub(super) fn windows_link_target_range<W: NativeHostWork>(
    bytes: &[u8],
    maximum: u64,
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
        _ => return Err(W::invalid_request()),
    };
    let offset = usize::from(u16_at(8)?);
    let length = usize::from(u16_at(10)?);
    if !offset.is_multiple_of(2) || !length.is_multiple_of(2) {
        return Err(malformed());
    }
    let mut start = header + offset;
    let end = start + length;
    let target = bytes.get(start..end).ok_or_else(&malformed)?;
    let relative = tag == 0xA000_000C && u16_at(16)? & 1 != 0;
    if !relative && target.starts_with(&[92, 0, 63, 0, 63, 0, 92, 0]) {
        start += 8;
    }
    if (end - start) as u64 > maximum {
        return Err(W::invalid_request());
    }
    Ok(start..end)
}

#[cfg(unix)]
pub(super) fn read_unix_link<W: NativeHostWork>(
    parent: &Dir,
    name: &std::ffi::CStr,
    maximum_bytes: u64,
    work: &mut W,
) -> Result<(Vec<u8>, usize, W::Allocation), W::Error> {
    let maximum = maximum_bytes.saturating_add(1).min(isize::MAX as u64);
    let mut size = maximum.min(256);
    work.admit(WorkCounters {
        source_bytes_read: size,
        ..WorkCounters::UNCHARGED
    })?;
    loop {
        work.admit(WorkCounters {
            source_bytes_read: size,
            ..WorkCounters::UNCHARGED
        })?;
        let (mut output, allocation) = buffer(size as usize, work)?;
        let result = (|| {
            work.started_io(copy_work(output.capacity() as u64))?;
            output.resize(output.capacity(), 0);
            read_unix_link_into(parent, name, &mut output[..size as usize], work)
        })();
        if let Ok(count) = result.as_ref() {
            if *count < size as usize && *count as u64 <= maximum_bytes {
                if let Err(error) = work.check() {
                    drop(output);
                    work.release(allocation)?;
                    return Err(error);
                }
                return Ok((output, *count, allocation));
            }
        }
        drop(output);
        work.release(allocation)?;
        result?;
        if size == maximum {
            return Err(W::invalid_request());
        }
        size = size.saturating_mul(2).min(maximum);
    }
}

#[cfg(unix)]
pub(super) fn read_unix_link_into<W: NativeHostWork>(
    parent: &Dir,
    name: &std::ffi::CStr,
    buffer: &mut [u8],
    work: &mut W,
) -> Result<usize, W::Error> {
    let count = loop {
        work.admit(WorkCounters {
            source_bytes_read: buffer.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        work.started_io(READ)?;
        // SAFETY: held parent, terminated name and initialized output are live.
        let count = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if count >= 0 {
            break count as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    };
    work.record(WorkCounters {
        source_bytes_read: count as u64,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(count)
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &[u16],
        maximum: u64,
        work: &mut W,
    ) -> Result<(Vec<u16>, W::Allocation), W::Error> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        read_link_held_wide(&self.open_dir_held(parent, work)?, leaf, maximum, work)
    }
}

impl HostRoot {
    pub(crate) fn read_source_link<W: SourceDemandReadWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        maximum: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        #[cfg(unix)]
        let (parent, leaf) = (
            path.parent().ok_or(DemandError::InvalidRequest)?,
            path.file_name().ok_or(DemandError::InvalidRequest)?,
        );
        #[cfg(windows)]
        let (parent, leaf) = wide_parent_leaf(path)?;
        read_source_link_held(
            &self.open_dir_held(parent, work)?,
            #[cfg(unix)]
            Path::new(leaf),
            #[cfg(windows)]
            leaf,
            maximum,
            work,
        )
    }
}

pub(crate) fn read_source_link_held<W: SourceDemandReadWork>(
    parent: &Dir,
    #[cfg(unix)] name: &Path,
    #[cfg(windows)] name: &[u16],
    maximum: u64,
    work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    let original = work
        .native_source_work()
        .ok_or(DemandError::NativeWorkUnavailable)?;
    NativeHostWork::check(&original)?;
    let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
    #[cfg(unix)]
    require_leaf(name)?;
    #[cfg(unix)]
    let (bytes, count, allocation) = unix_name(name.as_os_str(), work, |name, work| {
        read_unix_link(parent, name, maximum, work)
    })?;
    #[cfg(unix)]
    let target = 0..count;
    #[cfg(windows)]
    let (bytes, allocation, target) = {
        let file = open_windows_link(parent, name, work)?;
        let size = maximum.saturating_mul(4).saturating_add(32).min(16 * 1024) as usize;
        work.admit(WorkCounters {
            source_bytes_read: size as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let (mut bytes, allocation) = buffer(size, work)?;
        let result = (|| {
            work.started_io(copy_work(allocation))?;
            bytes.resize(bytes.capacity(), 0);
            let count = read_windows_link_record(&file, &mut bytes[..size], work)?;
            windows_link_target_range::<W>(&bytes[..count], maximum)
        })();
        match result {
            Ok(target) => (bytes, allocation, target),
            Err(error) => {
                drop(bytes);
                work.release(allocation)?;
                return Err(error);
            }
        }
    };
    if let Err(error) = ownership.retain(allocation, work) {
        drop(bytes);
        work.release(allocation)?;
        return Err(error);
    }
    let prepared =
        acyclic_stream::PreparedOwnedBytes::new(bytes, ownership, original.runtime_meter())
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    crate::native_name::original_name_error(original.runtime_error(error))
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    DemandError::InvalidRequest
                }
            })?;
    work.check()?;
    let bytes = prepared.freeze(target.end).map_err(|failed| {
        drop(failed);
        DemandError::InvalidRequest
    })?;
    Ok(bytes.slice(target))
}
