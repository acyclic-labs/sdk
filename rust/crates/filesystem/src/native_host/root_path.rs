use super::*;

pub(super) fn held_root_identity(directory: &Dir) -> io::Result<NativeRootIdentity> {
    #[cfg(unix)]
    {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: live held descriptor and correctly sized output.
        if unsafe { libc::fstat(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "host root is not a real directory"));
        }
        Ok(crate::NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        };
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        unsafe { GetFileInformationByHandle(HANDLE(directory.as_handle().as_raw_handle()), &raw mut information) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        if information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0
            || information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "host root is not a real directory"));
        }
        Ok(crate::NativeRootIdentity { device: u64::from(information.dwVolumeSerialNumber),
            object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow) })
    }
}

#[cfg(windows)]
pub(super) fn open_windows_root_name(
    name: &[u16], access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_SHARE_DELETE,
        OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };
    // SAFETY: caller supplies a terminated name, handle returned is newly owned.
    let handle = unsafe { CreateFileW(windows::core::PCWSTR(name.as_ptr()), access.0,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, None, OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, None) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

#[cfg(windows)]
pub(super) fn with_normalized_windows_root_name<W: NativeHostWork, T>(
    name: &[u16], force_absolute: bool, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use windows::Win32::Storage::FileSystem::GetFullPathNameW;
    const VERBATIM: &[u16] = &[92, 92, 63, 92];
    const NT: &[u16] = &[92, 63, 63, 92];
    const UNC: &[u16] = &[92, 92, 63, 92, 85, 78, 67, 92];
    const MAX_UNITS: usize = 32768;
    let short_absolute = name.len() < 248 && matches!(name,
        [drive, 58, 0] | [drive, 58, 92 | 47, ..] if *drive != 92 && *drive != 47)
        || name.len() < 248 && matches!(name, [92 | 47, 92 | 47, ..]);
    if name.starts_with(VERBATIM) || name.starts_with(NT)
        || !force_absolute && (short_absolute || name == [0]) {
        return operation(name, work);
    }
    let input = if name == [0] { &[46u16, 0][..] } else { name };
    let step = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    let mut requested = work.attempt_io( step, || {
        let required = unsafe { GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()), None, None) };
        if required == 0 { return Err(io::Error::last_os_error()); }
        Ok(required as usize)
    })?;
    let mut operation = Some(operation);
    loop {
        if requested > MAX_UNITS { return Err(io::Error::from_raw_os_error(206).into()); }
        let capacity = requested.checked_add(8).ok_or(crate::WorkError::Overflow)?;
        let mut allocation = work.claim(capacity as u64 * 2)?;
        let mut buffer = Vec::new();
        let result = (|| {
            let reserved = buffer.try_reserve_exact(capacity);
            let reconciled = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64 * 2);
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            buffer.resize(capacity, 0u16);
            let returned = work.attempt_io( step, || {
                let count = unsafe { GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()),
                    Some(&mut buffer[8..]), None) };
                if count == 0 { return Err(io::Error::last_os_error()); }
                Ok(count as usize)
            })?;
            if returned >= requested { return Ok(Err(returned)); }
            let absolute = &buffer[8..8 + returned];
            let (prefix, skip) = match absolute {
                [_, 58, 92, ..] => (VERBATIM, 0),
                [92, 92, 46, 92, ..] => (VERBATIM, 4),
                [92, 92, 63, 92, ..] | [92, 63, 63, 92, ..] => (&[][..], 0),
                [92, 92, ..] => (UNC, 2),
                _ => (&[][..], 0),
            };
            let begin = 8 + skip - prefix.len();
            let copied = WorkCounters { bytes_copied: prefix.len() as u64 * 2, ..WorkCounters::UNCHARGED };
            work.admit(copied)?;
            buffer[begin..begin + prefix.len()].copy_from_slice(prefix);
            work.record(copied)?;
            operation.take().expect("normalization callback runs only after a complete name")(&buffer[begin..8 + returned + 1], work).map(Ok)
        })();
        drop(buffer);
        work.release(allocation)?;
        match result? {
            Ok(value) => return Ok(value),
            Err(required) if required > MAX_UNITS || requested == MAX_UNITS => return Err(io::Error::from_raw_os_error(206).into()),
            Err(required) => requested = requested.saturating_mul(2).max(required).min(MAX_UNITS),
        }
    }
}

#[cfg(windows)]
pub(super) fn with_root_path_name<W: NativeHostWork, T>(
    path: &Path, force_absolute: bool, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let (name, allocation) = encode_root_path_name(path, work)?;
    let result = with_normalized_windows_root_name(&name, force_absolute, work, operation);
    drop(name);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
pub(crate) fn encode_root_path_name<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let units = wide_units_with(path.as_os_str(), || {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)
    })?.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let encoded = WorkCounters { bytes_encoded: (bytes - 2) as u64, ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut name = Vec::new();
    let result = (|| {
        let reserved = name.try_reserve_exact(units);
        let reconciled = work.reconcile_allocation(&mut allocation, name.capacity() as u64 * 2);
        reserved.map_err(W::allocation_failed).and(reconciled)?;
        name.extend(path.as_os_str().encode_wide());
        work.record(encoded)?;
        if name.contains(&0) { return Err(io::Error::new(io::ErrorKind::InvalidInput, "host path contains NUL").into()); }
        name.push(0);
        Ok(())
    })();
    if let Err(error) = result {
        drop(name);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((name, allocation))
}

#[cfg(windows)]
pub(crate) fn normalize_root_wide<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let (name, live_bytes) = encode_root_path_name(path, work)?;
    let short_absolute = path.is_absolute() && name.len() < 248;
    if short_absolute || name.starts_with(&[92, 92, 63, 92]) || name.starts_with(&[92, 63, 63, 92]) {
        return Ok((name, live_bytes));
    }
    let result = normalize_root_wide_name(&name, path.is_absolute(), work);
    drop(name);
    if let Err(error) = work.release(live_bytes) {
        if let Ok((output, allocation)) = result {
            drop(output);
            work.release(allocation)?;
        }
        return Err(error);
    }
    result
}
#[cfg(windows)]
pub(crate) fn normalize_root_wide_name<W: NativeHostWork>(
    name: &[u16], absolute: bool, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let mut owned = None;
    let result = with_normalized_windows_root_name(name, !absolute, work, |name, work| {
        let bytes = name.len() as u64 * 2;
        let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let mut allocation = work.claim(bytes)?;
        let mut output = Vec::new();
        let copied_result = (|| {
            let reserved = output.try_reserve_exact(name.len());
            let reconciled = work.reconcile_allocation(&mut allocation, output.capacity() as u64 * 2);
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            output.extend_from_slice(name);
            work.record(copied)
        })();
        if let Err(error) = copied_result {
            drop(output);
            work.release(allocation)?;
            return Err(error);
        }
        owned = Some((output, allocation));
        Ok(())
    });
    if let Err(error) = result {
        if let Some((output, allocation)) = owned {
            drop(output);
            work.release(allocation)?;
        }
        return Err(error);
    }
    Ok(owned.expect("normalization callback owns the completed native name"))
}

#[cfg(windows)]
pub(super) fn check_root_wide_name<W: NativeHostWork>(name: &[u16], work: &mut W) -> Result<(), W::Error> {
    work.check()?;
    let Some((last, body)) = name.split_last() else { return Err(W::invalid_request()); };
    if *last != 0 { return Err(W::invalid_request()); }
    for unit in body {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)?;
        if *unit == 0 { return Err(W::invalid_request()); }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn root_path_identity_wide<W: NativeHostWork>(
    name: &[u16], work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    check_root_wide_name(name, work)?;
    let file = work.attempt_io(
        WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
        || open_windows_root_name(name, windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES))?;
    let directory = Dir::from_std_file(file);
    directory_identity(&directory, work)
}
