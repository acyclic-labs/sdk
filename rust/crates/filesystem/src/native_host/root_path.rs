use super::*;

#[cfg(windows)]
pub(super) fn open_windows_root_name(
    name: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    // SAFETY: callers validate the terminated name; a successful call transfers a fresh handle.
    let handle = unsafe {
        CreateFileW(
            windows::core::PCWSTR(name.as_ptr()),
            access.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
    }
    .map_err(|e| io::Error::from_raw_os_error(e.code().0 & 0xffff))?;
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

#[cfg(windows)]
pub(crate) fn encode_root_path_name<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let units = wide_units(path.as_os_str(), work)?
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let encoded = WorkCounters {
        bytes_encoded: (units - 1)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let (mut name, allocation) = buffer(units, work)?;
    name.extend(path.as_os_str().encode_wide());
    let result = work.record(encoded).and_then(|()| {
        if name.contains(&0) {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "host path contains NUL").into(),
            );
        }
        name.push(0);
        Ok(())
    });
    if let Err(e) = result {
        drop(name);
        work.release(allocation)?;
        return Err(e);
    }
    Ok((name, allocation))
}

#[cfg(windows)]
pub(crate) fn normalize_root_wide_name<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use windows::Win32::Storage::FileSystem::GetFullPathNameW;
    const VERBATIM: &[u16] = &[92, 92, 63, 92];
    const NT: &[u16] = &[92, 63, 63, 92];
    const UNC: &[u16] = &[92, 92, 63, 92, 85, 78, 67, 92];
    const MAX_UNITS: usize = 32768;
    check_root_wide_name(name, work)?;
    if name.starts_with(VERBATIM) || name.starts_with(NT) {
        let copied = copy_work(name.len() as u64 * 2);
        work.admit(copied)?;
        let (mut result, allocation) = buffer(name.len(), work)?;
        result.extend_from_slice(name);
        if let Err(e) = work.record(copied) {
            drop(result);
            work.release(allocation)?;
            return Err(e);
        }
        return Ok((result, allocation));
    }
    let input = if name == [0] { &[46u16, 0][..] } else { name };
    let mut required = work.attempt_io(ITEM, || {
        // SAFETY: input is a validated terminated native name.
        let count = unsafe { GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()), None, None) };
        if count == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(count as usize)
    })?;
    loop {
        if required > MAX_UNITS {
            return Err(io::Error::from_raw_os_error(206).into());
        }
        let (mut output, allocation) = buffer(
            required.checked_add(8).ok_or(crate::WorkError::Overflow)?,
            work,
        )?;
        output.resize(required + 8, 0u16);
        let result = (|| {
            let count = work.attempt_io(ITEM, || {
                // SAFETY: the terminated input and complete output buffer remain live.
                let count = unsafe {
                    GetFullPathNameW(
                        windows::core::PCWSTR(input.as_ptr()),
                        Some(&mut output[8..]),
                        None,
                    )
                };
                if count == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(count as usize)
            })?;
            if count >= required {
                return Ok(None);
            }
            let (prefix, skip): (&[u16], usize) = match &output[8..8 + count] {
                [_, 58, 92, ..] => (VERBATIM, 0),
                [92, 92, 46, 92, ..] => (VERBATIM, 4),
                [92, 92, 63, 92, ..] | [92, 63, 63, 92, ..] => (&[], 0),
                [92, 92, ..] => (UNC, 2),
                _ => (&[], 0),
            };
            let begin = 8 + skip - prefix.len();
            let length = 8 + count + 1 - begin;
            let copied = copy_work((prefix.len() + length) as u64 * 2);
            work.admit(copied)?;
            output[begin..begin + prefix.len()].copy_from_slice(prefix);
            output.copy_within(begin..8 + count + 1, 0);
            output.truncate(length);
            work.record(copied)?;
            Ok(Some(()))
        })();
        match result {
            Ok(Some(())) => return Ok((output, allocation)),
            Ok(None) if required < MAX_UNITS => {
                drop(output);
                work.release(allocation)?;
                required = required.saturating_mul(2).min(MAX_UNITS);
            }
            result => {
                drop(output);
                work.release(allocation)?;
                return match result {
                    Err(e) => Err(e),
                    _ => Err(io::Error::from_raw_os_error(206).into()),
                };
            }
        }
    }
}

#[cfg(windows)]
pub(super) fn check_root_wide_name<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<(), W::Error> {
    work.check()?;
    let Some((last, body)) = name.split_last() else {
        return Err(W::invalid_request());
    };
    if *last != 0 {
        return Err(W::invalid_request());
    }
    for unit in body {
        work.started_io(ITEM)?;
        if *unit == 0 {
            return Err(W::invalid_request());
        }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn root_path_identity_wide<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    check_root_wide_name(name, work)?;
    let file = work.attempt_io(READ, || {
        open_windows_root_name(
            name,
            windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES
                | windows::Win32::Storage::FileSystem::SYNCHRONIZE,
        )
    })?;
    directory_identity(&Dir::from_std_file(file), work)
}

#[cfg(windows)]
pub(super) fn windows_file_information(
    file: &impl std::os::windows::io::AsRawHandle,
) -> io::Result<windows::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    let mut information =
        windows::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: the held handle and complete output outlive this synchronous query.
    unsafe {
        windows::Win32::Storage::FileSystem::GetFileInformationByHandle(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            &mut information,
        )
    }
    .map_err(|e| io::Error::from_raw_os_error(e.code().0 & 0xffff))?;
    Ok(information)
}
