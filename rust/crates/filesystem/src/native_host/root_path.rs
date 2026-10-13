use super::*;
use std::os::windows::{ffi::OsStrExt as _, io::FromRawHandle as _};

pub(super) fn open_windows_root_name(
    name: &[u16],
    access: fs::FILE_ACCESS_RIGHTS,
) -> io::Result<File> {
    // SAFETY: callers validate the terminated name; success transfers a fresh handle.
    let handle = unsafe {
        fs::CreateFileW(
            windows::core::PCWSTR(name.as_ptr()),
            access.0,
            fs::FILE_SHARE_READ | fs::FILE_SHARE_WRITE | fs::FILE_SHARE_DELETE,
            None,
            fs::OPEN_EXISTING,
            fs::FILE_FLAG_BACKUP_SEMANTICS | fs::FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
    }
    .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

pub(super) fn with_root_name<W: NativeHostWork, T>(
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    const VERBATIM: &[u16] = &[92, 92, 63, 92];
    const NT: &[u16] = &[92, 63, 63, 92];
    const UNC: &[u16] = &[92, 92, 63, 92, 85, 78, 67, 92];
    const MAX_UNITS: usize = 32768;
    let length = wide_units(path.as_os_str(), work)?;
    let (mut input, allocation) = buffer(
        length.checked_add(1).ok_or(crate::WorkError::Overflow)?,
        work,
    )?;
    let result = (|| {
        work.started_io(WorkCounters {
            bytes_encoded: (length as u64)
                .checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        for unit in path.as_os_str().encode_wide() {
            work.started_io(ITEM)?;
            if unit == 0 {
                return Err(W::invalid_request());
            }
            input.push(unit);
        }
        input.push(0);
        if input.starts_with(VERBATIM) || input.starts_with(NT) {
            return operation(&input, work);
        }
        let input = if length == 0 { &[46, 0][..] } else { &input };
        let query = |output: Option<&mut [u16]>, work: &mut W| {
            work.attempt_io(ITEM, || {
                // SAFETY: terminated input and any initialized output remain live.
                let count = unsafe {
                    fs::GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()), output, None)
                };
                if count == 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(count as usize)
                }
            })
        };
        let mut required = query(None, work)?;
        loop {
            if required > MAX_UNITS {
                return Err(io::Error::from_raw_os_error(206).into());
            }
            let (mut output, allocation) = buffer(required + 8, work)?;
            output.resize(required + 8, 0);
            let result = (|| {
                let count = query(Some(&mut output[8..]), work)?;
                if count >= required {
                    return Ok(Err(count));
                }
                let (prefix, skip): (&[u16], usize) = match &output[8..8 + count] {
                    [_, 58, 92, ..] => (VERBATIM, 0),
                    [92, 92, 46, 92, ..] => (VERBATIM, 4),
                    [92, 92, 63, 92, ..] | [92, 63, 63, 92, ..] => (&[], 0),
                    [92, 92, ..] => (UNC, 2),
                    _ => (&[], 0),
                };
                let begin = 8 + skip - prefix.len();
                work.started_io(copy_work(prefix.len() as u64 * 2))?;
                output[begin..begin + prefix.len()].copy_from_slice(prefix);
                Ok::<_, W::Error>(Ok((begin, 8 + count + 1)))
            })();
            if let Ok(Ok((begin, end))) = result {
                let result = operation(&output[begin..end], work);
                drop(output);
                work.release(allocation)?;
                return result;
            }
            drop(output);
            work.release(allocation)?;
            let Err(count) = result? else { unreachable!() };
            required = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
    })();
    drop(input);
    work.release(allocation)?;
    result
}

pub(super) fn check_root_wide_name<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<(), W::Error> {
    work.check()?;
    let Some((&0, body)) = name.split_last() else {
        return Err(W::invalid_request());
    };
    for unit in body {
        work.started_io(ITEM)?;
        if *unit == 0 {
            return Err(W::invalid_request());
        }
    }
    Ok(())
}

pub(super) fn windows_file_information(
    file: &impl std::os::windows::io::AsRawHandle,
) -> io::Result<fs::BY_HANDLE_FILE_INFORMATION> {
    let mut information = fs::BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: held handle and complete output remain live for this query.
    unsafe { fs::GetFileInformationByHandle(win::HANDLE(file.as_raw_handle()), &mut information) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
    Ok(information)
}
