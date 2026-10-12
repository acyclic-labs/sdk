use super::*;

#[cfg(windows)]
pub(crate) fn canonical_directory_wide<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        GETFINALPATHNAMEBYHANDLE_FLAGS, GetFinalPathNameByHandleW,
    };
    let query = |output: &mut [u16], work: &mut W| {
        let count = work.attempt_io(READ, || {
            // SAFETY: the held handle and initialized output remain live.
            let count = unsafe {
                GetFinalPathNameByHandleW(
                    windows::Win32::Foundation::HANDLE(directory.as_raw_handle()),
                    output,
                    GETFINALPATHNAMEBYHANDLE_FLAGS(0),
                )
            };
            if count == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        })?;
        work.check()?;
        Ok::<_, W::Error>(count)
    };
    let mut capacity = query(&mut [], work)?;
    loop {
        let (mut output, allocation) = buffer(capacity, work)?;
        output.resize(capacity, 0);
        let result = query(&mut output, work);
        if let Ok(count) = result.as_ref() {
            if *count < output.len() {
                output.truncate(*count);
                return Ok((output, allocation));
            }
        }
        drop(output);
        work.release(allocation)?;
        let count = result?;
        capacity = count
            .checked_add(usize::from(count == capacity))
            .ok_or(crate::WorkError::Overflow)?;
        if capacity > u32::MAX as usize {
            return Err(crate::WorkError::Overflow.into());
        }
    }
}

#[cfg(unix)]
pub(crate) fn normalize_root_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    path: &Path,
    work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.check()?;
    if path.is_absolute() {
        return clone_path(path, work).map(|(path, _)| path);
    }
    let cwd = cwd_path(work)?;
    let result = (|| {
        let prefix = cwd.as_os_str().as_bytes();
        let suffix = path.as_os_str().as_bytes();
        let separator = usize::from(!prefix.ends_with(b"/"));
        let size = prefix
            .len()
            .checked_add(suffix.len())
            .ok_or(crate::WorkError::Overflow)?;
        let copy = copy_work(size as u64);
        work.admit(copy)?;
        let (mut output, allocation) = buffer(
            size.checked_add(separator)
                .ok_or(crate::WorkError::Overflow)?,
            work,
        )?;
        output.extend_from_slice(prefix);
        if separator != 0 {
            output.push(b'/');
        }
        output.extend_from_slice(suffix);
        if let Err(error) = work.record(copy) {
            drop(output);
            work.release(allocation)?;
            return Err(error);
        }
        Ok(OsString::from_vec(output).into())
    })();
    let capacity = cwd.capacity() as u64;
    drop(cwd);
    work.release(capacity)?;
    result
}

#[cfg(unix)]
pub(super) fn cwd_metadata_flags() -> i32 {
    #[cfg(target_os = "linux")]
    let access = libc::O_PATH;
    #[cfg(target_os = "macos")]
    let access = libc::O_EVTONLY;
    access | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
}

#[cfg(unix)]
fn cwd_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::OsStringExt as _;
    #[cfg(target_os = "macos")]
    let current = Dir::from_std_file(open_unix(libc::AT_FDCWD, c".", cwd_metadata_flags(), work)?);
    let mut bytes = [0u8; libc::PATH_MAX as usize];
    loop {
        work.started_io(READ)?;
        #[cfg(target_os = "linux")]
        // SAFETY: the entire initialized stack buffer is writable.
        let count = unsafe { libc::syscall(libc::SYS_getcwd, bytes.as_mut_ptr(), bytes.len()) };
        #[cfg(target_os = "macos")]
        // SAFETY: F_GETPATH receives its PATH_MAX-sized output.
        let count = unsafe { libc::fcntl(current.as_raw_fd(), libc::F_GETPATH, bytes.as_mut_ptr()) }
            as libc::c_long;
        if count >= 0 {
            let length = bytes
                .iter()
                .position(|byte| *byte == 0)
                .ok_or_else(|| io::Error::other("unterminated CWD"))?;
            if length == 0 || bytes[0] != b'/' {
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            #[cfg(target_os = "macos")]
            for _ in 0..=length {
                work.started_io(ITEM)?;
            }
            let (mut output, allocation) = buffer(length, work)?;
            if let Err(error) = work.started_io(copy_work(length as u64)) {
                drop(output);
                work.release(allocation)?;
                return Err(error);
            }
            output.extend_from_slice(&bytes[..length]);
            return Ok(OsString::from_vec(output).into());
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::ERANGE | libc::ENAMETOOLONG | libc::ENOSPC) => {
                #[cfg(target_os = "linux")]
                let current = Dir::from_std_file(open_unix(
                    libc::AT_FDCWD,
                    c".",
                    cwd_metadata_flags(),
                    work,
                )?);
                return canonical_directory_path_held(current, work).map(|(path, _)| path);
            }
            _ => return Err(error.into()),
        }
    }
}

#[cfg(unix)]
fn cwd_ancestor_name<W: NativeHostWork>(
    parent: &Dir,
    child: NativeRootIdentity,
    work: &mut W,
) -> Result<(OsString, W::Allocation), W::Error> {
    let (mut reader, allocation) = read_dir_held(parent, work)?;
    let result = (|| {
        while let Some((entry, allocation)) = reader.next(work)? {
            if entry
                .stat
                .as_ref()
                .is_some_and(|stat| stat.is_dir() && stat.identity() == child)
            {
                return Ok((entry.name, allocation));
            }
            drop(entry);
            work.release(allocation)?;
        }
        Err(io::Error::from_raw_os_error(libc::ENOENT).into())
    })();
    drop(reader);
    if let Err(error) = work.release(allocation) {
        if let Ok((name, allocation)) = result {
            drop(name);
            work.release(allocation)?;
        }
        return Err(error);
    }
    result
}

#[cfg(unix)]
pub(crate) fn canonical_directory_path_held<W: NativeHostWork>(
    mut current: Dir,
    work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    let root_directory =
        Dir::from_std_file(open_unix(libc::AT_FDCWD, c"/", cwd_metadata_flags(), work)?);
    let root = directory_identity(&root_directory, work)?;
    let (mut output, mut allocation) = buffer::<u8, _>(1, work)?;
    let result = (|| {
        loop {
            work.check()?;
            let child = directory_identity(&current, work)?;
            if child == root {
                break;
            }
            let parent = Dir::from_std_file(open_unix(
                current.as_raw_fd(),
                c"..",
                cwd_metadata_flags(),
                work,
            )?);
            if directory_identity(&parent, work)? == child {
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            let (name, name_allocation) = cwd_ancestor_name(&parent, child, work)?;
            let result = (|| {
                let size = output
                    .len()
                    .checked_add(name.len())
                    .and_then(|size| size.checked_add(1))
                    .ok_or(crate::WorkError::Overflow)?;
                if size > output.capacity() {
                    let (mut grown, next) = buffer(size, work)?;
                    if let Err(error) = work.started_io(copy_work(output.len() as u64)) {
                        drop(grown);
                        work.release(next)?;
                        return Err(error);
                    }
                    grown.append(&mut output);
                    drop(std::mem::replace(&mut output, grown));
                    work.release(std::mem::replace(&mut allocation, next))?;
                }
                work.started_io(copy_work(name.len() as u64))?;
                output.extend(name.as_bytes().iter().rev().copied());
                output.push(b'/');
                Ok(())
            })();
            drop(name);
            work.release(name_allocation)?;
            result?;
            current = parent;
        }
        if output.is_empty() {
            output.push(b'/');
        }
        work.started_io(copy_work(output.len() as u64))?;
        output.reverse();
        Ok(())
    })();
    if let Err(error) = result {
        drop(output);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((OsString::from_vec(output).into(), allocation))
}
