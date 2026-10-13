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
        work.attempt_io(READ, || {
            // SAFETY: held handle and initialized output remain live.
            let count = unsafe {
                GetFinalPathNameByHandleW(
                    win::HANDLE(directory.as_raw_handle()),
                    output,
                    GETFINALPATHNAMEBYHANDLE_FLAGS(0),
                )
            };
            if count == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        })
    };
    let mut capacity = query(&mut [], work)?;
    loop {
        let (mut output, allocation) = buffer(capacity, work)?;
        output.resize(capacity, 0);
        let result = query(&mut output, work).and_then(|count| {
            work.check()?;
            Ok(count)
        });
        if let Ok(count) = result.as_ref() {
            if *count < capacity {
                output.truncate(*count);
                return Ok((output, allocation));
            }
        }
        drop(output);
        work.release(allocation)?;
        capacity = result?.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        if capacity > u32::MAX as usize {
            return Err(crate::WorkError::Overflow.into());
        }
    }
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
pub(crate) fn normalize_root_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    path: &Path,
    work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.check()?;
    if path.is_absolute() {
        return clone_path(path, work).map(|(path, _)| path);
    }
    let current = Dir::from_std_file(open_unix(libc::AT_FDCWD, c".", cwd_metadata_flags(), work)?);
    let (cwd, cwd_allocation) = canonical_directory_path_held(current, work)?;
    let result = (|| {
        let prefix = cwd.as_os_str().as_bytes();
        let suffix = path.as_os_str().as_bytes();
        let separator = usize::from(!prefix.ends_with(b"/"));
        let length = prefix
            .len()
            .checked_add(suffix.len())
            .and_then(|n| n.checked_add(separator))
            .ok_or(crate::WorkError::Overflow)?;
        let (mut output, allocation) = buffer(length, work)?;
        if let Err(error) = work.started_io(copy_work((length - separator) as u64)) {
            drop(output);
            work.release(allocation)?;
            return Err(error);
        }
        output.extend_from_slice(prefix);
        if separator != 0 {
            output.push(b'/');
        }
        output.extend_from_slice(suffix);
        Ok(OsString::from_vec(output).into())
    })();
    drop(cwd);
    work.release(cwd_allocation)?;
    result
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
    let root = Dir::from_std_file(open_unix(libc::AT_FDCWD, c"/", cwd_metadata_flags(), work)?);
    let root = directory_identity(&root, work)?;
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
            let copied = (|| {
                let size = output
                    .len()
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(1))
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
            copied?;
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
