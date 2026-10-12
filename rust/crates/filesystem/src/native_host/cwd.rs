use super::*;

#[cfg(windows)]
pub(crate) fn canonical_directory_wide<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_NAME_NORMALIZED, VOLUME_NAME_DOS, GETFINALPATHNAMEBYHANDLE_FLAGS, GetFinalPathNameByHandleW,
    };
    let query = |buffer: &mut [u16], work: &mut W| -> Result<usize, W::Error> {
        native_read(work)?;
        // SAFETY: the held handle and admitted mutable native buffer remain live; the API supports an empty slice for its required-size query.
        let count = unsafe { GetFinalPathNameByHandleW(HANDLE(directory.as_raw_handle()),
            buffer, GETFINALPATHNAMEBYHANDLE_FLAGS(FILE_NAME_NORMALIZED.0 | VOLUME_NAME_DOS.0)) };
        if count == 0 { return Err(io::Error::last_os_error().into()); }
        work.check()?;
        Ok(count as usize)
    };
    let mut capacity = query(&mut [], work)?;
    loop {
        let bytes = capacity.checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)? as u64;
        let mut allocation = work.claim(bytes)?;
        let mut buffer = Vec::new();
        let reserved = buffer.try_reserve_exact(capacity);
        let actual = buffer.capacity().checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)? as u64;
        let reconciled = work.reconcile_allocation(&mut allocation, actual);
        if let Err(error) = reserved {
            drop(buffer);
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        if let Err(error) = reconciled {
            drop(buffer);
            work.release(allocation)?;
            return Err(error);
        }
        buffer.resize(capacity, 0u16);
        let count = match query(&mut buffer, work) {
            Ok(count) => count,
            Err(error) => {
                drop(buffer);
                work.release(allocation)?;
                return Err(error);
            }
        };
        if count < buffer.len() {
            buffer.truncate(count);
            return Ok((buffer, allocation));
        }
        drop(buffer);
        work.release(allocation)?;
        capacity = count.checked_add(usize::from(count == capacity)).ok_or(crate::WorkError::Overflow)?;
        if capacity > u32::MAX as usize { return Err(crate::WorkError::Overflow.into()); }
    }
}

#[cfg(unix)]
pub(crate) fn normalize_root_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    path: &Path, work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.check()?;
    if path.is_absolute() { return clone_path(path, work).map(|(path, _allocation)| path); }
    let cwd = cwd_path(work)?;
    let cwd_capacity = cwd.capacity() as u64;
    let result = (|| {
        let prefix = cwd.as_os_str().as_bytes();
        let suffix = path.as_os_str().as_bytes();
        let separator = usize::from(!prefix.ends_with(b"/"));
        let length = prefix.len().checked_add(separator)
            .and_then(|length| length.checked_add(suffix.len()))
            .ok_or(crate::WorkError::Overflow)?;
        let copied = prefix.len().checked_add(suffix.len()).ok_or(crate::WorkError::Overflow)?;
        let delta = WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let mut bytes = buffer::<u8, _>(length, work)?.0;
        bytes.extend_from_slice(prefix);
        if separator != 0 { bytes.push(b'/'); }
        bytes.extend_from_slice(suffix);
        if let Err(error) = work.record(delta) {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            work.release(capacity)?;
            return Err(error);
        }
        Ok(std::path::PathBuf::from(OsString::from_vec(bytes)))
    })();
    drop(cwd);
    work.release(cwd_capacity)?;
    result
}

#[cfg(unix)]
pub(super) fn cwd_metadata_flags() -> i32 {
    libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
        | {
            #[cfg(target_os = "linux")]
            { libc::O_PATH }
            #[cfg(target_os = "macos")]
            { libc::O_EVTONLY }
        }
}

#[cfg(unix)]
pub(crate) fn cwd_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(work: &mut W)
    -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::OsStringExt as _;
    #[cfg(target_os = "macos")]
    let current = Dir::from_std_file(open_unix(libc::AT_FDCWD, c".", cwd_metadata_flags(), || native_read(work))?);
    let mut bytes = [0u8; libc::PATH_MAX as usize];
    let result = loop {
        native_read(work)?;
        #[cfg(target_os = "linux")]
        // SAFETY: the kernel receives the complete writable stack buffer.
        let count = unsafe { libc::syscall(libc::SYS_getcwd, bytes.as_mut_ptr(), bytes.len()) };
        #[cfg(target_os = "macos")]
        // SAFETY: F_GETPATH receives its required PATH_MAX-sized output buffer.
        let count = unsafe { libc::fcntl(current.as_raw_fd(), libc::F_GETPATH, bytes.as_mut_ptr()) } as libc::c_long;
        if count >= 0 { break Ok(()); }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { break Err(error); }
    };
    match result {
        Ok(()) => {
            let length = bytes.iter().position(|byte| *byte == 0)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unterminated CWD"))?;
            if length == 0 || bytes[0] != b'/' { return Err(io::Error::from_raw_os_error(libc::ENOENT).into()); }
            #[cfg(target_os = "macos")]
            for _ in 0..=length {
                work.started_io(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            }
            let (mut owned, allocation) = buffer(length, work)?;
            let copied = WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED };
            if let Err(error) = work.started_io(copied) {
                drop(owned); work.release(allocation)?; return Err(error);
            }
            owned.extend_from_slice(&bytes[..length]);
            Ok(OsString::from_vec(owned).into())
        }
        Err(error) if matches!(error.raw_os_error(), Some(libc::ERANGE | libc::ENAMETOOLONG | libc::ENOSPC)) => {
            #[cfg(target_os = "linux")]
            let current = Dir::from_std_file(open_unix(libc::AT_FDCWD, c".", cwd_metadata_flags(), || native_read(work))?);
            canonical_directory_path_held(current, work).map(|(path, _)| path)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
pub(super) fn cwd_ancestor_name<W: NativeHostWork>(
    parent: &Dir, child: &HostStat, work: &mut W,
) -> Result<(OsString, W::Allocation), W::Error> {
    let (mut reader, allocation) = read_dir_held(parent, work)?;
    let mut found = None;
    let result = (|| {
        while let Some((entry, entry_allocation)) = reader.next(work)? {
            if entry.stat.as_ref().is_some_and(|stat|
                stat.is_dir() && stat.dev() == child.dev() && stat.ino() == child.ino()) {
                found = Some((entry.name, entry_allocation));
                return Ok(());
            }
            drop(entry);
            work.release(entry_allocation)?;
        }
        Err(io::Error::from_raw_os_error(libc::ENOENT).into())
    })();
    drop(reader);
    let released = work.release(allocation);
    if let Err(error) = result.and(released) {
        if let Some((name, allocation)) = found {
            drop(name);
            work.release(allocation)?;
        }
        return Err(error);
    }
    Ok(found.expect("matching held directory entry owns its native name"))
}

#[cfg(unix)]
pub(crate) fn canonical_directory_path_held<W: NativeHostWork>(mut current: Dir, work: &mut W)
    -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    let root = Dir::from_std_file(open_unix(libc::AT_FDCWD, c"/", cwd_metadata_flags(), || native_read(work))?);
    let root_stat = stat_unix_fd_at(root.as_raw_fd(), OsStr::new(""), true, work)?;
    let mut ancestors: Vec<(OsString, W::Allocation)> = Vec::new();
    let mut slots = None;
    let result = (|| {
        let mut length = 1usize;
        let mut copied = 0usize;
        loop {
            work.check()?;
            let child = stat_unix_fd_at(current.as_raw_fd(), OsStr::new(""), true, work)?;
            if child.dev() == root_stat.dev() && child.ino() == root_stat.ino() { break; }
            let parent = Dir::from_std_file(open_unix(current.as_raw_fd(), c"..", cwd_metadata_flags(), || native_read(work))?);
            let parent_stat = stat_unix_fd_at(parent.as_raw_fd(), OsStr::new(""), true, work)?;
            if parent_stat.dev() == child.dev() && parent_stat.ino() == child.ino() {
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            if ancestors.len() == ancestors.capacity() {
                let capacity = ancestors.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
                let (mut replacement, allocation) = buffer(capacity, work)?;
                replacement.append(&mut ancestors);
                drop(std::mem::replace(&mut ancestors, replacement));
                if let Some(old) = slots.replace(allocation) { work.release(old)?; }
            }
            let (name, allocation) = cwd_ancestor_name(&parent, &child, work)?;
            let bytes = name.len();
            ancestors.push((name, allocation));
            length = length.checked_add(bytes).and_then(|n| n.checked_add(usize::from(ancestors.len() > 1)))
                .ok_or(crate::WorkError::Overflow)?;
            copied = copied.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
            current = parent;
        }
        let delta = WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let (mut bytes, allocation) = buffer::<u8, _>(length, work)?;
        let copied = (|| {
            work.started_io(delta)?;
            bytes.push(b'/');
            for (name, _) in ancestors.iter().rev() {
                if bytes.len() != 1 { bytes.push(b'/'); }
                bytes.extend_from_slice(name.as_bytes());
            }
            work.check()
        })();
        if let Err(error) = copied {
            drop(bytes);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((std::path::PathBuf::from(OsString::from_vec(bytes)), allocation))
    })();
    let mut cleanup = Ok(());
    for (name, allocation) in ancestors {
        drop(name);
        let released = work.release(allocation);
        cleanup = cleanup.and(released);
    }
    if let Some(allocation) = slots { cleanup = cleanup.and(work.release(allocation)); }
    if let Err(error) = cleanup {
        if let Ok((path, allocation)) = result { drop(path); work.release(allocation)?; }
        return Err(error);
    }
    result
}
