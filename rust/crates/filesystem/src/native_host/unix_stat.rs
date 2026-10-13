use super::*;

pub(crate) fn root_path_identity<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    #[cfg(unix)]
    {
        let stat = unix_name(path.as_os_str(), work, |name, work| {
            stat_at(libc::AT_FDCWD, name, work)
        })?;
        if !stat.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "native root is not a directory",
            )
            .into());
        }
        Ok(stat.identity())
    }
    #[cfg(windows)]
    {
        with_root_name(path, work, |name, work| {
            let file = work.attempt_io(READ, || {
                open_windows_root_name(name, fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE)
            })?;
            directory_identity(&Dir::from_std_file(file), work)
        })
    }
}

pub(super) fn clone_directory<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<Dir, W::Error> {
    retry_io(work, READ, || directory.try_clone())
}

#[cfg(unix)]
pub(super) fn open_unix<W: NativeHostWork>(
    directory: i32,
    name: &std::ffi::CStr,
    mut flags: i32,
    work: &mut W,
) -> Result<File, W::Error> {
    let delta = if flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT) != 0 {
        WRITE
    } else {
        READ
    };
    loop {
        let result = retry_io(work, delta, || {
            use std::os::fd::FromRawFd as _;
            // SAFETY: callers retain the parent and name; a successful open returns one owned descriptor.
            let fd =
                unsafe { libc::openat(directory, name.as_ptr(), flags, 0o666 as libc::c_uint) };
            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(unsafe { File::from_raw_fd(fd) })
            }
        });
        #[cfg(target_os = "linux")]
        if result
            .as_ref()
            .is_err_and(|error| W::io_error_kind(error) == Some(io::ErrorKind::PermissionDenied))
            && flags & libc::O_NOATIME != 0
        {
            flags &= !libc::O_NOATIME;
            continue;
        }
        return result;
    }
}

#[cfg(unix)]
pub(super) fn stat_at<W: NativeHostWork>(
    fd: i32,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(target_os = "linux")]
    {
        let directory = if fd == libc::AT_FDCWD {
            rustix::fs::CWD
        } else {
            // SAFETY: the caller holds this descriptor for the complete query.
            unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
        };
        let flags = rustix::fs::AtFlags::SYMLINK_NOFOLLOW
            | if name.is_empty() && fd != libc::AT_FDCWD {
                rustix::fs::AtFlags::EMPTY_PATH
            } else {
                rustix::fs::AtFlags::empty()
            };
        loop {
            work.started_io(READ)?;
            match rustix::fs::statx(
                directory,
                name,
                flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
            ) {
                Ok(stat) => return Ok(HostStat::from_statx(stat)),
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
    }
    Ok(HostStat::from_stat(fstat(fd, name, work)?)?)
}

#[cfg(unix)]
pub(super) fn fstat<W: NativeHostWork>(
    fd: std::os::fd::RawFd,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<libc::stat, W::Error> {
    retry_io(work, READ, || {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: held descriptor, terminated name and complete output remain live.
        let result = unsafe {
            if name.is_empty() && fd != libc::AT_FDCWD {
                libc::fstat(fd, stat.as_mut_ptr())
            } else {
                libc::fstatat(
                    fd,
                    name.as_ptr(),
                    stat.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            }
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call initialized the complete record.
        Ok(unsafe { stat.assume_init() })
    })
}
