use super::*;

pub(crate) fn root_path_identity<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    #[cfg(unix)]
    {
        let stat = stat_unix_fd_at(libc::AT_FDCWD, path.as_os_str(), false, work)?;
        if !stat.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "native root is not a directory",
            )
            .into());
        }
        Ok(NativeRootIdentity {
            device: stat.dev(),
            object: stat.ino(),
        })
    }
    #[cfg(windows)]
    {
        let (input, input_allocation) = encode_root_path_name(path, work)?;
        let result = normalize_root_wide_name(&input, work);
        drop(input);
        work.release(input_allocation)?;
        let (name, allocation) = result?;
        let result = root_path_identity_wide(&name, work);
        drop(name);
        work.release(allocation)?;
        result
    }
}

pub(super) fn clone_directory<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<Dir, W::Error> {
    loop {
        match work.attempt_io(READ, || directory.try_clone()) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}

#[cfg(unix)]
pub(super) fn open_unix<W: NativeHostWork>(
    directory: i32,
    name: &std::ffi::CStr,
    mut flags: i32,
    work: &mut W,
) -> Result<File, W::Error> {
    use std::os::fd::FromRawFd as _;
    loop {
        work.started_io(
            if flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT) != 0 {
                WRITE
            } else {
                READ
            },
        )?;
        // SAFETY: the held parent or AT_FDCWD and terminated name are live.
        let fd = unsafe { libc::openat(directory, name.as_ptr(), flags, 0o666) };
        if fd >= 0 {
            // SAFETY: openat returned a new owned descriptor.
            return Ok(unsafe { File::from_raw_fd(fd) });
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        #[cfg(target_os = "linux")]
        if error.kind() == io::ErrorKind::PermissionDenied && flags & libc::O_NOATIME != 0 {
            flags &= !libc::O_NOATIME;
            continue;
        }
        return Err(error.into());
    }
}

#[cfg(unix)]
pub(super) fn open_unix_at<W: NativeHostWork>(
    directory: &Dir,
    name: &OsStr,
    flags: i32,
    work: &mut W,
) -> Result<cap_std::fs::File, W::Error> {
    unix_name(name, work, |name, work| {
        open_unix(directory.as_raw_fd(), name, flags, work).map(cap_std::fs::File::from_std)
    })
}

#[cfg(unix)]
pub(crate) fn stat_leaf<W: NativeHostWork>(
    directory: &Dir,
    name: &OsStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    require_leaf(Path::new(name))?;
    stat_unix_fd_at(directory.as_raw_fd(), name, false, work)
}

#[cfg(unix)]
pub(super) fn stat_unix_fd_at<W: NativeHostWork>(
    fd: i32,
    name: &OsStr,
    empty_path: bool,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(target_os = "linux")]
    let directory = if fd == libc::AT_FDCWD {
        rustix::fs::CWD
    } else {
        // SAFETY: callers pass a live held descriptor, except the explicit CWD sentinel above, supplied by rustix itself.
        unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
    };
    #[cfg(target_os = "linux")]
    let flags = rustix::fs::AtFlags::SYMLINK_NOFOLLOW
        | if empty_path {
            rustix::fs::AtFlags::EMPTY_PATH
        } else {
            rustix::fs::AtFlags::empty()
        };
    unix_name(name, work, |name, work| {
        #[cfg(target_os = "linux")]
        loop {
            work.started_io(READ)?;
            match rustix::fs::statx(
                directory,
                name,
                flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
            ) {
                Ok(stat) => return Ok(HostStat::from_statx(stat)?),
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
        loop {
            work.started_io(READ)?;
            #[cfg(target_os = "linux")]
            let result = if empty_path {
                rustix::fs::fstat(directory)
            } else {
                rustix::fs::statat(directory, name, flags)
            };
            #[cfg(target_os = "macos")]
            let result = {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                let result = unsafe {
                    if empty_path {
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
                if result == 0 {
                    Ok(unsafe { stat.assume_init() })
                } else {
                    Err(io::Error::last_os_error())
                }
            };
            match result {
                Ok(stat) => return Ok(HostStat::from_stat(stat)?),
                #[cfg(target_os = "linux")]
                Err(rustix::io::Errno::INTR) => {}
                #[cfg(target_os = "macos")]
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
    })
}

#[cfg(unix)]
pub(super) fn fstat<W: NativeHostWork>(
    fd: std::os::fd::RawFd,
    work: &mut W,
) -> Result<libc::stat, W::Error> {
    loop {
        let result = work.attempt_io(READ, || {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: a successful fstat initializes complete output for a live descriptor.
            if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { stat.assume_init() })
        });
        match result {
            Err(e) if W::io_error_kind(&e) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}
