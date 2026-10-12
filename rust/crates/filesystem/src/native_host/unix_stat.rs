use super::*;

pub(crate) fn root_path_identity<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
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
        Ok(crate::NativeRootIdentity {
            device: stat.dev(),
            object: stat.ino(),
        })
    }
    #[cfg(windows)]
    {
        with_root_path_name(path, false, work, |name, work| {
            root_path_identity_wide(name, work)
        })
    }
}

pub(super) fn clone_directory<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<Dir, W::Error> {
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd as _;
        loop {
            native_read(work)?;
            // SAFETY: the held descriptor remains live and fcntl returns a new owner.
            let fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
            if fd >= 0 {
                return Ok(Dir::from_std_file(unsafe { File::from_raw_fd(fd) }));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
        use windows::Win32::Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE};
        use windows::Win32::System::Threading::GetCurrentProcess;
        native_read(work)?;
        let mut duplicated = HANDLE::default();
        // SAFETY: both handles belong to this process and the output receives a new owner.
        unsafe {
            let process = GetCurrentProcess();
            DuplicateHandle(
                process,
                HANDLE(directory.as_raw_handle()),
                process,
                &mut duplicated,
                0,
                false,
                DUPLICATE_SAME_ACCESS,
            )
            .map_err(|error| {
                W::Error::from(io::Error::from_raw_os_error(error.code().0 & 0xffff))
            })?;
            Ok(Dir::from_std_file(File::from_raw_handle(duplicated.0)))
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
pub(super) fn stat_unix_at<W: NativeHostWork>(
    directory: &Dir,
    name: &OsStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    stat_unix_fd_at(directory.as_raw_fd(), name, false, work)
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
            native_read(work)?;
            match rustix::fs::statx(
                directory,
                name,
                flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
            ) {
                Ok(stat) => {
                    return Ok(HostStat {
                        mode: u32::from(stat.stx_mode),
                        len: stat.stx_size,
                        dev: rustix::fs::makedev(stat.stx_dev_major, stat.stx_dev_minor),
                        ino: stat.stx_ino,
                        nlink: u64::from(stat.stx_nlink),
                        uid: stat.stx_uid,
                        gid: stat.stx_gid,
                        rdev: rustix::fs::makedev(stat.stx_rdev_major, stat.stx_rdev_minor),
                        atime: stat.stx_atime.tv_sec,
                        atime_nsec: i64::from(stat.stx_atime.tv_nsec),
                        mtime: stat.stx_mtime.tv_sec,
                        mtime_nsec: i64::from(stat.stx_mtime.tv_nsec),
                        ctime: stat.stx_ctime.tv_sec,
                        ctime_nsec: i64::from(stat.stx_ctime.tv_nsec),
                        created: if stat.stx_mask & rustix::fs::StatxFlags::BTIME.bits() != 0 {
                            unix_time(stat.stx_btime.tv_sec, i64::from(stat.stx_btime.tv_nsec)).ok()
                        } else {
                            None
                        },
                    });
                }
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
        loop {
            native_read(work)?;
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
    work.attempt_io(READ, || {
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the held descriptor stays live; successful fstat initializes the complete output.
        if unsafe { libc::fstat(fd, observed.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { observed.assume_init() })
    })
}
