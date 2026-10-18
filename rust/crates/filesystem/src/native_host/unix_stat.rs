use super::*;

#[cfg(any(test, feature = "native-watch"))]
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
    flags: i32,
    work: &mut W,
) -> Result<File, W::Error> {
    let delta = if flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT) != 0 {
        WRITE
    } else {
        READ
    };
    let result = retry_io(work, delta, || {
        #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
        stat_probe::event("openat");
        use std::os::fd::FromRawFd as _;
        // SAFETY: callers retain the parent and name; a successful open returns one owned descriptor.
        let fd = unsafe { libc::openat(directory, name.as_ptr(), flags, 0o666 as libc::c_uint) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: openat returned a new descriptor; ownership transfers to this File.
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    });
    #[cfg(target_os = "linux")]
    if result
        .as_ref()
        .is_err_and(|error| W::io_error_kind(error) == Some(io::ErrorKind::PermissionDenied))
        && flags & libc::O_NOATIME != 0
    {
        drop(result);
        return open_unix(directory, name, flags & !libc::O_NOATIME, work);
    }
    result
}

#[cfg(unix)]
pub(super) fn stat_at<W: NativeHostWork>(
    fd: i32,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(target_os = "linux")]
    {
        statx_at(fd, name, true, work)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(HostStat::from_stat(fstat(fd, name, work)?)?)
    }
}

#[cfg(all(target_os = "linux", any(test, feature = "native-watch")))]
pub(crate) fn stat_file_metadata<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    stat_at_metadata(file.as_raw_fd(), c"", work)
}

#[cfg(target_os = "linux")]
// Capture consumes identity and canonical metadata, not inode-generation cookies.
pub(super) fn stat_at_metadata<W: NativeHostWork>(
    fd: i32,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(test)]
    if stat_probe::old() {
        return stat_probe::baseline_at(fd, name, work);
    }
    statx_at(fd, name, false, work)
}

#[cfg(target_os = "linux")]
fn statx_at<W: NativeHostWork>(
    fd: i32,
    name: &std::ffi::CStr,
    with_handle: bool,
    work: &mut W,
) -> Result<HostStat, W::Error> {
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
        #[cfg(test)]
        stat_probe::query(false);
        let result = rustix::fs::statx(
            directory,
            name,
            flags,
            rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
        );
        #[cfg(test)]
        let result = stat_probe::statx_result(result);
        match result {
            Ok(stat) => {
                let handle = if with_handle {
                    file_handle_at(fd, name, flags)
                } else {
                    None
                };
                return Ok(HostStat::from_statx(stat).with_handle(handle));
            }
            Err(rustix::io::Errno::INTR) => continue,
            Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
            Err(error) => return Err(io::Error::from(error).into()),
        }
    }
    Ok(HostStat::from_stat(fstat(fd, name, work)?)?)
}

#[cfg(all(test, target_os = "linux"))]
#[path = "stat_probe.rs"]
pub(crate) mod stat_probe;

/// [`HostStat::handle`] of what `name` names relative to `fd`, resolved as
/// the stat that precedes it resolved it. The two queries are separate, so
/// a name replaced between them pairs one object's facts with another's
/// handle: an identity no object has, which the next query of the name
/// corrects. The query is metered as part of its stat.
#[cfg(target_os = "linux")]
fn file_handle_at(fd: i32, name: &std::ffi::CStr, flags: rustix::fs::AtFlags) -> Option<[u8; 32]> {
    #[cfg(test)]
    stat_probe::query(true);
    /// `struct file_handle` with room for `MAX_HANDLE_SZ` bytes.
    #[repr(C)]
    struct FileHandle {
        handle_bytes: libc::c_uint,
        handle_type: libc::c_int,
        f_handle: [u8; 128],
    }

    let flags = if flags.contains(rustix::fs::AtFlags::EMPTY_PATH) {
        libc::AT_EMPTY_PATH
    } else {
        0
    };
    let mut handle = FileHandle {
        handle_bytes: 128,
        handle_type: 0,
        f_handle: [0; 128],
    };
    let mut mount_id: libc::c_int = 0;
    // SAFETY: every pointer refers to a live, writable local of the layout
    // the kernel expects, `handle_bytes` is the capacity of `f_handle`, and
    // `name` is NUL-terminated; the call is synchronous.
    let result = unsafe {
        libc::syscall(
            libc::SYS_name_to_handle_at,
            fd,
            name.as_ptr(),
            std::ptr::from_mut(&mut handle),
            std::ptr::from_mut(&mut mount_id),
            flags,
        )
    };
    if result != 0 {
        return None;
    }
    let bytes = handle
        .f_handle
        .get(..usize::try_from(handle.handle_bytes).ok()?)?;
    let mut digest = blake3::Hasher::new();
    digest.update(&handle.handle_type.to_le_bytes());
    digest.update(bytes);
    Some(*digest.finalize().as_bytes())
}

#[cfg(unix)]
pub(super) fn fstat<W: NativeHostWork>(
    fd: std::os::fd::RawFd,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<libc::stat, W::Error> {
    retry_io(work, READ, || {
        #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
        stat_probe::fstat_query();
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
