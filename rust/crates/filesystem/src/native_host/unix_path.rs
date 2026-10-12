use super::*;

#[cfg(unix)]
pub(crate) fn unix_name<W: NativeHostWork, T>(
    name: &OsStr,
    work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    unix_name_checked(name, false, work, operation)
}

#[cfg(unix)]
pub(super) fn unix_name_checked<W: NativeHostWork, T>(
    name: &OsStr,
    single_leaf: bool,
    work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = name.as_bytes();
    if single_leaf && matches!(bytes, b"" | b"." | b"..") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected one owned native child",
        )
        .into());
    }
    for byte in bytes {
        let delta = ITEM;
        work.admit(delta)?;
        let invalid = *byte == 0 || single_leaf && *byte == b'/';
        work.record(delta)?;
        if invalid {
            return Err(W::invalid_request());
        }
    }
    let size = bytes
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let delta = WorkCounters {
        bytes_copied: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta)?;
    let (mut owned, allocation) = buffer(size, work)?;
    let result = (|| {
        owned.extend_from_slice(bytes);
        owned.push(0);
        let name = std::ffi::CStr::from_bytes_with_nul(&owned)
            .expect("validated name and one appended terminator");
        work.record(delta)?;
        operation(name, work)
    })();
    drop(owned);
    work.release(allocation)?;
    result
}

#[cfg(unix)]
pub(super) fn create_unix_directory_leaf<W: NativeHostWork>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<(), W::Error> {
    loop {
        let delta = WRITE;
        work.started_io(delta)?;
        // SAFETY: held directory and terminated single-component name remain live.
        if unsafe { libc::mkdirat(parent.as_raw_fd(), leaf.as_ptr(), 0o777) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}

#[cfg(unix)]
pub(crate) fn open_unix_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &OsStr,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    unix_name(name, work, |name, work| {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let open =
            |work: &mut W| open_unix(parent.as_raw_fd(), name, flags, work).map(Dir::from_std_file);
        match open(work) {
            Ok(directory) => return Ok(directory),
            Err(error) if create && W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        match create_unix_directory_leaf(parent, name, work) {
            Ok(()) => {}
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::AlreadyExists) => {}
            Err(error) => return Err(error),
        }
        open(work)
    })
}

pub(crate) fn open_owned_child_directory<W: NativeHostWork>(
    parent: &Dir,
    #[cfg(unix)] name: &OsStr,
    #[cfg(windows)] name: &[u16],
    work: &mut W,
) -> Result<Dir, W::Error> {
    #[cfg(unix)]
    let file = unix_name_checked(name, true, work, |name, work| {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::FromRawFd as _;
            // SAFETY: open_how contains only integer fields.
            let mut how: libc::open_how = unsafe { std::mem::zeroed() };
            how.flags =
                (libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_RDONLY | libc::O_DIRECTORY) as u64;
            how.resolve = libc::RESOLVE_NO_XDEV | libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS;
            loop {
                let result = work.attempt_io(READ, || {
                    // SAFETY: held parent, terminated leaf, and initialized kernel ABI storage remain live.
                    let fd = unsafe {
                        libc::syscall(
                            libc::SYS_openat2,
                            parent.as_raw_fd(),
                            name.as_ptr(),
                            &raw const how,
                            std::mem::size_of::<libc::open_how>(),
                        )
                    };
                    if fd < 0 {
                        let error = io::Error::last_os_error();
                        return Err(
                            if matches!(error.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) {
                                io::Error::new(
                                    io::ErrorKind::Unsupported,
                                    "owned scratch requires native no-cross resolution",
                                )
                            } else {
                                error
                            },
                        );
                    }
                    // SAFETY: successful openat2 returned a new owned descriptor.
                    Ok(unsafe { File::from_raw_fd(fd as i32) })
                });
                match result {
                    Err(error)
                        if matches!(
                            W::io_error_kind(&error),
                            Some(io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock)
                        ) =>
                    {
                        continue;
                    }
                    result => return result,
                }
            }
        }
        #[cfg(target_os = "macos")]
        {
            let mount = held_mount_identity(parent, work)?;
            let child = open_unix(
                parent.as_raw_fd(),
                name,
                libc::O_EVTONLY
                    | libc::O_CLOEXEC
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_DIRECTORY,
                work,
            )?;
            if held_mount_identity(&child, work)? != mount {
                return Err(io::Error::from_raw_os_error(libc::EXDEV).into());
            }
            Ok(child)
        }
    })?;
    #[cfg(windows)]
    let file = {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
            FILE_GENERIC_READ, GetFileInformationByHandle,
        };
        let child = open_windows_leaf_handle(parent, name, FILE_GENERIC_READ, Some(true), work)?;
        let identity = directory_identity(parent, work)?;
        work.attempt_io(READ, || {
            let mut metadata = BY_HANDLE_FILE_INFORMATION::default();
            // SAFETY: the held nofollow child and complete output remain live.
            unsafe {
                GetFileInformationByHandle(
                    windows::Win32::Foundation::HANDLE(child.as_raw_handle()),
                    &raw mut metadata,
                )
            }
            .map_err(io::Error::from)?;
            if metadata.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
                || metadata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "owned scratch child redirects",
                ));
            }
            if u64::from(metadata.dwVolumeSerialNumber) != identity.device {
                return Err(io::Error::new(
                    io::ErrorKind::CrossesDevices,
                    "owned scratch child crosses a volume",
                ));
            }
            Ok(())
        })?;
        child
    };
    work.check()?;
    Ok(Dir::from_std_file(file))
}

#[cfg(target_os = "macos")]
pub(super) fn held_mount_identity<W: NativeHostWork>(
    file: &impl std::os::fd::AsRawFd,
    work: &mut W,
) -> Result<[i32; 2], W::Error> {
    loop {
        let result = work.attempt_io(READ, || {
            let mut metadata = std::mem::MaybeUninit::<libc::statfs>::uninit();
            // SAFETY: live held descriptor and exactly sized native output.
            if unsafe { libc::fstatfs(file.as_raw_fd(), metadata.as_mut_ptr()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful fstatfs initialized its public result.
            let metadata = unsafe { metadata.assume_init() };
            // SAFETY: Darwin fsid_t is repr(C) with exactly two i32 words; this preserves its opaque mounted-filesystem identity bits.
            Ok(unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(metadata.f_fsid) })
        });
        match result {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}

#[cfg(unix)]
pub(super) fn open_unix_directory_path<W: NativeHostWork>(
    root: &Dir,
    path: &Path,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let mut current = clone_directory(root, work)?;
    for component in path.components() {
        let delta = ITEM;
        work.admit(delta)?;
        let name = match component {
            std::path::Component::Normal(name) => name,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid held path component",
                )
                .into());
            }
        };
        work.record(delta)?;
        current = open_unix_directory(&current, name, create, work)?;
    }
    Ok(current)
}

#[cfg(unix)]
pub(super) fn with_unix_parent_leaf<W: crate::demand::SourceDemandReadWork, T>(
    root: &Dir,
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&Dir, &std::ffi::CStr, &mut W) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    let leaf = path.file_name().ok_or(DemandError::InvalidRequest)?;
    let parent = open_unix_directory_path(
        root,
        path.parent().ok_or(DemandError::InvalidRequest)?,
        false,
        work,
    )?;
    unix_name(leaf, work, |leaf, work| operation(&parent, leaf, work))
}
