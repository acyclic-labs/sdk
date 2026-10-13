use super::*;

impl HostRoot {
    pub(crate) fn create_file<W: SourceDemandReadWork>(
        &self,
        path: &NativePath,
        #[cfg(windows)] options: nt::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<File, DemandError> {
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            #[cfg(windows)]
            return open_windows_at(
                parent,
                leaf,
                fs::FILE_GENERIC_READ
                    | fs::FILE_GENERIC_WRITE
                    | if options.0 & nt::FILE_DELETE_ON_CLOSE.0 != 0 {
                        fs::DELETE
                    } else {
                        fs::FILE_ACCESS_RIGHTS(0)
                    },
                nt::FILE_CREATE,
                nt::FILE_NON_DIRECTORY_FILE | options,
                work,
            );
            #[cfg(unix)]
            open_unix(
                parent.as_raw_fd(),
                leaf,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                work,
            )
        })
    }
    pub(crate) fn create_dir<W: SourceDemandReadWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            #[cfg(windows)]
            return open_windows_at(
                parent,
                leaf,
                fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE,
                nt::FILE_CREATE,
                nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )
            .map(drop);
            #[cfg(unix)]
            create_unix_directory_leaf(parent, leaf, work)
        })
    }
    pub(crate) fn create_dir_all_held<W: SourceDemandReadWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let directory = directory_path(&self.directory, path, true, work)?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
    #[cfg(unix)]
    pub(crate) fn symlink<W: SourceDemandReadWork>(
        &self,
        target: &OsStr,
        destination: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            unix_name(target, work, |target, work| {
                // SAFETY: held directory and terminated names outlive the syscall.
                unix_mutation(work, || unsafe {
                    libc::symlinkat(target.as_ptr(), parent.as_raw_fd(), leaf.as_ptr())
                })
            })
        })
    }
    #[cfg(all(unix, any(feature = "native-mount", test)))]
    pub(crate) fn create_fifo_held<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        mode: u32,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated leaf outlive the syscall.
            unix_mutation(work, || unsafe {
                libc::mkfifoat(
                    parent.as_raw_fd(),
                    leaf.as_ptr(),
                    (mode & 0o7777) as libc::mode_t,
                )
            })
        })
    }
    #[cfg(all(unix, any(feature = "native-mount", all(test, target_os = "macos"))))]
    pub(crate) fn create_device_held<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        mode: u32,
        device: libc::dev_t,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if !matches!(mode & libc::S_IFMT as u32, kind if kind == libc::S_IFCHR as u32 || kind == libc::S_IFBLK as u32)
        {
            return Err(DemandError::InvalidRequest);
        }
        let mode = libc::mode_t::try_from(mode).map_err(|_| DemandError::InvalidRequest)?;
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated leaf outlive the syscall.
            unix_mutation(work, || unsafe {
                libc::mknodat(parent.as_raw_fd(), leaf.as_ptr(), mode, device)
            })
        })
    }
    #[cfg(unix)]
    pub(crate) fn bind_unix_socket<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, path, work, bind_unix_socket_in)
    }
    #[cfg(windows)]
    pub(crate) fn hard_link<W: SourceDemandReadWork>(
        &self,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, source, work, |parent, leaf, work| {
            let file = open_windows_at(
                parent,
                leaf,
                fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                nt::FILE_OPEN,
                nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?;
            with_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
                set_windows_name(&file, parent, leaf, false, nt::FileLinkInformation, work)
            })
        })
    }
    #[cfg(windows)]
    pub(crate) fn symlink_file<W: SourceDemandReadWork>(
        &self,
        target: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        if target.is_empty()
            || target[0] == 92
                && !target.starts_with(&[92, 92])
                && !target.starts_with(&[92, 63, 63, 92])
            || target.len() >= 2 && target[1] == 58 && (target.len() < 3 || target[2] != 92)
        {
            return Err(DemandError::InvalidRequest);
        }
        for unit in target {
            work.started_io(ITEM)?;
            if *unit == 0 {
                return Err(DemandError::InvalidRequest);
            }
        }
        with_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            let (target, target_allocation) = decode_windows_name(target, work)?;
            let result = (|| {
                let (leaf, allocation) = decode_windows_name(leaf, work)?;
                let result = work.attempt_io(WRITE, || parent.symlink_file(&target, &leaf));
                drop(leaf);
                work.release(allocation)?;
                result
            })();
            drop(target);
            work.release(target_allocation)?;
            result
        })
    }
}

#[cfg(unix)]
fn bind_unix_socket_in<W: SourceDemandReadWork>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::fd::{FromRawFd as _, OwnedFd};
    let mut path = [0_u8; libc::PATH_MAX as usize];
    #[cfg(target_os = "linux")]
    let base = {
        let prefix = b"/proc/self/fd/";
        let fd = rustix::path::DecInt::new(parent.as_raw_fd() as u32);
        let digits = fd.as_bytes();
        work.started_io(WorkCounters {
            bytes_encoded: digits.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        work.started_io(copy_work((prefix.len() + digits.len()) as u64))?;
        path[..prefix.len()].copy_from_slice(prefix);
        path[prefix.len()..prefix.len() + digits.len()].copy_from_slice(digits);
        prefix.len() + digits.len()
    };
    #[cfg(target_os = "macos")]
    let base = {
        work.started_io(READ)?;
        // SAFETY: F_GETPATH writes at most PATH_MAX bytes to this live buffer.
        if unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        path.iter()
            .position(|byte| *byte == 0)
            .ok_or(DemandError::InvalidRequest)?
    };
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let length = (base + 1)
        .checked_add(leaf.to_bytes().len())
        .ok_or(crate::WorkError::Overflow)?;
    if length >= address.sun_path.len() {
        return Err(DemandError::InvalidRequest);
    }
    work.started_io(copy_work((length + leaf.to_bytes().len()) as u64))?;
    path[base] = b'/';
    path[base + 1..length].copy_from_slice(leaf.to_bytes());
    for (destination, byte) in address.sun_path.iter_mut().zip(&path[..length]) {
        *destination = *byte as libc::c_char;
    }
    let address_length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = address_length as u8;
    }
    #[cfg(target_os = "linux")]
    let kind = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(target_os = "macos")]
    let kind = libc::SOCK_STREAM;
    let socket = retry_io(work, WRITE, || {
        // SAFETY: socket returns a fresh descriptor or -1.
        let fd = unsafe { libc::socket(libc::AF_UNIX, kind, 0) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    })?;
    #[cfg(target_os = "macos")]
    unix_mutation(work, || unsafe {
        libc::fcntl(socket.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC)
    })?;
    // SAFETY: initialized sockaddr_un and held socket outlive bind and listen.
    unix_mutation(work, || unsafe {
        libc::bind(
            socket.as_raw_fd(),
            (&raw const address).cast(),
            address_length as libc::socklen_t,
        )
    })?;
    unix_mutation(work, || unsafe { libc::listen(socket.as_raw_fd(), 128) })?;
    #[cfg(target_os = "macos")]
    {
        let held = stat_at(parent.as_raw_fd(), leaf, work)?;
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        work.started_io(READ)?;
        if unsafe { libc::lstat(path.as_ptr().cast(), observed.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let observed = unsafe { observed.assume_init() };
        if held.raw.st_dev as u64 != observed.st_dev as u64
            || held.raw.st_ino as u64 != observed.st_ino as u64
        {
            unix_mutation(work, || unsafe { libc::unlink(path.as_ptr().cast()) })?;
            return Err(io::Error::other("socket parent directory moved during bind").into());
        }
    }
    Ok(())
}

pub(super) fn require_leaf(path: &Path) -> io::Result<()> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(())
}

pub(crate) fn rename_held<W: NativeHostWork>(
    from: &Dir,
    from_name: &NativePath,
    to: &Dir,
    to_name: &NativePath,
    mode: acyclic_native_runtime::RenameMode,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        if expected.is_some() {
            return Err(W::invalid_request());
        }
        require_leaf(from_name)?;
        require_leaf(to_name)?;
        unix_name(from_name.as_os_str(), work, |source, work| {
            unix_name(to_name.as_os_str(), work, |destination, work| {
                retry_io(work, WRITE, || {
                    rename_unix_names_once(from, source, to, destination, mode)
                })
            })
        })
    }
    #[cfg(windows)]
    {
        let file = open_windows_at(
            from,
            from_name,
            fs::DELETE
                | fs::SYNCHRONIZE
                | if expected.is_some() {
                    fs::FILE_READ_ATTRIBUTES
                } else {
                    Default::default()
                },
            nt::FILE_OPEN,
            nt::FILE_OPEN_REPARSE_POINT | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        if let Some(expected) = expected
            && file_identity_and_length(&file, work)?.0 != expected {
                return Err(io::Error::other("rename source identity changed").into());
            }
        set_windows_name(
            &file,
            to,
            to_name,
            mode == acyclic_native_runtime::RenameMode::Replace,
            nt::FileRenameInformation,
            work,
        )
    }
}

pub(crate) fn sync_directory_held<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    return sync_file_held(directory, work);
    #[cfg(windows)]
    sync_file_held(
        &open_windows_at(
            directory,
            &[46],
            fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE,
            nt::FILE_OPEN,
            nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?,
        work,
    )
}

pub(crate) fn sync_file_held<W: NativeHostWork>(
    #[cfg(unix)] file: &impl std::os::fd::AsRawFd,
    #[cfg(windows)] file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<(), W::Error> {
    retry_io(
        work,
        WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        },
        || sync_held_descriptor_once(file),
    )
}
