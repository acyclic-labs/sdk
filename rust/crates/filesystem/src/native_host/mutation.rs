use super::*;

impl HostRoot {
    #[cfg(any(test, feature = "native-mount"))]
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
    #[cfg(any(test, feature = "native-mount"))]
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
    #[cfg(any(test, feature = "native-mount"))]
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
        let native_mode = libc::mode_t::try_from(mode).map_err(|_| DemandError::InvalidRequest)?;
        if !matches!(native_mode & libc::S_IFMT, kind if kind == libc::S_IFCHR || kind == libc::S_IFBLK)
        {
            return Err(DemandError::InvalidRequest);
        }
        let mode = native_mode;
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated leaf outlive the syscall.
            unix_mutation(work, || unsafe {
                libc::mknodat(parent.as_raw_fd(), leaf.as_ptr(), mode, device)
            })
        })
    }
    #[cfg(unix)]
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn bind_unix_socket<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_parent_leaf(&self.directory, path, work, bind_unix_socket_in)
    }

    #[cfg(all(unix, feature = "native-mount"))]
    pub(crate) fn hard_link(
        &self,
        source: &Path,
        destination: &Path,
        work: &mut &NativeMaterializationWork,
    ) -> Result<(), NativeTreeMaterializationError> {
        use NativeTreeMaterializationError as Error;
        let source_parent =
            self.open_dir_held(source.parent().ok_or(Error::InvalidLayout)?, work)?;
        let destination_parent =
            self.open_dir_held(destination.parent().ok_or(Error::InvalidLayout)?, work)?;
        unix_name(
            source.file_name().ok_or(Error::InvalidLayout)?,
            work,
            |source, work| {
                unix_name(
                    destination.file_name().ok_or(Error::InvalidLayout)?,
                    work,
                    |destination, work| {
                        // SAFETY: both held parents and terminated names outlive linkat.
                        unix_mutation(work, || unsafe {
                            libc::linkat(
                                source_parent.as_raw_fd(),
                                source.as_ptr(),
                                destination_parent.as_raw_fd(),
                                destination.as_ptr(),
                                0,
                            )
                        })
                    },
                )
            },
        )
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
                set_windows_name(&file, parent, leaf, 0, nt::FileLinkInformation, work)
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
            || target.first() == Some(&92)
                && !target.starts_with(&[92, 92])
                && !target.starts_with(&[92, 63, 63, 92])
            || matches!(target, [_, 58, ..]) && target.get(2) != Some(&92)
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
#[cfg(any(test, feature = "native-mount"))]
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
        let fd = rustix::path::DecInt::new(
            u32::try_from(parent.as_raw_fd()).map_err(|_| DemandError::InvalidRequest)?,
        );
        let digits = fd.as_bytes();
        work.started_io(WorkCounters {
            bytes_encoded: digits.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        work.started_io(copy_work((prefix.len() + digits.len()) as u64))?;
        path.get_mut(..prefix.len())
            .ok_or(DemandError::InvalidRequest)?
            .copy_from_slice(prefix);
        path.get_mut(prefix.len()..prefix.len() + digits.len())
            .ok_or(DemandError::InvalidRequest)?
            .copy_from_slice(digits);
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
    // SAFETY: sockaddr_un contains only integer fields and a character array.
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family =
        libc::sa_family_t::try_from(libc::AF_UNIX).map_err(|_| DemandError::InvalidRequest)?;
    let length = (base + 1)
        .checked_add(leaf.to_bytes().len())
        .ok_or(crate::WorkError::Overflow)?;
    if length >= address.sun_path.len() {
        return Err(DemandError::InvalidRequest);
    }
    work.started_io(copy_work((length + leaf.to_bytes().len()) as u64))?;
    *path.get_mut(base).ok_or(DemandError::InvalidRequest)? = b'/';
    path.get_mut(base + 1..length)
        .ok_or(DemandError::InvalidRequest)?
        .copy_from_slice(leaf.to_bytes());
    for (destination, byte) in address
        .sun_path
        .iter_mut()
        .zip(path.get(..length).ok_or(DemandError::InvalidRequest)?)
    {
        *destination = libc::c_char::from_ne_bytes([*byte]);
    }
    let address_length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = u8::try_from(address_length).map_err(|_| DemandError::InvalidRequest)?;
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
            // SAFETY: socket returned a fresh descriptor owned by this call.
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    })?;
    #[cfg(target_os = "macos")]
    // SAFETY: the fresh socket is held live while setting its descriptor flags.
    unix_mutation(work, || unsafe {
        libc::fcntl(socket.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC)
    })?;
    let address_length =
        libc::socklen_t::try_from(address_length).map_err(|_| DemandError::InvalidRequest)?;
    // SAFETY: initialized sockaddr_un and held socket outlive bind.
    unix_mutation(work, || unsafe {
        libc::bind(
            socket.as_raw_fd(),
            (&raw const address).cast(),
            address_length,
        )
    })?;
    // SAFETY: the held socket remains live during listen.
    unix_mutation(work, || unsafe { libc::listen(socket.as_raw_fd(), 128) })?;
    #[cfg(target_os = "macos")]
    {
        let held = stat_at(parent.as_raw_fd(), leaf, work)?;
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        work.started_io(READ)?;
        // SAFETY: the terminated path and output stat remain live during lstat.
        if unsafe { libc::lstat(path.as_ptr().cast(), observed.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: lstat succeeded and initialized the output stat.
        let observed = unsafe { observed.assume_init() };
        if held.raw.st_dev != observed.st_dev || held.raw.st_ino != observed.st_ino {
            // SAFETY: the terminated path remains live during unlink.
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
            && file_identity_and_length(&file, work)?.0 != expected
        {
            return Err(io::Error::other("rename source identity changed").into());
        }
        set_windows_name(
            &file,
            to,
            to_name,
            u32::from(mode == acyclic_native_runtime::RenameMode::Replace),
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
