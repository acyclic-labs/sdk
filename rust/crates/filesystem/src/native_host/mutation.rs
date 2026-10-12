use super::*;

impl HostRoot {
    pub(crate) fn create_file<W: SourceDemandReadWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        #[cfg(windows)] options: nt::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<File, DemandError> {
        #[cfg(windows)]
        {
            let (parent, leaf) = wide_parent_leaf(path)?;
            let access = fs::FILE_GENERIC_READ
                | fs::FILE_GENERIC_WRITE
                | if options.0 & nt::FILE_DELETE_ON_CLOSE.0 != 0 {
                    fs::DELETE
                } else {
                    fs::FILE_ACCESS_RIGHTS(0)
                };
            open_windows_at(
                &self.open_dir_held(parent, work)?,
                leaf,
                access,
                nt::FILE_CREATE,
                nt::FILE_NON_DIRECTORY_FILE | options,
                work,
            )
        }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                open_unix(
                    parent.as_raw_fd(),
                    leaf,
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    work,
                )
            })
        }
    }
    pub(crate) fn create_dir<W: SourceDemandReadWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        #[cfg(windows)]
        {
            let (parent, leaf) = wide_parent_leaf(path)?;
            open_windows_at(
                &self.open_dir_held(parent, work)?,
                leaf,
                fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE,
                nt::FILE_CREATE,
                nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?;
            Ok(())
        }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                create_unix_directory_leaf(parent, leaf, work)
            })
        }
    }
    pub(crate) fn create_dir_all_held<W: SourceDemandReadWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        #[cfg(windows)]
        {
            let mut directory = clone_directory(&self.directory, work)?;
            if !path.is_empty() {
                for name in path.split(|u| *u == 92) {
                    work.started_io(ITEM)?;
                    directory =
                        Dir::from_std_file(create_windows_directory(&directory, name, work)?);
                }
            }
            let identity = directory_identity(&directory, work)?;
            Ok(Self {
                directory,
                identity,
            })
        }
        #[cfg(unix)]
        {
            let directory = open_unix_directory_path(&self.directory, path, true, work)?;
            let identity = directory_identity(&directory, work)?;
            Ok(Self {
                directory,
                identity,
            })
        }
    }
    #[cfg(unix)]
    pub(crate) fn symlink<W: SourceDemandReadWork>(
        &self,
        target: &OsStr,
        destination: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            unix_name(target, work, |target, work| {
                // SAFETY: held directory and both terminated strings are live.
                unix_mutation(work, || unsafe {
                    libc::symlinkat(target.as_ptr(), parent.as_raw_fd(), leaf.as_ptr())
                })
            })
        })
    }
    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn create_fifo_held<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        mode: u32,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mode =
            libc::mode_t::try_from(mode & 0o7777).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name remain live.
            unix_mutation(work, || unsafe {
                libc::mkfifoat(parent.as_raw_fd(), leaf.as_ptr(), mode)
            })
        })
    }
    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", all(test, target_os = "macos")))]
    pub(crate) fn create_device_held<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        mode: u32,
        device: libc::dev_t,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mask = mode & libc::S_IFMT as u32;
        if mask != libc::S_IFCHR as u32 && mask != libc::S_IFBLK as u32 {
            return Err(DemandError::InvalidRequest);
        }
        let mode = libc::mode_t::try_from(mode).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name remain live.
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
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            bind_unix_socket_in(parent, leaf, work)
        })
    }
}

#[cfg(unix)]
pub(super) fn bind_unix_socket_in<W: SourceDemandReadWork>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::fd::FromRawFd as _;
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let mut path = [0_u8; libc::PATH_MAX as usize];
    #[cfg(target_os = "linux")]
    let base = {
        let prefix = b"/proc/self/fd/";
        let fd = parent.as_raw_fd() as u32;
        work.started_io(WorkCounters {
            bytes_encoded: u64::from(fd.checked_ilog10().unwrap_or(0) + 1),
            ..WorkCounters::UNCHARGED
        })?;
        let number = rustix::path::DecInt::new(fd);
        let digits = number.as_bytes();
        work.started_io(copy_work((prefix.len() + digits.len()) as u64))?;
        path[..prefix.len()].copy_from_slice(prefix);
        path[prefix.len()..prefix.len() + digits.len()].copy_from_slice(digits);
        prefix.len() + digits.len()
    };
    #[cfg(target_os = "macos")]
    let base = {
        work.started_io(READ)?;
        // SAFETY: F_GETPATH writes no more than PATH_MAX into this live buffer.
        if unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        path.iter()
            .position(|byte| *byte == 0)
            .ok_or(DemandError::InvalidRequest)?
    };
    let length = base
        .checked_add(1)
        .and_then(|n| n.checked_add(leaf.to_bytes().len()))
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
    #[cfg(target_os = "macos")]
    {
        address.sun_len = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1) as u8;
    }
    work.started_io(WRITE)?;
    #[cfg(target_os = "linux")]
    let kind = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let kind = libc::SOCK_STREAM;
    // SAFETY: socket returns a fresh owned descriptor or -1.
    let fd = unsafe { libc::socket(libc::AF_UNIX, kind, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let socket = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
    #[cfg(target_os = "macos")]
    {
        work.started_io(WRITE)?;
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    // SAFETY: correctly initialized sockaddr_un storage remains live.
    unix_mutation(work, || unsafe {
        libc::bind(
            socket.as_raw_fd(),
            (&raw const address).cast(),
            (std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1) as libc::socklen_t,
        )
    })?;
    unix_mutation(work, || unsafe { libc::listen(socket.as_raw_fd(), 128) })?;
    #[cfg(target_os = "macos")]
    {
        let through_descriptor = stat_at(parent.as_raw_fd(), leaf, work)?;
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        work.started_io(READ)?;
        if unsafe { libc::lstat(path.as_ptr().cast(), observed.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let observed = unsafe { observed.assume_init() };
        if through_descriptor.dev() != observed.st_dev as u64
            || through_descriptor.ino() != observed.st_ino as u64
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
    #[cfg(unix)] from_name: &Path,
    #[cfg(windows)] from_name: &[u16],
    to: &Dir,
    #[cfg(unix)] to_name: &Path,
    #[cfg(windows)] to_name: &[u16],
    mode: acyclic_native_runtime::RenameMode,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    if expected.is_some() {
        return Err(W::invalid_request());
    }
    #[cfg(unix)]
    {
        require_leaf(from_name)?;
        require_leaf(to_name)?;
    }
    #[cfg(unix)]
    {
        unix_name(from_name.as_os_str(), work, |from_name, work| {
            unix_name(to_name.as_os_str(), work, |to_name, work| {
                loop {
                    match work.attempt_io(WRITE, || {
                        rename_unix_names_once(from, from_name, to, to_name, mode)
                    }) {
                        Err(error)
                            if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) =>
                        {
                            ()
                        }
                        result => break result,
                    }
                }
            })
        })
    }
    #[cfg(windows)]
    {
        use fs::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        use nt::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        let access = DELETE
            | SYNCHRONIZE
            | if expected.is_some() {
                FILE_READ_ATTRIBUTES
            } else {
                Default::default()
            };
        let file = open_windows_at(
            from,
            from_name,
            access,
            nt::FILE_OPEN,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        if let Some(expected) = expected {
            if file_identity_and_length(&file, work)?.0 != expected {
                return Err(io::Error::other("rename source identity changed").into());
            }
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
    {
        sync_file_held(directory, work)
    }
    #[cfg(windows)]
    {
        use fs::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
        use nt::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
        let file = open_windows_at(
            directory,
            &[46],
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            nt::FILE_OPEN,
            FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        sync_file_held(&file, work)
    }
}

pub(crate) fn sync_file_held<W: NativeHostWork>(
    #[cfg(unix)] file: &impl std::os::fd::AsRawFd,
    #[cfg(windows)] file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<(), W::Error> {
    loop {
        let result = work.attempt_io(
            WorkCounters {
                durability_operations: 1,
                ..WorkCounters::UNCHARGED
            },
            || sync_held_descriptor_once(file),
        );
        match result {
            #[cfg(unix)]
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn hard_link<W: SourceDemandReadWork>(
        &self,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let (parent, leaf) = wide_parent_leaf(source)?;
        let file = open_windows_at(
            &self.open_dir_held(parent, work)?,
            leaf,
            fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
            nt::FILE_OPEN,
            nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        let (parent, leaf) = wide_parent_leaf(destination)?;
        set_windows_name(
            &file,
            &self.open_dir_held(parent, work)?,
            leaf,
            false,
            nt::FileLinkInformation,
            work,
        )
    }

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
        let (parent, leaf) = wide_parent_leaf(destination)?;
        windows_leaf_name(leaf, work)?;
        let parent = self.open_dir_held(parent, work)?;
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
    }
}
