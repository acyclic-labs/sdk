use super::*;

pub(super) fn fence_cookie_held<W: NativeHostWork>(
    parent: &Dir,
    #[cfg(unix)] leaf: &std::ffi::CStr,
    #[cfg(windows)] leaf: &[u16],
    work: &mut W,
) -> Result<(), W::Error> {
    let cleanup = work.reserve_cleanup_write()?;
    #[cfg(unix)]
    let created = open_unix(
        parent.as_raw_fd(),
        leaf,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        work,
    );
    #[cfg(windows)]
    let created = create_windows_wide_at(parent, leaf, false, false, false, true, work);
    let created = match created {
        Ok(file) => file,
        Err(error) => {
            work.cancel_cleanup_write(cleanup);
            return Err(error);
        }
    };
    let completion = work.check();
    let (removed, recorded) = work.attempt_cleanup_write(cleanup, || {
        #[cfg(unix)]
        {
            // SAFETY: the held parent and prepared leaf survive cancellation.
            if unsafe { libc::unlinkat(parent.as_raw_fd(), leaf.as_ptr(), 0) } < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
        #[cfg(windows)]
        {
            use windows::Wdk::Storage::FileSystem::{
                FILE_DISPOSITION_INFORMATION, FileDispositionInformation,
            };
            windows_set_information(
                &created,
                &FILE_DISPOSITION_INFORMATION { DeleteFile: true },
                FileDispositionInformation,
            )
        }
    });
    drop(created);
    recorded?;
    removed?;
    completion?;
    work.check()
}

impl HostRoot {
    pub(crate) fn fence_cookie(
        &self,
        relative: &Path,
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, relative, work, |parent, leaf, work| {
                fence_cookie_held(parent, leaf, work)
            })
        }
        #[cfg(windows)]
        {
            with_wide_path(relative, work, |relative, work| {
                let (parent, leaf) = wide_parent_leaf(relative)?;
                let parent = self
                    .open_dir_held_wide(parent, work)
                    .map(|(directory, _)| directory)?;
                fence_cookie_held(&parent, leaf, work)
            })
        }
    }
    pub(crate) fn create_file<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<File, DemandError> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.create_windows_file_wide(path, false, false, work)
            })
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
    pub(crate) fn create_dir<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| self.create_dir_wide(path, work))
        }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                create_unix_directory_leaf(parent, leaf, work)
            })
        }
    }
    pub(crate) fn create_dir_all_held<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.create_dir_all_held_wide(path, work)
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
    pub(crate) fn hard_link<W: crate::demand::SourceDemandReadWork>(
        &self,
        source: &Path,
        destination: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        #[cfg(windows)]
        {
            with_wide_path(source, work, |source, work| {
                with_wide_path(destination, work, |destination, work| {
                    self.hard_link_wide(source, destination, work)
                })
            })
        }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(
                &self.directory,
                source,
                work,
                |source_parent, source_leaf, work| {
                    with_unix_parent_leaf(
                        &self.directory,
                        destination,
                        work,
                        |parent, leaf, work| {
                            // SAFETY: both capability parents and terminated names outlive linkat.
                            unix_mutation(work, || unsafe {
                                libc::linkat(
                                    source_parent.as_raw_fd(),
                                    source_leaf.as_ptr(),
                                    parent.as_raw_fd(),
                                    leaf.as_ptr(),
                                    0,
                                )
                            })
                        },
                    )
                },
            )
        }
    }
    #[cfg(unix)]
    pub(crate) fn symlink<W: crate::demand::SourceDemandReadWork>(
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
    pub(crate) fn set_permissions_without_open<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        mode: u32,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mode =
            libc::mode_t::try_from(mode & 0o7777).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name outlive the nofollow operation.
            unix_mutation(work, || unsafe {
                libc::fchmodat(
                    parent.as_raw_fd(),
                    leaf.as_ptr(),
                    mode,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            })
        })
    }
    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn create_fifo_held<W: crate::demand::SourceDemandReadWork>(
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
    pub(crate) fn create_device_held<W: crate::demand::SourceDemandReadWork>(
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
    pub(crate) fn bind_unix_socket<W: crate::demand::SourceDemandReadWork>(
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
pub(super) fn bind_unix_socket_in<W: crate::demand::SourceDemandReadWork>(
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
        let mut number = parent.as_raw_fd() as u32;
        let encoded = u64::from(if number == 0 { 1 } else { number.ilog10() + 1 });
        work.charge(WorkCounters {
            bytes_encoded: encoded,
            ..WorkCounters::UNCHARGED
        })?;
        let mut digits = [0_u8; 10];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (number % 10) as u8;
            number /= 10;
            if number == 0 {
                break;
            }
        }
        let length = digits.len() - start;
        work.charge(WorkCounters {
            bytes_copied: (prefix.len() + length) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        path[..prefix.len()].copy_from_slice(prefix);
        path[prefix.len()..prefix.len() + length].copy_from_slice(&digits[start..]);
        prefix.len() + length
    };
    #[cfg(target_os = "macos")]
    let base = {
        native_read(work)?;
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
    work.charge(WorkCounters {
        bytes_copied: (length + leaf.to_bytes().len()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    path[base] = b'/';
    path[base + 1..length].copy_from_slice(leaf.to_bytes());
    for (destination, byte) in address.sun_path.iter_mut().zip(&path[..length]) {
        *destination = *byte as libc::c_char;
    }
    #[cfg(target_os = "macos")]
    {
        address.sun_len = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1) as u8;
    }
    native_write(work)?;
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
        native_write(work)?;
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
        use std::os::unix::ffi::OsStrExt as _;
        let through_descriptor = stat_unix_at(parent, OsStr::from_bytes(leaf.to_bytes()), work)?;
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        native_read(work)?;
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
pub(crate) fn rename_no_replace_held<W: NativeHostWork>(
    from: &Dir,
    from_name: &Path,
    to: &Dir,
    to_name: &Path,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    if expected.is_some() {
        return Err(W::invalid_request());
    }
    require_leaf(from_name)?;
    require_leaf(to_name)?;
    #[cfg(unix)]
    {
        unix_name(from_name.as_os_str(), work, |from_name, work| {
            unix_name(to_name.as_os_str(), work, |to_name, work| {
                work.attempt_io(
                    WorkCounters {
                        backend_write_operations: 1,
                        ..WorkCounters::UNCHARGED
                    },
                    || rename_unix_names_once(from, from_name, to, to_name, false),
                )
            })
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Wdk::Storage::FileSystem::{
            FILE_OPEN_REPARSE_POINT, FILE_RENAME_INFORMATION, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let access = DELETE
            | SYNCHRONIZE
            | if expected.is_some() {
                FILE_READ_ATTRIBUTES
            } else {
                Default::default()
            };
        let file = open_windows_held(
            from,
            from_name,
            access,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        if let Some(expected) = expected {
            if file_identity_and_length(&file, work)?.0 != expected {
                return Err(io::Error::other("rename source identity changed").into());
            }
        }
        let units = wide_units(to_name.as_os_str(), work)?;
        let name_bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
        let header = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
        let bytes = header
            .checked_add(name_bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let words = bytes
            .max(std::mem::size_of::<FILE_RENAME_INFORMATION>())
            .div_ceil(8);
        let mut allocation = work.claim(words as u64 * 8)?;
        let mut buffer = Vec::new();
        let reserved = buffer.try_reserve_exact(words);
        let reconciled = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64 * 8);
        let result = (|| {
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            buffer.resize(words, 0u64);
            let encoded = WorkCounters {
                bytes_encoded: name_bytes as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(encoded)?;
            work.record(encoded)?;
            let information = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
            // SAFETY: aligned storage covers the fields and entire UTF-16 tail.
            unsafe {
                std::ptr::addr_of_mut!((*information).RootDirectory)
                    .write(HANDLE(to.as_handle().as_raw_handle()));
                std::ptr::addr_of_mut!((*information).FileNameLength)
                    .write(u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?);
                let tail = buffer.as_mut_ptr().cast::<u8>().add(header).cast::<u16>();
                for (index, unit) in to_name.as_os_str().encode_wide().enumerate() {
                    if unit == 0 {
                        return Err(W::invalid_request());
                    }
                    tail.add(index).write(unit);
                }
            }
            let length = u32::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
            work.attempt_io(
                WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                },
                || {
                    windows_name_packet_once(
                        &file,
                        &buffer,
                        length,
                        windows::Wdk::Storage::FileSystem::FileRenameInformation,
                    )
                },
            )
        })();
        drop(buffer);
        work.release(allocation)?;
        result
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
        use windows::Wdk::Storage::FileSystem::{
            FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
        let file = open_windows_held(
            directory,
            Path::new("."),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?;
        sync_file_held(&file, work)
    }
}

#[cfg(windows)]
pub(super) fn open_windows_held<W: NativeHostWork>(
    directory: &Dir,
    path: &Path,
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W,
) -> Result<File, W::Error> {
    if path != Path::new(".") {
        require_leaf(path)?;
    }
    with_wide_path(path, work, |path, work| {
        open_windows_wide_at(directory, path, access, options, work, W::invalid_request)?
            .map(cap_std::fs::File::into_std)
            .ok_or_else(|| {
                status_error(windows::Win32::Foundation::NTSTATUS(
                    0xC000_050B_u32.cast_signed(),
                ))
                .into()
            })
    })
}

#[cfg(windows)]
pub(crate) fn set_file_attributes_held(
    parent: &Dir,
    name: &Path,
    attributes: u32,
    expected: Option<NativeRootIdentity>,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    sync_windows_metadata_held(parent, name, Some(attributes), expected, &mut &*work)
}

pub(crate) fn sync_file_held<W: NativeHostWork>(
    #[cfg(unix)] file: &impl std::os::fd::AsRawFd,
    #[cfg(windows)] file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<(), W::Error> {
    loop {
        let error = work.attempt_io(
            WorkCounters {
                durability_operations: 1,
                ..WorkCounters::UNCHARGED
            },
            || Ok(sync_held_descriptor_once(file).err()),
        )?;
        match error {
            #[cfg(unix)]
            Some(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Some(error) => return Err(error.into()),
            None => return Ok(()),
        }
    }
}
