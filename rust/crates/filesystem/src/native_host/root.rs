use super::*;

/// A held directory capability whose relative operations cannot escape through
/// path traversal or an intermediate symbolic link/reparse point.
pub struct HostRoot {
    pub(crate) directory: Dir,
    pub(crate) identity: crate::NativeRootIdentity,
}

impl HostRoot {
    pub(crate) fn exchange_to(
        &self,
        name: &Path,
        destination: &Self,
        destination_name: &Path,
        work: &mut impl NativeHostWork<Error = DemandError>,
    ) -> Result<(), DemandError> {
        crate::native_exchange::exchange_native_entries_held(
            &self.directory,
            name,
            &destination.directory,
            destination_name,
            work,
        )
    }
    pub(crate) fn recover_entry_exchange(
        &self,
        name: &Path,
        work: &mut impl NativeHostWork<Error = DemandError>,
    ) -> Result<(), DemandError> {
        crate::native_exchange::recover_native_entry_exchange_held(
            &self.directory,
            name,
            work,
        )
    }
}

impl HostRoot {
    pub(crate) fn open_dir_held<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<Dir, W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.open_dir_held_wide(path, work)
                    .map(|(directory, _)| directory)
            })
        }
        #[cfg(unix)]
        {
            let mut current = clone_directory(&self.directory, work)?;
            for component in path.components() {
                let item = ITEM;
                work.admit(item)?;
                work.record(item)?;
                let std::path::Component::Normal(name) = component else {
                    return Err(W::invalid_request());
                };
                let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW;
                #[cfg(target_os = "linux")]
                let flags = flags | libc::O_NOATIME;
                current = Dir::from_std_file(open_unix_at(&current, name, flags, work)?.into_std());
            }
            Ok(current)
        }
    }
    pub(crate) fn read_dir_stats<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.read_dir_stats_wide(path, work)
            })
        }
        #[cfg(unix)]
        {
            let directory = self.open_dir_held(path, work)?;
            read_dir_held(&directory, work)
        }
    }
    pub(crate) fn stat<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| self.stat_wide(path, work))
        }
        #[cfg(unix)]
        {
            work.check()?;
            if path.as_os_str().is_empty() {
                return stat_unix_at(&self.directory, OsStr::new("."), work);
            }
            let leaf = path.file_name().ok_or_else(W::invalid_request)?;
            let parent = self.open_dir_held(path.parent().ok_or_else(W::invalid_request)?, work)?;
            stat_unix_at(&parent, leaf, work)
        }
    }
    pub(crate) fn open_file<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.open_file_wide(
                    path,
                    windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
                    work,
                )
            })
        }
        #[cfg(unix)]
        {
            work.check()?;
            let leaf = path.file_name().ok_or_else(W::invalid_request)?;
            let parent = self.open_dir_held(path.parent().ok_or_else(W::invalid_request)?, work)?;
            let flags = libc::O_RDONLY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | libc::O_NOCTTY;
            #[cfg(target_os = "linux")]
            let flags = flags | libc::O_NOATIME;
            open_unix_at(&parent, leaf, flags, work)
        }
    }
    pub(crate) fn open_file_positional<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| {
                self.open_file_positional_wide(path, work)
            })
        }
        #[cfg(unix)]
        {
            self.open_file(path, work)
        }
    }
    pub(crate) fn stat_file<W: NativeHostWork>(
        &self,
        file: &File,
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(unix)]
        {
            stat_unix_fd_at(file.as_raw_fd(), OsStr::new(""), true, work)
        }
        #[cfg(windows)]
        {
            stat_windows_file(file, self.volume_serial_number()?, work)
        }
    }
    #[cfg(unix)]
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &Path,
        maximum_bytes: u64,
        work: &mut W,
    ) -> Result<(Vec<u8>, W::Allocation), W::Error> {
        let leaf = path.file_name().ok_or_else(W::invalid_request)?;
        let parent = self.open_dir_held(path.parent().ok_or_else(W::invalid_request)?, work)?;
        read_unix_link(&parent, leaf, maximum_bytes, work, W::invalid_request)
    }
    #[cfg(unix)]
    pub(crate) fn read_source_link<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        maximum_bytes: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let leaf = path.file_name().ok_or(DemandError::InvalidRequest)?;
        let parent = self.open_dir_held(path.parent().ok_or(DemandError::InvalidRequest)?, work)?;
        read_source_link_held(&parent, Path::new(leaf), maximum_bytes, work)
    }
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn open_root_wide<W: NativeHostWork>(
        name: &[u16],
        work: &mut W,
    ) -> Result<Self, W::Error> {
        check_root_wide_name(name, work)?;
        let file = work.attempt_io(READ, || {
            open_windows_root_name(name, windows::Win32::Storage::FileSystem::FILE_GENERIC_READ)
        })?;
        let directory = Dir::from_std_file(file);
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
}
pub(crate) fn open_root_directory<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<(Dir, NativeRootIdentity), W::Error> {
    let cleanup = work.reserve_cleanup_write()?;
    let mut opened = None;
    #[cfg(unix)]
    let result = unix_name(path.as_os_str(), work, |name, work| {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC | {
            #[cfg(target_os = "linux")]
            {
                libc::O_NOATIME
            }
            #[cfg(not(target_os = "linux"))]
            {
                0
            }
        };
        opened = Some(open_unix(libc::AT_FDCWD, name, flags, work)?);
        Ok(())
    });
    #[cfg(windows)]
    let result = with_root_path_name(path, false, work, |name, work| {
        opened = Some(work.attempt_io(READ, || {
            open_windows_root_name(name, windows::Win32::Storage::FileSystem::FILE_GENERIC_READ)
        })?);
        Ok(())
    });
    if let Err(error) = result {
        if let Some(file) = opened {
            let ((), recorded) = work.attempt_cleanup_write(cleanup, || drop(file));
            recorded?;
        } else {
            work.cancel_cleanup_write(cleanup);
        }
        return Err(error);
    }
    let directory =
        Dir::from_std_file(opened.expect("successful native root open retains its file"));
    match directory_identity(&directory, work) {
        Ok(identity) => {
            work.cancel_cleanup_write(cleanup);
            Ok((directory, identity))
        }
        Err(error) => {
            let ((), recorded) = work.attempt_cleanup_write(cleanup, || drop(directory));
            recorded?;
            Err(error)
        }
    }
}

impl HostRoot {
    pub(crate) fn open<W: NativeHostWork>(path: &Path, work: &mut W) -> Result<Self, W::Error> {
        let (directory, identity) = open_root_directory(path, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
}

impl HostRoot {
    #[must_use]
    pub const fn identity(&self) -> crate::NativeRootIdentity {
        self.identity
    }
    pub(crate) fn held_identity<W: NativeHostWork>(
        &self,
        work: &mut W,
    ) -> Result<crate::NativeRootIdentity, W::Error> {
        directory_identity(&self.directory, work)
    }
    pub(crate) fn is_local<W: NativeHostWork>(&self, work: &mut W) -> Result<bool, W::Error> {
        filesystem_is_local(&self.directory, work)
    }
    #[cfg(windows)]
    pub(crate) fn directory_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        use std::os::windows::io::AsHandle as _;
        self.directory.as_handle()
    }
    #[cfg(unix)]
    pub(crate) fn directory_fd(&self) -> std::os::fd::RawFd {
        self.directory.as_raw_fd()
    }
    #[cfg(windows)]
    pub(super) fn volume_serial_number(&self) -> io::Result<u32> {
        u32::try_from(self.identity.device)
            .map_err(|_| io::Error::other("the root's volume has no serial number"))
    }
    #[cfg(target_os = "macos")]
    pub(crate) fn clone_file_from<W: crate::demand::SourceDemandReadWork>(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let source = match source_root.open_file(source, work) {
            Ok(source) => source,
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let stat = stat_unix_fd_at(source.as_raw_fd(), OsStr::new(""), true, work)?;
        if !stat.file_type().is_file() {
            return Err(DemandError::NotRegularFile);
        }
        with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            let result = unix_mutation(work, || {
                // SAFETY: source inode, destination parent and terminated leaf are live.
                unsafe {
                    libc::fclonefileat(source.as_raw_fd(), parent.as_raw_fd(), leaf.as_ptr(), 0)
                }
            });
            match result {
                Ok(()) => Ok(true),
                Err(DemandError::Io(error))
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOTSUP | libc::ENOSYS | libc::EXDEV)
                    ) =>
                {
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        })
    }
    #[cfg(windows)]
    pub(crate) fn clone_file_from<W: crate::demand::SourceDemandReadWork>(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        with_wide_path(source, work, |source, work| {
            with_wide_path(destination, work, |destination, work| {
                self.clone_file_from_wide(source_root, source, destination, work)
            })
        })
    }
    /// Copies one pinned regular source into a new file using `ReFS` block cloning when available, then owned-buffer overlapped I/O otherwise.
    #[cfg(windows)]
    pub async fn copy_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<()> {
        let source_root = HostRoot {
            directory: source_root.directory.try_clone()?,
            identity: source_root.identity,
        };
        if destination
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let name = destination.file_name().ok_or(io::ErrorKind::InvalidInput)?;
        let parent = self
            .open_dir_held(
                destination.parent().ok_or(io::ErrorKind::InvalidInput)?,
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        let name = name.to_os_string();
        let source = source.to_path_buf();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let context = crate::obs::caller_context();
        tokio::spawn(async move {
            let result = crate::obs::in_span(&context, async {
                let staged = std::sync::Arc::new(
                    acyclic_native_runtime::run_blocking_io(
                        move || StagedWindowsFile::create(parent),
                        None,
                    )
                    .expect("unmetered task construction cannot fail")
                    .await??,
                );
                copy_windows_file_worker(source_root, source, &staged, &sender).await?;
                if sender.is_closed() {
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
                }
                acyclic_native_runtime::run_blocking_io(
                    move || {
                        let result = staged.publish(&name);
                        drop(staged);
                        result
                    },
                    None,
                )
                .expect("unmetered task construction cannot fail")
                .await?
            })
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| io::Error::other("native copy owner stopped before completion"))?
    }
    #[cfg(target_os = "macos")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_macos_metadata_target<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<MacMetadataTarget, DemandError> {
        open_macos_metadata_target(&self.directory, path, work)
    }
    #[cfg(target_os = "linux")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_linux_metadata_target<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<File, DemandError> {
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            open_unix(
                parent.as_raw_fd(),
                leaf,
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                work,
            )
        })
    }
    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<cap_std::fs::File, DemandError> {
        with_wide_path(path, work, |path, work| {
            self.open_windows_metadata_target_wide(path, work)
        })
    }
    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<cap_std::fs::File, DemandError> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{
            FILE_GENERIC_READ, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
        };
        work.check()?;
        if path.is_empty() {
            let file = open_windows_wide_at(
                &self.directory,
                &[u16::from(b'.')],
                FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                work,
                || DemandError::InvalidRequest,
            )?
            .ok_or(DemandError::InvalidRequest)?;
            return Ok(file);
        }
        let mut separator = None;
        for (index, unit) in path.iter().enumerate() {
            work.charge(ITEM)?;
            if *unit == u16::from(b'\\') {
                separator = Some(index);
            }
        }
        let (parent_path, leaf) = match separator {
            Some(index) => (&path[..index], &path[index + 1..]),
            None => (&[][..], path),
        };
        if separator.is_some() && parent_path.is_empty() {
            return Err(DemandError::InvalidRequest);
        }
        let parent = if parent_path.is_empty() {
            None
        } else {
            Some(Dir::from_std_file(
                open_windows_wide_at(
                    &self.directory,
                    parent_path,
                    FILE_GENERIC_READ,
                    FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                    work,
                    || DemandError::InvalidRequest,
                )?
                .ok_or(DemandError::InvalidRequest)?
                .into_std(),
            ))
        };
        let parent = parent.as_ref().unwrap_or(&self.directory);
        let file = cap_std::fs::File::from_std(open_windows_leaf_handle(
            parent,
            leaf,
            FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
            None,
            work,
        )?);
        Ok(file)
    }
}
