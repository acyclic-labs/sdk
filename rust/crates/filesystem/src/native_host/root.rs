use super::*;

/// A held root whose relative operations cannot escape through links.
pub struct HostRoot {
    pub(crate) directory: Dir,
    pub(crate) identity: NativeRootIdentity,
}
impl HostRoot {
    pub(crate) fn open<W: NativeHostWork>(path: &Path, work: &mut W) -> Result<Self, W::Error> {
        let (directory, identity) = open_root_directory(path, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
    pub const fn identity(&self) -> NativeRootIdentity {
        self.identity
    }
    #[cfg(unix)]
    pub(crate) fn directory_fd(&self) -> std::os::fd::RawFd {
        self.directory.as_raw_fd()
    }
    #[cfg(windows)]
    pub(crate) fn directory_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        use std::os::windows::io::AsHandle as _;
        self.directory.as_handle()
    }
    #[cfg(windows)]
    pub(super) fn volume_serial_number(&self) -> io::Result<u32> {
        self.identity
            .device
            .try_into()
            .map_err(|_| io::Error::other("the root's volume has no serial number"))
    }
    pub(crate) fn open_dir_held<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<Dir, W::Error> {
        #[cfg(windows)]
        {
            with_wide_path(path, work, |path, work| self.open_dir_held_wide(path, work))
        }
        #[cfg(unix)]
        {
            let mut directory = clone_directory(&self.directory, work)?;
            for component in path.components() {
                work.started_io(ITEM)?;
                let std::path::Component::Normal(name) = component else {
                    return Err(W::invalid_request());
                };
                let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW;
                #[cfg(target_os = "linux")]
                let flags = flags | libc::O_NOATIME;
                directory =
                    Dir::from_std_file(open_unix_at(&directory, name, flags, work)?.into_std());
            }
            Ok(directory)
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
            read_dir_held(&self.open_dir_held(path, work)?, work)
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
                return stat_unix_fd_at(self.directory.as_raw_fd(), OsStr::new("."), false, work);
            }
            let leaf = path.file_name().ok_or_else(W::invalid_request)?;
            let parent = self.open_dir_held(path.parent().ok_or_else(W::invalid_request)?, work)?;
            stat_leaf(&parent, leaf, work)
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
                self.open_file_wide(
                    path,
                    windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
                    work,
                )
            })
        }
        #[cfg(unix)]
        {
            self.open_file(path, work)
        }
    }
    #[cfg(unix)]
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &Path,
        maximum: u64,
        work: &mut W,
    ) -> Result<(Vec<u8>, W::Allocation), W::Error> {
        let leaf = path.file_name().ok_or_else(W::invalid_request)?;
        read_unix_link(
            &self.open_dir_held(path.parent().ok_or_else(W::invalid_request)?, work)?,
            leaf,
            maximum,
            work,
        )
    }
    #[cfg(unix)]
    pub(crate) fn read_source_link<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        maximum: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let leaf = path.file_name().ok_or(DemandError::InvalidRequest)?;
        read_source_link_held(
            &self.open_dir_held(path.parent().ok_or(DemandError::InvalidRequest)?, work)?,
            Path::new(leaf),
            maximum,
            work,
        )
    }
    #[cfg(windows)]
    pub(crate) fn open_root_wide<W: NativeHostWork>(
        name: &[u16],
        work: &mut W,
    ) -> Result<Self, W::Error> {
        check_root_wide_name(name, work)?;
        let directory = Dir::from_std_file(work.attempt_io(READ, || {
            open_windows_root_name(name, windows::Win32::Storage::FileSystem::FILE_GENERIC_READ)
        })?);
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
    #[cfg(target_os = "macos")]
    pub(crate) fn clone_file_from<W: SourceDemandReadWork>(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let source = match source_root.open_file(source, work) {
            Ok(source) => source,
            Err(DemandError::Io(e)) if e.kind() == io::ErrorKind::PermissionDenied => {
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        if !stat_unix_fd_at(source.as_raw_fd(), OsStr::new(""), true, work)?
            .file_type()
            .is_file()
        {
            return Err(DemandError::NotRegularFile);
        }
        with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            // SAFETY: the held source, destination parent and terminated leaf outlive the call.
            match unix_mutation(work, || unsafe {
                libc::fclonefileat(source.as_raw_fd(), parent.as_raw_fd(), leaf.as_ptr(), 0)
            }) {
                Ok(()) => Ok(true),
                Err(DemandError::Io(e))
                    if matches!(
                        e.raw_os_error(),
                        Some(libc::ENOTSUP | libc::ENOSYS | libc::EXDEV)
                    ) =>
                {
                    Ok(false)
                }
                Err(e) => Err(e),
            }
        })
    }
    #[cfg(windows)]
    pub(crate) fn clone_file_from<W: SourceDemandReadWork>(
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
    /// Atomically publishes a pinned source copy, preserving sparse data and COW when supported.
    #[cfg(windows)]
    pub async fn copy_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<()> {
        if destination
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let name = destination
            .file_name()
            .ok_or(io::ErrorKind::InvalidInput)?
            .to_os_string();
        let parent = self
            .open_dir_held(
                destination.parent().ok_or(io::ErrorKind::InvalidInput)?,
                &mut ordinary_work(),
            )
            .map_err(ordinary_error)?;
        let source_root = HostRoot {
            directory: source_root.directory.try_clone()?,
            identity: source_root.identity,
        };
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
                acyclic_native_runtime::run_blocking_io(move || staged.publish(&name), None)
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
    #[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
    pub(crate) fn open_linux_metadata_target<W: SourceDemandReadWork>(
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
    #[cfg(all(windows, any(feature = "native-mount", test)))]
    pub(crate) fn open_windows_metadata_target_wide<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<cap_std::fs::File, DemandError> {
        use windows::{
            Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT},
            Win32::Storage::FileSystem::{
                FILE_GENERIC_READ, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
            },
        };
        work.check()?;
        let access = FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE;
        if path.is_empty() {
            return open_windows_wide_at(
                &self.directory,
                &[46],
                access,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?
            .ok_or(DemandError::InvalidRequest);
        }
        for _ in path {
            work.charge(ITEM)?;
        }
        let (parent_path, leaf) = wide_parent_leaf(path)?;
        if path.first() == Some(&92) {
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
                )?
                .ok_or(DemandError::InvalidRequest)?
                .into_std(),
            ))
        };
        Ok(cap_std::fs::File::from_std(open_windows_leaf_handle(
            parent.as_ref().unwrap_or(&self.directory),
            leaf,
            access,
            None,
            work,
        )?))
    }
}

pub(crate) fn open_root_directory<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<(Dir, NativeRootIdentity), W::Error> {
    #[cfg(unix)]
    let file = unix_name(path.as_os_str(), work, |name, work| {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        #[cfg(target_os = "linux")]
        let flags = flags | libc::O_NOATIME;
        open_unix(libc::AT_FDCWD, name, flags, work)
    })?;
    #[cfg(windows)]
    let file = {
        if path.as_os_str().is_empty() {
            return Err(io::Error::from_raw_os_error(3).into());
        }
        let (input, input_allocation) = encode_root_path_name(path, work)?;
        let result = normalize_root_wide_name(&input, work);
        drop(input);
        work.release(input_allocation)?;
        let (name, allocation) = result?;
        let file = work.attempt_io(READ, || {
            open_windows_root_name(
                &name,
                windows::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            )
        });
        drop(name);
        work.release(allocation)?;
        file?
    };
    let directory = Dir::from_std_file(file);
    let identity = directory_identity(&directory, work)?;
    Ok((directory, identity))
}
