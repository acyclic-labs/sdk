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
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        work: &mut W,
    ) -> Result<Dir, W::Error> {
        #[cfg(windows)]
        {
            let mut directory = clone_directory(&self.directory, work)?;
            if !path.is_empty() {
                for name in path.split(|u| *u == 92) {
                    work.started_io(ITEM)?;
                    directory = open_windows_directory(&directory, name, work)?.0;
                }
            }
            Ok(directory)
        }
        #[cfg(unix)]
        {
            open_unix_directory_path(&self.directory, path, false, work)
        }
    }
    pub(crate) fn read_dir_stats<W: NativeHostWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        read_dir_held(&self.open_dir_held(path, work)?, work)
    }
    pub(crate) fn stat<W: NativeHostWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(windows)]
        {
            let volume = self.volume_serial_number()?;
            if path.is_empty() || path == [46] {
                return stat_windows_file(&self.directory, volume, work);
            }
            let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
            let parent = self.open_dir_held(parent, work)?;
            let file = open_windows_at(
                &parent,
                leaf,
                fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                nt::FILE_OPEN,
                nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?;
            stat_windows_file(&file, volume, work)
        }
        #[cfg(unix)]
        {
            work.check()?;
            if path.as_os_str().is_empty() {
                return stat_at(self.directory.as_raw_fd(), c"", work);
            }
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                stat_at(parent.as_raw_fd(), leaf, work)
            })
        }
    }
    pub(crate) fn stat_file<W: NativeHostWork>(
        &self,
        file: &File,
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(unix)]
        {
            stat_at(file.as_raw_fd(), c"", work)
        }
        #[cfg(windows)]
        {
            stat_windows_file(file, self.volume_serial_number()?, work)
        }
    }
    pub(crate) fn open_file<W: NativeHostWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        #[cfg(windows)] synchronization: nt::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        #[cfg(windows)]
        {
            let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
            let file = open_windows_at(
                &self.open_dir_held(parent, work)?,
                leaf,
                fs::FILE_GENERIC_READ,
                nt::FILE_OPEN,
                nt::FILE_NON_DIRECTORY_FILE | synchronization,
                work,
            )?;
            Ok(cap_std::fs::File::from_std(file))
        }
        #[cfg(unix)]
        {
            work.check()?;
            let flags = libc::O_RDONLY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | libc::O_NOCTTY;
            #[cfg(target_os = "linux")]
            let flags = flags | libc::O_NOATIME;
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                open_unix(parent.as_raw_fd(), leaf, flags, work).map(cap_std::fs::File::from_std)
            })
        }
    }

    #[cfg(unix)]
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &Path,
        maximum: u64,
        work: &mut W,
    ) -> Result<(Vec<u8>, W::Allocation), W::Error> {
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            read_unix_link(parent, leaf, maximum, work)
        })
        .map(|(mut bytes, count, allocation)| {
            bytes.truncate(count);
            (bytes, allocation)
        })
    }
    #[cfg(windows)]
    pub(crate) fn open_root_wide<W: NativeHostWork>(
        name: &[u16],
        work: &mut W,
    ) -> Result<Self, W::Error> {
        check_root_wide_name(name, work)?;
        let directory = Dir::from_std_file(
            work.attempt_io(READ, || open_windows_root_name(name, fs::FILE_GENERIC_READ))?,
        );
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
        if !stat_at(source.as_raw_fd(), c"", work)?
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
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let source = source_root
            .open_file(source, nt::FILE_SYNCHRONOUS_IO_NONALERT, work)?
            .into_std();
        let stat = source_root.stat_file(&source, work)?;
        if !stat.file_type().is_file()
            || stat.file_attributes() & fs::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        {
            return Err(DemandError::InvalidRequest);
        }
        if !clonable_windows_length(stat.len()) {
            return Ok(false);
        }
        let target = self.create_file(
            destination,
            nt::FILE_SYNCHRONOUS_IO_NONALERT | nt::FILE_DELETE_ON_CLOSE,
            work,
        )?;
        let cloned = clone_windows_file(&source, &target, stat.len(), work)?;
        if cloned {
            keep_windows_created_file(&target, work)?;
        }
        Ok(cloned)
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
        let parent = with_wide_path(
            destination.parent().ok_or(io::ErrorKind::InvalidInput)?,
            &mut ordinary_work(),
            |path, work| self.open_dir_held(path, work),
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
    pub(crate) fn open_windows_metadata_target<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<cap_std::fs::File, DemandError> {
        let access = fs::FILE_READ_ATTRIBUTES | fs::FILE_WRITE_ATTRIBUTES | fs::SYNCHRONIZE;
        let file = if path.is_empty() {
            open_windows_at(
                &self.directory,
                &[46],
                access,
                nt::FILE_OPEN,
                nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?
        } else {
            let (parent, leaf) = wide_parent_leaf(path)?;
            open_windows_at(
                &self.open_dir_held(parent, work)?,
                leaf,
                access,
                nt::FILE_OPEN,
                nt::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?
        };
        Ok(cap_std::fs::File::from_std(file))
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
            open_windows_root_name(&name, fs::FILE_GENERIC_READ)
        });
        drop(name);
        work.release(allocation)?;
        file?
    };
    let directory = Dir::from_std_file(file);
    let identity = directory_identity(&directory, work)?;
    Ok((directory, identity))
}

#[cfg(windows)]
pub(super) fn wide_parent_leaf(path: &[u16]) -> Result<(&[u16], &[u16]), DemandError> {
    let index = path.iter().rposition(|u| *u == 92);
    let (parent, leaf) = index.map_or((&[][..], path), |i| (&path[..i], &path[i + 1..]));
    if leaf.is_empty() || path.first() == Some(&92) {
        return Err(DemandError::InvalidRequest);
    }
    Ok((parent, leaf))
}

#[cfg(windows)]
pub(crate) fn reports_every_change<W: NativeHostWork<Error = DemandError>>(
    file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<bool, DemandError> {
    use windows::Win32::{Foundation::HANDLE, Storage::FileSystem::GetVolumeInformationByHandleW};
    let mut name = [0u16; 64];
    work.started_io(READ)?;
    // SAFETY: the held handle and output buffer remain live for the query.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(file.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(&mut name),
        )
    }
    .map_err(io::Error::from)?;
    let mut length = 0;
    for unit in name {
        work.started_io(ITEM)?;
        if unit == 0 {
            break;
        }
        length += 1;
    }
    Ok(name[..length] == [78, 84, 70, 83])
}

/// Allows attributes removed by ProjFS hydration.
#[cfg(windows)]
pub(crate) const fn same_windows_attributes(before: u32, after: u32) -> bool {
    const MARKS: u32 = 0x0000_0200 | 0x0000_0400 | 0x0004_0000 | 0x0040_0000;
    let marks = before & MARKS;
    let ignored = marks | if before & 0x0040_0000 != 0 { 0x20 } else { 0 };
    before & !ignored == after & !ignored && after & MARKS & !marks == 0
}
