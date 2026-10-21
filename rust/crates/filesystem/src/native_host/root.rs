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
    pub(crate) fn open_dir_held<W: NativeHostWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<Dir, W::Error> {
        directory_path(&self.directory, path, false, work)
    }
    pub(crate) fn read_dir_stats<W: NativeHostWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        read_dir_held(&self.open_dir_held(path, work)?, work)
    }
    pub(crate) fn stat<W: NativeHostWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(windows)]
        if path.is_empty() || path == [46] {
            return stat_windows_file(
                &self.directory,
                u32::try_from(self.identity.device).map_err(|_| W::invalid_request())?,
                work,
            );
        }
        #[cfg(unix)]
        if path.as_os_str().is_empty() {
            return stat_at(self.directory.as_raw_fd(), c"", work);
        }
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            #[cfg(windows)]
            {
                let file = open_windows_at(
                    parent,
                    leaf,
                    fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                    nt::FILE_OPEN,
                    nt::FILE_SYNCHRONOUS_IO_NONALERT,
                    work,
                )?;
                stat_windows_file(
                    &file,
                    u32::try_from(self.identity.device).map_err(|_| W::invalid_request())?,
                    work,
                )
            }
            #[cfg(unix)]
            {
                stat_at(parent.as_raw_fd(), leaf, work)
            }
        })
    }
    #[cfg(all(feature = "native-watch", not(target_os = "linux")))]
    pub(crate) fn stat_source<W: SourceDemandReadWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<HostStat, DemandError> {
        #[cfg(unix)]
        return self.stat(path, work);
        #[cfg(windows)]
        {
            if path.is_empty() || path == [46] {
                return self.stat(path, work);
            }
            with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                let volume =
                    u32::try_from(self.identity.device).map_err(|_| W::invalid_request())?;
                let name = windows_open_name(leaf, work)?;
                let fallback = |work: &mut W| {
                    let file = open_windows_named(
                        parent,
                        &name,
                        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                        nt::FILE_OPEN,
                        nt::FILE_SYNCHRONOUS_IO_NONALERT,
                        work,
                    )?;
                    stat_windows_file(&file, volume, work)
                };
                // With two reads left, use the held-leaf route without a speculative query.
                if name.Length == 0 || work.remaining()?.backend_read_operations == 2 {
                    return fallback(work);
                }
                query_windows_stat(
                    work,
                    READ,
                    || {
                        let metadata = stat_windows_name(parent, &name, volume, true)?;
                        // A name query need not enforce the synchronous-open right.
                        Ok(metadata
                            .filter(|stat| stat.raw.EffectiveAccess & fs::SYNCHRONIZE.0 != 0))
                    },
                    fallback,
                )
            })
        }
    }
    #[cfg(feature = "native-watch")]
    pub(crate) fn stat_source_metadata<W: SourceDemandReadWork>(
        &self,
        path: &NativePath,
        work: &mut W,
    ) -> Result<HostStat, DemandError> {
        #[cfg(test)]
        #[cfg(windows)]
        if stat_probe::old() {
            return self.stat_source(path, work);
        }
        #[cfg(target_os = "linux")]
        {
            if path.as_os_str().is_empty() {
                return stat_at_metadata(self.directory.as_raw_fd(), c"", work);
            }
            with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                stat_at_metadata(parent.as_raw_fd(), leaf, work)
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.stat_source(path, work)
        }
    }
    #[cfg(any(
        test,
        feature = "native-mount",
        feature = "native-watch",
        not(target_os = "linux")
    ))]
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
            stat_windows_file(
                file,
                u32::try_from(self.identity.device).map_err(|_| W::invalid_request())?,
                work,
            )
        }
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn stat_file_metadata<W: NativeHostWork>(
        &self,
        file: &File,
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        #[cfg(test)]
        #[cfg(any(target_os = "linux", windows))]
        if stat_probe::duplicate() {
            self.stat_file(file, work)?;
        }
        #[cfg(test)]
        #[cfg(windows)]
        if stat_probe::old() {
            return self.stat_file(file, work);
        }
        #[cfg(target_os = "linux")]
        {
            super::stat_file_metadata(file, work)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.stat_file(file, work)
        }
    }
    pub(crate) fn open_file<W: NativeHostWork>(
        &self,
        path: &NativePath,
        #[cfg(windows)] synchronization: nt::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            #[cfg(windows)]
            let file = open_windows_at(
                parent,
                leaf,
                fs::FILE_GENERIC_READ,
                nt::FILE_OPEN,
                nt::FILE_NON_DIRECTORY_FILE | synchronization,
                work,
            )?;
            #[cfg(unix)]
            let file = {
                let flags = libc::O_RDONLY
                    | libc::O_CLOEXEC
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_NOCTTY;
                #[cfg(target_os = "linux")]
                let flags = flags | libc::O_NOATIME;
                open_unix(parent.as_raw_fd(), leaf, flags, work)?
            };
            Ok(cap_std::fs::File::from_std(file))
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
        if stat_at(source.as_raw_fd(), c"", work)?.file_type()
            != crate::demand::SourceNodeKind::RegularFile
        {
            return Err(DemandError::NotRegularFile);
        }
        with_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
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
    /// Opens a held-root output with the access needed by FlushFileBuffers.
    #[cfg(all(windows, any(test, feature = "native-mount")))]
    pub(crate) fn open_file_for_sync<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            open_windows_at(
                parent,
                leaf,
                fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE,
                nt::FILE_OPEN,
                nt::FILE_NON_DIRECTORY_FILE,
                work,
            )
            .map(cap_std::fs::File::from_std)
        })
    }
    #[cfg(all(windows, any(test, feature = "native-mount")))]
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
        if (stat.file_type() != crate::demand::SourceNodeKind::RegularFile)
            || stat.raw.FileAttributes & fs::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
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
    #[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
    pub(crate) fn open_linux_metadata_target<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<File, DemandError> {
        if path.as_os_str().is_empty() {
            return open_unix(
                self.directory.as_raw_fd(),
                c".",
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                work,
            );
        }
        with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
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
        let open = |parent: &Dir, leaf: &[u16], work: &mut W| {
            open_windows_at(
                parent,
                leaf,
                fs::FILE_READ_ATTRIBUTES | fs::FILE_WRITE_ATTRIBUTES | fs::SYNCHRONIZE,
                nt::FILE_OPEN,
                nt::FILE_SYNCHRONOUS_IO_NONALERT
                    | if path.is_empty() {
                        nt::FILE_DIRECTORY_FILE
                    } else {
                        nt::NTCREATEFILE_CREATE_OPTIONS(0)
                    },
                work,
            )
        };
        let file = if path.is_empty() {
            open(&self.directory, &[46], work)?
        } else {
            with_parent_leaf(&self.directory, path, work, open)?
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
        with_root_name(path, work, |name, work| {
            work.attempt_io(READ, || open_windows_root_name(name, fs::FILE_GENERIC_READ))
        })?
    };
    let directory = Dir::from_std_file(file);
    let identity = directory_identity(&directory, work)?;
    Ok((directory, identity))
}

#[cfg(windows)]
pub(super) fn wide_parent_leaf(path: &[u16]) -> Result<(&[u16], &[u16]), DemandError> {
    let mut parts = path.rsplitn(2, |unit| *unit == 92);
    let leaf = parts.next().ok_or(DemandError::InvalidRequest)?;
    let parent = parts.next().unwrap_or_default();
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
    for unit in name {
        work.started_io(ITEM)?;
        if unit == 0 {
            break;
        }
    }
    Ok(name.starts_with(&[78, 84, 70, 83, 0]))
}

/// Allows attributes removed by `ProjFS` hydration.
#[cfg(windows)]
pub(crate) const fn same_windows_attributes(before: u32, after: u32) -> bool {
    const MARKS: u32 = 0x0000_0200 | 0x0000_0400 | 0x0004_0000 | 0x0040_0000;
    let marks = before & MARKS;
    let ignored = marks | if before & 0x0040_0000 != 0 { 0x20 } else { 0 };
    before & !ignored == after & !ignored && after & MARKS & !marks == 0
}
