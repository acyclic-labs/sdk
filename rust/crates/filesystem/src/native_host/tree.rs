use super::*;

impl HostRoot {
    pub(crate) fn open_child<W: SourceDemandReadWork>(
        &self,
        name: &Path,
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        #[cfg(unix)]
        let directory = open_unix_directory_path(&self.directory, name, false, work)?;
        #[cfg(windows)]
        let directory = { self.open_dir_held(name, work)? };
        work.started_io(READ)?;
        let identity = held_root_identity(&directory)?;
        Ok(HostRoot {
            directory,
            identity,
        })
    }
    pub(crate) fn open_file_with<W: SourceDemandReadWork>(
        &self,
        name: &Path,
        options: &NativeOpenOptions,
        work: &mut W,
    ) -> Result<File, DemandError> {
        open_leaf(&self.directory, name, options, work)
    }
    pub(crate) fn rename_to<W: SourceDemandReadWork>(
        &self,
        name: &Path,
        destination: &Self,
        destination_name: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        require_leaf(name)?;
        require_leaf(destination_name)?;
        #[cfg(unix)]
        {
            unix_name(name.as_os_str(), work, |name, work| {
                unix_name(
                    destination_name.as_os_str(),
                    work,
                    |destination_name, work| {
                        // SAFETY: held parent capabilities and terminated leaf names are live.
                        unix_mutation(work, || unsafe {
                            libc::renameat(
                                self.directory.as_raw_fd(),
                                name.as_ptr(),
                                destination.directory.as_raw_fd(),
                                destination_name.as_ptr(),
                            )
                        })
                    },
                )
            })
        }
        #[cfg(windows)]
        {
            with_wide_path(name, work, |name, work| {
                with_wide_path(destination_name, work, |destination_name, work| {
                    rename_windows_leaf(
                        &self.directory,
                        name,
                        &destination.directory,
                        destination_name,
                        work,
                    )
                })
            })
        }
    }
    pub(crate) fn sync<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        sync_directory_held(&self.directory, work)
    }
    pub(crate) fn remove<W: SourceDemandReadWork>(
        &self,
        name: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let stat = match self.stat(name, work) {
            Ok(stat) => stat,
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        self.remove_identity(name, stat.identity()?, work)
    }
    pub(crate) fn remove_identity<W: SourceDemandReadWork>(
        &self,
        name: &Path,
        expected: NativeRootIdentity,
        work: &mut W,
    ) -> Result<(), DemandError> {
        require_leaf(name)?;
        #[cfg(unix)]
        {
            let stat = self.stat(name, work)?;
            if stat.identity()? != expected {
                return Err(DemandError::StaleVersion);
            }
            if stat.file_type().is_dir() && !stat.file_type().is_symlink() {
                let root = self.open_child(name, work)?;
                if root.identity() != expected {
                    return Err(DemandError::StaleVersion);
                }
                let child = root;
                let (mut reader, buffer_bytes) = child.read_dir_stats(Path::new(""), work)?;
                let result: Result<(), DemandError> = (|| {
                    while let Some((entry, name_bytes)) = reader.next(work)? {
                        let result = child.remove(Path::new(&entry.name), work);
                        drop(entry);
                        work.release(name_bytes)?;
                        result?;
                    }
                    Ok(())
                })();
                drop(reader);
                work.release(buffer_bytes)?;
                result?;
                drop(child);
            }
            if self.stat(name, work)?.identity()? != expected {
                return Err(DemandError::StaleVersion);
            }
            remove_leaf(
                &self.directory,
                name,
                stat.file_type().is_dir() && !stat.file_type().is_symlink(),
                work,
            )
        }
        #[cfg(windows)]
        {
            with_wide_path(name, work, |name, work| {
                self.remove_identity_wide(name, expected, work)
            })
        }
    }
    #[cfg(windows)]
    pub(super) fn remove_identity_wide<W: SourceDemandReadWork>(
        &self,
        name: &[u16],
        expected: NativeRootIdentity,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let root = self;
        let stat = root.stat_wide(name, work)?;
        if stat.identity()? != expected {
            return Err(DemandError::StaleVersion);
        }
        let real_directory = stat.is_dir()
            && stat.file_attributes()
                & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0
                == 0;
        if real_directory {
            let directory = root
                .open_dir_held_wide(name, work)
                .map(|(directory, _)| directory)?;
            let identity = directory_identity(&directory, work)?;
            let child = HostRoot {
                directory,
                identity,
            };
            let child_root = &child;
            if child_root.identity() != expected {
                return Err(DemandError::StaleVersion);
            }
            let (mut reader, bytes) = child_root.read_dir_stats_wide(&[], work)?;
            let result: Result<(), DemandError> = (|| {
                while let Some((entry, bytes)) = reader.next_wide(work)? {
                    let result = (|| {
                        let stat = child_root.stat_wide(&entry.name, work)?;
                        child.remove_identity_wide(&entry.name, stat.identity()?, work)
                    })();
                    drop(entry);
                    work.release(bytes)?;
                    result?;
                }
                Ok(())
            })();
            drop(reader);
            work.release(bytes)?;
            result?;
        }
        if root.stat_wide(name, work)?.identity()? != expected {
            return Err(DemandError::StaleVersion);
        }
        remove_windows_leaf(&self.directory, name, stat.is_dir(), None, work)
    }
}
