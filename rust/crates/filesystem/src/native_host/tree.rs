use super::*;

impl HostRoot {
    pub(crate) fn open_child<W: SourceDemandReadWork>(
        &self,
        name: &NativePath,
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        let directory = self.open_dir_held(name, work)?;
        let identity = directory_identity(&directory, work)?;
        Ok(HostRoot::from_parts(directory, identity))
    }
    pub(crate) fn remove<W: SourceDemandReadWork>(
        &self,
        name: &NativePath,
        expected: Option<NativeRootIdentity>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        require_leaf(name)?;
        #[cfg(windows)]
        windows_leaf_name(name, work)?;
        let stat = match self.stat(name, work) {
            Ok(stat) => stat,
            Err(DemandError::Io(error))
                if expected.is_none() && error.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let expected = expected.unwrap_or(stat.identity());
        if stat.identity() != expected {
            return Err(DemandError::StaleVersion);
        }
        let real_directory = stat.file_type() == crate::demand::SourceNodeKind::Directory;
        #[cfg(windows)]
        let real_directory =
            real_directory && stat.raw.FileAttributes & fs::FILE_ATTRIBUTE_REPARSE_POINT.0 == 0;
        if real_directory {
            let child = self.open_child(name, work)?;
            if child.identity() != expected {
                return Err(DemandError::StaleVersion);
            }
            let (mut reader, allocation) = child.read_dir_stats(
                #[cfg(unix)]
                Path::new(""),
                #[cfg(windows)]
                &[],
                work,
            )?;
            let result = (|| {
                while let Some((entry, allocation)) = reader.next(work)? {
                    #[cfg(unix)]
                    let name = Path::new(&entry.name);
                    #[cfg(windows)]
                    let name = &entry.name[..];
                    let result = match child.stat(name, work) {
                        Ok(stat) => child.remove(name, Some(stat.identity()), work),
                        #[cfg(unix)]
                        Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                            Ok(())
                        }
                        Err(error) => Err(error),
                    };
                    drop(entry);
                    work.release(allocation)?;
                    result?;
                }
                Ok::<_, DemandError>(())
            })();
            drop(reader);
            work.release(allocation)?;
            result?;
        }
        if self.stat(name, work)?.identity() != expected {
            return Err(DemandError::StaleVersion);
        }
        #[cfg(unix)]
        return remove_leaf(&self.directory, name, real_directory, work);
        #[cfg(windows)]
        remove_windows_leaf(
            &self.directory,
            name,
            stat.file_type() == crate::demand::SourceNodeKind::Directory,
            Some(expected),
            work,
        )
    }
}
