use super::*;

pub(super) fn atomic_write(path: &Path, bytes: &[u8], immutable: bool) -> Result<(), Error> {
    reject_reparse_ancestors(path)?;
    if let Ok(existing) = fs::read(path) {
        if immutable && existing != bytes {
            return Err(Error::Invalid(format!(
                "refusing to rewrite immutable file {}",
                path.display()
            )));
        }
        if immutable {
            return Ok(());
        }
    }
    let parent = path.parent().unwrap_or(path);
    let mut file = publication_temp_file(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    if immutable {
        file.persist_noclobber(path)
            .map_err(|error| Error::Io(error.error))?;
    } else {
        file.persist(path).map_err(|error| Error::Io(error.error))?;
    }
    Ok(())
}

pub(super) fn reject_reparse_ancestors(path: &Path) -> Result<(), Error> {
    let mut current = Some(path);
    while let Some(candidate) = current {
        match fs::symlink_metadata(candidate) {
            Ok(metadata) => {
                if is_reparse_or_symlink(&metadata) {
                    return Err(Error::Invalid(format!(
                        "path contains a reparse point or symlink: {}",
                        candidate.display()
                    )));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Invalid(format!(
                    "cannot inspect path ancestor {}: {error}",
                    candidate.display()
                )));
            }
        }
        current = candidate.parent();
    }
    Ok(())
}

pub(super) fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

pub(super) fn publication_temp_file(
    parent: &Path,
) -> Result<tempfile::NamedTempFile, std::io::Error> {
    tempfile::Builder::new().prefix(".tmp-").tempfile_in(parent)
}
