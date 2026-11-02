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
        if existing == bytes {
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
    sdk_file_identity::reject_linked_ancestors(
        path,
        |candidate| {
            Error::Invalid(format!(
                "path contains a reparse point or symlink: {}",
                candidate.display()
            ))
        },
        |candidate, error| {
            Error::Invalid(format!(
                "cannot inspect path ancestor {}: {error}",
                candidate.display()
            ))
        },
    )
}

pub(super) fn publication_temp_file(
    parent: &Path,
) -> Result<tempfile::NamedTempFile, std::io::Error> {
    tempfile::Builder::new().prefix(".tmp-").tempfile_in(parent)
}
