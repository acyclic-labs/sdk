//! Directories and small records below a held root. Every step is relative
//! to a held directory handle, so replacing the root's path never redirects
//! one.
use super::*;
use std::io::{Read as _, Write as _};
#[cfg(any(test, feature = "native-mount"))]
use std::path::PathBuf;

impl HostRoot {
    /// A second handle on the same held directory.
    pub(crate) fn try_clone<W: NativeHostWork>(&self, work: &mut W) -> Result<Self, W::Error> {
        Ok(Self {
            directory: clone_directory(&self.directory, work)?,
            identity: self.identity,
        })
    }

    /// The directory at `relative` below this root, each missing component
    /// created in its held parent, and never through a link.
    pub(crate) fn create_directories<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let mut directory = clone_directory(&self.directory, work)?;
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(W::invalid_request());
            };
            #[cfg(unix)]
            let child = directory_path(&directory, Path::new(name), true, work)?;
            #[cfg(windows)]
            let child = with_wide_path(Path::new(name), work, |name, work| {
                directory_path(&directory, name, true, work)
            })?;
            directory = child;
        }
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }

    /// The `N` bytes published at `leaf` in this directory, after publishing
    /// `candidate` there if nothing is. A publication writes a unique
    /// temporary leaf, syncs it, renames it into place without replacement
    /// and syncs the directory: a kill at any step leaves no record or the
    /// complete one, and concurrent publishers all return the one that won.
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn read_or_publish<const N: usize, W: NativeHostWork<Error = DemandError>>(
        &self,
        leaf: &Path,
        candidate: [u8; N],
        work: &mut W,
    ) -> Result<[u8; N], DemandError> {
        loop {
            match read_record(&self.directory, leaf, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
                record => return record,
            }
            let temporary = PathBuf::from(format!(".{}.next", uuid::Uuid::new_v4().simple()));
            match publish_record(&self.directory, &temporary, leaf, &candidate, work) {
                // Another publisher won; its record is the one.
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::AlreadyExists) => {}
                published => return published.map(|()| candidate),
            }
        }
    }
}

#[cfg(any(test, feature = "native-mount"))]
fn read_record<const N: usize, W: NativeHostWork<Error = DemandError>>(
    directory: &Dir,
    leaf: &Path,
    work: &mut W,
) -> Result<[u8; N], DemandError> {
    let file = open_leaf(directory, leaf, &NativeOpenOptions::Read, work)?;
    let mut bytes = Vec::with_capacity(N + 1);
    file.take(N as u64 + 1).read_to_end(&mut bytes)?;
    <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "published record is incomplete").into()
    })
}

#[cfg(any(test, feature = "native-mount"))]
fn publish_record<W: NativeHostWork<Error = DemandError>>(
    directory: &Dir,
    temporary: &Path,
    leaf: &Path,
    bytes: &[u8],
    work: &mut W,
) -> Result<(), DemandError> {
    #[cfg(test)]
    let temporary = if publish_probe::torn() {
        leaf
    } else {
        temporary
    };
    let file = open_leaf(directory, temporary, &NativeOpenOptions::CreateNew, work)?;
    let result = (|| -> Result<(), DemandError> {
        let mut file = file;
        #[cfg(test)]
        publish_probe::step(0)?;
        #[cfg(test)]
        let bytes = {
            let (first, rest) = bytes.split_at(bytes.len() / 2);
            file.write_all(first)?;
            publish_probe::step(1)?;
            rest
        };
        file.write_all(bytes)?;
        sync_file_held(&file, work)?;
        // Closed before the rename, which a host may refuse for an open file.
        drop(file);
        #[cfg(test)]
        publish_probe::step(2)?;
        #[cfg(test)]
        if publish_probe::torn() {
            return Ok(());
        }
        rename_no_replace(directory, temporary, leaf, work)?;
        #[cfg(test)]
        publish_probe::step(3)?;
        sync_directory_held(directory, work)
    })();
    #[cfg(test)]
    if publish_probe::killed(&result) {
        // A killed process runs no cleanup.
        return result;
    }
    if result.is_err() {
        let _ = remove_leaf(directory, temporary, false, work);
    }
    result
}

#[cfg(any(test, feature = "native-mount"))]
fn rename_no_replace<W: NativeHostWork>(
    directory: &Dir,
    from: &Path,
    to: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    let mode = acyclic_native_runtime::RenameMode::NoReplace;
    #[cfg(unix)]
    return rename_held(directory, from, directory, to, mode, None, work);
    #[cfg(windows)]
    with_wide_path(from, work, |from, work| {
        with_wide_path(to, work, |to, work| {
            rename_held(directory, from, directory, to, mode, None, work)
        })
    })
}

/// Simulated kills at each durable step of a record publication.
#[cfg(test)]
pub(crate) mod publish_probe {
    use std::cell::Cell;

    /// The steps a kill can follow: created, half written, synced, renamed.
    #[cfg(target_os = "linux")]
    pub(crate) const STEPS: usize = 4;
    const KILLED: &str = "killed during record publication";

    thread_local! {
        static KILL_AT: Cell<Option<usize>> = const { Cell::new(None) };
        static TORN: Cell<bool> = const { Cell::new(false) };
    }

    /// Kills the next publication on this thread after `step`.
    #[cfg(target_os = "linux")]
    pub(crate) fn kill_at(step: Option<usize>) {
        KILL_AT.set(step);
    }

    /// Negative control: publish by writing the record in place.
    #[cfg(target_os = "linux")]
    pub(crate) fn tear(torn: bool) {
        TORN.set(torn);
    }

    pub(super) fn torn() -> bool {
        TORN.get()
    }

    pub(super) fn step(step: usize) -> Result<(), crate::demand::DemandError> {
        if KILL_AT.get() == Some(step) {
            KILL_AT.set(None);
            return Err(std::io::Error::other(KILLED).into());
        }
        Ok(())
    }

    pub(super) fn killed(result: &Result<(), crate::demand::DemandError>) -> bool {
        matches!(result, Err(crate::demand::DemandError::Io(error)) if error.to_string() == KILLED)
    }
}
