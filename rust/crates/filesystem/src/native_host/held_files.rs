//! Directories and small records below a held root. Every step is relative
//! to a held directory handle, so replacing the root's path never redirects
//! one.
use super::*;
use std::io::Write as _;
#[cfg(any(test, feature = "native-mount"))]
use std::path::PathBuf;

#[cfg(test)]
#[cfg(all(target_os = "linux", acyclic_fs_native_loom))]
pub(crate) mod models;
#[cfg(any(test, feature = "native-mount"))]
mod record;

impl HostRoot {
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn remove_record_file<W: NativeHostWork>(
        &self,
        leaf: &Path,
        work: &mut W,
    ) -> Result<(), W::Error> {
        remove_leaf(&self.directory, leaf, false, work)?;
        sync_directory_held(&self.directory, work)
    }

    /// Publishes variable-length immutable bytes using the same held-directory
    /// transaction as incarnation records. Existing bytes must agree exactly.
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn publish_immutable<W: NativeHostWork<Error = DemandError>>(
        &self,
        leaf: &Path,
        bytes: &[u8],
        work: &mut W,
    ) -> Result<(), DemandError> {
        use std::io::Read as _;
        loop {
            match open_leaf(&self.directory, leaf, &NativeOpenOptions::Read, work) {
                Ok(mut file) => {
                    let mut offset = 0;
                    let mut chunk = [0; 4096];
                    loop {
                        let count = file.read(&mut chunk)?;
                        if count == 0 {
                            break;
                        }
                        if bytes.get(offset..offset + count) != chunk.get(..count) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "immutable publication changed",
                            )
                            .into());
                        }
                        offset += count;
                    }
                    if offset != bytes.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "immutable publication length changed",
                        )
                        .into());
                    }
                    return sync_directory_held(&self.directory, work);
                }
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
                Err(error) => return Err(error),
            }
            let temporary =
                PathBuf::from(format!(".{}.next", crate::identity::fresh_uuid().simple()));
            match publish_record(&self.directory, &temporary, leaf, bytes, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::AlreadyExists) => {}
                result => return result,
            }
        }
    }

    pub(crate) fn open_directory<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        #[cfg(unix)]
        let directory = directory_path(&self.directory, relative, false, work)?;
        #[cfg(windows)]
        let directory = with_wide_path(relative, work, |relative, work| {
            directory_path(&self.directory, relative, false, work)
        })?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }

    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn open_record_file<W: NativeHostWork>(
        &self,
        leaf: &Path,
        create: bool,
        work: &mut W,
    ) -> Result<std::fs::File, W::Error> {
        let options = if create {
            NativeOpenOptions::ReadWriteCreate
        } else {
            NativeOpenOptions::Read
        };
        open_leaf(&self.directory, leaf, &options, work)
    }

    pub(crate) fn sync_directory<W: NativeHostWork>(&self, work: &mut W) -> Result<(), W::Error> {
        sync_directory_held(&self.directory, work)
    }

    pub(crate) fn sync_record_file<W: NativeHostWork>(
        &self,
        file: &std::fs::File,
        work: &mut W,
    ) -> Result<(), W::Error> {
        sync_file_held(file, work)
    }

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
        let mut held: Option<(Dir, Option<NativeRootIdentity>)> = None;
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(W::invalid_request());
            };
            let directory = held.as_ref().map_or(&self.directory, |held| &held.0);
            #[cfg(test)]
            let duplicate = if publish_probe::duplicate_parents() {
                Some(clone_directory(directory, work)?)
            } else {
                None
            };
            #[cfg(test)]
            let directory = duplicate.as_ref().unwrap_or(directory);
            work.started_io(ITEM)?;
            #[cfg(unix)]
            let child = (open_unix_directory(directory, name, true, work)?, None);
            #[cfg(windows)]
            let child = with_wide_path(Path::new(name), work, |name, work| {
                open_windows_directory(directory, name, true, work)
                    .map(|(directory, identity)| (directory, Some(identity)))
            })?;
            // Even an existing child may have just been created by another
            // opener. Persist its name before returning a capability whose
            // records callers may use as durable identity.
            #[cfg(test)]
            let sync = !publish_probe::skip_parent_sync();
            #[cfg(not(test))]
            let sync = true;
            if sync {
                sync_directory_held(directory, work)?;
            }
            held = Some(child);
        }
        let Some((directory, identity)) = held else {
            return self.try_clone(work);
        };
        let identity = match identity {
            Some(identity) => identity,
            None => directory_identity(&directory, work)?,
        };
        Ok(Self {
            directory,
            identity,
        })
    }

    /// The `N` bytes published at `leaf` in this directory, after publishing
    /// a lazily generated candidate there if nothing is. A publication writes
    /// a unique temporary leaf, syncs it, renames it into place without replacement
    /// and syncs the directory: a kill at any step leaves no record or the
    /// complete one, and concurrent publishers all return the one that won.
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn read_or_publish<const N: usize, W: NativeHostWork<Error = DemandError>>(
        &self,
        leaf: &Path,
        mut candidate: impl FnMut() -> Result<[u8; N], DemandError>,
        work: &mut W,
    ) -> Result<[u8; N], DemandError> {
        #[cfg(test)]
        if publish_probe::eager_candidates() {
            candidate()?;
        }
        loop {
            match read_record(&self.directory, leaf, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
                Ok(record) => {
                    // The winner may have renamed but not synced yet. A
                    // reader must establish durability before using the token.
                    #[cfg(test)]
                    let sync = !publish_probe::skip_reader_sync();
                    #[cfg(not(test))]
                    let sync = true;
                    if sync {
                        sync_directory_held(&self.directory, work)?;
                    }
                    return Ok(record);
                }
                Err(error) => return Err(error),
            }
            let candidate = candidate()?;
            let temporary =
                PathBuf::from(format!(".{}.next", crate::identity::fresh_uuid().simple()));
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
    #[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
    let kernel = models::current();
    #[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
    let _atomic_read = kernel.as_ref().map(|kernel| kernel.state.lock().unwrap());
    let mut file = open_leaf(directory, leaf, &NativeOpenOptions::Read, work)?;
    record::read(&mut file).map_err(Into::into)
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
        publish_probe::after_rename();
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
    #[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
    let kernel = models::current();
    #[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
    let mut atomic_rename = kernel.as_ref().map(|kernel| kernel.state.lock().unwrap());
    let mode = acyclic_native_runtime::RenameMode::NoReplace;
    #[cfg(unix)]
    let result = rename_held(directory, from, directory, to, mode, None, work);
    #[cfg(windows)]
    let result = with_wide_path(from, work, |from, work| {
        with_wide_path(to, work, |to, work| {
            rename_held(directory, from, directory, to, mode, None, work)
        })
    });
    #[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
    if result.is_ok()
        && let Some(state) = atomic_rename.as_mut()
    {
        **state = false;
    }
    result
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
        static SKIP_PARENT_SYNC: Cell<bool> = const { Cell::new(false) };
        static SKIP_READER_SYNC: Cell<bool> = const { Cell::new(false) };
        static DUPLICATE_PARENTS: Cell<bool> = const { Cell::new(false) };
        static EAGER_CANDIDATES: Cell<bool> = const { Cell::new(false) };
        static AFTER_RENAME: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn generate_candidates_eagerly(eager: bool) {
        EAGER_CANDIDATES.set(eager);
    }

    pub(super) fn eager_candidates() -> bool {
        EAGER_CANDIDATES.get()
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn duplicate_parent_handles(duplicate: bool) {
        DUPLICATE_PARENTS.set(duplicate);
    }

    pub(super) fn duplicate_parents() -> bool {
        DUPLICATE_PARENTS.get()
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn omit_parent_sync(omit: bool) {
        SKIP_PARENT_SYNC.set(omit);
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn omit_reader_sync(omit: bool) {
        SKIP_READER_SYNC.set(omit);
    }

    pub(super) fn skip_parent_sync() -> bool {
        SKIP_PARENT_SYNC.get()
    }
    pub(super) fn skip_reader_sync() -> bool {
        SKIP_READER_SYNC.get()
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn before_directory_sync(hook: impl FnOnce() + 'static) {
        AFTER_RENAME.set(Some(Box::new(hook)));
    }

    pub(super) fn after_rename() {
        if let Some(hook) = AFTER_RENAME.take() {
            hook();
        }
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
