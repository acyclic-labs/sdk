//! Directories held for name lookup only. Holding one needs search
//! (traverse) permission, never read (list) permission, so an ancestor a
//! process may cross but not list still anchors no-follow lookups below it.
use super::*;

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn search_creation_retains_its_root_and_refuses_directory_aliases() -> Result<(), DemandError> {
        use std::os::unix::fs::PermissionsExt as _;
        let fixture = tempfile::tempdir()?;
        let original = fixture.path().join("original");
        let moved = fixture.path().join("moved");
        let outside = fixture.path().join("outside");
        std::fs::create_dir(&original)?;
        std::fs::create_dir(&outside)?;
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o300))?;
        let created = (|| {
            let root = crate::hold_search_root(&original)?;
            std::fs::rename(&original, &moved)?;
            std::os::unix::fs::symlink(&outside, &original)?;
            root.create_directories(Path::new("a/b"))?;
            std::os::unix::fs::symlink(&outside, moved.join("alias"))?;
            assert!(root.create_directories(Path::new("alias/escaped")).is_err());
            Ok::<_, DemandError>(())
        })();
        // Restore listing permission before reporting any failure or cleanup.
        let retained = if moved.exists() { &moved } else { &original };
        std::fs::set_permissions(retained, std::fs::Permissions::from_mode(0o700))?;
        created?;
        assert!(moved.join("a/b").is_dir());
        assert!(!outside.join("a").exists());
        assert!(!outside.join("escaped").exists());
        assert!(crate::hold_search_root(&original).is_err());
        Ok(())
    }
}

#[cfg(unix)]
fn search_child<W: NativeHostWork>(
    parent: &Dir,
    name: &OsStr,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    open_unix_directory_with_flags(parent, name, create, directory_metadata_flags(), work)
}

#[cfg(windows)]
fn search_child<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    open_windows_at_without_reparse(
        parent,
        name,
        SEARCH_ACCESS,
        if create {
            nt::FILE_OPEN_IF
        } else {
            nt::FILE_OPEN
        },
        nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )
    .map(Dir::from_std_file)
}

/// The directory at `relative` below `root`, each component opened for
/// search only and never through a link.
fn search_path<W: NativeHostWork>(
    root: &Dir,
    relative: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let mut current = clone_directory(root, work)?;
    #[cfg(unix)]
    for component in relative.components() {
        work.started_io(ITEM)?;
        let std::path::Component::Normal(name) = component else {
            return Err(W::invalid_request());
        };
        current = search_child(&current, name, false, work)?;
    }
    #[cfg(windows)]
    with_wide_path(relative, work, |path, work| {
        for name in path.split(|unit| *unit == 92).filter(|_| !path.is_empty()) {
            work.started_io(ITEM)?;
            current = search_child(&current, name, false, work)?;
        }
        Ok(())
    })?;
    Ok(current)
}

/// The device and object id of an open file.
pub(crate) fn file_identity<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    #[cfg(unix)]
    {
        let stat = fstat(file.as_raw_fd(), c"", work)?;
        Ok(NativeRootIdentity {
            device: device_id(stat.st_dev),
            object: stat.st_ino,
        })
    }
    #[cfg(windows)]
    work.attempt_io(READ, || {
        let info = windows_file_information(file)?;
        Ok(NativeRootIdentity {
            device: u64::from(info.dwVolumeSerialNumber),
            object: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    })
}

impl HostRoot {
    /// Creates missing descendants without acquiring directory-list access.
    /// These disposable names are visible on return; no barrier is requested.
    pub(crate) fn create_search_directories<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        self.create_directories_with(
            relative,
            NativeDurability::Visibility,
            work,
            |directory, name, work| {
                #[cfg(unix)]
                return search_child(directory, name, true, work);
                #[cfg(windows)]
                with_wide_path(Path::new(name), work, |name, work| {
                    search_child(directory, name, true, work)
                })
            },
        )
    }

    /// Holds the directory at `path` for search only. Intermediate path
    /// components need search permission as for any open; the directory
    /// itself is never opened for listing, and a final link is refused.
    pub(crate) fn open_search<W: NativeHostWork>(
        path: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let directory = open_root_directory_handle(
            path,
            DirectoryAccess::Search,
            DirectoryLinks::Refuse,
            work,
        )?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self::from_parts(directory, identity))
    }

    /// The descendant directory at `relative`, held for search only.
    pub(crate) fn open_search_directory<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let directory = search_path(&self.directory(), relative, work)?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self::from_parts(directory, identity))
    }

    /// The descendant directory at `relative`, readable, reached through
    /// search-only intermediate directories and never through a link.
    pub(crate) fn open_directory_searching<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let (parent, leaf) = match relative.file_name() {
            Some(leaf) => (
                search_path(
                    &self.directory(),
                    relative.parent().unwrap_or(Path::new("")),
                    work,
                )?,
                leaf,
            ),
            // An empty path names this directory, reopened through its handle.
            None if relative.as_os_str().is_empty() => {
                (clone_directory(&self.directory(), work)?, OsStr::new("."))
            }
            None => return Err(W::invalid_request()),
        };
        #[cfg(unix)]
        let directory = open_unix_directory(&parent, leaf, false, work)?;
        #[cfg(windows)]
        let directory = with_wide_path(Path::new(leaf), work, |leaf, work| {
            open_windows_directory(&parent, leaf, false, work).map(|(directory, _)| directory)
        })?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self::from_parts(directory, identity))
    }

    /// The device and object id of whatever `path` names once every link is
    /// followed, resolved by the kernel from this held directory. On Unix the
    /// path may hold several components, `..`, or be absolute; on Windows,
    /// where an NT relative open cannot name `..`, it must be one leaf.
    pub(crate) fn resolved_identity<W: NativeHostWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<NativeRootIdentity, W::Error> {
        #[cfg(unix)]
        if path.as_os_str().is_empty() {
            return Err(W::invalid_request());
        }
        #[cfg(windows)]
        require_leaf(path)?;
        #[cfg(unix)]
        return unix_name(path.as_os_str(), work, |name, work| {
            let stat = work.attempt_io(READ, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: held descriptor, terminated name and complete output remain live.
                if unsafe {
                    libc::fstatat(
                        self.directory().as_raw_fd(),
                        name.as_ptr(),
                        stat.as_mut_ptr(),
                        0,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: the successful call initialized the complete record.
                Ok(unsafe { stat.assume_init() })
            })?;
            Ok(NativeRootIdentity {
                device: device_id(stat.st_dev),
                object: stat.st_ino,
            })
        });
        #[cfg(windows)]
        with_wide_path(path, work, |leaf, work| {
            let name = windows_open_name(leaf, work)?;
            let file = open_windows_named_with_options(
                &self.directory(),
                &name,
                fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                nt::FILE_OPEN,
                nt::FILE_SYNCHRONOUS_IO_NONALERT,
                win::OBJ_CASE_INSENSITIVE,
                work,
            )?;
            file_identity(&file, work)
        })
    }
}
