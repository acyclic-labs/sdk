//! Directories held for name lookup only. Holding one needs search
//! (traverse) permission, never read (list) permission, so an ancestor a
//! process may cross but not list still anchors no-follow lookups below it.
use super::*;

#[cfg(unix)]
const fn search_flags() -> i32 {
    #[cfg(target_os = "linux")]
    let access = libc::O_PATH | libc::O_DIRECTORY;
    #[cfg(target_os = "macos")]
    let access = libc::O_SEARCH;
    access | libc::O_NOFOLLOW | libc::O_CLOEXEC
}

#[cfg(windows)]
const SEARCH_ACCESS: fs::FILE_ACCESS_RIGHTS =
    fs::FILE_ACCESS_RIGHTS(fs::FILE_TRAVERSE.0 | fs::FILE_READ_ATTRIBUTES.0 | fs::SYNCHRONIZE.0);

#[cfg(unix)]
fn search_child<W: NativeHostWork>(
    parent: &Dir,
    name: &OsStr,
    work: &mut W,
) -> Result<Dir, W::Error> {
    unix_name(name, work, |name, work| {
        open_unix(parent.as_raw_fd(), name, search_flags(), work).map(Dir::from_std_file)
    })
}

#[cfg(windows)]
fn search_child<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<Dir, W::Error> {
    open_windows_at(
        parent,
        name,
        SEARCH_ACCESS,
        nt::FILE_OPEN,
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
        current = search_child(&current, name, work)?;
    }
    #[cfg(windows)]
    with_wide_path(relative, work, |path, work| {
        for name in path.split(|unit| *unit == 92).filter(|_| !path.is_empty()) {
            work.started_io(ITEM)?;
            current = search_child(&current, name, work)?;
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
    /// Holds the directory at `path` for search only. Intermediate path
    /// components need search permission as for any open; the directory
    /// itself is never opened for listing, and a final link is refused.
    pub(crate) fn open_search<W: NativeHostWork>(
        path: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        #[cfg(unix)]
        let file = unix_name(path.as_os_str(), work, |name, work| {
            open_unix(libc::AT_FDCWD, name, search_flags(), work)
        })?;
        #[cfg(windows)]
        let file = {
            if path.as_os_str().is_empty() {
                return Err(io::Error::from_raw_os_error(3).into());
            }
            with_root_name(path, work, |name, work| {
                work.attempt_io(READ, || open_windows_root_name(name, SEARCH_ACCESS))
            })?
        };
        let directory = Dir::from_std_file(file);
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }

    /// The descendant directory at `relative`, held for search only.
    pub(crate) fn open_search_directory<W: NativeHostWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        let directory = search_path(&self.directory, relative, work)?;
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
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
                    &self.directory,
                    relative.parent().unwrap_or(Path::new("")),
                    work,
                )?,
                leaf,
            ),
            // An empty path names this directory, reopened through its handle.
            None if relative.as_os_str().is_empty() => {
                (clone_directory(&self.directory, work)?, OsStr::new("."))
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
        Ok(Self {
            directory,
            identity,
        })
    }

    /// The device and object id of whatever the leaf names once every link
    /// is followed, resolved by the kernel relative to this held directory.
    pub(crate) fn resolved_identity<W: NativeHostWork>(
        &self,
        leaf: &Path,
        work: &mut W,
    ) -> Result<NativeRootIdentity, W::Error> {
        require_leaf(leaf)?;
        #[cfg(unix)]
        return unix_name(leaf.as_os_str(), work, |name, work| {
            let stat = retry_io(work, READ, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: held descriptor, terminated name and complete output remain live.
                if unsafe {
                    libc::fstatat(
                        self.directory.as_raw_fd(),
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
        with_wide_path(leaf, work, |leaf, work| {
            let name = windows_open_name(leaf, work)?;
            let file = open_windows_named_with_options(
                &self.directory,
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
