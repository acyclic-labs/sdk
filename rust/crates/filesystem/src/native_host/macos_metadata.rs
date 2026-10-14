use super::*;

/// An exact, no-follow target pinned before metadata work is deferred.
pub(crate) enum MacMetadataTarget {
    Held(File),
    Noop {
        parent: Dir,
        leaf: std::path::PathBuf,
        device: libc::dev_t,
        inode: u64,
    },
}

pub(crate) fn open_macos_metadata_target<
    W: NativeHostWork<Error = DemandError, Allocation = u64>,
>(
    root: &Dir,
    path: &Path,
    work: &mut W,
) -> Result<MacMetadataTarget, DemandError> {
    use std::os::unix::ffi::OsStrExt as _;
    if path.as_os_str().is_empty() {
        return Ok(MacMetadataTarget::Held(
            clone_directory(root, work)?.into_std_file(),
        ));
    }
    with_parent_leaf(root, path, work, |parent, leaf, work| {
        let before = fstat(parent.as_raw_fd(), leaf, work)?;
        let kind = before.st_mode & libc::S_IFMT;
        if !matches!(kind, libc::S_IFREG | libc::S_IFDIR | libc::S_IFIFO) {
            let parent = clone_directory(parent, work)?;
            let bytes = leaf.to_bytes();
            let (leaf, _allocation) = clone_path(Path::new(OsStr::from_bytes(bytes)), work)?;
            let target = MacMetadataTarget::Noop {
                parent,
                leaf,
                device: before.st_dev,
                inode: before.st_ino,
            };
            return Ok(target);
        }
        let file = open_unix(
            parent.as_raw_fd(),
            leaf,
            libc::O_RDONLY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | if kind == libc::S_IFIFO {
                    libc::O_EVTONLY
                } else {
                    0
                },
            work,
        )?;
        let after = fstat(file.as_raw_fd(), c"", work)?;
        if before.st_dev != after.st_dev || before.st_ino != after.st_ino {
            return Err(io::Error::other("metadata target changed during admission").into());
        }
        if !matches!(
            after.st_mode & libc::S_IFMT,
            libc::S_IFREG | libc::S_IFDIR | libc::S_IFIFO
        ) {
            return Err(NativeMetadataError("non-ordinary file kind").into());
        }
        Ok(MacMetadataTarget::Held(file))
    })
}

impl MacMetadataTarget {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Held(_) => 0,
            Self::Noop { leaf, .. } => leaf.capacity() as u64,
        }
    }
    pub(crate) fn apply<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
        self,
        metadata: crate::kernel::FileMetadata,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let owned = self.logical_owned_bytes();
        let result = (|| {
            work.check()?;
            match self {
                Self::Held(file) => apply_metadata(&file, metadata, work),
                Self::Noop {
                    parent,
                    leaf,
                    device,
                    inode,
                } => {
                    if metadata != crate::kernel::FileMetadata::default() {
                        return Err(NativeMetadataError("non-ordinary file kind").into());
                    }
                    let observed = unix_name(leaf.as_os_str(), work, |leaf, work| {
                        fstat(parent.as_raw_fd(), leaf, work)
                    })?;
                    if observed.st_dev != device || observed.st_ino != inode {
                        return Err(io::Error::other("metadata target changed during no-op").into());
                    }
                    Ok(())
                }
            }
        })();
        work.release(owned)?;
        result
    }
}
