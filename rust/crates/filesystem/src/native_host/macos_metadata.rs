use super::*;

/// An exact, no-follow target pinned before metadata work is deferred.
pub(crate) enum MacMetadataTarget {
    Held(File),
    Noop {
        parent: Dir,
        leaf: std::path::PathBuf,
        device: u64,
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
    with_unix_parent_leaf(root, path, work, |parent, leaf, work| {
        let before = macos_stat_at(parent, leaf, work)?;
        let kind = before.st_mode & libc::S_IFMT;
        if !matches!(kind, libc::S_IFREG | libc::S_IFDIR | libc::S_IFIFO) {
            let parent = clone_directory(parent, work)?;
            let bytes = leaf.to_bytes();
            let (leaf, _allocation) = clone_path(Path::new(OsStr::from_bytes(bytes)), work)?;
            let target = MacMetadataTarget::Noop {
                parent,
                leaf,
                device: before.st_dev as u64,
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
        let after = fstat(file.as_raw_fd(), work)?;
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

pub(super) fn macos_stat_at<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<libc::stat, DemandError> {
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    work.started_io(READ)?;
    // SAFETY: the parent, single-component name, and output remain live.
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            leaf.as_ptr(),
            observed.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: successful fstatat initialized the complete structure.
    Ok(unsafe { observed.assume_init() })
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
        use crate::kernel::{FileMetadata, MetadataField};
        let owned = self.logical_owned_bytes();
        let result = (|| {
            work.check()?;
            if !matches!(
                metadata,
                FileMetadata {
                    windows_attributes: MetadataField::Unavailable,
                    posix_flags: MetadataField::Unavailable | MetadataField::Value(0),
                    named_attributes: MetadataField::Unavailable,
                    acl: MetadataField::Unavailable,
                    security_descriptor: MetadataField::Unavailable,
                    changed_ns: MetadataField::Unavailable,
                    ..
                }
            ) {
                return Err(NativeMetadataError("unsupported macOS metadata").into());
            }
            let file = match self {
                Self::Held(file) => file,
                Self::Noop {
                    parent,
                    leaf,
                    device,
                    inode,
                } => {
                    if metadata != FileMetadata::default() {
                        return Err(NativeMetadataError("non-ordinary file kind").into());
                    }
                    let observed = unix_name(leaf.as_os_str(), work, |leaf, work| {
                        macos_stat_at(&parent, leaf, work)
                    })?;
                    if observed.st_dev as u64 != device || observed.st_ino != inode {
                        return Err(io::Error::other("metadata target changed during no-op").into());
                    }
                    return Ok(());
                }
            };
            let fd = file.as_raw_fd();
            let current = fstat(fd, work)?;
            if let MetadataField::Value(mode) = metadata.posix_mode
                && mode & u32::from(libc::S_IFMT) != 0
                && mode & u32::from(libc::S_IFMT) != u32::from(current.st_mode & libc::S_IFMT)
            {
                return Err(NativeMetadataError("posix_mode file kind").into());
            }
            let uid = match metadata.posix_uid {
                MetadataField::Value(value) => value,
                MetadataField::Unavailable => current.st_uid,
            };
            let gid = match metadata.posix_gid {
                MetadataField::Value(value) => value,
                MetadataField::Unavailable => current.st_gid,
            };
            if uid != current.st_uid || gid != current.st_gid {
                // SAFETY: fd pins the admitted inode.
                unix_mutation(work, || unsafe { libc::fchown(fd, uid, gid) })?;
            }
            if let MetadataField::Value(mode) = metadata.posix_mode {
                // Ownership changes can clear setuid/setgid, so restore mode last.
                // SAFETY: fd pins the admitted inode.
                unix_mutation(work, || unsafe {
                    libc::fchmod(fd, (mode & 0o7777) as libc::mode_t)
                })?;
            }
            if let MetadataField::Value(_) = metadata.created_ns {
                let mut attributes = libc::attrlist {
                    bitmapcount: libc::ATTR_BIT_MAP_COUNT,
                    reserved: 0,
                    commonattr: libc::ATTR_CMN_CRTIME,
                    volattr: 0,
                    dirattr: 0,
                    fileattr: 0,
                    forkattr: 0,
                };
                let mut created = unix_metadata_timespec(metadata.created_ns)?;
                // SAFETY: both initialized buffers outlive the synchronous call.
                unix_mutation(work, || unsafe {
                    libc::fsetattrlist(
                        fd,
                        (&raw mut attributes).cast(),
                        (&raw mut created).cast(),
                        std::mem::size_of::<libc::timespec>(),
                        0,
                    )
                })?;
            }
            if matches!(metadata.accessed_ns, MetadataField::Value(_))
                || matches!(metadata.modified_ns, MetadataField::Value(_))
            {
                let times = [
                    unix_metadata_timespec(metadata.accessed_ns)?,
                    unix_metadata_timespec(metadata.modified_ns)?,
                ];
                // SAFETY: fd and two initialized timespecs remain live.
                unix_mutation(work, || unsafe { libc::futimens(fd, times.as_ptr()) })?;
            }
            let observed = fstat(fd, work)?;
            if matches!(metadata.posix_mode, MetadataField::Value(value) if u32::from(observed.st_mode) & 0o7777 != value & 0o7777)
                || matches!(metadata.posix_flags, MetadataField::Value(0) if observed.st_flags != 0)
            {
                return Err(NativeMetadataError("metadata readback mismatch").into());
            }
            for (requested, actual) in [
                (metadata.posix_uid, observed.st_uid),
                (metadata.posix_gid, observed.st_gid),
            ] {
                if matches!(requested, MetadataField::Value(value) if value != actual) {
                    return Err(NativeMetadataError("ownership readback mismatch").into());
                }
            }
            for (requested, seconds, nanos) in [
                (
                    metadata.created_ns,
                    observed.st_birthtime,
                    observed.st_birthtime_nsec,
                ),
                (
                    metadata.accessed_ns,
                    observed.st_atime,
                    observed.st_atime_nsec,
                ),
                (
                    metadata.modified_ns,
                    observed.st_mtime,
                    observed.st_mtime_nsec,
                ),
            ] {
                if matches!(requested, MetadataField::Value(value) if unix_metadata_nanos(seconds, nanos) != Some(value))
                {
                    return Err(NativeMetadataError("timestamp readback mismatch").into());
                }
            }
            Ok(())
        })();
        work.release(owned)?;
        result
    }
}
