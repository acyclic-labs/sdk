use super::*;

/// An exact, no-follow target pinned before metadata work is deferred.
#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum MacMetadataTarget {
    Held(File),
    Noop {
        parent: Dir,
        leaf: std::path::PathBuf,
        device: u64,
        inode: u64,
    },
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn open_macos_metadata_target<W: crate::demand::SourceDemandReadWork>(
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
            let copied = WorkCounters {
                bytes_copied: bytes.len() as u64,
                ..WorkCounters::UNCHARGED
            };
            work.allocate_for(bytes.len() as u64, copied)?;
            let leaf = Path::new(OsStr::from_bytes(bytes)).to_path_buf();
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

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn macos_stat_at<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<libc::stat, DemandError> {
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    native_read(work)?;
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

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
impl MacMetadataTarget {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Held(_) => 0,
            Self::Noop { leaf, .. } => leaf.capacity() as u64,
        }
    }
    pub(crate) fn apply<W: crate::demand::SourceDemandReadWork>(
        self,
        metadata: crate::kernel::FileMetadata,
        work: &mut W,
    ) -> Result<(), DemandError> {
        use crate::kernel::MetadataField;
        let owned = self.logical_owned_bytes();
        let result = (|| {
            work.check()?;
            validate_macos_metadata_fields(metadata)?;
            let file = match self {
                Self::Held(file) => file,
                Self::Noop {
                    parent,
                    leaf,
                    device,
                    inode,
                } => {
                    let result = (|| {
                        if metadata != crate::kernel::FileMetadata::default() {
                            return Err(NativeMetadataError("non-ordinary file kind").into());
                        }
                        let observed = unix_name(leaf.as_os_str(), work, |leaf, work| {
                            macos_stat_at(&parent, leaf, work)
                        })?;
                        if observed.st_dev as u64 != device || observed.st_ino != inode {
                            return Err(
                                io::Error::other("metadata target changed during no-op").into()
                            );
                        }
                        Ok(())
                    })();
                    drop(leaf);
                    return result;
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
                native_write(work)?;
                // SAFETY: fd pins the admitted inode; no later path lookup occurs.
                if unsafe { libc::fchown(fd, uid, gid) } != 0 {
                    return Err(io::Error::last_os_error().into());
                }
            }
            if let MetadataField::Value(mode) = metadata.posix_mode {
                let mask = libc::mode_t::try_from(mode & 0o7777)
                    .map_err(|_| NativeMetadataError("posix_mode"))?;
                native_write(work)?;
                // SAFETY: fd pins the admitted inode; fchmod reads no memory.
                if unsafe { libc::fchmod(fd, mask) } != 0 {
                    return Err(io::Error::last_os_error().into());
                }
            }
            apply_macos_timestamp_metadata(fd, metadata, work)?;
            let observed = fstat(fd, work)?;
            if macos_mismatch(metadata.posix_uid, observed.st_uid)
                || macos_mismatch(metadata.posix_gid, observed.st_gid)
                || matches!(metadata.posix_mode, MetadataField::Value(value) if u32::from(observed.st_mode) & 0o7777 != value & 0o7777)
                || matches!(metadata.posix_flags, MetadataField::Value(0) if observed.st_flags != 0)
                || matches!(metadata.created_ns, MetadataField::Value(value) if macos_nanos(observed.st_birthtime, observed.st_birthtime_nsec) != Some(value))
                || matches!(metadata.accessed_ns, MetadataField::Value(value) if macos_nanos(observed.st_atime, observed.st_atime_nsec) != Some(value))
                || matches!(metadata.modified_ns, MetadataField::Value(value) if macos_nanos(observed.st_mtime, observed.st_mtime_nsec) != Some(value))
            {
                return Err(NativeMetadataError("metadata readback mismatch").into());
            }
            Ok(())
        })();
        work.release(owned)?;
        result
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn validate_macos_metadata_fields(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), NativeMetadataError> {
    use crate::kernel::MetadataField;
    reject_metadata(&[
        (
            matches!(metadata.posix_flags, MetadataField::Value(value) if value != 0),
            "posix_flags",
        ),
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.named_attributes, MetadataField::Value(_)),
            "named_attributes",
        ),
        (matches!(metadata.acl, MetadataField::Value(_)), "acl"),
        (
            matches!(metadata.security_descriptor, MetadataField::Value(_)),
            "security_descriptor",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns without native-view baseline",
        ),
    ])?;
    Ok(())
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn apply_macos_timestamp_metadata<W: crate::demand::SourceDemandReadWork>(
    fd: libc::c_int,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;
    if let MetadataField::Value(value) = metadata.created_ns {
        let mut attributes = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: libc::ATTR_CMN_CRTIME,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        let mut created = macos_timespec(value);
        native_write(work)?;
        // SAFETY: both buffers remain live for the synchronous system call.
        if unsafe {
            libc::fsetattrlist(
                fd,
                (&raw mut attributes).cast(),
                (&raw mut created).cast(),
                std::mem::size_of::<libc::timespec>(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
    }
    if matches!(metadata.accessed_ns, MetadataField::Value(_))
        || matches!(metadata.modified_ns, MetadataField::Value(_))
    {
        let omitted = libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        };
        let times = [
            match metadata.accessed_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
            match metadata.modified_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
        ];
        native_write(work)?;
        // SAFETY: fd pins the inode and times is a live two-element array.
        if unsafe { libc::futimens(fd, times.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn macos_mismatch<T: Copy + Eq>(
    field: crate::kernel::MetadataField<T>,
    observed: T,
) -> bool {
    matches!(field, crate::kernel::MetadataField::Value(expected) if expected != observed)
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn macos_timespec(value: i64) -> libc::timespec {
    libc::timespec {
        tv_sec: value.div_euclid(1_000_000_000),
        tv_nsec: value.rem_euclid(1_000_000_000),
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn macos_nanos(seconds: libc::time_t, nanoseconds: libc::c_long) -> Option<i64> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds);
    i64::try_from(nanos).ok()
}
