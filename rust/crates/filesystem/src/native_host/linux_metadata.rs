use super::*;

#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    feature = "native-mount"
))]
pub(crate) fn set_extended_attributes<W: crate::demand::SourceDemandReadWork>(
    file: &File,
    attributes: &[(Vec<u8>, acyclic_stream::OwnedBytes)],
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::unix::ffi::OsStrExt as _;
    work.check()?;
    if attributes.is_empty() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    let mut pinned = [0_u8; 32];
    #[cfg(target_os = "linux")]
    let pinned = {
        if fstat(file.as_raw_fd(), work)?.st_mode & libc::S_IFMT == libc::S_IFLNK {
            return Err(NativeMetadataError("named_attributes on a symbolic link").into());
        }
        let prefix = b"/proc/self/fd/";
        let fd = file.as_raw_fd() as u32;
        let digits = fd.checked_ilog10().unwrap_or(0) as usize + 1;
        let length = prefix.len() + digits;
        work.charge(WorkCounters {
            bytes_copied: prefix.len() as u64,
            bytes_encoded: digits as u64,
            ..WorkCounters::UNCHARGED
        })?;
        pinned[..prefix.len()].copy_from_slice(prefix);
        let mut number = fd;
        for index in (prefix.len()..length).rev() {
            pinned[index] = b'0' + (number % 10) as u8;
            number /= 10;
        }
        let pinned = std::ffi::CStr::from_bytes_with_nul(&pinned[..=length])
            .map_err(|_| NativeMetadataError("named_attributes"))?;
        pinned
    };
    #[cfg(target_os = "macos")]
    let fd = file.as_raw_fd();
    for (name, value) in attributes {
        work.charge(ITEM)?;
        let mut entered = false;
        let result = unix_name(OsStr::from_bytes(name), work, |name, work| {
            entered = true;
            native_write(work)?;
            // SAFETY: the held target, terminated name, and value remain live.
            #[cfg(target_os = "linux")]
            let written = unsafe {
                libc::setxattr(
                    pinned.as_ptr(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            };
            #[cfg(target_os = "macos")]
            let written = unsafe {
                libc::fsetxattr(fd, name.as_ptr(), value.as_ptr().cast(), value.len(), 0, 0)
            };
            if written != 0 {
                return Err(io::Error::last_os_error().into());
            }
            let length = value
                .len()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            work.allocate(length as u64)?;
            let mut readback = vec![0_u8; length];
            let result = (|| {
                work.check()?;
                work.admit(WorkCounters {
                    source_bytes_read: length as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                native_read(work)?;
                // SAFETY: the output buffer is writable for its full length.
                #[cfg(target_os = "linux")]
                let read = unsafe {
                    libc::getxattr(
                        pinned.as_ptr(),
                        name.as_ptr(),
                        readback.as_mut_ptr().cast(),
                        readback.len(),
                    )
                };
                #[cfg(target_os = "macos")]
                let read = unsafe {
                    libc::fgetxattr(
                        fd,
                        name.as_ptr(),
                        readback.as_mut_ptr().cast(),
                        readback.len(),
                        0,
                        0,
                    )
                };
                if let Ok(read) = u64::try_from(read) {
                    work.record(WorkCounters {
                        source_bytes_read: read,
                        ..WorkCounters::UNCHARGED
                    })?;
                }
                if usize::try_from(read).ok() != Some(value.len()) {
                    return Err(NativeMetadataError("named_attributes readback mismatch").into());
                }
                for (actual, expected) in readback.iter().zip(value.iter()) {
                    work.charge(ITEM)?;
                    if actual != expected {
                        return Err(
                            NativeMetadataError("named_attributes readback mismatch").into()
                        );
                    }
                }
                Ok(())
            })();
            drop(readback);
            work.release(length as u64)?;
            result
        });
        match result {
            Err(DemandError::Io(error))
                if !entered && error.kind() == io::ErrorKind::InvalidInput =>
            {
                return Err(NativeMetadataError("named_attributes").into());
            }
            result => result?,
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) fn apply_linux_metadata<W: crate::demand::SourceDemandReadWork>(
    file: &File,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;
    let mut observed = fstat(file.as_raw_fd(), work)?;
    validate_linux_metadata(observed, metadata, work)?;
    let requested_uid = match metadata.posix_uid {
        MetadataField::Value(uid) if uid != observed.st_uid => uid,
        _ => libc::uid_t::MAX,
    };
    let requested_gid = match metadata.posix_gid {
        MetadataField::Value(gid) if gid != observed.st_gid => gid,
        _ => libc::gid_t::MAX,
    };
    if requested_uid != libc::uid_t::MAX || requested_gid != libc::gid_t::MAX {
        native_write(work)?;
        // SAFETY: AT_EMPTY_PATH acts on the pinned inode, including a final symlink.
        if unsafe {
            libc::fchownat(
                file.as_raw_fd(),
                c"".as_ptr(),
                requested_uid,
                requested_gid,
                libc::AT_EMPTY_PATH,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
        observed = fstat(file.as_raw_fd(), work)?;
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        let desired = mode & 0o7777;
        if desired != observed.st_mode & 0o7777 {
            native_write(work)?;
            // SAFETY: fchmodat2 with AT_EMPTY_PATH acts only on the held inode.
            if unsafe {
                libc::syscall(
                    LINUX_FCHMODAT2_SYSCALL,
                    file.as_raw_fd(),
                    c"".as_ptr(),
                    desired,
                    libc::AT_EMPTY_PATH,
                )
            } != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            observed = fstat(file.as_raw_fd(), work)?;
        }
    }
    if matches!(metadata.modified_ns, MetadataField::Value(_))
        || matches!(metadata.accessed_ns, MetadataField::Value(_))
    {
        let times = [
            linux_time_spec(metadata.accessed_ns)?,
            linux_time_spec(metadata.modified_ns)?,
        ];
        native_write(work)?;
        // SAFETY: AT_EMPTY_PATH targets the held inode and the timespecs remain live until the syscall returns.
        if unsafe {
            libc::utimensat(
                file.as_raw_fd(),
                c"".as_ptr(),
                times.as_ptr(),
                libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
        observed = fstat(file.as_raw_fd(), work)?;
    }
    verify_linux_metadata(observed, metadata).map_err(Into::into)
}

#[cfg(target_os = "linux")]
#[cfg(target_arch = "aarch64")]
#[cfg(any(feature = "native-mount", test))]
pub(super) const LINUX_FCHMODAT2_SYSCALL: libc::c_long = 452;

#[cfg(target_os = "linux")]
#[cfg(not(target_arch = "aarch64"))]
#[cfg(any(feature = "native-mount", test))]
pub(super) const LINUX_FCHMODAT2_SYSCALL: libc::c_long = libc::SYS_fchmodat2;

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn validate_linux_metadata<W: crate::demand::SourceDemandReadWork>(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;
    reject_metadata(&[
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.posix_flags, MetadataField::Value(_)),
            "posix_flags",
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
            matches!(metadata.created_ns, MetadataField::Value(_)),
            "created_ns",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns",
        ),
    ])?;
    if let MetadataField::Value(mode) = metadata.posix_mode {
        let kind = mode & libc::S_IFMT;
        if kind != 0 && kind != observed.st_mode & libc::S_IFMT {
            return Err(NativeMetadataError("posix_mode file type").into());
        }
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            if observed.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(NativeMetadataError("posix_mode symlink").into());
            }
            linux_require_fchmodat2(work)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn linux_require_fchmodat2<W: crate::demand::SourceDemandReadWork>(
    work: &mut W,
) -> Result<(), DemandError> {
    native_write(work)?;
    let result = unsafe {
        libc::syscall(
            LINUX_FCHMODAT2_SYSCALL,
            -1,
            c"".as_ptr(),
            0,
            libc::AT_EMPTY_PATH,
        )
    };
    if result == 0 {
        return Err(NativeMetadataError("fchmodat2 probe").into());
    }
    match io::Error::last_os_error().raw_os_error() {
        Some(libc::EBADF) => Ok(()),
        Some(libc::ENOSYS | libc::EINVAL) => {
            Err(NativeMetadataError("fchmodat2 AT_EMPTY_PATH").into())
        }
        _ => Err(io::Error::last_os_error().into()),
    }
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn verify_linux_metadata(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
) -> Result<(), NativeMetadataError> {
    use crate::kernel::MetadataField;
    if let MetadataField::Value(uid) = metadata.posix_uid
        && observed.st_uid != uid
    {
        return Err(NativeMetadataError("posix_uid"));
    }
    if let MetadataField::Value(gid) = metadata.posix_gid
        && observed.st_gid != gid
    {
        return Err(NativeMetadataError("posix_gid"));
    }
    if let MetadataField::Value(mode) = metadata.posix_mode
        && observed.st_mode & 0o7777 != mode & 0o7777
    {
        return Err(NativeMetadataError("posix_mode"));
    }
    if let MetadataField::Value(ns) = metadata.accessed_ns
        && linux_observed_ns(observed.st_atime, observed.st_atime_nsec)? != ns
    {
        return Err(NativeMetadataError("accessed_ns"));
    }
    if let MetadataField::Value(ns) = metadata.modified_ns
        && linux_observed_ns(observed.st_mtime, observed.st_mtime_nsec)? != ns
    {
        return Err(NativeMetadataError("modified_ns"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn linux_time_spec(
    field: crate::kernel::MetadataField<i64>,
) -> Result<libc::timespec, NativeMetadataError> {
    let crate::kernel::MetadataField::Value(ns) = field else {
        return Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        });
    };
    Ok(libc::timespec {
        tv_sec: libc::time_t::try_from(ns.div_euclid(1_000_000_000))
            .map_err(|_| NativeMetadataError("timestamp range"))?,
        tv_nsec: libc::c_long::try_from(ns.rem_euclid(1_000_000_000))
            .map_err(|_| NativeMetadataError("timestamp range"))?,
    })
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(super) fn linux_observed_ns(seconds: i64, nanos: i64) -> Result<i64, NativeMetadataError> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|whole| whole.checked_add(nanos))
        .ok_or(NativeMetadataError("timestamp range"))
}
