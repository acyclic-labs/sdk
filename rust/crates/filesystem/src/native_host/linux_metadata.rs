use super::*;

#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    feature = "native-mount"
))]
pub(crate) fn set_extended_attributes<W: NativeHostWork<Error = DemandError>>(
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
        work.started_io(WorkCounters {
            bytes_copied: prefix.len() as u64,
            bytes_encoded: digits as u64,
            ..WorkCounters::UNCHARGED
        })?;
        pinned[..prefix.len()].copy_from_slice(prefix);
        let number = rustix::path::DecInt::new(fd);
        work.started_io(copy_work(digits as u64))?;
        pinned[prefix.len()..length].copy_from_slice(number.as_bytes());
        let pinned = std::ffi::CStr::from_bytes_with_nul(&pinned[..=length])
            .map_err(|_| NativeMetadataError("named_attributes"))?;
        pinned
    };
    #[cfg(target_os = "macos")]
    let fd = file.as_raw_fd();
    for (name, value) in attributes {
        work.started_io(ITEM)?;
        let mut entered = false;
        let result = unix_name(OsStr::from_bytes(name), work, |name, work| {
            entered = true;
            work.started_io(WRITE)?;
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
            let (mut readback, allocation) = buffer(length, work)?;
            readback.resize(length, 0u8);
            let result = (|| {
                work.check()?;
                work.admit(WorkCounters {
                    source_bytes_read: length as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                work.started_io(READ)?;
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
                    work.started_io(ITEM)?;
                    if actual != expected {
                        return Err(
                            NativeMetadataError("named_attributes readback mismatch").into()
                        );
                    }
                }
                Ok(())
            })();
            drop(readback);
            work.release(allocation)?;
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

#[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
pub(crate) fn apply_linux_metadata<W: NativeHostWork<Error = DemandError>>(
    file: &File,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::{FileMetadata, MetadataField};
    let fd = file.as_raw_fd();
    let mut observed = fstat(fd, work)?;
    if !matches!(
        metadata,
        FileMetadata {
            windows_attributes: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            ..
        }
    ) {
        return Err(NativeMetadataError("unsupported Linux metadata").into());
    }
    if metadata == FileMetadata::default() {
        return Ok(());
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        let kind = mode & libc::S_IFMT;
        if kind != 0 && kind != observed.st_mode & libc::S_IFMT {
            return Err(NativeMetadataError("posix_mode file type").into());
        }
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            if observed.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(NativeMetadataError("posix_mode symlink").into());
            }
            work.started_io(WRITE)?;
            // SAFETY: a non-mutating capability probe uses descriptor -1 and an empty name.
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
                Some(libc::EBADF) => (),
                Some(libc::ENOSYS | libc::EINVAL) => {
                    return Err(NativeMetadataError("fchmodat2 AT_EMPTY_PATH").into());
                }
                _ => return Err(io::Error::last_os_error().into()),
            }
        }
    }
    let uid = match metadata.posix_uid {
        MetadataField::Value(value) if value != observed.st_uid => value,
        _ => libc::uid_t::MAX,
    };
    let gid = match metadata.posix_gid {
        MetadataField::Value(value) if value != observed.st_gid => value,
        _ => libc::gid_t::MAX,
    };
    if uid != libc::uid_t::MAX || gid != libc::gid_t::MAX {
        // SAFETY: AT_EMPTY_PATH acts on the held inode, including a final symlink.
        unix_mutation(work, || unsafe {
            libc::fchownat(fd, c"".as_ptr(), uid, gid, libc::AT_EMPTY_PATH)
        })?;
        observed = fstat(fd, work)?;
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            // SAFETY: fchmodat2 with AT_EMPTY_PATH acts only on the held inode.
            unix_mutation(work, || unsafe {
                (libc::syscall(
                    LINUX_FCHMODAT2_SYSCALL,
                    fd,
                    c"".as_ptr(),
                    mode & 0o7777,
                    libc::AT_EMPTY_PATH,
                )) as libc::c_int
            })?;
        }
    }
    if matches!(metadata.accessed_ns, MetadataField::Value(_))
        || matches!(metadata.modified_ns, MetadataField::Value(_))
    {
        let times = [
            unix_metadata_timespec(metadata.accessed_ns)?,
            unix_metadata_timespec(metadata.modified_ns)?,
        ];
        // SAFETY: the held inode and two initialized timespecs remain live for the call.
        unix_mutation(work, || unsafe {
            libc::utimensat(
                fd,
                c"".as_ptr(),
                times.as_ptr(),
                libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
            )
        })?;
    }
    observed = fstat(fd, work)?;
    for (requested, actual) in [
        (metadata.posix_uid, observed.st_uid),
        (metadata.posix_gid, observed.st_gid),
    ] {
        if matches!(requested, MetadataField::Value(value) if value != actual) {
            return Err(NativeMetadataError("ownership readback").into());
        }
    }
    if matches!(metadata.posix_mode, MetadataField::Value(value) if value & 0o7777 != observed.st_mode & 0o7777)
    {
        return Err(NativeMetadataError("mode readback").into());
    }
    for (requested, seconds, nanos) in [
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
            return Err(NativeMetadataError("timestamp readback").into());
        }
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    target_arch = "aarch64",
    any(feature = "native-mount", test)
))]
pub(super) const LINUX_FCHMODAT2_SYSCALL: libc::c_long = 452;
#[cfg(all(
    target_os = "linux",
    not(target_arch = "aarch64"),
    any(feature = "native-mount", test)
))]
pub(super) const LINUX_FCHMODAT2_SYSCALL: libc::c_long = libc::SYS_fchmodat2;
