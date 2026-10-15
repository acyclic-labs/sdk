#[cfg(any(feature = "native-mount", test))]
use super::metadata::{unix_metadata_nanos, unix_metadata_timespec};
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
        if fstat(file.as_raw_fd(), c"", work)?.st_mode & libc::S_IFMT == libc::S_IFLNK {
            return Err(NativeMetadataError("named_attributes on a symbolic link").into());
        }
        let prefix = b"/proc/self/fd/";
        let fd =
            u32::try_from(file.as_raw_fd()).map_err(|_| NativeMetadataError("named_attributes"))?;
        let number = rustix::path::DecInt::new(fd);
        let digits = number.as_bytes().len();
        let length = prefix.len() + digits;
        work.started_io(WorkCounters {
            bytes_copied: prefix.len() as u64,
            bytes_encoded: digits as u64,
            ..WorkCounters::UNCHARGED
        })?;
        pinned
            .get_mut(..prefix.len())
            .ok_or(NativeMetadataError("named_attributes"))?
            .copy_from_slice(prefix);
        work.started_io(copy_work(digits as u64))?;
        pinned
            .get_mut(prefix.len()..length)
            .ok_or(NativeMetadataError("named_attributes"))?
            .copy_from_slice(number.as_bytes());
        std::ffi::CStr::from_bytes_with_nul(
            pinned
                .get(..=length)
                .ok_or(NativeMetadataError("named_attributes"))?,
        )
        .map_err(|_| NativeMetadataError("named_attributes"))?
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
            // SAFETY: the held target, terminated name, and value remain live.
            let written = unsafe {
                libc::fsetxattr(fd, name.as_ptr(), value.as_ptr().cast(), value.len(), 0, 0)
            };
            if written != 0 {
                return Err(io::Error::last_os_error().into());
            }
            verify_extended_attribute(
                #[cfg(target_os = "linux")]
                pinned,
                #[cfg(target_os = "macos")]
                fd,
                name,
                value,
                work,
            )
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

#[cfg(any(feature = "native-mount", test))]
pub(crate) fn apply_metadata<W: NativeHostWork<Error = DemandError>>(
    file: &File,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::{FileMetadata, MetadataField};
    work.check()?;
    if !matches!(
        metadata,
        FileMetadata {
            windows_attributes: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            ..
        }
    ) {
        return Err(NativeMetadataError("unsupported Unix metadata").into());
    }
    #[cfg(target_os = "linux")]
    if metadata.created_ns != MetadataField::Unavailable
        || metadata.posix_flags != MetadataField::Unavailable
    {
        return Err(NativeMetadataError("unsupported Linux metadata").into());
    }
    #[cfg(target_os = "macos")]
    if !matches!(
        metadata.posix_flags,
        MetadataField::Unavailable | MetadataField::Value(0)
    ) {
        return Err(NativeMetadataError("unsupported macOS flags").into());
    }
    if metadata == FileMetadata::default() {
        return Ok(());
    }
    let mode = match metadata.posix_mode {
        MetadataField::Value(mode) => Some(
            libc::mode_t::try_from(mode & 0o177777)
                .map_err(|_| NativeMetadataError("posix_mode"))?,
        ),
        MetadataField::Unavailable => None,
    };
    let fd = file.as_raw_fd();
    let mut observed = fstat(fd, c"", work)?;
    validate_mode(mode, &observed, work)?;
    let uid = match metadata.posix_uid {
        MetadataField::Value(value) if value != observed.st_uid => value,
        _ => libc::uid_t::MAX,
    };
    let gid = match metadata.posix_gid {
        MetadataField::Value(value) if value != observed.st_gid => value,
        _ => libc::gid_t::MAX,
    };
    if uid != libc::uid_t::MAX || gid != libc::gid_t::MAX {
        // SAFETY: both backends act on the held inode, including Linux O_PATH links.
        unix_mutation(work, || unsafe {
            #[cfg(target_os = "linux")]
            {
                libc::fchownat(fd, c"".as_ptr(), uid, gid, libc::AT_EMPTY_PATH)
            }
            #[cfg(target_os = "macos")]
            {
                libc::fchown(fd, uid, gid)
            }
        })?;
        observed = fstat(fd, c"", work)?;
    }
    if let Some(mode) = mode
        && mode & 0o7777 != observed.st_mode & 0o7777
    {
        // Ownership changes can clear setuid/setgid; restore mode afterwards.
        // SAFETY: both backends act only on the held inode.
        unix_mutation(work, || unsafe {
            #[cfg(target_os = "linux")]
            {
                if libc::syscall(
                    LINUX_FCHMODAT2_SYSCALL,
                    fd,
                    c"".as_ptr(),
                    mode & 0o7777,
                    libc::AT_EMPTY_PATH,
                ) == 0
                {
                    0
                } else {
                    -1
                }
            }
            #[cfg(target_os = "macos")]
            {
                libc::fchmod(fd, mode & 0o7777)
            }
        })?;
    }
    apply_times(fd, metadata, work)?;
    observed = fstat(fd, c"", work)?;
    verify_metadata(metadata, mode, &observed)?;
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

#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    feature = "native-mount"
))]
fn verify_extended_attribute<W: NativeHostWork<Error = DemandError>>(
    #[cfg(target_os = "linux")] pinned: &std::ffi::CStr,
    #[cfg(target_os = "macos")] fd: std::os::fd::RawFd,
    name: &std::ffi::CStr,
    value: &[u8],
    work: &mut W,
) -> Result<(), DemandError> {
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
        // SAFETY: the held target, terminated name, and bounded output remain live.
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
                return Err(NativeMetadataError("named_attributes readback mismatch").into());
            }
        }
        Ok(())
    })();
    drop(readback);
    work.release(allocation)?;
    result
}

#[cfg(any(feature = "native-mount", test))]
fn validate_mode<W: NativeHostWork<Error = DemandError>>(
    mode: Option<libc::mode_t>,
    observed: &libc::stat,
    work: &mut W,
) -> Result<(), DemandError> {
    if let Some(mode) = mode {
        let kind = mode & libc::S_IFMT;
        if kind != 0 && kind != observed.st_mode & libc::S_IFMT {
            return Err(NativeMetadataError("posix_mode file type").into());
        }
        #[cfg(target_os = "linux")]
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            if observed.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(NativeMetadataError("posix_mode symlink").into());
            }
            work.started_io(WRITE)?;
            // SAFETY: descriptor -1 makes this support probe non-mutating.
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
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EBADF) => (),
                Some(libc::ENOSYS | libc::EINVAL) => {
                    return Err(NativeMetadataError("fchmodat2 AT_EMPTY_PATH").into());
                }
                _ => return Err(error.into()),
            }
        }
    }
    #[cfg(target_os = "macos")]
    let _ = work;
    Ok(())
}

#[cfg(any(feature = "native-mount", test))]
fn apply_times<W: NativeHostWork<Error = DemandError>>(
    fd: std::os::fd::RawFd,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;
    #[cfg(target_os = "macos")]
    if matches!(metadata.created_ns, MetadataField::Value(_)) {
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
        // SAFETY: initialized attribute and timestamp records remain live.
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
        // SAFETY: the held inode and two complete timespecs remain live.
        unix_mutation(work, || unsafe {
            #[cfg(target_os = "linux")]
            {
                libc::utimensat(
                    fd,
                    c"".as_ptr(),
                    times.as_ptr(),
                    libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
                )
            }
            #[cfg(target_os = "macos")]
            {
                libc::futimens(fd, times.as_ptr())
            }
        })?;
    }
    Ok(())
}

#[cfg(any(feature = "native-mount", test))]
fn verify_metadata(
    metadata: crate::kernel::FileMetadata,
    mode: Option<libc::mode_t>,
    observed: &libc::stat,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;
    for (requested, actual) in [
        (metadata.posix_uid, observed.st_uid),
        (metadata.posix_gid, observed.st_gid),
    ] {
        if matches!(requested, MetadataField::Value(value) if value != actual) {
            return Err(NativeMetadataError("ownership readback").into());
        }
    }
    if matches!(mode, Some(value) if value & 0o7777 != observed.st_mode & 0o7777) {
        return Err(NativeMetadataError("mode readback").into());
    }
    #[cfg(target_os = "macos")]
    if matches!(metadata.posix_flags, MetadataField::Value(0) if observed.st_flags != 0)
        || matches!(metadata.created_ns, MetadataField::Value(value) if unix_metadata_nanos(observed.st_birthtime, observed.st_birthtime_nsec) != Some(value))
    {
        return Err(NativeMetadataError("metadata readback").into());
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
