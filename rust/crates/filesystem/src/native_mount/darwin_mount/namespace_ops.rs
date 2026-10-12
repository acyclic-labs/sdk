use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_mkdir(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
) -> c_int {
    let span = callback_span!(TRACE, mkdir);
    ffi(address, &span, true, |context, work| {
        let path = mount_path(path, work)?;
        context.mutate(
            || {
                context.source.create_directory(
                    &path,
                    create_metadata(mode, mode::IFDIR, uid, gid),
                    work,
                )
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_remove(
    address: usize,
    path: *const c_char,
    directory: c_int,
) -> c_int {
    let span = callback_span!(TRACE, remove);
    ffi(address, &span, true, |context, work| {
        let path = mount_path(path, work)?;
        let lookup = context.lookup(&path, work)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(driver_errno(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            }));
        }
        context.unlink(
            &path,
            lookup,
            || {
                context
                    .source
                    .remove(&path, Some(lookup.node.file_id), work)
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_rename(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
    flags: u32,
) -> c_int {
    let span = callback_span!(DEBUG, rename);
    ffi(address, &span, true, |context, work| {
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(driver_errno(libc::EOPNOTSUPP));
        }
        let (source, destination) = (mount_path(source, work)?, mount_path(destination, work)?);
        let replace = flags & RENAME_NOREPLACE == 0;
        let rename = || context.source.rename(&source, &destination, replace, work);
        match replace.then(|| context.lookup(&destination, work)) {
            Some(Ok(replaced)) => context.unlink(&destination, replaced, rename, work)?,
            None => context.mutate(rename, work)?,
            Some(Err(cause)) if callback_errno(&cause) == libc::ENOENT => {
                context.mutate(rename, work)?
            }
            Some(Err(cause)) => return Err(cause),
        }
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_link(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
) -> c_int {
    let span = callback_span!(TRACE, link);
    ffi(address, &span, true, |context, work| {
        let (source, destination) = (mount_path(source, work)?, mount_path(destination, work)?);
        context.mutate(
            || context.source.hard_link(&source, &destination, work),
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_symlink(
    address: usize,
    target: *const c_char,
    destination: *const c_char,
    uid: u32,
    gid: u32,
) -> c_int {
    let span = callback_span!(TRACE, symlink);
    ffi(address, &span, true, |context, work| {
        let target = copy_bytes(work, c_bytes(target, work)?)?;
        let destination = mount_path(destination, work)?;
        context.mutate(
            || {
                context.source.create_symbolic_link(
                    &destination,
                    target,
                    create_metadata(0o777, mode::IFLNK, uid, gid),
                    work,
                )
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_readlink(
    address: usize,
    path: *const c_char,
    buffer: *mut c_char,
    length: usize,
) -> c_int {
    let span = callback_span!(TRACE, readlink);
    ffi(address, &span, false, |context, work| {
        if length == 0 {
            return Err(driver_errno(libc::ERANGE));
        }
        let target = context.source.read_link(&*mount_path(path, work)?, work)?;
        source_bytes(target, work, |target| {
            if target.len() >= length {
                return Err(driver_errno(libc::ENAMETOOLONG));
            }
            // SAFETY: target and its terminator fit the bridge output buffer.
            work.charge(copy_work((target.len() + 1) as u64))?;
            unsafe {
                ptr::copy_nonoverlapping(target.as_ptr(), buffer.cast(), target.len());
                buffer.add(target.len()).write(0);
            };
            Ok(0)
        })
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_mknod(
    address: usize,
    path: *const c_char,
    mode: u32,
    device: u64,
    uid: u32,
    gid: u32,
) -> c_int {
    let span = callback_span!(TRACE, mknod);
    ffi(address, &span, true, |context, work| {
        let kind = match mode & mode::IFMT {
            mode::IFIFO => MountNodeKind::Fifo,
            mode::IFSOCK => MountNodeKind::Socket,
            mode::IFCHR => MountNodeKind::CharacterDevice,
            mode::IFBLK => MountNodeKind::BlockDevice,
            _ => return Err(driver_errno(libc::EOPNOTSUPP)),
        };
        let device = matches!(
            kind,
            MountNodeKind::CharacterDevice | MountNodeKind::BlockDevice
        )
        .then(|| crate::native_mount::device::split_device(device));
        let path = mount_path(path, work)?;
        context.mutate(
            || {
                context.source.create_special(
                    &path,
                    kind,
                    device,
                    create_metadata(mode, mode & mode::IFMT, uid, gid),
                    work,
                )
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_chmod(
    address: usize,
    path: *const c_char,
    mode: u32,
    handle: u64,
) -> c_int {
    let span = callback_span!(DEBUG, chmod, fh = handle);
    ffi(address, &span, true, |context, work| {
        context.mutate_metadata(
            &*mount_path(path, work)?,
            handle,
            |metadata| {
                let kind = metadata_or(metadata.posix_mode, 0) & mode::IFMT;
                metadata.posix_mode = MetadataField::Value(kind | (mode & 0o7777));
                Ok(())
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_chown(
    address: usize,
    path: *const c_char,
    uid: u32,
    gid: u32,
    handle: u64,
) -> c_int {
    let span = callback_span!(DEBUG, chown, fh = handle);
    ffi(address, &span, true, |context, work| {
        context.mutate_metadata(
            &*mount_path(path, work)?,
            handle,
            |metadata| {
                if uid != u32::MAX {
                    metadata.posix_uid = MetadataField::Value(uid);
                }
                if gid != u32::MAX {
                    metadata.posix_gid = MetadataField::Value(gid);
                }
                Ok(())
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_utimens(
    address: usize,
    path: *const c_char,
    times: *const NativeTimes,
    handle: u64,
) -> c_int {
    let span = callback_span!(DEBUG, utimens, fh = handle);
    ffi(address, &span, true, |context, work| {
        // SAFETY: the bridge retains a nonnull NativeTimes through this callback.
        let times = unsafe { times.as_ref() }.ok_or_else(|| driver_errno(libc::EINVAL))?;
        context.mutate_metadata(
            &*mount_path(path, work)?,
            handle,
            |metadata| {
                update_time(
                    &mut metadata.accessed_ns,
                    times.accessed_seconds,
                    times.accessed_nanoseconds,
                )
                .map_err(driver_errno)?;
                update_time(
                    &mut metadata.modified_ns,
                    times.modified_seconds,
                    times.modified_nanoseconds,
                )
                .map_err(driver_errno)?;
                Ok(())
            },
            work,
        )?;
        Ok(0)
    })
}
