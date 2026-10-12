use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_mkdir(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
) -> c_int {
    let _span = callback_span!(TRACE, mkdir);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        context.mutate(|| {
            context
                .source
                .create_directory(&path, create_metadata(mode, mode::IFDIR, uid, gid))
        })?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_remove(
    address: usize,
    path: *const c_char,
    directory: c_int,
) -> c_int {
    let _span = callback_span!(TRACE, remove);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        let lookup = context.lookup(&path)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            });
        }
        context.unlink(&path, lookup, || {
            context.source.remove(&path, Some(lookup.node.file_id))
        })?;
        context.namespace_changed();
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
    let _span = callback_span!(DEBUG, rename);
    ffi_status(address, &_span, |context| {
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let (source, destination) = (mount_path(source)?, mount_path(destination)?);
        let replace = flags & RENAME_NOREPLACE == 0;
        let rename = || context.source.rename(&source, &destination, replace);
        // Replacing the destination unlinks it.
        match replace.then(|| context.lookup(&destination)) {
            Some(Ok(replaced)) => context.unlink(&destination, replaced, rename)?,
            None | Some(Err(libc::ENOENT)) => context.mutate(rename)?,
            Some(Err(error)) => return Err(error),
        }
        context.namespace_changed();

        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_link(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
) -> c_int {
    let _span = callback_span!(TRACE, link);
    ffi_status(address, &_span, |context| {
        let (source, destination) = (mount_path(source)?, mount_path(destination)?);
        context.mutate(|| context.source.hard_link(&source, &destination))?;
        context.namespace_changed();
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
    let _span = callback_span!(TRACE, symlink);
    ffi_status(address, &_span, |context| {
        // SAFETY: the bridge forwards FUSE's NUL-terminated link target, valid for
        // the callback.
        let target = unsafe { CStr::from_ptr(target) }.to_bytes();
        let destination = mount_path(destination)?;
        context.mutate(|| {
            context.source.create_symbolic_link(
                &destination,
                Bytes::copy_from_slice(target),
                create_metadata(0o777, mode::IFLNK, uid, gid),
            )
        })?;
        context.namespace_changed();
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
    let _span = callback_span!(TRACE, readlink);
    ffi_status(address, &_span, |context| {
        if length == 0 {
            return Err(libc::ERANGE);
        }
        let target = context
            .source
            .read_link(&mount_path(path)?)
            .map_err(errno)?;
        if target.len() >= length {
            return Err(libc::ENAMETOOLONG);
        }
        // SAFETY: the bridge buffer holds `length` bytes and `target.len() < length`,
        // so the copy and its NUL terminator stay in bounds.
        unsafe {
            ptr::copy_nonoverlapping(target.as_ptr(), buffer.cast(), target.len());
            buffer.add(target.len()).write(0);
        }
        Ok(0)
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
    let _span = callback_span!(TRACE, mknod);
    ffi_status(address, &_span, |context| {
        let kind = match mode & mode::IFMT {
            mode::IFIFO => MountNodeKind::Fifo,
            mode::IFSOCK => MountNodeKind::Socket,
            mode::IFCHR => MountNodeKind::CharacterDevice,
            mode::IFBLK => MountNodeKind::BlockDevice,
            _ => return Err(libc::EOPNOTSUPP),
        };
        let device = matches!(
            kind,
            MountNodeKind::CharacterDevice | MountNodeKind::BlockDevice
        )
        .then(|| super::device::split_device(device));
        let path = mount_path(path)?;
        context.mutate(|| {
            context.source.create_special(
                &path,
                kind,
                device,
                create_metadata(mode, mode & mode::IFMT, uid, gid),
            )
        })?;
        context.namespace_changed();
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
    let _span = callback_span!(DEBUG, chmod, fh = handle);
    ffi_status(address, &_span, |context| {
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            let kind = metadata_or(metadata.posix_mode, 0) & mode::IFMT;
            metadata.posix_mode = MetadataField::Value(kind | (mode & 0o7777));
            Ok(())
        })?;
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
    let _span = callback_span!(DEBUG, chown, fh = handle);
    ffi_status(address, &_span, |context| {
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            if uid != u32::MAX {
                metadata.posix_uid = MetadataField::Value(uid);
            }
            if gid != u32::MAX {
                metadata.posix_gid = MetadataField::Value(gid);
            }
            Ok(())
        })?;
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
    let _span = callback_span!(DEBUG, utimens, fh = handle);
    ffi_status(address, &_span, |context| {
        // SAFETY: the bridge passes null (rejected here) or a `NativeTimes` valid for
        // the callback.
        let times = unsafe { times.as_ref() }.ok_or(libc::EINVAL)?;
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            update_time(
                &mut metadata.accessed_ns,
                times.accessed_seconds,
                times.accessed_nanoseconds,
            )?;
            update_time(
                &mut metadata.modified_ns,
                times.modified_seconds,
                times.modified_nanoseconds,
            )?;
            Ok(())
        })?;
        Ok(0)
    })
}

