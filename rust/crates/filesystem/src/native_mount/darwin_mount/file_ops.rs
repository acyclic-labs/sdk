use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    let _span = callback_span!(TRACE, getattr, fh = handle);
    ffi_status(address, &_span, |context| {
        let attributes = context.attributes(&mount_path(path)?, handle)?;
        // SAFETY: the bridge passes a valid, writable `NativeStat` for the callback.
        unsafe { result.write(attributes) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_open(
    address: usize,
    path: *const c_char,
    flags: c_int,
    handle: *mut u64,
) -> c_int {
    let _span = callback_span!(TRACE, open);
    ffi_status(address, &_span, |context| {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            context.admit_write()?;
        }
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        unsafe { handle.write(context.open(&mount_path(path)?, InitialHandleState::Clean)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_create(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
    _flags: c_int,
    handle: *mut u64,
) -> c_int {
    let _span = callback_span!(DEBUG, create);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        let lookup = context.mutate(|| {
            context
                .source
                .create_file(&path, create_metadata(mode, mode::IFREG, uid, gid))
        })?;
        context.remember_current_lookup(&path, Some(lookup))?;
        context.namespace_changed();
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        unsafe { handle.write(context.open(&path, InitialHandleState::Written)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_release(
    address: usize,
    _path: *const c_char,
    handle: u64,
) -> c_int {
    let _span = callback_span!(DEBUG, release, fh = handle);
    ffi_status(address, &_span, |context| {
        if handle == 0 {
            return Ok(0);
        }
        context
            .files
            .write()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_read(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *mut c_char,
    length: usize,
    offset: i64,
) -> c_int {
    let _span = callback_span!(TRACE, read, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context| {
        let requested_length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let bytes = context
            .file_or_open(&mount_path(path)?, handle)?
            .read_up_to(offset, requested_length)
            .map_err(errno)?;
        if bytes.len() > usize::try_from(requested_length).unwrap_or(usize::MAX) {
            return Err(libc::EIO);
        }
        // SAFETY: the bridge buffer holds `length` writable bytes, `bytes` is no
        // longer than that (checked above), and Rust owns `bytes` separately.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len()) };
        c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_write(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *const c_char,
    length: usize,
    offset: i64,
) -> c_int {
    let _span = callback_span!(TRACE, write, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context| {
        let length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        // SAFETY: the bridge forwards FUSE's `length`-byte write buffer, readable for
        // the callback; `bounded_length` capped that length.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) };
        context.mutate_file(&mount_path(path)?, handle, |file| {
            file.write_range(offset, Bytes::copy_from_slice(bytes))
        })?;
        c_int::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_truncate(
    address: usize,
    path: *const c_char,
    handle: u64,
    length: i64,
) -> c_int {
    let _span = callback_span!(DEBUG, truncate, fh = handle, len = length);
    ffi_status(address, &_span, |context| {
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        context.mutate_file(&mount_path(path)?, handle, |file| file.resize(length))?;
        Ok(0)
    })
}

/// NFS CLOSE: publishes only what this handle wrote, and only for sources
/// whose close is a publication boundary.
#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_flush(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(DEBUG, flush, fh = handle);
    ffi_status(address, &_span, |context| {
        context.flush_on_close(handle)?;
        Ok(0)
    })
}

/// NFS COMMIT and stable WRITE. The client sends these for `fsync` and
/// for its close-to-open flush alike, so each keeps the `fsync` guarantee:
/// every acknowledged mutation is published once this returns.
#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    let _span = callback_span!(DEBUG, fsync);
    ffi_status(address, &_span, |context| {
        context.sync()?;
        Ok(0)
    })
}

/// Whether every acknowledged write is already as durable as `fsync` would
/// make it, because the source's flush publishes nothing: WRITE replies are
/// then stable and the client never needs to COMMIT.
#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    let _span = callback_span!(TRACE, durable_writes);
    ffi_status(address, &_span, |context| {
        Ok(c_int::from(!context.source.flush_publishes()))
    })
}

