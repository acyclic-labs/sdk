use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    let span = callback_span!(TRACE, getattr, fh = handle);
    ffi(address, &span, false, |context, work| {
        let attributes = context.attributes(&*mount_path(path, work)?, handle, work)?;
        // SAFETY: the bridge supplies a writable NativeStat.
        work.charge(copy_work(std::mem::size_of::<NativeStat>() as u64))?;
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
    let span = callback_span!(TRACE, open);
    ffi(
        address,
        &span,
        flags & libc::O_ACCMODE != libc::O_RDONLY,
        |context, work| {
            let opened = context.open(&*mount_path(path, work)?, 0, work)?;
            // SAFETY: the bridge supplies a writable handle.
            work.charge(copy_work(std::mem::size_of::<u64>() as u64))?;
            unsafe { handle.write(opened) };
            Ok(0)
        },
    )
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
    let span = callback_span!(DEBUG, create);
    ffi(address, &span, true, |context, work| {
        let path = mount_path(path, work)?;
        context.mutate(
            || {
                context.source.create_file(
                    &path,
                    create_metadata(mode, mode::IFREG, uid, gid),
                    None,
                    work,
                )
            },
            work,
        )?;
        let opened = context.open(&path, context.ledger.latest(), work)?;
        // SAFETY: the bridge supplies a writable handle.
        work.charge(copy_work(std::mem::size_of::<u64>() as u64))?;
        unsafe { handle.write(opened) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_release(
    address: usize,
    _path: *const c_char,
    handle: u64,
) -> c_int {
    let span = callback_span!(DEBUG, release, fh = handle);
    ffi(address, &span, false, |context, work| {
        if handle != 0 {
            let mut files = context.files.write().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            files
                .remove(&handle)
                .ok_or_else(|| driver_errno(libc::ESTALE))?;
        }
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
    let span = callback_span!(TRACE, read, fh = handle, offset = offset, len = length);
    ffi(address, &span, false, |context, work| {
        let maximum = bounded_length(length).map_err(driver_errno)?;
        let offset = u64::try_from(offset).map_err(|_| driver_errno(libc::EINVAL))?;
        let target = context.file_target(&*mount_path(path, work)?, handle, work)?;
        let chunk = target.file.0.read_up_to_with_meter(offset, maximum, work)?;
        source_bytes(chunk, work, |bytes| {
            if bytes.len() > maximum as usize {
                return Err(driver_errno(libc::EIO));
            }
            // SAFETY: the reply fits the bridge's length-byte output buffer.
            work.charge(copy_work(bytes.len() as u64))?;
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len());
            };
            c_int::try_from(bytes.len()).map_err(|_| driver_errno(libc::EOVERFLOW))
        })
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
    let span = callback_span!(TRACE, write, fh = handle, offset = offset, len = length);
    ffi(address, &span, true, |context, work| {
        let length = bounded_length(length).map_err(driver_errno)?;
        let offset = u64::try_from(offset).map_err(|_| driver_errno(libc::EINVAL))?;
        // SAFETY: the bridge retains this readable length-byte input through the callback.
        let input = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) }
        };
        let bytes = copy_bytes(work, input)?;
        context.mutate_file(
            &*mount_path(path, work)?,
            handle,
            |file| file.write_range_with_meter(offset, bytes, work),
            work,
        )?;
        c_int::try_from(length).map_err(|_| driver_errno(libc::EOVERFLOW))
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_truncate(
    address: usize,
    path: *const c_char,
    handle: u64,
    length: i64,
) -> c_int {
    let span = callback_span!(TRACE, truncate, fh = handle);
    ffi(address, &span, true, |context, work| {
        let length = u64::try_from(length).map_err(|_| driver_errno(libc::EINVAL))?;
        context.mutate_file(
            &*mount_path(path, work)?,
            handle,
            |file| file.resize_with_meter(length, work),
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_flush(
    address: usize,
    handle: u64,
) -> c_int {
    let span = callback_span!(TRACE, flush, fh = handle);
    ffi(address, &span, true, |context, work| {
        context.flush_on_close(handle, work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    let span = callback_span!(TRACE, fsync);
    ffi(address, &span, true, |context, work| {
        context.sync(work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    let span = callback_span!(TRACE, durable_writes);
    ffi(address, &span, false, |context, _| {
        Ok(i32::from(!context.source.flush_publishes()))
    })
}
