use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    ffi(address, false, |context, work| {
        let attributes = context.attributes(&*mount_path(path, work)?, handle, work)?;
        work.charge(copy_work(std::mem::size_of::<NativeStat>() as u64))?;
        // SAFETY: the bridge supplies a writable NativeStat.
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
    ffi(
        address,
        flags & libc::O_ACCMODE != libc::O_RDONLY,
        |context, work| {
            let access = match flags & libc::O_ACCMODE {
                libc::O_RDONLY => super::super::MountOpenAccess::Read,
                libc::O_WRONLY => super::super::MountOpenAccess::Write,
                _ => super::super::MountOpenAccess::ReadWrite,
            };
            let opened = context.open(&*mount_path(path, work)?, access, 0, work)?;
            work.charge(copy_work(std::mem::size_of::<u64>() as u64))?;
            // SAFETY: the bridge supplies a writable handle.
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
    ffi(address, true, |context, work| {
        let path = mount_path(path, work)?;
        let created = context.mutate(
            || {
                context.source.create_file(
                    &path,
                    create_metadata(&context.clock, mode, u32::from(libc::S_IFREG), uid, gid),
                    None,
                    work,
                )
            },
            work,
        )?;
        let opened = context.open_created(
            &path,
            &created,
            context.completed.load(Ordering::Acquire),
            work,
        )?;
        work.charge(copy_work(std::mem::size_of::<u64>() as u64))?;
        // SAFETY: the bridge supplies a writable handle.
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
    ffi(address, false, |context, _work| {
        if handle != 0 {
            let mut files = context.files.write().map_err(|_| driver_errno(libc::EIO))?;

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
    ffi(address, false, |context, work| {
        let maximum = bounded_length(length).map_err(driver_errno)?;
        let offset = u64::try_from(offset).map_err(|_| driver_errno(libc::EINVAL))?;
        let target = context.file_target(&*mount_path(path, work)?, handle, work)?;
        let chunk = target.0.0.read_up_to_with_meter(offset, maximum, work)?;
        source_bytes(chunk, work, |bytes| {
            if bytes.len() > maximum as usize {
                return Err(driver_errno(libc::EIO));
            }
            work.charge(copy_work(bytes.len() as u64))?;
            // SAFETY: the reply fits the bridge's length-byte output buffer.
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
    ffi(address, true, |context, work| {
        let length = bounded_length(length).map_err(driver_errno)?;
        let offset = u64::try_from(offset).map_err(|_| driver_errno(libc::EINVAL))?;
        let input = if length == 0 {
            &[]
        } else {
            // SAFETY: the bridge retains this readable length-byte input through the callback.
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
    ffi(address, true, |context, work| {
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
    ffi(address, true, |context, work| {
        context.flush_on_close(handle, work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    ffi(address, true, |context, work| {
        context.sync(work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    ffi(address, false, |context, _| {
        Ok(i32::from(!context.source.flush_publishes()))
    })
}
