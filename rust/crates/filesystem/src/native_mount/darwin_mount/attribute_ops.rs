use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    length: usize,
) -> c_int {
    let span = callback_span!(TRACE, getxattr);
    ffi(address, &span, false, |context, work| {
        let path = mount_path(path, work)?;
        if !context.may_have_named_attributes(&path, work)? {
            return Err(driver_errno(libc::ENOATTR));
        }
        let bytes = context
            .source
            .read_attribute(&path, c_bytes(name, work)?, work)?
            .ok_or_else(|| driver_errno(libc::ENOATTR))?;
        source_bytes(bytes, work, |bytes| {
            copy_variable_result(bytes, value, length, work)
        })
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_setxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *const c_char,
    length: usize,
    flags: c_int,
) -> c_int {
    let span = callback_span!(TRACE, setxattr);
    ffi(address, &span, true, |context, work| {
        bounded_length(length).map_err(driver_errno)?;
        let mode = match flags {
            0 => MountAttributeWriteMode::Upsert,
            libc::XATTR_CREATE => MountAttributeWriteMode::Create,
            libc::XATTR_REPLACE => MountAttributeWriteMode::Replace,
            _ => return Err(driver_errno(libc::EINVAL)),
        };
        // SAFETY: the bridge retains each nonempty length-byte input.
        let input = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) }
        };
        let bytes = copy_bytes(work, input)?;
        let (path, name) = (mount_path(path, work)?, c_bytes(name, work)?);
        context.mutate(
            || {
                context
                    .source
                    .write_attribute(&path, name, bytes, mode, work)
            },
            work,
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_listxattr(
    address: usize,
    path: *const c_char,
    list: *mut c_char,
    length: usize,
) -> c_int {
    let span = callback_span!(TRACE, listxattr);
    ffi(address, &span, false, |context, work| {
        let path = mount_path(path, work)?;
        if !context.may_have_named_attributes(&path, work)? {
            return copy_variable_result(&[], list, length, work);
        }
        let mut cursor: Option<(Vec<u8>, crate::NativeAllocation)> = None;
        let mut encoded = DarwinOwned::empty(Vec::new());
        loop {
            let mut page = context.source.list_attributes_with_meter(
                &path,
                cursor.as_ref().map(|cursor| cursor.0.as_slice()),
                ATTRIBUTE_PAGE_SIZE,
                work,
            )?;
            for name in &page.0.names {
                work.charge(scan_work(name.len() as u64))?;
                if name.contains(&0) {
                    return Err(driver_errno(libc::EIO));
                }
                let required = encoded
                    .len()
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(1))
                    .ok_or(crate::WorkError::Overflow)?;
                grow_bytes(&mut encoded, required, work)?;
                work.charge(copy_work(name.len() as u64 + 1))?;
                encoded.extend_from_slice(name);
                encoded.push(0);
            }
            let Some(next) = page.0.next_cursor.take() else {
                break;
            };
            let next_allocation = page.1.split_off(next.capacity() as u64)?;
            if let Some((previous, _)) = &cursor {
                work.charge(scan_work(previous.len().min(next.len()) as u64))?;
                if *previous == next {
                    return Err(driver_errno(libc::EIO));
                }
            }
            cursor = Some((next, next_allocation));
        }
        copy_variable_result(&encoded, list, length, work)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    let span = callback_span!(TRACE, removexattr);
    ffi(address, &span, true, |context, work| {
        let (path, name) = (mount_path(path, work)?, c_bytes(name, work)?);
        context.mutate(|| context.source.remove_attribute(&path, name, work), work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_fallocate(
    address: usize,
    path: *const c_char,
    handle: u64,
    mode: c_int,
    offset: i64,
    length: i64,
) -> c_int {
    let span = callback_span!(TRACE, fallocate, fh = handle, offset = offset, len = length);
    ffi(address, &span, true, |context, work| {
        let offset = u64::try_from(offset).map_err(|_| driver_errno(libc::EINVAL))?;
        let length = u64::try_from(length).map_err(|_| driver_errno(libc::EINVAL))?;
        let (operation, permitted) = if mode & FALLOC_FL_PUNCH_HOLE != 0 {
            (
                MountRangeAllocation::PunchHole,
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            )
        } else if mode & FALLOC_FL_ZERO_RANGE != 0 {
            (
                MountRangeAllocation::ZeroRange {
                    extend: mode & FALLOC_FL_KEEP_SIZE == 0,
                },
                FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
            )
        } else {
            (
                MountRangeAllocation::Preallocate {
                    keep_size: mode & FALLOC_FL_KEEP_SIZE != 0,
                },
                FALLOC_FL_KEEP_SIZE,
            )
        };
        if mode & !permitted != 0 {
            return Err(driver_errno(libc::EOPNOTSUPP));
        }
        context.mutate_file(
            &*mount_path(path, work)?,
            handle,
            |file| file.allocate_range_with_meter(offset, length, operation, work),
            work,
        )?;
        Ok(0)
    })
}
