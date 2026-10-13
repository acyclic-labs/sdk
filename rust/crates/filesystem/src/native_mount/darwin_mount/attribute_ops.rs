use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    length: usize,
) -> c_int {
    ffi(address, false, |context, work| {
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
    ffi(address, true, |context, work| {
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
    ffi(address, false, |context, work| {
        let path = mount_path(path, work)?;
        if !context.may_have_named_attributes(&path, work)? {
            return copy_variable_result(&[], list, length, work);
        }
        let mut cursor: Option<(Vec<u8>, NativeAllocation)> = None;
        if length != 0 && list.is_null() {
            return Err(driver_errno(libc::ERANGE));
        }
        let mut count = 0_usize;
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
                let next = count
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(1))
                    .ok_or(WorkError::Overflow)?;
                if next > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(driver_errno(libc::E2BIG));
                }
                if length != 0 {
                    if next > length {
                        return Err(driver_errno(libc::ERANGE));
                    }
                    work.charge(copy_work(name.len() as u64 + 1))?;
                    // SAFETY: next fits the non-null bridge buffer; the name stays live.
                    unsafe {
                        ptr::copy_nonoverlapping(
                            name.as_ptr(),
                            list.cast::<u8>().add(count),
                            name.len(),
                        );
                        list.add(next - 1).write(0);
                    }
                }
                count = next;
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
        c_int::try_from(count).map_err(|_| driver_errno(libc::EOVERFLOW))
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    ffi(address, true, |context, work| {
        let (path, name) = (mount_path(path, work)?, c_bytes(name, work)?);
        context.mutate(|| context.source.remove_attribute(&path, name, work), work)?;
        Ok(0)
    })
}
