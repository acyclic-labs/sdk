use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_getxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    length: usize,
) -> c_int {
    let _span = callback_span!(TRACE, getxattr);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        if !context.may_have_named_attributes(&path)? {
            return Err(libc::ENOATTR);
        }
        let bytes = context
            .source
            .read_attribute(&path, c_bytes(name)?)
            .map_err(errno)?
            .ok_or(libc::ENOATTR)?;
        copy_variable_result(&bytes, value, length)
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
    let _span = callback_span!(TRACE, setxattr);
    ffi_status(address, &_span, |context| {
        if length > MAXIMUM_CALLBACK_BYTES {
            return Err(libc::E2BIG);
        }
        let mode = match flags {
            0 => MountAttributeWriteMode::Upsert,
            libc::XATTR_CREATE => MountAttributeWriteMode::Create,
            libc::XATTR_REPLACE => MountAttributeWriteMode::Replace,
            _ => return Err(libc::EINVAL),
        };
        // SAFETY: the bridge forwards FUSE's `length`-byte attribute value, readable
        // for the callback.
        let bytes = unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) };
        let (path, name) = (mount_path(path)?, c_bytes(name)?);
        context.mutate(|| {
            context
                .source
                .write_attribute(&path, name, Bytes::copy_from_slice(bytes), mode)
        })?;
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
    let _span = callback_span!(TRACE, listxattr);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        if !context.may_have_named_attributes(&path)? {
            return copy_variable_result(&[], list, length);
        }
        let mut cursor = None;
        let mut encoded = Vec::new();
        loop {
            let page = context
                .source
                .list_attributes(&path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
                .map_err(errno)?;
            for name in page.names {
                if name.contains(&0) {
                    return Err(libc::EIO);
                }
                let required = encoded
                    .len()
                    .checked_add(name.len())
                    .and_then(|value| value.checked_add(1))
                    .ok_or(libc::EOVERFLOW)?;
                if required > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG);
                }
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            match page.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                Some(_) => return Err(libc::EIO),
                None => break,
            }
        }
        copy_variable_result(&encoded, list, length)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    let _span = callback_span!(TRACE, removexattr);
    ffi_status(address, &_span, |context| {
        let (path, name) = (mount_path(path)?, c_bytes(name)?);
        context.mutate(|| context.source.remove_attribute(&path, name))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_lseek(
    address: usize,
    path: *const c_char,
    handle: u64,
    offset: i64,
    whence: c_int,
) -> i64 {
    let _span = callback_span!(TRACE, lseek, fh = handle, offset = offset);
    ffi_offset(address, &_span, |context| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let result = context
            .file_or_open(&mount_path(path)?, handle)?
            .seek(offset, target)
            .map_err(errno)?
            .ok_or(libc::ENXIO)?;
        i64::try_from(result).map_err(|_| libc::EOVERFLOW)
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
    let _span = callback_span!(TRACE, fallocate, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        let (operation, permitted_flags) = if mode & FALLOC_FL_PUNCH_HOLE != 0 {
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
        if mode & !permitted_flags != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        context.mutate_file(&mount_path(path)?, handle, |file| {
            file.allocate_range(offset, length, operation)
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_copy_file_range(
    address: usize,
    source_path: *const c_char,
    source_handle: u64,
    source_offset: i64,
    destination_path: *const c_char,
    destination_handle: u64,
    destination_offset: i64,
    length: usize,
    flags: c_int,
) -> i64 {
    let _span = callback_span!(TRACE, copy_file_range, fh = source_handle);
    ffi_offset(address, &_span, |context| {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        let source_offset = u64::try_from(source_offset).map_err(|_| libc::EINVAL)?;
        let destination_offset = u64::try_from(destination_offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let source = context.file_or_open(&mount_path(source_path)?, source_handle)?;
        let length = context.mutate_file(
            &mount_path(destination_path)?,
            destination_handle,
            |destination| {
                let source_lookup = source.lookup()?;
                let destination_lookup = destination.lookup()?;
                let length = length.min(
                    source_lookup
                        .node
                        .logical_bytes
                        .saturating_sub(source_offset),
                );
                context.source.clone_range_by_id(
                    source_lookup.node.file_id,
                    source_offset,
                    destination_lookup.node.file_id,
                    destination_offset,
                    length,
                )?;
                Ok(length)
            },
        )?;
        i64::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

