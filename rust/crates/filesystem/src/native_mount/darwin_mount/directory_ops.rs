use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_opendir(
    address: usize,
    path: *const c_char,
    handle: *mut u64,
) -> c_int {
    let span = callback_span!(TRACE, opendir);
    ffi(address, &span, false, |context, work| {
        let path = mount_path(path, work)?;
        let binding = context.source.binding_epoch_with_meter(work)?;
        let epochs = context.cache_epochs(work)?;
        let _lease = context
            .source
            .acquire_binding_lease_with_meter(binding, work)?;
        if context.lookup(&path, work)?.node.kind != MountNodeKind::Directory {
            return Err(driver_errno(libc::ENOTDIR));
        }
        let mut directory = DirectoryHandle::new(path, binding, epochs);
        directory.keep_current(context, work)?;
        let allocated = context.allocate_handle().map_err(driver_errno)?;
        let directory = shared(work, Mutex::new(directory))?;
        let mut directories = context
            .directories
            .lock()
            .map_err(|_| driver_errno(libc::EIO))?;
        grow_map(&mut directories, work)?;
        copying(
            work,
            copy_work(
                std::mem::size_of::<(u64, Arc<DarwinOwned<Mutex<DirectoryHandle>>>)>() as u64,
            ),
            || directories.insert(allocated, directory),
        )?;
        // SAFETY: the bridge supplies a writable handle slot.
        copying(
            work,
            copy_work(std::mem::size_of::<u64>() as u64),
            || unsafe { handle.write(allocated) },
        )?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_readdir(
    address: usize,
    _path: *const c_char,
    buffer: *mut c_void,
    filler: DirectoryFiller,
    offset: i64,
    handle: u64,
) -> c_int {
    let span = callback_span!(TRACE, readdir, fh = handle, offset = offset);
    ffi(address, &span, false, |context, work| {
        let directory = {
            let directories = context
                .directories
                .lock()
                .map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(directories.capacity() as u64))?;
            directories
                .get(&handle)
                .cloned()
                .ok_or_else(|| driver_errno(libc::ESTALE))?
        };
        let mut directory = directory.lock().map_err(|_| driver_errno(libc::EIO))?;
        let _lease = context
            .source
            .acquire_binding_lease_with_meter(directory.binding_epoch, work)?;
        directory.keep_current(context, work)?;
        if offset < 0 {
            return Err(driver_errno(libc::EINVAL));
        }
        if directory.epochs.is_none() {
            directory.rewind();
            context
                .directory_checkpoints
                .lock()
                .map_err(|_| driver_errno(libc::EIO))?
                .remove(&*directory.path);
        } else if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(checkpoints.capacity() as u64))?;
            if let Some(checkpoint) = checkpoints.get(&*directory.path)
                && checkpoint.1 == offset
                && Some(checkpoint.0.binding) == directory.binding_epoch
                && context.source.view_is_stable_with_meter(work)?
                && context.unchanged_since(&directory.path, None, checkpoint.0.view, work)?
            {
                directory.epochs = Some(checkpoint.0);
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset(context, &mut directory, offset, work)?;
        while directory.emitted < 2 {
            let name = if directory.emitted == 0 { c"." } else { c".." };
            let next = directory.emitted + 1;
            // SAFETY: the synchronous fill borrows the bridge output and static name.
            let full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    ptr::null(),
                    next,
                )
            };
            if full != 0 {
                directory.keep_current(context, work)?;
                checkpoint_directory(context, &directory, work)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory, work)?;
            let Some(entry) = directory.entries.front() else {
                directory.keep_current(context, work)?;
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| driver_errno(libc::EIO))?
                    .remove(&*directory.path);
                return Ok(0);
            };
            let (name, attributes) = directory_entry(context, &directory, entry, work)?;
            let next = directory
                .emitted
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            // SAFETY: the synchronous fill borrows the bridge output, name and stat.
            let full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    &raw const attributes,
                    next,
                )
            };
            if full != 0 {
                directory.keep_current(context, work)?;
                checkpoint_directory(context, &directory, work)?;
                return Ok(0);
            }
            directory.pop_entry();
            directory.emitted = next;
        }
    })
}

pub(super) fn directory_entry(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    entry: &MountDirectoryEntry,
    work: &crate::NativeMaterializationWork,
) -> Result<(DarwinOwned<CString>, NativeStat), NativeMountError> {
    work.charge(scan_work(1))?;
    let name = native_name(&entry.name, work)?;
    let path = child_path(&directory.path, &entry.name, work)?;
    let attributes = context.attributes_from_lookup(
        &path,
        MountLookup {
            node: entry.node,
            metadata: entry.metadata,
        },
        directory.epochs.map(|epochs| epochs.view),
        work,
    )?;
    Ok((name, attributes))
}

pub(super) fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    while directory.emitted < offset {
        work.charge(scan_work(1))?;
        if directory.emitted >= 2 {
            ensure_directory_page(context, directory, work)?;
            if !directory.pop_entry() {
                return Err(driver_errno(libc::EINVAL));
            }
        }
        directory.emitted += 1;
    }
    Ok(())
}

pub(super) fn ensure_directory_page(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    directory.keep_current(context, work)?;
    for _ in 0..MAXIMUM_LISTING_RESTARTS {
        if !directory.entries.is_empty()
            || (directory.page_source_ownership.is_some() && directory.cursor.is_none())
        {
            return Ok(());
        }
        let read_in = directory.epochs;
        let page = context.source.read_directory_with_meter(
            &directory.path,
            directory.cursor.as_deref(),
            DIRECTORY_PAGE_SIZE,
            work,
        )?;
        directory.keep_current(context, work)?;
        if directory.epochs != read_in {
            continue;
        }
        if page.entries().is_empty() && page.next_cursor().is_some() {
            return Err(driver_errno(libc::EIO));
        }
        directory.clear_page();
        let (entries, cursor, ownership) = page.into_parts();
        directory.cursor = cursor;
        directory.entries = entries.into();
        directory.page_source_ownership = Some(ownership);
        return Ok(());
    }
    Err(driver_errno(libc::ESTALE))
}

pub(super) fn checkpoint_directory(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    const MAXIMUM_DIRECTORY_CHECKPOINTS: usize = 64;
    let Some(epochs) = directory.epochs else {
        return Ok(());
    };
    let mut checkpoints = context
        .directory_checkpoints
        .lock()
        .map_err(|_| driver_errno(libc::EIO))?;
    work.charge(scan_work(checkpoints.capacity() as u64))?;
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key(&*directory.path)
    {
        checkpoints.clear();
    }
    if !checkpoints.contains_key(&*directory.path) {
        grow_map(&mut checkpoints, work)?;
    }
    let (path, allocation) = clone_path(&directory.path, work)?.into_parts();
    drop(checkpoints.remove_entry(&path));
    copying(
        work,
        copy_work(std::mem::size_of::<(MountPath, DarwinOwned<(CacheEpochs, i64)>)>() as u64),
        || {
            checkpoints.insert(
                path,
                DarwinOwned {
                    value: (epochs, directory.emitted),
                    allocation,
                },
            )
        },
    )?;
    Ok(())
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(
    address: usize,
    handle: u64,
) -> c_int {
    let span = callback_span!(TRACE, releasedir, fh = handle);
    ffi(address, &span, false, |context, work| {
        let mut directories = context
            .directories
            .lock()
            .map_err(|_| driver_errno(libc::EIO))?;
        work.charge(scan_work(directories.capacity() as u64))?;
        directories
            .remove(&handle)
            .ok_or_else(|| driver_errno(libc::ESTALE))?;
        Ok(0)
    })
}
