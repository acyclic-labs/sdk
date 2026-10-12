use super::*;

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_opendir(
    address: usize,
    path: *const c_char,
    handle: *mut u64,
) -> c_int {
    let _span = callback_span!(TRACE, opendir);
    ffi_status(address, &_span, |context| {
        let path = mount_path(path)?;
        let binding_epoch = context.source.binding_epoch();
        let epochs = context.cache_epochs();

        let _binding_lease = context
            .source
            .acquire_binding_lease(binding_epoch)
            .map_err(errno)?;
        if context.lookup(&path)?.node.kind != MountNodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let mut directory = DirectoryHandle::new(path, binding_epoch, epochs);
        directory.keep_current(context)?;
        let allocated = context.allocate_handle()?;
        let mut directories = context.directories.lock().map_err(|_| libc::EIO)?;
        directories.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        directories.insert(allocated, Arc::new(Mutex::new(directory)));
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        unsafe { handle.write(allocated) };
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
    let _span = callback_span!(TRACE, readdir, fh = handle, offset = offset);
    ffi_status(address, &_span, |context| {
        let directory = context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .cloned()
            .ok_or(libc::ESTALE)?;
        let mut directory = directory.lock().map_err(|_| libc::EIO)?;
        let _binding_lease = context
            .source
            .acquire_binding_lease(directory.binding_epoch)
            .map_err(errno)?;
        directory.keep_current(context)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        if !directory.can_reuse_pages() {
            directory.rewind();
            context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?
                .remove(&directory.path);
        } else if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?;
            if let Some(checkpoint) = checkpoints.get(&directory.path)
                && checkpoint.revision == context.namespace_revision.load(Ordering::Acquire)
                && checkpoint.handle.emitted == offset
                && checkpoint.handle.binding_epoch == directory.binding_epoch
                && checkpoint.handle.is_current(context)
            {
                *directory = checkpoint.handle.clone();
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset(context, &mut directory, offset)?;
        while directory.emitted < 2 {
            let name = if directory.emitted == 0 { c"." } else { c".." };
            let next = directory.emitted + 1;
            // SAFETY: `buffer` and `filler` are FUSE's live readdir fill context for
            // this callback, and `name` outlives the synchronous call.
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    ptr::null(),
                    next,
                )
            };
            if buffer_full != 0 {
                finish_directory_page(context, &directory, true)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory)?;
            let Some(entry) = directory.entries.front() else {
                finish_directory_page(context, &directory, false)?;
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| libc::EIO)?
                    .remove(&directory.path);
                return Ok(0);
            };
            let (name, attributes) = directory_entry(context, &directory, entry)?;
            let next = directory.emitted.checked_add(1).ok_or(libc::EOVERFLOW)?;
            // SAFETY: `buffer` and `filler` are FUSE's live readdir fill context for
            // this callback; `name` and `attributes` outlive the synchronous call.
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    &raw const attributes,
                    next,
                )
            };
            if buffer_full != 0 {
                finish_directory_page(context, &directory, true)?;
                return Ok(0);
            }
            directory.entries.pop_front();
            directory.emitted = next;
        }
    })
}

/// One listed entry's native name and attributes. The page was read after
/// the directory's stamp was sampled, which therefore labels the entry.
pub(super) fn directory_entry(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    entry: &MountDirectoryEntry,
) -> Result<(CString, NativeStat), i32> {
    let name = CString::new(entry.name.as_slice()).map_err(|_| libc::EIO)?;
    let attributes = context.attributes_from_lookup(
        &directory.path.child(entry.name.clone()),
        MountLookup {
            node: entry.node,
            metadata: entry.metadata,
        },
        directory.epochs.map(|epochs| epochs.view),
    )?;
    Ok((name, attributes))
}

pub(super) fn finish_directory_page(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    checkpoint: bool,
) -> Result<(), i32> {
    // A mutation can happen after the last page read, while the native filler
    // copies entries into its buffer. Returning ESTALE discards that buffer
    // rather than exposing a listing assembled from different view epochs.
    if directory.emitted > 2 && directory.can_reuse_pages() && !directory.is_current(context) {
        return Err(libc::ESTALE);
    }
    if checkpoint {
        checkpoint_directory(context, directory)?;
    }
    Ok(())
}

pub(super) fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64,
) -> Result<(), i32> {
    while directory.emitted < offset {
        if directory.emitted < 2 {
            directory.emitted += 1;
            continue;
        }
        ensure_directory_page(context, directory)?;
        if directory.entries.pop_front().is_none() {
            return Err(libc::EINVAL);
        }
        directory.emitted += 1;
    }
    Ok(())
}

pub(super) fn ensure_directory_page(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
) -> Result<(), i32> {
    directory.keep_current(context)?;
    for _ in 0..MAXIMUM_LISTING_RESTARTS {
        if !directory.entries.is_empty() || directory.exhausted {
            return Ok(());
        }
        let read_in = directory.epochs;
        directory
            .revision
            .get_or_insert_with(|| context.namespace_revision.load(Ordering::Acquire));
        let page = context
            .source
            .read_directory(
                &directory.path,
                directory.cursor.as_deref(),
                DIRECTORY_PAGE_SIZE,
            )
            .map_err(errno)?;
        directory.keep_current(context)?;
        if directory.epochs != read_in {
            // Started over in a newer view; this page is from the older one.
            continue;
        }
        if page.entries.is_empty() && page.next_cursor.is_some() {
            return Err(libc::EIO);
        }
        directory.cursor = page.next_cursor;
        directory.exhausted = directory.cursor.is_none();
        directory.entries = page.entries.into();
        return Ok(());
    }
    Err(libc::ESTALE)
}

pub(super) fn checkpoint_directory(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
) -> Result<(), i32> {
    const MAXIMUM_DIRECTORY_CHECKPOINTS: usize = 64;
    if !directory.can_reuse_pages() {
        return Ok(());
    }
    let mut checkpoints = context
        .directory_checkpoints
        .lock()
        .map_err(|_| libc::EIO)?;
    let current_revision = context.namespace_revision.load(Ordering::Acquire);
    let revision = directory.revision.unwrap_or(current_revision);
    if revision != current_revision {
        checkpoints.remove(&directory.path);
        return Ok(());
    }
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key(&directory.path)
    {
        checkpoints.clear();
    }
    checkpoints.insert(
        directory.path.clone(),
        DirectoryCheckpoint {
            revision,
            handle: directory.clone(),
        },
    );
    Ok(())
}

#[unsafe(no_mangle)]
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(TRACE, releasedir, fh = handle);
    ffi_status(address, &_span, |context| {
        context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

