use super::*;

pub(super) struct DirectoryHandle {
    pub(super) path: DarwinOwned<MountPath>,
    pub(super) binding_epoch: Option<u64>,
    pub(super) epochs: Option<CacheEpochs>,
    pub(super) cursor: Option<Vec<u8>>,
    pub(super) entries: VecDeque<MountDirectoryEntry>,
    pub(super) emitted: i64,
    pub(super) page_source_ownership: Option<crate::demand::SourcePayloadOwnership>,
}

impl DirectoryHandle {
    pub(super) fn new(
        path: DarwinOwned<MountPath>,
        binding_epoch: Option<u64>,
        epochs: Option<CacheEpochs>,
    ) -> Self {
        Self {
            path,
            binding_epoch,
            epochs,
            cursor: None,
            entries: VecDeque::new(),
            emitted: 0,
            page_source_ownership: None,
        }
    }

    pub(super) fn clear_page(&mut self) {
        drop(self.cursor.take());
        drop(std::mem::take(&mut self.entries));
        drop(self.page_source_ownership.take());
    }

    pub(super) fn rewind(&mut self) {
        self.clear_page();
        self.emitted = 0;
    }

    pub(super) fn pop_entry(&mut self) -> bool {
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        let bytes = entry.name.capacity() as u64;
        let owner = self
            .page_source_ownership
            .as_mut()
            .filter(|owner| owner.has_native_custody())
            .map(|owner| {
                owner
                    .split_owned_payload(bytes)
                    .expect("source page owns each name")
            });
        drop(entry);
        drop(owner);
        true
    }

    pub(super) fn is_current(
        &self,
        context: &DarwinMountContext,
        work: &Work,
    ) -> Result<bool, NativeMountError> {
        Ok(match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable_with_meter(work)?
                    && context.source.binding_epoch_with_meter(work)? == Some(epochs.binding)
                    && context.unchanged_since(&self.path, None, epochs.view, work)?
            }
            None => context.cache_epochs(work)?.is_none(),
        })
    }

    pub(super) fn keep_current(
        &mut self,
        context: &DarwinMountContext,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        for _ in 0..MAXIMUM_LISTING_RESTARTS {
            if self.epochs.is_none() || self.is_current(context, work)? {
                return Ok(());
            }
            if self.emitted > 2
                || context.source.binding_epoch_with_meter(work)? != self.binding_epoch
            {
                return Err(driver_errno(libc::ESTALE));
            }
            self.epochs = context.cache_epochs(work)?;
            self.clear_page();
        }
        Err(driver_errno(libc::ESTALE))
    }
}

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
        let directory = shared(work, DarwinOwned::empty(Mutex::new(directory)))?;
        let mut directories = context
            .directories
            .lock()
            .map_err(|_| driver_errno(libc::EIO))?;
        grow_map(&mut directories, work)?;
        directories.insert(allocated, directory);
        // SAFETY: the bridge supplies a writable handle slot.
        work.charge(copy_work(std::mem::size_of::<u64>() as u64))?;
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
    let span = callback_span!(TRACE, readdir, fh = handle, offset = offset);
    ffi(address, &span, false, |context, work| {
        let directory = {
            let directories = context
                .directories
                .lock()
                .map_err(|_| driver_errno(libc::EIO))?;

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
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory, work)?;
            let Some(entry) = directory.entries.front() else {
                directory.keep_current(context, work)?;
                return Ok(0);
            };
            let (name, attributes) = directory_entry(context, &directory, entry, work)?;
            let next = directory
                .emitted
                .checked_add(1)
                .ok_or(WorkError::Overflow)?;
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
    work: &Work,
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
    work: &Work,
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
    work: &Work,
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

        directories
            .remove(&handle)
            .ok_or_else(|| driver_errno(libc::ESTALE))?;
        Ok(0)
    })
}
