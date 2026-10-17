use super::*;
use crate::native_mount::MountDirectoryPage;

pub(super) struct DirectoryRead {
    pub(super) path: DarwinOwned<MountPath>,
    pub(super) epochs: Option<CacheEpochs>,
    page: Option<MountDirectoryPage>,
    pub(super) emitted: i64,
}

impl DirectoryRead {
    pub(super) fn new(path: DarwinOwned<MountPath>, epochs: Option<CacheEpochs>) -> Self {
        Self {
            path,
            epochs,
            page: None,
            emitted: 0,
        }
    }

    pub(super) fn clear_page(&mut self) {
        self.page = None;
    }

    #[allow(
        clippy::expect_used,
        reason = "The admitted source page owns each entry name until it is popped."
    )]
    pub(super) fn pop_entry(&mut self) -> bool {
        let Some(page) = &mut self.page else {
            return false;
        };
        let Some(entry) = page.entries.pop() else {
            return false;
        };
        let owner = if page.ownership.has_native_custody() {
            Some(
                page.ownership
                    .split_owned_payload(entry.name.capacity() as u64)
                    .expect("source page owns each name"),
            )
        } else {
            None
        };
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
                || context.source.binding_epoch_with_meter(work)?
                    != self.epochs.map(|epochs| epochs.binding)
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
pub(super) unsafe extern "C" fn acyclic_fs_darwin_mount_readdir(
    address: usize,
    path: *const c_char,
    buffer: *mut c_void,
    filler: DirectoryFiller,
    offset: i64,
) -> c_int {
    ffi(address, false, |context, work| {
        if offset < 0 {
            return Err(driver_errno(libc::EINVAL));
        }
        let path = mount_path(path, work)?;
        let binding = context.source.binding_epoch_with_meter(work)?;
        let epochs = context.cache_epochs(work)?;
        let _lease = context
            .source
            .acquire_binding_lease_with_meter(binding, work)?;
        if context.lookup(&path, work)?.node.kind != MountNodeKind::Directory {
            return Err(driver_errno(libc::ENOTDIR));
        }
        let mut directory = DirectoryRead::new(path, epochs);
        directory.keep_current(context, work)?;
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
            let Some(entry) = directory.page.as_ref().and_then(|page| page.entries.last()) else {
                directory.keep_current(context, work)?;
                return Ok(0);
            };
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

pub(super) fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryRead,
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
    directory: &mut DirectoryRead,
    work: &Work,
) -> Result<(), NativeMountError> {
    directory.keep_current(context, work)?;
    let mut restarts = 0;
    loop {
        work.charge(scan_work(1))?;
        if directory
            .page
            .as_ref()
            .is_some_and(|page| !page.entries.is_empty() || page.next_cursor.is_none())
        {
            return Ok(());
        }
        let read_in = directory.epochs;
        let mut page = context.source.read_directory_with_meter(
            &directory.path,
            directory
                .page
                .as_ref()
                .and_then(MountDirectoryPage::next_cursor),
            DIRECTORY_PAGE_SIZE,
            work,
        )?;
        directory.keep_current(context, work)?;
        if directory.epochs != read_in {
            restarts += 1;
            if restarts == MAXIMUM_LISTING_RESTARTS {
                return Err(driver_errno(libc::ESTALE));
            }
            continue;
        }
        work.charge(scan_work(page.entries.len() as u64))?;
        page.entries.reverse();
        directory.page = Some(page);
    }
}
