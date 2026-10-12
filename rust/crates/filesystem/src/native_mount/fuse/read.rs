use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ContentVersion {
    pub(super) logical_bytes: u64,
    pub(super) modified_ns: MetadataField<i64>,
    pub(super) changed_ns: MetadataField<i64>,
}

impl ContentVersion {
    pub(super) fn of(lookup: &MountLookup) -> Self {
        Self {
            logical_bytes: lookup.node.logical_bytes,
            modified_ns: lookup.metadata.modified_ns,
            changed_ns: lookup.metadata.changed_ns,
        }
    }
}

#[derive(Default)]
pub(super) struct PageStores {
    pub(super) in_flight: HashMap<crate::FileId, FuseOwned<ViewStamp>>,
    pub(super) claims: HashMap<crate::FileId, FuseOwned<u64>>,
}

impl PageStores {
    pub(super) fn may_begin(&self, file_id: crate::FileId) -> bool {
        !self.in_flight.contains_key(&file_id) && !self.claims.contains_key(&file_id)
    }

    pub(super) fn begin(&mut self, file_id: crate::FileId, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        debug_assert!(self.may_begin(file_id));
        let stamp = fuse_initialized(stamp, std::mem::size_of::<(crate::FileId, FuseOwned<ViewStamp>)>() as u64, work)?;
        self.in_flight.try_reserve(1).map_err(NativeMountError::Allocation)?;
        self.in_flight.insert(file_id, stamp);
        Ok(())
    }

    pub(super) fn land(&mut self, file_id: crate::FileId) {
        self.in_flight.remove(&file_id);
    }

    pub(super) fn claim(&mut self, file_id: crate::FileId, work: &crate::NativeMaterializationWork)
        -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        debug_assert!(!self.in_flight.contains_key(&file_id));
        if let Some(claims) = self.claims.get_mut(&file_id) {
            **claims = claims.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        } else {
            let claims = fuse_initialized(1, std::mem::size_of::<(crate::FileId, FuseOwned<u64>)>() as u64, work)?;
            self.claims.try_reserve(1).map_err(NativeMountError::Allocation)?;
            self.claims.insert(file_id, claims);
        }
        Ok(())
    }

    pub(super) fn release(&mut self, file_id: crate::FileId) {
        if let Some(claims) = self.claims.get_mut(&file_id) {
            **claims -= 1;
            if **claims == 0 {
                self.claims.remove(&file_id);
            }
        }
    }

    pub(super) fn precede(&self, target: ViewStamp) -> bool {
        self.in_flight.values().any(|stamp| **stamp < target)
    }
}

pub(super) struct PageStore {
    pub(super) inode: u64,
    pub(super) file_id: crate::FileId,
    pub(super) path: FuseOwned<MountPath>,
    pub(super) stamp: ViewStamp,
    pub(super) length: u32,
    pub(super) open_file: FuseOpenFile,
}

impl FuseProjection {
    pub(super) fn read_link(&self, inode: u64, work: &crate::NativeMaterializationWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let path = fuse_clone_path(self.core.state()?.path(inode)?, work)?;
        let stamp = source.view_stamp_with_meter(work)?;
        let target = source.read_link(&path, work).map_err(FuseRequestError::from)?;
        let target = fuse_owned_source_chunk(target, work)?;
        // The kernel keeps the target it receives.
        self.core.state()?.admit_attributes(source, inode, stamp, work)?;
        Ok(target)
    }

}

impl FuseProjection {
    pub(super) fn store_pages(
        &self,
        store: Option<PageStore>,
        mut open_flags: FopenFlags,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FopenFlags, FuseRequestError> {
        let Some(store) = store else { return Ok(open_flags); };
        let stored = (|| -> Result<bool, FuseRequestError> {
            let Some(notifier) = self.core.notifier.get() else { return Ok(false); };
            let bytes = store.open_file.file(work)?.read_up_to_with_meter(0, store.length, work)
                .map_err(FuseRequestError::from)?;
            let bytes = fuse_owned_source_chunk(bytes, work)?;
            let delta = crate::WorkCounters {
                backend_write_operations: 1,
                output_bytes: bytes.bytes.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            let materialization = work.clone();
            materialization.admit(delta).map_err(NativeMountError::from)?;
            materialization.charge(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(NativeMountError::from)?;
            let result = notifier.store(INodeNo(store.inode), 0, &bytes.bytes);
            materialization.record_admitted(crate::WorkCounters {
                output_bytes: if result.is_ok() { delta.output_bytes } else { 0 },
                ..crate::WorkCounters::UNCHARGED
            }).map_err(NativeMountError::from)?;
            result.map_err(NativeMountError::Io)?;
            Ok(true)
        })();
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => {
                self.core.record_handler_failure(work, NativeMountError::from(error));
                false
            }
        };
        let mut state = self.core.state_to_finish();
        state.page_stores.land(store.file_id);
        self.core.page_stored.notify_all();
        let admitted = match state.admissible(
            self.core.source.as_ref(), &store.path, Some(store.file_id), Some(store.stamp),
            work,
        ) {
            Ok(admitted) => admitted,
            Err(error) => {
                self.core.record_handler_failure(work, NativeMountError::from(error));
                false
            }
        };
        if !admitted {
            match fuse_reserve(&mut state.invalidation.deferred, 1, work) {
                Ok(()) => state.invalidation.deferred.push(KernelCacheItem::Inode(store.inode)),
                Err(error) => self.core.record_handler_failure(work, NativeMountError::from(error)),
            }
        }
        drop(state);
        open_flags.set(FopenFlags::FOPEN_KEEP_CACHE, stored && admitted);
        Ok(open_flags)
    }

}

impl FuseProjection {
    pub(super) fn read_through(&self, inode: u64, handle: u64, offset: u64, size: u32, work: &crate::NativeMaterializationWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let read = self.with_handle(inode, handle, Access::Read, |opened| {
            self.read_handle(inode, opened, offset, size, work)
        }, work);
        match read {
            Err(FuseRequestError::Errno(libc::ESTALE) | FuseRequestError::Native(NativeMountError::Source(MountSourceError::Stale))) if handle == 0 && self.skips_open => {
                let Some(stale) = self.core.state()?.implicit.get(&inode).map(|held| **held) else {
                    return Err(libc::ESTALE.into());
                };
                let reopened = {
                    let _names = self.core.names()?;
                    self.reopen_implicit(inode, Some(stale), Access::Read, work)?
                };
                self.read_handle(inode, reopened, offset, size, work)
            }
            read => read,
        }
    }

    pub(super) fn read_handle(&self, inode: u64, handle: u64, offset: u64, size: u32, work: &crate::NativeMaterializationWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let open_file = self.core.state()?.open_handle(inode, handle, work)?;
        let bytes = open_file.file(work)?.read_up_to_with_meter(offset, size, work).map_err(FuseRequestError::from)?;
        let bytes = fuse_owned_source_chunk(bytes, work)?;
        if self.core.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV.into());
        }
        Ok(bytes)
    }

}
