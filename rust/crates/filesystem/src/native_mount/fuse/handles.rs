use super::*;

pub(super) type FuseFile = Arc<FuseOwned<Arc<dyn MountOpenFile>>>;

pub(super) enum FuseOpenFile {
    Source(FuseFile),
    Deferred(Arc<DeferredOpenFile>),
}

impl FuseOpenFile {
    pub(super) fn file(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<std::borrow::Cow<'_, FuseFile>, MountSourceError> {
        match self {
            Self::Source(file) => Ok(std::borrow::Cow::Borrowed(file)),
            Self::Deferred(file) => file.file(work).map(std::borrow::Cow::Owned),
        }
    }

    pub(super) fn clone(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        Ok(match self {
            Self::Source(file) => Self::Source(fuse_arc_clone(file, work)?),
            Self::Deferred(file) => Self::Deferred(fuse_arc_clone(file, work)?),
        })
    }

    pub(super) fn same_file(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Source(left), Self::Source(right)) => Arc::ptr_eq(left, right),
            (Self::Deferred(left), Self::Deferred(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(super) fn settle(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        match self {
            Self::Source(file) => file.settle(work),
            Self::Deferred(file) => {
                let opened = file
                    .file
                    .lock()
                    .map_err(|_| MountSourceError::Stale)?
                    .as_ref()
                    .map(|file| fuse_arc_clone(file, work).map_err(MountSourceError::from))
                    .transpose()?;
                opened.map_or(Ok(()), |file| file.settle(work))
            }
        }
    }
}

pub(super) fn fuse_open_file(
    (file, bytes): (Arc<dyn MountOpenFile>, u64),
    work: &crate::NativeMaterializationWork,
) -> Result<FuseFile, MountSourceError> {
    let mut owner = FuseOwned::adopt(file, bytes, work).map_err(MountSourceError::from)?;
    owner
        .claim(
            std::mem::size_of::<FuseOwned<Arc<dyn MountOpenFile>>>() as u64,
            work,
        )
        .map_err(MountSourceError::NativeWork)?;
    Ok(Arc::new(owner))
}

pub(super) struct FileHandle {
    pub(super) inode: u64,
    pub(super) open_file: FuseOpenFile,
    pub(super) operation: Arc<(Mutex<()>, crate::NativeAllocation)>,
    pub(super) dirty: bool,
    pub(super) claim: Option<crate::FileId>,
    pub(super) _storage: crate::NativeAllocation,
}

pub(super) struct DeferredOpenFile {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) path: FuseOwned<MountPath>,
    pub(super) file_id: crate::FileId,
    pub(super) file: Mutex<Option<FuseFile>>,
    pub(super) _allocation: crate::NativeAllocation,
}

impl DeferredOpenFile {
    pub(super) fn file(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseFile, MountSourceError> {
        let mut file = self.file.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = file.as_ref() {
            return fuse_arc_clone(file, work).map_err(MountSourceError::from);
        }
        let opened = self.source.open_file_with_meter(&self.path, work)?;
        let opened = fuse_open_file(opened, work)?;
        if opened.lookup_with_meter(work)?.node.file_id != self.file_id {
            return Err(MountSourceError::Stale);
        }
        *file = Some(fuse_arc_clone(&opened, work).map_err(MountSourceError::from)?);
        Ok(opened)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Access {
    Read,
    Write,
}

pub(super) fn changes_content(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0
}

pub(super) struct OpenedFile<'a> {
    pub(super) path: &'a MountPath,
    pub(super) lookup: &'a MountLookup,
    pub(super) stamp: Option<ViewStamp>,
    pub(super) flags: i32,
    pub(super) open_file: FuseOpenFile,
    pub(super) dirty: bool,
    pub(super) stores_pages: bool,
}

impl ProjectionState {
    pub(super) fn admit_file_handle(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        opened: OpenedFile<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(u64, FopenFlags, Option<PageStore>), NativeMountError> {
        fuse_item(work)?;
        let OpenedFile {
            path,
            lookup,
            stamp,
            flags,
            open_file,
            dirty,
            stores_pages,
        } = opened;
        let file_id = lookup.node.file_id;
        let handle = self.next_handle;
        if self.files.contains_key(&handle) {
            return Err(libc::EMFILE.into());
        }
        let admitted = self.admissible(source, path, Some(file_id), stamp, work)?;
        let previous = self
            .by_inode
            .get(&inode)
            .ok_or(libc::ESTALE)?
            .cached_content;
        let opened_version = ContentVersion::of(lookup);
        let claim = changes_content(flags).then_some(file_id);
        let storage = work.claim(
            std::mem::size_of::<(u64, FileHandle)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let operation = work.claim(
            std::mem::size_of::<(Mutex<()>, crate::NativeAllocation)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let operation = Arc::new((Mutex::new(()), operation));
        self.files
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        let store = match (stamp, u32::try_from(lookup.node.logical_bytes)) {
            (Some(stamp), Ok(length))
                if stores_pages
                    && admitted
                    && previous.is_none()
                    && claim.is_none()
                    && flags & libc::O_DIRECT == 0
                    && length != 0
                    && u64::from(length) <= PAGE_STORE_LIMIT
                    && self.page_stores.may_begin(file_id) =>
            {
                let path = fuse_clone_path(path, work)?;
                let open_file = open_file.clone(work)?;
                self.page_stores.begin(file_id, stamp, work)?;
                Some(PageStore {
                    inode,
                    file_id,
                    path,
                    stamp,
                    length,
                    open_file,
                })
            }
            _ => None,
        };
        if let Some(file_id) = claim {
            self.page_stores.claim(file_id, work)?;
        }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.open_handles = entry.open_handles.saturating_add(1);
        hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        entry.cached_content = Some(opened_version);
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        self.files.insert(
            handle,
            FileHandle {
                inode,
                open_file,
                operation,
                dirty,
                claim,
                _storage: storage,
            },
        );
        let flushes_on_close =
            flags & libc::O_ACCMODE != libc::O_RDONLY && source.flush_on_handle_close();
        let mut open_flags = FopenFlags::empty();
        open_flags.set(
            FopenFlags::FOPEN_KEEP_CACHE,
            admitted && previous == Some(opened_version),
        );
        open_flags.set(FopenFlags::FOPEN_NOFLUSH, !flushes_on_close);
        Ok((handle, open_flags, store))
    }

    pub(super) fn discard_file_handle(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let Some(file) = self.files.remove(&handle) else {
            return Err(libc::ESTALE);
        };
        if file.inode != inode {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        }
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        };
        if entry.open_handles == 0 {
            self.files.insert(handle, file);
            return Err(libc::EIO);
        }
        entry.open_handles -= 1;
        if let Some(file_id) = file.claim {
            self.page_stores.release(file_id);
        }
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(mut entry) = self.by_inode.remove(&inode)
        {
            forget_file(
                &mut self.inode_by_file,
                entry.lookup.node.file_id,
                inode,
                &mut entry.identity_storage,
            );
        }
        Ok(())
    }

    pub(super) fn open_handle(
        &self,
        inode: u64,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOpenFile, NativeMountError> {
        fuse_item(work)?;
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE.into());
        }
        file.open_file.clone(work)
    }

    pub(super) fn open_handle_operation(
        &self,
        inode: u64,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<(Mutex<()>, crate::NativeAllocation)>, NativeMountError> {
        fuse_item(work)?;
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE.into());
        }
        fuse_arc_clone(&file.operation, work).map_err(Into::into)
    }

    pub(super) fn mark_handle_dirty(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let file = self.files.get_mut(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        file.dirty = true;
        Ok(())
    }

    pub(super) fn open_inode(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<FuseOpenFile>, NativeMountError> {
        for file in self.files.values() {
            fuse_item(work)?;
            if file.inode == inode {
                return file.open_file.clone(work).map(Some);
            }
        }
        Ok(None)
    }

    pub(super) fn attributes_to_read(
        &self,
        source: &dyn MountFilesystem,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<AttributeTarget>, NativeMountError> {
        fuse_item(work)?;
        if let Some(entry) = self.by_inode.get(&inode) {
            if entry.lacks_named_attributes(source, work)? {
                return Ok(None);
            }
        }
        self.attribute_target(inode, work).map(Some)
    }

    pub(super) fn attribute_target(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<AttributeTarget, NativeMountError> {
        match self.open_inode(inode, work)? {
            Some(open_file) => Ok(AttributeTarget::Handle(open_file)),
            None => Ok(AttributeTarget::Path(fuse_clone_path(
                self.path(inode)?,
                work,
            )?)),
        }
    }

    pub(super) fn apply_handle_facts(
        &mut self,
        inode: u64,
        lookup: MountLookup,
        stamp: Option<ViewStamp>,
    ) {
        if let Some(entry) = self.by_inode.get_mut(&inode)
            && entry.lookup.node.file_id == lookup.node.file_id
        {
            entry.lookup = lookup;
            entry.facts = stamp;
        }
    }

    pub(super) fn retain_detached_handles(
        &mut self,
        inode: u64,
        detached: &FuseOpenFile,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut assigned = 0_u64;
        for file in self.files.values_mut() {
            fuse_item(work)?;
            if file.inode == inode {
                file.open_file = detached.clone(work)?;
                assigned = assigned.saturating_add(1);
            }
        }
        let expected = self
            .by_inode
            .get(&inode)
            .map_or(0, |entry| entry.open_handles);
        debug_assert_eq!(assigned, expected);
        Ok(())
    }

    pub(super) fn unopened_referenced_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references != 0 && entry.open_handles == 0)
            .then_some(inode)
    }

    pub(super) fn detaching_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.open_handles != 0)
            .then_some(inode)
    }
}

impl FuseProjection {
    // Caller holds the names lock.
    pub(super) fn open_node(
        &self,
        inode: u64,
        flags: i32,
        storing: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(u64, FopenFlags), NativeMountError> {
        if flags & libc::O_TRUNC != 0 {
            self.check_source_write(work)?;
        }
        let source = self.core.source.as_ref();
        let (path, file_id, stamp_before, binding_before, held) = {
            let state = self.core.state()?;
            admit_open(state.defaults.writable, flags)?;
            let node = state.node(inode)?;
            if node.kind != MountNodeKind::Regular {
                return Err(libc::EISDIR.into());
            }
            let held = (!changes_content(flags) && flags & libc::O_DIRECT == 0)
                .then(|| state.by_inode.get(&inode))
                .flatten()
                .filter(|entry| entry.cached_content == Some(ContentVersion::of(&entry.lookup)))
                .and_then(|entry| Some((entry.lookup, entry.facts?)));
            (
                fuse_clone_path(state.path(inode)?, work)?,
                node.file_id,
                source.view_stamp_with_meter(work)?,
                source.binding_epoch_with_meter(work)?,
                held,
            )
        };
        let _claim = changes_content(flags)
            .then(|| self.core.claim_content(file_id, work))
            .transpose()?;
        let held = match held {
            Some((lookup, facts)) if lookup.node.file_id == file_id => source
                .unchanged_since_with_meter(&path, Some(file_id), facts, work)?
                .then_some((lookup, facts)),
            _ => None,
        };
        let (open_file, refreshed, stamp, truncated) = match held {
            Some((lookup, facts)) => {
                let allocation = work.claim(
                    std::mem::size_of::<DeferredOpenFile>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )?;
                let deferred = FuseOpenFile::Deferred(Arc::new(DeferredOpenFile {
                    source: fuse_arc_clone(&self.core.source, work)?,
                    path: fuse_clone_path(&path, work)?,
                    file_id: lookup.node.file_id,
                    file: Mutex::new(None),
                    _allocation: allocation,
                }));
                (deferred, lookup, Some(facts), false)
            }
            None => self.open_source(source, &path, file_id, flags, stamp_before, work)?,
        };
        let _binding = source
            .acquire_binding_lease_with_meter(binding_before, work)
            .map_err(NativeMountError::from)?;
        let mut state = self.core.state()?;
        if state.path(inode).ok() != Some(&*path) {
            return Err(libc::ESTALE.into());
        }
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        entry.lookup = refreshed;
        entry.facts = stamp;
        if let Some(binding) = entry.binding_mut(&path, work)? {
            binding.verified = stamp;
        }
        let (handle, open_flags, store) = state.admit_file_handle(
            source,
            inode,
            OpenedFile {
                path: &path,
                lookup: &refreshed,
                stamp,
                flags,
                open_file,
                dirty: truncated,
                stores_pages: storing,
            },
            work,
        )?;
        drop(state);
        Ok((handle, self.store_pages(store, open_flags, work)))
    }

    #[allow(
        clippy::type_complexity,
        reason = "returns the opened file with its facts, stamp, and truncation flag in one tuple"
    )]
    pub(super) fn open_source(
        &self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: crate::FileId,
        flags: i32,
        stamp_before: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FuseOpenFile, MountLookup, Option<ViewStamp>, bool), NativeMountError> {
        if flags & libc::O_TRUNC != 0 {
            self.check_source_write(work)?;
        }
        let (open_file, opened) = source
            .open_file_with_lookup_with_meter(path, work)
            .map_err(NativeMountError::from)?;
        let open_file = FuseOpenFile::Source(fuse_open_file(open_file, work)?);
        if opened.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        let truncated = flags & libc::O_TRUNC != 0;
        if truncated {
            self.source_write(work, || {
                open_file
                    .file(work)?
                    .resize_with_meter(0, work)
                    .map_err(NativeMountError::from)
            })?;
        }
        let unchanged = if !truncated {
            match stamp_before {
                Some(stamp) => {
                    source.unchanged_since_with_meter(path, Some(file_id), stamp, work)?
                }
                None => false,
            }
        } else {
            false
        };
        let (refreshed, stamp) = if unchanged {
            (opened, stamp_before)
        } else {
            let stamp = source.view_stamp_with_meter(work)?;
            let lookup = open_file
                .file(work)?
                .lookup_with_meter(work)
                .map_err(NativeMountError::from)?;
            if lookup.node.file_id != file_id {
                return Err(libc::ESTALE.into());
            }
            (lookup, stamp)
        };
        Ok((open_file, refreshed, stamp, truncated))
    }

    pub(super) fn create_file(
        &self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: i32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Entry, u64, FopenFlags), NativeMountError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (child, absent_since) = {
            let state = self.core.state()?;
            state.admit_write()?;
            let child = self.child(&state, parent, name, work)?;
            let absent_since = state.absent_since(parent, &child);
            (child, absent_since)
        };
        let path = &child.spelled;
        let stamp = source.view_stamp_with_meter(work)?;
        let claim = |file_id| {
            changes_content(flags)
                .then(|| self.core.claim_content(file_id, work))
                .transpose()
        };
        let (existing, proof) = match absent_since {
            Some(held) if source.unchanged_since_with_meter(child.key(), None, held, work)? => {
                (None, Some(held))
            }
            _ => (
                source
                    .lookup_with_meter(path, work)
                    .map_err(NativeMountError::from)?,
                stamp,
            ),
        };
        let (lookup, open_file, dirty, _claim, confirmed) = if let Some(existing) = existing {
            {
                if flags & libc::O_EXCL != 0 {
                    return Err(libc::EEXIST.into());
                }
                if existing.node.kind != MountNodeKind::Regular {
                    return Err(libc::EISDIR.into());
                }
                // Confirmed before the open changes anything.
                let confirmed =
                    self.confirm_names(HashMap::new(), [(child.key(), existing.node)], work)?;
                let claimed = claim(existing.node.file_id)?;
                let open_file = FuseOpenFile::Source(fuse_open_file(
                    source.open_file_with_meter(path, work)?,
                    work,
                )?);
                let opened = open_file
                    .file(work)?
                    .lookup_with_meter(work)
                    .map_err(NativeMountError::from)?;
                if opened.node.file_id != existing.node.file_id {
                    return Err(libc::ESTALE.into());
                }
                if flags & libc::O_TRUNC == 0 {
                    (opened, open_file, false, claimed, confirmed)
                } else {
                    self.source_write(work, || {
                        open_file
                            .file(work)?
                            .resize_with_meter(0, work)
                            .map_err(NativeMountError::from)
                    })?;
                    let resized = open_file
                        .file(work)?
                        .lookup_with_meter(work)
                        .map_err(NativeMountError::from)?;
                    (resized, open_file, true, claimed, confirmed)
                }
            }
        } else {
            let metadata = create_metadata(mode, S_IFREG, request.uid(), request.gid());
            let created = self.source_write(work, || {
                source
                    .create_file(path, metadata, proof, work)
                    .map_err(NativeMountError::from)
            })?;
            let claimed = claim(created.node.file_id)?;
            (
                created,
                FuseOpenFile::Source(fuse_open_file(
                    source.open_created_with_meter(path, &created, work)?,
                    work,
                )?),
                true,
                claimed,
                // A file just created has no other name.
                HashMap::new(),
            )
        };
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        let created = state
            .attr(inode, &lookup)
            .map_err(NativeMountError::from)
            .and_then(|attr| {
                let opened = OpenedFile {
                    path: child.key(),
                    lookup: &lookup,
                    stamp,
                    flags,
                    open_file,
                    dirty,
                    stores_pages: true,
                };
                let (handle, open_flags, store) =
                    state.admit_file_handle(source, inode, opened, work)?;
                Ok((attr, handle, open_flags, store))
            });
        let (attr, handle, open_flags, store) = created.map_err(|error| {
            state
                .release_lookup_reference(inode, 1, work)
                .err()
                .unwrap_or_else(|| error.into())
        })?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        drop(state);
        let open_flags = self.store_pages(store, open_flags, work);
        Ok((Entry { attr, ttl }, handle, open_flags))
    }

    pub(super) fn with_handle<T>(
        &self,
        inode: u64,
        handle: u64,
        access: Access,
        operation: impl FnOnce(u64) -> Result<T, NativeMountError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<T, NativeMountError> {
        if access == Access::Write {
            self.check_source_write(work)?;
        }
        if handle != 0 || !self.skips_open {
            return operation(handle);
        }
        let (held, writes) = {
            let state = self.core.state()?;
            let held = state.implicit.get(&inode).map(|held| held.0);
            let writes = held
                .and_then(|handle| state.files.get(&handle))
                .is_some_and(|file| file.claim.is_some());
            (held, writes)
        };
        match (held, access) {
            (Some(handle), Access::Read) => operation(handle),
            (Some(handle), Access::Write) if writes => operation(handle),
            (_, Access::Write) => {
                let reopened = {
                    let _names = self.core.names()?;
                    self.reopen_implicit(inode, held, access, work)?
                };
                operation(reopened)
            }
            (None, Access::Read) => {
                let (transient, _) = {
                    let _names = self.core.names()?;
                    self.open_node(inode, libc::O_RDONLY, false, work)?
                };
                let result = operation(transient);
                let _ = self.core.state()?.discard_file_handle(inode, transient);
                result
            }
        }
    }

    pub(super) fn reopen_implicit(
        &self,
        inode: u64,
        stale: Option<u64>,
        access: Access,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        let writes = access == Access::Write || {
            let state = self.core.state()?;
            stale
                .and_then(|handle| state.files.get(&handle))
                .is_some_and(|file| file.claim.is_some())
        };
        let flags = if writes { libc::O_RDWR } else { libc::O_RDONLY };
        let (opened, _) = self.open_node(inode, flags, false, work)?;
        let mut state = self.core.state()?;
        let current = state.implicit.get(&inode).map(|implicit| implicit.0);
        if current.is_some() && current != stale {
            // Another callback reopened it meanwhile.
            let _ = state.discard_file_handle(inode, opened);
            return current.ok_or_else(|| libc::ESTALE.into());
        }
        let storage = work.claim(
            std::mem::size_of::<(u64, (u64, crate::NativeAllocation))>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        if let Some(stale) = state.implicit.insert(inode, (opened, storage)) {
            let _ = state.discard_file_handle(inode, stale.0);
        }
        Ok(opened)
    }
}
