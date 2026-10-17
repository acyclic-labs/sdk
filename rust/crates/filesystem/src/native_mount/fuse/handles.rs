use super::*;

pub(super) type FuseFile = Arc<FuseOwned<Arc<dyn MountOpenFile>>>;

#[derive(Clone)]
pub(super) enum FuseOpenFile {
    Source(FuseFile),
    Deferred(Arc<DeferredOpenFile>),
}

impl FuseOpenFile {
    pub(super) fn file(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseFile, MountSourceError> {
        match self {
            Self::Source(file) => Ok(Arc::clone(file)),
            Self::Deferred(file) => file.file(work),
        }
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
                    .cloned();
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
        .map_err(MountSourceError::from)?;
    Ok(Arc::new(owner))
}

pub(super) struct FileHandle {
    pub(super) inode: u64,
    pub(super) open_file: FuseOpenFile,
    pub(super) operation: Arc<(RankedMutex<(), OPERATION>, crate::NativeAllocation)>,
    pub(super) dirty: bool,
    pub(super) writes: bool,
    pub(super) _storage: crate::NativeAllocation,
}

pub(super) struct DeferredOpenFile {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) path: FuseOwned<MountPath>,
    pub(super) file_id: crate::FileId,
    // Initialization invokes the source's view/cache locks. It precedes VIEW;
    // callers release projection state before resolving a deferred handle.
    pub(super) file: RankedMutex<Option<FuseFile>, DEFERRED_OPEN>,
    pub(super) _allocation: crate::NativeAllocation,
}

impl DeferredOpenFile {
    pub(super) fn file(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseFile, MountSourceError> {
        initialize(&self.file, || {
            let opened = fuse_open_file(self.source.open_file_with_meter(&self.path, work)?, work)?;
            if opened.lookup_with_meter(work)?.node.file_id != self.file_id {
                return Err(MountSourceError::Stale);
            }
            Ok(opened)
        })
    }
}

fn initialize<T: Clone, const RANK: u16>(
    state: &RankedMutex<Option<T>, RANK>,
    open: impl FnOnce() -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let mut file = state.lock().map_err(|_| MountSourceError::Stale)?;
    if let Some(file) = file.as_ref() {
        return Ok(file.clone());
    }
    let opened = open()?;
    *file = Some(opened.clone());
    Ok(opened)
}

#[cfg(all(test, acyclic_fs_loom))]
mod model;

pub(super) fn changes_content(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0
}

impl ProjectionState {
    #[allow(clippy::too_many_arguments, reason = "open callback data")]
    pub(super) fn admit_file_handle(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        path: &MountPath,
        lookup: &MountLookup,
        stamp: Option<ViewStamp>,
        flags: i32,
        open_file: FuseOpenFile,
        dirty: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(u64, FopenFlags), NativeMountError> {
        work.charge(Items.delta(1))?;
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
        let storage = work.claim(
            std::mem::size_of::<(u64, FileHandle)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let operation = work.claim(
            std::mem::size_of::<(RankedMutex<(), OPERATION>, crate::NativeAllocation)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let operation = Arc::new((RankedMutex::new(()), operation));
        self.files
            .try_reserve(1)
            .map_err(NativeMountError::Allocation)?;
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.open_handles = entry.open_handles.saturating_add(1);
        entry.write_handles += u64::from(changes_content(flags));
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
                writes: changes_content(flags),
                _storage: storage,
            },
        );
        let mut open_flags = FopenFlags::empty();
        open_flags.set(
            FopenFlags::FOPEN_KEEP_CACHE,
            admitted && previous == Some(opened_version),
        );
        open_flags.set(
            FopenFlags::FOPEN_NOFLUSH,
            !(flags & libc::O_ACCMODE != libc::O_RDONLY && source.flush_on_handle_close()),
        );
        Ok((handle, open_flags))
    }

    pub(super) fn discard_file_handle(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let writes = self
            .files
            .get(&handle)
            .filter(|file| file.inode == inode)
            .ok_or(libc::ESTALE)?
            .writes;
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.open_handles == 0 {
            return Err(libc::EIO);
        }
        entry.open_handles -= 1;
        entry.write_handles -= u64::from(writes);
        self.files.remove(&handle);
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(entry) = self.by_inode.remove(&inode)
        {
            self.forget_file(entry.lookup.node.file_id, inode);
        }
        Ok(())
    }

    pub(super) fn file_handle(&self, inode: u64, handle: u64) -> Result<&FileHandle, i32> {
        self.files
            .get(&handle)
            .filter(|file| file.inode == inode)
            .ok_or(libc::ESTALE)
    }

    pub(super) fn mark_handle_dirty(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let file = self
            .files
            .get_mut(&handle)
            .filter(|file| file.inode == inode)
            .ok_or(libc::ESTALE)?;
        file.dirty = true;
        Ok(())
    }

    pub(super) fn open_inode(&self, inode: u64) -> Option<FuseOpenFile> {
        self.files
            .values()
            .find(|file| file.inode == inode)
            .map(|file| file.open_file.clone())
    }

    pub(super) fn attributes_to_read(
        &self,
        source: &dyn MountFilesystem,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<AttributeTarget>, NativeMountError> {
        work.charge(Items.delta(1))?;
        if let Some(entry) = self.by_inode.get(&inode)
            && entry.lacks_named_attributes(source, work)?
        {
            return Ok(None);
        }
        self.attribute_target(inode, work).map(Some)
    }

    pub(super) fn attribute_target(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<AttributeTarget, NativeMountError> {
        match self.open_inode(inode) {
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
            entry.kernel_content = Some(ContentVersion::of(&lookup));
            entry.lookup = lookup;
            entry.facts = stamp;
        }
    }

    pub(super) fn retain_detached_handles(&mut self, inode: u64, detached: &FuseOpenFile) {
        for file in self.files.values_mut() {
            if file.inode == inode {
                file.open_file = detached.clone();
            }
        }
    }
}

impl FuseProjection {
    // Caller holds the names lock.
    pub(super) fn open_node(
        &self,
        inode: u64,
        flags: i32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(u64, FopenFlags), NativeMountError> {
        if flags & libc::O_TRUNC != 0 {
            self.check_source_write(work)?;
        }
        let source = self.core.source.as_ref();
        let (path, file_id, stamp_before, binding_before, held) = {
            let state = self.core.state()?;
            if !state.writable && flags & libc::O_TRUNC != 0 {
                return Err(libc::EROFS.into());
            }
            let node = state.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup.node;
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
        let held = match held {
            Some((lookup, facts)) if lookup.node.file_id == file_id => source
                .unchanged_since_with_meter(&path, Some(file_id), facts, work)?
                .then_some((lookup, facts)),
            _ => None,
        };
        let (open_file, refreshed, stamp) = match held {
            Some((lookup, facts)) => {
                let allocation = work.claim(
                    std::mem::size_of::<DeferredOpenFile>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )?;
                let deferred = FuseOpenFile::Deferred(Arc::new(DeferredOpenFile {
                    source: Arc::clone(&self.core.source),
                    path: fuse_clone_path(&path, work)?,
                    file_id: lookup.node.file_id,
                    file: RankedMutex::new(None),
                    _allocation: allocation,
                }));
                (deferred, lookup, Some(facts))
            }
            None => self.open_source(&path, file_id, flags, stamp_before, work)?,
        };
        let _binding = source.acquire_binding_lease_with_meter(binding_before, work)?;
        let mut state = self.core.state()?;
        if state.path(inode).ok() != Some(&*path) {
            return Err(libc::ESTALE.into());
        }
        let entry = state.by_inode.get(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        // The open pins the version it opened: a size of any other version
        // the kernel holds is dropped before the open is answered, so the
        // kernel asks again before it reads through this handle.
        if !changes_content(flags) && entry.write_handles == 0 && !entry.kernel_reads(&refreshed) {
            state.invalidate_attributes(inode, work)?;
        }
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.lookup = refreshed;
        entry.facts = stamp;
        if let Some(binding) = entry.binding_mut(&path) {
            binding.verified = stamp;
        }
        state.admit_file_handle(
            source,
            inode,
            &path,
            &refreshed,
            stamp,
            flags,
            open_file,
            flags & libc::O_TRUNC != 0,
            work,
        )
    }

    pub(super) fn open_source(
        &self,
        path: &MountPath,
        file_id: crate::FileId,
        flags: i32,
        stamp_before: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FuseOpenFile, MountLookup, Option<ViewStamp>), NativeMountError> {
        let source = self.core.source.as_ref();
        let truncated = flags & libc::O_TRUNC != 0;
        if truncated {
            self.check_source_write(work)?;
        }
        let (open_file, opened) = source.open_file_with_lookup_with_meter(path, work)?;
        let open_file =
            FuseOpenFile::Source(fuse_open_file((open_file.file, open_file.bytes), work)?);
        if opened.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        if truncated {
            self.source_write(work, || {
                Ok(open_file.file(work)?.resize_with_meter(0, work)?)
            })?;
        }
        let unchanged = !truncated
            && match stamp_before {
                Some(stamp) => {
                    source.unchanged_since_with_meter(path, Some(file_id), stamp, work)?
                }
                None => false,
            };
        let (refreshed, stamp) = if unchanged {
            (opened, stamp_before)
        } else {
            let stamp = source.view_stamp_with_meter(work)?;
            let lookup = open_file.file(work)?.lookup_with_meter(work)?;
            if lookup.node.file_id != file_id {
                return Err(libc::ESTALE.into());
            }
            (lookup, stamp)
        };
        Ok((open_file, refreshed, stamp))
    }

    #[allow(
        clippy::mutable_key_type,
        reason = "FuseOwned hashes and compares only its immutable path, never its accounting owner."
    )]
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
            let child = ChildName::new(source, state.child_path(parent, name, work)?, work)?;
            let absent_since = state.absent_since(parent, &child);
            (child, absent_since)
        };
        let path = &child.spelled;
        let stamp = source.view_stamp_with_meter(work)?;
        let (existing, proof) = match absent_since {
            Some(held)
                if source.unchanged_since_with_meter(&child.spelled, None, held, work)? =>
            {
                (None, Some(held))
            }
            _ => (source.lookup_with_meter(path, work)?, stamp),
        };
        let mut confirmed = HashMap::new();
        let (lookup, open_file, dirty) =
            if let Some(existing) = existing {
                if flags & libc::O_EXCL != 0 {
                    return Err(libc::EEXIST.into());
                }
                if existing.node.kind != MountNodeKind::Regular {
                    return Err(libc::EISDIR.into());
                }
                self.confirm_names(&child.spelled, existing.node, &mut confirmed, work)?;
                let (open_file, lookup, _) =
                    self.open_source(path, existing.node.file_id, flags, stamp, work)?;
                (lookup, open_file, flags & libc::O_TRUNC != 0)
            } else {
                let metadata = create_metadata(mode, S_IFREG, request.uid(), request.gid());
                let created = self.source_write(work, || {
                    Ok(source.create_file(path, metadata, proof, work)?)
                })?;
                let open_file = FuseOpenFile::Source(fuse_open_file(
                    source.open_created_with_meter(path, &created, work)?,
                    work,
                )?);
                (created, open_file, true)
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
        let created = (|| -> Result<_, NativeMountError> {
            let attr = state.attr(inode, &lookup)?;
            let (handle, open_flags) = state.admit_file_handle(
                source,
                inode,
                &child.spelled,
                &lookup,
                stamp,
                flags,
                open_file,
                dirty,
                work,
            )?;
            Ok((attr, handle, open_flags))
        })();
        let (attr, handle, open_flags) = created.inspect_err(|_| {
            state.release_lookup_reference(inode, 1);
        })?;
        let ttl = state.admit_entry(source, inode, child, stamp, work)?;
        Ok((Entry { attr, ttl }, handle, open_flags))
    }
}
